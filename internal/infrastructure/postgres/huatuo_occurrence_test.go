package postgres

// P0-05 (rh-p0-05) 失败测试：HUATUO 链路 ——
// ① occurrence 处理后必须可在事件历史查询到（修复前 ApplyEvent 对
//    OCCURRENCE 只写 event_receipts 即提交，一次性内核异常不可见）；
// ② HUATUO 实体身份（container_id/hostname → resource_uid）映射后，
//    promoted firing 的指纹必须成功生成（修复前 EntityUID 缺失 →
//    "incomplete alert identity"，告警进 Kafka 后在 Processor 侧失败）。

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/application/query"
)

const (
	srcHT   = "cccc0000-0000-0000-0000-000000000002"
	htUID   = "containerd://a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
	htClust = "ht-cluster"
)

// huatuoEvent mirrors the collector's canonical mapping (contract:
// contracts/external/huatuo/v1/contract.md §5).
func huatuoEvent(transportID string, kind eventv1.EventKind, signal eventv1.EventSignal, severity eventv1.Severity, alertName, ruleID string, at time.Time) *eventv1.AlertEvent {
	return &eventv1.AlertEvent{
		EventId:     transportID,
		TransportId: transportID,
		TenantId:    tenantA,
		Source: &eventv1.SourceRef{
			SourceId:        srcHT,
			SourceType:      eventv1.SourceType_SOURCE_TYPE_HUATUO,
			ExternalEventId: transportID,
			ExternalRuleId:  ruleID,
		},
		Resource: &eventv1.ResourceRef{
			ClusterId:    htClust,
			ResourceType: "container",
			ResourceUid:  htUID,
			ResourceName: "pay-web-7d9f",
			Namespace:    "prod-pay",
		},
		Kind:        kind,
		Signal:      signal,
		Severity:    severity,
		AlertName:   alertName,
		Labels:      map[string]string{"tracer_name": "memory_oom_kill", "hostname": "node-7", "region": "cn-east-1"},
		OccurredAt:  timestamppb.New(at),
		ReceivedAt:  timestamppb.New(at.Add(time.Second)),
		EventKey:    transportID,
		RawPayload:  []byte(`{"trigger":{"pid":1234,"comm":"java"}}`),
		Annotations: map[string]string{"cloudevent_type": "tech.huatuo.kernel.event"},
	}
}

func TestHuatuoOccurrencePersistedToEventHistory(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 15, 9, 41, 22, 100000000, time.UTC)

	ev := huatuoEvent("7f9d3a1e-5b2c-4f8a-9e1d-3c6b2a7d4f01",
		eventv1.EventKind_EVENT_KIND_OCCURRENCE, eventv1.EventSignal_EVENT_SIGNAL_OCCURRED,
		eventv1.Severity_SEVERITY_INFO, "HUATUO memory_oom_kill", "huatuo-memory_oom_kill", at)
	dup, err := store.ApplyEvent(ctx, ev)
	if err != nil {
		t.Fatalf("apply occurrence: %v", err)
	}
	if dup {
		t.Fatal("first occurrence must not be a duplicate")
	}

	// 事件历史必须可查询（修复前：只有 receipt，工作台看不到任何记录）。
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_history
		WHERE tenant_id=$1 AND event_id=$2 AND kind='occurrence'`,
		tenantA, ev.GetEventId()).Scan(&n); err != nil {
		t.Fatalf("event_history query: %v", err)
	}
	if n != 1 {
		t.Fatalf("event_history rows = %d, want 1 — the occurrence vanished", n)
	}
	var entityUID string
	if err := pool.QueryRow(ctx, `
		SELECT entity_uid FROM event_history WHERE tenant_id=$1 AND event_id=$2`,
		tenantA, ev.GetEventId()).Scan(&entityUID); err != nil {
		t.Fatal(err)
	}
	if entityUID != htUID {
		t.Fatalf("entity_uid = %q, want the stable container id", entityUID)
	}

	// 重复投递：幂等吸收，不重复落历史。
	if _, err := store.ApplyEvent(ctx, ev); err != nil {
		t.Fatalf("apply duplicate: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_history WHERE tenant_id=$1 AND event_id=$2`,
		tenantA, ev.GetEventId()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("event_history rows after redelivery = %d, want 1", n)
	}

	// 读面：query.Service.ListEvents 可查询到该 occurrence（事件历史/工作台）。
	qs := NewQueryStore(pool)
	rows, err := qs.ListEvents(ctx, query.EventFilter{TenantID: tenantA, Limit: 10})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("ListEvents rows = %d, want 1", len(rows))
	}
	r := rows[0]
	if r.EventID != ev.GetEventId() || r.Kind != "occurrence" || r.EntityUID != htUID ||
		r.Signal != "occurred" || r.AlertName != ev.GetAlertName() {
		t.Fatalf("event row mismatch: %+v", r)
	}
}

func TestHuatuoBurstFiringFingerprintSucceeds(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 15, 9, 45, 0, 0, time.UTC)

	// promoted burst firing（collector 映射后的 canonical 形态）
	ev := huatuoEvent("htp-7f9d3a1e",
		eventv1.EventKind_EVENT_KIND_STATEFUL, eventv1.EventSignal_EVENT_SIGNAL_FIRING,
		eventv1.Severity_SEVERITY_ERROR, "HUATUO memory_oom_kill burst on containerd://a1b2", "huatuo-burst", at)
	if _, err := store.ApplyEvent(ctx, ev); err != nil {
		t.Fatalf("apply firing (fingerprint must succeed, not 'incomplete alert identity'): %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM alert_instances
		WHERE tenant_id=$1 AND entity_uid=$2 AND monitor_state='firing'`,
		tenantA, htUID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("firing instances with entity_uid = %d, want 1", n)
	}
	// incident 1:1 开立（firing episode 同事务）
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM incidents WHERE tenant_id=$1`, tenantA).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("incidents = %d, want 1", n)
	}
}
