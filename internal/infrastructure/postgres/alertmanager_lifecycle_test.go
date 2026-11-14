package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/alert-platform/internal/adapter/translate"
)

// P0-01 (rh-p0-01): Alertmanager 事件身份回归 —— 逻辑告警身份（fingerprint）
// 与每次观察身份（observation key）分离后，一个告警周期的
// Firing → Firing(重复投递) → Resolved → 下一周期 Firing 全链路在真实 PG 上
// 必须语义正确：Resolved 持久化且状态机正确转换，下一周期 Firing 不被误判为
// 重复观察。修复前同一 fingerprint 的 Firing/Resolved 共用 source_event_key，
// Resolved 撞 event_receipts 唯一索引被当重复丢弃（恢复丢失）。
func TestAlertmanagerEpisodeLifecycle(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()

	// 默认渠道：让 resolved 转换真正落通知任务（否则 unrouted 无行可断言）。
	if _, err := pool.Exec(ctx, `
		INSERT INTO notification_channels (id, tenant_id, name, type, config, is_default)
		VALUES ('dddd0000-0000-0000-0000-000000000001', $1, 'default-webhook', 'webhook',
		        '{"webhook_url":"http://example.invalid/hook"}', true)`, tenantA); err != nil {
		t.Fatalf("seed channel: %v", err)
	}

	const fingerprint = "fp-am-abc123"
	starts1 := "2026-11-15T10:00:00Z"
	ends1 := "2026-11-15T10:30:00Z"
	starts2 := "2026-11-15T11:00:00Z" // 下一周期（同 fingerprint，新 startsAt）

	labelsJSON := `{"alertname":"LatencyHigh","cluster":"c1","pod":"p-1","severity":"critical"}`

	webhook := func(status, startsAt, endsAt string) []byte {
		alert := fmt.Sprintf(`{"status":%q,"labels":%s,"annotations":{"summary":"s"},"startsAt":%q,"endsAt":%q,"fingerprint":%q}`,
			status, labelsJSON, startsAt, endsAt, fingerprint)
		return []byte(fmt.Sprintf(`{"version":"4","groupKey":"g1","status":%q,"receiver":"r","alerts":[%s]}`,
			status, alert))
	}

	apply := func(t *testing.T, body []byte, transportID string) {
		t.Helper()
		events, err := translate.AlertManagerBatch(tenantA, srcDF, "c1", body, time.Now())
		if err != nil {
			t.Fatalf("translate: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("events = %d, want 1", len(events))
		}
		ev := events[0]
		ev.TransportId = transportID
		if _, err := store.ApplyEvent(ctx, ev); err != nil {
			t.Fatalf("apply %s: %v", transportID, err)
		}
	}

	// ① 第一周期 Firing：开 episode。
	apply(t, webhook("firing", starts1, "0001-01-01T00:00:00Z"), "am-t1")
	if got := count(t, pool, `SELECT count(*) FROM alert_instances`); got != 1 {
		t.Fatalf("after first firing: instances = %d, want 1", got)
	}
	assertEpisodeState(t, pool, starts1, "firing")

	// ② 同一观察重复投递（新 transport id、同 startsAt）：幂等吸收，不新增转换。
	apply(t, webhook("firing", starts1, "0001-01-01T00:00:00Z"), "am-t1-retry")
	if got := count(t, pool, `SELECT count(*) FROM alert_transitions`); got != 1 {
		t.Fatalf("after repeat firing: transitions = %d, want 1", got)
	}
	assertEpisodeState(t, pool, starts1, "firing")

	// ③ Resolved：必须持久化并转换状态（修复前被 receipt 唯一索引吞掉）。
	apply(t, webhook("resolved", starts1, ends1), "am-t2")
	assertEpisodeState(t, pool, starts1, "resolved")
	if got := count(t, pool, `SELECT count(*) FROM alert_transitions WHERE to_state='resolved'`); got != 1 {
		t.Fatalf("resolved transitions = %d, want 1", got)
	}
	if got := count(t, pool, `SELECT count(*) FROM alert_instances WHERE ends_at IS NOT NULL`); got != 1 {
		t.Fatalf("instances with ends_at = %d, want 1 (episode closed)", got)
	}
	// 恢复通知任务必须存在（恢复消息不丢）。
	if got := count(t, pool, `SELECT count(*) FROM notification_outbox WHERE payload->>'notification_type'='resolved'`); got < 1 {
		t.Fatalf("resolved notification tasks = %d, want >= 1", got)
	}

	// ④ 下一周期 Firing（同 fingerprint、新 startsAt）：开新 episode，
	// 不被误判为重复观察、也不复活旧 episode。
	apply(t, webhook("firing", starts2, "0001-01-01T00:00:00Z"), "am-t3")
	if got := count(t, pool, `SELECT count(*) FROM alert_instances`); got != 2 {
		t.Fatalf("after next-cycle firing: instances = %d, want 2", got)
	}
	if got := count(t, pool, `SELECT count(*) FROM alert_instances WHERE monitor_state='firing' AND starts_at=$1`, mustTime(t, starts2)); got != 1 {
		t.Fatalf("next-cycle firing instances = %d, want 1", got)
	}
	assertEpisodeState(t, pool, starts1, "resolved") // 旧 episode 保持 resolved

	// ⑤ 观察身份：3 次独立观察各占一条 receipt（重复投递不新增）。
	if got := count(t, pool, `SELECT count(*) FROM event_receipts`); got != 3 {
		t.Fatalf("receipts = %d, want 3 (firing / repeat-dedup / resolved / next-firing)", got)
	}
}

// assertEpisodeState asserts the episode opened at startsAt sits in `want`.
func assertEpisodeState(t *testing.T, pool *pgxpool.Pool, startsAt, want string) {
	t.Helper()
	var state string
	err := pool.QueryRow(context.Background(),
		`SELECT monitor_state FROM alert_instances WHERE starts_at=$1`,
		mustTime(t, startsAt)).Scan(&state)
	if err != nil {
		t.Fatalf("episode @ %s: %v", startsAt, err)
	}
	if state != want {
		t.Fatalf("episode @ %s: monitor_state = %q, want %q", startsAt, state, want)
	}
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return at
}
