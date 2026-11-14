package collector

// P0-04 (rh-p0-04) 失败测试③：官方真实样本契约 —— HUATUO 事件流是
// CloudEvents 1.0 JSON（id/source/type/time/data + data.tracer_name/hostname/
// container_*），不是顶层 kind/node。以 contracts/external/huatuo/v1/fixtures
// 的官方结构样本驱动 collector，断言身份映射（P0-05 一并验收）：
// 容器事件 → resource_uid=container_id（稳定实体 UID，指纹必需）、
// 节点事件 → resource_uid=hostname；occurred_at 取 observed_timestamp。

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/example/alert-platform/internal/application/edge"
)

type captureWAL struct {
	recs []edge.Record
}

func (w *captureWAL) Append(rec edge.Record) (edge.Record, error) {
	w.recs = append(w.recs, rec)
	return rec, nil
}

func fixture(t *testing.T, name string) Event {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "contracts", "external", "huatuo", "v1", "fixtures", name))
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return Event{ID: ceID(t, raw), Name: "message", Data: strings.TrimSpace(string(raw))}
}

func ceID(t *testing.T, raw []byte) string {
	t.Helper()
	var ce struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &ce); err != nil || ce.ID == "" {
		t.Fatalf("fixture is not a CloudEvent: %v", err)
	}
	return ce.ID
}

func decodeRecord(t *testing.T, rec edge.Record) map[string]any {
	t.Helper()
	var payload struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(rec.Body, &payload); err != nil || len(payload.Events) != 1 {
		t.Fatalf("record body: %v (%d events)", err, len(payload.Events))
	}
	return payload.Events[0]
}

func TestCloudEventsContractMapping(t *testing.T) {
	w := &captureWAL{}
	s := New(nilStream{}, w, Config{
		TenantID: "ht-tenant", SourceID: "ht-source", ClusterID: "ht-cluster",
	})
	oom := fixture(t, "SYNTHETIC-cloudevent-oom.json")
	if err := s.handle(oom); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(w.recs) != 1 {
		t.Fatalf("appended %d records, want 1", len(w.recs))
	}
	ev := decodeRecord(t, w.recs[0])

	// 观察身份 = CloudEvent id（重投递幂等）
	if ev["event_key"] != oom.ID || ev["event_id"] != oom.ID {
		t.Fatalf("event id/key = %v/%v, want CloudEvent id %q", ev["event_id"], ev["event_key"], oom.ID)
	}
	res, _ := ev["resource"].(map[string]any)
	if res == nil {
		t.Fatal("no resource mapping")
	}
	// 容器身份：resource_uid = container_id（稳定 EntityUID → 指纹必需）
	if res["resource_uid"] != "containerd://a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2" {
		t.Fatalf("resource_uid = %v, want the container id", res["resource_uid"])
	}
	if res["resource_type"] != "container" {
		t.Fatalf("resource_type = %v, want container", res["resource_type"])
	}
	if res["namespace"] != "prod-pay" {
		t.Fatalf("namespace = %v, want container_host_namespace", res["namespace"])
	}
	// tracer_name 进 alert_name 与 external_rule_id
	if !strings.Contains(ev["alert_name"].(string), "memory_oom_kill") {
		t.Fatalf("alert_name = %v, want tracer_name suffix", ev["alert_name"])
	}
	src, _ := ev["source"].(map[string]any)
	if src["external_rule_id"] != "huatuo-memory_oom_kill" {
		t.Fatalf("external_rule_id = %v", src["external_rule_id"])
	}
	// occurred_at 取 observed_timestamp（2026-11-15T09:41:22.1Z）
	if ts, ok := ev["occurred_at"].(map[string]any); !ok || ts["seconds"].(float64) != float64(time.Date(2026, 11, 15, 9, 41, 22, 100000000, time.UTC).Unix()) {
		t.Fatalf("occurred_at = %v, want observed_timestamp", ev["occurred_at"])
	}
}

func TestCloudEventsNodeIdentityMapping(t *testing.T) {
	w := &captureWAL{}
	s := New(nilStream{}, w, Config{
		TenantID: "ht-tenant", SourceID: "ht-source", ClusterID: "ht-cluster",
	})
	tick := fixture(t, "SYNTHETIC-cloudevent-schedtick.json")
	if err := s.handle(tick); err != nil {
		t.Fatalf("handle: %v", err)
	}
	ev := decodeRecord(t, w.recs[0])
	res, _ := ev["resource"].(map[string]any)
	// 节点身份：resource_uid = hostname（稳定 EntityUID → 指纹必需）
	if res["resource_uid"] != "node-9" {
		t.Fatalf("resource_uid = %v, want hostname node-9", res["resource_uid"])
	}
	if res["resource_type"] != "node" {
		t.Fatalf("resource_type = %v, want node", res["resource_type"])
	}
}

// promoted bursts carry the same stable entity identity (P0-05).
func TestBurstPromotionCarriesEntityUID(t *testing.T) {
	w := &captureWAL{}
	s := New(nilStream{}, w, Config{
		TenantID: "ht-tenant", SourceID: "ht-source", ClusterID: "ht-cluster",
		PromoteThreshold: 2, PromoteWindow: time.Minute,
	})
	oom := fixture(t, "SYNTHETIC-cloudevent-oom.json")
	_ = s.handle(oom)
	if err := s.handle(oom); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(w.recs) < 3 {
		t.Fatalf("records = %d, want >=3 (2 occurrences + 1 promoted firing)", len(w.recs))
	}
	last := decodeRecord(t, w.recs[len(w.recs)-1])
	res, _ := last["resource"].(map[string]any)
	if res["resource_uid"] == "" {
		t.Fatal("promoted firing has no entity uid — fingerprint would fail with 'incomplete alert identity'")
	}
}

type nilStream struct{}

func (nilStream) Run(_ context.Context, _ func(Event) error) error { return nil }
func (nilStream) LastID() string                                   { return "" }
