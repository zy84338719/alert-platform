package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

type fakeStore struct {
	active    int64
	incidents int64
	sources   []SourceHeartbeat
}

func (f *fakeStore) CountActiveAlerts(context.Context) (int64, error)  { return f.active, nil }
func (f *fakeStore) CountOpenIncidents(context.Context) (int64, error) { return f.incidents, nil }
func (f *fakeStore) ListSourceHeartbeats(context.Context) ([]SourceHeartbeat, error) {
	return f.sources, nil
}

// P9-OBS-06：活跃告警/未关闭 Incident/来源心跳年龄三个 gauges；
// 禁用与从未上报的来源不产生序列；来源消失后旧序列被清除。
func TestSampleBusinessGauges(t *testing.T) {
	now := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	store := &fakeStore{
		active: 3, incidents: 1,
		sources: []SourceHeartbeat{
			{SourceID: "s1", Name: "deepflow", LastSeenAt: now.Add(-42 * time.Second), Enabled: true},
			{SourceID: "s2", Name: "alertmanager", LastSeenAt: now.Add(-5 * time.Second), Enabled: false},
			{SourceID: "s3", Name: "never", Enabled: true},
		},
	}
	svc := NewService(store)
	svc.now = func() time.Time { return now }

	if err := svc.SampleOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(activeAlerts); got != 3 {
		t.Fatalf("active_instances = %v, want 3", got)
	}
	if got := testutil.ToFloat64(openIncidents); got != 1 {
		t.Fatalf("incident_open = %v, want 1", got)
	}
	var m dto.Metric
	if err := sourceHeartbeatAge.WithLabelValues("deepflow").Write(&m); err != nil {
		t.Fatal(err)
	}
	if got := m.GetGauge().GetValue(); got != 42 {
		t.Fatalf("heartbeat age = %v, want 42", got)
	}
	if got := testutil.CollectAndCount(sourceHeartbeatAge); got != 1 {
		t.Fatalf("heartbeat series = %d, want 1 (disabled/never-seen skipped)", got)
	}

	// 来源被删除后，Reset 必须清掉旧序列（否则监控会保留幽灵来源）。
	store.sources = nil
	if err := svc.SampleOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := testutil.CollectAndCount(sourceHeartbeatAge); got != 0 {
		t.Fatalf("stale heartbeat series = %d, want 0", got)
	}
}
