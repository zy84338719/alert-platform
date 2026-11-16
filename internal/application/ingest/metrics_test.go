package ingest

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

type fakePublisher struct{ err error }

func (f *fakePublisher) PublishEvent(context.Context, string, [][2]string, []byte) error {
	return f.err
}
func (f *fakePublisher) PublishEvents(context.Context, []BatchMessage) error { return f.err }

func metricEvent(id string) *eventv1.AlertEvent {
	return &eventv1.AlertEvent{
		EventId:     id,
		TransportId: id,
		TenantId:    "tenant-metrics",
		Source:      &eventv1.SourceRef{SourceId: "source-metrics"},
		Kind:        eventv1.EventKind_EVENT_KIND_STATEFUL,
		Signal:      eventv1.EventSignal_EVENT_SIGNAL_FIRING,
		Severity:    eventv1.Severity_SEVERITY_CRITICAL,
	}
}

// P9-OBS-02：成功批次计入 events_total 且记录批延迟；发布失败不误记成功。
func TestIngestMetrics(t *testing.T) {
	ctx := context.Background()
	before := testutil.ToFloat64(eventsTotal)

	svc := NewService(&fakePublisher{})
	if _, err := svc.PushBatch(ctx, "batch-metrics-1", []*eventv1.AlertEvent{metricEvent("m-1"), metricEvent("m-2")}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(eventsTotal) - before; got != 2 {
		t.Fatalf("events_total delta = %v, want 2", got)
	}
	var hist dto.Metric
	if err := batchDuration.Write(&hist); err != nil {
		t.Fatal(err)
	}
	if hist.GetHistogram().GetSampleCount() == 0 {
		t.Fatal("batch duration histogram recorded no observations")
	}

	// 发布失败：拒绝原因由 handler 计数，这里只保证误记成功不发生。
	beforeFail := testutil.ToFloat64(eventsTotal)
	failSvc := NewService(&fakePublisher{err: errors.New("broker down")})
	if _, err := failSvc.PushBatch(ctx, "batch-metrics-2", []*eventv1.AlertEvent{metricEvent("m-3")}); err == nil {
		t.Fatal("publisher failure must surface")
	}
	if got := testutil.ToFloat64(eventsTotal); got != beforeFail {
		t.Fatalf("failed batch incremented events_total: %v → %v", beforeFail, got)
	}
}
