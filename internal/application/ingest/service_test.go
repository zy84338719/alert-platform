package ingest

import (
	"context"
	"strings"
	"testing"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// AUD-007 failure matrix: oversized events / over-long batches are caller
// errors, not outages. They must be rejected before the broker sees them.

type nopPublisher struct{ calls int }

func (n *nopPublisher) PublishEvent(context.Context, string, [][2]string, []byte) error {
	n.calls++
	return nil
}
func (n *nopPublisher) PublishEvents(context.Context, []BatchMessage) error {
	n.calls++
	return nil
}

func validEvent(id string) *eventv1.AlertEvent {
	return &eventv1.AlertEvent{
		EventId: id, TransportId: id, TenantId: "tenant-a",
		Source:    &eventv1.SourceRef{SourceId: "src-1", ExternalRuleId: "rule-1"},
		Resource:  &eventv1.ResourceRef{ClusterId: "c1", ResourceType: "pod", ResourceUid: "p1"},
		Kind:      eventv1.EventKind_EVENT_KIND_STATEFUL,
		Signal:    eventv1.EventSignal_EVENT_SIGNAL_FIRING,
		Severity:  eventv1.Severity_SEVERITY_CRITICAL,
		AlertName: "LatencyHigh",
		EventKey:  id,
	}
}

func TestPushBatchRejectsOversizedEvent(t *testing.T) {
	pub := &nopPublisher{}
	svc := NewService(pub)
	ev := validEvent("big-1")
	ev.Labels = map[string]string{"blob": strings.Repeat("x", MaxEventBytes+1024)}
	_, err := svc.PushBatch(context.Background(), "b1", []*eventv1.AlertEvent{ev})
	if err == nil {
		t.Fatal("oversized event accepted")
	}
	if !IsValidation(err) {
		t.Fatalf("oversized event error is not a ValidationError: %v", err)
	}
	if !strings.Contains(err.Error(), "per-event limit") {
		t.Fatalf("error lacks size detail: %v", err)
	}
	if pub.calls != 0 {
		t.Fatalf("oversized event reached the publisher (%d calls)", pub.calls)
	}
}

func TestPushBatchRejectsTooManyEvents(t *testing.T) {
	svc := NewService(&nopPublisher{})
	events := make([]*eventv1.AlertEvent, MaxBatchEvents+1)
	for i := range events {
		ev := validEvent("e")
		ev.TransportId = string(rune('a'+i%26)) + ev.TransportId
		events[i] = ev
	}
	if _, err := svc.PushBatch(context.Background(), "b2", events); err == nil || !IsValidation(err) {
		t.Fatalf("over-long batch: %v (want ValidationError)", err)
	}
}

func TestPushBatchValidPassesValidation(t *testing.T) {
	pub := &nopPublisher{}
	svc := NewService(pub)
	if _, err := svc.PushBatch(context.Background(), "b3", []*eventv1.AlertEvent{validEvent("ok-1")}); err != nil {
		t.Fatalf("valid batch: %v", err)
	}
	if pub.calls != 1 {
		t.Fatalf("publisher calls = %d", pub.calls)
	}
}
