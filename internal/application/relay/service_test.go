package relay

import (
	"context"
	"errors"
	"testing"
	"time"
)

// AUD-011 failure matrix: the relay must (a) publish in claim order,
// (b) stop at the first failure and requeue this + every later claim so a
// newer same-key event can never overtake, and (c) surface sweep counts.

type fakeStore struct {
	rows      []OutboxRow
	published []string
	requeued  []string
	delay     time.Duration
	reqN      int64
	deadN     int64
	sweepErr  error
}

func (f *fakeStore) AcquireOutboxLease(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}
func (f *fakeStore) ReleaseOutboxLease(context.Context, string) error { return nil }
func (f *fakeStore) SweepOutbox(context.Context, time.Duration, int) (int64, int64, error) {
	return f.reqN, f.deadN, f.sweepErr
}
func (f *fakeStore) ClaimOutboxRows(context.Context, int, int) ([]OutboxRow, error) {
	return f.rows, nil
}
func (f *fakeStore) MarkOutboxPublished(_ context.Context, id string) error {
	f.published = append(f.published, id)
	return nil
}
func (f *fakeStore) RequeueOutboxRows(_ context.Context, ids []string, delay time.Duration) error {
	f.requeued = append(f.requeued, ids...)
	f.delay = delay
	return nil
}
func (f *fakeStore) OutboxStats(context.Context) (int64, int64, int64, error) {
	return 0, 0, 0, nil
}

type fakePublisher struct {
	failAt  int
	calls   int
	keys    []string
	headers []string
}

func (f *fakePublisher) PublishEvent(_ context.Context, key string, headers [][2]string, _ []byte) error {
	f.calls++
	f.keys = append(f.keys, key)
	for _, h := range headers {
		if h[0] == "outbox_id" {
			f.headers = append(f.headers, h[1])
		}
	}
	if f.failAt > 0 && f.calls == f.failAt {
		return errors.New("broker unavailable")
	}
	return nil
}

func outboxRows(ids ...string) []OutboxRow {
	rows := make([]OutboxRow, 0, len(ids))
	for i, id := range ids {
		rows = append(rows, OutboxRow{
			ID: id, TenantID: "t1", AggregateID: "agg-1",
			EventType: "alert.state_changed.v1", Payload: []byte(`{}`), Attempts: i + 1,
		})
	}
	return rows
}

func TestRelayPublishesInOrder(t *testing.T) {
	store := &fakeStore{rows: outboxRows("r1", "r2", "r3")}
	pub := &fakePublisher{}
	svc := NewService(store, pub, "00000000-0000-0000-0000-000000000001")

	n, err := svc.RunOnce(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("publish: %v n=%d", err, n)
	}
	if len(store.published) != 3 || store.published[0] != "r1" || store.published[2] != "r3" {
		t.Fatalf("published order = %v", store.published)
	}
	if len(pub.keys) != 3 || pub.keys[0] != "t1|agg-1" {
		t.Fatalf("keys = %v", pub.keys)
	}
	if len(pub.headers) != 3 || pub.headers[1] != "r2" {
		t.Fatalf("outbox_id headers = %v", pub.headers)
	}
}

func TestRelayStopsAtFirstFailureAndRequeuesTail(t *testing.T) {
	store := &fakeStore{rows: outboxRows("r1", "r2", "r3")}
	pub := &fakePublisher{failAt: 2}
	svc := NewService(store, pub, "00000000-0000-0000-0000-000000000001")

	n, err := svc.RunOnce(context.Background())
	if err == nil {
		t.Fatal("want publish error")
	}
	if n != 1 || len(store.published) != 1 || store.published[0] != "r1" {
		t.Fatalf("published = %v n=%d, want only r1", store.published, n)
	}
	if len(store.requeued) != 2 || store.requeued[0] != "r2" || store.requeued[1] != "r3" {
		t.Fatalf("requeued = %v, want [r2 r3] (r3 must not overtake)", store.requeued)
	}
	// The failed row (attempts=2) backs off exponentially, bounded at 30s.
	if store.delay < time.Second || store.delay > 30*time.Second {
		t.Fatalf("backoff = %s", store.delay)
	}
}

func TestRelaySweepDeadLettersAndContinues(t *testing.T) {
	store := &fakeStore{rows: outboxRows("r9"), deadN: 2}
	svc := NewService(store, &fakePublisher{}, "00000000-0000-0000-0000-000000000001")
	if n, err := svc.RunOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("run after sweep: %v n=%d", err, n)
	}
}

func TestRelayBackoffCap(t *testing.T) {
	if d := backoff(1); d != time.Second {
		t.Fatalf("backoff(1) = %s", d)
	}
	if d := backoff(2); d != 2*time.Second {
		t.Fatalf("backoff(2) = %s", d)
	}
	if d := backoff(50); d != 30*time.Second {
		t.Fatalf("backoff(50) = %s", d)
	}
}