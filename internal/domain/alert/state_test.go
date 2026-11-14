package alert

import (
	"errors"
	"testing"
	"time"
)

func TestLifecycle(t *testing.T) {
	t1 := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)
	t3 := t2.Add(time.Minute)
	start, err := Apply(Snapshot{}, Observation{"ep-1", SignalFiring, t1, t1})
	if err != nil || !start.Changed || start.Next.State != StateFiring {
		t.Fatalf("start=%+v err=%v", start, err)
	}
	duplicate, err := Apply(start.Next, Observation{"ep-1", SignalFiring, t1, t2})
	if err != nil || duplicate.Changed || duplicate.Next.LastObservedAt != t2 {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
	resolved, err := Apply(duplicate.Next, Observation{"ep-1", SignalResolved, t2, t3})
	if err != nil || !resolved.Changed || resolved.Next.State != StateResolved {
		t.Fatalf("resolve=%+v err=%v", resolved, err)
	}
	_, err = Apply(resolved.Next, Observation{"ep-1", SignalFiring, t1, t3})
	if !errors.Is(err, ErrOutOfOrder) && !errors.Is(err, ErrEpisodeClosed) {
		t.Fatalf("expected old episode protection, got %v", err)
	}
	_, err = Apply(resolved.Next, Observation{"ep-2", SignalFiring, t3, t3})
	if !errors.Is(err, ErrEpisodeMismatch) {
		t.Fatalf("unexpected episode: %v", err)
	}
}

func TestStaleIsNotResolved(t *testing.T) {
	now := time.Now().UTC()
	first, err := Apply(Snapshot{}, Observation{"ep-1", SignalFiring, now, now})
	if err != nil {
		t.Fatal(err)
	}
	stale, err := Apply(first.Next, Observation{"ep-1", SignalStale, now.Add(time.Second), now.Add(time.Second)})
	if err != nil || stale.Next.State != StateStale {
		t.Fatalf("stale=%+v %v", stale, err)
	}
	back, err := Apply(stale.Next, Observation{"ep-1", SignalFiring, now.Add(2 * time.Second), now.Add(2 * time.Second)})
	if err != nil || back.Next.State != StateFiring {
		t.Fatalf("back=%+v %v", back, err)
	}
}
func TestCannotResolveUnknown(t *testing.T) {
	now := time.Now().UTC()
	_, err := Apply(Snapshot{}, Observation{"ep", SignalResolved, now, now})
	if !errors.Is(err, ErrNoActiveEpisode) {
		t.Fatalf("got %v", err)
	}
}

// Missing occurred_at must not wipe LastTransitionAt; otherwise the
// out-of-order guard silently stops protecting later stale duplicates.
func TestMissingOccurredAtKeepsTransitionTime(t *testing.T) {
	t1 := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)
	start, err := Apply(Snapshot{}, Observation{"ep-1", SignalFiring, t1, t1})
	if err != nil {
		t.Fatal(err)
	}
	// Stale marker without a source timestamp.
	stale, err := Apply(start.Next, Observation{"ep-1", SignalStale, time.Time{}, t2})
	if err != nil || !stale.Changed || stale.Next.State != StateStale {
		t.Fatalf("stale=%+v err=%v", stale, err)
	}
	if !stale.Next.LastTransitionAt.Equal(t1) {
		t.Fatalf("LastTransitionAt wiped to %v, want %v", stale.Next.LastTransitionAt, t1)
	}
	// An older firing carrying a timestamp must still be rejected as out-of-order.
	old := t1.Add(-time.Hour)
	_, err = Apply(stale.Next, Observation{"ep-1", SignalFiring, old, t2})
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("expected out-of-order protection to survive, got %v", err)
	}
}

func TestInitialFiringWithoutOccurredAtStartsUnknownOrdering(t *testing.T) {
	recv := time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)
	start, err := Apply(Snapshot{}, Observation{"ep-1", SignalFiring, time.Time{}, recv})
	if err != nil || !start.Changed || start.Next.LastTransitionAt != (time.Time{}) {
		t.Fatalf("start=%+v err=%v", start, err)
	}
}
