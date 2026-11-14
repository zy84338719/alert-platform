package occurrence

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

// HT-003: threshold hits promote exactly once per burst window; cooldown
// prevents alert storms; the window slides.
func TestBurstPromotion(t *testing.T) {
	a := New(3, 5*time.Minute)
	occ := func(m int) time.Time { return t0.Add(time.Duration(m) * time.Minute) }

	if a.Observe("oom/node1", occ(0)) {
		t.Fatal("first occurrence promoted early")
	}
	if a.Observe("oom/node1", occ(1)) {
		t.Fatal("second occurrence promoted early")
	}
	if !a.Observe("oom/node1", occ(2)) {
		t.Fatal("third occurrence within window must promote")
	}
	// Fourth hit inside the same burst: no second promotion (cooldown).
	if a.Observe("oom/node1", occ(3)) {
		t.Fatal("double promotion inside one burst")
	}
	// After the window cools, a NEW burst promotes again.
	if a.Observe("oom/node1", occ(20)) {
		t.Fatal("stale burst promoted")
	}
	a2 := New(3, 5*time.Minute)
	_ = a2
	// New burst after cooldown: 20,21,22 rebuild the count and 22 promotes
	// (cooldown from the first burst has long elapsed).
	if a.Observe("oom/node1", occ(21)) {
		t.Fatal("second occurrence of new burst promoted early")
	}
	if !a.Observe("oom/node1", occ(22)) {
		t.Fatal("new burst must promote after cooldown")
	}
	// Different keys are independent.
	if a.Observe("oom/node2", occ(22)) || a.Observe("oom/node2", occ(23)) {
		t.Fatal("node2 promoted early")
	}
}

// Count reflects the sliding window only.
func TestWindowSlides(t *testing.T) {
	a := New(100, 1*time.Minute)
	a.Observe("k", t0)
	a.Observe("k", t0.Add(10*time.Second))
	if got := a.Count("k", t0.Add(11*time.Second)); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
	if got := a.Count("k", t0.Add(2*time.Minute)); got != 0 {
		t.Fatalf("count after window = %d, want 0", got)
	}
}
