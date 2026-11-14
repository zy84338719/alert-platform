// Package occurrence implements HT-003 occurrence rules: one-shot events
// (e.g. HUATUO OOM kills) never fabricate a resolved follow-up — each event
// is recorded as an occurrence, and a burst inside a sliding window promotes
// to ONE synthetic firing alert (threshold-based aggregation), which follows
// the normal stateful lifecycle (recovered only by an explicit rule or human).
package occurrence

import (
	"sync"
	"time"
)

// Aggregator counts occurrences per key in a sliding window.
type Aggregator struct {
	mu       sync.Mutex
	window   time.Duration
	threshold int
	events   map[string][]time.Time
	fired    map[string]time.Time // last promotion per key (cooldown = window)
}

// New builds an aggregator: threshold events within window promote the key.
func New(threshold int, window time.Duration) *Aggregator {
	return &Aggregator{
		window: window, threshold: threshold,
		events: map[string][]time.Time{}, fired: map[string]time.Time{},
	}
}

// Observe records one occurrence; returns true when the burst threshold is
// hit (promote to a stateful firing alert). The promotion re-arms only after
// the window cools down — no alert storm.
func (a *Aggregator) Observe(key string, at time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	cutoff := at.Add(-a.window)
	list := a.events[key]
	kept := list[:0]
	for _, t := range list {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	kept = append(kept, at)
	a.events[key] = kept
	if len(kept) >= a.threshold {
		if last, ok := a.fired[key]; ok && at.Sub(last) < a.window {
			return false // already promoted for this burst
		}
		a.fired[key] = at
		return true
	}
	return false
}

// Count returns the in-window count for a key (tests/metrics).
func (a *Aggregator) Count(key string, at time.Time) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	cutoff := at.Add(-a.window)
	n := 0
	for _, t := range a.events[key] {
		if t.After(cutoff) {
			n++
		}
	}
	return n
}
