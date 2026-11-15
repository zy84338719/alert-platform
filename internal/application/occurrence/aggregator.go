// Package occurrence implements HT-003 occurrence rules: one-shot events
// (e.g. HUATUO OOM kills) never fabricate a resolved follow-up — each event
// is recorded as an occurrence, and a burst inside a sliding window promotes
// to ONE synthetic firing alert (threshold-based aggregation), which follows
// the normal stateful lifecycle (recovered only by an explicit rule or human).
package occurrence

import (
	"sort"
	"sync"
	"time"
)

// DefaultMaxKeys bounds the aggregation key space per collector (P8-P1-06):
// HUATUO emits per-container/per-node keys, and the long tail never appears
// again — without a cap the maps grow for the process lifetime.
const DefaultMaxKeys = 10000

// Aggregator counts occurrences per key in a sliding window.
type Aggregator struct {
	mu        sync.Mutex
	window    time.Duration
	threshold int
	events    map[string][]time.Time
	fired     map[string]time.Time // last promotion per key (cooldown = window)
	maxKeys   int
}

// New builds an aggregator: threshold events within window promote the key.
func New(threshold int, window time.Duration) *Aggregator {
	return &Aggregator{
		window: window, threshold: threshold, maxKeys: DefaultMaxKeys,
		events: map[string][]time.Time{}, fired: map[string]time.Time{},
	}
}

// SetMaxKeys overrides the key cap (tests/ops tuning); n<=0 keeps the default.
func (a *Aggregator) SetMaxKeys(n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n > 0 {
		a.maxKeys = n
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
	// P8-P1-06: a burst of unique keys must not grow the map without bound.
	if len(a.events) > a.maxKeys {
		a.evictOverflowLocked(at)
	}
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

// Len reports the tracked key count (memory waterline metric, P8-P1-06).
func (a *Aggregator) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.events)
}

// Prune drops keys whose sliding window fully elapsed and cooldowns that
// expired (P8-P1-06). Observe only ever cleans the key it touches, so keys
// that stop appearing would otherwise live forever; the collector sweeps
// periodically. Returns the number of event keys evicted.
func (a *Aggregator) Prune(now time.Time) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pruneLocked(now)
}

func (a *Aggregator) pruneLocked(now time.Time) int {
	cutoff := now.Add(-a.window)
	evicted := 0
	for k, list := range a.events {
		if len(list) == 0 || !list[len(list)-1].After(cutoff) {
			delete(a.events, k)
			evicted++
		}
	}
	for k, t := range a.fired {
		if !t.After(cutoff) {
			delete(a.fired, k)
		}
	}
	return evicted
}

// evictOverflowLocked first drops expired keys, then evicts the
// least-recently-observed keys until the cap holds.
func (a *Aggregator) evictOverflowLocked(now time.Time) {
	a.pruneLocked(now)
	if len(a.events) <= a.maxKeys {
		return
	}
	type keyAge struct {
		key  string
		last time.Time
	}
	ages := make([]keyAge, 0, len(a.events))
	for k, list := range a.events {
		ages = append(ages, keyAge{key: k, last: list[len(list)-1]})
	}
	sort.Slice(ages, func(i, j int) bool { return ages[i].last.Before(ages[j].last) })
	for _, age := range ages {
		if len(a.events) <= a.maxKeys {
			return
		}
		delete(a.events, age.key)
		delete(a.fired, age.key)
	}
}