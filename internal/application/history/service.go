// Package history (INC-007 / P10-003): read-side access to historical events.
// PG serves the hot window (now - pgWindow); ClickHouse serves older data.
// Cross-boundary queries merge both sides (dedup by transition_id, newest
// first). The boundary is relative to NOW, not to the caller's `to`: a query
// for an old day must route to the archive even when the window is short.
package history

import (
	"context"
	"sort"
	"time"
)

// Transition is one archived state transition.
type Transition struct {
	TransitionID string    `json:"transition_id"`
	InstanceID   string    `json:"instance_id"`
	FromState    string    `json:"from_state"`
	ToState      string    `json:"to_state"`
	EventID      string    `json:"event_id"`
	At           time.Time `json:"at"`
}

// Store is a read port (PG window or ClickHouse archive).
type Store interface {
	// ListTransitionsRange returns transitions in [from,to), newest first.
	ListTransitionsRange(ctx context.Context, tenantID string, from, to time.Time, limit int) ([]Transition, error)
}

// Coverage states how much of the requested range could be answered.
const (
	CoverageFull     = "full"            // archive configured, or range entirely hot
	CoveragePGWindow = "pg-window-only"  // archive NOT configured; older data unavailable
)

// Result is one history page.
type Result struct {
	Transitions []Transition `json:"transitions"`
	// Archived is true when the ClickHouse archive answered (any part).
	Archived bool `json:"archived"`
	// Truncated means more rows matched than limit; NextFrom is the cursor
	// (exclusive upper bound) for the next page.
	Truncated bool       `json:"truncated"`
	NextFrom  *time.Time `json:"next_from,omitempty"`
	// Coverage tells the caller whether the requested window is fully covered.
	Coverage string `json:"coverage"`
}

// Service routes queries to the PG window or the CH archive.
type Service struct {
	pg       Store
	archive  Store // nil until CH is wired
	pgWindow time.Duration
	now      func() time.Time
}

// NewService builds the history service; archive may be nil (PG-only mode).
func NewService(pg Store, archive Store, pgWindow time.Duration) *Service {
	if pgWindow <= 0 {
		pgWindow = 168 * time.Hour
	}
	return &Service{pg: pg, archive: archive, pgWindow: pgWindow, now: time.Now}
}

// SetClock overrides the clock (tests).
func (s *Service) SetClock(now func() time.Time) { s.now = now }

// Query returns at most limit transitions in [from,to), newest first, merging
// the hot (PG) and cold (CH) sides when the range crosses the boundary.
func (s *Service) Query(ctx context.Context, tenantID string, from, to time.Time, limit int) (Result, error) {
	if limit <= 0 {
		limit = 500
	}
	hotStart := s.now().Add(-s.pgWindow)

	// Fully hot (or older than the window but no archive to consult).
	if !from.Before(hotStart) {
		rows, err := s.pg.ListTransitionsRange(ctx, tenantID, from, to, limit)
		if err != nil {
			return Result{}, err
		}
		res := Result{Transitions: rows, Coverage: CoverageFull}
		if len(rows) == limit {
			res.Truncated = true
			res.NextFrom = nextFrom(rows)
		}
		return res, nil
	}

	// No archive: PG can only answer the hot part; say so explicitly.
	if s.archive == nil {
		rows, err := s.pg.ListTransitionsRange(ctx, tenantID, maxTime(from, hotStart), to, limit)
		if err != nil {
			return Result{}, err
		}
		res := Result{Transitions: rows, Coverage: CoveragePGWindow}
		if len(rows) == limit {
			res.Truncated = true
			res.NextFrom = nextFrom(rows)
		}
		return res, nil
	}

	coldTo := minTime(to, hotStart)
	var cold []Transition
	if from.Before(coldTo) {
		rows, err := s.archive.ListTransitionsRange(ctx, tenantID, from, coldTo, limit+1)
		if err != nil {
			return Result{}, err
		}
		cold = rows
	}
	var hot []Transition
	if hotStart.Before(to) {
		rows, err := s.pg.ListTransitionsRange(ctx, tenantID, maxTime(from, hotStart), to, limit+1)
		if err != nil {
			return Result{}, err
		}
		hot = rows
	}

	merged := mergeDedup(cold, hot)
	if len(merged) > limit {
		merged = merged[:limit]
		res := Result{Transitions: merged, Archived: true, Truncated: true, Coverage: CoverageFull}
		res.NextFrom = nextFrom(merged)
		return res, nil
	}
	res := Result{Transitions: merged, Archived: true, Coverage: CoverageFull}
	if len(cold) > limit || len(hot) > limit {
		// One side filled its limit+1 probe: more rows exist beyond this page.
		res.Truncated = true
		res.NextFrom = nextFrom(merged)
	}
	return res, nil
}

// mergeDedup merges two newest-first lists, dropping duplicate transition ids
// (a row can exist in both sides mid-archival), newest first.
func mergeDedup(a, b []Transition) []Transition {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]Transition, 0, len(a)+len(b))
	for _, list := range [][]Transition{a, b} {
		for _, t := range list {
			if _, ok := seen[t.TransitionID]; ok {
				continue
			}
			seen[t.TransitionID] = struct{}{}
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].At.Equal(out[j].At) {
			return out[i].TransitionID > out[j].TransitionID
		}
		return out[i].At.After(out[j].At)
	})
	return out
}

func nextFrom(rows []Transition) *time.Time {
	if len(rows) == 0 {
		return nil
	}
	t := rows[len(rows)-1].At
	return &t
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}