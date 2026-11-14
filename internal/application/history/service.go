// Package history (INC-007): read-side access to historical events. PG
// serves the window it retains; ClickHouse serves the archive when
// configured. The write path is the domain-outbox relay — archiving NEVER
// runs inside a PG write transaction, so history can't block alerting.
package history

import (
	"context"
	"time"
)

// Transition is one archived state transition.
type Transition struct {
	TransitionID string
	InstanceID   string
	FromState    string
	ToState      string
	EventID      string
	At           time.Time
}

// Store is the read port.
type Store interface {
	// ListTransitions returns transitions in [from,to), newest first.
	ListTransitions(ctx context.Context, tenantID string, from, to time.Time, limit int) ([]Transition, error)
}

// Service routes queries to the PG window or the CH archive.
type Service struct {
	pg       Store
	archive  Store // nil until CH is wired
	pgWindow time.Duration
}

// NewService builds the history service; archive may be nil (PG-only mode).
func NewService(pg Store, archive Store, pgWindow time.Duration) *Service {
	return &Service{pg: pg, archive: archive, pgWindow: pgWindow}
}

// Query picks the backend by age; archived=true means the answer came from
// ClickHouse (the PG-blocking guarantee holds either way).
func (s *Service) Query(ctx context.Context, tenantID string, from, to time.Time, limit int) ([]Transition, bool, error) {
	oldestPossible := to.Add(-s.pgWindow)
	if from.Before(oldestPossible) && s.archive != nil {
		rows, err := s.archive.ListTransitions(ctx, tenantID, from, to, limit)
		return rows, true, err
	}
	rows, err := s.pg.ListTransitions(ctx, tenantID, from, to, limit)
	return rows, false, err
}
