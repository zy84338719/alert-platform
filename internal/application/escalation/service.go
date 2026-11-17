// Package escalation implements the INC-005 on-call ladder: an open incident
// unacknowledged for EscalateAfter (acceptance: 5 minutes) bumps a stage,
// records it with the policy version and pages the default channel. ACK (or
// any disposal progress) stops the ladder; maintenance-window silences apply
// at send time like every notification (docs/04 §5).
package escalation

import (
	"context"
	"time"

	"github.com/oekit/logx"
)

// EscalateAfter is the ladder interval (docs/13 INC-005 验收值).
const EscalateAfter = 5 * time.Minute

// Store is the escalation persistence port.
type Store interface {
	EscalatableIncidents(ctx context.Context, every time.Duration) ([]EscalationCandidate, error)
	RecordEscalation(ctx context.Context, cand EscalationCandidate, policyVersion int64) error
	// AcquireLease/ReleaseLease implement the single-flight lease (AUD-008):
	// only one replica scans + escalates per tick. Empty owner skips leasing
	// (unit tests / single-replica tooling).
	AcquireLease(ctx context.Context, owner string, ttl time.Duration) (bool, error)
	ReleaseLease(ctx context.Context, owner string) error
}

// EscalationCandidate mirrors the postgres row at the application boundary.
type EscalationCandidate struct {
	IncidentID   string
	TenantID     string
	CreatedAt    time.Time
	CurrentStage int
	InstanceID   string
}

// Correlator is the optional INC-002 auto-merge hook (worker wiring enables
// it once per tick before escalation).
type Correlator interface {
	CorrelateOnce(ctx context.Context) (merged int, err error)
}

// Service runs the escalation (and optional correlation) scheduler.
type Service struct {
	store    Store
	corr     Correlator
	every    time.Duration
	interval time.Duration
	owner    string // single-flight lease owner; empty = no leasing
}

// NewService builds the scheduler.
func NewService(store Store, corr Correlator) *Service {
	return &Service{store: store, corr: corr, every: EscalateAfter, interval: 30 * time.Second}
}

// SetOwner enables the single-flight lease (worker wiring passes a per-process
// UUID; tests leave it empty).
func (s *Service) SetOwner(owner string) { s.owner = owner }

// SetTuning overrides the escalation threshold and scan interval.
func (s *Service) SetTuning(every, interval time.Duration) {
	if every > 0 {
		s.every = every
	}
	if interval > 0 {
		s.interval = interval
	}
}

// Run ticks until ctx is done.
func (s *Service) Run(ctx context.Context) error {
	logx.CtxInfo(ctx, "escalation scheduler started", "every", s.every.String())
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	if s.owner != "" {
		defer func() { _ = s.store.ReleaseLease(context.Background(), s.owner) }()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if s.owner != "" {
				ok, err := s.store.AcquireLease(ctx, s.owner, s.interval*3)
				if err != nil {
					logx.CtxError(ctx, "escalation lease failed", logx.ErrKey, err)
					continue
				}
				if !ok {
					continue // another replica owns this tick
				}
			}
			s.RunOnce(ctx)
		}
	}
}

// RunOnce performs one pass (tests/ops).
func (s *Service) RunOnce(ctx context.Context) {
	if s.corr != nil {
		if merged, err := s.corr.CorrelateOnce(ctx); err != nil {
			logx.CtxError(ctx, "correlation pass failed", logx.ErrKey, err)
		} else if merged > 0 {
			logx.CtxInfo(ctx, "auto-correlated alerts into incidents", "merged", merged)
		}
	}
	cands, err := s.store.EscalatableIncidents(ctx, s.every)
	if err != nil {
		logx.CtxError(ctx, "escalation scan failed", logx.ErrKey, err)
		return
	}
	for _, c := range cands {
		if err := s.store.RecordEscalation(ctx, c, 0); err != nil {
			logx.CtxError(ctx, "escalation record failed", logx.ErrKey, err, "incident", c.IncidentID)
			continue
		}
		logx.CtxWarn(ctx, "INCIDENT ESCALATED (unacknowledged)",
			"incident", c.IncidentID, "next_stage", c.CurrentStage+1,
			"after", s.every.String(), "tenant", c.TenantID)
	}
}
