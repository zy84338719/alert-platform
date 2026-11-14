// Package health implements the source-health reconciliation scheduler
// (PROC-006): a source that stops pushing degrades its active alerts to
// STALE — never resolved (docs/03 §8: 来源不可用时标记 Unknown/Stale，不自动
// 标记 Resolved). NoData observations likewise never auto-resolve.
package health

import (
	"context"
	"time"

	"github.com/oekit/logx"
)

// StaleSource is one source whose last push is older than the threshold.
type StaleSource struct {
	TenantID string
	SourceID string
}

// Store is the reconciliation persistence port (infrastructure/postgres).
type Store interface {
	// StaleSources returns enabled sources whose last_seen_at predates the
	// cutoff and whose health is not yet marked stale.
	StaleSources(ctx context.Context, cutoff time.Time) ([]StaleSource, error)
	// MarkSourceHealth flips the source's health marker (stale <-> ok).
	MarkSourceHealth(ctx context.Context, tenantID, sourceID, status string) error
	// MarkStaleBySources degrades active instances of the sources to stale
	// (transition + domain outbox rows included). Returns rows affected.
	MarkStaleBySources(ctx context.Context, tenantID string, sourceIDs []string, reason string) (int, error)
	// RecoveredSources returns sources marked stale whose last_seen_at is
	// fresh again (recovery is a health statement only — instances stay
	// stale until re-confirmed by a real event).
	RecoveredSources(ctx context.Context, cutoff time.Time) ([]StaleSource, error)
}

// Service runs the reconciliation loop.
type Service struct {
	store    Store
	after    time.Duration // stale threshold
	interval time.Duration
}

// NewService builds the scheduler; defaults: threshold 5m, tick 30s.
func NewService(store Store) *Service {
	return &Service{store: store, after: 5 * time.Minute, interval: 30 * time.Second}
}

// SetTuning overrides threshold/interval (worker config wiring).
func (s *Service) SetTuning(after, interval time.Duration) {
	if after > 0 {
		s.after = after
	}
	if interval > 0 {
		s.interval = interval
	}
}

// Run reconciles until ctx is done.
func (s *Service) Run(ctx context.Context) error {
	logx.CtxInfo(ctx, "source health reconciler started",
		"stale_after", s.after.String(), "interval", s.interval.String())
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.reconcileOnce(ctx)
		}
	}
}

func (s *Service) reconcileOnce(ctx context.Context) {
	now := time.Now()

	// 1) Stale direction: sources silent past the threshold.
	if stale, err := s.store.StaleSources(ctx, now.Add(-s.after)); err != nil {
		logx.CtxError(ctx, "reconcile: list stale sources", logx.ErrKey, err)
	} else {
		byTenant := map[string][]string{}
		for _, src := range stale {
			byTenant[src.TenantID] = append(byTenant[src.TenantID], src.SourceID)
		}
		for tenant, ids := range byTenant {
			n, err := s.store.MarkStaleBySources(ctx, tenant, ids, "source-silent")
			if err != nil {
				logx.CtxError(ctx, "reconcile: mark instances stale", logx.ErrKey, err, "tenant", tenant)
				continue
			}
			for _, id := range ids {
				if err := s.store.MarkSourceHealth(ctx, tenant, id, "stale"); err != nil {
					logx.CtxError(ctx, "reconcile: mark source stale", logx.ErrKey, err)
				}
			}
			if n > 0 {
				logx.CtxWarn(ctx, "source silent: active alerts degraded to STALE (never resolved)",
					"tenant", tenant, "sources", len(ids), "instances", n)
			}
		}
	}

	// 2) Recovery direction: push activity resumed -> health ok. Instances
	// stay stale until a fresh FIRING/RESOLVED event re-confirms them.
	if rec, err := s.store.RecoveredSources(ctx, now.Add(-s.after)); err != nil {
		logx.CtxError(ctx, "reconcile: list recovered sources", logx.ErrKey, err)
	} else {
		for _, src := range rec {
			if err := s.store.MarkSourceHealth(ctx, src.TenantID, src.SourceID, "ok"); err != nil {
				logx.CtxError(ctx, "reconcile: mark source recovered", logx.ErrKey, err)
			}
		}
	}
}
