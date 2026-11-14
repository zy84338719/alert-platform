// Package retention implements retention cleanup jobs (RETN-001: event
// receipts). The window MUST stay >= the idempotency horizon (Edge retries /
// consumer replays); the default of 7d comfortably covers it.
package retention

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/oekit/logx"
)

// Store is the cleanup persistence port.
type Store interface {
	// CleanupReceipts deletes receipts last seen before the cutoff and
	// returns (deleted, retained).
	CleanupReceipts(ctx context.Context, cutoff time.Time) (deleted, retained int64, err error)
}

// ExtraStore is the secondary cleanup surface (RETN-002: notifications+audit).
type ExtraStore interface {
	CleanupNotifications(ctx context.Context, olderThan time.Time) (int64, error)
	CleanupAudit(ctx context.Context, olderThan time.Time) (int64, error)
}

type multiStore struct {
	Store
	extra ExtraStore
}

// Multi combines the receipts store with the RETN-002 cleanups.
func Multi(base Store, extra ExtraStore) Store { return multiStore{Store: base, extra: extra} }

func (m multiStore) CleanupReceipts(ctx context.Context, cutoff time.Time) (int64, int64, error) {
	d, r, err := m.Store.CleanupReceipts(ctx, cutoff)
	if err != nil {
		return d, r, err
	}
	// Notifications/audit ride the same cadence with their own (longer)
	// windows derived from the receipt window: 30d for both by default.
	older := cutoff.Add(-23 * 24 * time.Hour)
	if _, err := m.extra.CleanupNotifications(ctx, older); err != nil {
		return d, r, err
	}
	if _, err := m.extra.CleanupAudit(ctx, older); err != nil {
		return d, r, err
	}
	return d, r, nil
}

// Service runs the retention loop.
type Service struct {
	store    Store
	window   time.Duration
	interval time.Duration
	deleted  prometheus.Counter
	retained prometheus.Counter
}

// NewService builds the retention job. Counters register on the REGISTRY
// the /metrics endpoint actually serves (oekit metrics.Registry, NOT the
// client_golang default) — the wiring passes it in.
func NewService(store Store, window, interval time.Duration, reg prometheus.Registerer) *Service {
	if reg == nil {
		reg = prometheus.NewRegistry() // tests without an HTTP surface
	}
	s := &Service{
		store:    store,
		window:   window,
		interval: interval,
		deleted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "alert_platform", Subsystem: "retention", Name: "receipts_deleted_total",
			Help: "Event receipts deleted by the retention job (RETN-001).",
		}),
		retained: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "alert_platform", Subsystem: "retention", Name: "receipts_retained_total",
			Help: "Event receipts inside the retention window after a cleanup run.",
		}),
	}
	reg.MustRegister(s.deleted, s.retained)
	return s
}

// Describe/Collect wiring happens at construction in cmd via
// prometheus.MustRegister — exposed here for tests.
func (s *Service) Counters() (deleted, retained prometheus.Counter) { return s.deleted, s.retained }

// Run cleans until ctx is done.
func (s *Service) Run(ctx context.Context) error {
	logx.CtxInfo(ctx, "retention job started",
		"window", s.window.String(), "interval", s.interval.String())
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.RunOnce(ctx)
		}
	}
}

// RunOnce performs one cleanup pass (tests/ops).
func (s *Service) RunOnce(ctx context.Context) {
	deleted, retained, err := s.store.CleanupReceipts(ctx, time.Now().Add(-s.window))
	if err != nil {
		logx.CtxError(ctx, "retention cleanup failed", logx.ErrKey, err)
		return
	}
	s.deleted.Add(float64(deleted))
	s.retained.Add(float64(retained))
	logx.CtxInfo(ctx, "retention cleanup",
		"deleted", deleted, "retained", retained, "window", s.window.String())
}
