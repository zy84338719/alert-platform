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

// Leaser is the named single-writer lease port (P10-008): with N worker
// replicas only the lease owner runs the cleanup. Optional — unit tests and
// single-process tooling leave it unset.
type Leaser interface {
	AcquireNamedLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error)
	ReleaseNamedLease(ctx context.Context, name, owner string) error
}

// Service runs the retention loop.
type Service struct {
	store    Store
	window   time.Duration
	interval time.Duration
	deleted  prometheus.Counter
	retained prometheus.Counter
	leaser   Leaser
	owner    string
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

// SetLeaser enables single-writer leasing (worker wiring).
func (s *Service) SetLeaser(l Leaser) { s.leaser = l }

// SetOwner enables leasing with this replica's owner id; empty disables it.
func (s *Service) SetOwner(owner string) { s.owner = owner }

// Run cleans until ctx is done.
func (s *Service) Run(ctx context.Context) error {
	logx.CtxInfo(ctx, "retention job started",
		"window", s.window.String(), "interval", s.interval.String())
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	defer func() {
		if s.leaser != nil && s.owner != "" {
			_ = s.leaser.ReleaseNamedLease(context.Background(), "retention.cleanup", s.owner)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if s.leaser != nil && s.owner != "" {
				ok, err := s.leaser.AcquireNamedLease(ctx, "retention.cleanup", s.owner, s.interval*3)
				if err != nil {
					logx.CtxError(ctx, "retention lease failed", logx.ErrKey, err)
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
