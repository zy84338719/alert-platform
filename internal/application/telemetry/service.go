// Package telemetry samples business-level gauges that cannot be derived from
// event counters (P9-OBS-06): active alert instances, open incidents and the
// per-source heartbeat age. Sampling runs as a non-critical worker task.
package telemetry

import (
	"context"
	"sync"
	"time"

	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oekit/logx"
)

// SourceHeartbeat is one registered source's liveness input.
type SourceHeartbeat struct {
	SourceID   string
	TenantID   string
	Name       string
	LastSeenAt time.Time // zero = never seen
	Enabled    bool
}

// TenantCount is one tenant's gauge value. Business gauges are emitted per
// tenant so the metrics center can inject a tenant matcher server-side
// (P10-006); the tenant count is bounded by the SaaS tenant budget.
type TenantCount struct {
	TenantID string
	Count    int64
}

// Store is the read port (infrastructure/postgres).
type Store interface {
	CountActiveAlertsByTenant(ctx context.Context) ([]TenantCount, error)
	CountOpenIncidentsByTenant(ctx context.Context) ([]TenantCount, error)
	ListSourceHeartbeats(ctx context.Context) ([]SourceHeartbeat, error)
}

var (
	activeAlerts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "alert", Name: "active_instances",
		Help: "Alert instances in pending/firing/stale state, per tenant.",
	}, []string{"tenant"})
	openIncidents = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "incident", Name: "open",
		Help: "Incidents not yet closed, per tenant.",
	}, []string{"tenant"})
	sourceHeartbeatAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "source", Name: "heartbeat_age_seconds",
		Help: "Seconds since the source last pushed (enabled sources only, per tenant).",
	}, []string{"tenant", "source"})
)

// registerOnce keeps the gauges out of binaries that only import the types
// (postgres imports SourceHeartbeat for its store): the API process must not
// expose idle 0-valued business gauges.
var registerOnce sync.Once

func registerMetrics() {
	registerOnce.Do(func() {
		for _, c := range []prometheus.Collector{activeAlerts, openIncidents, sourceHeartbeatAge} {
			if err := oekitmetrics.Registry.Register(c); err != nil {
				if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
					panic(err)
				}
			}
		}
	})
}

// Leaser is the named single-writer lease port (P10-008).
type Leaser interface {
	AcquireNamedLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error)
	ReleaseNamedLease(ctx context.Context, name, owner string) error
}

// Service periodically refreshes the business gauges.
type Service struct {
	store    Store
	interval time.Duration
	now      func() time.Time
	leaser   Leaser
	owner    string
}

// NewService builds the sampler (default tick 30s).
func NewService(store Store) *Service {
	registerMetrics()
	return &Service{store: store, interval: 30 * time.Second, now: time.Now}
}

// SetTuning overrides the sample interval (tests/wiring).
func (s *Service) SetTuning(interval time.Duration) {
	if interval > 0 {
		s.interval = interval
	}
}

// SetLeaser enables single-writer sampling (worker wiring).
func (s *Service) SetLeaser(l Leaser) { s.leaser = l }

// SetOwner enables leasing with this replica's owner id; empty disables it.
func (s *Service) SetOwner(owner string) { s.owner = owner }

// Run samples until ctx is done. Failures are logged and retried on the next
// tick — a metrics outage must never affect alert processing.
func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	logx.CtxInfo(ctx, "telemetry sampler started", "interval", s.interval.String())
	defer func() {
		if s.leaser != nil && s.owner != "" {
			_ = s.leaser.ReleaseNamedLease(context.Background(), "telemetry.sampler", s.owner)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if s.leaser != nil && s.owner != "" {
				ok, err := s.leaser.AcquireNamedLease(ctx, "telemetry.sampler", s.owner, s.interval*3)
				if err != nil {
					logx.CtxError(ctx, "telemetry lease failed", logx.ErrKey, err)
					continue
				}
				if !ok {
					continue // another replica samples; gauges are global
				}
			}
			_ = s.sample(ctx)
		}
	}
}

// SampleOnce performs one sampling pass (tests/ops). Returns error only when
// a count query fails; partial failures are logged and the rest still update.
func (s *Service) SampleOnce(ctx context.Context) error {
	return s.sample(ctx)
}

func (s *Service) sample(ctx context.Context) error {
	var firstErr error
	// Reset first: deleted tenants must not leave stale series.
	activeAlerts.Reset()
	if counts, err := s.store.CountActiveAlertsByTenant(ctx); err != nil {
		firstErr = err
		logx.CtxError(ctx, "telemetry: active alerts count failed", logx.ErrKey, err)
	} else {
		for _, c := range counts {
			activeAlerts.WithLabelValues(c.TenantID).Set(float64(c.Count))
		}
	}
	openIncidents.Reset()
	if counts, err := s.store.CountOpenIncidentsByTenant(ctx); err != nil {
		firstErr = err
		logx.CtxError(ctx, "telemetry: open incidents count failed", logx.ErrKey, err)
	} else {
		for _, c := range counts {
			openIncidents.WithLabelValues(c.TenantID).Set(float64(c.Count))
		}
	}
	sources, err := s.store.ListSourceHeartbeats(ctx)
	if err != nil {
		firstErr = err
		logx.CtxError(ctx, "telemetry: source heartbeats failed", logx.ErrKey, err)
		return firstErr
	}
	// Reset first: sources deleted/disabled must not leave stale series.
	sourceHeartbeatAge.Reset()
	now := s.now()
	for _, src := range sources {
		if !src.Enabled || src.LastSeenAt.IsZero() {
			continue
		}
		age := now.Sub(src.LastSeenAt).Seconds()
		if age < 0 {
			age = 0
		}
		label := src.Name
		if label == "" {
			label = src.SourceID
		}
		tenant := src.TenantID
		if tenant == "" {
			tenant = "unknown"
		}
		sourceHeartbeatAge.WithLabelValues(tenant, label).Set(age)
	}
	return firstErr
}
