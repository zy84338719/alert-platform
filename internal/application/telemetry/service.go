// Package telemetry samples business-level gauges that cannot be derived from
// event counters (P9-OBS-06): active alert instances, open incidents and the
// per-source heartbeat age. Sampling runs as a non-critical worker task.
package telemetry

import (
	"context"
	"time"

	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oekit/logx"
)

// SourceHeartbeat is one registered source's liveness input.
type SourceHeartbeat struct {
	SourceID   string
	Name       string
	LastSeenAt time.Time // zero = never seen
	Enabled    bool
}

// Store is the read port (infrastructure/postgres).
type Store interface {
	CountActiveAlerts(ctx context.Context) (int64, error)
	CountOpenIncidents(ctx context.Context) (int64, error)
	ListSourceHeartbeats(ctx context.Context) ([]SourceHeartbeat, error)
}

var (
	activeAlerts = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "alert", Name: "active_instances",
		Help: "Alert instances in pending/firing/stale state.",
	})
	openIncidents = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "incident", Name: "open",
		Help: "Incidents not yet closed.",
	})
	sourceHeartbeatAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "source", Name: "heartbeat_age_seconds",
		Help: "Seconds since the source last pushed (enabled sources only).",
	}, []string{"source"})
)

func init() {
	for _, c := range []prometheus.Collector{activeAlerts, openIncidents, sourceHeartbeatAge} {
		if err := oekitmetrics.Registry.Register(c); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
				panic(err)
			}
		}
	}
}

// Service periodically refreshes the business gauges.
type Service struct {
	store    Store
	interval time.Duration
	now      func() time.Time
}

// NewService builds the sampler (default tick 30s).
func NewService(store Store) *Service {
	return &Service{store: store, interval: 30 * time.Second, now: time.Now}
}

// SetTuning overrides the sample interval (tests/wiring).
func (s *Service) SetTuning(interval time.Duration) {
	if interval > 0 {
		s.interval = interval
	}
}

// Run samples until ctx is done. Failures are logged and retried on the next
// tick — a metrics outage must never affect alert processing.
func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	logx.CtxInfo(ctx, "telemetry sampler started", "interval", s.interval.String())
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
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
	if n, err := s.store.CountActiveAlerts(ctx); err != nil {
		firstErr = err
		logx.CtxError(ctx, "telemetry: active alerts count failed", logx.ErrKey, err)
	} else {
		activeAlerts.Set(float64(n))
	}
	if n, err := s.store.CountOpenIncidents(ctx); err != nil {
		firstErr = err
		logx.CtxError(ctx, "telemetry: open incidents count failed", logx.ErrKey, err)
	} else {
		openIncidents.Set(float64(n))
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
		sourceHeartbeatAge.WithLabelValues(label).Set(age)
	}
	return firstErr
}
