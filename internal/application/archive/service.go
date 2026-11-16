// Package archive implements the PG -> ClickHouse transition archiver
// (P9-CH-01 / INC-007). It runs OUTSIDE every write transaction: a periodic
// worker task copies committed alert_transitions rows in cursor order and
// advances the cursor only after ClickHouse acknowledged the batch. A CH
// outage therefore pauses archiving without affecting alerting.
package archive

import (
	"context"
	"sync"
	"time"

	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oekit/logx"
)

// Transition is one row to archive.
type Transition struct {
	TenantID     string
	TransitionID string
	InstanceID   string
	FromState    string
	ToState      string
	EventID      string
	At           time.Time
}

// Store is the PG cursor port (infrastructure/postgres).
type Store interface {
	// NextTransitions returns the next batch after the persisted cursor,
	// ordered by (transition_at, transition_id).
	NextTransitions(ctx context.Context, limit int) ([]Transition, error)
	// AdvanceCursor persists the last archived row (monotonic).
	AdvanceCursor(ctx context.Context, at time.Time, transitionID string) error
}

// Writer is the ClickHouse insert port (infrastructure/clickhouse).
type Writer interface {
	InsertTransitions(ctx context.Context, rows []Transition) error
}

var (
	writtenTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "archive", Name: "written_total",
		Help: "Transitions written to the ClickHouse archive.",
	})
	errorTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "archive", Name: "errors_total",
		Help: "Archiver failures (write or cursor advance).",
	})
	lagSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "archive", Name: "lag_seconds",
		Help: "Age of the most recently archived transition.",
	})
)

// registerOnce: postgres imports Transition for its store, so binaries that
// only use the types (alert-api) must not expose idle archive metrics.
var registerOnce sync.Once

func registerMetrics() {
	registerOnce.Do(func() {
		for _, c := range []prometheus.Collector{writtenTotal, errorTotal, lagSeconds} {
			if err := oekitmetrics.Registry.Register(c); err != nil {
				if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
					panic(err)
				}
			}
		}
	})
}

// Service runs the archiver loop.
type Service struct {
	store      Store
	writer     Writer
	interval   time.Duration
	batch      int
	maxBatches int // per tick, bounds one slow CH run
	now        func() time.Time
}

// NewService builds the archiver; defaults: tick 10s, batch 500, 10 batches.
func NewService(store Store, writer Writer) *Service {
	registerMetrics()
	return &Service{store: store, writer: writer, interval: 10 * time.Second,
		batch: 500, maxBatches: 10, now: time.Now}
}

// SetTuning overrides loop parameters (wiring/tests).
func (s *Service) SetTuning(interval time.Duration, batch, maxBatches int) {
	if interval > 0 {
		s.interval = interval
	}
	if batch > 0 {
		s.batch = batch
	}
	if maxBatches > 0 {
		s.maxBatches = maxBatches
	}
}

// Run archives until ctx is done; failures are logged and retried next tick
// (this task is auxiliary — it never takes the worker down).
func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	logx.CtxInfo(ctx, "transition archiver started", "interval", s.interval.String())
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := s.RunOnce(ctx); err != nil && ctx.Err() == nil {
				errorTotal.Inc()
				logx.CtxError(ctx, "archive batch failed; cursor not advanced", logx.ErrKey, err)
			}
		}
	}
}

// RunOnce archives up to maxBatches batches; returns rows written.
func (s *Service) RunOnce(ctx context.Context) (int, error) {
	total := 0
	for i := 0; i < s.maxBatches; i++ {
		rows, err := s.store.NextTransitions(ctx, s.batch)
		if err != nil {
			return total, err
		}
		if len(rows) == 0 {
			return total, nil
		}
		if err := s.writer.InsertTransitions(ctx, rows); err != nil {
			return total, err
		}
		last := rows[len(rows)-1]
		if err := s.store.AdvanceCursor(ctx, last.At, last.TransitionID); err != nil {
			return total, err
		}
		writtenTotal.Add(float64(len(rows)))
		total += len(rows)
		if age := s.now().Sub(last.At).Seconds(); age >= 0 {
			lagSeconds.Set(age)
		}
		if len(rows) < s.batch {
			return total, nil
		}
	}
	return total, nil
}
