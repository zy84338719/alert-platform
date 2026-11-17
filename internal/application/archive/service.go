// Package archive implements the PG -> ClickHouse transition archiver
// (P10-001/002, AUD-001/002/010).
//
// Model: EVERY transition enqueues one row in transition_archive_queue inside
// the same transaction that inserts the transition. The archiver therefore
// never scans by event time, so late / out-of-order / Edge-replayed events
// (transition_at older than anything already archived) are archived when they
// land — the old high-water cursor silently dropped them forever.
//
// Concurrency: a DB lease (component_leases) makes the archiver a SINGLE
// writer across worker replicas; ClickHouse writes are idempotent
// (ReplacingMergeTree), so a crash between CH ack and the queue mark only
// causes a harmless duplicate insert, observable via the dedup engine and
// metrics — never a lost row.
package archive

import (
	"context"
	"sync"
	"time"

	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oekit/logx"
)

// Transition is one queued row to archive.
type Transition struct {
	TenantID     string
	TransitionID string
	InstanceID   string
	FromState    string
	ToState      string
	EventID      string
	At           time.Time
}

// Store is the PG queue/lease port (infrastructure/postgres).
type Store interface {
	// AcquireLease renews (or takes over) the single-writer lease. False means
	// another live owner holds it — this replica must skip the tick.
	AcquireLease(ctx context.Context, owner string, ttl time.Duration) (bool, error)
	ReleaseLease(ctx context.Context, owner string) error
	// ClaimTransitions claims pending or lease-expired rows, increments
	// attempts and leases them to this writer.
	ClaimTransitions(ctx context.Context, limit int, lease time.Duration, maxAttempts int) ([]Transition, error)
	// MarkArchived marks rows done (only after ClickHouse acknowledged).
	MarkArchived(ctx context.Context, ids []string) error
	// MarkFailed dead-letters rows (permanent rejection).
	MarkFailed(ctx context.Context, ids []string, reason string) error
	// MarkRetry returns rows to pending after a transient failure.
	MarkRetry(ctx context.Context, ids []string, reason string) error
	// MarkExhausted dead-letters pending rows that already hit maxAttempts.
	MarkExhausted(ctx context.Context, maxAttempts int) (int64, error)
	// QueueStats reports the pending depth and the oldest queued transition.
	QueueStats(ctx context.Context) (pending int64, oldest time.Time, err error)
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
		Help: "Transient archiver failures (retried).",
	})
	failedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "archive", Name: "failed_total",
		Help: "Transitions dead-lettered (permanent rejection or attempts exhausted).",
	}, []string{"reason"})
	lagSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "archive", Name: "lag_seconds",
		Help: "Age of the oldest queued transition (archiver backlog).",
	})
	queuePending = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "archive", Name: "queue_pending",
		Help: "Transitions waiting to be archived (pending + leased).",
	})
)

// registerOnce keeps idle archive metrics out of binaries that only import the
// types (alert-api imports postgres -> this package).
var registerOnce sync.Once

func registerMetrics() {
	registerOnce.Do(func() {
		for _, c := range []prometheus.Collector{writtenTotal, errorTotal, failedTotal, lagSeconds, queuePending} {
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
	store       Store
	writer      Writer
	owner       string
	interval    time.Duration
	batch       int
	maxBatches  int
	maxAttempts int
	lease       time.Duration
	now         func() time.Time
}

// NewService builds the archiver; defaults: tick 10s, batch 500, 10 batches,
// max attempts 10, lease 45s.
func NewService(store Store, writer Writer, owner string) *Service {
	registerMetrics()
	return &Service{
		store: store, writer: writer, owner: owner,
		interval: 10 * time.Second, batch: 500, maxBatches: 10,
		maxAttempts: 10, lease: 45 * time.Second, now: time.Now,
	}
}

// SetTuning overrides loop parameters (worker config wiring / tests).
func (s *Service) SetTuning(interval time.Duration, batch, maxBatches, maxAttempts int) {
	if interval > 0 {
		s.interval = interval
	}
	if batch > 0 {
		s.batch = batch
	}
	if maxBatches > 0 {
		s.maxBatches = maxBatches
	}
	if maxAttempts > 0 {
		s.maxAttempts = maxAttempts
	}
}

// Run archives until ctx is done; failures are logged and retried next tick
// (auxiliary task — never takes the worker down).
func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	defer func() { _ = s.store.ReleaseLease(context.Background(), s.owner) }()
	logx.CtxInfo(ctx, "transition archiver started", "interval", s.interval.String())
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			ok, err := s.store.AcquireLease(ctx, s.owner, s.lease)
			if err != nil {
				errorTotal.Inc()
				logx.CtxError(ctx, "archive lease failed", logx.ErrKey, err)
				continue
			}
			if !ok {
				continue // another live replica is the single writer
			}
			if _, err := s.RunOnce(ctx); err != nil {
				errorTotal.Inc()
				logx.CtxError(ctx, "archive batch failed; rows will retry", logx.ErrKey, err)
			}
			s.reportQueue(ctx)
		}
	}
}

// RunOnce archives up to maxBatches batches; returns rows archived.
func (s *Service) RunOnce(ctx context.Context) (int, error) {
	if n, err := s.store.MarkExhausted(ctx, s.maxAttempts); err != nil {
		return 0, err
	} else if n > 0 {
		failedTotal.WithLabelValues("max_attempts").Add(float64(n))
		logx.CtxWarn(ctx, "archive rows exhausted max attempts; dead-lettered", "rows", n)
	}
	total := 0
	for i := 0; i < s.maxBatches; i++ {
		rows, err := s.store.ClaimTransitions(ctx, s.batch, s.lease, s.maxAttempts)
		if err != nil {
			return total, err
		}
		if len(rows) == 0 {
			return total, nil
		}
		ids := make([]string, 0, len(rows))
		for _, r := range rows {
			ids = append(ids, r.TransitionID)
		}
		if err := s.writer.InsertTransitions(ctx, rows); err != nil {
			if isPermanent(err) {
				// Data/format rejection: retrying can never succeed. Rows stay
				// visible in the queue as failed (auditable, manually replayable).
				if ferr := s.store.MarkFailed(ctx, ids, err.Error()); ferr != nil {
					return total, ferr
				}
				failedTotal.WithLabelValues("permanent").Add(float64(len(ids)))
				logx.CtxError(ctx, "archive batch rejected permanently; rows dead-lettered",
					"rows", len(ids), logx.ErrKey, err)
				return total, nil
			}
			// Transient (5xx/timeout/network): back to pending; the next tick retries.
			if rerr := s.store.MarkRetry(ctx, ids, err.Error()); rerr != nil {
				return total, rerr
			}
			return total, err
		}
		if err := s.store.MarkArchived(ctx, ids); err != nil {
			return total, err
		}
		writtenTotal.Add(float64(len(rows)))
		total += len(rows)
		if len(rows) < s.batch {
			return total, nil
		}
	}
	return total, nil
}

// isPermanent matches any error exposing IsPermanent() bool (the ClickHouse
// client's PermanentError) without importing the infrastructure type here.
func isPermanent(err error) bool {
	for e := err; e != nil; {
		if p, ok := e.(interface{ IsPermanent() bool }); ok {
			return p.IsPermanent()
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		e = u.Unwrap()
	}
	return false
}

func (s *Service) reportQueue(ctx context.Context) {
	pending, oldest, err := s.store.QueueStats(ctx)
	if err != nil {
		logx.CtxError(ctx, "archive queue stats failed", logx.ErrKey, err)
		return
	}
	queuePending.Set(float64(pending))
	if !oldest.IsZero() {
		if age := s.now().Sub(oldest).Seconds(); age >= 0 {
			lagSeconds.Set(age)
		}
	}
}