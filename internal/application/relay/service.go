// Package relay implements the domain-outbox relay (PROC-003): publish
// committed domain events to Kafka after — never before — the PG transaction
// that produced them.
//
// Semantics (AUD-011):
//   - Delivery is AT-LEAST-ONCE. kafka-go's Writer is NOT an idempotent
//     producer; a retry after a broker-side timeout may duplicate a message.
//     Consumers deduplicate by transition id / outbox_id in the headers.
//   - Ordering is per publish key (tenant|aggregate): a single-writer DB lease
//     (component_leases) keeps one relay publishing at a time, claims are
//     ordered by created_at, and a claim never includes a key whose older row
//     is still pending/publishing. A failed publish therefore cannot be
//     overtaken by a newer event of the same aggregate.
//   - Terminal state: rows reaching OutboxMaxAttempts are dead-lettered
//     (status='failed') instead of retrying forever; they stay queryable for
//     manual replay.
package relay

import (
	"context"
	"sync"
	"time"

	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oekit/logx"
)

// OutboxRow is one claimed domain event (application-level view).
type OutboxRow struct {
	ID          string
	TenantID    string
	AggregateID string
	EventType   string
	Payload     []byte
	Attempts    int
}

// Store is the outbox persistence port (infrastructure/postgres implements
// it; claim semantics carry the visibility-timeout requeue).
type Store interface {
	// AcquireOutboxLease renews (or takes over) the relay's single-writer
	// lease. False means another live replica holds it — skip the tick.
	AcquireOutboxLease(ctx context.Context, owner string, ttl time.Duration) (bool, error)
	ReleaseOutboxLease(ctx context.Context, owner string) error
	// SweepOutbox requeues claims abandoned by a crashed/timed-out relay and
	// dead-letters claims that exhausted their attempts.
	SweepOutbox(ctx context.Context, visibilityTimeout time.Duration, maxAttempts int) (requeued, deadLettered int64, err error)
	// ClaimOutboxRows claims pending rows in created_at order, bumping
	// attempts; rows whose older same-key row is still in flight are skipped.
	ClaimOutboxRows(ctx context.Context, limit, maxAttempts int) ([]OutboxRow, error)
	// MarkOutboxPublished records a broker-acked publish.
	MarkOutboxPublished(ctx context.Context, id string) error
	// RequeueOutboxRows returns claimed-but-unpublished rows to pending after
	// a failed publish (delay backs off the row that failed).
	RequeueOutboxRows(ctx context.Context, ids []string, delay time.Duration) error
	// OutboxStats reports pending/publishing/failed depths for metrics.
	OutboxStats(ctx context.Context) (pending, inflight, failed int64, err error)
}

// Publisher is the same durable-producer port the ingest side uses.
type Publisher interface {
	PublishEvent(ctx context.Context, key string, headers [][2]string, value []byte) error
}

var (
	publishedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "outbox", Name: "published_total",
		Help: "Domain outbox rows acknowledged by Kafka.",
	})
	publishErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "outbox", Name: "publish_errors_total",
		Help: "Transient outbox publish failures (rows retried).",
	})
	deadLetterTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "outbox", Name: "dead_letter_total",
		Help: "Outbox rows dead-lettered after exhausting max attempts.",
	})
	pendingGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "outbox", Name: "pending",
		Help: "Outbox rows waiting to be published.",
	})
	failedGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "outbox", Name: "failed",
		Help: "Outbox rows in the terminal failed state (manual replay).",
	})
)

// registerOnce keeps idle outbox metrics out of binaries that only import
// the types (alert-api imports postgres -> this package).
var registerOnce sync.Once

func registerMetrics() {
	registerOnce.Do(func() {
		for _, c := range []prometheus.Collector{publishedTotal, publishErrorsTotal, deadLetterTotal, pendingGauge, failedGauge} {
			if err := oekitmetrics.Registry.Register(c); err != nil {
				if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
					panic(err)
				}
			}
		}
	})
}

// Service runs the relay loop.
type Service struct {
	store       Store
	pub         Publisher
	owner       string
	interval    time.Duration
	visibility  time.Duration
	batch       int
	maxAttempts int
	lease       time.Duration
}

// NewService builds the relay. Defaults: interval 500ms, batch 100,
// visibility timeout 60s (a crashed relay's claims lapse back to pending),
// max attempts 20, lease 45s.
func NewService(store Store, pub Publisher, owner string) *Service {
	registerMetrics()
	return &Service{
		store: store, pub: pub, owner: owner,
		interval: 500 * time.Millisecond, visibility: time.Minute,
		batch: 100, maxAttempts: 20, lease: 45 * time.Second,
	}
}

// SetTuning overrides loop parameters (worker config wiring / tests).
func (s *Service) SetTuning(interval, visibility time.Duration, batch, maxAttempts int) {
	if interval > 0 {
		s.interval = interval
	}
	if visibility > 0 {
		s.visibility = visibility
	}
	if batch > 0 {
		s.batch = batch
	}
	if maxAttempts > 0 {
		s.maxAttempts = maxAttempts
	}
}

// backoff bounds the retry delay for a failed row: 1s doubling by attempt,
// capped at 30s. Later rows of the same key stay blocked meanwhile, so this is
// the aggregate's publish latency floor during an outage.
func backoff(attempts int) time.Duration {
	d := time.Second
	for i := 1; i < attempts && d < 30*time.Second; i++ {
		d *= 2
	}
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// Run relays until ctx is done. Errors are logged and retried on the next
// tick — a Kafka outage must never lose committed outbox rows.
func (s *Service) Run(ctx context.Context) error {
	logx.CtxInfo(ctx, "domain outbox relay started", "interval", s.interval.String())
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	defer func() { _ = s.store.ReleaseOutboxLease(context.Background(), s.owner) }()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			ok, err := s.store.AcquireOutboxLease(ctx, s.owner, s.lease)
			if err != nil {
				publishErrorsTotal.Inc()
				logx.CtxError(ctx, "outbox relay lease failed", logx.ErrKey, err)
				continue
			}
			if !ok {
				continue // another live replica is the single writer
			}
			if _, err := s.RunOnce(ctx); err != nil && ctx.Err() == nil {
				logx.CtxError(ctx, "outbox relay batch failed; rows will retry", logx.ErrKey, err)
			}
		}
	}
}

// RunOnce sweeps stale claims, publishes one ordered batch and reports depth.
// It returns the number of rows successfully published.
func (s *Service) RunOnce(ctx context.Context) (int, error) {
	requeued, dead, err := s.store.SweepOutbox(ctx, s.visibility, s.maxAttempts)
	if err != nil {
		return 0, err
	}
	if dead > 0 {
		deadLetterTotal.Add(float64(dead))
		logx.CtxError(ctx, "outbox rows exhausted max attempts; dead-lettered", "rows", dead)
	}
	if requeued > 0 {
		logx.CtxWarn(ctx, "outbox claims from a timed-out relay requeued", "rows", requeued)
	}

	rows, err := s.store.ClaimOutboxRows(ctx, s.batch, s.maxAttempts)
	if err != nil {
		return 0, err
	}
	published := 0
	for i, row := range rows {
		key := row.TenantID + "|" + row.AggregateID // docs/04 §1 keying
		headers := [][2]string{
			{"schema_name", row.EventType},
			{"schema_version", "1"},
			{"outbox_id", row.ID},
		}
		if err := s.pub.PublishEvent(ctx, key, headers, row.Payload); err != nil {
			publishErrorsTotal.Inc()
			// Requeue this row and every claim after it: publishing a newer
			// row of the same aggregate before this one would invert the
			// lifecycle order. The failed row backs off by attempt count.
			ids := make([]string, 0, len(rows)-i)
			for _, pending := range rows[i:] {
				ids = append(ids, pending.ID)
			}
			if rerr := s.store.RequeueOutboxRows(ctx, ids, backoff(row.Attempts)); rerr != nil && ctx.Err() == nil {
				logx.CtxError(ctx, "outbox requeue after publish failure failed", logx.ErrKey, rerr)
			}
			s.reportStats(ctx)
			return published, err
		}
		if err := s.store.MarkOutboxPublished(ctx, row.ID); err != nil && ctx.Err() == nil {
			// Broker ack was received; the row stays 'publishing' and will be
			// republished after the visibility timeout. Consumers dedupe.
			logx.CtxError(ctx, "outbox mark published failed (duplicate publish on requeue is safe)",
				logx.ErrKey, err, "outbox_id", row.ID)
		}
		publishedTotal.Inc()
		published++
	}
	s.reportStats(ctx)
	return published, nil
}

func (s *Service) reportStats(ctx context.Context) {
	pending, _, failed, err := s.store.OutboxStats(ctx)
	if err != nil {
		return
	}
	pendingGauge.Set(float64(pending))
	failedGauge.Set(float64(failed))
}
