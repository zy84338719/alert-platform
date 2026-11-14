// Package relay implements the domain-outbox relay (PROC-003): publish
// committed domain events to Kafka after — never before — the PG transaction
// that produced them. Delivery is at-least-once; consumers deduplicate by
// transition id carried in the payload.
package relay

import (
	"context"
	"time"

	"github.com/oekit/logx"
)

// OutboxRow is one claimed domain event (application-level view).
type OutboxRow struct {
	ID          string
	TenantID    string
	AggregateID string
	EventType   string
	Payload     []byte
}

// Store is the outbox persistence port (infrastructure/postgres implements
// it; claim semantics carry the visibility-timeout requeue).
type Store interface {
	ClaimOutboxRows(ctx context.Context, limit int, visibilityTimeout time.Duration) ([]OutboxRow, error)
	MarkOutboxPublished(ctx context.Context, id string) error
}

// Publisher is the same durable-producer port the ingest side uses.
type Publisher interface {
	PublishEvent(ctx context.Context, key string, headers [][2]string, value []byte) error
}

// Service runs the relay loop.
type Service struct {
	store      Store
	pub        Publisher
	interval   time.Duration
	visibility time.Duration
	batch      int
}

// NewService builds the relay. Defaults: interval 500ms, batch 100,
// visibility timeout 60s (a crashed relay's claims lapse back to pending).
func NewService(store Store, pub Publisher) *Service {
	return &Service{store: store, pub: pub, interval: 500 * time.Millisecond, visibility: time.Minute, batch: 100}
}

// Run relays until ctx is done. Errors are logged and retried on the next
// tick — a Kafka outage must never lose committed outbox rows.
func (s *Service) Run(ctx context.Context) error {
	logx.CtxInfo(ctx, "domain outbox relay started", "interval", s.interval.String())
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			rows, err := s.store.ClaimOutboxRows(ctx, s.batch, s.visibility)
			if err != nil {
				if ctx.Err() == nil {
					logx.CtxError(ctx, "relay claim failed", logx.ErrKey, err)
				}
				continue
			}
			for _, row := range rows {
				key := row.TenantID + "|" + row.AggregateID // docs/04 §1 keying
				headers := [][2]string{
					{"schema_name", row.EventType},
					{"schema_version", "1"},
					{"outbox_id", row.ID},
				}
				if err := s.pub.PublishEvent(ctx, key, headers, row.Payload); err != nil {
					// Row stays 'publishing'; visibility timeout requeues it.
					if ctx.Err() == nil {
						logx.CtxError(ctx, "relay publish failed; will requeue", logx.ErrKey, err, "outbox_id", row.ID)
					}
					continue
				}
				if err := s.store.MarkOutboxPublished(ctx, row.ID); err != nil && ctx.Err() == nil {
					logx.CtxError(ctx, "relay mark published failed (duplicate publish on requeue is safe)",
						logx.ErrKey, err, "outbox_id", row.ID)
				}
			}
		}
	}
}
