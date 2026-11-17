// Package kafka provides the Kafka infrastructure adapter (BASE-006): the
// durable producer behind the ingest API's 202 contract. Consumer wiring
// lands with BASE-007/PROC tasks.
package kafka

import (
	"github.com/example/alert-platform/internal/application/ingest"

	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Message headers mandated by docs/07 §2 (schema envelope on every event).
const (
	HeaderSchemaName    = "schema_name"
	HeaderSchemaVersion = "schema_version"
	HeaderTransportID   = "transport_id"
	HeaderTraceID       = "trace_id"
)

// Producer writes events durably: synchronous WriteMessages (Async=false)
// with RequiredAcks=WaitAll — Publish returns only after all in-sync replicas
// acknowledged, which is the persistence guarantee the API's 202 response
// leans on (docs/04 §2). Failures return an error to the caller: a down
// broker surfaces as a retriable error, never as a silent drop (docs/13
// BASE-006).
type Producer struct {
	writer *kafka.Writer
}

// NewProducer builds a producer for one topic with acks=all, synchronous
// writes, and a hash balancer so same-key messages land on one partition.
func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, // acks=all: durability before response
		Async:        false,            // synchronous: Publish blocks until ack
		BatchTimeout: 10 * time.Millisecond,
	}
	// kafka-go's Writer does NOT implement the idempotent producer (there is
	// no enable.idempotence equivalent): a retry after a broker-side timeout
	// can duplicate a message. Callers must treat publish as AT-LEAST-ONCE and
	// keep consumers idempotent (transition id / outbox_id headers). Hash
	// balancing only preserves order per key within this writer's stream —
	// the relay enforces a single writer + per-key ordering (AUD-011).
	return &Producer{writer: w}
}

// Publish writes one message with the standard envelope headers. It blocks
// until the broker cluster acknowledges or the context expires.
func (p *Producer) Publish(ctx context.Context, key string, headers []kafka.Header, value []byte) error {
	msg := kafka.Message{
		Key:     []byte(key),
		Headers: headers,
		Value:   value,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka publish: %w", err)
	}
	return nil
}

// Close flushes pending writes and releases connections.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// RequiredAcks reports the configured ack level — used by tests to assert
// the acks=all contract (docs/13 BASE-006 "configuration assertion").
func (p *Producer) RequiredAcks() kafka.RequiredAcks {
	return p.writer.RequiredAcks
}

// ErrNoBrokers is returned by validation helpers when the broker list is
// empty — a configuration error that must fail fast at construction time.
var ErrNoBrokers = errors.New("kafka: broker list is empty")

// PublishEvent realizes the application/ingest.Publisher port: header pairs
// in, durable write, error on non-acknowledgement.
func (p *Producer) PublishEvent(ctx context.Context, key string, headers [][2]string, value []byte) error {
	hs := make([]kafka.Header, 0, len(headers))
	for _, h := range headers {
		hs = append(hs, kafka.Header{Key: h[0], Value: []byte(h[1])})
	}
	return p.Publish(ctx, key, hs, value)
}

// PublishEvents writes a whole batch in ONE Writer call: kafka-go pipelines
// the messages into a single acks=all round trip, collapsing per-event
// latency (PERF-002: batch of 50 went from ~50 sequential sync publishes to
// one batched produce).
//
// P1-01 semantics: NOT an atomic whole-batch write. Messages routed to
// different partitions ack independently — when a produce request for one
// partition fails, messages on other partitions may already be durable, and
// the returned error covers the failed subset only. Callers must treat this
// as at-least-once: retry the whole batch (same transport ids) and rely on
// consumer-side receipts for dedup (verified by
// TestBatchPartialSuccessRetryIdempotent).
func (p *Producer) PublishEvents(ctx context.Context, msgs []ingest.BatchMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	kmsgs := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		hdrs := make([]kafka.Header, 0, len(m.Headers))
		for _, h := range m.Headers {
			hdrs = append(hdrs, kafka.Header{Key: h[0], Value: []byte(h[1])})
		}
		kmsgs = append(kmsgs, kafka.Message{Key: []byte(m.Key), Headers: hdrs, Value: m.Value})
	}
	return p.writer.WriteMessages(ctx, kmsgs...)
}
