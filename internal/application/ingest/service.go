// Package ingest implements the ingest use case: accepting a batch of
// canonical events from trusted adapters/edges and handing each one to the
// durable publisher (Kafka). Batch semantics are v1 whole-batch: either the
// whole batch is durably enqueued or the caller retries with the same
// batch_id (transport identity is derived deterministically so retries reuse
// it — docs/03 §2, docs/04 §2).
package ingest

import (
	"context"
	"fmt"
	"time"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/oekit/tracing/traceid"

	domainalert "github.com/example/alert-platform/internal/domain/alert"
)

// Publisher is the outbound port implemented by the Kafka infrastructure.
// Failure semantics (P1-01): an error means "not every message of the batch
// was acknowledged" — messages MAY have partially succeeded (per-partition
// acks), which is safe: the caller retries the whole batch with the same
// transport ids and the consumer receipts absorb the duplicates
// (at-least-once + consumer idempotency). Silent drops are forbidden.
type Publisher interface {
	PublishEvent(ctx context.Context, key string, headers [][2]string, value []byte) error
	// PublishEvents enqueues a whole batch in one durable round trip.
	PublishEvents(ctx context.Context, msgs []BatchMessage) error
}

// BatchMessage is one marshaled event with its transport headers.
type BatchMessage struct {
	Key     string
	Headers [][2]string
	Value   []byte
}

// Header names must match the transport envelope (docs/07 §2).
const (
	hdrSchemaName    = "schema_name"
	hdrSchemaVersion = "schema_version"
	hdrTransportID   = "transport_id"
	hdrTraceID       = "trace_id"
)

// Schema identity of the canonical event payload carried on the wire.
const (
	eventSchemaName    = "alert.event.v1.AlertEvent"
	eventSchemaVersion = "1"
)

// Service wires the PushBatch use case.
type Service struct {
	pub Publisher
}

// NewService builds the ingest service over a publisher port.
func NewService(pub Publisher) *Service {
	return &Service{pub: pub}
}

// PushBatch validates and publishes every event of the batch. It returns the
// durable batch id (v1: echo of the request id) only after ALL events were
// acknowledged by the broker.
//
// Batch semantics (P1-01): the durable write is AT-LEAST-ONCE, not atomic —
// kafka-go's sync batch write may partially succeed (messages that reached
// different partitions ack independently), and the reported error can cover
// only the failed subset. A failed PushBatch is therefore retried WHOLE by
// the caller (same batch_id → same transport ids), and the consumer-side
// receipts layer absorbs the redeliveries — no loss, no duplicate effects.
func (s *Service) PushBatch(ctx context.Context, batchID string, events []*eventv1.AlertEvent) (string, error) {
	start := time.Now()
	defer func() { ObserveBatch(time.Since(start)) }()
	if batchID == "" {
		return "", fmt.Errorf("batch_id is required")
	}
	if len(events) == 0 {
		return "", fmt.Errorf("batch must contain at least one event")
	}
	traceID := traceid.FromContext(ctx)
	msgs := make([]BatchMessage, 0, len(events))
	for i, ev := range events {
		if ev.GetTenantId() == "" || ev.GetSource() == nil || ev.GetSource().GetSourceId() == "" {
			return "", fmt.Errorf("event %d: tenant_id and source.source_id are required", i)
		}
		// Deterministic transport identity: a retry of the same batch_id must
		// enqueue with the same transport_id so the consumer's receipts layer
		// deduplicates (docs/03 §2 transport_id contract).
		if ev.GetTransportId() == "" {
			ev.TransportId = fmt.Sprintf("%s:%d", batchID, i)
		}
		if ev.GetEventId() == "" && ev.GetEventKey() != "" {
			ev.EventId = ev.GetEventKey() // stable logical id from source identity
		}
		value, err := protoMarshal(ev)
		if err != nil {
			return "", fmt.Errorf("event %d: marshal: %w", i, err)
		}
		headers := [][2]string{
			{hdrSchemaName, eventSchemaName},
			{hdrSchemaVersion, eventSchemaVersion},
			{hdrTransportID, ev.GetTransportId()},
		}
		if traceID != "" {
			headers = append(headers, [][2]string{{hdrTraceID, traceID}}...)
		}
		key := partitionKey(ev)
		msgs = append(msgs, BatchMessage{Key: key, Headers: headers, Value: value})
	}
	// ONE durable round trip for the whole batch (PERF-002): the caller sees
	// 202 only after every event of the batch is broker-acked, same contract
	// as before — just without N sequential sync publishes.
	if err := s.pub.PublishEvents(ctx, msgs); err != nil {
		return "", err
	}
	RecordAccepted(len(events))
	return batchID, nil
}

// partitionKey (P1-01) computes the producer key: tenant + server-side logical
// alert identity — the same logical alert keeps its ordering within one
// partition while different alerts of the same source spread across
// partitions (a per-source key pinned large sources to a single hotspot
// partition, defeating Consumer Group scaling). The identity fields match
// the processor's fingerprintOf exactly; identity-incomplete events fall
// back to the transport-scoped tenant|source key.
func partitionKey(ev *eventv1.AlertEvent) string {
	fp, err := domainalert.Fingerprint(domainalert.Identity{
		TenantID:   ev.GetTenantId(),
		SourceID:   ev.GetSource().GetSourceId(),
		RuleID:     ev.GetSource().GetExternalRuleId(),
		ClusterID:  ev.GetResource().GetClusterId(),
		EntityType: ev.GetResource().GetResourceType(),
		EntityUID:  ev.GetResource().GetResourceUid(),
		Dimensions: ev.GetLabels(),
	})
	if err == nil {
		return ev.GetTenantId() + "|" + fp
	}
	return ev.GetTenantId() + "|" + ev.GetSource().GetSourceId()
}
