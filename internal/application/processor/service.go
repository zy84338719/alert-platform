// Package processor implements the consume-side use case (BASE-007): turn one
// canonical event from the transport into a transactional PG write (receipt +
// alert instance + transition). Offset semantics live with the transport
// adapter: the store transaction commits first, the offset commits after
// (docs/04 §2).
package processor

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	domainalert "github.com/example/alert-platform/internal/domain/alert"
	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/oekit/logx"
	"github.com/oekit/tracing/traceid"
)

// Store is the transactional write port realized by infrastructure/postgres.
// duplicate=true means the transport occurrence was already applied
// (same transport_id) — callers treat it as success.
type Store interface {
	ApplyEvent(ctx context.Context, ev *eventv1.AlertEvent) (duplicate bool, err error)
}

// IncomingMessage is the transport-neutral unit delivered to Handle.
type IncomingMessage struct {
	Key     string
	Headers map[string]string
	Value   []byte
}

// Service processes canonical events into the store.
type Service struct {
	store Store
}

// NewService builds the processor over a store port.
func NewService(store Store) *Service {
	return &Service{store: store}
}

// Handle decodes one canonical event and applies it transactionally.
// The message header trace_id is promoted into the context so every log line
// in the processing path correlates with the producing side (BASE-007).
// Decode failures and identity contract violations are Permanent — the
// transport quarantines them into the DLQ (PROC-001).
func (s *Service) Handle(ctx context.Context, msg IncomingMessage) error {
	if id := msg.Headers["trace_id"]; id != "" {
		ctx = traceid.ContextWith(ctx, id)
	}
	ev := &eventv1.AlertEvent{}
	if err := proto.Unmarshal(msg.Value, ev); err != nil {
		return Permanent(fmt.Errorf("decode canonical event: %w", err))
	}
	duplicate, err := s.store.ApplyEvent(ctx, ev)
	if err != nil {
		if errors.Is(err, domainalert.ErrIncompleteIdentity) {
			return Permanent(fmt.Errorf("apply event %s: %w", ev.GetTransportId(), err))
		}
		return fmt.Errorf("apply event %s: %w", ev.GetTransportId(), err)
	}
	reason := "applied"
	if duplicate {
		reason = "duplicate-observation"
	}
	logx.CtxInfo(ctx, "event processed",
		"component", "alert-worker",
		"transport_id", ev.GetTransportId(),
		"event_id", ev.GetEventId(),
		"outcome", reason)
	return nil
}
