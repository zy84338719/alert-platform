// Package collector implements the HUATUO SSE collector use case (HT-001):
// stream kernel events, convert each to a canonical OCCURRENCE event buffered
// through the edge WAL (same durability as vendor webhooks), promote bursts
// via the occurrence aggregator (HT-003), and REPORT gaps — never promise
// replay (HT-004).
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oekit/logx"

	"github.com/example/alert-platform/internal/application/edge"
	"github.com/example/alert-platform/internal/application/occurrence"
)

// Stream is the SSE port (infrastructure/sseclient).
type Stream interface {
	Run(ctx context.Context, handle func(event Event) error) error
	LastID() string
}

// Event mirrors sseclient.Event at the application boundary.
type Event struct {
	ID                       string
	Name                     string
	Data                     string
	Gap                      bool
	GapFrom, GapTo           string
}

// Config wires the collector.
type Config struct {
	TenantID   string
	SourceID   string
	ClusterID  string
	PromoteThreshold int    // occurrences within Window -> one firing alert
	PromoteWindow    time.Duration
	AlertNamePrefix  string
}

// Service runs one collector.
type Service struct {
	stream Stream
	wal    Buffer
	cfg    Config
	agg    *occurrence.Aggregator
}

// Buffer is the WAL port (subset used here).
type Buffer interface {
	Append(rec edge.Record) (edge.Record, error)
}

// New builds the collector service.
func New(stream Stream, buf Buffer, cfg Config) *Service {
	if cfg.PromoteThreshold <= 0 {
		cfg.PromoteThreshold = 3
	}
	if cfg.PromoteWindow <= 0 {
		cfg.PromoteWindow = 5 * time.Minute
	}
	if cfg.AlertNamePrefix == "" {
		cfg.AlertNamePrefix = "HUATUO"
	}
	return &Service{stream: stream, wal: buf, cfg: cfg, agg: occurrence.New(cfg.PromoteThreshold, cfg.PromoteWindow)}
}

// Run streams until ctx is done.
func (s *Service) Run(ctx context.Context) error {
	logx.CtxInfo(ctx, "huatuo collector started",
		"source", s.cfg.SourceID, "threshold", s.cfg.PromoteThreshold, "window", s.cfg.PromoteWindow.String())
	return s.stream.Run(ctx, s.handle)
}

// Gaps records how many gap markers fired (self-monitoring, HT-004).
var gapCount int64

// GapCount reports detected gaps (tests/metrics).
func GapCount() int64 { return gapCount }

func (s *Service) handle(ev Event) error {
	if ev.Gap {
		gapCount++
		logx.CtxWarn(nil, "SSE GAP detected: events lost while disconnected (SSE is not replayable)",
			"from", ev.GapFrom, "to", ev.GapTo, "source", s.cfg.SourceID)
		// The gap itself becomes a first-class occurrence so the platform
		// shows the blind window instead of silently missing data.
		return s.appendOccurrence("sse-gap", fmt.Sprintf("gap %s..%s", ev.GapFrom, ev.GapTo), ev.ID)
	}
	// Kernel events carry a JSON payload with at least kind+node.
	var p struct {
		Kind string `json:"kind"`
		Node string `json:"node"`
		Name string `json:"name"`
	}
	_ = json.Unmarshal([]byte(ev.Data), &p)
	kind := p.Kind
	if kind == "" {
		kind = ev.Name
	}
	key := kind + "/" + p.Node
	if s.agg.Observe(key, time.Now()) {
		// Burst threshold: promote to ONE stateful firing alert. Recovery of
		// a promoted alert is explicit (rule/human), never fabricated.
		alertName := s.cfg.AlertNamePrefix + " " + kind + " burst on " + p.Node
		if err := s.appendFiring(alertName, key, ev.Data, ev.ID); err != nil {
			return err
		}
	}
	return s.appendOccurrence(kind, ev.Data, ev.ID)
}

func (s *Service) appendOccurrence(kind, data, eventID string) error {
	body, _ := json.Marshal(map[string]any{
		"batch_id": "ht-" + eventID,
		"events": []map[string]any{{
			"event_id":     "ht-" + eventID,
			"transport_id": "ht-" + eventID,
			"tenant_id":    s.cfg.TenantID,
			"source":       map[string]any{"source_id": s.cfg.SourceID, "source_type": 3, "external_event_id": eventID, "external_rule_id": "huatuo-" + kind},
			"resource":     map[string]any{"cluster_id": s.cfg.ClusterID, "resource_type": "node"},
			"kind":         2, // OCCURRENCE — no firing/resolved lifecycle (HT-003)
			"signal":       5, // OCCURRED
			"severity":     4, // INFO by default; promotion carries the weight
			"alert_name":   s.cfg.AlertNamePrefix + " " + kind,
			"occurred_at":  nowObj(),
			"received_at":  nowObj(),
			"event_key":    "ht-" + eventID,
			"labels":       map[string]string{"kind": kind},
			"raw_payload":  data,
		}},
	})
	_, err := s.wal.Append(edge.Record{ID: "ht-" + eventID, Body: body, SourceID: s.cfg.SourceID})
	return err
}

func (s *Service) appendFiring(alertName, key, data, eventID string) error {
	body, _ := json.Marshal(map[string]any{
		"batch_id": "htp-" + eventID,
		"events": []map[string]any{{
			"event_id":     "htp-" + eventID,
			"transport_id": "htp-" + eventID,
			"tenant_id":    s.cfg.TenantID,
			"source":       map[string]any{"source_id": s.cfg.SourceID, "source_type": 3, "external_rule_id": "huatuo-burst"},
			"resource":     map[string]any{"cluster_id": s.cfg.ClusterID, "resource_type": "node"},
			"kind":         1, // STATEFUL
			"signal":       1, // FIRING
			"severity":     2, // ERROR
			"alert_name":   alertName,
			"occurred_at":  nowObj(),
			"received_at":  nowObj(),
			"starts_at":    nowObj(),
			"event_key":    "htp-" + eventID,
			"labels":       map[string]string{"kind": key},
			"annotations":  map[string]string{"source_event": eventID},
		}},
	})
	_, err := s.wal.Append(edge.Record{ID: "htp-" + eventID, Body: body, SourceID: s.cfg.SourceID})
	return err
}

func nowObj() map[string]int64 {
	now := time.Now().Unix()
	return map[string]int64{"seconds": now}
}
