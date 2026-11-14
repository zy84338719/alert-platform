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

// cloudEvent is the official HUATUO wire format (P0-04): every SSE `data:`
// line is a CloudEvents 1.0 JSON object; the kernel payload lives in `data`
// (WatchEventData), NOT in top-level kind/node fields. Contract pinned in
// contracts/external/huatuo/v1/contract.md (docs.huatuo.tech, CC BY 4.0).
type cloudEvent struct {
	Specversion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	DataContentType string          `json:"datacontenttype"`
	Time            string          `json:"time"`
	Data            watchEventData  `json:"data"`
	Raw             json.RawMessage `json:"-"`
}

type watchEventData struct {
	Hostname              string          `json:"hostname"`
	Region                string          `json:"region"`
	ObservedTimestamp     string          `json:"observed_timestamp"`
	UploadedTimestamp     string          `json:"uploaded_timestamp"`
	TracerName            string          `json:"tracer_name"`
	TracerID              json.RawMessage `json:"tracer_id"`
	TracerRunType         string          `json:"tracer_run_type"`
	TracerType            string          `json:"tracer_type"`
	ContainerID           string          `json:"container_id"`
	ContainerHostname     string          `json:"container_hostname"`
	ContainerHostNamespace string         `json:"container_host_namespace"`
	ContainerType         string          `json:"container_type"`
	ContainerQos          string          `json:"container_qos"`
	TracerData            json.RawMessage `json:"tracer_data"`
}

// entityIdentity maps the WatchEventData node/container identity to the
// canonical ResourceRef (P0-04/P0-05): a stable EntityUID — container id when
// present, hostname otherwise — is REQUIRED, or the server-side fingerprint
// rejects the event with "incomplete alert identity".
func (d *watchEventData) entityIdentity() (uid, rtype, name, namespace string) {
	if d.ContainerID != "" {
		return d.ContainerID, "container", d.ContainerHostname, d.ContainerHostNamespace
	}
	return d.Hostname, "node", d.Hostname, ""
}

func (d *watchEventData) resourceRef(clusterID string) map[string]any {
	uid, rtype, name, namespace := d.entityIdentity()
	res := map[string]any{
		"cluster_id":    clusterID,
		"resource_type": rtype,
		"resource_uid":  uid,
	}
	if name != "" {
		res["resource_name"] = name
	}
	if namespace != "" {
		res["namespace"] = namespace
	}
	return res
}

func (d *watchEventData) labels() map[string]string {
	l := map[string]string{
		"tracer_name": d.TracerName,
		"hostname":    d.Hostname,
	}
	if d.Region != "" {
		l["region"] = d.Region
	}
	if d.ContainerID != "" {
		l["container_id"] = d.ContainerID
		l["container_hostname"] = d.ContainerHostname
		l["container_host_namespace"] = d.ContainerHostNamespace
		l["container_type"] = d.ContainerType
		l["container_qos"] = d.ContainerQos
	}
	return l
}

// occurredAt prefers the user-space observation time, falling back to the
// CloudEvent time and then to now.
func occurredAt(d *watchEventData, ceTime string) map[string]int64 {
	for _, cand := range []string{d.ObservedTimestamp, ceTime, d.UploadedTimestamp} {
		if cand == "" {
			continue
		}
		if ts, err := time.Parse(time.RFC3339Nano, cand); err == nil {
			return map[string]int64{"seconds": ts.Unix(), "nanos": int64(ts.Nanosecond())}
		}
	}
	return nowObj()
}

// parseCloudEvent decodes one SSE data line as CloudEvents 1.0. A line that
// is not a CloudEvent (wrong specversion / missing id) is a contract
// violation — surfaced as an error so the caller can see it, never silently
// parsed as legacy kind/node.
func parseCloudEvent(data string) (*cloudEvent, error) {
	var ce cloudEvent
	if err := json.Unmarshal([]byte(data), &ce); err != nil {
		return nil, fmt.Errorf("huatuo sse: cloudevent decode: %w", err)
	}
	if ce.Specversion != "" && ce.Specversion != "1.0" {
		return nil, fmt.Errorf("huatuo sse: unsupported cloudevents specversion %q", ce.Specversion)
	}
	if ce.ID == "" {
		return nil, fmt.Errorf("huatuo sse: cloudevent without id")
	}
	if ce.Data.TracerName == "" {
		return nil, fmt.Errorf("huatuo sse: cloudevent %s without data.tracer_name", ce.ID)
	}
	return &ce, nil
}

func (s *Service) handle(ev Event) error {
	if ev.Gap {
		gapCount++
		logx.CtxWarn(nil, "SSE GAP detected: events lost while disconnected (SSE is not replayable)",
			"from", ev.GapFrom, "to", ev.GapTo, "source", s.cfg.SourceID)
		// The gap itself becomes a first-class occurrence so the platform
		// shows the blind window instead of silently missing data. Gaps have
		// no node/container identity — OCCURRENCE receipts carry no lifecycle,
		// so a bare resource ref is fine here.
		return s.appendGapOccurrence(ev)
	}
	ce, err := parseCloudEvent(ev.Data)
	if err != nil {
		return err
	}
	id := ce.ID
	if ev.ID != "" {
		id = ev.ID // SSE cursor; identical to the CloudEvent id in practice
	}
	kind := ce.Data.TracerName
	uid, _, _, _ := ce.Data.entityIdentity()
	key := kind + "/" + uid
	if s.agg.Observe(key, time.Now()) {
		// Burst threshold: promote to ONE stateful firing alert. Recovery of
		// a promoted alert is explicit (rule/human), never fabricated.
		alertName := s.cfg.AlertNamePrefix + " " + kind + " burst on " + uid
		if err := s.appendFiring(&ce.Data, alertName, key, id); err != nil {
			return err
		}
	}
	return s.appendOccurrence(ce, id)
}

// appendGapOccurrence records a synthesized gap marker as an OCCURRENCE.
func (s *Service) appendGapOccurrence(ev Event) error {
	eventID := "gap-" + ev.ID
	if eventID == "gap-" {
		eventID = fmt.Sprintf("gap-%d", time.Now().UnixNano())
	}
	body, _ := json.Marshal(map[string]any{
		"batch_id": "ht-" + eventID,
		"events": []map[string]any{{
			"event_id":     eventID,
			"transport_id": eventID,
			"tenant_id":    s.cfg.TenantID,
			"source":       map[string]any{"source_id": s.cfg.SourceID, "source_type": 3, "external_event_id": eventID, "external_rule_id": "huatuo-sse-gap"},
			"resource":     map[string]any{"cluster_id": s.cfg.ClusterID},
			"kind":         2,
			"signal":       5,
			"severity":     3, // WARNING — a blind window deserves visibility
			"alert_name":   s.cfg.AlertNamePrefix + " sse-gap",
			"occurred_at":  nowObj(),
			"received_at":  nowObj(),
			"event_key":    eventID,
			"labels":       map[string]string{"kind": "sse-gap", "gap_from": ev.GapFrom, "gap_to": ev.GapTo},
			"annotations":  map[string]string{"detail": fmt.Sprintf("gap %s..%s", ev.GapFrom, ev.GapTo)},
		}},
	})
	_, err := s.wal.Append(edge.Record{ID: "ht-" + eventID, Body: body, SourceID: s.cfg.SourceID})
	return err
}

// appendOccurrence records one kernel observation. The CloudEvent id is the
// observation identity (event_id/transport_id/event_key) — redeliveries of
// the same CE id dedup at the center's receipts layer.
func (s *Service) appendOccurrence(ce *cloudEvent, eventID string) error {
	kind := ce.Data.TracerName
	body, _ := json.Marshal(map[string]any{
		"batch_id": "ht-" + eventID,
		"events": []map[string]any{{
			"event_id":     eventID,
			"transport_id": eventID,
			"tenant_id":    s.cfg.TenantID,
			"source":       map[string]any{"source_id": s.cfg.SourceID, "source_type": 3, "external_event_id": eventID, "external_rule_id": "huatuo-" + kind, "external_source": ce.Source},
			"resource":     ce.Data.resourceRef(s.cfg.ClusterID),
			"kind":         2, // OCCURRENCE — no firing/resolved lifecycle (HT-003)
			"signal":       5, // OCCURRED
			"severity":     4, // INFO by default; promotion carries the weight
			"alert_name":   s.cfg.AlertNamePrefix + " " + kind,
			"occurred_at":  occurredAt(&ce.Data, ce.Time),
			"received_at":  nowObj(),
			"event_key":    eventID,
			"labels":       ce.Data.labels(),
			"annotations":  map[string]string{"cloudevent_type": ce.Type, "tracer_type": ce.Data.TracerType},
			"raw_payload":  string(ce.Data.TracerData),
		}},
	})
	_, err := s.wal.Append(edge.Record{ID: "ht-" + eventID, Body: body, SourceID: s.cfg.SourceID})
	return err
}

// appendFiring promotes a burst to ONE stateful alert carrying the SAME
// stable entity identity — the server-side fingerprint needs the non-empty
// resource_uid (P0-05: no more "incomplete alert identity" for HUATUO).
func (s *Service) appendFiring(d *watchEventData, alertName, key, eventID string) error {
	body, _ := json.Marshal(map[string]any{
		"batch_id": "htp-" + eventID,
		"events": []map[string]any{{
			"event_id":     "htp-" + eventID,
			"transport_id": "htp-" + eventID,
			"tenant_id":    s.cfg.TenantID,
			"source":       map[string]any{"source_id": s.cfg.SourceID, "source_type": 3, "external_event_id": "htp-" + eventID, "external_rule_id": "huatuo-burst"},
			"resource":     d.resourceRef(s.cfg.ClusterID),
			"kind":         1, // STATEFUL
			"signal":       1, // FIRING
			"severity":     2, // ERROR
			"alert_name":   alertName,
			"occurred_at":  nowObj(),
			"received_at":  nowObj(),
			"starts_at":    nowObj(),
			"event_key":    "htp-" + eventID,
			"labels":       d.labels(),
			"annotations":  map[string]string{"source_event": eventID, "burst_key": key},
		}},
	})
	_, err := s.wal.Append(edge.Record{ID: "htp-" + eventID, Body: body, SourceID: s.cfg.SourceID})
	return err
}

func nowObj() map[string]int64 {
	now := time.Now().Unix()
	return map[string]int64{"seconds": now}
}
