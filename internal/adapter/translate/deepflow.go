// Package translate holds the vendor→canonical adapters (ING-001/002).
// Each adapter is a pure function from a registered source + raw HTTP body to
// canonical events: no I/O, no clock (receivedAt is injected), no tenant
// trust (tenantID comes from the credential-derived registry entry only).
package translate

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// DeepFlow adapter constants (docs/06). The bridge schema is what we ask the
// enterprise push template to emit; semantics of level/state follow the
// official open-source enums (event_level 1-6, state 0=Ongoing/1=Ended).
const (
	DeepFlowSchemaVersion = "deepflow.bridge.v1"
	DeepFlowAdapterVer    = "deepflow-bridge/v1"
)

// deepFlowLevel is one row of the docs/06 §4 mapping table.
type deepFlowLevel struct {
	signal   eventv1.EventSignal
	severity eventv1.Severity
}

// deepFlowLevels maps BOTH accepted spellings: the bridge string form and the
// official numeric enum (json.Number / float64 from encoding/json).
var deepFlowLevels = map[string]deepFlowLevel{
	// Bridge strings (mapping.yaml signal_map)
	"critical": {eventv1.EventSignal_EVENT_SIGNAL_FIRING, eventv1.Severity_SEVERITY_CRITICAL},
	"error":    {eventv1.EventSignal_EVENT_SIGNAL_FIRING, eventv1.Severity_SEVERITY_ERROR},
	"warning":  {eventv1.EventSignal_EVENT_SIGNAL_FIRING, eventv1.Severity_SEVERITY_WARNING},
	"no_data":  {eventv1.EventSignal_EVENT_SIGNAL_NO_DATA, eventv1.Severity_SEVERITY_UNSPECIFIED},
	"recovery": {eventv1.EventSignal_EVENT_SIGNAL_RESOLVED, eventv1.Severity_SEVERITY_UNSPECIFIED},
	"info":     {eventv1.EventSignal_EVENT_SIGNAL_INFO, eventv1.Severity_SEVERITY_INFO},
	// Official numeric enums (docs/06 §1)
	"1": {eventv1.EventSignal_EVENT_SIGNAL_FIRING, eventv1.Severity_SEVERITY_CRITICAL},
	"2": {eventv1.EventSignal_EVENT_SIGNAL_FIRING, eventv1.Severity_SEVERITY_ERROR},
	"3": {eventv1.EventSignal_EVENT_SIGNAL_FIRING, eventv1.Severity_SEVERITY_WARNING},
	"4": {eventv1.EventSignal_EVENT_SIGNAL_NO_DATA, eventv1.Severity_SEVERITY_UNSPECIFIED},
	"5": {eventv1.EventSignal_EVENT_SIGNAL_RESOLVED, eventv1.Severity_SEVERITY_UNSPECIFIED},
	"6": {eventv1.EventSignal_EVENT_SIGNAL_INFO, eventv1.Severity_SEVERITY_INFO},
}

// deepFlowBridge is the typed view of one bridge event (docs/06 §3). Fields
// beyond this struct are preserved verbatim in raw_payload. event_level /
// event_state accept BOTH spellings (bridge string, official numeric enum).
type deepFlowBridge struct {
	SchemaVersion   string            `json:"schema_version"`
	SourceID        string            `json:"source_id"`
	SourceVersion   string            `json:"source_version"`
	ExternalEventID string            `json:"external_event_id"`
	EventState      any               `json:"event_state"`
	PolicyID        string            `json:"policy_id"`
	EventLevel      any               `json:"event_level"`
	EventKind       string            `json:"event_kind"`
	AlertName       string            `json:"alert_name"`
	OccurredAt      string            `json:"occurred_at"`
	StartsAt        string            `json:"starts_at"`
	EndsAt          string            `json:"ends_at"`
	TargetKey       string            `json:"target_key"`
	ClusterID       string            `json:"cluster_id"`
	EntityType      string            `json:"entity_type"`
	Labels          map[string]string `json:"labels"`
	DetailsURL      string            `json:"details_url"`
	// TenantClaim exists to PROVE the override: any tenant field in a vendor
	// payload is decoded but always replaced by the credential-derived tenant.
	TenantClaim string `json:"tenant_id"`
}

// scalarString normalizes the dual spellings of enum-ish bridge fields.
func scalarString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return ""
	}
}

// DeepFlowBatch converts one DeepFlow bridge push (single object or array)
// into canonical events. sourceID/tenantID/clusterFallback come from the
// authenticated registry entry — never from the payload.
func DeepFlowBatch(tenantID, sourceID, clusterFallback string, body []byte, receivedAt time.Time) ([]*eventv1.AlertEvent, error) {
	var raws []json.RawMessage
	trimmed := strings.TrimSpace(string(body))
	if strings.HasPrefix(trimmed, "[") {
		if err := json.Unmarshal(body, &raws); err != nil {
			return nil, fmt.Errorf("deepflow bridge: body must be a JSON object or array: %w", err)
		}
	} else {
		raws = []json.RawMessage{json.RawMessage(body)}
	}
	if len(raws) == 0 {
		return nil, errors.New("deepflow bridge: empty batch")
	}
	events := make([]*eventv1.AlertEvent, 0, len(raws))
	for i, raw := range raws {
		ev, err := deepFlowOne(tenantID, sourceID, clusterFallback, raw, body)
		if err != nil {
			return nil, fmt.Errorf("deepflow bridge event %d: %w", i, err)
		}
		ev.ReceivedAt = timestamppb.New(receivedAt)
		events = append(events, ev)
	}
	return events, nil
}

func deepFlowOne(tenantID, sourceID, clusterFallback string, raw json.RawMessage, fullBody []byte) (*eventv1.AlertEvent, error) {
	var b deepFlowBridge
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&b); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if b.SchemaVersion != DeepFlowSchemaVersion {
		return nil, fmt.Errorf("schema_version must be %q, got %q", DeepFlowSchemaVersion, b.SchemaVersion)
	}
	if b.SourceID == "" {
		return nil, errors.New("source_id is required")
	}
	if b.SourceID != sourceID {
		return nil, fmt.Errorf("payload source_id %q does not match the credential's source %q", b.SourceID, sourceID)
	}
	if b.PolicyID == "" || b.AlertName == "" || b.TargetKey == "" {
		return nil, errors.New("policy_id, alert_name and target_key are required")
	}
	if b.OccurredAt == "" {
		return nil, errors.New("occurred_at is required (RFC3339)")
	}
	occurredAt, err := time.Parse(time.RFC3339, b.OccurredAt)
	if err != nil {
		return nil, fmt.Errorf("occurred_at: %w", err)
	}

	// Level: bridge string spelling or the official numeric enum, both map
	// through one table (docs/06 §4 + mapping.yaml signal_map).
	levelKey := strings.ToLower(scalarString(b.EventLevel))
	if levelKey == "" {
		return nil, errors.New("event_level is required (1-6 or critical/error/warning/no_data/recovery/info)")
	}
	level, ok := deepFlowLevels[levelKey]
	if !ok {
		return nil, fmt.Errorf("unknown event_level %q", levelKey)
	}

	kind := eventv1.EventKind_EVENT_KIND_STATEFUL
	switch strings.ToLower(b.EventKind) {
	case "", "stateful":
	case "occurrence":
		kind = eventv1.EventKind_EVENT_KIND_OCCURRENCE
	default:
		return nil, fmt.Errorf("unknown event_kind %q", b.EventKind)
	}

	signal := level.signal
	// state=1 (Ended) closes the lifecycle regardless of level spelling; the
	// store's out-of-order guard (PROC-005) keeps a late Ended from closing a
	// NEW episode and "starts_at alignment" reconciliation stays server-side
	// (docs/06 §4 recovery correlation).
	if scalarString(b.EventState) == "1" {
		signal = eventv1.EventSignal_EVENT_SIGNAL_RESOLVED
	}

	labels, err := sanitizeLabels(b.Labels)
	if err != nil {
		return nil, err
	}
	clusterID := b.ClusterID
	if clusterID == "" {
		clusterID = clusterFallback
	}
	entityType := b.EntityType
	if entityType == "" {
		// Deterministic default so the fingerprint stays stable; upgradable
		// to an explicit entity_type once real payloads confirm the field.
		entityType = "deepflow_target"
	}

	ev := &eventv1.AlertEvent{
		// tenant_id: credential-derived ONLY. b.TenantClaim is deliberately
		// never read beyond the decode above (ING-001 ③).
		TenantId: tenantID,
		Source: &eventv1.SourceRef{
			SourceId:        sourceID,
			SourceType:      eventv1.SourceType_SOURCE_TYPE_DEEPFLOW,
			SourceVersion:   b.SourceVersion,
			ExternalEventId: b.ExternalEventID,
			ExternalRuleId:  b.PolicyID,
		},
		Resource: &eventv1.ResourceRef{
			ClusterId:    clusterID,
			ResourceType: entityType,
			ResourceUid:  b.TargetKey,
		},
		Kind:           kind,
		Signal:         signal,
		Severity:       level.severity,
		AlertName:      b.AlertName,
		Labels:         labels,
		OccurredAt:     timestamppb.New(occurredAt),
		EventKey:       b.ExternalEventID,
		RawPayload:     fullBody,
		AdapterVersion: DeepFlowAdapterVer,
		SourceUrl:      b.DetailsURL,
	}
	if ev.EventKey == "" {
		// Fallback per mapping.yaml: stable only if the source repeats the
		// same fields on retries — verified once real samples land (ARC-002).
		ev.EventKey = fmt.Sprintf("%s|%s|%s|%d", b.PolicyID, b.TargetKey, levelKey, occurredAt.Unix())
	} else {
		// The official event_id identifies the AGGREGATE occurrence — its
		// Ongoing and Ended BOTH carry it (docs/06 §1). The transport dedup
		// key must therefore ALSO embed the state, otherwise a recovery is
		// swallowed by the receipts layer as a duplicate of its firing.
		ev.EventKey = ev.EventKey + "#" + scalarString(b.EventState)
	}
	if ev.EventId == "" {
		ev.EventId = ev.EventKey
	}
	if b.StartsAt != "" {
		if t, err := time.Parse(time.RFC3339, b.StartsAt); err == nil {
			ev.StartsAt = timestamppb.New(t)
		}
	}
	if b.EndsAt != "" {
		if t, err := time.Parse(time.RFC3339, b.EndsAt); err == nil {
			ev.EndsAt = timestamppb.New(t)
		}
	}
	return ev, nil
}

// sanitizeLabels enforces the bridge limits (docs/06 §3: limited key, count
// and size) and rejects non-string values instead of coercing them.
func sanitizeLabels(in map[string]string) (map[string]string, error) {
	if len(in) > 32 {
		return nil, fmt.Errorf("too many labels (%d > 32)", len(in))
	}
	for k, v := range in {
		if len(k) > 128 {
			return nil, fmt.Errorf("label key too long (truncated to 32 chars): %.32s", k)
		}
		if len(v) > 512 {
			return nil, fmt.Errorf("label value too long for key %q", k)
		}
	}
	return in, nil
}

// parseRFC3339OrZero accepts the Alertmanager time format (RFC3339 with
// optional fractional seconds) and returns the zero Time for the sentinel
// "0001-01-01..." value that AM uses for absent timestamps.
func parseRFC3339OrZero(s string) (time.Time, error) {
	if s == "" || strings.HasPrefix(s, "0001-01-01") {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}
