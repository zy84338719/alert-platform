package translate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// AlertManagerAdapterVer identifies the mapping implementation (ING-002).
const AlertManagerAdapterVer = "alertmanager/v1"

// alertmanagerWebhook is the public Alertmanager webhook payload
// (prometheus/alertmanager docs, "version: 4"). Mapping is per-alert
// (alerts[] -> one canonical event each), never whole-batch (T-003).
type alertmanagerWebhook struct {
	Receiver  string           `json:"receiver"`
	Status    string           `json:"status"`
	Alerts    []alertmanagerAlert `json:"alerts"`
	GroupKey  string           `json:"groupKey"`
	Version   string           `json:"version"`
	Truncated int              `json:"truncatedAlerts"`
}

type alertmanagerAlert struct {
	Status      string            `json:"status"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    string            `json:"startsAt"`
	EndsAt      string            `json:"endsAt"`
	Fingerprint string            `json:"fingerprint"`
	GeneratorURL string           `json:"generatorURL"`
}

// AlertManagerBatch converts one Alertmanager webhook into canonical events.
// tenantID/sourceID/clusterFallback come from the authenticated registry
// entry. AM's per-alert "fingerprint" is the LOGICAL alert identity (kept in
// source.external_event_id) but NEVER the observation key: event_receipts'
// (tenant, source, source_event_key) unique index must see a distinct key per
// OBSERVATION, or the Resolved delivery of the same alert collides with the
// Firing receipt and is silently dropped as a duplicate (P0-01). The platform
// fingerprint is always recomputed server-side from mapped identity fields
// (docs/03 §2).
func AlertManagerBatch(tenantID, sourceID, clusterFallback string, body []byte, receivedAt time.Time) ([]*eventv1.AlertEvent, error) {
	var w alertmanagerWebhook
	if err := json.Unmarshal(body, &w); err != nil {
		return nil, fmt.Errorf("alertmanager webhook: decode: %w", err)
	}
	if len(w.Alerts) == 0 {
		return nil, errors.New("alertmanager webhook: alerts[] is empty")
	}
	events := make([]*eventv1.AlertEvent, 0, len(w.Alerts))
	for i, a := range w.Alerts {
		ev, err := alertManagerOne(tenantID, sourceID, clusterFallback, a, w.GroupKey)
		if err != nil {
			return nil, fmt.Errorf("alertmanager alert %d: %w", i, err)
		}
		ev.ReceivedAt = timestamppb.New(receivedAt)
		events = append(events, ev)
	}
	return events, nil
}

func alertManagerOne(tenantID, sourceID, clusterFallback string, a alertmanagerAlert, groupKey string) (*eventv1.AlertEvent, error) {
	name := a.Labels["alertname"]
	if name == "" {
		return nil, errors.New("labels.alertname is required")
	}
	if a.Status != "firing" && a.Status != "resolved" {
		return nil, fmt.Errorf("status must be firing|resolved, got %q", a.Status)
	}
	labels, err := sanitizeLabels(a.Labels)
	if err != nil {
		return nil, err
	}

	signal := eventv1.EventSignal_EVENT_SIGNAL_FIRING
	if a.Status == "resolved" {
		signal = eventv1.EventSignal_EVENT_SIGNAL_RESOLVED
	}
	severity := amSeverity(a.Labels["severity"])

	startsAt, err := parseRFC3339OrZero(a.StartsAt)
	if err != nil {
		return nil, fmt.Errorf("startsAt: %w", err)
	}
	endsAt, err := parseRFC3339OrZero(a.EndsAt)
	if err != nil {
		return nil, fmt.Errorf("endsAt: %w", err)
	}
	occurredAt := startsAt
	if endsAt.After(startsAt) {
		occurredAt = endsAt // resolved events carry their verdict at end time
	}

	// Identity: entity_uid = AM fingerprint (stable per alert across the
	// firing/resolved lifecycle); explicit label override supported.
	entityUID := a.Labels["uid"]
	if entityUID == "" {
		entityUID = a.Fingerprint
	}
	if entityUID == "" {
		// Deterministic fallback over sorted labels so retries stay stable
		// even on AM versions that omit the fingerprint field.
		entityUID = syntheticUID(a.Labels)
	}
	clusterID := a.Labels["cluster"]
	if clusterID == "" {
		clusterID = clusterFallback
	}

	resType, resName := amResource(a.Labels)
	ev := &eventv1.AlertEvent{
		TenantId: tenantID, // credential-derived; AM payloads carry no tenant claim
		Source: &eventv1.SourceRef{
			SourceId:       sourceID,
			SourceType:     eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER,
			ExternalRuleId: name,
			ExternalEventId: a.Fingerprint,
		},
		Resource: &eventv1.ResourceRef{
			ClusterId:    clusterID,
			Namespace:    a.Labels["namespace"],
			ResourceType: resType,
			ResourceUid:  entityUID,
			ResourceName: resName,
			Environment:  a.Labels["environment"],
		},
		Kind:           eventv1.EventKind_EVENT_KIND_STATEFUL,
		Signal:         signal,
		Severity:       severity,
		AlertName:      name,
		Labels:         labels,
		Annotations:    a.Annotations,
		StartsAt:       timestamppb.New(startsAt),
		EndsAt:         timestamppb.New(endsAt),
		OccurredAt:     timestamppb.New(occurredAt),
		EventKey:       amObservationKey(entityUID, signal, name, startsAt),
		AdapterVersion: AlertManagerAdapterVer,
		SourceUrl:      a.GeneratorURL,
	}
	ev.EventId = ev.EventKey
	return ev, nil
}

// amObservationKey derives the per-observation dedup identity (P0-01): a
// stable hash over the logical alert identity (fingerprint-or-synthetic uid),
// the signal, the rule name and the episode's startsAt. Redeliveries of the
// same observation (AM retry, same startsAt/status) hash to the same key and
// dedup at the receipt layer; the episode's Resolved and the next episode's
// Firing hash to different keys and reach the state machine. The signal is
// spelled "firing|resolved" so Firing≠Resolved even when timestamps coincide.
func amObservationKey(entityUID string, signal eventv1.EventSignal, alertName string, startsAt time.Time) string {
	obs := fmt.Sprintf("am-obs/v1\n%s\n%s\n%s\n%d",
		entityUID, signal.String(), alertName, startsAt.UnixNano())
	sum := sha256.Sum256([]byte(obs))
	return "amobs-" + hex.EncodeToString(sum[:16])
}

// amSeverity maps the conventional severity label; unknown/missing values
// fall back to WARNING (conservative: notify rather than stay silent) and the
// original label value stays visible in Labels.
func amSeverity(v string) eventv1.Severity {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "critical", "crit", "p0", "p1":
		return eventv1.Severity_SEVERITY_CRITICAL
	case "error":
		return eventv1.Severity_SEVERITY_ERROR
	case "info", "none":
		return eventv1.Severity_SEVERITY_INFO
	default:
		return eventv1.Severity_SEVERITY_WARNING
	}
}

// amResource derives a coarse resource_type/name from conventional labels.
func amResource(labels map[string]string) (rtype, rname string) {
	for _, candidate := range []struct{ typ, label string }{
		{"pod", "pod"},
		{"node", "node"},
		{"service", "service"},
		{"instance", "instance"},
		{"job", "job"},
	} {
		if v := labels[candidate.label]; v != "" {
			return candidate.typ, v
		}
	}
	return "", ""
}

// syntheticUID builds a stable uid from sorted labels when neither AM's
// fingerprint nor an explicit uid label exists.
func syntheticUID(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
		b.WriteByte(';')
	}
	sum := sha256.Sum256([]byte("am-uid/v1\n" + b.String()))
	return "am-" + hex.EncodeToString(sum[:16])
}
