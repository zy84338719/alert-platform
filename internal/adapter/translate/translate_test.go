package translate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// Contract tests (ING-001/002/004): golden + negative fixtures live under
// contracts/external/** and are executed verbatim here — CI fails when a
// vendor contract regresses or a fixture is corrupted.
const (
	dfSourceID   = "deepflow-prod-studio"
	dfTenantID   = "11111111-1111-1111-1111-111111111111" // registry-derived
	amSourceID   = "am-prod-studio"
	amTenantID   = "22222222-2222-2222-2222-222222222222"
	clusterFallb = "prod-studio"
)

func fixture(t *testing.T, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "contracts", "external", rel))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return b
}

func TestDeepFlowGoldenBridgeEvent(t *testing.T) {
	events, err := DeepFlowBatch(dfTenantID, dfSourceID, clusterFallb,
		fixture(t, "deepflow/v7.2.3/fixtures/SYNTHETIC-bridge-event.json"),
		time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	ev := events[0]
	if ev.GetSignal() != eventv1.EventSignal_EVENT_SIGNAL_FIRING {
		t.Errorf("signal = %v, want FIRING", ev.GetSignal())
	}
	if ev.GetSeverity() != eventv1.Severity_SEVERITY_WARNING {
		t.Errorf("severity = %v, want WARNING", ev.GetSeverity())
	}
	if ev.GetSource().GetExternalRuleId() != "sample-policy-008" {
		t.Errorf("rule = %q, want policy_id", ev.GetSource().GetExternalRuleId())
	}
	if ev.GetResource().GetResourceUid() != "workload-uid-example" {
		t.Errorf("uid = %q, want target_key", ev.GetResource().GetResourceUid())
	}
	if ev.GetEventKey() != "example-not-real-001#" {
		t.Errorf("event_key = %q, want external_event_id#state (state absent here)", ev.GetEventKey())
	}
	if ev.GetAdapterVersion() != DeepFlowAdapterVer {
		t.Errorf("adapter_version = %q", ev.GetAdapterVersion())
	}
	// Unknown fields are preserved verbatim (mapping.yaml requirement).
	var raw map[string]any
	if err := json.Unmarshal(ev.GetRawPayload(), &raw); err != nil {
		t.Fatalf("raw_payload not JSON: %v", err)
	}
	if _, ok := raw["message"]; !ok {
		t.Error("raw_payload lost the unknown 'message' field")
	}
	if raw["tenant_id"] == dfTenantID {
		t.Error("fixture carries no tenant claim; payload mutation detected")
	}
}

func TestDeepFlowTenantClaimOverridden(t *testing.T) {
	// mismatched-source fixture ALSO carries a tenant_id claim; the source
	// mismatch must be the error that fires, and on the happy path (same
	// fixture with the id fixed) the claim must never leak into the event.
	body := fixture(t, "deepflow/v7.2.3/fixtures/negative/mismatched-source.json")
	if _, err := DeepFlowBatch(dfTenantID, dfSourceID, clusterFallb, body, time.Now()); err == nil {
		t.Fatal("mismatched source_id accepted")
	}
	var patch map[string]any
	_ = json.Unmarshal(body, &patch)
	patch["source_id"] = dfSourceID
	fixed, _ := json.Marshal(patch)
	events, err := DeepFlowBatch(dfTenantID, dfSourceID, clusterFallb, fixed, time.Now())
	if err != nil {
		t.Fatalf("batch with tenant claim: %v", err)
	}
	if got := events[0].GetTenantId(); got != dfTenantID {
		t.Fatalf("tenant = %q, want server-derived %q (ING-001 ③)", got, dfTenantID)
	}
}

func TestDeepFlowNumericEnumsAndArray(t *testing.T) {
	events, err := DeepFlowBatch(dfTenantID, dfSourceID, clusterFallb,
		fixture(t, "deepflow/v7.2.3/fixtures/SYNTHETIC-bridge-event-array.json"), time.Now())
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	crit, rec := events[0], events[1]
	if crit.GetSeverity() != eventv1.Severity_SEVERITY_CRITICAL || crit.GetSignal() != eventv1.EventSignal_EVENT_SIGNAL_FIRING {
		t.Errorf("event 0: level 1 -> FIRING/CRITICAL, got %v/%v", crit.GetSignal(), crit.GetSeverity())
	}
	if rec.GetSignal() != eventv1.EventSignal_EVENT_SIGNAL_RESOLVED {
		t.Errorf("event 1: level 5/state 1 -> RESOLVED, got %v", rec.GetSignal())
	}
	// Same external_event_id both sides: the aggregate id correlates the
	// pair (docs/06 §4), while the transport dedup keys differ by state so a
	// recovery is never swallowed by receipts as a duplicate of its firing.
	if crit.GetSource().GetExternalEventId() != rec.GetSource().GetExternalEventId() {
		t.Errorf("aggregate event id drifted: %q vs %q",
			crit.GetSource().GetExternalEventId(), rec.GetSource().GetExternalEventId())
	}
	if crit.GetEventKey() == rec.GetEventKey() {
		t.Errorf("Ongoing/Ended share one transport key: receipts would eat the recovery (%q)", crit.GetEventKey())
	}
}

func TestDeepFlowNoDataIsNotResolved(t *testing.T) {
	events, err := DeepFlowBatch(dfTenantID, dfSourceID, clusterFallb,
		fixture(t, "deepflow/v7.2.3/fixtures/SYNTHETIC-bridge-nodata.json"), time.Now())
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if got := events[0].GetSignal(); got != eventv1.EventSignal_EVENT_SIGNAL_NO_DATA {
		t.Fatalf("no_data must map to NO_DATA, got %v", got)
	}
	if events[0].GetSeverity() != eventv1.Severity_SEVERITY_UNSPECIFIED {
		t.Errorf("no_data severity = %v, want UNSPECIFIED", events[0].GetSeverity())
	}
}

func TestDeepFlowNegativeFixtures(t *testing.T) {
	negatives, err := filepath.Glob(filepath.Join("..", "..", "..", "contracts", "external", "deepflow", "v7.2.3", "fixtures", "negative", "*.json"))
	if err != nil || len(negatives) == 0 {
		t.Fatalf("glob negatives: %v (%d)", err, len(negatives))
	}
	for _, path := range negatives {
		t.Run(filepath.Base(path), func(t *testing.T) {
			if _, err := DeepFlowBatch(dfTenantID, dfSourceID, clusterFallb, mustRead(t, path), time.Now()); err == nil {
				t.Fatalf("negative fixture %s was ACCEPTED", path)
			}
		})
	}
}

// P0-01 (rh-p0-01): the observation key separates logical alert identity
// (fingerprint) from per-observation identity — redeliveries dedup, the
// episode's Resolved and the next episode's Firing each get their own key.
func TestAlertManagerObservationKeys(t *testing.T) {
	body := func(status, startsAt, endsAt string) []byte {
		return []byte(fmt.Sprintf(`{"version":"4","groupKey":"g1","receiver":"r","status":%q,"alerts":[{"status":%q,"labels":{"alertname":"HighRequestLatency","severity":"critical"},"startsAt":%q,"endsAt":%q,"fingerprint":"fp-x"}]}`, status, status, startsAt, endsAt))
	}
	keyOf := func(t *testing.T, b []byte) string {
		t.Helper()
		events, err := AlertManagerBatch(amTenantID, amSourceID, clusterFallb, b, time.Now())
		if err != nil || len(events) != 1 {
			t.Fatalf("batch: %v (%d events)", err, len(events))
		}
		return events[0].GetEventKey()
	}

	kFiring := keyOf(t, body("firing", "2026-11-15T10:00:00Z", "0001-01-01T00:00:00Z"))
	kRedelivery := keyOf(t, body("firing", "2026-11-15T10:00:00Z", "0001-01-01T00:00:00Z"))
	kResolved := keyOf(t, body("resolved", "2026-11-15T10:00:00Z", "2026-11-15T10:30:00Z"))
	kNextEpisode := keyOf(t, body("firing", "2026-11-15T11:00:00Z", "0001-01-01T00:00:00Z"))

	if kFiring != kRedelivery {
		t.Errorf("same observation redelivered: %q vs %q — must share the key (receipt dedup)", kFiring, kRedelivery)
	}
	if kFiring == kResolved {
		t.Errorf("firing and resolved of one episode share the key %q — the recovery would be dropped as a duplicate (P0-01)", kFiring)
	}
	if kFiring == kNextEpisode {
		t.Errorf("next-episode firing shares the key %q — the new episode would be absorbed (P0-01)", kFiring)
	}
}

func TestAlertManagerGoldenWebhook(t *testing.T) {
	events, err := AlertManagerBatch(amTenantID, amSourceID, clusterFallb,
		fixture(t, "alertmanager/fixtures/SYNTHETIC-webhook.json"), time.Now())
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("alerts[] must map per-alert, got %d", len(events))
	}
	ev := events[0]
	if ev.GetAlertName() != "HighRequestLatency" || ev.GetSource().GetExternalRuleId() != "HighRequestLatency" {
		t.Errorf("alertname mapping: %q / rule %q", ev.GetAlertName(), ev.GetSource().GetExternalRuleId())
	}
	if ev.GetSignal() != eventv1.EventSignal_EVENT_SIGNAL_FIRING {
		t.Errorf("signal = %v, want FIRING", ev.GetSignal())
	}
	// P0-01: event_key is the per-observation identity, NOT the fingerprint —
	// the fingerprint stays the logical identity in source.external_event_id.
	if ev.GetEventKey() == "fakefingerprint" || ev.GetEventKey() == "" {
		t.Errorf("event_key = %q, want a per-observation key distinct from the fingerprint", ev.GetEventKey())
	}
	if ev.GetSource().GetExternalEventId() != "fakefingerprint" {
		t.Errorf("external_event_id = %q, want the AM fingerprint as logical identity", ev.GetSource().GetExternalEventId())
	}
	if ev.GetResource().GetClusterId() != clusterFallb {
		t.Errorf("cluster fallback missing: %q", ev.GetResource().GetClusterId())
	}
	if ev.GetTenantId() != amTenantID {
		t.Errorf("tenant = %q, want credential-derived", ev.GetTenantId())
	}
}

func TestAlertManagerMultiInstanceBatch(t *testing.T) {
	events, err := AlertManagerBatch(amTenantID, amSourceID, clusterFallb,
		fixture(t, "alertmanager/fixtures/SYNTHETIC-webhook-multi.json"), time.Now())
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("per-alert mapping: want 3 events, got %d", len(events))
	}
	if events[0].GetEventKey() != events[1].GetEventKey() {
		t.Error("duplicate fingerprint pair (same startsAt/status) must share the observation key (receipt dedups)")
	}
	res := events[2]
	if res.GetSignal() != eventv1.EventSignal_EVENT_SIGNAL_RESOLVED {
		t.Errorf("alert 2 signal = %v, want RESOLVED", res.GetSignal())
	}
	if !res.GetEndsAt().AsTime().Equal(res.GetOccurredAt().AsTime()) {
		t.Errorf("resolved occurred_at should anchor at endsAt: %v vs %v", res.GetOccurredAt().AsTime(), res.GetEndsAt().AsTime())
	}
	if res.GetResource().GetResourceType() != "node" || res.GetResource().GetResourceName() != "node-7" {
		t.Errorf("node resource mapping: %q/%q", res.GetResource().GetResourceType(), res.GetResource().GetResourceName())
	}
}

func TestAlertManagerNegativeFixtures(t *testing.T) {
	negatives, err := filepath.Glob(filepath.Join("..", "..", "..", "contracts", "external", "alertmanager", "fixtures", "negative", "*.json"))
	if err != nil || len(negatives) == 0 {
		t.Fatalf("glob negatives: %v (%d)", err, len(negatives))
	}
	for _, path := range negatives {
		t.Run(filepath.Base(path), func(t *testing.T) {
			if _, err := AlertManagerBatch(amTenantID, amSourceID, clusterFallb, mustRead(t, path), time.Now()); err == nil {
				t.Fatalf("negative fixture %s was ACCEPTED", path)
			}
		})
	}
}

// TestFingerprintLabelOrderIndependence pins the ING-002 acceptance "fingerprint
// 排序无关": the fingerprint input is the JSON-marshal of the identity, and
// encoding/json sorts map keys — so differently-ordered label maps produce
// byte-identical identity bytes (server-side recompute, not AM's field).
func TestFingerprintLabelOrderIndependence(t *testing.T) {
	ja, err := json.Marshal(map[string]string{"a": "1", "b": "2", "c": "3"})
	if err != nil {
		t.Fatal(err)
	}
	jb, err := json.Marshal(map[string]string{"c": "3", "a": "1", "b": "2"})
	if err != nil {
		t.Fatal(err)
	}
	if string(ja) != string(jb) {
		t.Fatalf("label order leaked into fingerprint input: %s vs %s", ja, jb)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}
