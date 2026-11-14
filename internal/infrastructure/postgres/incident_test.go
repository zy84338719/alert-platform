package postgres

import (
	"context"
	"testing"
	"time"


	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/application/incident"
	"github.com/example/alert-platform/internal/application/policymgmt"
)

// UI-004/FR-008: every firing episode opens a 1:1 incident; ACK moves the
// DISPOSAL state only (monitor state untouched), guards the version (409) and
// is idempotent per key.
func TestIncidentOneToOneAndAck(t *testing.T) {
	store, pool := newStoreTest(t)
	incStore := NewIncidentStore(pool)
	svc := incident.NewService(incStore)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	if _, err := store.ApplyEvent(ctx, canonical("ui-1", at)); err != nil {
		t.Fatal(err)
	}
	var incidentID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM incidents`).Scan(&incidentID); err != nil {
		t.Fatalf("1:1 incident missing after firing: %v", err)
	}

	// ACK with the wrong expected version -> 409 semantics.
	_, err := svc.Acknowledge(ctx, tenantA, incidentID, "alice", "ack-1", 99)
	if err != incident.ErrVersionConflict {
		t.Fatalf("wrong expected_version: %v, want ErrVersionConflict", err)
	}

	// Correct version acks.
	in, err := svc.Acknowledge(ctx, tenantA, incidentID, "alice", "ack-1", 1)
	if err != nil || in.Status != incident.StatusAcknowledged || in.Version != 2 {
		t.Fatalf("ack: %v status=%s v=%d", err, in.Status, in.Version)
	}

	// Monitor state is NOT part of the disposal transition (FR-008).
	var monitorState string
	if err := pool.QueryRow(ctx, `SELECT monitor_state FROM alert_instances`).Scan(&monitorState); err != nil {
		t.Fatal(err)
	}
	if monitorState != "firing" {
		t.Fatalf("ack touched monitor state: %q", monitorState)
	}

	// Same key replays idempotently (no second version bump).
	in2, err := svc.Acknowledge(ctx, tenantA, incidentID, "alice", "ack-1", 1)
	if err != nil || in2.Version != 2 {
		t.Fatalf("idempotent replay: %v v=%d, want v=2", err, in2.Version)
	}

	// The ACK landed in the audit trail within the same transaction.
	var acks int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM audit_logs WHERE operation='incident.ack'`).Scan(&acks); err != nil {
		t.Fatal(err)
	}
	if acks != 1 {
		t.Fatalf("audit ack rows = %d, want 1", acks)
	}

	// A later resolved event closes the EPISODE, not the incident.
	res := canonical("ui-2", at.Add(10*time.Minute))
	res.Signal, res.Severity = eventv1.EventSignal_EVENT_SIGNAL_RESOLVED, eventv1.Severity_SEVERITY_UNSPECIFIED
	if _, err := store.ApplyEvent(ctx, res); err != nil {
		t.Fatal(err)
	}
	in3, err := svc.Get(ctx, tenantA, incidentID)
	if err != nil || in3.Status != incident.StatusAcknowledged {
		t.Fatalf("resolved event auto-closed the incident: %s", in3.Status)
	}
}

// POL-001/UI-002: route/silence management + the explainable preview.
func TestPolicySilenceManagementAndPreview(t *testing.T) {
	_, pool := newStoreTest(t)
	svc := policymgmt.NewService(NewPolicyStore(pool))
	ctx := context.Background()

	// Invalid matcher ops are rejected at the boundary.
	if _, err := svc.CreateRoute(ctx, tenantA, "alice", &policymgmt.Route{
		Priority: 1, Matchers: []policymgmt.Matcher{{Name: "ns", Op: "CONTAINS", Value: "x"}},
		Receivers: []policymgmt.RouteReceiver{{ChannelID: "c", ReceiverID: "r"}},
	}); err == nil {
		t.Fatal("invalid matcher op accepted")
	}

	// Route versions are monotonic per tenant.
	r1, err := svc.CreateRoute(ctx, tenantA, "alice", &policymgmt.Route{
		Priority: 10, Matchers: []policymgmt.Matcher{{Name: "ns", Op: "EQ", Value: "prod"}},
		Receivers: []policymgmt.RouteReceiver{{ChannelID: "c1", ReceiverID: "sre", SendResolved: true}},
	})
	if err != nil || r1.Version != 1 {
		t.Fatalf("route v1: %v %+v", err, r1)
	}
	r2, err := svc.CreateRoute(ctx, tenantA, "alice", &policymgmt.Route{
		Priority: 5, Matchers: []policymgmt.Matcher{{Name: "ns", Op: "EQ", Value: "dev"}},
		Receivers: []policymgmt.RouteReceiver{{ChannelID: "c1", ReceiverID: "dev"}},
	})
	if err != nil || r2.Version != 2 {
		t.Fatalf("route v2: %v %+v", err, r2)
	}
	routes, err := svc.ListRoutes(ctx, tenantA)
	if err != nil || len(routes) != 2 || routes[0].Priority != 5 {
		t.Fatalf("list routes: %v %+v", err, routes)
	}
	// Optimistic disable: wrong version -> conflict.
	if _, err := svc.SetRouteEnabled(ctx, tenantA, r2.PolicyID, false, 1); err != policymgmt.ErrVersionConflict {
		t.Fatalf("stale version accepted: %v", err)
	}
	if v, err := svc.SetRouteEnabled(ctx, tenantA, r2.PolicyID, false, 2); err != nil || v != 2 {
		t.Fatalf("disable: %v", err)
	}

	// Silences: window validation + revoke + explainable preview.
	now := time.Now()
	sl := &policymgmt.Silence{
		Matchers: []policymgmt.Matcher{
			{Name: "ns", Op: "EQ", Value: "prod"},
			{Name: "severity", Op: "EQ", Value: "critical"},
		},
		StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Hour),
		CreatedBy: "alice", Reason: "window",
	}
	if err := svc.CreateSilence(ctx, tenantA, sl); err != nil {
		t.Fatal(err)
	}
	bad := *sl
	bad.EndsAt = now.Add(-2 * time.Hour)
	if err := svc.CreateSilence(ctx, tenantA, &bad); err == nil {
		t.Fatal("past window accepted")
	}

	matches, err := svc.PreviewSilences(ctx, tenantA, map[string]string{"ns": "prod", "severity": "critical"}, time.Now())
	if err != nil || len(matches) != 1 {
		t.Fatalf("preview: %v (%d)", err, len(matches))
	}
	if !matches[0].Silenced || len(matches[0].Matched) != 2 || len(matches[0].Unmatched) != 0 {
		t.Fatalf("preview not explainable: %+v", matches[0])
	}
	matches, _ = svc.PreviewSilences(ctx, tenantA, map[string]string{"ns": "prod", "severity": "warning"}, time.Now())
	if matches[0].Silenced || len(matches[0].Unmatched) != 1 {
		t.Fatalf("partial match must not silence: %+v", matches[0])
	}
	if _, err := svc.RevokeSilence(ctx, tenantA, sl.SilenceID); err != nil {
		t.Fatal(err)
	}
	matches, _ = svc.PreviewSilences(ctx, tenantA, map[string]string{"ns": "prod", "severity": "critical"}, time.Now())
	if len(matches) != 0 || true { // revoked silence is out of the active window
		if len(matches) != 0 {
			t.Fatalf("revoked silence still active: %+v", matches)
		}
	}
}

// Channel config-time SSRF validation (T-013 config layer; dial layer is
// covered by the channel tests).
func TestChannelConfigSSRF(t *testing.T) {
	_, pool := newStoreTest(t)
	svc := policymgmt.NewService(NewPolicyStore(pool))
	ctx := context.Background()
	for _, url := range []string{
		"http://169.254.169.254/latest/meta-data",
		"http://10.0.0.5/hook",
		"http://localhost:9999/hook",
		"ftp://example.com/hook",
		"not a url",
	} {
		if err := svc.CreateChannel(ctx, tenantA, &policymgmt.Channel{
			Name: "bad", Type: "webhook", WebhookURL: url,
		}); err == nil {
			t.Fatalf("channel endpoint %q accepted", url)
		}
	}
	// A public endpoint passes config validation (send-time dial guard still applies).
	if err := svc.CreateChannel(ctx, tenantA, &policymgmt.Channel{
		Name: "feishu-official", Type: "feishu", WebhookURL: "https://open.feishu.cn/open-apis/bot/v2/hook/x",
	}); err != nil {
		t.Fatalf("public feishu endpoint rejected: %v", err)
	}
}
