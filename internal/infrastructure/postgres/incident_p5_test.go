package postgres

import (
	"context"
	"testing"
	"time"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/application/correlation"
	"github.com/example/alert-platform/internal/application/escalation"
	"github.com/example/alert-platform/internal/application/incident"
)

// INC-002: L3 identity + window auto-merges into the anchor's incident with
// recorded evidence; cross-tenant NEVER merges (red line).
func TestAutoCorrelationMergesAndNeverCrossTenant(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	now := time.Now().UTC()

	fire := func(rule, uid, tenant string, at time.Time) {
		ev := canonical("c-"+rule+"-"+uid+"-"+tenant, at)
		ev.TenantId = tenant
		ev.Source.ExternalRuleId = rule
		ev.Resource.ResourceUid = uid
		if _, err := store.ApplyEvent(ctx, ev); err != nil {
			t.Fatalf("fire %s/%s: %v", rule, uid, err)
		}
	}

	// Anchor: tenant A, pod-1 firing 1 minute ago.
	fire("rule-a", "pod-1", tenantA, now.Add(-time.Minute))
	// Same L3 identity (different RULE — correlation is identity-based):
	fire("rule-b", "pod-1", tenantA, now)
	// Different tenant, same identity: must NEVER merge.
	fire("rule-c", "pod-1", tenantB, now)
	// Different object entirely.
	fire("rule-d", "pod-2", tenantA, now)

	merged, err := store.CorrelateOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if merged != 1 {
		t.Fatalf("merged = %d, want exactly 1 (L3+window)", merged)
	}

	// The anchor incident now links the correlated alert with provenance.
	var relations []string
	rows, err := pool.Query(ctx, `SELECT relation_type FROM incident_alerts ORDER BY relation_type`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var r string
		_ = rows.Scan(&r)
		relations = append(relations, r)
	}
	rows.Close()
	hasAuto, hasDerived := false, false
	for _, r := range relations {
		if r == "auto_correlated" {
			hasAuto = true
		}
		if r == "derived_1to1" {
			hasDerived = true
		}
	}
	if !hasAuto || !hasDerived {
		t.Fatalf("relations = %v, want derived_1to1 + auto_correlated", relations)
	}

	// Evidence is recorded in the audit trail (可解释).
	var evidence int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM audit_logs WHERE operation='incident.auto_correlate' AND details->'evidence' IS NOT NULL`).
		Scan(&evidence); err != nil {
		t.Fatal(err)
	}
	if evidence != 1 {
		t.Fatalf("evidence rows = %d, want 1", evidence)
	}

	// Tenant B's incident is untouched (cross-tenant isolation).
	var bIncidents int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM incidents i JOIN incident_alerts a ON a.incident_id=i.id
		WHERE i.tenant_id=$1`, tenantB).Scan(&bIncidents); err != nil {
		t.Fatal(err)
	}
	if bIncidents != 1 {
		t.Fatalf("tenant B links = %d, want 1 (own only)", bIncidents)
	}
}

// INC-005: an un-ACKed open incident escalates after 5 minutes (clock
// advanced by backdating created_at); ACK stops the ladder.
func TestEscalationLadder(t *testing.T) {
	store, pool := newStoreTest(t)
	incStore := NewIncidentStore(pool)
	ctx := context.Background()
	now := time.Now().UTC()

	// Two firing alerts -> two open incidents; age one beyond the ladder.
	if _, err := store.ApplyEvent(ctx, canonical("e-old", now.Add(-10*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyEvent(ctx, canonical2("e-new", now, "rule-2", "pod-9")); err != nil {
		t.Fatal(err)
	}
	var oldIncident string
	if err := pool.QueryRow(ctx, `
		SELECT id::text FROM incidents ORDER BY created_at LIMIT 1`).Scan(&oldIncident); err != nil {
		t.Fatal(err)
	}
	var newIncident string
	if err := pool.QueryRow(ctx, `
		SELECT id::text FROM incidents ORDER BY created_at DESC LIMIT 1`).Scan(&newIncident); err != nil {
		t.Fatal(err)
	}

	// Age the first incident past the ladder (its created_at is insert-time).
	if _, err := pool.Exec(ctx, `
		UPDATE incidents SET created_at = now() - interval '10 minutes' WHERE id=$1`, oldIncident); err != nil {
		t.Fatal(err)
	}
	svc := escalation.NewService(store, nil)
	svc.RunOnce(ctx)

	// Only the aged incident escalated — with a recorded stage-1 row.
	var stages int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM incident_escalations WHERE incident_id=$1 AND stage=1`, oldIncident).Scan(&stages); err != nil {
		t.Fatal(err)
	}
	if stages != 1 {
		t.Fatalf("aged incident escalations = %d, want 1", stages)
	}
	var anyEsc int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM incident_escalations WHERE incident_id=$1`, newIncident).Scan(&anyEsc); err != nil {
		t.Fatal(err)
	}
	if anyEsc != 0 {
		t.Fatal("fresh incident escalated early")
	}

	// ACK stops the ladder: after ACK, further passes never escalate.
	if _, err := incStore.Acknowledge(ctx, tenantA, oldIncident, "alice", "ack-e1", 1); err != nil {
		t.Fatal(err)
	}
	svc.RunOnce(ctx)
	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM incident_escalations WHERE incident_id=$1`, oldIncident).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("escalations after ACK = %d, want 1 (ladder stopped)", total)
	}
}

// INC-001/006: manual link/unlink is reversible, version-guarded and audited
// (the reversal itself leaves a trace).
func TestManualLinkUnlinkReversible(t *testing.T) {
	store, pool := newStoreTest(t)
	incStore := NewIncidentStore(pool)
	svc := incident.NewService(incStore)
	ctx := context.Background()
	now := time.Now().UTC()

	if _, err := store.ApplyEvent(ctx, canonical("l-1", now)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyEvent(ctx, canonical2("l-2", now, "rule-x", "pod-77")); err != nil {
		t.Fatal(err)
	}
	// Manual linking works ACROSS incidents (multi-member correlation).
	var incidentID, instance2 string
	if err := pool.QueryRow(ctx, `
		SELECT i.id::text FROM incidents i
		JOIN incident_alerts a ON a.incident_id = i.id
		JOIN alert_instances ai ON ai.id = a.alert_instance_id
		WHERE ai.source_id IS NOT NULL ORDER BY i.created_at LIMIT 1`).Scan(&incidentID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT a.alert_instance_id::text FROM incident_alerts a
		JOIN incidents i ON i.id = a.incident_id
		WHERE i.id::text <> $1 LIMIT 1`, incidentID).Scan(&instance2); err != nil {
		t.Fatal(err)
	}

	// Wrong version -> 409 semantics.
	if _, err := svc.LinkAlert(ctx, incStore, tenantA, incidentID, instance2, "alice", 99); err != incident.ErrVersionConflict {
		t.Fatalf("wrong version: %v", err)
	}
	v, err := svc.LinkAlert(ctx, incStore, tenantA, incidentID, instance2, "alice", 1)
	if err != nil || v != 2 {
		t.Fatalf("link: v=%d err=%v", v, err)
	}
	// Unlink (reversible) — and the reversal is audited.
	if err := svc.UnlinkAlert(ctx, incStore, tenantA, incidentID, instance2, "alice"); err != nil {
		t.Fatal(err)
	}
	var unlinkAudit int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM audit_logs WHERE operation='incident.unlink_alert'`).Scan(&unlinkAudit); err != nil {
		t.Fatal(err)
	}
	if unlinkAudit != 1 {
		t.Fatalf("unlink audit rows = %d, want 1 (reversal leaves a trace)", unlinkAudit)
	}
	// Relink works after unlink (reversible in both directions).
	if _, err := svc.LinkAlert(ctx, incStore, tenantA, incidentID, instance2, "alice", 3); err != nil {
		t.Fatalf("relink: %v", err)
	}
}

// INC-004 / T-015: the same IP at different times is a DIFFERENT object —
// uid intervals answer identity queries, never names alone.
func TestUIDHistoryTimeWindow(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// pod-alpha owns uid-1 in window 1; pod-beta owns uid-2 (same IP name)
	// in window 2. Names collide; uids must not.
	old := canonical2("u-1", now.Add(-2*time.Hour), "rule-old", "pod-alpha")
	old.Resource.ResourceName = "10.1.1.5"
	if _, err := store.ApplyEvent(ctx, old); err != nil {
		t.Fatal(err)
	}
	fresh := canonical2("u-2", now, "rule-new", "pod-beta")
	fresh.Resource.ResourceName = "10.1.1.5"
	if _, err := store.ApplyEvent(ctx, fresh); err != nil {
		t.Fatal(err)
	}

	var uids []string
	rows, err := pool.Query(ctx, `
		SELECT entity_uid FROM uid_history WHERE display_name='10.1.1.5' ORDER BY valid_from`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var u string
		_ = rows.Scan(&u)
		uids = append(uids, u)
	}
	rows.Close()
	if len(uids) != 2 || uids[0] == uids[1] {
		t.Fatalf("uid intervals for the shared name = %v, want two DISTINCT uids", uids)
	}

	// The two alerts did NOT auto-correlate (different uid = different object).
	if merged, _ := store.CorrelateOnce(ctx); merged != 0 {
		t.Fatalf("same-name/different-uid alerts merged: %d", merged)
	}
}

// Pure-rule unit checks for the correlation matrix (INC-002/006 negatives).
func TestCorrelationRuleMatrix(t *testing.T) {
	base := correlation.Alert{
		InstanceID: "i1", TenantID: "t1", ClusterID: "c1", EntityType: "pod",
		EntityUID: "uid-1", DisplayName: "web", StartedAt: time.Now(),
	}
	sameL3 := base
	sameL3.InstanceID = "i2"
	crossTenant := base
	crossTenant.TenantID, crossTenant.InstanceID = "t2", "i3"
	farApart := base
	farApart.InstanceID = "i4"
	farApart.StartedAt = base.StartedAt.Add(-time.Hour)
	nameOnly := correlation.Alert{
		InstanceID: "i5", TenantID: "t1", ClusterID: "c1", EntityType: "pod",
		EntityUID: "uid-2", DisplayName: "web", StartedAt: base.StartedAt,
	}

	if d := correlation.SameTenantL3(sameL3, base); !d.Merge || d.Confidence != "high" {
		t.Fatalf("same L3 in window: %+v", d)
	}
	if d := correlation.SameTenantL3(crossTenant, base); d.Merge || d.Confidence != "none" {
		t.Fatalf("cross-tenant: %+v (RED LINE)", d)
	}
	if d := correlation.SameTenantL3(farApart, base); d.Merge || d.Confidence != "suggestion" {
		t.Fatalf("same L3 outside window: %+v", d)
	}
	if d := correlation.SameTenantL3(nameOnly, base); d.Merge || d.Confidence != "suggestion" {
		t.Fatalf("name-only: %+v (must NOT merge)", d)
	}
}

// canonical2 varies rule/uid so identities differ.
func canonical2(transportID string, at time.Time, rule, uid string) *eventv1.AlertEvent {
	ev := canonical(transportID, at)
	ev.Source.ExternalRuleId = rule
	ev.Resource.ResourceUid = uid
	return ev
}

