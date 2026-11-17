package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/example/alert-platform/internal/application/correlation"
	"github.com/example/alert-platform/internal/application/escalation"
)

// AUD-008（#17，真实 PG）：escalation 阶段唯一 —— 双副本并发记录同一
// (incident, stage) 只产生一行审计 + 一条通知（不再双 page）。
func TestRecordEscalationStageIsIdempotent(t *testing.T) {
	store, pool := newNotifyTest(t) // same harness: migrated DB + channel helper
	ctx := context.Background()
	const channelID = "abab0000-0000-0000-0000-000000000001"
	seedChannel(t, pool, channelID)

	tenant := notifyTenant
	var incidentID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO incidents (id, tenant_id, correlation_key, title, status, priority)
		VALUES (gen_random_uuid(), $1, 'ck-1', 'mutex', 'open', 'warning') RETURNING id::text`, tenant).
		Scan(&incidentID); err != nil {
		t.Fatal(err)
	}
	cand := escalation.EscalationCandidate{IncidentID: incidentID, TenantID: tenant, CurrentStage: 0}

	if err := store.RecordEscalation(ctx, cand, 0); err != nil {
		t.Fatal(err)
	}
	// A second replica records the same stage: must be a silent no-op.
	if err := store.RecordEscalation(ctx, cand, 0); err != nil {
		t.Fatal(err)
	}

	var escalations, notifications int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM incident_escalations WHERE incident_id=$1`, incidentID).Scan(&escalations); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM notification_outbox`).Scan(&notifications); err != nil {
		t.Fatal(err)
	}
	if escalations != 1 {
		t.Fatalf("escalation rows = %d, want 1 (stage unique)", escalations)
	}
	if notifications != 1 {
		t.Fatalf("notification tasks = %d, want 1 (no double page)", notifications)
	}
}

// AUD-008：FindAnchor 在 starts_at 并列时使用确定性 tie-breaker
// (incident_id DESC)，同一 candidate 不会因副本不同而关联到不同 incident。
func TestFindAnchorDeterministicOnTie(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)

	// Two active alerts with the SAME entity identity and SAME starts_at,
	// linked to two different incidents.
	var incA, incB, alertA, alertB string
	if err := pool.QueryRow(ctx, `
		INSERT INTO incidents (id, tenant_id, correlation_key, title, status, priority)
		VALUES (gen_random_uuid(), $1, 'ck-a', 'a', 'open', 'warning') RETURNING id::text`, tenantA).Scan(&incA); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO incidents (id, tenant_id, correlation_key, title, status, priority)
		VALUES (gen_random_uuid(), $1, 'ck-b', 'b', 'open', 'warning') RETURNING id::text`, tenantA).Scan(&incB); err != nil {
		t.Fatal(err)
	}
	insertAlert := func(incident, uid string) string {
		t.Helper()
		var id string
		if err := pool.QueryRow(ctx, `
			INSERT INTO alert_instances (id, tenant_id, source_id, fingerprint, episode_key, name, severity,
				monitor_state, cluster_id, entity_type, entity_uid, labels, starts_at, last_observed_at, last_transition_at)
			VALUES (gen_random_uuid(), $1, $2, $3, gen_random_uuid()::text, 'Tie', 'critical',
				'firing', 'c1', 'pod', $4, '{}', $5, $5, $5) RETURNING id::text`,
			tenantA, srcDF, "fp-"+incident, uid, at).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO incident_alerts (tenant_id, incident_id, alert_instance_id, relation_type)
			VALUES ($1,$2,$3,'associated')`, tenantA, incident, id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	alertA = insertAlert(incA, "pod-tie-1")
	alertB = insertAlert(incB, "pod-tie-1")

	cand := correlation.Alert{
		InstanceID: "99999999-9999-9999-9999-999999999999", TenantID: tenantA,
		ClusterID: "c1", EntityType: "pod", EntityUID: "pod-tie-1", StartedAt: at,
	}
	anchor, incident, err := store.FindAnchor(ctx, cand)
	if err != nil || anchor == nil {
		t.Fatalf("anchor=%v err=%v", anchor, err)
	}
	// Deterministic: incident_id DESC tie-breaker, every call the same anchor.
	for i := 0; i < 3; i++ {
		a2, inc2, err := store.FindAnchor(ctx, cand)
		if err != nil || a2 == nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
		if a2.InstanceID != anchor.InstanceID || inc2 != incident {
			t.Fatalf("anchor flapped: %s/%s -> %s/%s", anchor.InstanceID, incident, a2.InstanceID, inc2)
		}
	}
	_ = alertA
	_ = alertB
}
