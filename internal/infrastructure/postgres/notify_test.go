package postgres

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/infrastructure/testenv"
)

const notifyTenant = "eeee0000-0000-0000-0000-000000000011"

func newNotifyTest(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	dsn := testenv.StartPostgres(t)
	if _, err := Up(dsn, absMigrations(t)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return NewStore(pool), pool
}

func notifyEvent(transportID string, signal eventv1.EventSignal, at time.Time) *eventv1.AlertEvent {
	ev := canonical(transportID, at)
	ev.TenantId = notifyTenant
	return ev
}

func seedChannel(t *testing.T, pool *pgxpool.Pool, id string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO notification_channels (id, tenant_id, name, type, config, is_default)
		VALUES ($1, $2, 'default-webhook', 'webhook', '{"webhook_url":"http://example.invalid/hook"}', true)
		ON CONFLICT (id) DO NOTHING`, id, notifyTenant)
	if err != nil {
		t.Fatal(err)
	}
}

// NOTI-001 core: a firing transition enqueues notification tasks in the same
// transaction; duplicates (same transition) never double-insert; SKIP LOCKED
// prevents double claims; expired leases re-claim with attempts climbing.
func TestNotifyOutboxPipeline(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	channelID := "abab0000-0000-0000-0000-000000000001"
	seedChannel(t, pool, channelID)

	// 1) firing -> task enqueued via the default channel.
	if _, err := store.ApplyEvent(ctx, notifyEvent("np-1", eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)); err != nil {
		t.Fatal(err)
	}
	var taskID string
	var status string
	if err := pool.QueryRow(ctx, `
		SELECT id::text, status FROM notification_outbox`).Scan(&taskID, &status); err != nil {
		t.Fatalf("notification task not enqueued: %v", err)
	}
	if status != "pending" {
		t.Fatalf("status = %q, want pending", status)
	}

	// 2) claim (SKIP LOCKED) marks sending + bumps attempts.
	tasks, err := store.ClaimNotifyTasks(ctx, 10, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v (%d)", err, len(tasks))
	}
	if tasks[0].ID != taskID || tasks[0].Attempts != 1 {
		t.Fatalf("claimed task: %+v", tasks[0])
	}
	// A second claim while the lease is live finds nothing (no double send).
	again, err := store.ClaimNotifyTasks(ctx, 10, time.Minute)
	if err != nil || len(again) != 0 {
		t.Fatalf("double claim leaked tasks: %v (%d)", err, len(again))
	}

	// 3) simulated sender crash: lease expires, task re-claims with attempts=2.
	if _, err := pool.Exec(ctx, `
		UPDATE notification_outbox SET lease_until = now() - interval '2 hours' WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := store.ClaimNotifyTasks(ctx, 10, time.Minute)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("lease-expiry reclaim: %v (%d)", err, len(reclaimed))
	}
	if reclaimed[0].Attempts != 2 {
		t.Fatalf("attempts after reclaim = %d, want 2", reclaimed[0].Attempts)
	}

	// 4) successful delivery records the attempt row and finalizes the task.
	err = store.FinishNotifyTask(ctx, taskID, true, 0, reclaimed[0].Attempts, 200, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM notification_outbox WHERE id=$1`, taskID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "sent" {
		t.Fatalf("status after send = %q", status)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM notification_attempts WHERE task_id=$1`, taskID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("attempt rows = %d, want 1", attempts)
	}

	// 5) T-008 classification is reflected by FinishNotifyTask:
	//    429 + Retry-After -> retry_scheduled with the honored delay.
	//    (A DIFFERENT alert: the first identity is a repeat-firing no-op now.)
	np2 := notifyEvent("np-2", eventv1.EventSignal_EVENT_SIGNAL_FIRING, at.Add(time.Minute))
	np2.Source.ExternalRuleId = "rule-notify-2"
	if _, err := store.ApplyEvent(ctx, np2); err != nil {
		t.Fatal(err)
	}
	var task2 string
	if err := pool.QueryRow(ctx, `
		SELECT id::text FROM notification_outbox WHERE id::text <> $1 LIMIT 1`, taskID).Scan(&task2); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishNotifyTask(ctx, task2, false, 42*time.Second, 1, 429, "retryable", "rate limited"); err != nil {
		t.Fatal(err)
	}
	var st2 string
	var next time.Time
	if err := pool.QueryRow(ctx, `SELECT status, next_attempt_at FROM notification_outbox WHERE id=$1`, task2).
		Scan(&st2, &next); err != nil {
		t.Fatal(err)
	}
	if st2 != "retry_scheduled" {
		t.Fatalf("429 outcome = %q, want retry_scheduled", st2)
	}
	if d := time.Until(next); d < 30*time.Second || d > 60*time.Second {
		t.Fatalf("Retry-After 42s not honored: next attempt in %v", d)
	}

	// 6) permanent 4xx marks failed (no further retries).
	if err := store.FinishNotifyTask(ctx, task2, false, 0, 2, 404, "permanent", "gone"); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM notification_outbox WHERE id=$1`, task2).Scan(&st2); err != nil {
		t.Fatal(err)
	}
	if st2 != "failed" {
		t.Fatalf("permanent outcome = %q, want failed", st2)
	}
}

// A silence covering the alert enqueues a SUPPRESSED (auditable) task, never
// touching monitor state (T-009).
func TestSilenceSuppressesNotificationOnly(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	channelID := "abab0000-0000-0000-0000-000000000002"
	seedChannel(t, pool, channelID)

	if _, err := pool.Exec(ctx, `
		INSERT INTO silences (id, tenant_id, matchers, starts_at, ends_at, created_by, reason)
		VALUES ($1, $2, '[{"name":"ns","operation":"EQ","value":"demo"}]', now()-interval '1 minute', now()+interval '1 hour', 'tester', 'maintenance')`,
		"abab0000-0000-0000-0000-0000000000ff", notifyTenant); err != nil {
		t.Fatal(err)
	}

	if _, err := store.ApplyEvent(ctx, notifyEvent("np-s1", eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM notification_outbox`).Scan(&status); err != nil {
		t.Fatalf("suppressed row missing: %v", err)
	}
	if status != "suppressed" {
		t.Fatalf("silenced task status = %q, want suppressed", status)
	}
	var monitorState string
	if err := pool.QueryRow(ctx, `SELECT monitor_state FROM alert_instances`).Scan(&monitorState); err != nil {
		t.Fatal(err)
	}
	if monitorState != "firing" {
		t.Fatalf("T-009 violated: silence changed monitor state to %q", monitorState)
	}
}

// Route policies win over the default channel and stamp policy_version.
func TestRoutePolicyStampsVersion(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	channelID := "abab0000-0000-0000-0000-000000000003"
	seedChannel(t, pool, channelID)

	spec := `{"priority":1,"matchers":[{"name":"ns","operation":"EQ","value":"demo"}],` +
		`"receivers":[{"channel_id":"` + channelID + `","receiver_id":"sre-team","send_resolved":true}]}`
	if _, err := pool.Exec(ctx, `
		INSERT INTO policy_versions (id, tenant_id, version, policy_type, spec)
		VALUES ($1, $2, 3, 'route', $3::jsonb)`,
		"abab0000-0000-0000-0000-0000000000aa", notifyTenant, spec); err != nil {
		t.Fatal(err)
	}

	if _, err := store.ApplyEvent(ctx, notifyEvent("np-r1", eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)); err != nil {
		t.Fatal(err)
	}
	var receiver string
	var version int64
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT receiver_id, policy_version, payload FROM notification_outbox`).
		Scan(&receiver, &version, &payload); err != nil {
		t.Fatal(err)
	}
	if receiver != "sre-team" || version != 3 {
		t.Fatalf("route resolution: receiver=%s version=%d", receiver, version)
	}
	// The rendered message embeds the policy version (NOTI-002 acceptance).
	var p struct {
		Message string `json:"message"`
	}
	if err := jsonUnmarshal(payload, &p); err != nil {
		t.Fatal(err)
	}
	if !containsStr(p.Message, "policy_version=3") {
		t.Fatalf("message missing policy version: %q", p.Message)
	}
}

// RETN-001: expired receipts are deleted, in-window receipts retained.
func TestRetentionCleanupReceipts(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	if _, err := store.ApplyEvent(ctx, notifyEvent("rt-1", eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyEvent(ctx, notifyEvent("rt-2", eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)); err != nil {
		t.Fatal(err)
	}
	// Age one receipt beyond the window.
	if _, err := pool.Exec(ctx, `
		UPDATE event_receipts SET last_seen_at = now() - interval '30 days' WHERE transport_id='rt-1'`); err != nil {
		t.Fatal(err)
	}
	deleted, retained, err := store.CleanupReceipts(ctx, time.Now().Add(-7*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 || retained != 1 {
		t.Fatalf("deleted=%d retained=%d, want 1/1", deleted, retained)
	}
	var left string
	if err := pool.QueryRow(ctx, `SELECT transport_id FROM event_receipts`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != "rt-2" {
		t.Fatalf("wrong survivor: %s", left)
	}
}

// AUTH-003: audit details never persist credential material.
func TestAuditSanitization(t *testing.T) {
	_, pool := newNotifyTest(t)
	store := NewAuthnStore(pool)
	ctx := context.Background()
	if err := store.WriteAudit(ctx, AuditEntry{
		TenantID: notifyTenant, ActorID: "alice", Operation: "source.create",
		ObjectType: "source", ObjectID: "s1",
		Details: map[string]any{
			"name": "df", "credential_token": "aps_secret123", "note": "token: leakme",
		},
	}); err != nil {
		t.Fatal(err)
	}
	var details []byte
	if err := pool.QueryRow(ctx, `SELECT details FROM audit_logs`).Scan(&details); err != nil {
		t.Fatal(err)
	}
	if containsStr(string(details), "aps_secret123") || containsStr(string(details), "leakme") {
		t.Fatalf("audit leaked credential material: %s", details)
	}
	if !containsStr(string(details), "[REDACTED]") {
		t.Fatalf("redaction marker missing: %s", details)
	}
	rows, err := store.ListAudit(ctx, notifyTenant, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list audit: %v (%d)", err, len(rows))
	}
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
func containsStr(s, sub string) bool { return strings.Contains(s, sub) }
