package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/application/notify"
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

// activeAuthority initializes the notification-authority ledger for the test
// as an active deployment (P8-P0-01) and returns it. Call it BEFORE enqueuing
// tasks that are supposed to be delivered: the first sync always opens a new
// generation, and pre-authority task rows are never adopted.
func activeAuthority(t *testing.T, store *Store) notify.Authority {
	t.Helper()
	a, err := store.SyncNotifyAuthority(context.Background(), notify.ModeActive)
	if err != nil {
		t.Fatalf("sync notify authority: %v", err)
	}
	return a
}

func notifyEvent(transportID string, signal eventv1.EventSignal, at time.Time) *eventv1.AlertEvent {
	ev := canonical(transportID, at)
	ev.TenantId = notifyTenant
	return ev
}

func seedChannel(t *testing.T, pool *pgxpool.Pool, id string) {
	seedChannelFor(t, pool, id, notifyTenant)
}

func seedChannelFor(t *testing.T, pool *pgxpool.Pool, id, tenant string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO notification_channels (id, tenant_id, name, type, config, is_default)
		VALUES ($1, $2, 'default-webhook', 'webhook', '{"webhook_url":"http://example.invalid/hook"}', true)
		ON CONFLICT (id) DO NOTHING`, id, tenant)
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
	activeAuthority(t, store)

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
	err = store.FinishNotifyTask(ctx, taskID, reclaimed[0].LeaseToken, true, 0, reclaimed[0].Attempts, 200, "", "", 15)
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
	if err := store.FinishNotifyTask(ctx, task2, "", false, 42*time.Second, 1, 429, "retryable", "rate limited", 15); err != nil {
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
	if err := store.FinishNotifyTask(ctx, task2, "", false, 0, 2, 404, "permanent", "gone", 15); err != nil {
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
func containsStr(s, sub string) bool      { return strings.Contains(s, sub) }

// P0-06 (rh-p0-06) 失败测试：发送前静默复核矩阵 ——
// ① 两条规则：第一条不匹配、第二条匹配 → 必须抑制（修复前 LIMIT 1 只看第一条，
//
//	漏抑制 → 通知静默失效）；
//
// ② 尚未开始的规则 → 不抑制（修复前 SQL 只查 ends_at > now()，未来静默被当有效）；
// ③ 已结束的规则 → 不抑制；边界时间规则 → 抑制。
func TestSuppressNotifyAtSendMultiRuleMatrix(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	// 每个子用例独立租户 + 默认渠道：断言彼此零污染，旧代码下每条都确定性
	// 命中目标缺陷路径（LIMIT 1 顺序无关）。
	setupTenant := func(t *testing.T, suffix string) (tenant, channel string) {
		t.Helper()
		tenant = fmt.Sprintf("eeee0000-0000-0000-0000-0000000000%s", suffix)
		channel = fmt.Sprintf("eeee0000-0000-0000-0000-00000000c0%s", suffix)
		seedChannelFor(t, pool, channel, tenant)
		return tenant, channel
	}
	pendingTask := func(t *testing.T, tenant, name string) string {
		t.Helper()
		ev := notifyEvent("np-p0-"+name, eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)
		ev.TenantId = tenant
		ev.AlertName = name
		ev.EventKey = "ek-" + name
		ev.Source.ExternalRuleId = "rule-" + name // 独立指纹 → 各自 episode/task
		if _, err := store.ApplyEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
		var id string
		if err := pool.QueryRow(ctx, `
			SELECT o.id::text FROM notification_outbox o
			JOIN alert_instances i ON i.id = o.alert_instance_id
			WHERE i.name = $1 AND o.status='pending'`, name).Scan(&id); err != nil {
			t.Fatalf("pending task for %s: %v", name, err)
		}
		return id
	}

	t.Run("second rule matches", func(t *testing.T) {
		tenant, _ := setupTenant(t, "01")
		taskID := pendingTask(t, tenant, "P0SixA")
		labels := map[string]string{"ns": "demo-a"}
		// 第一条不匹配 + 第二条匹配：修复前 LIMIT 1 拿到第一条即返回 false（漏抑制）
		_, err := pool.Exec(ctx, `
			INSERT INTO silences (id, tenant_id, matchers, starts_at, ends_at, created_by, reason) VALUES
			('abab0000-0000-0000-0000-00000000e001', $1, '[{"name":"ns","operation":"EQ","value":"OTHER"}]', now()-interval '1 minute', now()+interval '1 hour', 'tester', 'no-match'),
			('abab0000-0000-0000-0000-00000000e002', $1, '[{"name":"ns","operation":"EQ","value":"demo-a"}]', now()-interval '1 minute', now()+interval '1 hour', 'tester', 'match')`,
			tenant)
		if err != nil {
			t.Fatal(err)
		}
		suppressed, err := store.SuppressNotifyAtSend(ctx, taskID, labels)
		if err != nil {
			t.Fatal(err)
		}
		if !suppressed {
			t.Fatal("the matching second rule must suppress (LIMIT 1 miss = leak)")
		}
	})

	t.Run("future silence does not suppress", func(t *testing.T) {
		tenant, _ := setupTenant(t, "02")
		taskID := pendingTask(t, tenant, "P0SixB")
		labels := map[string]string{"ns": "demo-b"}
		_, err := pool.Exec(ctx, `
			INSERT INTO silences (id, tenant_id, matchers, starts_at, ends_at, created_by, reason)
			VALUES ('abab0000-0000-0000-0000-00000000e003', $1, '[{"name":"ns","operation":"EQ","value":"demo-b"}]', now()+interval '1 hour', now()+interval '2 hour', 'tester', 'future')`,
			tenant)
		if err != nil {
			t.Fatal(err)
		}
		suppressed, err := store.SuppressNotifyAtSend(ctx, taskID, labels)
		if err != nil {
			t.Fatal(err)
		}
		if suppressed {
			t.Fatal("a not-yet-started silence must not suppress")
		}
	})

	t.Run("ended silence does not suppress", func(t *testing.T) {
		tenant, _ := setupTenant(t, "03")
		taskID := pendingTask(t, tenant, "P0SixC")
		labels := map[string]string{"ns": "demo-c"}
		_, err := pool.Exec(ctx, `
			INSERT INTO silences (id, tenant_id, matchers, starts_at, ends_at, created_by, reason)
			VALUES ('abab0000-0000-0000-0000-00000000e004', $1, '[{"name":"ns","operation":"EQ","value":"demo-c"}]', now()-interval '2 hour', now()-interval '1 minute', 'tester', 'ended')`,
			tenant)
		if err != nil {
			t.Fatal(err)
		}
		suppressed, err := store.SuppressNotifyAtSend(ctx, taskID, labels)
		if err != nil {
			t.Fatal(err)
		}
		if suppressed {
			t.Fatal("an ended silence must not suppress")
		}
	})
}

// R27（真实 PG）：可投递性由"创建时的授权模式"决定 ——
// ① active 纪元入队 → created_mode='active'，可领；
// ② pre-ledger（”）永不可领；
// ③ shadow 纪元任务切回 active 后不补投（无风暴）；
// ④ active 期任务被 shadow 打断 → 回 active 后补投恰一次（REL-002 phase 4）。
// 断言为成员关系式：reset 会恢复其他任务，逐名断言更稳。
func TestClaimNotifyTasksCreatedModeIsolation(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	const channelID = "abab0000-0000-0000-0000-000000000001"
	seedChannel(t, pool, channelID)
	activeAuthority(t, store)
	at := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)

	mkTask := func(t *testing.T, name string) {
		t.Helper()
		ev := notifyEvent("np-cm-"+name, eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)
		ev.AlertName = name
		ev.EventKey = "ek-" + name
		ev.Source.ExternalRuleId = "rule-" + name
		if _, err := store.ApplyEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	modeOf := func(t *testing.T, name string) string {
		t.Helper()
		var cm string
		if err := pool.QueryRow(ctx, `
			SELECT o.created_mode FROM notification_outbox o
			JOIN alert_instances i ON i.id = o.alert_instance_id
			WHERE i.name = $1`, name).Scan(&cm); err != nil {
			t.Fatalf("created_mode of %s: %v", name, err)
		}
		return cm
	}
	names := func(rows []notify.Task) []string {
		var out []string
		for _, r := range rows {
			var n string
			if err := pool.QueryRow(ctx, `
				SELECT i.name FROM alert_instances i
				JOIN notification_outbox o ON o.alert_instance_id = i.id
				WHERE o.id = $1`, r.ID).Scan(&n); err != nil {
				t.Fatal(err)
			}
			out = append(out, n)
		}
		return out
	}
	setPending := func(t *testing.T, name string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `UPDATE notification_outbox SET status='pending', lease_until=NULL, lease_token=NULL
			WHERE id IN (SELECT o.id FROM notification_outbox o JOIN alert_instances i ON i.id=o.alert_instance_id WHERE i.name=$1)`, name); err != nil {
			t.Fatal(err)
		}
	}
	claim := func() []notify.Task {
		rows, err := store.ClaimNotifyTasks(ctx, 10, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}

	// ① active 纪元入队 → created_mode='active'，可领。
	mkTask(t, "Active1")
	if cm := modeOf(t, "Active1"); cm != "active" {
		t.Fatalf("created_mode = %q, want active", cm)
	}
	if got := names(claim()); len(got) != 1 || got[0] != "Active1" {
		t.Fatalf("claimed %v, want [Active1]", got)
	}

	// ② pre-ledger（created_mode=''）永不可领。
	mkTask(t, "PreLedger")
	if _, err := pool.Exec(ctx, `UPDATE notification_outbox SET created_mode='' WHERE id IN (
		SELECT o.id FROM notification_outbox o JOIN alert_instances i ON i.id=o.alert_instance_id WHERE i.name='PreLedger')`); err != nil {
		t.Fatal(err)
	}
	for _, n := range names(claim()) {
		if n == "PreLedger" {
			t.Fatal("pre-ledger row was claimed (created_mode='' must never deliver)")
		}
	}

	// ③ shadow 纪元任务：切 shadow 入队 → 切回 active → 不补投；新任务可领。
	if _, err := store.SyncNotifyAuthority(ctx, notify.ModeShadow); err != nil {
		t.Fatal(err)
	}
	mkTask(t, "ShadowEra")
	if cm := modeOf(t, "ShadowEra"); cm != "shadow" {
		t.Fatalf("created_mode = %q, want shadow", cm)
	}
	if _, err := store.SyncNotifyAuthority(ctx, notify.ModeActive); err != nil {
		t.Fatal(err)
	}
	mkTask(t, "Fresh")
	hasFresh, hasShadow := false, false
	for _, n := range names(claim()) {
		if n == "Fresh" {
			hasFresh = true
		}
		if n == "ShadowEra" {
			hasShadow = true
		}
	}
	if !hasFresh {
		t.Fatal("Fresh (active era) must be claimable after return")
	}
	if hasShadow {
		t.Fatal("ShadowEra task was claimed (shadow era must never replay)")
	}

	// ④ active 期任务被 shadow 打断 → 回 active 后补投恰一次。
	setPending(t, "Active1") // active 期任务重新入队（模拟被 shadow 打断的在途任务）
	if _, err := store.SyncNotifyAuthority(ctx, notify.ModeShadow); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SyncNotifyAuthority(ctx, notify.ModeActive); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, n := range names(claim()) {
		if n == "Active1" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("Active1 claimed %d times, want exactly 1 (deliver after return, no duplicates)", count)
	}
}

// P1-02 (rh-p1-02) 失败测试：通知 Lease Fencing —— 完成操作必须做所有权
// 条件更新（WHERE lease_token 匹配）：超时后任务被他 Worker 重领，旧 Worker
// 的迟到完成只写审计、不覆盖新所有者状态；新 Worker 正常完成。
func TestFinishNotifyTaskLeaseFencing(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	const channelID = "abab0000-0000-0000-0000-000000000001"
	seedChannel(t, pool, channelID)
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	activeAuthority(t, store)

	ev := notifyEvent("np-fence", eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)
	if _, err := store.ApplyEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}

	// Worker A 认领。
	tasksA, err := store.ClaimNotifyTasks(ctx, 10, time.Minute)
	if err != nil || len(tasksA) != 1 {
		t.Fatalf("claim A: %d tasks err=%v", len(tasksA), err)
	}
	tokenA := tasksA[0].LeaseToken
	if tokenA == "" {
		t.Fatal("claim did not return a lease token (fencing impossible)")
	}

	// 租约过期，Worker B 重领 —— B 成为当前所有者。
	if _, err := pool.Exec(ctx, `UPDATE notification_outbox SET lease_until = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	tasksB, err := store.ClaimNotifyTasks(ctx, 10, time.Minute)
	if err != nil || len(tasksB) != 1 {
		t.Fatalf("claim B: %d tasks err=%v", len(tasksB), err)
	}
	tokenB := tasksB[0].LeaseToken
	if tokenB == "" || tokenB == tokenA {
		t.Fatalf("lease tokens must differ per claim: A=%q B=%q", tokenA, tokenB)
	}

	// Worker A 的迟到完成：不得改变状态，但必须留下审计行。
	if err := store.FinishNotifyTask(ctx, tasksA[0].ID, tokenA, true, 0, tasksA[0].Attempts, 200, "", "", 15); err != notify.ErrStaleNotifyLease {
		t.Fatalf("stale finish err = %v, want ErrStaleNotifyLease", err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM notification_outbox WHERE id=$1`, tasksA[0].ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "sending" {
		t.Fatalf("status after stale finish = %q, want 'sending' (owner B untouched)", status)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM notification_attempts WHERE task_id=$1`, tasksA[0].ID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("late attempt audit rows = %d, want 1 (迟到行为有审计)", attempts)
	}

	// Worker B（当前所有者）正常完成。
	if err := store.FinishNotifyTask(ctx, tasksB[0].ID, tokenB, true, 0, tasksB[0].Attempts, 200, "", "", 15); err != nil {
		t.Fatalf("owner finish: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM notification_outbox WHERE id=$1`, tasksB[0].ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "sent" {
		t.Fatalf("status after owner finish = %q, want 'sent'", status)
	}
}

// P8-P1-04（真实 PG）：发送前续租的 fencing ——
// 一个批次里续租过的任务不会被第二个 Worker 重领（避免慢渠道下重复外发）；
// 未续租/过期任务可被重领，旧 token 的续租返回 ErrStaleNotifyLease。
func TestRenewNotifyLeaseProtectsFromReclaim(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	const channelID = "abab0000-0000-0000-0000-000000000001"
	seedChannel(t, pool, channelID)
	activeAuthority(t, store)
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	for _, name := range []string{"RenewA", "RenewB"} {
		ev := notifyEvent("np-renew-"+name, eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)
		ev.AlertName = name
		ev.EventKey = "ek-" + name
		ev.Source.ExternalRuleId = "rule-" + name
		if _, err := store.ApplyEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}

	// Worker A 认领两条（短租约），并像 deliverOne 那样在执行前续租第一条。
	tasks, err := store.ClaimNotifyTasks(ctx, 10, time.Second)
	if err != nil || len(tasks) != 2 {
		t.Fatalf("claim A: %d tasks err=%v", len(tasks), err)
	}
	renewed, expiring := tasks[0], tasks[1]
	if err := store.RenewNotifyLease(ctx, renewed.ID, renewed.LeaseToken, time.Minute); err != nil {
		t.Fatalf("renew: %v", err)
	}
	// 模拟第二条租约已过期（不等待）。
	if _, err := pool.Exec(ctx, `
		UPDATE notification_outbox SET lease_until = now() - interval '1 second' WHERE id=$1`, expiring.ID); err != nil {
		t.Fatal(err)
	}

	// Worker B 重领：只能拿到未续租的那条。
	again, err := store.ClaimNotifyTasks(ctx, 10, time.Minute)
	if err != nil || len(again) != 1 || again[0].ID != expiring.ID {
		t.Fatalf("reclaim = %+v (err=%v), want only the expired task %s", again, err, expiring.ID)
	}

	// 旧 token 的续租被 fencing（任务已被 B 重领）。
	if err := store.RenewNotifyLease(ctx, expiring.ID, expiring.LeaseToken, time.Minute); err != notify.ErrStaleNotifyLease {
		t.Fatalf("stale renew err = %v, want ErrStaleNotifyLease", err)
	}
	// 已续租任务仍归 A：B 的再次认领不会包含它。
	third, err := store.ClaimNotifyTasks(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, tk := range third {
		if tk.ID == renewed.ID {
			t.Fatalf("renewed task %s was re-claimed (double-page risk)", renewed.ID)
		}
	}
}

// P8-REG-01 失败测试：同一周期重复 Firing（稳定 observation key，如
// Alertmanager amObservationKey）—— Receipt 去重会跳过状态机输入，
// 但不得冻结 last_observed_at：修复前重复 Firing 只写到 receipt 即返回，
// 持续 10 分钟 firing 的告警观察时间停留在首次，可能被 stale 对账误判。
// 同时必须零副作用：不重复创建通知、不重复写事件历史。
func TestRepeatFiringRefreshesObservation(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	const channelID = "abab0000-0000-0000-0000-000000000001"
	seedChannel(t, pool, channelID)
	activeAuthority(t, store)
	now := time.Now().UTC()

	ev1 := notifyEvent("reg-1", eventv1.EventSignal_EVENT_SIGNAL_FIRING, now)
	ev1.EventKey = "ek-shared-cycle" // 一个告警周期内稳定的观察键
	if _, err := store.ApplyEvent(ctx, ev1); err != nil {
		t.Fatal(err)
	}
	// 模拟该周期 10 分钟前已被首次观察（旧代码下 repeat firing 不会刷新）。
	if _, err := pool.Exec(ctx, `
		UPDATE alert_instances SET last_observed_at = now() - interval '10 minutes'`); err != nil {
		t.Fatal(err)
	}

	before := time.Now()
	ev2 := notifyEvent("reg-2", eventv1.EventSignal_EVENT_SIGNAL_FIRING, now.Add(time.Minute))
	ev2.EventKey = "ek-shared-cycle" // 同 observation key，新 transport id（重复投递）
	ev2.ReceivedAt = timestamppb.New(time.Now())
	dup, err := store.ApplyEvent(ctx, ev2)
	if err != nil {
		t.Fatalf("repeat firing: %v", err)
	}
	if !dup {
		t.Fatal("repeat firing with the same observation key must be deduped")
	}

	var observed time.Time
	if err := pool.QueryRow(ctx, `SELECT last_observed_at FROM alert_instances`).Scan(&observed); err != nil {
		t.Fatal(err)
	}
	if observed.Before(before.Add(-time.Second)) {
		t.Fatalf("last_observed_at = %s, want a fresh timestamp (>= %s) — liveness froze on repeat firing",
			observed.Format(time.RFC3339), before.Format(time.RFC3339))
	}

	var notifications, history int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM notification_outbox`).Scan(&notifications); err != nil {
		t.Fatal(err)
	}
	if notifications != 1 {
		t.Fatalf("notification rows = %d, want 1 (repeat firing must not page again)", notifications)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_history`).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if history != 1 {
		t.Fatalf("event history rows = %d, want 1 (receipt dedup keeps history unique)", history)
	}
}

// AUD-002（#11，真实 PG）：通知终态出口 —— max attempts / ssrf / 禁用渠道
// 僵尸行 / 人工重放。
func TestNotifyTerminalStates(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	const channelID = "abab0000-0000-0000-0000-000000000001"
	seedChannel(t, pool, channelID)
	activeAuthority(t, store)
	at := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)

	enqueue := func(name string) string {
		t.Helper()
		ev := notifyEvent("nt-"+name, eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)
		ev.AlertName = name
		ev.EventKey = "ek-" + name
		ev.Source.ExternalRuleId = "rule-" + name
		if _, err := store.ApplyEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
		var id string
		if err := pool.QueryRow(ctx, `
			SELECT o.id::text FROM notification_outbox o
			JOIN alert_instances i ON i.id=o.alert_instance_id
			WHERE i.name=$1`, name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	claim := func() notify.Task {
		t.Helper()
		tasks, err := store.ClaimNotifyTasks(ctx, 10, time.Minute)
		if err != nil || len(tasks) != 1 {
			t.Fatalf("claim: %d err=%v", len(tasks), err)
		}
		return tasks[0]
	}
	statusOf := func(id string) string {
		t.Helper()
		var st string
		if err := pool.QueryRow(ctx, `SELECT status FROM notification_outbox WHERE id=$1`, id).Scan(&st); err != nil {
			t.Fatal(err)
		}
		return st
	}

	// ① max attempts → failed（不再无限重试）
	id := enqueue("MaxAttempts")
	task := claim()
	if err := store.FinishNotifyTask(ctx, task.ID, task.LeaseToken, false, 0, 15, 500, "retryable", "still failing", 15); err != nil {
		t.Fatal(err)
	}
	if st := statusOf(id); st != "failed" {
		t.Fatalf("max-attempts status = %s, want failed", st)
	}

	// ② ssrf 失败码 → 终态（旧实现只认 permanent，SSRF 无限重试）
	id = enqueue("SSRF")
	task = claim()
	if err := store.FinishNotifyTask(ctx, task.ID, task.LeaseToken, false, 0, 1, 0, "ssrf", "forbidden address", 15); err != nil {
		t.Fatal(err)
	}
	if st := statusOf(id); st != "failed" {
		t.Fatalf("ssrf status = %s, want failed", st)
	}

	// ③ 禁用渠道的僵尸 pending 行 → 死信（sweep + gauge 一致）
	id = enqueue("Orphan")
	if _, err := pool.Exec(ctx, `UPDATE notification_channels SET enabled=false WHERE id=$1`, channelID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE notification_outbox SET updated_at=now()-interval '2 minutes' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	n, err := store.FailOrphanNotifyTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("orphan sweep = %d, want 1", n)
	}
	if st := statusOf(id); st != "failed" {
		t.Fatalf("orphan status = %s, want failed", st)
	}
	if pending, err := store.CountPendingNotifyTasks(ctx); err != nil || pending != 0 {
		t.Fatalf("pending gauge = %d err=%v, want 0 (disabled channel excluded)", pending, err)
	}

	// ④ 人工重放：failed → pending（attempts 归零），二次重放返回 not-found
	replayed, err := store.ReplayNotifyTask(ctx, notifyTenant, id)
	if err != nil || !replayed {
		t.Fatalf("replay = %v err=%v", replayed, err)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT attempts, status FROM notification_outbox WHERE id=$1`, id).Scan(&attempts, new(string)); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || statusOf(id) != "pending" {
		t.Fatalf("after replay attempts=%d status=%s", attempts, statusOf(id))
	}
	if replayed, _ := store.ReplayNotifyTask(ctx, notifyTenant, id); replayed {
		t.Fatal("replaying a pending task must report not-found")
	}
}

// AUD-012（#21，真实 PG）：shadow trail 覆盖切换前的 active 期积压；
// RecordShadowDelivery 由唯一索引保证幂等（双副本并发不重复）。
func TestShadowTrailAndIdempotentRecord(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	const channelID = "abab0000-0000-0000-0000-000000000001"
	seedChannel(t, pool, channelID)
	activeAuthority(t, store)
	at := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)

	ev := notifyEvent("st-1", eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)
	if _, err := store.ApplyEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	// Active→shadow switch: the pre-switch backlog must still appear in the trail.
	if _, err := store.SyncNotifyAuthority(ctx, notify.ModeShadow); err != nil {
		t.Fatal(err)
	}
	tasks, err := store.PeekNotifyTasks(ctx, 10)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("peek = %d err=%v, want the pre-switch backlog", len(tasks), err)
	}

	first, err := store.RecordShadowDelivery(ctx, tasks[0].ID)
	if err != nil || !first {
		t.Fatalf("first record = %v err=%v", first, err)
	}
	second, err := store.RecordShadowDelivery(ctx, tasks[0].ID)
	if err != nil || second {
		t.Fatalf("duplicate record = %v err=%v (unique index must dedup)", second, err)
	}
}
