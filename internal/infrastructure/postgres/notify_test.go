package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

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
	auth := activeAuthority(t, store)

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
	tasks, err := store.ClaimNotifyTasks(ctx, 10, time.Minute, auth.Generation)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v (%d)", err, len(tasks))
	}
	if tasks[0].ID != taskID || tasks[0].Attempts != 1 {
		t.Fatalf("claimed task: %+v", tasks[0])
	}
	// A second claim while the lease is live finds nothing (no double send).
	again, err := store.ClaimNotifyTasks(ctx, 10, time.Minute, auth.Generation)
	if err != nil || len(again) != 0 {
		t.Fatalf("double claim leaked tasks: %v (%d)", err, len(again))
	}

	// 3) simulated sender crash: lease expires, task re-claims with attempts=2.
	if _, err := pool.Exec(ctx, `
		UPDATE notification_outbox SET lease_until = now() - interval '2 hours' WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := store.ClaimNotifyTasks(ctx, 10, time.Minute, auth.Generation)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("lease-expiry reclaim: %v (%d)", err, len(reclaimed))
	}
	if reclaimed[0].Attempts != 2 {
		t.Fatalf("attempts after reclaim = %d, want 2", reclaimed[0].Attempts)
	}

	// 4) successful delivery records the attempt row and finalizes the task.
	err = store.FinishNotifyTask(ctx, taskID, reclaimed[0].LeaseToken, true, 0, reclaimed[0].Attempts, 200, "", "")
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
	if err := store.FinishNotifyTask(ctx, task2, "", false, 42*time.Second, 1, 429, "retryable", "rate limited"); err != nil {
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
	if err := store.FinishNotifyTask(ctx, task2, "", false, 0, 2, 404, "permanent", "gone"); err != nil {
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

// P8-P0-01：真实 PG 上的 generation 隔离 ——
// ① 首次初始化强制开启新 generation（迁移种子 gen1 → sync 后 gen2），
//
//	迁移前（gen0）任务永不被领取（保守：不冒险补发 shadow 历史）；
//
// ② 每次模式切换 generation+1，旧 generation 的 pending 任务不再可领；
// ③ 任务入队时 stamp 当前 generation（enqueue 与 authority 同源）。
func TestClaimNotifyTasksGenerationIsolation(t *testing.T) {
	store, pool := newNotifyTest(t)
	ctx := context.Background()
	const channelID = "abab0000-0000-0000-0000-000000000001"
	seedChannel(t, pool, channelID)
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	mkTask := func(t *testing.T, name string) {
		t.Helper()
		ev := notifyEvent("np-gen-"+name, eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)
		ev.AlertName = name
		ev.EventKey = "ek-" + name
		ev.Source.ExternalRuleId = "rule-" + name
		if _, err := store.ApplyEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	generationOf := func(t *testing.T, name string) int64 {
		t.Helper()
		var gen int64
		if err := pool.QueryRow(ctx, `
			SELECT o.authority_generation FROM notification_outbox o
			JOIN alert_instances i ON i.id = o.alert_instance_id
			WHERE i.name = $1`, name).Scan(&gen); err != nil {
			t.Fatalf("generation of %s: %v", name, err)
		}
		return gen
	}
	reset := func(t *testing.T) {
		t.Helper()
		if _, err := pool.Exec(ctx, `UPDATE notification_outbox SET status='pending', lease_until=NULL, lease_token=NULL`); err != nil {
			t.Fatal(err)
		}
	}

	// ① 迁移后、首次 sync 前入队：任务 stamp 的是种子 generation 1。
	mkTask(t, "PreInit")
	if gen := generationOf(t, "PreInit"); gen != 1 {
		t.Fatalf("pre-init task generation = %d, want 1 (migration seed)", gen)
	}

	// 首次初始化（active）：强制新 generation（2），且不收养 pre-authority 任务。
	auth := activeAuthority(t, store)
	if auth.Generation != 2 {
		t.Fatalf("first-init generation = %d, want 2 (never adopt pre-authority rows)", auth.Generation)
	}
	tasks, err := store.ClaimNotifyTasks(ctx, 10, time.Minute, auth.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 0 {
		t.Fatalf("first-init claimed %d pre-authority tasks, want 0 (no history replay)", len(tasks))
	}

	// ② 初始化后入队：stamp 当前 generation（2），可领取。
	mkTask(t, "Gen2")
	if gen := generationOf(t, "Gen2"); gen != 2 {
		t.Fatalf("post-init task generation = %d, want 2", gen)
	}
	tasks, err = store.ClaimNotifyTasks(ctx, 10, time.Minute, auth.Generation)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("gen2 claim: %d tasks err=%v", len(tasks), err)
	}
	reset(t)

	// ③ 切 shadow（gen3）再切回 active（gen4）：gen2/gen3 时代任务永不补发。
	if _, err := store.SyncNotifyAuthority(ctx, notify.ModeShadow); err != nil {
		t.Fatal(err)
	}
	auth3, err := store.SyncNotifyAuthority(ctx, notify.ModeActive)
	if err != nil {
		t.Fatal(err)
	}
	if auth3.Generation != 4 {
		t.Fatalf("generation after two switches = %d, want 4", auth3.Generation)
	}
	tasks, err = store.ClaimNotifyTasks(ctx, 10, time.Minute, auth3.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 0 {
		t.Fatalf("active-after-shadow claimed %d old-era tasks, want 0 (storm guard)", len(tasks))
	}

	// 新 era 的任务正常领取。
	mkTask(t, "Gen4")
	if gen := generationOf(t, "Gen4"); gen != 4 {
		t.Fatalf("post-switch task generation = %d, want 4", gen)
	}
	tasks, err = store.ClaimNotifyTasks(ctx, 10, time.Minute, auth3.Generation)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("gen4 claim: %d tasks err=%v", len(tasks), err)
	}

	// 幂等：同 mode 重复 sync 不再 bump（多副本 worker 安全）。
	same, err := store.SyncNotifyAuthority(ctx, notify.ModeActive)
	if err != nil {
		t.Fatal(err)
	}
	if same.Generation != 4 {
		t.Fatalf("same-mode sync bumped generation to %d, want 4", same.Generation)
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
	auth := activeAuthority(t, store)

	ev := notifyEvent("np-fence", eventv1.EventSignal_EVENT_SIGNAL_FIRING, at)
	if _, err := store.ApplyEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}

	// Worker A 认领。
	tasksA, err := store.ClaimNotifyTasks(ctx, 10, time.Minute, auth.Generation)
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
	tasksB, err := store.ClaimNotifyTasks(ctx, 10, time.Minute, auth.Generation)
	if err != nil || len(tasksB) != 1 {
		t.Fatalf("claim B: %d tasks err=%v", len(tasksB), err)
	}
	tokenB := tasksB[0].LeaseToken
	if tokenB == "" || tokenB == tokenA {
		t.Fatalf("lease tokens must differ per claim: A=%q B=%q", tokenA, tokenB)
	}

	// Worker A 的迟到完成：不得改变状态，但必须留下审计行。
	if err := store.FinishNotifyTask(ctx, tasksA[0].ID, tokenA, true, 0, tasksA[0].Attempts, 200, "", ""); err != notify.ErrStaleNotifyLease {
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
	if err := store.FinishNotifyTask(ctx, tasksB[0].ID, tokenB, true, 0, tasksB[0].Attempts, 200, "", ""); err != nil {
		t.Fatalf("owner finish: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM notification_outbox WHERE id=$1`, tasksB[0].ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "sent" {
		t.Fatalf("status after owner finish = %q, want 'sent'", status)
	}
}
