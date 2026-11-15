package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/application/query"
	"github.com/example/alert-platform/internal/infrastructure/testenv"
)

// PROC-004/005/008 integration: the transactional store against a disposable
// PG. Each test drives canonical events through ApplyEvent and asserts the
// exact invariant named by its acceptance line.

const (
	tenantA = "bbbb0000-0000-0000-0000-000000000001"
	tenantB = "bbbb0000-0000-0000-0000-000000000002"
	srcDF   = "cccc0000-0000-0000-0000-000000000001"
)

func newStoreTest(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	dsn := testenv.StartPostgres(t)
	if _, err := Up(dsn, absMigrations(t)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return NewStore(pool), pool
}

// canonical builds a well-formed event; deltas mutate the copy.
func canonical(transportID string, at time.Time) *eventv1.AlertEvent {
	return &eventv1.AlertEvent{
		EventId:     transportID,
		TransportId: transportID,
		TenantId:    tenantA,
		Source: &eventv1.SourceRef{
			SourceId: srcDF, SourceType: eventv1.SourceType_SOURCE_TYPE_DEEPFLOW,
			ExternalRuleId: "rule-1",
		},
		Resource: &eventv1.ResourceRef{
			ClusterId: "c1", ResourceType: "pod", ResourceUid: "pod-1",
		},
		Kind:       eventv1.EventKind_EVENT_KIND_STATEFUL,
		Signal:     eventv1.EventSignal_EVENT_SIGNAL_FIRING,
		Severity:   eventv1.Severity_SEVERITY_CRITICAL,
		AlertName:  "LatencyHigh",
		Labels:     map[string]string{"ns": "demo"},
		OccurredAt: timestamppb.New(at),
		ReceivedAt: timestamppb.New(at.Add(time.Second)),
		StartsAt:   timestamppb.New(at),
		EventKey:   "ek-" + transportID,
	}
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

// T-005: 1000 same-source FIRING redeliveries produce exactly ONE transition.
func TestThousandFiringsOneTransition(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 1000; i++ {
		ev := canonical("tp-firing", base)
		ev.TransportId = "tp-" + itoa(i)
		ev.EventKey = "ek-" + itoa(i) // distinct deliveries of the SAME alert
		ev.EventId = "evt-" + itoa(i)
		if _, err := store.ApplyEvent(ctx, ev); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	if n := count(t, pool, `SELECT count(*) FROM alert_instances`); n != 1 {
		t.Fatalf("instances = %d, want 1", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM alert_transitions`); n != 1 {
		t.Fatalf("transitions = %d, want 1 (T-005)", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM domain_outbox`); n != 1 {
		t.Fatalf("domain_outbox = %d, want 1", n)
	}
	// Every delivery is a distinct transport occurrence: 1000 receipts, but
	// the state machine collapsed them into one firing episode.
	if n := count(t, pool, `SELECT count(*) FROM event_receipts`); n != 1000 {
		t.Fatalf("receipts = %d, want 1000", n)
	}
}

// T-007: business commit succeeded, offset commit failed -> the same message
// replays -> no duplicate transition and no duplicate domain event.
func TestOffsetFailureReplayIsIdempotent(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	fire := canonical("tp-e1", at)
	if _, err := store.ApplyEvent(ctx, fire); err != nil {
		t.Fatal(err)
	}
	resolve := canonical("tp-e2", at.Add(10*time.Minute))
	resolve.Signal = eventv1.EventSignal_EVENT_SIGNAL_RESOLVED
	resolve.Severity = eventv1.Severity_SEVERITY_UNSPECIFIED
	resolve.EndsAt = timestamppb.New(at.Add(10 * time.Minute))
	if _, err := store.ApplyEvent(ctx, resolve); err != nil {
		t.Fatal(err)
	}

	// Offset commit failed both times: replay both messages verbatim.
	for _, ev := range []*eventv1.AlertEvent{fire, resolve} {
		dup, err := store.ApplyEvent(ctx, ev)
		if err != nil {
			t.Fatalf("replay %s: %v", ev.GetTransportId(), err)
		}
		if !dup {
			t.Fatalf("replay %s must be flagged duplicate", ev.GetTransportId())
		}
	}
	if n := count(t, pool, `SELECT count(*) FROM alert_transitions`); n != 2 {
		t.Fatalf("transitions after replay = %d, want 2", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM domain_outbox`); n != 2 {
		t.Fatalf("domain events after replay = %d, want 2 (no duplicate notification effects)", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM event_receipts`); n != 2 {
		t.Fatalf("receipts = %d, want 2", n)
	}
}

// T-006: an OLDER firing cannot reopen a closed episode; a NEWER firing
// opens a NEW episode (docs/03 §2 episode_key).
func TestOutOfOrderFiringAndNewEpisode(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	t0 := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	fire := canonical("tp-a1", t0)
	mustApply(t, store, ctx, fire)
	res := canonical("tp-a2", t0.Add(10*time.Minute))
	res.Signal, res.Severity = eventv1.EventSignal_EVENT_SIGNAL_RESOLVED, eventv1.Severity_SEVERITY_UNSPECIFIED
	mustApply(t, store, ctx, res)

	// Older firing: suppressed, no rollback, no new episode.
	old := canonical("tp-a3", t0.Add(5*time.Minute))
	mustApply(t, store, ctx, old)
	var state string
	if err := pool.QueryRow(ctx, `SELECT monitor_state FROM alert_instances`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "resolved" {
		t.Fatalf("older firing rolled state back to %q (T-006: no wrong rollback)", state)
	}

	// Newer firing: a NEW episode (second instance row).
	fresh := canonical("tp-a4", t0.Add(20*time.Minute))
	mustApply(t, store, ctx, fresh)
	if n := count(t, pool, `SELECT count(*) FROM alert_instances`); n != 2 {
		t.Fatalf("instances = %d, want 2 (new episode)", n)
	}
	if n := count(t, pool, `SELECT count(DISTINCT episode_key) FROM alert_instances`); n != 2 {
		t.Fatalf("episodes = %d, want 2", n)
	}
	var active int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM alert_instances WHERE monitor_state='firing'`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("active instances = %d, want 1", active)
	}
}

// PROC-006 semantics at the store level: NoData never resolves; occurrence
// events record receipts only.
func TestNoDataAndOccurrenceSemantics(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	mustApply(t, store, ctx, canonical("tp-n1", at))
	nd := canonical("tp-n2", at.Add(time.Minute))
	nd.Signal = eventv1.EventSignal_EVENT_SIGNAL_NO_DATA
	nd.Severity = eventv1.Severity_SEVERITY_UNSPECIFIED
	mustApply(t, store, ctx, nd)

	var state string
	if err := pool.QueryRow(ctx, `SELECT monitor_state FROM alert_instances`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "firing" {
		t.Fatalf("NoData changed state to %q — must never auto-resolve", state)
	}

	occ := canonical("tp-n3", at.Add(2*time.Minute))
	occ.Kind = eventv1.EventKind_EVENT_KIND_OCCURRENCE
	occ.Signal = eventv1.EventSignal_EVENT_SIGNAL_OCCURRED
	mustApply(t, store, ctx, occ)
	if n := count(t, pool, `SELECT count(*) FROM alert_instances`); n != 1 {
		t.Fatalf("occurrence created instances: %d", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM event_receipts`); n != 3 {
		t.Fatalf("receipts = %d, want 3", n)
	}
}

// PROC-003: half-write prevention. Forcing the transition insert to fail
// (unique violation) rolls back receipt AND outbox — all or nothing.
func TestNoPartialWriteOnFailure(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	fire := canonical("tp-h1", at)
	fire.EventId = "evt-h"
	mustApply(t, store, ctx, fire)

	// Plant a transition row colliding on (instance, event_id, to_state) with
	// the transition the NEXT resolved event will try to insert.
	var instanceID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM alert_instances`).Scan(&instanceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO alert_transitions (transition_id, tenant_id, alert_instance_id, from_state, to_state, event_id, transition_at)
		VALUES (gen_random_uuid(), $1, $2, 'firing', 'resolved', 'evt-h', now())`, tenantA, instanceID); err != nil {
		t.Fatal(err)
	}

	res := canonical("tp-h2", at.Add(time.Minute))
	res.Signal, res.Severity = eventv1.EventSignal_EVENT_SIGNAL_RESOLVED, eventv1.Severity_SEVERITY_UNSPECIFIED
	res.EventId = "evt-h"
	if _, err := store.ApplyEvent(ctx, res); err == nil {
		t.Fatal("expected failure from planted constraint collision")
	}
	// All-or-nothing: the failed event left NO receipt, NO domain event, and
	// the instance is still firing.
	if n := count(t, pool, `SELECT count(*) FROM event_receipts WHERE transport_id='tp-h2'`); n != 0 {
		t.Fatalf("failed tx leaked a receipt: %d", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM domain_outbox`); n != 1 {
		t.Fatalf("failed tx leaked domain events: %d, want 1 (from tp-h1 only)", n)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT monitor_state FROM alert_instances`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "firing" {
		t.Fatalf("failed tx changed state to %q", state)
	}
}

// PROC-006: silent sources degrade active instances to stale; recovery of the
// source health does NOT touch instances; a fresh firing re-confirms.
func TestStaleReconciliation(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	now := time.Now()
	at := now.Add(-2 * time.Hour) // event times comfortably in the past

	mustApply(t, store, ctx, canonical("tp-s1", at))
	// Seed the source with an old last_seen (1h ago, past the 5m threshold).
	seedSource(t, pool, srcDF, tenantA, now.Add(-time.Hour))

	stale, err := store.StaleSources(ctx, now.Add(-5*time.Minute))
	if err != nil || len(stale) != 1 {
		t.Fatalf("StaleSources: %v (%d)", err, len(stale))
	}
	n, err := store.MarkStaleBySources(ctx, tenantA, []string{stale[0].SourceID}, "test-silent")
	if err != nil || n != 1 {
		t.Fatalf("MarkStaleBySources: %v (%d)", err, n)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT monitor_state FROM alert_instances`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "stale" {
		t.Fatalf("state = %q, want stale (silent source never resolves)", state)
	}
	if err := store.MarkSourceHealth(ctx, tenantA, stale[0].SourceID, "stale"); err != nil {
		t.Fatal(err)
	}

	// Source pushes again: health recovers, instance STAYS stale until
	// re-confirmed by a real event.
	if err := NewSourceStore(pool).TouchLastSeen(ctx, srcDF); err != nil {
		t.Fatal(err)
	}
	rec, err := store.RecoveredSources(ctx, now.Add(-5*time.Minute))
	if err != nil || len(rec) != 1 {
		t.Fatalf("RecoveredSources: %v (%d)", err, len(rec))
	}
	if err := pool.QueryRow(ctx, `SELECT monitor_state FROM alert_instances`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "stale" {
		t.Fatalf("recovery must not change instance state, got %q", state)
	}

	// Fresh firing re-confirms: stale -> firing (docs/03 §4).
	reconfirm := canonical("tp-s2", now)
	mustApply(t, store, ctx, reconfirm)
	if err := pool.QueryRow(ctx, `SELECT monitor_state FROM alert_instances`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "firing" {
		t.Fatalf("stale->firing re-confirmation failed, state=%q", state)
	}
}

// PROC-003 relay loop at the store level: claim -> publish marker -> gone;
// visibility requeue resurrects claims after the timeout.
func TestOutboxClaimPublishRequeue(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	mustApply(t, store, ctx, canonical("tp-o1", at))

	rows, err := store.ClaimOutboxRows(ctx, 10, time.Minute)
	if err != nil || len(rows) != 1 {
		t.Fatalf("claim: %v (%d)", err, len(rows))
	}
	again, err := store.ClaimOutboxRows(ctx, 10, time.Minute)
	if err != nil || len(again) != 0 {
		t.Fatalf("double claim leaked rows: %v (%d)", err, len(again))
	}
	if err := store.MarkOutboxPublished(ctx, rows[0].ID); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM domain_outbox`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "published" {
		t.Fatalf("status = %q, want published", status)
	}

	// Visibility requeue: a crashed relay's stale claim goes back to pending.
	mustApply(t, store, ctx, func() *eventv1.AlertEvent {
		ev := canonical("tp-o2", at.Add(time.Minute))
		ev.Source.ExternalRuleId = "rule-2"
		return ev
	}())
	claimed, err := store.ClaimOutboxRows(ctx, 10, time.Hour) // huge timeout
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim2: %v (%d)", err, len(claimed))
	}
	// Backdate the claim past the timeout, then claim again with a short one.
	if _, err := pool.Exec(ctx, `
		UPDATE domain_outbox SET next_attempt_at = now() - interval '2 hours' WHERE id=$1`, claimed[0].ID); err != nil {
		t.Fatal(err)
	}
	requeued, err := store.ClaimOutboxRows(ctx, 10, time.Minute)
	if err != nil || len(requeued) != 1 {
		t.Fatalf("visibility requeue failed: %v (%d)", err, len(requeued))
	}
}

// PROC-007 read side: filters, pagination, timeline, tenant isolation.
func TestQueryStoreTenantIsolationAndFilters(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	a1 := canonical("tp-q1", at)
	mustApply(t, store, ctx, a1)
	b1 := canonical("tp-q2", at)
	b1.TenantId = tenantB
	b1.Source.ExternalRuleId = "rule-b"
	mustApply(t, store, ctx, b1)

	q := NewQueryStore(pool)
	svc := query.NewService(q)

	page, err := svc.List(ctx, query.AlertFilter{TenantID: tenantA})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("list A: %v (%d)", err, len(page.Items))
	}
	idA := page.Items[0].InstanceID

	// Cross-tenant detail: Forbidden, never a silent 404 (FR-009).
	got, err := svc.Get(ctx, tenantA, idA)
	if err != nil || got.TenantID != tenantA {
		t.Fatalf("get own: %v", err)
	}
	if _, err := svc.Get(ctx, tenantB, idA); err != query.ErrForbidden {
		t.Fatalf("cross-tenant get: %v, want ErrForbidden", err)
	}

	// Filters: severity + state.
	if _, err := svc.List(ctx, query.AlertFilter{TenantID: tenantA, Severity: "SEVERITY_CRITICAL"}); err != nil {
		t.Fatalf("severity filter: %v", err)
	}
	if _, err := svc.List(ctx, query.AlertFilter{TenantID: tenantA, State: "resolved"}); err != nil {
		t.Fatalf("state filter: %v", err)
	}
	if _, err := svc.List(ctx, query.AlertFilter{TenantID: tenantA, State: "bogus"}); err == nil {
		t.Fatal("invalid state filter accepted")
	}

	// Timeline returns the firing transition for tenant A's instance.
	tl, err := svc.Timeline(ctx, tenantA, idA, 10, "")
	if err != nil || len(tl.Items) != 1 {
		t.Fatalf("timeline: %v (%d)", err, len(tl.Items))
	}
	if tl.Items[0].ToState != "firing" || tl.Items[0].FromState != "" {
		t.Fatalf("timeline entry: %+v", tl.Items[0])
	}
	// Timeline for tenant B on A's instance: forbidden.
	if _, err := svc.Timeline(ctx, tenantB, idA, 10, ""); err != query.ErrForbidden {
		t.Fatalf("cross-tenant timeline: %v", err)
	}
}

func mustApply(t *testing.T, store *Store, ctx context.Context, ev *eventv1.AlertEvent) {
	t.Helper()
	if _, err := store.ApplyEvent(ctx, ev); err != nil {
		t.Fatalf("apply %s: %v", ev.GetTransportId(), err)
	}
}

func seedSource(t *testing.T, pool *pgxpool.Pool, sourceID, tenantID string, lastSeen time.Time) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO alert_sources (tenant_id, source_id, name, source_type, vendor_version, adapter_version, cluster_id, last_seen_at, health_status)
		VALUES ($1, $2, 'df-test', 'deepflow', '7.2.3', 'x', 'c1', $3, 'ok')
		ON CONFLICT (tenant_id, source_id) DO UPDATE SET last_seen_at=$3`,
		tenantID, sourceID, lastSeen)
	if err != nil {
		t.Fatal(err)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// P8-P1-05 失败测试：同一 Fingerprint 的并发 Firing ——
// 8 个不同 Transport ID 同时进入，ReadCommitted 下双方都可能看不到活跃实例，
// 各自生成 Episode ID 插入（(tenant,fingerprint,episode_key) 唯一约束拦不住）。
// 修复后：事务级 advisory lock 串行化同一身份，活跃 episode 恰好 1 个；
// 部分唯一索引作为数据库兜底，直接插入第二个活跃行必须被拒绝。
func TestConcurrentFiringSameFingerprintSingleEpisode(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	const workers = 8
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ev := canonical(fmt.Sprintf("race-%d", i), at)
			if _, err := store.ApplyEvent(ctx, ev); err != nil {
				errs <- err
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent apply: %v", err)
	}

	var active, episodes int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(DISTINCT episode_key) FROM alert_instances
		WHERE monitor_state IN ('pending','firing','stale')`).Scan(&active, &episodes); err != nil {
		t.Fatal(err)
	}
	if active != 1 || episodes != 1 {
		t.Fatalf("active instances=%d episodes=%d, want 1/1 (concurrent firing opened duplicate episodes)", active, episodes)
	}

	// 数据库兜底：直接插入第二个活跃行（新 episode_key）必须被部分唯一索引拒绝。
	var fingerprint, tenant, source string
	if err := pool.QueryRow(ctx, `
		SELECT fingerprint, tenant_id::text, source_id::text FROM alert_instances LIMIT 1`).
		Scan(&fingerprint, &tenant, &source); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO alert_instances
			(id, tenant_id, source_id, fingerprint, episode_key, name, severity,
			 monitor_state, cluster_id, entity_type, entity_uid, labels,
			 starts_at, ends_at, last_observed_at, last_transition_at)
		VALUES (gen_random_uuid(), $1, $2, $3, gen_random_uuid()::text, 'Dup', 'critical',
		        'firing', 'c1', 'pod', 'pod-1', '{}', now(), NULL, now(), now())`,
		tenant, source, fingerprint)
	if err == nil {
		t.Fatal("second ACTIVE row for the same fingerprint was accepted (partial unique index missing)")
	}
}
