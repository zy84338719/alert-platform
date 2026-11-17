package postgres

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// P10-001/002（真实 PG）：事务内归档队列 ——
// ① 每条 transition（含迟到/乱序事件）在插入事务里入队；
// ② Claim 按事件时间升序领取并递增 attempts；
// ③ 单写者租约互斥；MarkArchived/Failed/Exhausted 状态机。
func TestArchiveQueueClaimAndLease(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	as := NewArchiveStore(store.pool)
	at := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)

	// firing -> resolved：两条 transition 入队。
	if _, err := store.ApplyEvent(ctx, canonical("aq-1", at)); err != nil {
		t.Fatal(err)
	}
	resolved := canonical("aq-2", at.Add(time.Minute))
	resolved.Signal = eventv1.EventSignal_EVENT_SIGNAL_RESOLVED
	if _, err := store.ApplyEvent(ctx, resolved); err != nil {
		t.Fatal(err)
	}
	// LATE arrival (P10-001): an event that happened 2h BEFORE what we already
	// archived. The queue row is created at insert time, so it must be claimed.
	late := canonical("aq-late", at.Add(-2*time.Hour))
	late.Source.ExternalRuleId = "rule-late" // distinct identity -> new instance + transition
	late.OccurredAt = timestamppb.New(at.Add(-2 * time.Hour))
	late.ReceivedAt = timestamppb.New(at)
	if _, err := store.ApplyEvent(ctx, late); err != nil {
		t.Fatal(err)
	}

	var queued int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM transition_archive_queue WHERE status='pending'`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 3 {
		t.Fatalf("queued pending = %d, want 3 (including the late row)", queued)
	}

	// Single-writer lease (owner is a UUID column, as the worker generates).
	const ownerA, ownerB = "aaaaaaaa-0000-0000-0000-000000000001", "bbbbbbbb-0000-0000-0000-000000000002"
	if ok, err := as.AcquireLease(ctx, ownerA, time.Minute); err != nil || !ok {
		t.Fatalf("owner-a acquire: ok=%v err=%v", ok, err)
	}
	if ok, err := as.AcquireLease(ctx, ownerB, time.Minute); err != nil || ok {
		t.Fatalf("owner-b must be refused while a holds the lease: ok=%v err=%v", ok, err)
	}
	if err := as.ReleaseLease(ctx, ownerA); err != nil {
		t.Fatal(err)
	}
	if ok, err := as.AcquireLease(ctx, ownerB, time.Minute); err != nil || !ok {
		t.Fatalf("owner-b acquire after release: ok=%v err=%v", ok, err)
	}

	// Claim: ordered ascending by transition_at (late row first).
	rows, err := as.ClaimTransitions(ctx, 10, time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("claimed %d, want 3", len(rows))
	}
	if !rows[0].At.Equal(at.Add(-2 * time.Hour)) {
		t.Fatalf("claim order: first=%s, want the late row (event-time ascending)", rows[0].At)
	}
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.TransitionID)
	}
	if err := as.MarkArchived(ctx, ids[:2]); err != nil {
		t.Fatal(err)
	}
	if err := as.MarkFailed(ctx, ids[2:], "permanent test"); err != nil {
		t.Fatal(err)
	}
	pending, _, err := as.QueueStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("pending after mark = %d, want 0", pending)
	}

	// Attempts exhaustion dead-letters (crashed inflight rows included).
	if _, err := store.ApplyEvent(ctx, canonical("aq-3", at.Add(2*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE transition_archive_queue SET attempts=10, status='inflight' WHERE status='pending'`); err != nil {
		t.Fatal(err)
	}
	n, err := as.MarkExhausted(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("exhausted rows = %d, want 1", n)
	}
}
