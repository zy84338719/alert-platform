package postgres

import (
	"context"
	"testing"
	"time"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// P9-CH-01：PG 侧游标 —— 按 (transition_at, transition_id) 顺序取数，
// 推进后不重复；游标只能单调前进。
func TestArchiveStoreCursor(t *testing.T) {
	store, _ := newStoreTest(t)
	ctx := context.Background()
	as := NewArchiveStore(store.pool)
	at := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)

	// firing -> resolved 产生两条 transition。
	if _, err := store.ApplyEvent(ctx, canonical("arch-1", at)); err != nil {
		t.Fatal(err)
	}
	resolved := canonical("arch-2", at.Add(time.Minute))
	resolved.Signal = eventv1.EventSignal_EVENT_SIGNAL_RESOLVED
	if _, err := store.ApplyEvent(ctx, resolved); err != nil {
		t.Fatal(err)
	}

	rows, err := as.NextTransitions(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ToState != "firing" || rows[1].ToState != "resolved" {
		t.Fatalf("rows = %+v", rows)
	}
	if rows[0].At.After(rows[1].At) {
		t.Fatalf("rows not ordered ascending: %+v", rows)
	}

	// 推进到第一条后，下一批只剩 resolved。
	if err := as.AdvanceCursor(ctx, rows[0].At, rows[0].TransitionID); err != nil {
		t.Fatal(err)
	}
	rest, err := as.NextTransitions(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 1 || rest[0].TransitionID != rows[1].TransitionID {
		t.Fatalf("after advance rows = %+v", rest)
	}

	// 回退游标必须无效（单调）。
	if err := as.AdvanceCursor(ctx, at.Add(-time.Hour), zeroUUID); err != nil {
		t.Fatal(err)
	}
	still, err := as.NextTransitions(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(still) != 1 {
		t.Fatalf("cursor moved backwards: %+v", still)
	}

	// 推进到末尾后为空（重启也不会重复归档）。
	if err := as.AdvanceCursor(ctx, rest[0].At, rest[0].TransitionID); err != nil {
		t.Fatal(err)
	}
	done, err := as.NextTransitions(ctx, 10)
	if err != nil || len(done) != 0 {
		t.Fatalf("after full advance rows=%d err=%v", len(done), err)
	}
}
