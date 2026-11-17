package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/example/alert-platform/internal/infrastructure/testenv"
)

// AUD-006（#15）：池大小/超时显式生效 —— 默认 pgxpool 尺寸与零超时会让
// 慢 PG 静默悬挂。
func TestOpenPoolRuntimeParams(t *testing.T) {
	dsn := testenv.StartPostgres(t)
	pool, err := OpenPool(context.Background(), dsn, 5, 5*time.Second, 2*time.Second, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if got := pool.Stat().MaxConns(); got != 5 {
		t.Fatalf("MaxConns = %d, want 5", got)
	}
	for param, want := range map[string]time.Duration{
		"statement_timeout":                   5 * time.Second,
		"lock_timeout":                        2 * time.Second,
		"idle_in_transaction_session_timeout": 30 * time.Second,
	} {
		var raw string
		if err := pool.QueryRow(context.Background(), "SHOW "+param).Scan(&raw); err != nil {
			t.Fatalf("%s: %v", param, err)
		}
		got, err := time.ParseDuration(raw)
		if err != nil || got != want {
			t.Fatalf("%s = %q (%v), want %s", param, raw, err, want)
		}
	}
}

// AUD-006（#15）：身份锁竞争必须立即返回 transient 错误，而不是无限等待。
func TestApplyEventIdentityLockContention(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	ev := canonical("lock-1", time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC))
	fp, err := fingerprintOf(ev)
	if err != nil {
		t.Fatal(err)
	}
	key := ev.GetTenantId() + "|" + fp

	// Hold the identity lock from another transaction.
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyEvent(ctx, ev); err == nil {
		t.Fatal("ApplyEvent must fail fast on identity lock contention (not block)")
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// After release the same event applies normally.
	if _, err := store.ApplyEvent(ctx, ev); err != nil {
		t.Fatalf("apply after lock release: %v", err)
	}
}
