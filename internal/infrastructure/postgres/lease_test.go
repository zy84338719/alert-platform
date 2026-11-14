package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/alert-platform/internal/infrastructure/testenv"
)

// HT-002: exactly one live subscriber per shard; renewal keeps ownership;
// expiry hands over to the surviving replica (scale-down).
func TestShardLeases(t *testing.T) {
	dsn := testenv.StartPostgres(t)
	if _, err := Up(dsn, absMigrations(t)); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := NewLeaseStore(pool)
	ctx := context.Background()

	// First claim wins.
	gen, err := s.Claim(ctx, "node-a", "edge-1", time.Minute, "")
	if err != nil || gen != 1 {
		t.Fatalf("claim1: gen=%d err=%v", gen, err)
	}

	// A competing replica loses while the lease is live.
	if _, err := s.Claim(ctx, "node-a", "edge-2", time.Minute, ""); err != ErrLeaseHeld {
		t.Fatalf("competing claim: err=%v, want ErrLeaseHeld", err)
	}

	// The owner renews without losing the lease or bumping the generation.
	gen, err = s.Claim(ctx, "node-a", "edge-1", time.Minute, "cursor-42")
	if err != nil || gen != 1 {
		t.Fatalf("renew: gen=%d err=%v", gen, err)
	}

	// Expire the lease; the surviving replica takes over (generation+1) and
	// inherits the cursor for resumption.
	if _, err := pool.Exec(ctx, `UPDATE edge_leases SET expires_at=now()-interval '1 second' WHERE shard_key='node-a'`); err != nil {
		t.Fatal(err)
	}
	gen, err = s.Claim(ctx, "node-a", "edge-2", time.Minute, "cursor-42")
	if err != nil || gen != 2 {
		t.Fatalf("takeover: gen=%d err=%v", gen, err)
	}
	var cursor string
	if err := pool.QueryRow(ctx, `SELECT cursor FROM edge_leases WHERE shard_key='node-a'`).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if cursor != "cursor-42" {
		t.Fatalf("cursor = %q, want handoff cursor", cursor)
	}

	// The old owner is now the loser.
	if _, err := s.Claim(ctx, "node-a", "edge-1", time.Minute, ""); err != ErrLeaseHeld {
		t.Fatalf("old owner reclaimed a live lease: %v", err)
	}
}
