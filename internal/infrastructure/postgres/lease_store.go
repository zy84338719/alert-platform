package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LeaseStore implements HT-002 shard leases: one row per shard; a lease is
// claimable when free, expired, or already owned (renewal). Two replicas
// racing converge to exactly one winner via SELECT ... FOR UPDATE.
type LeaseStore struct{ pool *pgxpool.Pool }

// NewLeaseStore builds the lease store.
func NewLeaseStore(pool *pgxpool.Pool) *LeaseStore { return &LeaseStore{pool: pool} }

// ErrLeaseHeld means another owner holds a live lease.
var ErrLeaseHeld = errors.New("shard lease held by another owner")

// Claim acquires, renews or takes over the lease; cursor persists the last
// stream position for handoff. Returns the lease generation (1 on first
// claim, +1 per takeover after expiry).
func (s *LeaseStore) Claim(ctx context.Context, shard, owner string, ttl time.Duration, cursor string) (int64, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var curOwner string
	var expiresAt time.Time
	var generations int64
	err = tx.QueryRow(ctx, `
		SELECT owner, expires_at, generations FROM edge_leases
		WHERE shard_key=$1 FOR UPDATE`, shard).Scan(&curOwner, &expiresAt, &generations)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if _, err := tx.Exec(ctx, `
			INSERT INTO edge_leases (shard_key, owner, cursor, expires_at, generations)
			VALUES ($1, $2, $3, now()+$4::interval, 1)`, shard, owner, cursor, seconds(ttl)); err != nil {
			return 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 1, nil
	case err != nil:
		return 0, err
	}

	var nextGen int64
	switch {
	case curOwner == owner:
		nextGen = generations // renewal
	case !time.Now().Before(expiresAt):
		nextGen = generations + 1 // takeover after expiry (replica scale-down)
	default:
		return 0, ErrLeaseHeld
	}
	if _, err := tx.Exec(ctx, `
		UPDATE edge_leases
		SET owner=$2, cursor=$3, acquired_at=now(), expires_at=now()+$4::interval, generations=$5
		WHERE shard_key=$1`, shard, owner, cursor, seconds(ttl), nextGen); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return nextGen, nil
}

func seconds(d time.Duration) string {
	sec := int64(d.Seconds())
	if sec < 1 {
		sec = 1
	}
	return formatInt(sec)
}

func formatInt(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
