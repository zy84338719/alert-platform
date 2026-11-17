package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OpenPool builds the shared pgx pool with EXPLICIT bounds (AUD-006): the
// default pgxpool sizing (max(4, NumCPU)) is too small for a worker that runs
// seven background loops on one pool, and without statement/lock timeouts a
// slow PG leaves transactions hanging forever while the process still looks
// healthy (readiness only sees task exits).
func OpenPool(ctx context.Context, dsn string, maxConns int, statementTimeout, lockTimeout, idleTxTimeout time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		cfg.MaxConns = int32(maxConns)
	}
	set := func(key string, d time.Duration) {
		if d > 0 {
			cfg.ConnConfig.RuntimeParams[key] = fmt.Sprintf("%d", int64(d/time.Millisecond))
		}
	}
	set("statement_timeout", statementTimeout)
	set("lock_timeout", lockTimeout)
	set("idle_in_transaction_session_timeout", idleTxTimeout)
	return pgxpool.NewWithConfig(ctx, cfg)
}
