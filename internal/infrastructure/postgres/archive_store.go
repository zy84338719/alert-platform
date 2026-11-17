package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/alert-platform/internal/application/archive"
)

// ArchiveStore implements the PG archive queue + single-writer lease
// (transition_archive_queue / component_leases, migration 0014).
type ArchiveStore struct {
	pool *pgxpool.Pool
}

// NewArchiveStore builds the archiver store over a pool.
func NewArchiveStore(pool *pgxpool.Pool) *ArchiveStore { return &ArchiveStore{pool: pool} }

const archiveLeaseName = "archive.transitions"

// AcquireLease renews the single-writer lease. False means another live owner
// holds it (N worker replicas must not run N archivers).
func (s *ArchiveStore) AcquireLease(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	var got string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO component_leases (name, owner, expires_at)
		VALUES ($1, $2, now() + $3::bigint * interval '1 second')
		ON CONFLICT (name) DO UPDATE
		SET owner=EXCLUDED.owner, expires_at=EXCLUDED.expires_at, updated_at=now()
		WHERE component_leases.owner = $2::uuid OR component_leases.expires_at < now()
		RETURNING owner::text`,
		archiveLeaseName, owner, int64(ttl.Seconds())).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return got == owner, nil
}

// ReleaseLease drops the lease if this owner still holds it.
func (s *ArchiveStore) ReleaseLease(ctx context.Context, owner string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM component_leases WHERE name=$1 AND owner=$2::uuid`, archiveLeaseName, owner)
	return err
}

// ClaimTransitions claims pending rows or lease-expired inflight rows,
// bumping attempts and leasing them to this writer.
func (s *ArchiveStore) ClaimTransitions(ctx context.Context, limit int, lease time.Duration, maxAttempts int) ([]archive.Transition, error) {
	rows, err := s.pool.Query(ctx, `
		WITH claimed AS (
			SELECT transition_id FROM transition_archive_queue
			WHERE (status='pending' AND attempts < $3)
			   OR (status='inflight' AND lease_until < now() AND attempts < $3)
			ORDER BY transition_at, transition_id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE transition_archive_queue q
		SET status='inflight', lease_until=now() + $2::bigint * interval '1 second',
		    attempts=attempts+1, updated_at=now()
		FROM claimed c
		WHERE q.transition_id = c.transition_id
		RETURNING q.transition_id::text, q.tenant_id::text, q.instance_id::text,
		          q.from_state, q.to_state, q.event_id, q.transition_at`,
		limit, int64(lease.Seconds()), maxAttempts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []archive.Transition
	for rows.Next() {
		var t archive.Transition
		if err := rows.Scan(&t.TransitionID, &t.TenantID, &t.InstanceID,
			&t.FromState, &t.ToState, &t.EventID, &t.At); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// MarkArchived marks rows done (ClickHouse acknowledged).
func (s *ArchiveStore) MarkArchived(ctx context.Context, ids []string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE transition_archive_queue
		SET status='done', lease_until=NULL, last_error=NULL, updated_at=now()
		WHERE transition_id = ANY($1::uuid[])`, ids)
	return err
}

// MarkFailed dead-letters rows (permanent rejection).
func (s *ArchiveStore) MarkFailed(ctx context.Context, ids []string, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE transition_archive_queue
		SET status='failed', lease_until=NULL, last_error=$2, updated_at=now()
		WHERE transition_id = ANY($1::uuid[])`, ids, reason)
	return err
}

// MarkRetry returns rows to pending after a transient failure.
func (s *ArchiveStore) MarkRetry(ctx context.Context, ids []string, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE transition_archive_queue
		SET status='pending', lease_until=NULL, last_error=$2, updated_at=now()
		WHERE transition_id = ANY($1::uuid[])`, ids, reason)
	return err
}

// MarkExhausted dead-letters rows that already hit maxAttempts (including
// crashed inflight claims that will never be retried).
func (s *ArchiveStore) MarkExhausted(ctx context.Context, maxAttempts int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE transition_archive_queue
		SET status='failed', lease_until=NULL, last_error='max attempts exceeded', updated_at=now()
		WHERE status IN ('pending','inflight') AND attempts >= $1`, maxAttempts)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// QueueStats reports depth + oldest queued transition (backlog metrics).
func (s *ArchiveStore) QueueStats(ctx context.Context) (int64, time.Time, error) {
	var pending int64
	var oldest *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT count(*), min(transition_at)
		FROM transition_archive_queue WHERE status IN ('pending','inflight')`).Scan(&pending, &oldest)
	if err != nil {
		return 0, time.Time{}, err
	}
	if oldest == nil {
		return pending, time.Time{}, nil
	}
	return pending, *oldest, nil
}
