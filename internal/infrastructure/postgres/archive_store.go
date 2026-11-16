package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/alert-platform/internal/application/archive"
)

// ArchiveStore implements the PG-side cursor port for the transition
// archiver (P9-CH-01). Reads are plain SELECTs over committed rows; no locks
// are taken on the hot write path.
type ArchiveStore struct {
	pool *pgxpool.Pool
}

// NewArchiveStore builds the archiver store over a pool.
func NewArchiveStore(pool *pgxpool.Pool) *ArchiveStore { return &ArchiveStore{pool: pool} }

const (
	archiveCursorName = "transitions"
	zeroUUID          = "00000000-0000-0000-0000-000000000000"
)

// NextTransitions returns up to limit rows after the persisted cursor.
func (s *ArchiveStore) NextTransitions(ctx context.Context, limit int) ([]archive.Transition, error) {
	at := time.Unix(0, 0).UTC()
	id := zeroUUID
	err := s.pool.QueryRow(ctx, `
		SELECT last_at, last_id::text FROM archive_cursors WHERE name=$1`,
		archiveCursorName).Scan(&at, &id)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id::text, transition_id::text, alert_instance_id::text,
		       COALESCE(from_state,''), to_state, event_id, transition_at
		FROM alert_transitions
		WHERE (transition_at, transition_id) > ($1, $2::uuid)
		ORDER BY transition_at, transition_id
		LIMIT $3`, at, id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []archive.Transition
	for rows.Next() {
		var t archive.Transition
		if err := rows.Scan(&t.TenantID, &t.TransitionID, &t.InstanceID,
			&t.FromState, &t.ToState, &t.EventID, &t.At); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// AdvanceCursor persists the last archived row; monotonic by design (a
// concurrent/stale advancer cannot move the cursor backwards).
func (s *ArchiveStore) AdvanceCursor(ctx context.Context, at time.Time, transitionID string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO archive_cursors (name, last_at, last_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (name) DO UPDATE
		SET last_at=EXCLUDED.last_at, last_id=EXCLUDED.last_id, updated_at=now()
		WHERE (archive_cursors.last_at, archive_cursors.last_id)
		      < (EXCLUDED.last_at, EXCLUDED.last_id)`,
		archiveCursorName, at, transitionID)
	return err
}
