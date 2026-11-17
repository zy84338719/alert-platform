package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/example/alert-platform/internal/application/lifecycle"
)

// TransitionDaysBefore groups old transitions by UTC day with their
// archive-queue completion counts (P10-004 reconciliation input).
func (s *Store) TransitionDaysBefore(ctx context.Context, cutoff time.Time) ([]lifecycle.DayStats, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT (t.transition_at AT TIME ZONE 'UTC')::date AS day,
		       count(*) AS total,
		       count(*) FILTER (WHERE q.status='done') AS done
		FROM alert_transitions t
		LEFT JOIN transition_archive_queue q ON q.transition_id = t.transition_id
		WHERE t.transition_at < $1
		GROUP BY 1
		ORDER BY 1`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []lifecycle.DayStats
	for rows.Next() {
		var d lifecycle.DayStats
		if err := rows.Scan(&d.Day, &d.Total, &d.ArchivedDone); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// DeleteTransitionsForDay removes one verified UTC day in a single
// transaction: done queue rows first, then the transitions themselves. Callers
// MUST have verified the ClickHouse archive for that day (lifecycle.Service).
func (s *Store) DeleteTransitionsForDay(ctx context.Context, day time.Time) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	from := day.UTC()
	to := from.Add(24 * time.Hour)
	if _, err := tx.Exec(ctx, `
		DELETE FROM transition_archive_queue q
		USING alert_transitions t
		WHERE q.transition_id = t.transition_id
		  AND q.status = 'done'
		  AND t.transition_at >= $1 AND t.transition_at < $2`, from, to); err != nil {
		return 0, fmt.Errorf("delete archived queue rows: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM alert_transitions
		WHERE transition_at >= $1 AND transition_at < $2`, from, to)
	if err != nil {
		return 0, fmt.Errorf("delete transitions: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
