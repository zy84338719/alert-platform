package postgres

import (
	"context"
	"time"

	"github.com/example/alert-platform/internal/application/telemetry"
)

// Telemetry read port (P9-OBS-06): cheap aggregate queries run every 30s by
// the worker's sampler. They are intentionally outside any write transaction.

// CountActiveAlerts counts pending/firing/stale instances.
func (s *Store) CountActiveAlerts(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM alert_instances
		WHERE monitor_state IN ('pending','firing','stale')`).Scan(&n)
	return n, err
}

// CountOpenIncidents counts incidents that are not closed.
func (s *Store) CountOpenIncidents(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM incidents WHERE status <> 'closed'`).Scan(&n)
	return n, err
}

// ListSourceHeartbeats returns registered sources with their last push time.
func (s *Store) ListSourceHeartbeats(ctx context.Context) ([]telemetry.SourceHeartbeat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT source_id::text, name, last_seen_at, enabled
		FROM alert_sources
		ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []telemetry.SourceHeartbeat
	for rows.Next() {
		var src telemetry.SourceHeartbeat
		var lastSeen *time.Time
		if err := rows.Scan(&src.SourceID, &src.Name, &lastSeen, &src.Enabled); err != nil {
			return nil, err
		}
		if lastSeen != nil {
			src.LastSeenAt = *lastSeen
		}
		out = append(out, src)
	}
	return out, rows.Err()
}
