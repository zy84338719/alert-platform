package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/example/alert-platform/internal/application/telemetry"
)

// Telemetry read port (P9-OBS-06): cheap aggregate queries run every 30s by
// the worker's sampler. They are intentionally outside any write transaction.

// CountActiveAlertsByTenant counts pending/firing/stale instances per tenant.
func (s *Store) CountActiveAlertsByTenant(ctx context.Context) ([]telemetry.TenantCount, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id::text, count(*) FROM alert_instances
		WHERE monitor_state IN ('pending','firing','stale')
		GROUP BY tenant_id`)
	if err != nil {
		return nil, err
	}
	return scanTenantCounts(rows)
}

// CountOpenIncidentsByTenant counts incidents that are not closed per tenant.
func (s *Store) CountOpenIncidentsByTenant(ctx context.Context) ([]telemetry.TenantCount, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id::text, count(*) FROM incidents WHERE status <> 'closed'
		GROUP BY tenant_id`)
	if err != nil {
		return nil, err
	}
	return scanTenantCounts(rows)
}

func scanTenantCounts(rows pgx.Rows) ([]telemetry.TenantCount, error) {
	defer rows.Close()
	var out []telemetry.TenantCount
	for rows.Next() {
		var c telemetry.TenantCount
		if err := rows.Scan(&c.TenantID, &c.Count); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ListSourceHeartbeats returns registered sources with their last push time.
func (s *Store) ListSourceHeartbeats(ctx context.Context) ([]telemetry.SourceHeartbeat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT source_id::text, tenant_id::text, name, last_seen_at, enabled
		FROM alert_sources
		ORDER BY tenant_id, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []telemetry.SourceHeartbeat
	for rows.Next() {
		var src telemetry.SourceHeartbeat
		var lastSeen *time.Time
		if err := rows.Scan(&src.SourceID, &src.TenantID, &src.Name, &lastSeen, &src.Enabled); err != nil {
			return nil, err
		}
		if lastSeen != nil {
			src.LastSeenAt = *lastSeen
		}
		out = append(out, src)
	}
	return out, rows.Err()
}
