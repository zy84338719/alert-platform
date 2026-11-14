package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/alert-platform/internal/application/sourcereg"
	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// SourceStore implements the sourcereg.Store port on the alert_sources +
// source_credentials tables (migration 0001 §alert_sources + 0002).
type SourceStore struct {
	pool *pgxpool.Pool
}

// NewSourceStore builds the registry store over a pool.
func NewSourceStore(pool *pgxpool.Pool) *SourceStore { return &SourceStore{pool: pool} }

const sourceCols = `source_id::text, tenant_id::text, name, source_type,
	vendor_version, adapter_version, cluster_id, enabled, last_seen_at, health_status`

func scanSource(row pgx.Row) (*sourcereg.Source, error) {
	var s sourcereg.Source
	var sourceType string
	var lastSeenAt *time.Time
	if err := row.Scan(&s.SourceID, &s.TenantID, &s.Name, &sourceType,
		&s.VendorVersion, &s.AdapterVersion, &s.ClusterID, &s.Enabled, &lastSeenAt, &s.HealthStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, sourcereg.ErrNotFound
		}
		return nil, err
	}
	if lastSeenAt != nil {
		s.LastSeenAt = *lastSeenAt
	}
	s.SourceType = sourceTypeFromString(sourceType)
	return &s, nil
}

// sourceTypeFromString maps the DDL TEXT column back to the IDL enum. The
// alert_sources CHECK constraint keeps the set closed.
func sourceTypeFromString(s string) eventv1.SourceType {
	switch s {
	case "deepflow":
		return eventv1.SourceType_SOURCE_TYPE_DEEPFLOW
	case "alertmanager":
		return eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER
	case "huatuo":
		return eventv1.SourceType_SOURCE_TYPE_HUATUO
	case "custom":
		return eventv1.SourceType_SOURCE_TYPE_CUSTOM
	default:
		return eventv1.SourceType_SOURCE_TYPE_UNSPECIFIED
	}
}

func sourceTypeToString(t eventv1.SourceType) string {
	switch t {
	case eventv1.SourceType_SOURCE_TYPE_DEEPFLOW:
		return "deepflow"
	case eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER:
		return "alertmanager"
	case eventv1.SourceType_SOURCE_TYPE_HUATUO:
		return "huatuo"
	case eventv1.SourceType_SOURCE_TYPE_CUSTOM:
		return "custom"
	default:
		return ""
	}
}

// CreateSource inserts the source plus its first credential hash in one tx.
func (s *SourceStore) CreateSource(ctx context.Context, src *sourcereg.Source, tokenHash string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO alert_sources
			(tenant_id, source_id, name, source_type, vendor_version, adapter_version, cluster_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		src.TenantID, src.SourceID, src.Name, sourceTypeToString(src.SourceType),
		src.VendorVersion, src.AdapterVersion, src.ClusterID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO source_credentials (tenant_id, source_id, token_hash)
		VALUES ($1, $2, $3)`,
		src.TenantID, src.SourceID, tokenHash); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// GetSource enforces tenant ownership at the query level (T-012 read side).
func (s *SourceStore) GetSource(ctx context.Context, tenantID, sourceID string) (*sourcereg.Source, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+sourceCols+` FROM alert_sources
		WHERE tenant_id=$1 AND source_id=$2`, tenantID, sourceID)
	return scanSource(row)
}

// ListSources lists the tenant's sources, optionally including disabled ones.
func (s *SourceStore) ListSources(ctx context.Context, tenantID string, includeDisabled bool) ([]*sourcereg.Source, error) {
	q := `SELECT ` + sourceCols + ` FROM alert_sources WHERE tenant_id=$1`
	if !includeDisabled {
		q += ` AND enabled`
	}
	q += ` ORDER BY created_at`
	rows, err := s.pool.Query(ctx, q, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*sourcereg.Source
	for rows.Next() {
		src, err := scanSource(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, src)
	}
	return out, rows.Err()
}

// SetEnabled flips the enabled flag; no rows means not-found-or-wrong-tenant.
func (s *SourceStore) SetEnabled(ctx context.Context, tenantID, sourceID string, enabled bool) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE alert_sources SET enabled=$3, updated_at=now()
		WHERE tenant_id=$1 AND source_id=$2`, tenantID, sourceID, enabled)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return sourcereg.ErrNotFound
	}
	return nil
}

// RevokeCredentials revokes every live token of the source.
func (s *SourceStore) RevokeCredentials(ctx context.Context, tenantID, sourceID string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE source_credentials SET revoked_at=now()
		WHERE tenant_id=$1 AND source_id=$2 AND revoked_at IS NULL`, tenantID, sourceID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return sourcereg.ErrNotFound
	}
	return nil
}

// FindByTokenHash is the auth hot path: hash -> unrevoked source (enabled is
// checked by the service afterwards so a disabled source maps to a distinct
// 403, not a misleading 401). It never takes a tenant parameter — the tenant
// is DERIVED here (docs/00 FR-009: wire claims cannot influence the lookup).
func (s *SourceStore) FindByTokenHash(ctx context.Context, tokenHash string) (*sourcereg.Source, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT s.source_id::text, s.tenant_id::text, s.name, s.source_type,
			s.vendor_version, s.adapter_version, s.cluster_id, s.enabled,
			s.last_seen_at, s.health_status
		FROM source_credentials c
		JOIN alert_sources s ON s.tenant_id = c.tenant_id AND s.source_id = c.source_id
		WHERE c.token_hash = $1 AND c.revoked_at IS NULL
		LIMIT 1`, tokenHash)
	return scanSource(row)
}

// TouchLastSeen marks the source as recently seen (PROC-006 input).
func (s *SourceStore) TouchLastSeen(ctx context.Context, sourceID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE alert_sources SET last_seen_at=now(), updated_at=now()
		WHERE source_id=$1`, sourceID)
	return err
}
