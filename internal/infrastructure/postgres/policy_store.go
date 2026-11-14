package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/alert-platform/internal/application/policymgmt"
)

// PolicyStore implements policymgmt.Store (routes + silences + channels).
type PolicyStore struct{ pool *pgxpool.Pool }

// NewPolicyStore builds the management store.
func NewPolicyStore(pool *pgxpool.Pool) *PolicyStore { return &PolicyStore{pool: pool} }

// CreateRoute appends a new immutable route version (policy_versions).
func (s *PolicyStore) CreateRoute(ctx context.Context, r *policymgmt.Route) error {
	r.PolicyID = uuid.NewString()
	spec, err := json.Marshal(map[string]any{
		"priority":  r.Priority,
		"matchers":  r.Matchers,
		"receivers": r.Receivers,
	})
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Version = max(existing)+1 within (tenant, policy_type='route').
	var maxVer int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(max(version), 0) FROM policy_versions
		WHERE tenant_id=$1 AND policy_type='route'`, r.TenantID).Scan(&maxVer); err != nil {
		return err
	}
	r.Version = maxVer + 1
	if _, err := tx.Exec(ctx, `
		INSERT INTO policy_versions (id, tenant_id, version, policy_type, spec)
		VALUES ($1,$2,$3,'route',$4)`, r.PolicyID, r.TenantID, r.Version, spec); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListRoutes returns the tenant's enabled routes priority-ordered.
func (s *PolicyStore) ListRoutes(ctx context.Context, tenantID string) ([]*policymgmt.Route, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, version, spec FROM policy_versions
		WHERE tenant_id=$1 AND policy_type='route' AND enabled
		ORDER BY (spec->>'priority')::int ASC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*policymgmt.Route
	for rows.Next() {
		var r policymgmt.Route
		var spec []byte
		if err := rows.Scan(&r.PolicyID, &r.Version, &spec); err != nil {
			return nil, err
		}
		var parsed struct {
			Priority  int64                      `json:"priority"`
			Matchers  []policymgmt.Matcher      `json:"matchers"`
			Receivers []policymgmt.RouteReceiver `json:"receivers"`
		}
		if err := json.Unmarshal(spec, &parsed); err != nil {
			return nil, err
		}
		r.TenantID, r.Priority, r.Matchers, r.Receivers = tenantID, parsed.Priority, parsed.Matchers, parsed.Receivers
		out = append(out, &r)
	}
	return out, rows.Err()
}

// SetRouteEnabled flips a route version (optimistic check on expected).
func (s *PolicyStore) SetRouteEnabled(ctx context.Context, tenantID, policyID string, enabled bool, expectedVersion int64) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE policy_versions SET enabled=$4
		WHERE tenant_id=$1 AND id=$2 AND policy_type='route' AND version=$3`,
		tenantID, policyID, expectedVersion, enabled)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() == 0 {
		return 0, policymgmt.ErrVersionConflict
	}
	return expectedVersion, nil
}

// --- silences ---

func (s *PolicyStore) CreateSilence(ctx context.Context, sl *policymgmt.Silence) error {
	sl.SilenceID = uuid.NewString()
	matchers, err := json.Marshal(sl.Matchers)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO silences (id, tenant_id, matchers, starts_at, ends_at, created_by, reason)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		sl.SilenceID, sl.TenantID, matchers, sl.StartsAt, sl.EndsAt, sl.CreatedBy, sl.Reason)
	return err
}

func (s *PolicyStore) ListSilences(ctx context.Context, tenantID string, includeExpired bool) ([]*policymgmt.Silence, error) {
	q := `SELECT id::text, matchers, starts_at, ends_at, created_by, reason, version
		FROM silences WHERE tenant_id=$1`
	if !includeExpired {
		q += ` AND ends_at > now()`
	}
	q += ` ORDER BY created_at DESC`
	rows, err := s.pool.Query(ctx, q, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*policymgmt.Silence
	for rows.Next() {
		var sl policymgmt.Silence
		var matchers []byte
		if err := rows.Scan(&sl.SilenceID, &matchers, &sl.StartsAt, &sl.EndsAt, &sl.CreatedBy, &sl.Reason, &sl.Version); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(matchers, &sl.Matchers); err != nil {
			return nil, err
		}
		sl.TenantID = tenantID
		out = append(out, &sl)
	}
	return out, rows.Err()
}

// RevokeSilence closes the window (ends_at = now) — revocation, not deletion.
func (s *PolicyStore) RevokeSilence(ctx context.Context, tenantID, silenceID string) (*policymgmt.Silence, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE silences SET ends_at=now(), version=version+1, updated_at=now()
		WHERE tenant_id=$1 AND id=$2 AND ends_at > now()`, tenantID, silenceID)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, policymgmt.ErrNotFound
	}
	row := s.pool.QueryRow(ctx, `
		SELECT id::text, matchers, starts_at, ends_at, created_by, reason, version
		FROM silences WHERE tenant_id=$1 AND id=$2`, tenantID, silenceID)
	return scanSilence(row, tenantID)
}

func scanSilence(row pgx.Row, tenantID string) (*policymgmt.Silence, error) {
	var sl policymgmt.Silence
	var matchers []byte
	if err := row.Scan(&sl.SilenceID, &matchers, &sl.StartsAt, &sl.EndsAt, &sl.CreatedBy, &sl.Reason, &sl.Version); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, policymgmt.ErrNotFound
		}
		return nil, err
	}
	if err := json.Unmarshal(matchers, &sl.Matchers); err != nil {
		return nil, err
	}
	sl.TenantID = tenantID
	return &sl, nil
}

// --- channels ---

func (s *PolicyStore) CreateChannel(ctx context.Context, ch *policymgmt.Channel) error {
	ch.ChannelID = uuid.NewString()
	cfg := map[string]any{"webhook_url": ch.WebhookURL}
	if ch.Secret != "" {
		cfg["secret"] = ch.Secret
	}
	raw, _ := json.Marshal(cfg)
	if ch.IsDefault {
		// one default per tenant: demote the previous one atomically.
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `UPDATE notification_channels SET is_default=false WHERE tenant_id=$1 AND is_default`, ch.TenantID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO notification_channels (id, tenant_id, name, type, config, is_default)
			VALUES ($1,$2,$3,$4,$5,$6)`,
			ch.ChannelID, ch.TenantID, ch.Name, ch.Type, raw, ch.IsDefault); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO notification_channels (id, tenant_id, name, type, config, is_default)
		VALUES ($1,$2,$3,$4,$5,$6)`,
		ch.ChannelID, ch.TenantID, ch.Name, ch.Type, raw, ch.IsDefault)
	return err
}

func (s *PolicyStore) ListChannels(ctx context.Context, tenantID string) ([]*policymgmt.Channel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, name, type, config->>'webhook_url', enabled, is_default
		FROM notification_channels WHERE tenant_id=$1 ORDER BY created_at`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*policymgmt.Channel
	for rows.Next() {
		var ch policymgmt.Channel
		if err := rows.Scan(&ch.ChannelID, &ch.Name, &ch.Type, &ch.WebhookURL, &ch.Enabled, &ch.IsDefault); err != nil {
			return nil, err
		}
		ch.TenantID = tenantID
		out = append(out, &ch)
	}
	return out, rows.Err()
}

func (s *PolicyStore) SetChannelEnabled(ctx context.Context, tenantID, channelID string, enabled bool) (*policymgmt.Channel, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE notification_channels SET enabled=$3, updated_at=now()
		WHERE tenant_id=$1 AND id=$2`, tenantID, channelID, enabled)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, policymgmt.ErrNotFound
	}
	row := s.pool.QueryRow(ctx, `
		SELECT id::text, name, type, config->>'webhook_url', enabled, is_default
		FROM notification_channels WHERE tenant_id=$1 AND id=$2`, tenantID, channelID)
	var ch policymgmt.Channel
	if err := row.Scan(&ch.ChannelID, &ch.Name, &ch.Type, &ch.WebhookURL, &ch.Enabled, &ch.IsDefault); err != nil {
		return nil, err
	}
	ch.TenantID = tenantID
	return &ch, nil
}

var _ = time.Now // keep time import for future window validation
