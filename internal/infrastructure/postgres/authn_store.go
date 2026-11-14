package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/alert-platform/internal/application/authn"
)

// AuthnStore implements authn.Store over platform_tokens (migration 0003).
type AuthnStore struct{ pool *pgxpool.Pool }

// NewAuthnStore builds the token store.
func NewAuthnStore(pool *pgxpool.Pool) *AuthnStore { return &AuthnStore{pool: pool} }

// FindToken resolves a live token hash to the actor identity.
func (s *AuthnStore) FindToken(ctx context.Context, tokenHash string) (*authn.Identity, error) {
	var id authn.Identity
	err := s.pool.QueryRow(ctx, `
		SELECT tenant_id::text, actor_id, role FROM platform_tokens
		WHERE token_hash=$1 AND revoked_at IS NULL LIMIT 1`, tokenHash).
		Scan(&id.TenantID, &id.ActorID, &id.Role)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, authn.ErrUnauthenticated
	}
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// MintToken inserts a platform token for dev/ops bootstrap. The plaintext is
// returned ONCE; production issuance goes through the managed secret flow.
func (s *AuthnStore) MintToken(ctx context.Context, tenantID, actorID, role, tokenHash string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO platform_tokens (tenant_id, actor_id, role, token_hash)
		VALUES ($1,$2,$3,$4)`, tenantID, actorID, role, tokenHash)
	return err
}

// AuditEntry is one management operation record (AUTH-003).
type AuditEntry struct {
	TenantID   string
	ActorID    string
	Operation  string
	ObjectType string
	ObjectID   string
	Details    map[string]any
}

// jsonMarshalSanitized defers masking to authn.Sanitize (single definition;
// the DB layer keeps it as defense in depth for direct store callers).
func jsonMarshalSanitized(details map[string]any) ([]byte, error) {
	if details == nil {
		details = map[string]any{}
	}
	return json.Marshal(authn.Sanitize(details))
}

func unmarshalJSON(b []byte, v *any) error { return json.Unmarshal(b, v) }

// WriteAudit records one management operation. Details pass through
// SanitizeDetails (credential material never reaches the audit trail).
func (s *AuthnStore) WriteAudit(ctx context.Context, e AuditEntry) error {
	raw, err := jsonMarshalSanitized(e.Details)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO audit_logs (id, tenant_id, actor_id, operation, object_type, object_id, details)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6::jsonb)`,
		e.TenantID, e.ActorID, e.Operation, e.ObjectType, e.ObjectID, raw)
	return err
}

// ListAudit returns tenant audit rows newest-first with a limit.
func (s *AuthnStore) ListAudit(ctx context.Context, tenantID string, limit int) ([]map[string]any, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, actor_id, operation, object_type, object_id, details, created_at
		FROM audit_logs WHERE tenant_id=$1
		ORDER BY created_at DESC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, actor, op, objType, objID string
		var createdAt time.Time
		var det []byte
		if err := rows.Scan(&id, &actor, &op, &objType, &objID, &det, &createdAt); err != nil {
			return nil, err
		}
		var d any
		_ = unmarshalJSON(det, &d)
		out = append(out, map[string]any{
			"id": id, "actor_id": actor, "operation": op,
			"object_type": objType, "object_id": objID, "details": d,
			"created_at": createdAt.UTC().Format(time.RFC3339),
		})
	}
	return out, rows.Err()
}
