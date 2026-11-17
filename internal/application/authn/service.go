// Package authn implements platform-token authentication and the RBAC
// matrix (AUTH-001/002, FR-009). Tenant identity ALWAYS comes from the token
// hash lookup — headers like X-Tenant-ID are never trusted for identity.
package authn

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
)

// Errors mapped to HTTP codes by the inbound adapters.
var (
	ErrUnauthenticated = errors.New("authentication required")
	ErrForbidden       = errors.New("role lacks the required permission")
)

// Roles (docs/10 AUTH-002 matrix).
const (
	RolePlatformAdmin = "platform_admin"
	RoleMaintainer    = "maintainer"
	RoleSRE           = "sre"
	RoleOncall        = "oncall"
	RoleAuditor       = "auditor"
)

// Permissions guarded at handler level.
const (
	PermAlertsRead         = "alerts:read"
	PermAlertsAck          = "alerts:ack"
	PermSourcesRead        = "sources:read"
	PermSourcesWrite       = "sources:write"
	PermPoliciesRead       = "policies:read"
	PermPoliciesWrite      = "policies:write"
	PermChannelsRead       = "channels:read"
	PermChannelsWrite      = "channels:write"
	PermNotificationsRead  = "notifications:read"
	PermNotificationsWrite = "notifications:write" // failed-task replay (AUD-002)
	PermAuditRead          = "audit:read"
)

// Matrix is the authoritative role -> permission set (AUTH-002 acceptance
// table; the test walks every cell).
var Matrix = map[string]map[string]bool{
	RolePlatformAdmin: allPerms(),
	RoleMaintainer: set(PermAlertsRead, PermAlertsAck, PermSourcesRead, PermSourcesWrite,
		PermPoliciesRead, PermPoliciesWrite, PermChannelsRead, PermChannelsWrite, PermNotificationsRead, PermNotificationsWrite),
	RoleSRE:     set(PermAlertsRead, PermAlertsAck, PermPoliciesRead, PermPoliciesWrite, PermChannelsRead, PermNotificationsRead, PermNotificationsWrite, PermAuditRead),
	RoleOncall:  set(PermAlertsRead, PermAlertsAck, PermChannelsRead, PermNotificationsRead),
	RoleAuditor: set(PermAlertsRead, PermChannelsRead, PermAuditRead),
}

func allPerms() map[string]bool {
	return set(PermAlertsRead, PermAlertsAck, PermSourcesRead, PermSourcesWrite,
		PermPoliciesRead, PermPoliciesWrite, PermChannelsRead, PermChannelsWrite, PermNotificationsRead,
		PermNotificationsWrite, PermAuditRead)
}

func set(ps ...string) map[string]bool {
	m := make(map[string]bool, len(ps))
	for _, p := range ps {
		m[p] = true
	}
	return m
}

// Allowed checks role -> permission.
func Allowed(role, permission string) bool {
	return Matrix[role][permission]
}

// Identity is the authenticated actor.
type Identity struct {
	TenantID string
	ActorID  string
	Role     string
}

// Store resolves token hashes to identities (infrastructure/postgres).
type Store interface {
	FindToken(ctx context.Context, tokenHash string) (*Identity, error)
}

// Service wires authentication.
type Service struct{ store Store }

// NewService builds the authn service.
func NewService(store Store) *Service { return &Service{store: store} }

// Authenticate resolves a bearer token. Unknown/revoked tokens are
// indistinguishable (401); role/tenant come from the database row only.
func (s *Service) Authenticate(ctx context.Context, token string) (*Identity, error) {
	if token == "" {
		return nil, ErrUnauthenticated
	}
	id, err := s.store.FindToken(ctx, TokenHash(token))
	if errors.Is(err, ErrUnauthenticated) {
		return nil, ErrUnauthenticated
	}
	if err != nil {
		return nil, err
	}
	return id, nil
}

// Require returns the identity when the token grants the permission.
func (s *Service) Require(ctx context.Context, token, permission string) (*Identity, error) {
	id, err := s.Authenticate(ctx, token)
	if err != nil {
		return nil, err
	}
	if !Allowed(id.Role, permission) {
		return nil, ErrForbidden
	}
	return id, nil
}

// TokenHash is the stored form of platform tokens.
func TokenHash(token string) string {
	sum := sha256.Sum256([]byte("platform-token/v1|" + token))
	return hex.EncodeToString(sum[:])
}

// secretKeys matches credential-shaped keys (AUTH-003 脱敏).
var secretKeys = regexp.MustCompile(`(?i)token|secret|password|credential`)

// Sanitize masks credential-shaped values before any persistence or logging.
func Sanitize(details map[string]any) map[string]any {
	out := make(map[string]any, len(details))
	for k, v := range details {
		if secretKeys.MatchString(k) {
			out[k] = "[REDACTED]"
			continue
		}
		if s, ok := v.(string); ok && secretKeys.MatchString(s) {
			out[k] = "[REDACTED]"
		} else {
			out[k] = v
		}
	}
	return out
}
