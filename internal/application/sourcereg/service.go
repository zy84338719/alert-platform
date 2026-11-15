// Package sourcereg implements the source registry use case (ING-003):
// registering alert sources, issuing bearer credentials and authenticating
// external pushes. Tenant identity is ALWAYS derived from the credential on
// the server side — tenant claims inside vendor payloads are never trusted.
package sourcereg

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// Sentinel errors surfaced as HTTP error codes by the inbound adapters.
var (
	ErrNotFound        = errors.New("source not found")
	ErrUnauthenticated = errors.New("unknown or revoked credential")
	ErrDisabled        = errors.New("source is disabled")
	ErrConflict        = errors.New("source conflict")
)

// Source is the registry's view of one external alert source.
type Source struct {
	SourceID       string
	TenantID       string
	Name           string
	SourceType     eventv1.SourceType
	VendorVersion  string
	AdapterVersion string
	ClusterID      string
	Enabled        bool
	LastSeenAt     time.Time
	HealthStatus   string
}

// EdgeIdentity is a trusted edge uplink credential (P8-P0-02): ONE credential
// that may upload a batch for a limited source subset of ONE tenant. It is the
// "edge -> center" identity, deliberately separate from the per-vendor webhook
// credentials the edge terminates. AllowedSourceIDs is filtered to sources
// that are currently enabled at resolution time.
type EdgeIdentity struct {
	EdgeID           string
	TenantID         string
	AllowedSourceIDs []string
}

// Authorizes reports whether this edge credential may upload for sourceID.
func (e *EdgeIdentity) Authorizes(sourceID string) bool {
	for _, id := range e.AllowedSourceIDs {
		if id == sourceID {
			return true
		}
	}
	return false
}

// Store is the persistence port (infrastructure/postgres implements it).
type Store interface {
	// CreateSource inserts the source and its first credential hash atomically.
	CreateSource(ctx context.Context, src *Source, tokenHash string) error
	// GetSource returns one source by id within a tenant (cross-tenant reads fail).
	GetSource(ctx context.Context, tenantID, sourceID string) (*Source, error)
	// ListSources lists the tenant's sources.
	ListSources(ctx context.Context, tenantID string, includeDisabled bool) ([]*Source, error)
	// SetEnabled flips the enabled flag.
	SetEnabled(ctx context.Context, tenantID, sourceID string, enabled bool) error
	// RevokeCredentials revokes every live credential of the source.
	RevokeCredentials(ctx context.Context, tenantID, sourceID string) error
	// FindByTokenHash resolves an active (unrevoked, enabled) source from a
	// credential hash. This is the auth hot path; it must not filter by any
	// tenant claim from the wire.
	FindByTokenHash(ctx context.Context, tokenHash string) (*Source, error)
	// FindEdgeByTokenHash resolves an unrevoked edge uplink credential (P8-P0-02).
	// The allowed set is filtered to currently-enabled sources: disabling one
	// source immediately removes it from the credential's authority.
	FindEdgeByTokenHash(ctx context.Context, tokenHash string) (*EdgeIdentity, error)
	// TouchLastSeen records that the source pushed recently (PROC-006 input).
	TouchLastSeen(ctx context.Context, sourceID string) error
}

// Service wires the registry use cases.
type Service struct {
	store Store
}

// NewService builds the registry service over its store port.
func NewService(store Store) *Service { return &Service{store: store} }

// TokenHash is the one-way form persisted for credentials. Plaintext tokens
// exist only in the Create response and in the caller's secret store.
func TokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// Create registers a source and returns it together with the plaintext
// credential token (shown exactly once).
func (s *Service) Create(ctx context.Context, src *Source) (*Source, string, error) {
	if src.TenantID == "" || src.Name == "" || src.ClusterID == "" {
		return nil, "", fmt.Errorf("tenant_id, name and cluster_id are required")
	}
	if src.SourceType == eventv1.SourceType_SOURCE_TYPE_UNSPECIFIED ||
		int(src.SourceType) > int(eventv1.SourceType_SOURCE_TYPE_CUSTOM) {
		return nil, "", fmt.Errorf("unsupported source_type %d", src.SourceType)
	}
	src.SourceID = uuid.NewString()
	src.Enabled = true
	src.HealthStatus = "unknown"
	token, err := newToken()
	if err != nil {
		return nil, "", err
	}
	if err := s.store.CreateSource(ctx, src, TokenHash(token)); err != nil {
		return nil, "", err
	}
	return src, token, nil
}

// Get returns one source, enforcing tenant ownership.
func (s *Service) Get(ctx context.Context, tenantID, sourceID string) (*Source, error) {
	src, err := s.store.GetSource(ctx, tenantID, sourceID)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrNotFound
	}
	return src, err
}

// List returns the tenant's sources.
func (s *Service) List(ctx context.Context, tenantID string, includeDisabled bool) ([]*Source, error) {
	return s.store.ListSources(ctx, tenantID, includeDisabled)
}

// SetEnabled enables/disables a source (disabled sources fail auth with 403).
func (s *Service) SetEnabled(ctx context.Context, tenantID, sourceID string, enabled bool) (*Source, error) {
	if err := s.store.SetEnabled(ctx, tenantID, sourceID, enabled); err != nil {
		return nil, err
	}
	return s.store.GetSource(ctx, tenantID, sourceID)
}

// RevokeCredential disables every live token of the source at once.
func (s *Service) RevokeCredential(ctx context.Context, tenantID, sourceID string) (*Source, error) {
	if err := s.store.RevokeCredentials(ctx, tenantID, sourceID); err != nil {
		return nil, err
	}
	return s.store.GetSource(ctx, tenantID, sourceID)
}

// Authenticate resolves the bearer token to an enabled source. The returned
// Source.TenantID is the server-derived tenant for every event of this push.
func (s *Service) Authenticate(ctx context.Context, token string) (*Source, error) {
	if token == "" {
		return nil, ErrUnauthenticated
	}
	src, err := s.store.FindByTokenHash(ctx, TokenHash(token))
	if errors.Is(err, ErrNotFound) {
		return nil, ErrUnauthenticated
	}
	if err != nil {
		return nil, err
	}
	if !src.Enabled {
		return nil, ErrDisabled
	}
	// last_seen feeds source-health reconciliation (PROC-006). A failed touch
	// must not fail the push itself — health only degrades, never blocks.
	_ = s.store.TouchLastSeen(ctx, src.SourceID)
	return src, nil
}

// AuthenticateEdge resolves an edge uplink bearer (P8-P0-02). The returned
// identity carries the ONE tenant and the enabled source subset it may upload
// for; every event of a batch is validated against that set by the caller.
// Same vague 401 for unknown/revoked credentials, same fail-closed semantics.
func (s *Service) AuthenticateEdge(ctx context.Context, token string) (*EdgeIdentity, error) {
	if token == "" {
		return nil, ErrUnauthenticated
	}
	ident, err := s.store.FindEdgeByTokenHash(ctx, TokenHash(token))
	if errors.Is(err, ErrNotFound) {
		return nil, ErrUnauthenticated
	}
	if err != nil {
		return nil, err
	}
	for _, sid := range ident.AllowedSourceIDs {
		_ = s.store.TouchLastSeen(ctx, sid)
	}
	return ident, nil
}

// newToken mints "aps_<48 hex chars>" (24 random bytes).
func newToken() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("mint credential: %w", err)
	}
	return "aps_" + hex.EncodeToString(b), nil
}
