package v1

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/example/alert-platform/internal/application/authn"
)

// fakeAuthnStore backs the authn service in-memory.
type fakeAuthnStore struct {
	mu   sync.Mutex
	byID map[string]*authn.Identity // token -> identity
}

func (f *fakeAuthnStore) FindToken(_ context.Context, tokenHash string) (*authn.Identity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for token, id := range f.byID {
		if authn.TokenHash(token) == tokenHash {
			return id, nil
		}
	}
	return nil, authn.ErrUnauthenticated
}

func newAuthedRouter(t *testing.T) *server.Hertz {
	t.Helper()
	store := &fakeAuthnStore{byID: map[string]*authn.Identity{
		"tok-admin":  {TenantID: "tenant-a", ActorID: "alice", Role: authn.RolePlatformAdmin},
		"tok-oncall": {TenantID: "tenant-a", ActorID: "bob", Role: authn.RoleOncall},
		"tok-auditor": {TenantID: "tenant-b", ActorID: "carol", Role: authn.RoleAuditor},
	}}
	SetAuthn(authn.NewService(store))
	SetSourceRegistry(nil) // unwired downstream service -> exercises fail-closed AFTER auth
	SetQuerySvc(nil)
	t.Cleanup(func() { SetAuthn(nil); SetQuerySvc(nil) })
	// Register the handlers directly (importing the router package here
	// would cycle; route WIRING is covered by the hz drift + merge checks).
	h := server.Default(server.WithHostPorts(":0"))
	h.GET("/sources/v1", ListSources)
	h.GET("/alerts/v1", ListAlerts)
	h.GET("/alerts/v1/{instance_id}", GetAlert)
	return h
}

func get(t *testing.T, h *server.Hertz, path, token string) *ut.ResponseRecorder {
	t.Helper()
	return ut.PerformRequest(h.Engine, http.MethodGet, path, nil,
		ut.Header{Key: "Authorization", Value: "Bearer " + token})
}

// AUTH-001: anonymous requests are 401 on the management surface.
func TestAnonymousIs401(t *testing.T) {
	h := newAuthedRouter(t)
	for _, path := range []string{"/sources/v1", "/alerts/v1"} {
		if w := get(t, h, path, ""); w.Code != http.StatusUnauthorized {
			t.Fatalf("GET %s anonymous: %d, want 401", path, w.Code)
		}
	}
}

// AUTH-002: role enforcement — oncall cannot create sources (403), admin can
// proceed past authz (fails closed at the unwired registry with 503, proving
// authz PASSED).
func TestRoleEnforcement(t *testing.T) {
	h := newAuthedRouter(t)
	w := get(t, h, "/sources/v1", "tok-oncall")
	if w.Code != http.StatusForbidden {
		t.Fatalf("oncall sources read: %d, want 403", w.Code)
	}
	w = get(t, h, "/sources/v1", "tok-admin")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("admin sources read: %d, want 503 (authz passed, registry unwired)", w.Code)
	}
}

// T-012: tenant identity comes from the TOKEN ONLY. A request from tenant B's
// auditor carrying tenant A's instance id is still scoped to tenant B — the
// handlers never read a tenant header. (The query service enforces Forbidden;
// here authz lets the auditor READ alerts, so the unwired-query 503 proves
// auth passed; the tenant-scope itself is asserted by the query store tests.)
func TestTenantFromTokenOnly(t *testing.T) {
	h := newAuthedRouter(t)
	// Forged tenant header must not grant tenant-a's view to tenant-b's token.
	w := ut.PerformRequest(h.Engine, http.MethodGet, "/alerts/v1", nil,
		ut.Header{Key: "Authorization", Value: "Bearer tok-auditor"},
		ut.Header{Key: "X-Tenant-ID", Value: "tenant-a"})
	if w.Code != http.StatusServiceUnavailable { // auditor HAS alerts:read
		t.Fatalf("auditor alerts read: %d, want 503 (authz passed; header ignored)", w.Code)
	}
}

// Audit details are recorded through the hook when wired (AUTH-003).
func TestAuditHookRedactsSecrets(t *testing.T) {
	h := newAuthedRouter(t)
	var got map[string]any
	SetAuditHook(func(_ context.Context, tenantID, actor, op, objType, objID string, details map[string]any) {
		got = map[string]any{"tenant": tenantID, "actor": actor, "op": op, "details": details}
	})
	defer SetAuditHook(nil)
	audit(context.Background(), "t1", "alice", "source.create", "source", "s1",
		map[string]any{"credential_token": "aps_secret", "name": "df"})
	if got["details"].(map[string]any)["credential_token"] != "[REDACTED]" {
		t.Fatalf("hook must receive sanitized details: %+v", got)
	}
	_ = h
}
