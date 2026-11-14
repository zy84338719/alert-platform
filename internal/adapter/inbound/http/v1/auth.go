// HTTP authentication helper (AUTH-001): platform bearer tokens resolve the
// tenant, actor and role server-side; anonymous requests are 401 and
// insufficient roles 403. X-Tenant-ID headers are IGNORED for identity — the
// tenant comes from the token row only (FR-009 / T-012). Management writes
// additionally record audit entries (AUTH-003).
package v1

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/example/alert-platform/internal/application/authn"
	"github.com/oekit/logx"
	"github.com/oekit/tracing/traceid"
	ekiterrors "github.com/oekit/pkg/errors"
	mw "github.com/oekit/middleware"
)

// Authn is bound by cmd wiring; nil fails closed — every protected endpoint
// returns 503 rather than degrading to anonymous access.
var Authn *authn.Service

// SetAuthn binds the authentication service.
func SetAuthn(svc *authn.Service) { Authn = svc }

// AuditHook records management operations; wired by cmd alongside Authn.
var AuditHook func(ctx context.Context, tenantID, actor, operation, objectType, objectID string, details map[string]any)

// SetAuditHook binds the audit recorder.
func SetAuditHook(fn func(ctx context.Context, tenantID, actor, operation, objectType, objectID string, details map[string]any)) {
	AuditHook = fn
}

// requirePerm authenticates the bearer token and enforces one permission.
// The actor/request_id are injected into the log context so every subsequent
// line is attributable (AUTH-001 acceptance).
func requirePerm(ctx context.Context, c *app.RequestContext, permission string) (*authn.Identity, bool) {
	if Authn == nil {
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50305, 503, "authn not wired"))
		return nil, false
	}
	token := bearerToken(c)
	id, err := Authn.Require(ctx, token, permission)
	if err != nil {
		switch {
		case err == authn.ErrUnauthenticated:
			mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(40101, 401, "authentication required"))
		case err == authn.ErrForbidden:
			// id is nil on this branch (Require failed before returning one).
			logx.CtxWarn(ctx, "rbac denied", "component", "alert-api", "permission", permission)
			mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(40301, 403, "role lacks permission: "+permission))
		default:
			mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50001, 500, "authn unavailable"))
		}
		return nil, false
	}
	// Attribute every downstream log line to the actor + request.
	logx.CtxInfo(ctx, "request authorized",
		"component", "alert-api", "actor", id.ActorID, "role", id.Role,
		"tenant", id.TenantID, "permission", permission, "request_id", traceid.FromContext(ctx))
	return id, true
}

// audit records a management operation (no-op when no hook is wired, e.g.
// unit tests without a database).
func audit(ctx context.Context, tenantID, actor, operation, objectType, objectID string, details map[string]any) {
	if AuditHook == nil {
		return
	}
	AuditHook(ctx, tenantID, actor, operation, objectType, objectID, authn.Sanitize(details))
}
