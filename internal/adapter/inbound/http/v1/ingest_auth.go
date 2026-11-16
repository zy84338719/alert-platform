// P0-03 (rh-p0-03): authentication + tenant/source binding for the INTERNAL
// ingest surface (/ingest/v1/events:batch). Before this hardening the route
// middleware returned nil and the handler trusted the payload's self-declared
// tenant_id/source_id — an unauthenticated caller could forge alerts for any
// tenant. The edge now authenticates with a source-registry credential and
// the SERVER identity is authoritative: absent payload identity is filled
// from the credential, mismatching identity is rejected with 403.
package v1

import (
	"context"
	"fmt"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/oekit/logx"
	mw "github.com/oekit/middleware"
	ekiterrors "github.com/oekit/pkg/errors"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/application/ingest"
	"github.com/example/alert-platform/internal/application/sourcereg"
)

// ctxKeyAuthSource stores the authenticated source inside the request context.
const ctxKeyAuthSource = "alert.auth.source"

// ctxKeyAuthEdge stores the authenticated edge uplink identity (P8-P0-02).
const ctxKeyAuthEdge = "alert.auth.edge"

// IngestAuthMiddleware authenticates the pusher via the source registry and
// binds the resolved identity to the request. Fail-closed at every step:
// unwired registry -> 503, missing/unknown credential -> 401 (deliberately
// vague), disabled source -> 403.
//
// Two credential kinds are accepted (P8-P0-02):
//   - X-Edge-Id present: an EDGE uplink credential — ONE token scoped to a
//     tenant and an allowed source subset (multi-source edges, rotation via
//     token rows);
//   - otherwise: a per-SOURCE credential (single-source edges, external
//     webhook push, legacy deployments).
func IngestAuthMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if SourceRegistry == nil {
			mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50301, 503, "source registry not wired"))
			c.Abort()
			return
		}
		if edgeID := string(c.GetHeader("X-Edge-Id")); edgeID != "" {
			ident, err := SourceRegistry.AuthenticateEdge(ctx, bearerToken(c))
			if err != nil {
				authFailure(ctx, c, err)
				return
			}
			c.Set(ctxKeyAuthEdge, ident)
			return
		}
		src, err := SourceRegistry.Authenticate(ctx, bearerToken(c))
		if err != nil {
			authFailure(ctx, c, err)
			return
		}
		c.Set(ctxKeyAuthSource, src)
	}
}

// authFailure maps registry sentinels to the fail-closed HTTP responses and
// accounts for them in the ingest rejection metric (P9-OBS-02).
func authFailure(ctx context.Context, c *app.RequestContext, err error) {
	switch err {
	case sourcereg.ErrUnauthenticated:
		ingest.RecordRejected("unauthenticated")
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(40101, 401, "authentication required"))
	case sourcereg.ErrDisabled:
		ingest.RecordRejected("disabled")
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(40302, 403, "source disabled"))
	default:
		ingest.RecordRejected("registry")
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50001, 500, "registry unavailable"))
	}
	c.Abort()
}

// authenticatedSource returns the credential-bound identity installed by
// IngestAuthMiddleware (nil when the middleware did not run).
func authenticatedSource(c *app.RequestContext) *sourcereg.Source {
	v, ok := c.Get(ctxKeyAuthSource)
	if !ok {
		return nil
	}
	src, _ := v.(*sourcereg.Source)
	return src
}

// authenticatedEdge returns the edge uplink identity installed by the
// middleware (P8-P0-02), or nil when a source credential was used.
func authenticatedEdge(c *app.RequestContext) *sourcereg.EdgeIdentity {
	v, ok := c.Get(ctxKeyAuthEdge)
	if !ok {
		return nil
	}
	ident, _ := v.(*sourcereg.EdgeIdentity)
	return ident
}

// enforceIngestIdentity binds every event of the batch to the authenticated
// identity (P0-03). Absent tenant/source/cluster are filled from the
// credential; non-empty values that DIFFER from the credential are forgery
// and reject the whole batch with 403. Returns false when rejected.
func enforceIngestIdentity(ctx context.Context, events []*eventv1.AlertEvent, src *sourcereg.Source) bool {
	for i, ev := range events {
		if ev.GetTenantId() != "" && ev.GetTenantId() != src.TenantID {
			rejectIdentity(ctx, i, fmt.Sprintf("tenant %q does not match the authenticated credential", ev.GetTenantId()))
			return false
		}
		if sid := ev.GetSource().GetSourceId(); sid != "" && sid != src.SourceID {
			rejectIdentity(ctx, i, fmt.Sprintf("source %q does not match the authenticated credential", sid))
			return false
		}
		ev.TenantId = src.TenantID
		if ev.GetSource() == nil {
			ev.Source = &eventv1.SourceRef{}
		}
		ev.Source.SourceId = src.SourceID
		// Cluster is registry-authoritative for the source: fill when absent,
		// overwrite when the credential carries one.
		if src.ClusterID != "" && ev.GetResource() != nil {
			ev.Resource.ClusterId = src.ClusterID
		}
	}
	return true
}

// enforceEdgeIngestIdentity binds every event to an EDGE uplink credential
// (P8-P0-02): the batch may mix sources, but each event's source must be in
// the credential's allowed set (enabled-filtered at resolution time) and the
// tenant must match. A missing source id is rejected — an edge credential
// cannot pick a source on the caller's behalf. Returns false when rejected.
func enforceEdgeIngestIdentity(ctx context.Context, events []*eventv1.AlertEvent, ident *sourcereg.EdgeIdentity) bool {
	for i, ev := range events {
		if ev.GetTenantId() != "" && ev.GetTenantId() != ident.TenantID {
			rejectIdentity(ctx, i, fmt.Sprintf("tenant %q does not match the edge credential", ev.GetTenantId()))
			return false
		}
		sid := ev.GetSource().GetSourceId()
		if sid == "" {
			rejectIdentity(ctx, i, "event carries no source_id (edge credentials cannot infer one)")
			return false
		}
		if !ident.Authorizes(sid) {
			rejectIdentity(ctx, i, fmt.Sprintf("source %q is not in the edge credential's allowed set", sid))
			return false
		}
		ev.TenantId = ident.TenantID
	}
	return true
}

func rejectIdentity(ctx context.Context, i int, why string) {
	logx.CtxWarn(ctx, "ingest identity forgery rejected",
		"component", "alert-api", "event_index", i, "reason", why)
}
