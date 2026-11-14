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
	"github.com/example/alert-platform/internal/application/sourcereg"
)

// ctxKeyAuthSource stores the authenticated source inside the request context.
const ctxKeyAuthSource = "alert.auth.source"

// IngestAuthMiddleware authenticates the pusher via the source registry and
// binds the resolved identity to the request. Fail-closed at every step:
// unwired registry -> 503, missing/unknown credential -> 401 (deliberately
// vague), disabled source -> 403.
func IngestAuthMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if SourceRegistry == nil {
			mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50301, 503, "source registry not wired"))
			c.Abort()
			return
		}
		src, err := SourceRegistry.Authenticate(ctx, bearerToken(c))
		if err != nil {
			switch {
			case err == sourcereg.ErrUnauthenticated:
				mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(40101, 401, "authentication required"))
			case err == sourcereg.ErrDisabled:
				mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(40302, 403, "source disabled"))
			default:
				mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50001, 500, "registry unavailable"))
			}
			c.Abort()
			return
		}
		c.Set(ctxKeyAuthSource, src)
	}
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

func rejectIdentity(ctx context.Context, i int, why string) {
	logx.CtxWarn(ctx, "ingest identity forgery rejected",
		"component", "alert-api", "event_index", i, "reason", why)
}
