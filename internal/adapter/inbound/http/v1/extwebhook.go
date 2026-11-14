// External vendor webhooks (ING-001/002/003): these endpoints speak VENDOR
// JSON (DeepFlow bridge / Alertmanager), so unlike the IDL-generated handlers
// they are handwritten: authenticate the bearer credential against the source
// registry, translate to canonical events, then reuse the same durable ingest
// path as trusted edges. Tenant identity is always the credential-derived
// registry value — vendor payload claims never reach the canonical event.
package v1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/oekit/logx"
	mw "github.com/oekit/middleware"

	"github.com/example/alert-platform/internal/adapter/translate"
	"github.com/example/alert-platform/internal/application/sourcereg"
	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	ekiterrors "github.com/oekit/pkg/errors"
)

// SourceRegistry is bound by cmd wiring; nil fails closed (503) so a
// mis-wired binary can never accept unauthenticated vendor pushes.
var SourceRegistry *sourcereg.Service

// SetSourceRegistry binds the source registry used by external webhooks.
func SetSourceRegistry(svc *sourcereg.Service) { SourceRegistry = svc }

// maxVendorBody caps vendor webhook bodies (labels-limited canonical events
// still allow sizable raw payloads; 4 MiB matches Edge v1 buffering bounds).
const maxVendorBody = 4 << 20

// RegisterExternalWebhooks mounts the two vendor ingest endpoints.
func RegisterExternalWebhooks(h *server.Hertz) {
	h.POST("/ingest/external/deepflow/v7_2/events", deepFlowWebhook)
	h.POST("/ingest/external/alertmanager/v1/events", alertManagerWebhook)
}

// bearerToken extracts the Authorization: Bearer value.
func bearerToken(c *app.RequestContext) string {
	auth := string(c.GetHeader("Authorization"))
	if v, ok := strings.CutPrefix(auth, "Bearer "); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

// authenticate resolves the credential and enforces the endpoint's source
// type. Every failure closes the request — no anonymous vendor pushes.
func authenticate(ctx context.Context, c *app.RequestContext, want eventv1.SourceType) *sourcereg.Source {
	if SourceRegistry == nil {
		failVendor(ctx, c, ekiterrors.NewBizErrorWithStatus(50301, 503, "source registry not wired"))
		return nil
	}
	src, err := SourceRegistry.Authenticate(ctx, bearerToken(c))
	if err != nil {
		switch {
		case err == sourcereg.ErrUnauthenticated:
			// Deliberately vague: unknown vs revoked must be indistinguishable.
			failVendor(ctx, c, ekiterrors.NewBizErrorWithStatus(40101, 401, "authentication required"))
		case err == sourcereg.ErrDisabled:
			failVendor(ctx, c, ekiterrors.NewBizErrorWithStatus(40302, 403, "source disabled"))
		default:
			failVendor(ctx, c, ekiterrors.NewBizErrorWithStatus(50001, 500, "registry unavailable"))
		}
		return nil
	}
	if src.SourceType != want {
		failVendor(ctx, c, ekiterrors.NewBizErrorWithStatus(40303, 403,
			fmt.Sprintf("credential belongs to a %s source", strings.ToLower(src.SourceType.String()))))
		return nil
	}
	return src
}

func vendorBatchID(sourceID string, body []byte) string {
	sum := sha256.Sum256(append([]byte(sourceID+"|"), body...))
	// Deterministic: the vendor retry of an identical body reuses the same
	// batch (and therefore the same transport ids), so consumer receipts
	// absorb the duplicate (docs/03 §2).
	return "ext-" + hex.EncodeToString(sum[:12])
}

func pushTranslated(ctx context.Context, c *app.RequestContext, events []*eventv1.AlertEvent, sourceID string, body []byte) {
	if len(events) == 0 {
		failVendor(ctx, c, ekiterrors.NewBadRequestError("no events in payload"))
		return
	}
	if Ingest == nil {
		failVendor(ctx, c, ekiterrors.NewBizErrorWithStatus(50300, 503, "ingest service not wired"))
		return
	}
	batchID, err := Ingest.PushBatch(ctx, vendorBatchID(sourceID, body), events)
	if err != nil {
		failVendor(ctx, c, ekiterrors.NewBizErrorWithStatus(50302, 503, "durably enqueuing failed; retry"))
		return
	}
	logx.CtxInfo(ctx, "vendor batch accepted",
		"component", "alert-api", "source_id", sourceID, "events", len(events), "batch_id", batchID)
	c.JSON(consts.StatusAccepted, map[string]string{"durable_batch_id": batchID})
}

func failVendor(ctx context.Context, c *app.RequestContext, err error) {
	logx.CtxWarn(ctx, "vendor webhook rejected", "component", "alert-api", logx.ErrKey, err)
	mw.Fail(ctx, c, err)
}

// deepFlowWebhook handles POST /ingest/external/deepflow/v7_2/events.
func deepFlowWebhook(ctx context.Context, c *app.RequestContext) {
	src := authenticate(ctx, c, eventv1.SourceType_SOURCE_TYPE_DEEPFLOW)
	if src == nil {
		return
	}
	body, _ := c.Body()
	if len(body) > maxVendorBody {
		failVendor(ctx, c, ekiterrors.NewBadRequestError("body too large"))
		return
	}
	events, err := translate.DeepFlowBatch(src.TenantID, src.SourceID, src.ClusterID, body, time.Now())
	if err != nil {
		failVendor(ctx, c, ekiterrors.NewBadRequestError(err.Error()))
		return
	}
	pushTranslated(ctx, c, events, src.SourceID, body)
}

// alertManagerWebhook handles POST /ingest/external/alertmanager/v1/events.
func alertManagerWebhook(ctx context.Context, c *app.RequestContext) {
	src := authenticate(ctx, c, eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER)
	if src == nil {
		return
	}
	body, _ := c.Body()
	if len(body) > maxVendorBody {
		failVendor(ctx, c, ekiterrors.NewBadRequestError("body too large"))
		return
	}
	events, err := translate.AlertManagerBatch(src.TenantID, src.SourceID, src.ClusterID, body, time.Now())
	if err != nil {
		failVendor(ctx, c, ekiterrors.NewBadRequestError(err.Error()))
		return
	}
	pushTranslated(ctx, c, events, src.SourceID, body)
}
