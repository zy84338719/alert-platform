// Audit query endpoint (AUTH-003): GET /audit/v1 returns the tenant's recent
// management operations. Handwritten (map-shaped response; the IDL surface
// covers the versioned domain aggregates).
package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/example/alert-platform/internal/application/authn"
)

// AuditList is bound by cmd wiring (postgres.AuthnStore.ListAudit).
var AuditList func(ctx context.Context, tenantID string, limit int) ([]map[string]any, error)

// SetAuditList binds the audit query function.
func SetAuditList(fn func(ctx context.Context, tenantID string, limit int) ([]map[string]any, error)) {
	AuditList = fn
}

// RegisterAuditAPI mounts GET /audit/v1 and the embedded console.
func RegisterAuditAPI(h *server.Hertz) {
	h.GET("/audit/v1", func(ctx context.Context, c *app.RequestContext) {
		id, ok := requirePerm(ctx, c, authn.PermAuditRead)
		if !ok {
			return
		}
		if AuditList == nil {
			c.JSON(http.StatusServiceUnavailable, map[string]string{"message": "audit store not wired"})
			return
		}
		limit := 100
		if v := string(c.Query("limit")); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
				limit = n
			}
		}
		entries, err := AuditList(ctx, id.TenantID, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
			return
		}
		if entries == nil {
			entries = []map[string]any{}
		}
		c.JSON(http.StatusOK, map[string]any{"entries": entries})
	})
}

// jsonMarker keeps the encoding import meaningful for future detail fields.
var _ = json.Marshal
