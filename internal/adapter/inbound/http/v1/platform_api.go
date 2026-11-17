// Platform internal APIs (MC-002 / UI-003 / RETN-002): edge waterline
// reporting + delivery-record queries. Handwritten (operational surface; the
// versioned domain aggregates stay IDL-driven).
package v1

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/example/alert-platform/internal/application/authn"
	"github.com/example/alert-platform/internal/application/ops"
	"github.com/example/alert-platform/internal/application/query"
	"github.com/example/alert-platform/internal/application/sourcereg"
	"github.com/google/uuid"
)

// PlatformStore is the operational store port (application/ops).
type PlatformStore = ops.Store

// OpsStore is bound by cmd wiring; nil fails closed.
var OpsStore PlatformStore

// SetOpsStore binds the operational store.
func SetOpsStore(s PlatformStore) { OpsStore = s }

// EdgeAPIKey authenticates edge heartbeats (shared secret; mTLS supersedes).
var EdgeAPIKey string

// SetEdgeAPIKey binds the edge shared key.
func SetEdgeAPIKey(key string) { EdgeAPIKey = key }

// RegisterPlatformAPI mounts the operational endpoints.
func RegisterPlatformAPI(h *server.Hertz) {
	// Edge waterline heartbeat (called by the edge uploader loop).
	//
	// AUD-009: identity is derived SERVER-SIDE.
	//   - X-Edge-Id present: edge uplink credential (P8-P0-02) — tenant from
	//     the credential, body.source_id must be in its allowed set;
	//   - legacy X-Edge-Key: shared-key auth, but (tenant, source) must be a
	//     registered pair (ReportEdgeStatus enforces it) — the body can no
	//     longer zero another tenant's waterline.
	h.POST("/edge/v1/heartbeat", func(ctx context.Context, c *app.RequestContext) {
		if OpsStore == nil {
			c.JSON(http.StatusServiceUnavailable, map[string]string{"message": "ops store not wired"})
			return
		}
		var edgeIdent *sourcereg.EdgeIdentity
		if edgeID := string(c.GetHeader("X-Edge-Id")); edgeID != "" {
			if SourceRegistry == nil {
				c.JSON(http.StatusServiceUnavailable, map[string]string{"message": "source registry not wired"})
				return
			}
			ident, err := SourceRegistry.AuthenticateEdge(ctx, bearerToken(c))
			if err != nil {
				c.JSON(http.StatusUnauthorized, map[string]string{"message": "bad edge credential"})
				return
			}
			edgeIdent = ident
		} else {
			// Hash both sides so ConstantTimeCompare sees equal length (no key
			// length disclosure).
			want := sha256.Sum256([]byte(EdgeAPIKey))
			got := sha256.Sum256([]byte(string(c.GetHeader("X-Edge-Key"))))
			if EdgeAPIKey == "" || subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
				c.JSON(http.StatusUnauthorized, map[string]string{"message": "bad edge key"})
				return
			}
		}
		var body struct {
			SourceID   string `json:"source_id"`
			TenantID   string `json:"tenant_id"`
			WalBytes   int64  `json:"wal_bytes"`
			WalPending int64  `json:"wal_pending"`
		}
		raw, _ := c.Body()
		if err := json.Unmarshal(raw, &body); err != nil || body.SourceID == "" {
			c.JSON(http.StatusBadRequest, map[string]string{"message": "source_id required"})
			return
		}
		tenantID := body.TenantID
		if edgeIdent != nil {
			// Credential is authoritative; body tenant is ignored entirely.
			tenantID = edgeIdent.TenantID
			if !edgeIdent.Authorizes(body.SourceID) {
				c.JSON(http.StatusForbidden, map[string]string{"message": "source not allowed for this edge credential"})
				return
			}
		}
		if tenantID == "" {
			c.JSON(http.StatusBadRequest, map[string]string{"message": "tenant_id required"})
			return
		}
		if err := OpsStore.ReportEdgeStatus(ctx, tenantID, body.SourceID, body.WalBytes, body.WalPending); err != nil {
			if errors.Is(err, ops.ErrSourceNotOwned) {
				c.JSON(http.StatusForbidden, map[string]string{"message": "source not registered for this tenant"})
				return
			}
			c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})

	// Event history (P0-05): queryable observations (HUATUO occurrences etc.).
	h.GET("/events/v1", func(ctx context.Context, c *app.RequestContext) {
		id, ok := requirePerm(ctx, c, authn.PermAlertsRead)
		if !ok {
			return
		}
		if QuerySvc == nil {
			c.JSON(http.StatusServiceUnavailable, map[string]string{"message": "query service not wired"})
			return
		}
		f := query.EventFilter{TenantID: id.TenantID}
		if v := string(c.Query("limit")); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				if n > 500 { // AUD-014: same cap class as the other query surfaces
					n = 500
				}
				f.Limit = n
			}
		}
		if v := string(c.Query("from")); v != "" {
			if ts, err := time.Parse(time.RFC3339, v); err == nil {
				f.From = ts
			} else {
				c.JSON(http.StatusBadRequest, map[string]string{"message": "from must be RFC3339"})
				return
			}
		}
		if v := string(c.Query("to")); v != "" {
			if ts, err := time.Parse(time.RFC3339, v); err == nil {
				f.To = ts
			} else {
				c.JSON(http.StatusBadRequest, map[string]string{"message": "to must be RFC3339"})
				return
			}
		}
		rows, err := QuerySvc.ListEvents(ctx, f)
		if err != nil {
			c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
			return
		}
		type ev struct {
			EventID     string            `json:"event_id"`
			TransportID string            `json:"transport_id"`
			Kind        string            `json:"kind"`
			Signal      string            `json:"signal"`
			Severity    string            `json:"severity,omitempty"`
			AlertName   string            `json:"alert_name"`
			ClusterID   string            `json:"cluster_id,omitempty"`
			EntityType  string            `json:"entity_type,omitempty"`
			EntityUID   string            `json:"entity_uid,omitempty"`
			Namespace   string            `json:"namespace,omitempty"`
			Labels      map[string]string `json:"labels,omitempty"`
			OccurredAt  string            `json:"occurred_at"`
		}
		out := make([]ev, 0, len(rows))
		for _, r := range rows {
			out = append(out, ev{
				EventID: r.EventID, TransportID: r.TransportID, Kind: r.Kind, Signal: r.Signal,
				Severity: r.Severity, AlertName: r.AlertName, ClusterID: r.ClusterID,
				EntityType: r.EntityType, EntityUID: r.EntityUID, Namespace: r.Namespace,
				Labels: r.Labels, OccurredAt: r.OccurredAt.UTC().Format(time.RFC3339),
			})
		}
		c.JSON(http.StatusOK, map[string]any{"events": out})
	})

	// Delivery records (UI-003: 发送记录/失败过滤).
	h.GET("/notifications/v1", func(ctx context.Context, c *app.RequestContext) {
		id, ok := requirePerm(ctx, c, authn.PermNotificationsRead)
		if !ok {
			return
		}
		if OpsStore == nil {
			c.JSON(http.StatusServiceUnavailable, map[string]string{"message": "ops store not wired"})
			return
		}
		limit := 50
		if v := string(c.Query("limit")); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 200 {
				limit = n
			}
		}
		rows, err := OpsStore.ListNotificationTasks(ctx, id.TenantID, string(c.Query("status")), limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
			return
		}
		type nv struct {
			ID          string `json:"id"`
			ChannelID   string `json:"channel_id"`
			Receiver    string `json:"receiver_id"`
			Status      string `json:"status"`
			Attempts    int    `json:"attempts"`
			Type        string `json:"notification_type"`
			CreatedAt   string `json:"created_at"`
			NextAttempt string `json:"next_attempt_at,omitempty"`
			Message     string `json:"message"`
		}
		out := make([]nv, 0, len(rows))
		for _, r := range rows {
			out = append(out, nv{
				ID: r.ID, ChannelID: r.ChannelID, Receiver: r.Receiver, Status: r.Status,
				Attempts: r.Attempts, Type: r.Type,
				CreatedAt:   r.CreatedAt.UTC().Format(time.RFC3339),
				NextAttempt: r.NextAttemptAt.UTC().Format(time.RFC3339),
				Message:     r.Message,
			})
		}
		c.JSON(http.StatusOK, map[string]any{"tasks": out})
	})

	// Manual replay of a dead-lettered notification (AUD-002). Audited;
	// requires notifications:write (maintainer/sre/platform_admin).
	h.POST("/notifications/v1/:id/replay", func(ctx context.Context, c *app.RequestContext) {
		id, ok := requirePerm(ctx, c, authn.PermNotificationsWrite)
		if !ok {
			return
		}
		if OpsStore == nil {
			c.JSON(http.StatusServiceUnavailable, map[string]string{"message": "ops store not wired"})
			return
		}
		taskID := c.Param("id")
		if _, err := uuid.Parse(taskID); err != nil {
			c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid notification id"})
			return
		}
		replayed, err := OpsStore.ReplayNotifyTask(ctx, id.TenantID, taskID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
			return
		}
		if !replayed {
			c.JSON(http.StatusNotFound, map[string]string{"message": "no failed notification with that id"})
			return
		}
		audit(ctx, id.TenantID, id.ActorID, "notification.replay", "notification", taskID, nil)
		c.JSON(http.StatusAccepted, map[string]any{"task_id": taskID, "status": "pending"})
	})

	// Edge waterlines for the cluster health page (MC-002).
	h.GET("/edge/v1/status", func(ctx context.Context, c *app.RequestContext) {
		id, ok := requirePerm(ctx, c, authn.PermSourcesRead)
		if !ok {
			return
		}
		if OpsStore == nil {
			c.JSON(http.StatusServiceUnavailable, map[string]string{"message": "ops store not wired"})
			return
		}
		rows, err := OpsStore.ListEdgeStatus(ctx, id.TenantID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
			return
		}
		type ev struct {
			SourceID   string `json:"source_id"`
			WalBytes   int64  `json:"wal_bytes"`
			WalPending int64  `json:"wal_pending"`
			UpdatedAt  string `json:"updated_at"`
		}
		out := make([]ev, 0, len(rows))
		for _, r := range rows {
			out = append(out, ev{
				SourceID: r.SourceID, WalBytes: r.WalBytes, WalPending: r.WalPending,
				UpdatedAt: r.UpdatedAt.UTC().Format(time.RFC3339),
			})
		}
		c.JSON(http.StatusOK, map[string]any{"edges": out})
	})
}
