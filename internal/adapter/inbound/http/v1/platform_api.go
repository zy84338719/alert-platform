// Platform internal APIs (MC-002 / UI-003 / RETN-002): edge waterline
// reporting + delivery-record queries. Handwritten (operational surface; the
// versioned domain aggregates stay IDL-driven).
package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/example/alert-platform/internal/application/authn"
	"github.com/example/alert-platform/internal/application/ops"
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
	h.POST("/edge/v1/heartbeat", func(ctx context.Context, c *app.RequestContext) {
		if EdgeAPIKey == "" || string(c.GetHeader("X-Edge-Key")) != EdgeAPIKey {
			c.JSON(http.StatusUnauthorized, map[string]string{"message": "bad edge key"})
			return
		}
		if OpsStore == nil {
			c.JSON(http.StatusServiceUnavailable, map[string]string{"message": "ops store not wired"})
			return
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
		if err := OpsStore.ReportEdgeStatus(ctx, body.TenantID, body.SourceID, body.WalBytes, body.WalPending); err != nil {
			c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, map[string]string{"status": "ok"})
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
				CreatedAt: r.CreatedAt.UTC().Format(time.RFC3339),
				NextAttempt: r.NextAttemptAt.UTC().Format(time.RFC3339),
				Message:   r.Message,
			})
		}
		c.JSON(http.StatusOK, map[string]any{"tasks": out})
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
