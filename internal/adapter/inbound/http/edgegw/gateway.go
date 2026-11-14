// Package edgegw is the edge gateway HTTP surface (EDGE-001): authenticate
// the vendor push against the LOCAL static source registry (the edge cannot
// reach the central PG), translate to canonical events, and append to the WAL
// — 202 is returned ONLY after the fsync. WAL full -> 503 (fail closed, the
// source retries) plus a self-alert line.
package edgegw

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/example/alert-platform/internal/adapter/translate"
	"github.com/example/alert-platform/internal/application/edge"
	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/oekit/logx"
	mw "github.com/oekit/middleware"
	ekiterrors "github.com/oekit/pkg/errors"
)

// LocalSource is one statically configured upstream (edge-sources.json).
type LocalSource struct {
	TokenHash string `json:"token_hash"` // sha256 hex of the bearer token
	TenantID  string `json:"tenant_id"`
	SourceID  string `json:"source_id"`
	ClusterID string `json:"cluster_id"`
	Type      string `json:"type"` // deepflow|alertmanager
}

type registry struct {
	mu      sync.RWMutex
	byToken map[string]LocalSource
}

func (r *registry) load(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var cfg struct {
		Sources []LocalSource `json:"sources"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	m := map[string]LocalSource{}
	for _, s := range cfg.Sources {
		m[s.TokenHash] = s
	}
	r.mu.Lock()
	r.byToken = m
	r.mu.Unlock()
	return nil
}

func (r *registry) auth(token string) (LocalSource, bool) {
	sum := sha256.Sum256([]byte(token))
	h := hex.EncodeToString(sum[:])
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.byToken[h]
	return s, ok
}

// Gateway wires the receiver over a WAL buffer.
type Gateway struct {
	reg *registry
	wal Buffer
}

// Buffer is the WAL append port.
type Buffer interface {
	Append(rec edge.Record) (edge.Record, error)
	Stats() (bytes int64, pending int)
}

// NewGateway builds the receiver from a sources file.
func NewGateway(sourcesFile string, buf Buffer) (*Gateway, error) {
	g := &Gateway{reg: &registry{byToken: map[string]LocalSource{}}, wal: buf}
	if sourcesFile != "" {
		if err := g.reg.load(sourcesFile); err != nil {
			return nil, err
		}
	}
	return g, nil
}

// Register mounts the vendor webhook routes on the edge's own Hertz server.
func (g *Gateway) Register(h *server.Hertz) {
	h.POST("/ingest/external/deepflow/v7_2/events", g.vendor(eventv1.SourceType_SOURCE_TYPE_DEEPFLOW))
	h.POST("/ingest/external/alertmanager/v1/events", g.vendor(eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER))
	h.GET("/edge/stats", func(ctx context.Context, c *app.RequestContext) {
		bytes, pending := g.wal.Stats()
		c.JSON(consts.StatusOK, map[string]any{"wal_bytes": bytes, "wal_pending": pending})
	})
}

func bearer(c *app.RequestContext) string {
	auth := string(c.GetHeader("Authorization"))
	if v, ok := strings.CutPrefix(auth, "Bearer "); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func (g *Gateway) vendor(want eventv1.SourceType) func(ctx context.Context, c *app.RequestContext) {
	return func(ctx context.Context, c *app.RequestContext) {
		src, ok := g.reg.auth(bearer(c))
		var wantType string
		switch want {
		case eventv1.SourceType_SOURCE_TYPE_DEEPFLOW:
			wantType = "deepflow"
		case eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER:
			wantType = "alertmanager"
		}
		if !ok || !strings.EqualFold(src.Type, wantType) {
			mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(40101, 401, "unknown credential"))
			return
		}
		body, _ := c.Body()
		var events []*eventv1.AlertEvent
		var err error
		switch want {
		case eventv1.SourceType_SOURCE_TYPE_DEEPFLOW:
			events, err = translate.DeepFlowBatch(src.TenantID, src.SourceID, src.ClusterID, body, time.Now())
		case eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER:
			events, err = translate.AlertManagerBatch(src.TenantID, src.SourceID, src.ClusterID, body, time.Now())
		}
		if err != nil {
			mw.Fail(ctx, c, ekiterrors.NewBadRequestError(err.Error()))
			return
		}
		batchID, batch, err := marshalBatch(events)
		if err != nil {
			mw.Fail(ctx, c, ekiterrors.NewBadRequestError(err.Error()))
			return
		}
		// fsync-before-ack: Append Syncs before returning (EDGE-001).
		if _, err := g.wal.Append(edge.Record{ID: batchID, Body: batch, SourceID: src.SourceID}); err != nil {
			// Fail closed AND raise the edge's own alert (disk waterline).
			logx.CtxError(ctx, "EDGE SELF-ALERT: WAL append failed — refusing to ack vendor data",
				logx.ErrKey, err, "source", src.SourceID, "batch", batchID)
			mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50309, http.StatusServiceUnavailable,
				"edge buffer unavailable; retry"))
			return
		}
		c.JSON(consts.StatusAccepted, map[string]string{"durable_batch_id": batchID})
	}
}

// marshalBatch serializes canonical events exactly like the central
// PushBatchRequest wire format, with a deterministic batch id.
func marshalBatch(events []*eventv1.AlertEvent) (string, []byte, error) {
	if len(events) == 0 {
		return "", nil, fmt.Errorf("no events")
	}
	id := events[0].GetTransportId()
	if id == "" {
		id = fmt.Sprintf("edge-%d", time.Now().UnixNano())
	}
	payload := map[string]any{"batch_id": id, "events": events}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", nil, err
	}
	return id, raw, nil
}
