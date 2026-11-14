package v1

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/example/alert-platform/internal/application/ingest"
	"github.com/example/alert-platform/internal/application/sourcereg"
	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// P0-03 (rh-p0-03) 失败测试：内部 Ingest 鉴权与租户/来源绑定。
// POST /ingest/v1/events:batch 此前直接信任 payload 自报的 tenant_id/source_id
// （路由中间件返回 nil，全局中间件只有日志/恢复/Trace）—— 未授权调用可伪造
// 任意租户告警。修复后：未认证 401；凭证身份与 payload 不一致（越权/跨租户
// 伪造）403；合法凭证 2xx 且入库身份取自凭证（服务端权威覆盖）。

func newIngestAuthRouter(t *testing.T) (*server.Hertz, *fakeRegistry, *capturePublisher) {
	t.Helper()
	reg := &fakeRegistry{token: "aps_edge-cred"}
	reg.mu.Lock()
	reg.source = &sourcereg.Source{
		SourceID: "edge-src-1", TenantID: "tenant-edge", ClusterID: "cluster-edge",
		SourceType: eventv1.SourceType_SOURCE_TYPE_DEEPFLOW, Enabled: true,
	}
	reg.mu.Unlock()
	SetSourceRegistry(sourcereg.NewService(reg))
	pub := &capturePublisher{}
	SetIngest(ingest.NewService(pub))
	h := server.Default(server.WithHostPorts(":0"))
	// 与 routes.go 的挂载方式一致：/ingest 组挂 IngestAuthMiddleware
	// （_ingestMw 的实现），组内 POST /v1/events:batch -> PushBatch。
	g := h.Group("/ingest", IngestAuthMiddleware())
	g.POST("/v1/events:batch", PushBatch)
	t.Cleanup(func() { SetSourceRegistry(nil); SetIngest(nil) })
	return h, reg, pub
}

func ingestBatchBody(tenant, source string) string {
	// Timestamps omitted: the real edge wire format serializes timestamppb as
	// {"seconds":..,"nanos":..} objects (edgegw json.Marshal); strings are not
	// bindable. Store-side orNow() fills absent times.
	ev := map[string]any{
		"kind":       1, // EVENT_KIND_STATEFUL
		"signal":     1, // EVENT_SIGNAL_FIRING
		"severity":   1, // SEVERITY_CRITICAL
		"alert_name": "LatencyHigh",
		"event_key":  "ek-1",
		"resource":   map[string]any{"resource_type": "pod", "resource_uid": "pod-1"},
	}
	if tenant != "" {
		ev["tenant_id"] = tenant
	}
	if source != "" {
		ev["source"] = map[string]any{"source_id": source}
	}
	body, _ := json.Marshal(map[string]any{"batch_id": "b-1", "events": []any{ev}})
	return string(body)
}

func postIngest(t *testing.T, h *server.Hertz, token, body string) *ut.ResponseRecorder {
	t.Helper()
	hdr := []ut.Header{{Key: "Content-Type", Value: "application/json"}}
	if token != "" {
		hdr = append(hdr, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	}
	return ut.PerformRequest(h.Engine, http.MethodPost, "/ingest/v1/events:batch",
		&ut.Body{Body: strings.NewReader(body), Len: len(body)}, hdr...)
}

func TestIngestAuthMatrix(t *testing.T) {
	h, reg, pub := newIngestAuthRouter(t)

	// ① 未认证 -> 401
	if w := postIngest(t, h, "", ingestBatchBody("tenant-edge", "edge-src-1")); w.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous ingest: status %d, want 401", w.Code)
	}
	// ② 未知凭证 -> 401（未知与吊销不可区分）
	if w := postIngest(t, h, "aps_wrong", ingestBatchBody("tenant-edge", "edge-src-1")); w.Code != http.StatusUnauthorized {
		t.Fatalf("unknown credential: status %d, want 401", w.Code)
	}
	// ③ 合法凭证 + payload 跨租户伪造 -> 403
	if w := postIngest(t, h, reg.token, ingestBatchBody("tenant-other", "edge-src-1")); w.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant forgery: status %d, want 403", w.Code)
	}
	// ④ 合法凭证 + payload 越权来源（非凭证绑定的 source）-> 403
	if w := postIngest(t, h, reg.token, ingestBatchBody("tenant-edge", "source-of-someone-else")); w.Code != http.StatusForbidden {
		t.Fatalf("cross-source forgery: status %d, want 403", w.Code)
	}
	// ⑤ 合法凭证 + payload 身份一致 -> 202
	if w := postIngest(t, h, reg.token, ingestBatchBody("tenant-edge", "edge-src-1")); w.Code != http.StatusAccepted {
		t.Fatalf("legit push: status %d body %s, want 202", w.Code, w.Body.String())
	}
	// ⑥ 合法凭证 + payload 自报身份缺席 -> 202，入库身份取自凭证
	if w := postIngest(t, h, reg.token, ingestBatchBody("", "")); w.Code != http.StatusAccepted {
		t.Fatalf("credential-only push: status %d body %s, want 202", w.Code, w.Body.String())
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 2 {
		t.Fatalf("published %d events, want 2", len(pub.events))
	}
	for i, ev := range pub.events {
		if ev.GetTenantId() != "tenant-edge" || ev.GetSource().GetSourceId() != "edge-src-1" {
			t.Fatalf("event %d identity = (%q,%q), want credential-derived (tenant-edge,edge-src-1)",
				i, ev.GetTenantId(), ev.GetSource().GetSourceId())
		}
	}
	if got := pub.events[0].GetResource().GetClusterId(); got != "cluster-edge" {
		t.Fatalf("cluster = %q, want credential-derived cluster-edge", got)
	}
}

// 禁用来源 -> 403（吊销/禁用的凭证不能继续上传）。
func TestIngestAuthRejectsDisabledSource(t *testing.T) {
	h, reg, _ := newIngestAuthRouter(t)
	reg.mu.Lock()
	reg.source.Enabled = false
	reg.mu.Unlock()
	if w := postIngest(t, h, reg.token, ingestBatchBody("tenant-edge", "edge-src-1")); w.Code != http.StatusForbidden {
		t.Fatalf("disabled source: status %d, want 403", w.Code)
	}
}

// 未接注册表（误装配）-> 503 fail closed，绝不匿名放行。
func TestIngestAuthFailsClosedWithoutRegistry(t *testing.T) {
	reg := &fakeRegistry{token: "aps_edge-cred"}
	SetSourceRegistry(sourcereg.NewService(reg))
	SetSourceRegistry(nil) // 模拟未接线
	SetIngest(ingest.NewService(&capturePublisher{}))
	defer SetIngest(nil)
	h := server.Default(server.WithHostPorts(":0"))
	g := h.Group("/ingest", IngestAuthMiddleware())
	g.POST("/v1/events:batch", PushBatch)
	if w := postIngest(t, h, "aps_any", ingestBatchBody("t", "s")); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("unwired registry: status %d, want 503", w.Code)
	}
}
