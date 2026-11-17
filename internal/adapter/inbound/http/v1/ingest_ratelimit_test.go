package v1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/example/alert-platform/internal/application/ingest"
	"github.com/example/alert-platform/internal/application/sourcereg"
)

// AUD-016: the bucket refills at the configured rate and denies with a
// Retry-After hint; disabled limiters always allow.
func TestIngestLimiterTokenBucket(t *testing.T) {
	l := NewIngestLimiter(1, 2)
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	l.now = func() time.Time { return now }
	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow("t|source:s"); !ok {
			t.Fatalf("burst request %d denied", i)
		}
	}
	if ok, wait := l.Allow("t|source:s"); ok || wait < time.Second {
		t.Fatalf("over-budget: ok=%v wait=%s", ok, wait)
	}
	// A different key has its own bucket.
	if ok, _ := l.Allow("t|source:other"); !ok {
		t.Fatal("independent key denied")
	}
	now = now.Add(time.Second)
	if ok, _ := l.Allow("t|source:s"); !ok {
		t.Fatal("bucket did not refill after one second")
	}
	// rate <= 0 means unlimited.
	off := NewIngestLimiter(0, 1)
	for i := 0; i < 100; i++ {
		if ok, _ := off.Allow("k"); !ok {
			t.Fatal("disabled limiter denied")
		}
	}
}

func TestIngestRateLimitMiddleware429(t *testing.T) {
	reg := &fakeRegistry{token: "aps_edge-cred"}
	reg.mu.Lock()
	reg.source = &sourcereg.Source{
		SourceID: "edge-src-1", TenantID: "tenant-edge", ClusterID: "cluster-edge",
		SourceType: 1, Enabled: true,
	}
	reg.mu.Unlock()
	SetSourceRegistry(sourcereg.NewService(reg))
	SetIngest(ingest.NewService(&capturePublisher{}))
	SetIngestLimiter(NewIngestLimiter(0.0001, 1)) // one token, effectively no refill
	t.Cleanup(func() {
		SetSourceRegistry(nil)
		SetIngest(nil)
		SetIngestLimiter(nil)
	})

	h := server.Default(server.WithHostPorts(":0"))
	g := h.Group("/ingest", IngestAuthMiddleware(), IngestRateLimit())
	g.POST("/v1/events:batch", PushBatch)

	body := ingestBatchBody("tenant-edge", "edge-src-1")
	post := func() *ut.ResponseRecorder {
		return ut.PerformRequest(h.Engine, http.MethodPost, "/ingest/v1/events:batch",
			&ut.Body{Body: strings.NewReader(body), Len: len(body)},
			ut.Header{Key: "Content-Type", Value: "application/json"},
			ut.Header{Key: "Authorization", Value: "Bearer aps_edge-cred"})
	}
	if w := post(); w.Code != http.StatusAccepted {
		t.Fatalf("first push: %d body=%s", w.Code, w.Body.String())
	}
	w := post()
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("second push: %d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("Retry-After") == "" {
		t.Fatal("429 lacks Retry-After")
	}
}

// AUD-007: an oversized event is a 400 with detail, never a broker 500, and it
// must not reach the publisher.
func TestPushBatchOversizedEventIs400(t *testing.T) {
	h, _, pub := newIngestAuthRouter(t)
	ev := map[string]any{
		"kind":       1,
		"signal":     1,
		"severity":   1,
		"alert_name": "LatencyHigh",
		"event_key":  "ek-big",
		"tenant_id":  "tenant-edge",
		"source":     map[string]any{"source_id": "edge-src-1"},
		"labels":     map[string]any{"blob": strings.Repeat("x", ingest.MaxEventBytes+2048)},
	}
	raw, _ := json.Marshal(map[string]any{"batch_id": "b-big", "events": []any{ev}})
	body := string(raw)
	w := ut.PerformRequest(h.Engine, http.MethodPost, "/ingest/v1/events:batch",
		&ut.Body{Body: strings.NewReader(body), Len: len(body)},
		ut.Header{Key: "Content-Type", Value: "application/json"},
		ut.Header{Key: "Authorization", Value: "Bearer aps_edge-cred"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized event: %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), fmt.Sprintf("%d-byte", ingest.MaxEventBytes)) {
		t.Fatalf("400 lacks size detail: %s", w.Body.String())
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 0 {
		t.Fatalf("oversized event reached the publisher: %d", len(pub.events))
	}
}
