package router

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	httpv1 "github.com/example/alert-platform/internal/adapter/inbound/http/v1"
	"github.com/example/alert-platform/internal/application/ingest"
)

// P0-03 (rh-p0-03): the PRODUCTION wiring (GeneratedRegister -> routes.go ->
// _ingestMw) must authenticate /ingest/v1/events:batch — an anonymous push is
// rejected before any handler logic runs.
func TestGeneratedRoutesIngestRequiresAuth(t *testing.T) {
	// Registry intentionally NOT wired: fail-closed 503 proves the middleware
	// runs on the generated route (an anonymous surface would 400 from bind
	// validation instead).
	h := server.Default(server.WithHostPorts(":0"))
	httpv1.SetIngest(ingest.NewService(stubPub{}))
	defer httpv1.SetIngest(nil)
	GeneratedRegister(h)

	body := `{"batch_id":"b","events":[{"kind":1,"signal":1,"severity":1,"alert_name":"x"}]}`
	w := ut.PerformRequest(h.Engine, http.MethodPost, "/ingest/v1/events:batch",
		&ut.Body{Body: strings.NewReader(body), Len: len(body)},
		ut.Header{Key: "Content-Type", Value: "application/json"})
	if w.Code != http.StatusServiceUnavailable && w.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous ingest through generated routes: status %d, want 503 (unwired registry, middleware ran) or 401; got body %s",
			w.Code, w.Body.String())
	}
}

type stubPub struct{}

func (stubPub) PublishEvent(_ context.Context, _ string, _ [][2]string, _ []byte) error { return nil }
func (stubPub) PublishEvents(_ context.Context, _ []ingest.BatchMessage) error          { return nil }
