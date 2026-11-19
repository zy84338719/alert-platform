package v1

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/example/alert-platform/internal/application/authn"
	"github.com/example/alert-platform/internal/application/metricsquery"
)

type fakeMetricsClient struct {
	series []metricsquery.Series
	err    error
	exprs  []string
}

func (f *fakeMetricsClient) QueryRange(_ context.Context, expr string, _, _ time.Time, _ time.Duration) ([]metricsquery.Series, error) {
	f.exprs = append(f.exprs, expr)
	return f.series, f.err
}

func newMetricsCenterRouter(t *testing.T, svc *metricsquery.Service) *server.Hertz {
	t.Helper()
	store := &fakeAuthnStore{byID: map[string]*authn.Identity{
		"tok-oncall":  {TenantID: "tenant-a", ActorID: "bob", Role: authn.RoleOncall},
		"tok-auditor": {TenantID: "tenant-b", ActorID: "carol", Role: authn.RoleAuditor},
		"tok-admin":   {TenantID: "tenant-a", ActorID: "alice", Role: authn.RolePlatformAdmin},
	}}
	SetAuthn(authn.NewService(store))
	SetMetricsQuery(svc)
	t.Cleanup(func() { SetAuthn(nil); SetMetricsQuery(nil) })
	h := server.Default(server.WithHostPorts(":0"))
	h.GET("/metrics/v1/catalog", MetricsCatalog)
	h.GET("/metrics/v1/query", MetricsQueryRange)
	return h
}

// P9-MC-01：指标中心 API 的鉴权、白名单与参数校验。
func TestMetricsCenterAPI(t *testing.T) {
	client := &fakeMetricsClient{series: []metricsquery.Series{{
		Labels: map[string]string{"result": "sent"},
		// A zero sample is meaningful: the JSON contract must carry
		// {"timestamp":..., "value":0}, not drop the key (AUD-007 follow-up).
		Points: [][2]float64{{1700000000, 3}, {1700000060, 0}},
	}}}
	svc := metricsquery.NewService(client)
	h := newMetricsCenterRouter(t, svc)
	now := time.Now()

	t.Run("anonymous is 401", func(t *testing.T) {
		if w := get(t, h, "/metrics/v1/catalog", ""); w.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", w.Code)
		}
	})
	t.Run("tenant catalog hides platform metrics", func(t *testing.T) {
		w := get(t, h, "/metrics/v1/catalog", "tok-auditor")
		if w.Code != http.StatusOK {
			t.Fatalf("catalog: status=%d", w.Code)
		}
		body := w.Body.String()
		if strings.Contains(body, "ingest_events_rate") || strings.Contains(body, "consumer_lag") {
			t.Fatalf("platform metrics leaked to tenant: %s", body)
		}
		if !strings.Contains(body, "active_instances") || !strings.Contains(body, `"tier":"tenant"`) {
			t.Fatalf("tenant metrics missing: %s", body)
		}
	})
	t.Run("admin catalog includes platform metrics", func(t *testing.T) {
		w := get(t, h, "/metrics/v1/catalog", "tok-admin")
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "ingest_events_rate") {
			t.Fatalf("admin catalog: status=%d body=%s", w.Code, w.Body.String())
		}
	})
	t.Run("tenant cannot query platform metric", func(t *testing.T) {
		path := fmt.Sprintf("/metrics/v1/query?metric=ingest_events_rate&start=%d&end=%d&step=60",
			now.Add(-time.Hour).Unix(), now.Unix())
		if w := get(t, h, path, "tok-auditor"); w.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403 body=%s", w.Code, w.Body.String())
		}
	})
	t.Run("unknown metric is 404", func(t *testing.T) {
		path := fmt.Sprintf("/metrics/v1/query?metric=evil&start=%d&end=%d&step=60",
			now.Add(-time.Hour).Unix(), now.Unix())
		if w := get(t, h, path, "tok-oncall"); w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
	})
	t.Run("invalid range is 400", func(t *testing.T) {
		path := fmt.Sprintf("/metrics/v1/query?metric=active_instances&start=%d&end=%d&step=60",
			now.Unix(), now.Add(-time.Hour).Unix())
		if w := get(t, h, path, "tok-oncall"); w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", w.Code)
		}
	})
	t.Run("tenant query injects tenant matcher", func(t *testing.T) {
		path := fmt.Sprintf("/metrics/v1/query?metric=active_instances&start=%d&end=%d&step=60",
			now.Add(-time.Hour).Unix(), now.Unix())
		w := get(t, h, path, "tok-oncall")
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"series"`) {
			t.Fatalf("query: status=%d body=%s", w.Code, w.Body.String())
		}
		if len(client.exprs) != 1 || !strings.Contains(client.exprs[0], `{tenant="tenant-a"}`) {
			t.Fatalf("tenant matcher not injected: %v", client.exprs)
		}
		if !strings.Contains(w.Body.String(), `"value":0`) {
			t.Fatalf("zero sample dropped from JSON: %s", w.Body.String())
		}
	})
	t.Run("admin query returns aggregate", func(t *testing.T) {
		path := fmt.Sprintf("/metrics/v1/query?metric=consumer_lag&start=%d&end=%d&step=60",
			now.Add(-time.Hour).Unix(), now.Unix())
		w := get(t, h, path, "tok-admin")
		if w.Code != http.StatusOK {
			t.Fatalf("admin query: status=%d body=%s", w.Code, w.Body.String())
		}
	})
	t.Run("unconfigured backend is 503", func(t *testing.T) {
		h2 := newMetricsCenterRouter(t, nil)
		if w := get(t, h2, "/metrics/v1/catalog", "tok-auditor"); w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", w.Code)
		}
	})
}
