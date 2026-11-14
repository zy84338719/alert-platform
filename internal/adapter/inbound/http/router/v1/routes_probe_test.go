package v1

import (
	"net/http"
	"testing"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
)

// Repro probe: every generated route must match (guards the merged router).
func TestMergedRoutesMatch(t *testing.T) {
	h := server.Default(server.WithHostPorts(":0"))
	Register(h)
	cases := []struct{ method, path string }{
		{http.MethodPost, "/ingest/v1/events:batch"},
		{http.MethodGet, "/sources/v1"},
		{http.MethodGet, "/sources/v1/00000000-0000-0000-0000-000000000001"},
		{http.MethodPost, "/sources/v1/00000000-0000-0000-0000-000000000001/set-enabled"},
		{http.MethodGet, "/alerts/v1"},
		{http.MethodGet, "/alerts/v1/00000000-0000-0000-0000-000000000001"},
		{http.MethodGet, "/alerts/v1/00000000-0000-0000-0000-000000000001/transitions"},
		{http.MethodPost, "/policies/v1/routes"},
		{http.MethodPost, "/policies/v1/silences"},
		{http.MethodPost, "/policies/v1/silences/preview"},
		{http.MethodPost, "/policies/v1/silences/00000000-0000-0000-0000-000000000001/revoke"},
		{http.MethodPost, "/channels/v1"},
		{http.MethodPost, "/channels/v1/00000000-0000-0000-0000-000000000001/set-enabled"},
		{http.MethodGet, "/incidents/v1"},
		{http.MethodGet, "/incidents/v1/00000000-0000-0000-0000-000000000001"},
		{http.MethodPost, "/incidents/v1/00000000-0000-0000-0000-000000000001/acknowledge"},
	}
	for _, tc := range cases {
		w := ut.PerformRequest(h.Engine, tc.method, tc.path, nil)
		if w.Code == http.StatusNotFound {
			t.Errorf("%s %s -> 404 (route missing)", tc.method, tc.path)
		}
	}
}
