package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/example/alert-platform/internal/application/authn"
	"github.com/example/alert-platform/internal/application/history"
)

type fakeHistoryStore struct {
	rows []history.Transition
	hits *int
}

func (f *fakeHistoryStore) ListTransitionsRange(context.Context, string, time.Time, time.Time, int) ([]history.Transition, error) {
	if f.hits != nil {
		*f.hits++
	}
	return f.rows, nil
}

func newHistoryRouter(t *testing.T, svc *history.Service) *server.Hertz {
	t.Helper()
	store := &fakeAuthnStore{byID: map[string]*authn.Identity{
		"tok-auditor": {TenantID: "tenant-a", ActorID: "carol", Role: authn.RoleAuditor},
	}}
	SetAuthn(authn.NewService(store))
	SetHistorySvc(svc)
	t.Cleanup(func() { SetAuthn(nil); SetHistorySvc(nil) })
	h := server.Default(server.WithHostPorts(":0"))
	h.GET("/history/v1/transitions", ListHistoryTransitions)
	return h
}

// P9-CH-01：历史查询 API —— 鉴权、窗口校验、PG/CH 路由标记。
func TestHistoryAPI(t *testing.T) {
	now := time.Now()
	row := history.Transition{TransitionID: "t-1", ToState: "firing", At: now.Add(-2 * time.Hour)}
	pg := &fakeHistoryStore{rows: []history.Transition{row}}
	svc := history.NewService(pg, nil, time.Hour)
	h := newHistoryRouter(t, svc)

	path := fmt.Sprintf("/history/v1/transitions?from=%d&to=%d&limit=100",
		now.Add(-time.Hour).Unix(), now.Unix())

	t.Run("anonymous is 401", func(t *testing.T) {
		if w := get(t, h, path, ""); w.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", w.Code)
		}
	})
	t.Run("pg window answers archived=false", func(t *testing.T) {
		w := get(t, h, path, "tok-auditor")
		if w.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		var resp struct {
			Archived    bool `json:"archived"`
			Transitions []struct {
				TransitionID string `json:"transition_id"`
			} `json:"transitions"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v body=%s", err, w.Body.String())
		}
		if resp.Archived || len(resp.Transitions) != 1 || resp.Transitions[0].TransitionID != "t-1" {
			t.Fatalf("resp=%+v body=%s", resp, w.Body.String())
		}
	})
	t.Run("invalid range is 400", func(t *testing.T) {
		bad := fmt.Sprintf("/history/v1/transitions?from=%d&to=%d", now.Unix(), now.Add(-time.Hour).Unix())
		if w := get(t, h, bad, "tok-auditor"); w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", w.Code)
		}
	})
	t.Run("unwired service is 503", func(t *testing.T) {
		h2 := newHistoryRouter(t, nil)
		if w := get(t, h2, path, "tok-auditor"); w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", w.Code)
		}
	})

	t.Run("old window routes to clickhouse archive", func(t *testing.T) {
		var pgHits, chHits int
		old := history.Transition{TransitionID: "ch-1", ToState: "resolved", At: now.Add(-5 * time.Hour)}
		svc2 := history.NewService(
			&fakeHistoryStore{hits: &pgHits},
			&fakeHistoryStore{rows: []history.Transition{old}, hits: &chHits},
			time.Hour)
		h2 := newHistoryRouter(t, svc2)
		oldPath := fmt.Sprintf("/history/v1/transitions?from=%d&to=%d",
			now.Add(-6*time.Hour).Unix(), now.Add(-4*time.Hour).Unix())
		w := get(t, h2, oldPath, "tok-auditor")
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"archived":true`) ||
			!strings.Contains(w.Body.String(), "ch-1") {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		if chHits != 1 || pgHits != 0 {
			t.Fatalf("routing hits pg=%d ch=%d, want 0/1", pgHits, chHits)
		}
	})
}
