package v1

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/example/alert-platform/internal/application/ops"
	"github.com/example/alert-platform/internal/application/sourcereg"
)

// AUD-009: edge heartbeats must bind identity server-side.
//   - edge credential (X-Edge-Id + bearer): tenant comes from the credential,
//     body.source_id must be in its allowed set;
//   - legacy shared key: (tenant, source) must be a registered pair, so a
//     leaked/guessed key still cannot zero another tenant's waterline;
//   - the key comparison hashes both sides (no length disclosure).

type heartbeatCall struct {
	tenant, source string
	bytes, pending int64
}

type fakeOpsStore struct {
	calls []heartbeatCall
	err   error
}

func (f *fakeOpsStore) ReportEdgeStatus(_ context.Context, tenantID, sourceID string, walBytes, walPending int64) error {
	f.calls = append(f.calls, heartbeatCall{tenantID, sourceID, walBytes, walPending})
	return f.err
}
func (f *fakeOpsStore) ListEdgeStatus(context.Context, string) ([]ops.EdgeStatus, error) {
	return nil, nil
}
func (f *fakeOpsStore) ListNotificationTasks(context.Context, string, string, int) ([]ops.NotificationTask, error) {
	return nil, nil
}
func (f *fakeOpsStore) ReplayNotifyTask(context.Context, string, string) (bool, error) {
	return false, nil
}

func newHeartbeatRouter(t *testing.T, store *fakeOpsStore, reg *fakeRegistry) *server.Hertz {
	t.Helper()
	SetOpsStore(store)
	SetEdgeAPIKey("edge-shared-key")
	if reg != nil {
		SetSourceRegistry(sourcereg.NewService(reg))
	} else {
		SetSourceRegistry(nil)
	}
	h := server.Default(server.WithHostPorts(":0"))
	RegisterPlatformAPI(h)
	t.Cleanup(func() { SetOpsStore(nil); SetEdgeAPIKey(""); SetSourceRegistry(nil) })
	return h
}

func postHeartbeat(t *testing.T, h *server.Hertz, body, edgeKey, edgeID, bearer string) *ut.ResponseRecorder {
	t.Helper()
	hdr := []ut.Header{{Key: "Content-Type", Value: "application/json"}}
	if edgeKey != "" {
		hdr = append(hdr, ut.Header{Key: "X-Edge-Key", Value: edgeKey})
	}
	if edgeID != "" {
		hdr = append(hdr, ut.Header{Key: "X-Edge-Id", Value: edgeID})
	}
	if bearer != "" {
		hdr = append(hdr, ut.Header{Key: "Authorization", Value: "Bearer " + bearer})
	}
	return ut.PerformRequest(h.Engine, http.MethodPost, "/edge/v1/heartbeat",
		&ut.Body{Body: strings.NewReader(body), Len: len(body)}, hdr...)
}

func edgeHeartbeatBody(tenant, source string) string {
	return `{"tenant_id":"` + tenant + `","source_id":"` + source + `","wal_bytes":1024,"wal_pending":3}`
}

func TestHeartbeatLegacyKeyMatrix(t *testing.T) {
	store := &fakeOpsStore{}
	h := newHeartbeatRouter(t, store, nil)

	// ① no auth -> 401, nothing recorded.
	if w := postHeartbeat(t, h, edgeHeartbeatBody("t1", "s1"), "", "", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("no key: want 401 got %d", w.Code)
	}
	// ② wrong key of DIFFERENT length -> 401 (comparison must not short-circuit
	// on length and leak the secret length).
	if w := postHeartbeat(t, h, edgeHeartbeatBody("t1", "s1"), "short", "", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong key: want 401 got %d", w.Code)
	}
	if len(store.calls) != 0 {
		t.Fatalf("unauthenticated heartbeats must not reach the store: %+v", store.calls)
	}
	// ③ correct legacy key -> 200, body identity used (store enforces binding).
	if w := postHeartbeat(t, h, edgeHeartbeatBody("t1", "s1"), "edge-shared-key", "", ""); w.Code != http.StatusOK {
		t.Fatalf("good key: want 200 got %d body=%s", w.Code, w.Body.String())
	}
	if len(store.calls) != 1 || store.calls[0] != (heartbeatCall{"t1", "s1", 1024, 3}) {
		t.Fatalf("unexpected call: %+v", store.calls)
	}
	// ④ store rejects an unregistered (tenant, source) pair -> 403, not 500.
	store.err = ops.ErrSourceNotOwned
	if w := postHeartbeat(t, h, edgeHeartbeatBody("victim", "someone-else"), "edge-shared-key", "", ""); w.Code != http.StatusForbidden {
		t.Fatalf("foreign pair: want 403 got %d body=%s", w.Code, w.Body.String())
	}
}

func TestHeartbeatEdgeCredentialMatrix(t *testing.T) {
	store := &fakeOpsStore{}
	reg := &fakeRegistry{
		edge:      &sourcereg.EdgeIdentity{EdgeID: "edge-1", TenantID: "tenant-a", AllowedSourceIDs: []string{"src-1", "src-2"}},
		edgeToken: "aps_edge-token",
	}
	h := newHeartbeatRouter(t, store, reg)

	// ① unknown bearer with X-Edge-Id -> 401.
	if w := postHeartbeat(t, h, edgeHeartbeatBody("tenant-a", "src-1"), "", "edge-1", "aps_wrong"); w.Code != http.StatusUnauthorized {
		t.Fatalf("bad token: want 401 got %d", w.Code)
	}
	// ② credential tenant wins over a spoofed body tenant.
	if w := postHeartbeat(t, h, edgeHeartbeatBody("victim-tenant", "src-1"), "", "edge-1", "aps_edge-token"); w.Code != http.StatusOK {
		t.Fatalf("good token: want 200 got %d body=%s", w.Code, w.Body.String())
	}
	if len(store.calls) != 1 || store.calls[0] != (heartbeatCall{"tenant-a", "src-1", 1024, 3}) {
		t.Fatalf("body tenant must be ignored on the credential path: %+v", store.calls)
	}
	// ③ source outside the credential's allowed set -> 403, store untouched.
	if w := postHeartbeat(t, h, edgeHeartbeatBody("tenant-a", "src-other"), "", "edge-1", "aps_edge-token"); w.Code != http.StatusForbidden {
		t.Fatalf("disallowed source: want 403 got %d body=%s", w.Code, w.Body.String())
	}
	if len(store.calls) != 1 {
		t.Fatalf("disallowed source must not reach the store: %+v", store.calls)
	}
	// ④ body tenant empty is fine on the credential path (tenant is derived).
	if w := postHeartbeat(t, h, edgeHeartbeatBody("", "src-2"), "", "edge-1", "aps_edge-token"); w.Code != http.StatusOK {
		t.Fatalf("empty body tenant: want 200 got %d body=%s", w.Code, w.Body.String())
	}
	if len(store.calls) != 2 || store.calls[1].tenant != "tenant-a" || store.calls[1].source != "src-2" {
		t.Fatalf("unexpected calls: %+v", store.calls)
	}
	// ⑤ legacy key is not accepted once edge identity is used: missing bearer.
	if w := postHeartbeat(t, h, edgeHeartbeatBody("tenant-a", "src-1"), "edge-shared-key", "edge-1", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("edge path without bearer: want 401 got %d", w.Code)
	}
}

func TestHeartbeatStoreErrorMapsTo500(t *testing.T) {
	store := &fakeOpsStore{err: errors.New("db down")}
	h := newHeartbeatRouter(t, store, nil)
	if w := postHeartbeat(t, h, edgeHeartbeatBody("t1", "s1"), "edge-shared-key", "", ""); w.Code != http.StatusInternalServerError {
		t.Fatalf("db error: want 500 got %d", w.Code)
	}
}