package v1

import (
	"context"
	"net/http"
	"testing"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/example/alert-platform/internal/application/authn"
	"github.com/example/alert-platform/internal/application/query"
)

// REGRESSION for the path-parameter binding gap: buf-generated models carry
// no hz binding tags, so BindAndValidate left every path param EMPTY (detail
// APIs 404'd). Handlers now read c.Param directly — this test pins it at the
// HTTP layer with a store that echoes the received id.

type echoQueryStore struct {
	seenID string
}

func (e *echoQueryStore) ListAlerts(context.Context, query.AlertFilter) (query.Page[query.Alert], error) {
	return query.Page[query.Alert]{}, nil
}
func (e *echoQueryStore) GetAlertByID(_ context.Context, instanceID string) (*query.Alert, error) {
	e.seenID = instanceID
	return &query.Alert{InstanceID: instanceID, TenantID: "tenant-x", Name: "echo"}, nil
}
func (e *echoQueryStore) ListTransitions(context.Context, string, string, int, string) (query.Page[query.Transition], error) {
	return query.Page[query.Transition]{}, nil
}

func TestPathParamsReachHandlers(t *testing.T) {
	store := &echoQueryStore{}
	SetQuerySvc(query.NewService(store))
	authnStore := &fakeAuthnStore{byID: map[string]*authn.Identity{
		"tok-admin": {TenantID: "tenant-x", ActorID: "alice", Role: authn.RolePlatformAdmin},
	}}
	SetAuthn(authn.NewService(authnStore))
	t.Cleanup(func() { SetQuerySvc(nil); SetAuthn(nil) })

	h := server.Default(server.WithHostPorts(":0"))
	h.GET("/alerts/v1/:instance_id", GetAlert)
	h.GET("/alerts/v1/:instance_id/transitions", ListTransitions)

	w := ut.PerformRequest(h.Engine, http.MethodGet, "/alerts/v1/5e6d71c5-test-id", nil,
		ut.Header{Key: "Authorization", Value: "Bearer tok-admin"})
	if w.Code != http.StatusOK {
		t.Fatalf("detail: %d %s", w.Code, w.Body.String())
	}
	if store.seenID != "5e6d71c5-test-id" {
		t.Fatalf("handler saw instance id %q, want the path value (binding gap regression)", store.seenID)
	}
}
