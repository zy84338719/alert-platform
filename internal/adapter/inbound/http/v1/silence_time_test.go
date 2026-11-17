package v1

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/example/alert-platform/internal/application/authn"
	"github.com/example/alert-platform/internal/application/policymgmt"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AUD-013: the silence API must not turn absent protobuf timestamps into the
// Unix epoch. starts_at defaults to now, ends_at is required, and the preview
// clock defaults to now.

type fakePolicyStore struct {
	last *policymgmt.Silence
}

func (f *fakePolicyStore) CreateRoute(context.Context, *policymgmt.Route) error { return nil }
func (f *fakePolicyStore) ListRoutes(context.Context, string) ([]*policymgmt.Route, error) {
	return nil, nil
}
func (f *fakePolicyStore) SetRouteEnabled(context.Context, string, string, bool, int64) (int64, error) {
	return 0, nil
}
func (f *fakePolicyStore) CreateSilence(_ context.Context, s *policymgmt.Silence) error {
	s.SilenceID = "sil-1"
	f.last = s
	return nil
}
func (f *fakePolicyStore) ListSilences(context.Context, string, bool) ([]*policymgmt.Silence, error) {
	return nil, nil
}
func (f *fakePolicyStore) RevokeSilence(context.Context, string, string) (*policymgmt.Silence, error) {
	return &policymgmt.Silence{}, nil
}
func (f *fakePolicyStore) CreateChannel(context.Context, *policymgmt.Channel) error { return nil }
func (f *fakePolicyStore) ListChannels(context.Context, string) ([]*policymgmt.Channel, error) {
	return nil, nil
}
func (f *fakePolicyStore) SetChannelEnabled(context.Context, string, string, bool) (*policymgmt.Channel, error) {
	return &policymgmt.Channel{}, nil
}

func newSilenceRouter(t *testing.T, st *fakePolicyStore) *server.Hertz {
	t.Helper()
	SetAuthn(authn.NewService(&fakeAuthnStore{byID: map[string]*authn.Identity{
		"tok-admin": {TenantID: "tenant-a", ActorID: "alice", Role: authn.RolePlatformAdmin},
	}}))
	SetPolicySvc(policymgmt.NewService(st))
	h := server.Default(server.WithHostPorts(":0"))
	h.POST("/policies/v1/silences", CreateSilence)
	h.POST("/policies/v1/silences/preview", PreviewSilences)
	t.Cleanup(func() { SetAuthn(nil); SetPolicySvc(nil) })
	return h
}

func postSilenceJSON(t *testing.T, h *server.Hertz, path, body string) *ut.ResponseRecorder {
	t.Helper()
	return ut.PerformRequest(h.Engine, http.MethodPost, path,
		&ut.Body{Body: strings.NewReader(body), Len: len(body)},
		ut.Header{Key: "Content-Type", Value: "application/json"},
		ut.Header{Key: "Authorization", Value: "Bearer tok-admin"})
}

func TestCreateSilenceDefaultsMissingStart(t *testing.T) {
	store := &fakePolicyStore{}
	h := newSilenceRouter(t, store)
	ends := time.Now().Add(time.Hour).Unix()
	body := fmt.Sprintf(`{"matchers":[{"name":"service","operation":"EQ","value":"api"}],`+
		`"ends_at":{"seconds":%d},"reason":"audit"}`, ends)
	if w := postSilenceJSON(t, h, "/policies/v1/silences", body); w.Code != http.StatusCreated {
		t.Fatalf("create: %d body=%s", w.Code, w.Body.String())
	}
	if store.last == nil {
		t.Fatal("silence not persisted")
	}
	if store.last.StartsAt.Year() < 2000 {
		t.Fatalf("missing starts_at became %s (epoch bug)", store.last.StartsAt)
	}
	if !store.last.EndsAt.After(store.last.StartsAt) {
		t.Fatalf("window inverted: %s..%s", store.last.StartsAt, store.last.EndsAt)
	}
}

func TestCreateSilenceRejectsMissingOrInvalidEnd(t *testing.T) {
	store := &fakePolicyStore{}
	h := newSilenceRouter(t, store)
	// Missing ends_at -> 400 (never a 1970 end).
	body := `{"matchers":[{"name":"service","operation":"EQ","value":"api"}]}`
	if w := postSilenceJSON(t, h, "/policies/v1/silences", body); w.Code != http.StatusBadRequest {
		t.Fatalf("missing end: %d body=%s", w.Code, w.Body.String())
	}
	// ends_at before starts_at -> 400.
	body = fmt.Sprintf(`{"matchers":[{"name":"service","operation":"EQ","value":"api"}],`+
		`"starts_at":{"seconds":%d},"ends_at":{"seconds":%d}}`,
		time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC).Unix(),
		time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC).Unix())
	if w := postSilenceJSON(t, h, "/policies/v1/silences", body); w.Code != http.StatusBadRequest {
		t.Fatalf("inverted window: %d body=%s", w.Code, w.Body.String())
	}
	if store.last != nil {
		t.Fatalf("invalid silence persisted: %+v", store.last)
	}
}

func TestPreviewSilenceDefaultsMissingAt(t *testing.T) {
	store := &fakePolicyStore{}
	h := newSilenceRouter(t, store)
	if w := postSilenceJSON(t, h, "/policies/v1/silences/preview", `{"labels":{"service":"api"}}`); w.Code != http.StatusOK {
		t.Fatalf("preview: %d body=%s", w.Code, w.Body.String())
	}
	// The handler replaces a nil clock with now before calling the service.
	now := time.Now().UTC()
	if got := protoTimeOrNow(nil, now); got.Year() < 2000 {
		t.Fatalf("nil timestamp mapped to %s (epoch bug)", got)
	}
	if got := protoTimeOrNow(&timestamppb.Timestamp{}, now); !got.Equal(now) {
		t.Fatalf("zero timestamp mapped to %s, want fallback %s", got, now)
	}
	explicit := timestamppb.New(time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC))
	if got := protoTimeOrNow(explicit, now); !got.Equal(explicit.AsTime()) {
		t.Fatalf("explicit timestamp overwritten: %s", got)
	}
}
