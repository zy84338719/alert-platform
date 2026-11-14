package v1

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/example/alert-platform/internal/application/ingest"
	"github.com/example/alert-platform/internal/application/sourcereg"
	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// fakeRegistry is an in-memory sourcereg.Store for handler-level tests.
type fakeRegistry struct {
	mu     sync.Mutex
	source *sourcereg.Source
	token  string
	touch  int
}

func (f *fakeRegistry) CreateSource(_ context.Context, src *sourcereg.Source, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.source = src
	return nil
}
func (f *fakeRegistry) GetSource(_ context.Context, _, _ string) (*sourcereg.Source, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.source != nil {
		return f.source, nil
	}
	return nil, sourcereg.ErrNotFound
}
func (f *fakeRegistry) ListSources(_ context.Context, _ string, _ bool) ([]*sourcereg.Source, error) {
	return nil, nil
}
func (f *fakeRegistry) SetEnabled(_ context.Context, _, _ string, _ bool) error { return nil }
func (f *fakeRegistry) RevokeCredentials(_ context.Context, _, _ string) error  { return nil }
func (f *fakeRegistry) FindByTokenHash(_ context.Context, tokenHash string) (*sourcereg.Source, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Mirrors the real store: only REVOKED credentials vanish here; the
	// disabled flag is decided by the service layer (ErrDisabled -> 403).
	if f.source != nil && tokenHash == sourcereg.TokenHash(f.token) {
		return f.source, nil
	}
	return nil, sourcereg.ErrNotFound
}
func (f *fakeRegistry) TouchLastSeen(_ context.Context, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.touch++
	return nil
}

// capturePublisher records every published canonical event.
type capturePublisher struct {
	mu     sync.Mutex
	events []*eventv1.AlertEvent
	keys   []string
}

func (p *capturePublisher) PublishEvent(_ context.Context, key string, _ [][2]string, value []byte) error {
	ev := &eventv1.AlertEvent{}
	if err := proto.Unmarshal(value, ev); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, ev)
	p.keys = append(p.keys, key)
	return nil
}

// PublishEvents (PERF-002 batched path): unmarshal every event of the batch.
func (p *capturePublisher) PublishEvents(_ context.Context, msgs []ingest.BatchMessage) error {
	for _, m := range msgs {
		if err := p.PublishEvent(context.Background(), m.Key, m.Headers, m.Value); err != nil {
			return err
		}
	}
	return nil
}

func newExtWebhookRouter(t *testing.T) (*server.Hertz, *fakeRegistry, *capturePublisher) {
	t.Helper()
	reg := &fakeRegistry{token: "aps_testtoken"}
	SetSourceRegistry(sourcereg.NewService(reg))
	pub := &capturePublisher{}
	SetIngest(ingest.NewService(pub))
	h := server.Default(server.WithHostPorts(":0"))
	RegisterExternalWebhooks(h)
	t.Cleanup(func() { SetSourceRegistry(nil); SetIngest(nil) })
	return h, reg, pub
}

const amBody = `{"receiver":"r","status":"firing","alerts":[
  {"status":"firing","labels":{"alertname":"T","severity":"critical","cluster":"c1"},
   "startsAt":"2026-11-12T10:00:00+08:00","endsAt":"0001-01-01T00:00:00Z","fingerprint":"fp1"}],
 "version":"4"}`

func post(t *testing.T, h *server.Hertz, path, token, body string) *ut.ResponseRecorder {
	t.Helper()
	w := ut.PerformRequest(h.Engine, http.MethodPost, path, &ut.Body{Body: strings.NewReader(body), Len: len(body)},
		ut.Header{Key: "Authorization", Value: "Bearer " + token},
		ut.Header{Key: "Content-Type", Value: "application/json"})
	return w
}

// ING-003 ② at the HTTP layer: unregistered/anonymous sources are rejected.
func TestExtWebhookRejectsAnonymousAndUnknown(t *testing.T) {
	h, _, _ := newExtWebhookRouter(t)
	if w := post(t, h, "/ingest/external/alertmanager/v1/events", "", amBody); w.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous push: status %d, want 401", w.Code)
	}
	if w := post(t, h, "/ingest/external/alertmanager/v1/events", "aps_wrong", amBody); w.Code != http.StatusUnauthorized {
		t.Fatalf("unknown credential: status %d, want 401", w.Code)
	}
}

// ING-001 ③ at the HTTP layer: the tenant of every published event is the
// registry-derived one, regardless of any payload claim.
func TestExtWebhookTenantDerivedFromCredential(t *testing.T) {
	h, reg, pub := newExtWebhookRouter(t)
	reg.mu.Lock()
	reg.source = &sourcereg.Source{
		SourceID: "am-prod", TenantID: "tenant-real", ClusterID: "fallback-c",
		SourceType: eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER, Enabled: true,
	}
	reg.mu.Unlock()

	if w := post(t, h, "/ingest/external/alertmanager/v1/events", reg.token, amBody); w.Code != http.StatusAccepted {
		t.Fatalf("push: status %d body %s", w.Code, w.Body.String())
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 1 {
		t.Fatalf("published %d events, want 1", len(pub.events))
	}
	if got := pub.events[0].GetTenantId(); got != "tenant-real" {
		t.Fatalf("tenant = %q, want credential-derived tenant-real", got)
	}
	if pub.events[0].GetSource().GetSourceId() != "am-prod" {
		t.Fatalf("source = %q", pub.events[0].GetSource().GetSourceId())
	}
}

// Endpoint/type mismatch is rejected: an Alertmanager credential cannot push
// to the DeepFlow endpoint and vice versa.
func TestExtWebhookRejectsWrongSourceType(t *testing.T) {
	h, reg, _ := newExtWebhookRouter(t)
	reg.mu.Lock()
	reg.source = &sourcereg.Source{
		SourceID: "am-prod", TenantID: "tenant-real",
		SourceType: eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER, Enabled: true,
	}
	reg.mu.Unlock()
	if w := post(t, h, "/ingest/external/deepflow/v7_2/events", reg.token, `{}`); w.Code != http.StatusForbidden {
		t.Fatalf("cross-type push: status %d, want 403", w.Code)
	}
}

// Vendor retries reuse the deterministic batch id; the consumer's receipts
// layer absorbs the duplicate (docs/03 §2).
func TestExtWebhookDeterministicBatchID(t *testing.T) {
	h, reg, pub := newExtWebhookRouter(t)
	reg.mu.Lock()
	reg.source = &sourcereg.Source{
		SourceID: "am-prod", TenantID: "tenant-real", ClusterID: "c",
		SourceType: eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER, Enabled: true,
	}
	reg.mu.Unlock()
	w1 := post(t, h, "/ingest/external/alertmanager/v1/events", reg.token, amBody)
	w2 := post(t, h, "/ingest/external/alertmanager/v1/events", reg.token, amBody)
	if w1.Code != http.StatusAccepted || w2.Code != http.StatusAccepted {
		t.Fatalf("statuses %d/%d", w1.Code, w2.Code)
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 2 {
		t.Fatalf("retries re-publish (at-least-once expected): %d events", len(pub.events))
	}
	if pub.events[0].GetTransportId() != pub.events[1].GetTransportId() {
		t.Fatalf("transport ids drifted across retries: %q vs %q",
			pub.events[0].GetTransportId(), pub.events[1].GetTransportId())
	}
}

// A broker failure surfaces as a retriable 5xx, never a silent 2xx (BASE-006
// contract extended to the vendor path).
func TestExtWebhookPublisherFailureIs5xx(t *testing.T) {
	h, reg, _ := newExtWebhookRouter(t)
	reg.mu.Lock()
	reg.source = &sourcereg.Source{
		SourceID: "am-prod", TenantID: "tenant-real", ClusterID: "c",
		SourceType: eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER, Enabled: true,
	}
	reg.mu.Unlock()
	SetIngest(ingest.NewService(errPublisher{}))
	defer SetIngest(nil)
	w := post(t, h, "/ingest/external/alertmanager/v1/events", reg.token, amBody)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("broker down: status %d, want 503", w.Code)
	}
}

// Invalid vendor payloads are 400 (client error), including the negative
// fixtures used by the contract tests (ING-004).
func TestExtWebhookBadPayloadIs400(t *testing.T) {
	h, reg, _ := newExtWebhookRouter(t)
	reg.mu.Lock()
	reg.source = &sourcereg.Source{
		SourceID: "am-prod", TenantID: "tenant-real", ClusterID: "c",
		SourceType: eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER, Enabled: true,
	}
	reg.mu.Unlock()
	if w := post(t, h, "/ingest/external/alertmanager/v1/events", reg.token, `{"alerts":[]}`); w.Code != http.StatusBadRequest {
		t.Fatalf("empty alerts: status %d, want 400", w.Code)
	}
}

type errPublisher struct{}

func (errPublisher) PublishEvent(context.Context, string, [][2]string, []byte) error {
	return context.DeadlineExceeded
}

func (errPublisher) PublishEvents(context.Context, []ingest.BatchMessage) error {
	return context.DeadlineExceeded
}
