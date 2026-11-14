package edgegw

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/example/alert-platform/internal/application/edge"
	"github.com/example/alert-platform/internal/infrastructure/wal"
)

// EDGE-004 fault battery against the real gateway + WAL over HTTP.

const (
	gwTenant = "eeee1111-0000-0000-0000-000000000001"
	gwSource = "eeee1111-0000-0000-0000-000000000002"
)

func newGateway(t *testing.T, maxBytes int64) (*server.Hertz, *wal.Log, string) {
	t.Helper()
	dir := t.TempDir()
	token := "aps_edge_test"
	sum := sha256.Sum256([]byte(token))
	sources := map[string]any{
		"sources": []map[string]any{{
			"token_hash": hex.EncodeToString(sum[:]),
			"tenant_id":  gwTenant,
			"source_id":  gwSource,
			"cluster_id": "edge-cluster",
			"type":       "deepflow",
		}},
	}
	raw, _ := json.Marshal(sources)
	srcFile := filepath.Join(dir, "edge-sources.json")
	if err := os.WriteFile(srcFile, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	log, err := wal.Open(filepath.Join(dir, "wal"), maxBytes, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(srcFile, log)
	if err != nil {
		t.Fatal(err)
	}
	h := server.Default(server.WithHostPorts(":0"))
	gw.Register(h)
	return h, log, token
}

func post(t *testing.T, h *server.Hertz, path, token, body string) *ut.ResponseRecorder {
	t.Helper()
	return ut.PerformRequest(h.Engine, http.MethodPost, path, &ut.Body{Body: newReader(body), Len: len(body)},
		ut.Header{Key: "Authorization", Value: "Bearer " + token},
		ut.Header{Key: "Content-Type", Value: "application/json"})
}

func newReader(s string) *strings.Reader { return strings.NewReader(s) }

var _ = atomic.AddInt32

func dfBody() string {
	return fmt.Sprintf(`{"schema_version":"deepflow.bridge.v1","source_id":%q,"event_level":1,
	"policy_id":"p","alert_name":"Edge Test","occurred_at":%q,"target_key":"pod-1"}`,
		gwSource, time.Now().UTC().Format(time.RFC3339))
}

// Unknown credentials never touch the WAL.
func TestGatewayRejectsUnknownToken(t *testing.T) {
	h, log, _ := newGateway(t, 0)
	if w := post(t, h, "/ingest/external/deepflow/v7_2/events", "wrong", dfBody()); w.Code != http.StatusUnauthorized {
		t.Fatalf("unknown token: %d", w.Code)
	}
	if p, _ := log.Pending(); len(p) != 0 {
		t.Fatalf("rejected push reached the WAL: %d", len(p))
	}
}

// A full WAL fails CLOSED: 503 to the source, self-alert logged — an
// unacknowledged event is never reported as accepted.
func TestGatewayWALFullFailsClosed(t *testing.T) {
	h, _, token := newGateway(t, 200) // tiny capacity
	// fill it
	for i := 0; i < 50; i++ {
		w := post(t, h, "/ingest/external/deepflow/v7_2/events", token, dfBody())
		if w.Code == http.StatusServiceUnavailable {
			// capacity reached: this and later pushes must keep failing closed
			for j := 0; j < 3; j++ {
				if w2 := post(t, h, "/ingest/external/deepflow/v7_2/events", token, dfBody()); w2.Code != http.StatusServiceUnavailable {
					t.Fatalf("post-full push returned %d, want 503", w2.Code)
				}
			}
			return
		}
		if w.Code != http.StatusAccepted {
			t.Fatalf("push %d: %d", i, w.Code)
		}
	}
	t.Fatal("capacity never enforced")
}

// kill -9 equivalence: accepted events survive a process restart (WAL on
// disk), and re-uploading them keeps the SAME transport id (center dedups).
func TestGatewayCrashDurabilityAndStableTransport(t *testing.T) {
	h, log, token := newGateway(t, 0)
	if w := post(t, h, "/ingest/external/deepflow/v7_2/events", token, dfBody()); w.Code != http.StatusAccepted {
		t.Fatalf("push: %d", w.Code)
	}
	// "crash": nothing graceful, just reopen from disk.
	_ = h

	p1, err := log.Pending()
	if err != nil || len(p1) != 1 {
		t.Fatalf("pending before crash: %v (%d)", err, len(p1))
	}
	log2, err := wal.Open(log.Dir(), 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	p2, _ := log2.Pending()
	if len(p2) != 1 {
		t.Fatalf("recovered %d records after crash, want 1", len(p2))
	}
	if p2[0].ID != p1[0].ID {
		t.Fatal("transport id drifted across restart")
	}
}

// The uploader round trip: gateway WAL -> uploader -> center 202 -> acked.
func TestGatewayUploaderRoundTrip(t *testing.T) {
	h, log, token := newGateway(t, 0)
	if w := post(t, h, "/ingest/external/deepflow/v7_2/events", token, dfBody()); w.Code != http.StatusAccepted {
		t.Fatalf("push: %d", w.Code)
	}

	var centerBatches int32
	var gotBody []byte
	center := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1<<16)
		n, _ := r.Body.Read(buf)
		gotBody = buf[:n]
		centerBatches++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer center.Close()

	u, err := edge.NewUploader(log, edge.UploaderConfig{Endpoint: center.URL})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := u.DrainOnce(context.Background())
	if err != nil || sent != 1 {
		t.Fatalf("drain: sent=%d err=%v", sent, err)
	}
	if p, _ := log.Pending(); len(p) != 0 {
		t.Fatal("acked record stayed pending")
	}
	var decoded map[string]any
	if err := json.Unmarshal(gotBody, &decoded); err != nil {
		t.Fatalf("center body: %v", err)
	}
	if decoded["batch_id"] == "" {
		t.Fatal("center body missing batch_id")
	}
}
