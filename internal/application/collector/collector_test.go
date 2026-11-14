package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/alert-platform/internal/application/edge"
	"github.com/example/alert-platform/internal/infrastructure/sseclient"
	"github.com/example/alert-platform/internal/infrastructure/wal"
)

// HT-001 end-to-end: SSE kernel event -> collector -> WAL -> uploader ->
// center, measured from the moment the SSE server writes the event to the
// center receiving the batch. Acceptance: < 10s (local: well under 1s).
func TestKernelEventToCenterUnder10s(t *testing.T) {
	receivedAt := make(chan time.Time, 16)
	var centerSrv *httptest.Server
	centerSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1<<16)
		n, _ := r.Body.Read(buf)
		var payload map[string]any
		_ = json.Unmarshal(buf[:n], &payload)
		receivedAt <- time.Now()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer centerSrv.Close()

	var sent atomic.Bool
	sseSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		fmt.Fprint(w, "id: 1\nevent: kernel\ndata: {\"kind\":\"oom\",\"node\":\"node-7\"}\n\n")
		fl.Flush()
		sent.Store(true)
		// keep the stream open until the test tears down
		time.Sleep(5 * time.Second)
	}))
	defer sseSrv.Close()

	dir := t.TempDir()
	buf, err := wal.Open(dir, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	up, err := edge.NewUploader(buf, edge.UploaderConfig{Endpoint: centerSrv.URL})
	if err != nil {
		t.Fatal(err)
	}

	stream := sseclient.New(sseSrv.URL)
	stream.HeartbeatTimeout = 4 * time.Second
	col := New(adaptStream(stream), buf, Config{
		TenantID: "ht-tenant", SourceID: "ht-source", ClusterID: "ht-cluster",
		PromoteThreshold: 3, PromoteWindow: time.Minute,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	go col.Run(ctx)
	go up.Run(ctx)

	select {
	case at := <-receivedAt:
		elapsed := at.Sub(start)
		if elapsed > 10*time.Second {
			t.Fatalf("end-to-end took %s, acceptance is <10s", elapsed)
		}
		t.Logf("kernel event reached the center in %s", elapsed)
	case <-time.After(15 * time.Second):
		t.Fatal("kernel event never reached the center")
	}
}

// HT-003 + HT-004 through the collector: gaps become first-class occurrences;
// bursts promote to exactly one stateful firing.
func TestGapRecordingAndBurstPromotion(t *testing.T) {
	sseSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		fmt.Fprint(w, "id: 1\ndata: {\"kind\":\"oom\",\"node\":\"n1\"}\n\n")
		fmt.Fprint(w, "id: 2\ndata: {\"kind\":\"oom\",\"node\":\"n1\"}\n\n")
		fmt.Fprint(w, "id: 3\ndata: {\"kind\":\"oom\",\"node\":\"n1\"}\n\n")
		// cursor jump: 4..6 lost while "disconnected"
		fmt.Fprint(w, "id: 7\ndata: {\"kind\":\"oom\",\"node\":\"n1\"}\n\n")
		fl.Flush()
		time.Sleep(3 * time.Second)
	}))
	defer sseSrv.Close()

	dir := t.TempDir()
	buf, _ := wal.Open(dir, 0, 1<<20)
	stream := sseclient.New(sseSrv.URL)
	stream.HeartbeatTimeout = 4 * time.Second
	col := New(adaptStream(stream), buf, Config{
		TenantID: "ht-tenant", SourceID: "ht-source", ClusterID: "ht-cluster",
		PromoteThreshold: 3, PromoteWindow: time.Minute,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go col.Run(ctx)

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		p, _ := buf.Pending()
		kinds := map[string]bool{}
		for _, rec := range p {
			var payload struct {
				Events []struct {
					Kind      any    `json:"kind"`
					AlertName string `json:"alert_name"`
					Signal    any    `json:"signal"`
				} `json:"events"`
			}
			_ = json.Unmarshal(rec.Body, &payload)
			for _, ev := range payload.Events {
				if ev.AlertName == "HUATUO sse-gap" {
					kinds["gap"] = true
				}
				if ev.AlertName == "HUATUO oom burst on n1" {
					kinds["promoted"] = true
					if fmt.Sprint(ev.Signal) != "1" {
						t.Errorf("promotion signal = %v, want FIRING(1)", ev.Signal)
					}
				}
			}
		}
		if kinds["gap"] && kinds["promoted"] {
			// No fabricated resolved: none of the records is a resolved signal.
			for _, rec := range p {
				if contains(rec.Body, `"signal":2`) {
					t.Fatal("occurrence pipeline fabricated a resolved event")
				}
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("gap occurrence and/or burst promotion never landed in the WAL")
}

func contains(b []byte, sub string) bool {
	return len(sub) > 0 && stringContains(string(b), sub)
}

func stringContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

type streamAdapter struct{ c *sseclient.Client }

func adaptStream(c *sseclient.Client) collectorStream { return &streamAdapter{c: c} }

type collectorStream interface {
	Run(ctx context.Context, handle func(Event) error) error
	LastID() string
}

func (a *streamAdapter) Run(ctx context.Context, handle func(Event) error) error {
	return a.c.Run(ctx, func(e sseclient.Event) error {
		return handle(Event{ID: e.ID, Name: e.Name, Data: e.Data, Gap: e.Gap, GapFrom: e.GapFrom, GapTo: e.GapTo})
	})
}
func (a *streamAdapter) LastID() string { return a.c.LastID() }
