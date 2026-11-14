package sseclient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// HT-001/HT-004: ordered delivery, heartbeat, reconnect with cursor, and gap
// detection on numeric cursors (SSE is not replayable — gaps are REPORTED).

func TestStreamDeliversEventsInOrder(t *testing.T) {
	var mu sync.Mutex
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for i := 1; i <= 3; i++ {
			fmt.Fprintf(w, "id: %d\nevent: kernel\ndata: {\"kind\":\"oom\",\"n\":%d}\n\n", i, i)
			fl.Flush()
		}
		time.Sleep(300 * time.Millisecond) // keep the stream open a bit
	}))
	defer srv.Close()

	c := New(srv.URL)
	c.HeartbeatTimeout = 5 * time.Second
	err := c.Run(context.Background(), func(ev Event) error {
		mu.Lock()
		got = append(got, ev.ID)
		mu.Unlock()
		if ev.Name != "kernel" {
			t.Errorf("event name = %q", ev.Name)
		}
		if len(got) == 3 {
			return Stop
		}
		return nil
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 || got[0] != "1" || got[2] != "3" {
		t.Fatalf("got %v, want ids 1..3 in order", got)
	}
	if c.LastID() != "3" {
		t.Fatalf("cursor = %q, want 3", c.LastID())
	}
}

// Reconnect carries Last-Event-ID; a cursor JUMP (server skipped ids while we
// were away) is reported as a gap event — never silently swallowed (HT-004).
func TestReconnectSendsCursorAndDetectsGap(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	var lastEventID string
	sawGap := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		lastEventID = r.Header.Get("Last-Event-ID")
		reqNo := requests
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		if reqNo == 1 {
			fmt.Fprint(w, "id: 1\ndata: a\n\nid: 2\ndata: b\n\n")
			fl.Flush()
			time.Sleep(150 * time.Millisecond) // then die (client reconnects)
			return
		}
		// Server resumed at id 5 while we last saw 2 -> ids 3..4 are LOST.
		fmt.Fprint(w, "id: 5\ndata: c\n\n")
		fl.Flush()
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()

	c := New(srv.URL)
	c.HeartbeatTimeout = 2 * time.Second
	_ = c.Run(context.Background(), func(ev Event) error {
		if ev.Gap {
			mu.Lock()
			sawGap = true
			mu.Unlock()
			if ev.GapFrom != "3" || ev.GapTo != "4" {
				t.Errorf("gap span = %s..%s, want 3..4", ev.GapFrom, ev.GapTo)
			}
		}
		if ev.ID == "5" {
			return Stop
		}
		return nil
	})
	mu.Lock()
	defer mu.Unlock()
	if requests < 2 {
		t.Fatalf("reconnects = %d, want >= 2", requests)
	}
	if lastEventID != "2" {
		t.Fatalf("Last-Event-ID on reconnect = %q, want 2", lastEventID)
	}
	if !sawGap {
		t.Fatal("cursor jump was not reported as a gap")
	}
}

// Heartbeat timeout: a silent server forces a reconnect.
func TestHeartbeatTimeoutReconnects(t *testing.T) {
	var mu sync.Mutex
	connections := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		connections++
		n := connections
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if n == 1 {
			// dead air — no heartbeat
			time.Sleep(3 * time.Second)
			return
		}
		fmt.Fprint(w, "id: 1\ndata: alive\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()

	c := New(srv.URL)
	c.HeartbeatTimeout = 500 * time.Millisecond
	done := make(chan struct{})
	go func() {
		_ = c.Run(context.Background(), func(ev Event) error {
			close(done)
			return Stop
		})
	}()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("heartbeat timeout never reconnected")
	}
}
