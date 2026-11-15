package sseclient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
			_, _ = fmt.Fprintf(w, "id: %d\nevent: kernel\ndata: {\"kind\":\"oom\",\"n\":%d}\n\n", i, i)
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
			return ErrStop
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
			_, _ = fmt.Fprint(w, "id: 1\ndata: a\n\nid: 2\ndata: b\n\n")
			fl.Flush()
			time.Sleep(150 * time.Millisecond) // then die (client reconnects)
			return
		}
		// Server resumed at id 5 while we last saw 2 -> ids 3..4 are LOST.
		_, _ = fmt.Fprint(w, "id: 5\ndata: c\n\n")
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
			return ErrStop
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
		_, _ = fmt.Fprint(w, "id: 1\ndata: alive\n\n")
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
			return ErrStop
		})
	}()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("heartbeat timeout never reconnected")
	}
}

// P0-04 (rh-p0-04) 失败测试①：官方 /v1/events/watch 是 HTTP POST + JSON 请求体
// + Bearer Token —— GET/无认证的握手必须被拒绝（契约：docs.huatuo.tech
// events-watch，见 contracts/external/huatuo/v1/contract.md）。
func TestWatchHandshakePOSTBodyBearer(t *testing.T) {
	var mu sync.Mutex
	handshakes := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		handshakes++
		method, auth := r.Method, r.Header.Get("Authorization")
		ct := r.Header.Get("Content-Type")
		body := make([]byte, 512)
		n, _ := r.Body.Read(body)
		mu.Unlock()
		if method != http.MethodPost {
			t.Errorf("method = %s, want POST (official /v1/events/watch)", method)
		}
		if auth != "Bearer node-token-1" {
			t.Errorf("Authorization = %q, want Bearer node-token-1", auth)
		}
		if ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		if !strings.Contains(string(body[:n]), `"tracer_name"`) {
			t.Errorf("request body = %q, want the filters JSON", body[:n])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		_, _ = fmt.Fprint(w, "id: 1\ndata: {\"specversion\":\"1.0\"}\n\n")
		fl.Flush()
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()

	c := New(srv.URL)
	c.Method = http.MethodPost
	c.BearerToken = "node-token-1"
	c.Body = []byte(`{"filters":{"tracer_name":"^memory_oom_kill$"}}`)
	c.HeartbeatTimeout = 5 * time.Second
	_ = c.Run(context.Background(), func(Event) error { return ErrStop })
	mu.Lock()
	defer mu.Unlock()
	if handshakes == 0 {
		t.Fatal("no handshake observed")
	}
}

// P0-04 (rh-p0-04) 失败测试②：心跳看门狗必须以"最后活跃时间"为基准 ——
// 持续收流（含 : ping 注释）不得被周期性强制断开重连（修复前：固定定时器
// 到点就关连接，正常收流也每 HeartbeatTimeout 被重连一次）。
func TestHeartbeatKeepAliveWhileStreaming(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		// 每隔 HeartbeatTimeout/4 就有流量（: ping 注释 + 事件），流随请求
		// ctx 存活：修复前固定定时器每 300ms 强断一次 → requests 远大于 1；
		// 修复后连接始终活跃，测试取消 ctx 前只应有 1 条连接。
		i := 0
		for {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			if i%2 == 0 {
				_, _ = fmt.Fprint(w, ": ping\n\n")
			} else {
				_, _ = fmt.Fprintf(w, "id: %d\ndata: {\"n\":%d}\n\n", i, i)
			}
			fl.Flush()
			i++
			time.Sleep(75 * time.Millisecond)
		}
	}))
	defer srv.Close()

	c := New(srv.URL)
	c.HeartbeatTimeout = 300 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx, func(Event) error { return nil })
	mu.Lock()
	defer mu.Unlock()
	if requests != 1 {
		t.Fatalf("connections = %d, want 1 — an actively streaming connection must not be force-reconnected", requests)
	}
}
