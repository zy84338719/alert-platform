package channel

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func cfg(url string) []byte {
	b, _ := json.Marshal(Config{WebhookURL: url})
	return b
}

var payload = []byte(`{"message":"[alert-platform] firing HighLat policy_version=7"}`)

// NOTI-002/T-008 classification matrix at the HTTP layer.
func TestSendClassification(t *testing.T) {
	sender := NewHTTPSender(true) // tests hit 127.0.0.1 mocks

	t.Run("2xx is sent", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		defer srv.Close()
		if res := sender.Send(context.Background(), "webhook", cfg(srv.URL), payload); !res.Sent {
			t.Fatalf("2xx must be Sent: %+v", res)
		}
	})

	t.Run("429 honors Retry-After", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "17")
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer srv.Close()
		res := sender.Send(context.Background(), "webhook", cfg(srv.URL), payload)
		if res.Sent || res.FailureCode != "retryable" || res.RetryAfter != 17*time.Second {
			t.Fatalf("429: %+v", res)
		}
	})

	t.Run("5xx retries with zero Retry-After", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
		}))
		defer srv.Close()
		res := sender.Send(context.Background(), "webhook", cfg(srv.URL), payload)
		if res.Sent || res.FailureCode != "retryable" || res.RetryAfter != 0 {
			t.Fatalf("5xx: %+v", res)
		}
	})

	t.Run("4xx is permanent", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
		}))
		defer srv.Close()
		res := sender.Send(context.Background(), "webhook", cfg(srv.URL), payload)
		if res.Sent || res.FailureCode != "permanent" {
			t.Fatalf("4xx: %+v", res)
		}
	})

	t.Run("feishu payload shape", func(t *testing.T) {
		var got map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&got)
			_, _ = w.Write([]byte(`{"code":0}`))
		}))
		defer srv.Close()
		res := sender.Send(context.Background(), "feishu", cfg(srv.URL), payload)
		if !res.Sent {
			t.Fatalf("feishu: %+v", res)
		}
		if got["msg_type"] != "text" || got["content"].(map[string]any)["text"] != "[alert-platform] firing HighLat policy_version=7" {
			t.Fatalf("feishu payload: %+v", got)
		}
	})

	t.Run("dingtalk payload shape", func(t *testing.T) {
		var got map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&got)
			if r.URL.Query().Get("timestamp") == "" || r.URL.Query().Get("sign") == "" {
				t.Error("signed dingtalk bot must carry timestamp/sign")
			}
			_, _ = w.Write([]byte(`{"errcode":0}`))
		}))
		defer srv.Close()
		signed, _ := json.Marshal(Config{WebhookURL: srv.URL, Secret: "SECtest"})
		res := sender.Send(context.Background(), "dingtalk", signed, payload)
		if !res.Sent {
			t.Fatalf("dingtalk: %+v", res)
		}
		if got["msgtype"] != "text" {
			t.Fatalf("dingtalk payload: %+v", got)
		}
	})

	t.Run("timeout is retryable", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(2 * time.Second)
		}))
		defer srv.Close()
		s := NewHTTPSender(true)
		s.Client.Timeout = 200 * time.Millisecond
		res := s.Send(context.Background(), "webhook", cfg(srv.URL), payload)
		if res.Sent || res.FailureCode != "retryable" {
			t.Fatalf("timeout: %+v", res)
		}
	})

	t.Run("missing webhook_url is permanent", func(t *testing.T) {
		res := sender.Send(context.Background(), "webhook", []byte(`{}`), payload)
		if res.FailureCode != "permanent" {
			t.Fatalf("empty config: %+v", res)
		}
	})
}

// T-013: channel endpoints may not reach metadata/private/loopback space;
// the guard sits on the resolved IP (dial layer).
func TestSSRFGuard(t *testing.T) {
	blocked := []string{
		"http://169.254.169.254/latest/meta-data/", // cloud metadata
		"http://127.0.0.1:8080/hook",
		"http://10.1.2.3/hook",
		"http://192.168.1.1/hook",
		"http://172.16.5.4/hook",
	}
	sender := NewHTTPSender(false)
	for _, url := range blocked {
		res := sender.Send(context.Background(), "webhook", cfg(url), payload)
		if res.Sent || res.FailureCode != "ssrf" {
			t.Errorf("%s must be SSRF-blocked, got %+v", url, res)
		}
	}

	// A hostname resolving into private space is blocked too (dial-layer).
	res := sender.Send(context.Background(), "webhook", cfg("http://localhost.localdomain/hook"), payload)
	if res.Sent {
		t.Errorf("private-resolving host accepted: %+v", res)
	}

	// The explicit dev/test escape hatch allows the same endpoint.
	allowed := NewHTTPSender(true)
	if res := allowed.Send(context.Background(), "webhook", cfg("http://127.0.0.1:1/hook"), payload); res.FailureCode == "ssrf" {
		t.Errorf("allowPrivate still blocked: %+v", res)
	}
}

func TestBlockedIPRanges(t *testing.T) {
	for _, c := range []struct {
		ip   string
		want bool
	}{
		{"169.254.169.254", true},
		{"127.0.0.1", true},
		{"10.0.0.1", true},
		{"192.168.0.1", true},
		{"172.31.255.255", true},
		{"100.64.0.1", true}, // CGNAT
		{"::1", true},
		{"fe80::1", true},
		{"8.8.8.8", false},
		{"1.1.1.1", false},
	} {
		if got := blockedIP(net.ParseIP(c.ip)); got != c.want {
			t.Errorf("blockedIP(%s) = %v, want %v", c.ip, got, c.want)
		}
	}
	_ = fmt.Sprint()
}
