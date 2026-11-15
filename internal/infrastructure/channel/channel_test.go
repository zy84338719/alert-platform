package channel

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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

// P8-P0-03 失败测试：HTTP 200 ≠ 发送成功 ——
// 飞书/钉钉业务错误码藏在 200 响应体里；修复前 vendorCheck 是占位实现且
// body 已被丢弃，{"code":非零} 会被判 sent（平台记录成功、机器人实际拒绝）。
// 矩阵覆盖：两家成功码、业务拒绝（permanent）、限流码（retryable）、
// 空体/非 JSON（retryable unverified）、审计信息脱敏截断。
func TestVendorBusinessCodeValidation(t *testing.T) {
	sender := NewHTTPSender(true)
	cases := []struct {
		name        string
		channelType string
		body        string
		sent        bool
		failure     string
	}{
		{"feishu ok", "feishu", `{"code":0,"msg":"success","data":{}}`, true, ""},
		{"feishu legacy envelope ok", "feishu", `{"StatusCode":0,"StatusMessage":"success"}`, true, ""},
		{"feishu business reject", "feishu", `{"code":19021,"msg":"param error"}`, false, "permanent"},
		{"feishu rate limited", "feishu", `{"code":9499,"msg":"too many request"}`, false, "retryable"},
		{"dingtalk ok", "dingtalk", `{"errcode":0,"errmsg":"ok"}`, true, ""},
		{"dingtalk business reject", "dingtalk", `{"errcode":310000,"errmsg":"sign not match"}`, false, "permanent"},
		{"dingtalk throttled", "dingtalk", `{"errcode":130101,"errmsg":"send too fast"}`, false, "retryable"},
		{"feishu empty body", "feishu", ``, false, "retryable"},
		{"dingtalk non-json body", "dingtalk", `<html>proxy error</html>`, false, "retryable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			res := sender.Send(context.Background(), tc.channelType, cfg(srv.URL), payload)
			if res.Sent != tc.sent || res.FailureCode != tc.failure {
				t.Fatalf("%s: sent=%v failure=%q err=%q, want sent=%v failure=%q",
					tc.name, res.Sent, res.FailureCode, res.Err, tc.sent, tc.failure)
			}
			if res.HTTPStatus != http.StatusOK {
				t.Fatalf("%s: HTTPStatus = %d, want 200", tc.name, res.HTTPStatus)
			}
		})
	}

	// 审计信息：业务错误消息截断到 200 字符且不含控制字符。
	long := strings.Repeat("x", 500) + "\nsecond-line"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":19001,"msg":"` + strings.Repeat("x", 500) + `\nsecond-line"}`))
	}))
	defer srv.Close()
	res := sender.Send(context.Background(), "feishu", cfg(srv.URL), payload)
	if res.Sent || res.FailureCode != "permanent" {
		t.Fatalf("long reject: %+v", res)
	}
	if strings.ContainsAny(res.Err, "\n\r\t") {
		t.Fatalf("audit message leaked control characters: %q", res.Err)
	}
	if len(res.Err) > 240 {
		t.Fatalf("audit message not capped: %d chars", len(res.Err))
	}
	_ = long
}

// P8-P0-04 失败测试：DNS Rebinding —— 校验之后必须连接"校验过的 IP 字面量"，
// 不能在 Dial 时二次解析域名。修复前 guardedDial 先 LookupIPAddr 校验、
// 再 d.DialContext(ctx, network, addr)（addr 仍是域名）→ 二次解析可返回
// 内网地址，绕过 T-013 防护。
func TestGuardedDialPinsValidatedIP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				_, _ = c.Read(buf)
				_, _ = c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"))
			}(c)
		}
	}()

	sender := NewHTTPSender(true) // 127.0.0.1 allowed for this drill
	sender.resolve = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	}
	// .invalid never resolves publicly: only IP pinning can make this succeed.
	host := "rebind.invalid"
	endpoint := fmt.Sprintf("http://%s:%d/hook", host, ln.Addr().(*net.TCPAddr).Port)
	res := sender.Send(context.Background(), "webhook", cfg(endpoint), payload)
	if !res.Sent {
		t.Fatalf("dial did not use the validated IP (re-resolved the hostname): %+v", res)
	}
}

// 多 A/AAAA：任一解析结果落在禁区内即整体拒绝（不给 rebinding 留窗口），
// 且失败被归类为 permanent/ssrf（配置错误，重试无意义）。
func TestGuardedDialRejectsBlockedRebind(t *testing.T) {
	sender := NewHTTPSender(false)
	sender.resolve = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{
			{IP: net.ParseIP("93.184.216.34")},   // public-looking first answer
			{IP: net.ParseIP("169.254.169.254")}, // metadata endpoint in the same set
		}, nil
	}
	res := sender.Send(context.Background(), "webhook", cfg("http://metadata.invalid:80/hook"), payload)
	if res.Sent || res.FailureCode != "ssrf" {
		t.Fatalf("mixed-resolution rebind must be rejected as ssrf: %+v", res)
	}
}
