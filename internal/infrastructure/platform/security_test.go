package platform

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
)

// AUD-016: every response carries the browser hardening headers, and the CSP
// remains compatible with the console's same-origin assets.
func TestSecurityHeadersMatrix(t *testing.T) {
	h := server.Default(server.WithHostPorts(":0"))
	h.Use(SecurityHeaders())
	h.GET("/console", func(_ context.Context, c *app.RequestContext) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte("<html></html>"))
	})
	w := ut.PerformRequest(h.Engine, http.MethodGet, "/console", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("console: %d", w.Code)
	}
	want := map[string]string{
		"X-Frame-Options":        "DENY",
		"X-Content-Type-Options": "nosniff",
		"Referrer-Policy":        "no-referrer",
	}
	for k, v := range want {
		if got := w.Header().Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	csp := w.Header().Get("Content-Security-Policy")
	for _, part := range []string{"default-src 'self'", "script-src 'self'", "frame-ancestors 'none'", "object-src 'none'"} {
		if !strings.Contains(csp, part) {
			t.Errorf("CSP missing %q: %s", part, csp)
		}
	}
	if pp := w.Header().Get("Permissions-Policy"); !strings.Contains(pp, "camera=()") {
		t.Errorf("Permissions-Policy = %q", pp)
	}
}

// AUD-016: the body bound is explicit and enforced by the real HTTP server
// (hertz applies it while reading the request, which ut.PerformRequest
// bypasses).
func TestMaxRequestBodyExplicit(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(server.WithListener(ln), server.WithMaxRequestBodySize(1024))
	h.POST("/echo", func(_ context.Context, c *app.RequestContext) {
		_, _ = c.Body()
		c.String(http.StatusOK, "ok")
	})
	go func() { _ = h.Run() }()
	defer func() { _ = h.Close() }()

	url := "http://" + ln.Addr().String() + "/echo"
	client := &http.Client{Timeout: 2 * time.Second}
	// Wait for the listener to serve.
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := client.Post(url, "application/json", strings.NewReader(`{}`))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	big := strings.NewReader(strings.Repeat("a", 2048))
	resp, err := client.Post(url, "application/json", big)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: %d, want 413", resp.StatusCode)
	}
	if MaxRequestBodyBytes != 8<<20 {
		t.Fatalf("MaxRequestBodyBytes = %d, constant drift", MaxRequestBodyBytes)
	}
}
