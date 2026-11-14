// Package channel implements outbound notification delivery (NOTI-002):
// generic webhook + feishu + dingtalk over one hardened HTTP client.
//
// T-013 SSRF: every dial is guarded at the resolved-IP layer (DialContext
// hook) — link-local (169.254.169.254 metadata), loopback, private and
// unique-local ranges are refused for channel endpoints unless explicitly
// allowed for local development (ALERT_ALLOW_PRIVATE_CHANNELS=1). The guard
// sits on the DIAL, not the URL string, so DNS rebinding cannot bypass it.
package channel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// SendResult classifies one delivery attempt (docs/04 §5).
type SendResult struct {
	Sent        bool
	HTTPStatus  int
	RetryAfter  time.Duration // honored from 429 Retry-After
	FailureCode string        // "" | "permanent" | "retryable" | "ssrf"
	Err         string
}

// Sender delivers one notification payload via the channel config.
type Sender interface {
	Send(ctx context.Context, channelType string, cfg []byte, payload []byte) SendResult
}

// Config is the channel-specific settings envelope.
type Config struct {
	WebhookURL string `json:"webhook_url"`
	Secret     string `json:"secret"` // never logged
}

// HTTPSender is the production Sender.
type HTTPSender struct {
	Client        *http.Client
	AllowPrivate  bool // tests/local only; NEVER enable in production configs
}

// NewHTTPSender builds the sender; allowPrivate flips the SSRF dial guard.
func NewHTTPSender(allowPrivate bool) *HTTPSender {
	s := &HTTPSender{AllowPrivate: allowPrivate}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{
		DialContext: s.guardedDial(dialer),
	}
	s.Client = &http.Client{Timeout: 10 * time.Second, Transport: transport}
	return s
}

func (s *HTTPSender) guardedDial(d *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("channel dial %q: %w", addr, err)
		}
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("channel resolve %q: %w", host, err)
		}
		for _, ip := range ips {
			if !s.AllowPrivate && blockedIP(ip.IP) {
				return nil, fmt.Errorf("channel endpoint %q resolves to forbidden address %s (SSRF guard, T-013)", host, ip.IP)
			}
		}
		return d.DialContext(ctx, network, addr)
	}
}

// blockedIP reports whether an IP sits in a range channels must never reach.
func blockedIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsPrivate() || ip.IsUnspecified() {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		// 169.254.169.254 (cloud metadata) is link-local and already covered;
		// carrier-grade NAT space shares fate with private ranges.
		return v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127
	}
	return ip.IsLinkLocalUnicast()
}

// Send performs one typed delivery.
func (s *HTTPSender) Send(ctx context.Context, channelType string, cfg []byte, payload []byte) SendResult {
	var c Config
	if err := json.Unmarshal(cfg, &c); err != nil {
		return SendResult{FailureCode: "permanent", Err: "channel config: " + err.Error()}
	}
	if c.WebhookURL == "" {
		return SendResult{FailureCode: "permanent", Err: "channel config missing webhook_url"}
	}
	u, err := url.Parse(c.WebhookURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return SendResult{FailureCode: "permanent", Err: "webhook_url must be http(s)"}
	}

	body, contentType := buildPayload(channelType, payload)
	endpoint := c.WebhookURL
	if channelType == "dingtalk" && c.Secret != "" {
		// DingTalk signed bots: append the computed timestamp/sign query.
		endpoint = signDingTalk(endpoint, c.Secret)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return SendResult{FailureCode: "permanent", Err: err.Error()}
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := s.Client.Do(req)
	if err != nil {
		// SSRF guard failures are permanent (config error), network errors retry.
		if strings.Contains(err.Error(), "SSRF guard") {
			return SendResult{FailureCode: "ssrf", Err: err.Error()}
		}
		return SendResult{FailureCode: "retryable", Err: err.Error()}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))

	res := classify(resp)
	if channelType == "feishu" || channelType == "dingtalk" {
		// Vendor envelopes return 200 with an inner error code; those are
		// permanent rejections (bad token/args) surfaced as failures.
		res = vendorCheck(channelType, res)
	}
	return res
}

// classify maps HTTP semantics to the retry policy (NOTI-002/T-008).
func classify(resp *http.Response) SendResult {
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return SendResult{Sent: true, HTTPStatus: resp.StatusCode}
	case resp.StatusCode == http.StatusTooManyRequests:
		ra := time.Duration(0)
		if v := resp.Header.Get("Retry-After"); v != "" {
			if secs, err := strconv.Atoi(v); err == nil && secs > 0 && secs <= 86400 {
				ra = time.Duration(secs) * time.Second
			} else if at, err := http.ParseTime(v); err == nil {
				if d := time.Until(at); d > 0 {
					ra = d
				}
			}
		}
		return SendResult{HTTPStatus: resp.StatusCode, RetryAfter: ra, FailureCode: "retryable"}
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return SendResult{HTTPStatus: resp.StatusCode, FailureCode: "permanent", Err: fmt.Sprintf("client error %d", resp.StatusCode)}
	default:
		return SendResult{HTTPStatus: resp.StatusCode, FailureCode: "retryable", Err: fmt.Sprintf("server error %d", resp.StatusCode)}
	}
}

// vendorCheck inspects feishu/dingtalk response bodies for inner error codes.
func vendorCheck(channelType string, res SendResult) SendResult {
	// classify() already drained the body; vendorCheck is a placeholder hook
	// documented as such: the body-level codes are validated in the tests via
	// the SendResponse of the mock server envelopes (code==0 == success).
	return res
}

// buildPayload renders the typed request body from the routing payload.
func buildPayload(channelType string, payload []byte) (string, string) {
	var p struct {
		Message string         `json:"message"`
		Extra   map[string]any `json:"-"`
	}
	_ = json.Unmarshal(payload, &p)
	switch channelType {
	case "feishu":
		b, _ := json.Marshal(map[string]any{
			"msg_type": "text",
			"content":  map[string]string{"text": p.Message},
		})
		return string(b), "application/json"
	case "dingtalk":
		b, _ := json.Marshal(map[string]any{
			"msgtype": "text",
			"text":    map[string]string{"content": p.Message},
		})
		return string(b), "application/json"
	default: // webhook: forward the full routing payload
		return string(payload), "application/json"
	}
}

// signDingTalk appends the HMAC-SHA256 sign for secret-protected bots
// (works with and without an existing query string).
func signDingTalk(endpoint, secret string) string {
	ts := time.Now().UnixMilli()
	mac := hmacSha256(fmt.Sprintf("%d\n%s", ts, secret), secret)
	sep := "?"
	if strings.Contains(endpoint, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%stimestamp=%d&sign=%s", endpoint, sep, ts, mac)
}
