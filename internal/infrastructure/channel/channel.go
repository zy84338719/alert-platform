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
	Client       *http.Client
	AllowPrivate bool // tests/local only; NEVER enable in production configs
	// resolve is overridable in tests (DNS rebinding drill); production uses
	// net.DefaultResolver.LookupIPAddr.
	resolve func(ctx context.Context, host string) ([]net.IPAddr, error)
}

// NewHTTPSender builds the sender; allowPrivate flips the SSRF dial guard.
func NewHTTPSender(allowPrivate bool) *HTTPSender {
	s := &HTTPSender{AllowPrivate: allowPrivate}
	s.resolve = func(ctx context.Context, host string) ([]net.IPAddr, error) {
		return net.DefaultResolver.LookupIPAddr(ctx, host)
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{
		DialContext: s.guardedDial(dialer),
	}
	s.Client = &http.Client{Timeout: 10 * time.Second, Transport: transport}
	return s
}

// guardedDial resolves and validates the endpoint, then dials the VALIDATED IP
// literally (P8-P0-04). Dialing the original hostname lets the dialer resolve
// again — a DNS-rebinding target could answer the check with a public address
// and the connect with a private one. Pinning the address closes that window.
// TLS SNI and the HTTP Host header still come from the URL, so certificates
// and virtual hosting are unaffected.
func (s *HTTPSender) guardedDial(d *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("channel dial %q: %w", addr, err)
		}
		resolve := s.resolve
		if resolve == nil { // sender built as a literal, not via NewHTTPSender
			resolve = func(ctx context.Context, host string) ([]net.IPAddr, error) {
				return net.DefaultResolver.LookupIPAddr(ctx, host)
			}
		}
		ips, err := resolve(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("channel resolve %q: %w", host, err)
		}
		// EVERY resolved address must pass the guard (multi-A/AAAA answers),
		// and the connect uses the first validated one — never the hostname.
		var chosen net.IP
		for _, ip := range ips {
			if !s.AllowPrivate && blockedIP(ip.IP) {
				return nil, fmt.Errorf("channel endpoint %q resolves to forbidden address %s (SSRF guard, T-013)", host, ip.IP)
			}
			if chosen == nil {
				chosen = ip.IP
			}
		}
		if chosen == nil {
			return nil, fmt.Errorf("channel resolve %q: no addresses", host)
		}
		return d.DialContext(ctx, network, net.JoinHostPort(chosen.String(), port))
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
	defer func() { _ = resp.Body.Close() }()
	// The body must survive until vendor validation (P8-P0-03): a 2xx alone
	// does NOT mean the bot accepted the message.
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))

	res := classify(resp)
	if channelType == "feishu" || channelType == "dingtalk" {
		res = vendorCheck(channelType, res, respBody)
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

// vendorBody is the union of feishu/dingtalk envelope fields.
type vendorBody struct {
	Code          *int   `json:"code"`
	Msg           string `json:"msg"`
	StatusCode    *int   `json:"StatusCode"`
	StatusMessage string `json:"StatusMessage"`
	ErrCode       *int   `json:"errcode"`
	ErrMsg        string `json:"errmsg"`
}

// vendorCheck validates the vendor envelope of a 2xx feishu/dingtalk response
// (P8-P0-03). Both vendors answer HTTP 200 even when the bot rejects the
// message; the business code inside the body is the real outcome:
//
//	feishu   {"code":0,"msg":"success"}            (legacy: {"StatusCode":0,...})
//	dingtalk {"errcode":0,"errmsg":"ok"}
//
// Non-zero business codes are failures: rate-limit codes stay retryable, every
// other code (bad token/signature/args) is permanent. A 2xx with an
// unparseable/empty body cannot prove acceptance — it is surfaced as a
// retryable "unverified" failure instead of a silent success.
func vendorCheck(channelType string, res SendResult, body []byte) SendResult {
	if !res.Sent {
		return res // HTTP-level failure already classified
	}
	var env vendorBody
	if err := json.Unmarshal(body, &env); err != nil {
		res.Sent = false
		res.FailureCode = "retryable"
		res.Err = "vendor response unparseable: " + sanitizeVendorMsg(string(body))
		return res
	}

	code, msg, ok := vendorEnvelope(channelType, &env)
	if !ok {
		res.Sent = false
		res.FailureCode = "retryable"
		res.Err = channelType + " response carries no business code: " + sanitizeVendorMsg(string(body))
		return res
	}
	if code == 0 {
		return res
	}
	res.Sent = false
	if vendorRetryableCode(channelType, code) {
		res.FailureCode = "retryable"
	} else {
		res.FailureCode = "permanent"
	}
	res.Err = fmt.Sprintf("%s rejected: code=%d msg=%s", channelType, code, sanitizeVendorMsg(msg))
	return res
}

// vendorEnvelope extracts the business code from the vendor-specific shape.
func vendorEnvelope(channelType string, env *vendorBody) (code int, msg string, ok bool) {
	switch channelType {
	case "feishu":
		if env.Code != nil {
			return *env.Code, env.Msg, true
		}
		if env.StatusCode != nil { // legacy feishu envelope
			return *env.StatusCode, env.StatusMessage, true
		}
		return 0, "", false
	case "dingtalk":
		if env.ErrCode != nil {
			return *env.ErrCode, env.ErrMsg, true
		}
		if env.Code != nil {
			return *env.Code, env.Msg, true
		}
		return 0, "", false
	}
	return 0, "", false
}

// vendorRetryableCode lists the vendor business codes that signal throttling
// rather than a rejected message; everything else non-zero is permanent.
func vendorRetryableCode(channelType string, code int) bool {
	switch channelType {
	case "dingtalk":
		return code == 130101 // "send too fast" rate limit
	case "feishu":
		return code == 9499 || code == 99991400 // frequency limit
	}
	return false
}

// sanitizeVendorMsg keeps the audit trail useful without leaking payloads:
// control characters stripped, length capped.
func sanitizeVendorMsg(msg string) string {
	msg = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		if r < 0x20 {
			return -1
		}
		return r
	}, msg)
	if len(msg) > 200 {
		msg = msg[:200]
	}
	return msg
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
