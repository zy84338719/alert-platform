package channel

import (
	"context"
	"encoding/json"
	"time"

	"github.com/oekit/logx"
)

// SelfAlerter delivers platform self-monitoring alerts through a dedicated
// webhook that does NOT touch notification_outbox (docs/08 §3: a notification
// pipeline outage must never silence the platform's own alarms). Delivery is
// best-effort with the same SSRF-dial guard as channels; failures are logged
// only — the caller is already in a failure path.
type SelfAlerter struct {
	sender *HTTPSender
	url    string
}

// NewSelfAlerter returns nil when no webhook is configured (disabled).
func NewSelfAlerter(webhookURL string, allowPrivate bool) *SelfAlerter {
	if webhookURL == "" {
		return nil
	}
	return &SelfAlerter{sender: NewHTTPSender(allowPrivate), url: webhookURL}
}

// Notify posts one self-alert. Safe to call on a nil receiver.
func (s *SelfAlerter) Notify(ctx context.Context, component, message string) {
	if s == nil || s.url == "" {
		return
	}
	payload, err := json.Marshal(map[string]any{
		"source":    "alert-platform",
		"component": component,
		"severity":  "critical",
		"message":   message,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return
	}
	cfg, err := json.Marshal(Config{WebhookURL: s.url})
	if err != nil {
		return
	}
	res := s.sender.Send(ctx, "webhook", cfg, payload)
	if !res.Sent {
		logx.CtxWarn(ctx, "self-alert fallback delivery failed",
			"component", component, "failure", res.FailureCode, "detail", res.Err)
		return
	}
	logx.CtxInfo(ctx, "self-alert fallback delivered", "component", component)
}
