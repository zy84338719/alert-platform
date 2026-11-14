// Package policymgmt implements route/silence/channel management (POL-001,
// UI-002 backend): version-stamped writes, explainable silence preview, and
// SSRF validation of channel endpoints at CONFIG time (the send path re-checks
// at the dial layer — T-013 is enforced twice).
package policymgmt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Errors surfaced as HTTP codes.
var (
	ErrNotFound        = errors.New("object not found")
	ErrVersionConflict = errors.New("expected_version conflict")
)

// Matcher mirrors policy.v1.LabelMatcher (validated ops only).
type Matcher struct {
	Name  string `json:"name"`
	Op    string `json:"operation"`
	Value string `json:"value"`
}

// RouteReceiver is one route destination.
type RouteReceiver struct {
	ChannelID    string `json:"channel_id"`
	ReceiverID   string `json:"receiver_id"`
	SendResolved bool   `json:"send_resolved"`
}

// Route is a tenant route policy (immutable versioned rows).
type Route struct {
	PolicyID  string
	TenantID  string
	Version   int64
	Priority  int64
	Matchers  []Matcher
	Receivers []RouteReceiver
	Enabled   bool
}

// Silence is a mute window.
type Silence struct {
	SilenceID string
	TenantID  string
	Matchers  []Matcher
	StartsAt  time.Time
	EndsAt    time.Time
	CreatedBy string
	Reason    string
	Version   int64
}

// Channel is a delivery channel (secret is write-only).
type Channel struct {
	ChannelID  string
	TenantID   string
	Name       string
	Type       string
	WebhookURL string
	Secret     string
	Enabled    bool
	IsDefault  bool
}

// Store is the persistence port (infrastructure/postgres).
type Store interface {
	CreateRoute(ctx context.Context, r *Route) error
	ListRoutes(ctx context.Context, tenantID string) ([]*Route, error)
	SetRouteEnabled(ctx context.Context, tenantID, policyID string, enabled bool, expectedVersion int64) (int64, error)
	CreateSilence(ctx context.Context, s *Silence) error
	ListSilences(ctx context.Context, tenantID string, includeExpired bool) ([]*Silence, error)
	RevokeSilence(ctx context.Context, tenantID, silenceID string) (*Silence, error)
	CreateChannel(ctx context.Context, c *Channel) error
	ListChannels(ctx context.Context, tenantID string) ([]*Channel, error)
	SetChannelEnabled(ctx context.Context, tenantID, channelID string, enabled bool) (*Channel, error)
}

// Service wires the management use cases.
type Service struct {
	store        Store
	allowPrivate bool // dev/test ONLY: skips the config-time SSRF host check
}

// NewService builds the management service.
func NewService(store Store) *Service { return &Service{store: store} }

// SetAllowPrivate enables the dev/test escape hatch (same env flag as the
// worker's dial guard; production configs never set it).
func (s *Service) SetAllowPrivate(allow bool) { s.allowPrivate = allow }

var matcherOps = map[string]bool{"EQ": true, "NEQ": true, "REGEX_IN": true, "REGEX_NOT_IN": true}

func validateMatchers(ms []Matcher) error {
	if len(ms) == 0 {
		return fmt.Errorf("at least one matcher is required")
	}
	for _, m := range ms {
		if m.Name == "" || !matcherOps[m.Op] {
			return fmt.Errorf("invalid matcher %q (op must be EQ/NEQ/REGEX_IN/REGEX_NOT_IN)", m.Name)
		}
		if m.Op == "REGEX_IN" || m.Op == "REGEX_NOT_IN" {
			if _, err := regexp.Compile(m.Value); err != nil {
				return fmt.Errorf("matcher %q: bad regex: %w", m.Name, err)
			}
		}
	}
	return nil
}

// CreateRoute validates and persists a route version.
func (s *Service) CreateRoute(ctx context.Context, tenantID, actor string, r *Route) (*Route, error) {
	r.TenantID = tenantID
	if err := validateMatchers(r.Matchers); err != nil {
		return nil, err
	}
	if len(r.Receivers) == 0 {
		return nil, fmt.Errorf("at least one receiver is required")
	}
	if err := s.store.CreateRoute(ctx, r); err != nil {
		return nil, err
	}
	return r, nil
}

// ListRoutes returns the tenant's routes.
func (s *Service) ListRoutes(ctx context.Context, tenantID string) ([]*Route, error) {
	return s.store.ListRoutes(ctx, tenantID)
}

// SetRouteEnabled flips a route with optimistic version check.
func (s *Service) SetRouteEnabled(ctx context.Context, tenantID, policyID string, enabled bool, expectedVersion int64) (int64, error) {
	return s.store.SetRouteEnabled(ctx, tenantID, policyID, enabled, expectedVersion)
}

// CreateSilence validates the window and persists it (tenant is stamped by
// the service, never trusted from the wire).
func (s *Service) CreateSilence(ctx context.Context, tenantID string, sl *Silence) error {
	sl.TenantID = tenantID
	if err := validateMatchers(sl.Matchers); err != nil {
		return err
	}
	if !sl.EndsAt.After(sl.StartsAt) {
		return fmt.Errorf("ends_at must be after starts_at")
	}
	if sl.EndsAt.Before(time.Now()) {
		return fmt.Errorf("ends_at is already in the past")
	}
	return s.store.CreateSilence(ctx, sl)
}

// ListSilences returns the tenant's silences.
func (s *Service) ListSilences(ctx context.Context, tenantID string, includeExpired bool) ([]*Silence, error) {
	return s.store.ListSilences(ctx, tenantID, includeExpired)
}

// RevokeSilence ends a silence early.
func (s *Service) RevokeSilence(ctx context.Context, tenantID, silenceID string) (*Silence, error) {
	return s.store.RevokeSilence(ctx, tenantID, silenceID)
}

// PreviewMatch is the explainable result for ONE silence (UI-002 模拟器).
type PreviewMatch struct {
	SilenceID string
	Silenced  bool
	Matched   []string
	Unmatched []string
}

// PreviewSilences explains, per active silence, which matchers hit the label
// set. A silence silences iff ALL matchers match and the window covers `at`.
func (s *Service) PreviewSilences(ctx context.Context, tenantID string, labels map[string]string, at time.Time) ([]PreviewMatch, error) {
	silences, err := s.store.ListSilences(ctx, tenantID, false)
	if err != nil {
		return nil, err
	}
	if at.IsZero() {
		at = time.Now()
	}
	out := make([]PreviewMatch, 0, len(silences))
	for _, sl := range silences {
		pm := PreviewMatch{SilenceID: sl.SilenceID}
		inWindow := !at.Before(sl.StartsAt) && at.Before(sl.EndsAt)
		for _, m := range sl.Matchers {
			label := m.Name + " " + m.Op + " " + m.Value
			if matchOne(m, labels) {
				pm.Matched = append(pm.Matched, label)
			} else {
				pm.Unmatched = append(pm.Unmatched, label)
			}
		}
		pm.Silenced = inWindow && len(pm.Unmatched) == 0
		out = append(out, pm)
	}
	return out, nil
}

func matchOne(m Matcher, labels map[string]string) bool {
	v, ok := labels[m.Name]
	switch m.Op {
	case "EQ":
		return ok && v == m.Value
	case "NEQ":
		return !ok || v != m.Value
	case "REGEX_IN":
		if !ok {
			return false
		}
		re := regexp.MustCompile(m.Value)
		return re.MatchString(v)
	case "REGEX_NOT_IN":
		if !ok {
			return true
		}
		re := regexp.MustCompile(m.Value)
		return !re.MatchString(v)
	}
	return false
}

// CreateChannel validates the endpoint (scheme + SSRF at config time) and
// persists. The send path re-checks at the DIAL layer (T-013 double guard).
func (s *Service) CreateChannel(ctx context.Context, tenantID string, ch *Channel) error {
	ch.TenantID = tenantID
	switch ch.Type {
	case "webhook", "feishu", "dingtalk":
	default:
		return fmt.Errorf("unsupported channel type %q (webhook|feishu|dingtalk)", ch.Type)
	}
	if ch.Name == "" {
		return fmt.Errorf("name is required")
	}
	u, err := url.Parse(ch.WebhookURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("webhook_url must be a valid http(s) URL")
	}
	if !s.allowPrivate {
		if strings.EqualFold(u.Hostname(), "localhost") || strings.HasSuffix(strings.ToLower(u.Hostname()), ".local") || strings.HasSuffix(strings.ToLower(u.Hostname()), ".internal") {
			return fmt.Errorf("webhook_url host %q is not allowed (SSRF, T-013)", u.Hostname())
		}
		if ips, err := net.LookupIP(u.Hostname()); err == nil {
			for _, ip := range ips {
				if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
					return fmt.Errorf("webhook_url resolves to forbidden address %s (SSRF, T-013)", ip)
				}
			}
		}
	}
	return s.store.CreateChannel(ctx, ch)
}

// ListChannels returns the tenant's channels (secrets never echo).
func (s *Service) ListChannels(ctx context.Context, tenantID string) ([]*Channel, error) {
	return s.store.ListChannels(ctx, tenantID)
}

// SetChannelEnabled flips a channel.
func (s *Service) SetChannelEnabled(ctx context.Context, tenantID, channelID string, enabled bool) (*Channel, error) {
	return s.store.SetChannelEnabled(ctx, tenantID, channelID, enabled)
}
