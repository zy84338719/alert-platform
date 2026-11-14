// Package routing is the pure notification-routing/silence evaluator
// (docs/04 §5 匹配顺序: route policy -> silence -> outbox). No I/O: callers
// load policies/silences/channels and pass them in; evaluation is
// deterministic and version-stamped so every notification records the
// policy_version that produced it.
package routing

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Matcher is one label condition (ops: EQ/NEQ/REGEX_IN/REGEX_NOT_IN).
type Matcher struct {
	Name  string `json:"name"`
	Op    string `json:"operation"`
	Value string `json:"value"`
}

// Matches evaluates one matcher against a label set.
func (m Matcher) Matches(labels map[string]string) bool {
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
		re, err := regexp.Compile(m.Value)
		return err == nil && re.MatchString(v)
	case "REGEX_NOT_IN":
		if !ok {
			return true
		}
		re, err := regexp.Compile(m.Value)
		return err == nil && !re.MatchString(v)
	default:
		return false
	}
}

// MatchAll evaluates an AND over matchers.
func MatchAll(ms []Matcher, labels map[string]string) bool {
	for _, m := range ms {
		if !m.Matches(labels) {
			return false
		}
	}
	return true
}

// Receiver is one notification destination resolved by a route.
type Receiver struct {
	ChannelID string `json:"channel_id"`
	ReceiverID string `json:"receiver_id"`
	SendResolved bool `json:"send_resolved"`
}

// Route is one tenant route policy (mirrors policy_versions.spec).
type Route struct {
	Version  int64
	Priority int32
	Matchers []Matcher
	Receivers []Receiver
}

// Silence is one silence window (mirrors silences rows).
type Silence struct {
	ID       string
	Matchers []Matcher
	StartsAt time.Time
	EndsAt   time.Time
	Version  int64
}

// Active reports whether the silence covers t.
func (s Silence) Active(t time.Time) bool {
	return !t.Before(s.StartsAt) && t.Before(s.EndsAt)
}

// Decision is the routing outcome for one state-change notification.
type Decision struct {
	// Receivers is empty when nothing matched (no route, or all muted).
	Receivers     []Receiver
	PolicyVersion int64 // version of the winning route; 0 = default channel
	SilencedBy    string
}

// EvaluateForStateChange resolves receivers for a firing/resolved transition
// in docs/04 §5 order: route policy FIRST, then silence (a silence suppresses
// the resolved receiver set; it never alters the state change itself — T-009).
func EvaluateForStateChange(routes []Route, silences []Silence, labels map[string]string, notificationType string, now time.Time) Decision {
	dec := Decision{}
	for _, r := range routes { // caller pre-sorts by priority
		if MatchAll(r.Matchers, labels) {
			dec.Receivers = filterByType(r.Receivers, notificationType)
			dec.PolicyVersion = r.Version
			break
		}
	}
	for _, s := range silences {
		if s.Active(now) && MatchAll(s.Matchers, labels) {
			dec.SilencedBy = s.ID
			break
		}
	}
	return dec
}

func filterByType(rs []Receiver, notificationType string) []Receiver {
	if notificationType == "firing" {
		return rs
	}
	out := make([]Receiver, 0, len(rs))
	for _, r := range rs {
		if r.SendResolved {
			out = append(out, r)
		}
	}
	return out
}

// IdempotencyKey builds the docs/04 §5 duplicate key:
// subject_type + subject_id + notification_type + channel + receiver +
// escalation_stage + occurrence_version.
func IdempotencyKey(subjectType, subjectID, notificationType, channelID, receiverID string, escalationStage int, occurrenceVersion string) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%d|%s",
		subjectType, subjectID, notificationType, channelID, receiverID, escalationStage, occurrenceVersion)
}

// RenderMessage produces the channel-agnostic body; policy_version is always
// embedded so the delivered text is traceable to the rule that caused it.
func RenderMessage(alertName, severity, state, policyVersion, tenantID, instanceID string) string {
	return strings.Join([]string{
		"[alert-platform]",
		state,
		alertName,
		"severity=" + severity,
		"tenant=" + tenantID,
		"instance=" + instanceID,
		"policy_version=" + policyVersion,
	}, " ")
}
