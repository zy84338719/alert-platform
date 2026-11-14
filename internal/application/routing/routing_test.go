package routing

import (
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

// Matcher ops: EQ/NEQ/REGEX_IN/REGEX_NOT_IN (docs policy.v1.LabelMatcher).
func TestMatcherOps(t *testing.T) {
	labels := map[string]string{"severity": "critical", "ns": "prod"}
	cases := []struct {
		m      Matcher
		want   bool
	}{
		{Matcher{"severity", "EQ", "critical"}, true},
		{Matcher{"severity", "EQ", "warning"}, false},
		{Matcher{"severity", "NEQ", "warning"}, true},
		{Matcher{"missing", "NEQ", "x"}, true},
		{Matcher{"ns", "REGEX_IN", "^pro(d|j)$"}, true},
		{Matcher{"ns", "REGEX_IN", "^dev$"}, false},
		{Matcher{"ns", "REGEX_NOT_IN", "^dev$"}, true},
		{Matcher{"ns", "REGEX_NOT_IN", "^pro"}, false},
		{Matcher{"ns", "BOGUS", "x"}, false},
	}
	for _, c := range cases {
		if got := c.m.Matches(labels); got != c.want {
			t.Errorf("%+v = %v, want %v", c.m, got, c.want)
		}
	}
}

// docs/04 §5 order: route policy first, then silence; silence suppresses the
// notification but NEVER touches the state change itself (T-009 — the caller
// applies state regardless; here we only assert Decision fields).
func TestEvaluateOrderRouteThenSilence(t *testing.T) {
	routes := []Route{{
		Version: 7, Priority: 1,
		Matchers: []Matcher{{"ns", "EQ", "prod"}},
		Receivers: []Receiver{{ChannelID: "ch1", ReceiverID: "sre", SendResolved: true}},
	}}
	silences := []Silence{{
		ID: "sil-1", StartsAt: t0.Add(-time.Minute), EndsAt: t0.Add(time.Hour),
		Matchers: []Matcher{{"ns", "EQ", "prod"}},
	}}

	dec := EvaluateForStateChange(routes, silences, map[string]string{"ns": "prod"}, "firing", t0)
	if len(dec.Receivers) != 1 || dec.PolicyVersion != 7 {
		t.Fatalf("route must still resolve receivers: %+v", dec)
	}
	if dec.SilencedBy != "sil-1" {
		t.Fatalf("active silence must mark the decision, got %+v", dec)
	}

	// Silence expired -> decision goes back to plain routing.
	dec = EvaluateForStateChange(routes, silences, map[string]string{"ns": "prod"}, "firing", t0.Add(2*time.Hour))
	if dec.SilencedBy != "" || !dec.Receivers[0].SendResolved {
		t.Fatalf("expired silence must not suppress: %+v", dec)
	}

	// Non-matching silence leaves unrelated labels alone.
	dec = EvaluateForStateChange(routes, []Silence{{
		ID: "sil-2", StartsAt: t0.Add(-time.Minute), EndsAt: t0.Add(time.Hour),
		Matchers: []Matcher{{"ns", "EQ", "dev"}},
	}}, map[string]string{"ns": "prod"}, "firing", t0)
	if dec.SilencedBy != "" {
		t.Fatalf("non-matching silence suppressed: %+v", dec)
	}

	// send_resolved=false receivers drop out of resolved notifications.
	routes[0].Receivers[0].SendResolved = false
	dec = EvaluateForStateChange(routes, nil, map[string]string{"ns": "prod"}, "resolved", t0)
	if len(dec.Receivers) != 0 {
		t.Fatalf("resolved must respect send_resolved=false: %+v", dec)
	}
}

// The docs/04 §5 duplicate key embeds every varying dimension: replaying the
// same transition must produce the identical key, any dimension change must
// not collide.
func TestIdempotencyKeyShape(t *testing.T) {
	a := IdempotencyKey("alert_instance", "inst-1", "firing", "ch-1", "sre", 0, "tr-1")
	b := IdempotencyKey("alert_instance", "inst-1", "firing", "ch-1", "sre", 0, "tr-1")
	if a != b {
		t.Fatal("same inputs must produce the same key")
	}
	for _, mutated := range []string{
		IdempotencyKey("incident", "inst-1", "firing", "ch-1", "sre", 0, "tr-1"),
		IdempotencyKey("alert_instance", "inst-2", "firing", "ch-1", "sre", 0, "tr-1"),
		IdempotencyKey("alert_instance", "inst-1", "resolved", "ch-1", "sre", 0, "tr-1"),
		IdempotencyKey("alert_instance", "inst-1", "firing", "ch-2", "sre", 0, "tr-1"),
		IdempotencyKey("alert_instance", "inst-1", "firing", "ch-1", "sre", 1, "tr-1"),
		IdempotencyKey("alert_instance", "inst-1", "firing", "ch-1", "sre", 0, "tr-2"),
	} {
		if mutated == a {
			t.Fatalf("dimension change collided: %s", mutated)
		}
	}
}

func TestRenderMessageEmbedsPolicyVersion(t *testing.T) {
	msg := RenderMessage("HighLatency", "critical", "firing", "42", "t1", "i1")
	for _, want := range []string{"HighLatency", "policy_version=42", "firing"} {
		if !contains(msg, want) {
			t.Fatalf("message %q missing %q", msg, want)
		}
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
