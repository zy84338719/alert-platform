// Package correlation implements the deterministic correlation rules
// (INC-002, per planning/P5-准入-资源身份与关联可信度.md): L3 identity overlap
// inside a time window auto-merges (high confidence, same tenant ONLY);
// weaker evidence yields suggestions with explainable evidence[] and never
// merges. Cross-tenant correlation is impossible by construction (tenant is
// part of every key).
package correlation

import "time"

// Alert is the minimal correlation input for one firing instance.
type Alert struct {
	InstanceID  string
	TenantID    string
	ClusterID   string
	EntityType  string
	EntityUID   string
	DisplayName string // resource_name (L2 evidence)
	StartedAt   time.Time
}

// Evidence is one explainable correlation argument.
type Evidence struct {
	Kind  string `json:"kind"`  // l3_identity | l2_name | time_window
	Value string `json:"value"` // human-readable
}

// Decision is the outcome for one candidate pair (alert -> incident anchor).
type Decision struct {
	Merge        bool
	Confidence   string   // high | suggestion | none
	Evidence     []Evidence
}

// Window is how far apart two firings may start and still correlate.
const Window = 10 * time.Minute

// SameTenantL3 decides alert vs anchor (an existing alert with an incident).
// RULES (docs): L3 = cluster+entity_type+uid; time window = starts within
// Window; both required for a high-confidence auto-merge.
func SameTenantL3(alert, anchor Alert) Decision {
	if alert.TenantID != anchor.TenantID {
		return Decision{Confidence: "none"} // cross-tenant NEVER (red line)
	}
	l3 := alert.ClusterID == anchor.ClusterID &&
		alert.EntityType == anchor.EntityType &&
		alert.EntityUID == anchor.EntityUID && alert.EntityUID != ""
	within := alert.StartedAt.Sub(anchor.StartedAt) <= Window &&
		anchor.StartedAt.Sub(alert.StartedAt) <= Window
	ev := []Evidence{}
	if l3 {
		ev = append(ev, Evidence{"l3_identity", alert.ClusterID + "/" + alert.EntityType + "/" + alert.EntityUID})
	}
	if within {
		ev = append(ev, Evidence{"time_window", "starts within " + Window.String()})
	}
	switch {
	case l3 && within:
		return Decision{Merge: true, Confidence: "high", Evidence: ev}
	case l3:
		// same object but far apart in time: two episodes, suggest only —
		// merging would swallow distinct incidents (US-01 traceability).
		return Decision{Confidence: "suggestion", Evidence: ev}
	case sameL2(alert, anchor) && within:
		ev = append(ev, Evidence{"l2_name", alert.DisplayName + "@" + alert.ClusterID})
		return Decision{Confidence: "suggestion", Evidence: ev}
	}
	return Decision{Confidence: "none"}
}

// sameL2 compares stable display names (L2 evidence; never merges alone).
func sameL2(a, b Alert) bool {
	return a.DisplayName != "" && a.DisplayName == b.DisplayName && a.ClusterID == b.ClusterID
}
