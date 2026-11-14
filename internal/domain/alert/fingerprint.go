package alert

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

// Identity keeps exact source deduplication separate from cross-source correlation.
type Identity struct {
	TenantID   string            `json:"tenant_id"`
	SourceID   string            `json:"source_id"`
	RuleID     string            `json:"rule_id"`
	ClusterID  string            `json:"cluster_id"`
	EntityType string            `json:"entity_type"`
	EntityUID  string            `json:"entity_uid"`
	Dimensions map[string]string `json:"dimensions"`
}

// ErrIncompleteIdentity marks events whose identity fields cannot form a
// fingerprint (rule/cluster/entity missing). Consumers quarantine such events
// into the DLQ — retrying can never fix the payload.
var ErrIncompleteIdentity = errors.New("incomplete alert identity")

// fingerprintVersion prefixes the hashed payload so that schema evolution
// (e.g. adding an Identity field) can bump to v2 without silently re-keying
// every in-flight active alert, which would strand them unresolvable.
const fingerprintVersion = "alert-fingerprint/v1\n"

func Fingerprint(i Identity) (string, error) {
	if i.TenantID == "" || i.SourceID == "" || i.RuleID == "" || i.ClusterID == "" || i.EntityUID == "" {
		return "", errors.New("incomplete alert identity")
	}
	// JSON object keys have deterministic ordering in encoding/json.
	raw, err := json.Marshal(i)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(fingerprintVersion + string(raw)))
	return hex.EncodeToString(sum[:]), nil
}
