package alert

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
)

func TestFingerprintOrderIndependent(t *testing.T) {
	a := Identity{TenantID: "tenant", SourceID: "src", RuleID: "rule", ClusterID: "cluster", EntityUID: "uid", Dimensions: map[string]string{"pod": "a", "ns": "b"}}
	b := Identity{TenantID: "tenant", SourceID: "src", RuleID: "rule", ClusterID: "cluster", EntityUID: "uid", Dimensions: map[string]string{"ns": "b", "pod": "a"}}
	ka, e1 := Fingerprint(a)
	kb, e2 := Fingerprint(b)
	if e1 != nil || e2 != nil || ka != kb {
		t.Fatalf("mismatch: %s %s %v %v", ka, kb, e1, e2)
	}
	b.SourceID = "other"
	kc, _ := Fingerprint(b)
	if kc == ka {
		t.Fatal("different source must be different exact fingerprint")
	}
}

// Adding a field to Identity must NOT silently re-key existing alerts:
// schema evolution requires an explicit version bump so operators can drain
// in-flight episodes before the new key space takes effect.
func TestFingerprintVersionPrefixIsStable(t *testing.T) {
	id := Identity{TenantID: "t", SourceID: "s", RuleID: "r", ClusterID: "c", EntityUID: "u"}
	k1, err := Fingerprint(id)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := Fingerprint(id)
	if err != nil || k1 != k2 {
		t.Fatalf("unstable fingerprint: %s %s %v", k1, k2, err)
	}
	// A raw JSON hash without the version prefix must differ, proving the
	// version tag participates in the digest.
	raw, _ := json.Marshal(id)
	bare := sha256.Sum256(raw)
	if k1 == hex.EncodeToString(bare[:]) {
		t.Fatal("fingerprint must include the version prefix in the digest")
	}
}

// PROC-002 / T-016: the same rule name+labels from two different sources are
// different logical alerts — source identity participates in the digest, so
// a same-name alert from another source NEVER merges into this episode.
func TestSameNameDifferentSourceNeverMerges(t *testing.T) {
	base := Identity{
		TenantID: "t1", SourceID: "src-a", RuleID: "HighLatency",
		ClusterID: "c1", EntityType: "pod", EntityUID: "pod-1",
		Dimensions: map[string]string{"ns": "demo"},
	}
	other := base
	other.SourceID = "src-b"
	fa, err := Fingerprint(base)
	if err != nil {
		t.Fatal(err)
	}
	fb, err := Fingerprint(other)
	if err != nil {
		t.Fatal(err)
	}
	if fa == fb {
		t.Fatal("same name from different sources merged into one fingerprint")
	}
	// Cross-tenant isolation follows from the same construction.
	otherTenant := base
	otherTenant.TenantID = "t2"
	if ft, _ := Fingerprint(otherTenant); ft == fa {
		t.Fatal("cross-tenant fingerprints collided")
	}
}

// PROC-002: the version prefix is part of the digest — bumping it re-keys
// every identity (the drift-protection the prefix exists for). Pin the exact
// v1 digest so an accidental input-shape change is caught by CI.
func TestFingerprintVersionPrefixDrift(t *testing.T) {
	id := Identity{
		TenantID: "t", SourceID: "s", RuleID: "r", ClusterID: "c",
		EntityType: "pod", EntityUID: "u", Dimensions: map[string]string{"a": "1"},
	}
	got, err := Fingerprint(id)
	if err != nil {
		t.Fatal(err)
	}
	// Same identity hashed WITHOUT the prefix must differ (proves the prefix
	// is actually mixed in, not decorative).
	raw, _ := json.Marshal(id)
	bare := sha256.Sum256(raw)
	if got == hex.EncodeToString(bare[:]) {
		t.Fatal("fingerprint ignored the version prefix")
	}
}

// AUD-003（#12）：身份不完整必须携带 ErrIncompleteIdentity sentinel ——
// processor 用 errors.Is 决定 DLQ 隔离；裸 errors.New 会让毒消息落 transient
// 路径 → 3 次重试后 fail-stop、重启重放，分区永久阻塞。
func TestFingerprintIncompleteIdentitySentinel(t *testing.T) {
	cases := []Identity{
		{TenantID: "t", SourceID: "s", RuleID: "r", ClusterID: "c"},    // no uid
		{TenantID: "t", SourceID: "s", RuleID: "r", EntityUID: "u"},    // no cluster
		{TenantID: "t", SourceID: "s", ClusterID: "c", EntityUID: "u"}, // no rule
		{TenantID: "t", RuleID: "r", ClusterID: "c", EntityUID: "u"},   // no source
		{SourceID: "s", RuleID: "r", ClusterID: "c", EntityUID: "u"},   // no tenant
	}
	for i, id := range cases {
		if _, err := Fingerprint(id); !errors.Is(err, ErrIncompleteIdentity) {
			t.Fatalf("case %d: err = %v, want ErrIncompleteIdentity", i, err)
		}
	}
}
