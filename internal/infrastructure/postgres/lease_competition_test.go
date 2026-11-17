package postgres

import (
	"context"
	"testing"
	"time"
)

// P10-008: named single-writer leases are independent and can be taken over
// only after expiry. Covers the outbox/retention/telemetry lease names used by
// the worker profiles.
func TestNamedLeaseCompetition(t *testing.T) {
	st, _ := newStoreTest(t)
	ctx := context.Background()
	a := "aaaaaaaa-0000-0000-0000-000000000001"
	b := "bbbbbbbb-0000-0000-0000-000000000002"

	names := []string{"outbox.relay", "retention.cleanup", "telemetry.sampler"}
	for _, name := range names {
		ok, err := st.AcquireNamedLease(ctx, name, a, time.Minute)
		if err != nil || !ok {
			t.Fatalf("%s first acquire: ok=%v err=%v", name, ok, err)
		}
		// Renewal by the same owner succeeds.
		if ok, err := st.AcquireNamedLease(ctx, name, a, time.Minute); err != nil || !ok {
			t.Fatalf("%s renew: ok=%v err=%v", name, ok, err)
		}
		// A second owner is denied while the lease is live.
		if ok, err := st.AcquireNamedLease(ctx, name, b, time.Minute); err != nil || ok {
			t.Fatalf("%s contended: ok=%v err=%v (want false,nil)", name, ok, err)
		}
	}
	// Leases are independent: releasing one does not release the others.
	if err := st.ReleaseNamedLease(ctx, "outbox.relay", a); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.AcquireNamedLease(ctx, "outbox.relay", b, time.Minute); !ok {
		t.Fatal("released lease not acquirable by the other owner")
	}
	if ok, _ := st.AcquireNamedLease(ctx, "retention.cleanup", b, time.Minute); ok {
		t.Fatal("release leaked across lease names")
	}
	// Expired lease is taken over (crashed replica).
	if _, err := st.pool.Exec(ctx, `UPDATE component_leases SET expires_at=now()-interval '1 minute' WHERE name='telemetry.sampler'`); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.AcquireNamedLease(ctx, "telemetry.sampler", b, time.Minute); err != nil || !ok {
		t.Fatalf("expired lease takeover: ok=%v err=%v", ok, err)
	}
}
