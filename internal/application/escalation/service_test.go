package escalation

import (
	"context"
	"testing"
	"time"
)

type fakeStore struct {
	allow    bool
	acquires int
	scans    int
	cands    []EscalationCandidate
}

func (f *fakeStore) EscalatableIncidents(context.Context, time.Duration) ([]EscalationCandidate, error) {
	f.scans++
	return f.cands, nil
}
func (f *fakeStore) RecordEscalation(context.Context, EscalationCandidate, int64) error {
	return nil
}
func (f *fakeStore) AcquireLease(context.Context, string, time.Duration) (bool, error) {
	f.acquires++
	return f.allow, nil
}
func (f *fakeStore) ReleaseLease(context.Context, string) error { return nil }

// AUD-008：多副本单飞 —— 未持有租约的副本不得执行 escalation/correlation 扫描。
func TestSingleFlightGate(t *testing.T) {
	run := func(allow bool) *fakeStore {
		fs := &fakeStore{allow: allow}
		svc := NewService(fs, nil)
		svc.SetOwner("11111111-1111-1111-1111-111111111111")
		svc.SetTuning(time.Minute, 5*time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()
		if err := svc.Run(ctx); err != nil {
			t.Fatal(err)
		}
		return fs
	}

	denied := run(false)
	if denied.acquires == 0 {
		t.Fatal("lease was never attempted")
	}
	if denied.scans != 0 {
		t.Fatalf("non-owner scanned %d times (single-flight broken)", denied.scans)
	}

	owner := run(true)
	if owner.scans == 0 {
		t.Fatal("lease owner never scanned")
	}
}
