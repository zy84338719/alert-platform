package retention

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type countingStore struct{ runs atomic.Int64 }

func (c *countingStore) CleanupReceipts(context.Context, time.Time) (int64, int64, error) {
	c.runs.Add(1)
	return 0, 0, nil
}

type deniedLeaser struct{}

func (deniedLeaser) AcquireNamedLease(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}
func (deniedLeaser) ReleaseNamedLease(context.Context, string, string) error { return nil }

type grantedLeaser struct{ acq, rel atomic.Int64 }

func (g *grantedLeaser) AcquireNamedLease(context.Context, string, string, time.Duration) (bool, error) {
	g.acq.Add(1)
	return true, nil
}
func (g *grantedLeaser) ReleaseNamedLease(context.Context, string, string) error {
	g.rel.Add(1)
	return nil
}

// P10-008: a replica that does not own the lease must not run cleanups.
func TestRetentionLeaseGating(t *testing.T) {
	store := &countingStore{}
	svc := NewService(store, time.Hour, 10*time.Millisecond, prometheus.NewRegistry())
	svc.SetLeaser(deniedLeaser{})
	svc.SetOwner("00000000-0000-0000-0000-000000000001")
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = svc.Run(ctx)
	if got := store.runs.Load(); got != 0 {
		t.Fatalf("non-owner ran %d cleanups", got)
	}

	g := &grantedLeaser{}
	store2 := &countingStore{}
	svc2 := NewService(store2, time.Hour, 10*time.Millisecond, prometheus.NewRegistry())
	svc2.SetLeaser(g)
	svc2.SetOwner("00000000-0000-0000-0000-000000000002")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel2()
	_ = svc2.Run(ctx2)
	if store2.runs.Load() == 0 {
		t.Fatal("lease owner never ran cleanup")
	}
	if g.acq.Load() == 0 || g.rel.Load() == 0 {
		t.Fatalf("lease lifecycle incomplete: acq=%d rel=%d", g.acq.Load(), g.rel.Load())
	}
}
