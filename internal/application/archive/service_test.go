package archive

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"
)

// fakeStore models the PG queue + lease in memory.
type fakeStore struct {
	mu      sync.Mutex
	rows    map[string]*fakeRow
	lease   string
	expires time.Time
	now     func() time.Time
}

type fakeRow struct {
	Transition
	status   string
	attempts int
}

func newFakeStore(rows ...Transition) *fakeStore {
	f := &fakeStore{rows: map[string]*fakeRow{}, now: time.Now}
	for _, r := range rows {
		f.rows[r.TransitionID] = &fakeRow{Transition: r, status: "pending"}
	}
	return f
}

func (f *fakeStore) AcquireLease(_ context.Context, owner string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lease != "" && f.lease != owner && f.now().Before(f.expires) {
		return false, nil
	}
	f.lease = owner
	f.expires = f.now().Add(ttl)
	return true, nil
}

func (f *fakeStore) ReleaseLease(_ context.Context, owner string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lease == owner {
		f.lease = ""
	}
	return nil
}

func (f *fakeStore) ClaimTransitions(_ context.Context, limit int, _ time.Duration, maxAttempts int) ([]Transition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var eligible []*fakeRow
	for _, r := range f.rows {
		if (r.status == "pending" || r.status == "inflight") && r.attempts < maxAttempts {
			eligible = append(eligible, r)
		}
	}
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].At.Before(eligible[j].At) })
	if len(eligible) > limit {
		eligible = eligible[:limit]
	}
	out := make([]Transition, 0, len(eligible))
	for _, r := range eligible {
		r.status = "inflight"
		r.attempts++
		out = append(out, r.Transition)
	}
	return out, nil
}

func (f *fakeStore) MarkArchived(_ context.Context, ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		if r, ok := f.rows[id]; ok {
			r.status = "done"
		}
	}
	return nil
}

func (f *fakeStore) MarkFailed(_ context.Context, ids []string, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		if r, ok := f.rows[id]; ok {
			r.status = "failed"
		}
	}
	return nil
}

func (f *fakeStore) MarkRetry(_ context.Context, ids []string, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		if r, ok := f.rows[id]; ok {
			r.status = "pending"
		}
	}
	return nil
}

func (f *fakeStore) MarkExhausted(_ context.Context, maxAttempts int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for _, r := range f.rows {
		if (r.status == "pending" || r.status == "inflight") && r.attempts >= maxAttempts {
			r.status = "failed"
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) QueueStats(context.Context) (int64, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	var oldest time.Time
	for _, r := range f.rows {
		if r.status == "pending" || r.status == "inflight" {
			n++
			if oldest.IsZero() || r.At.Before(oldest) {
				oldest = r.At
			}
		}
	}
	return n, oldest, nil
}

func (f *fakeStore) statuses() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	for id, r := range f.rows {
		out[id] = r.status
	}
	return out
}

type fakeWriter struct {
	mu      sync.Mutex
	batches int
	rows    int
	err     error
}

func (w *fakeWriter) InsertTransitions(_ context.Context, rows []Transition) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.batches++
	w.rows += len(rows)
	return nil
}

func tr(id string, at time.Time) Transition {
	return Transition{TenantID: "t", TransitionID: id, InstanceID: "i", ToState: "firing", At: at}
}

// permanentErr mimics clickhouse.PermanentError via the IsPermanent() hook.
type permanentErr struct{}

func (permanentErr) Error() string     { return "clickhouse permanent: bad data" }
func (permanentErr) IsPermanent() bool { return true }

// P10-001：归档按队列工作 —— 包括"迟到/乱序"行（事件时间早于已归档），
// 因为它们是在插入时入队的，不再被事件时间水位漏掉。
func TestRunOnceArchivesLateRows(t *testing.T) {
	base := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	store := newFakeStore(tr("00000000-0000-0000-0000-000000000001", base.Add(time.Hour)))
	writer := &fakeWriter{}
	svc := NewService(store, writer, "owner-a")
	svc.SetTuning(time.Second, 10, 10, 10)

	if _, err := svc.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Late arrival: an event that happened two hours BEFORE the archived one.
	store.mu.Lock()
	store.rows["00000000-0000-0000-0000-000000000002"] = &fakeRow{
		Transition: tr("00000000-0000-0000-0000-000000000002", base.Add(-2*time.Hour)), status: "pending"}
	store.mu.Unlock()
	if _, err := svc.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := store.statuses()
	for _, id := range []string{"00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"} {
		if got[id] != "done" {
			t.Fatalf("%s status = %s, want done (late rows must archive)", id, got[id])
		}
	}
	if writer.rows != 2 {
		t.Fatalf("written rows = %d, want 2", writer.rows)
	}
}

// P10-002：单写者租约 —— 非 owner 副本不归档。
func TestSingleWriterLease(t *testing.T) {
	store := newFakeStore(tr("00000000-0000-0000-0000-000000000003", time.Now()))
	a := NewService(store, &fakeWriter{}, "owner-a")
	bWriter := &fakeWriter{}
	_ = NewService(store, bWriter, "owner-b")
	if ok, _ := store.AcquireLease(context.Background(), "owner-a", time.Minute); !ok {
		t.Fatal("owner-a should acquire")
	}
	if ok, _ := store.AcquireLease(context.Background(), "owner-b", time.Minute); ok {
		t.Fatal("owner-b must NOT acquire while owner-a lease is live")
	}
	if _, err := a.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if bWriter.rows != 0 {
		t.Fatalf("non-owner archived %d rows", bWriter.rows)
	}
}

// AUD-001：CH 永久拒绝 → 死信（不无限重试）；瞬时错误 → 回 pending。
func TestPermanentFailIsDeadLetteredNotRetried(t *testing.T) {
	store := newFakeStore(tr("00000000-0000-0000-0000-000000000004", time.Now()))
	writer := &fakeWriter{err: permanentErr{}}
	svc := NewService(store, writer, "o")
	svc.SetTuning(time.Second, 10, 10, 10)
	if _, err := svc.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := store.statuses()["00000000-0000-0000-0000-000000000004"]; st != "failed" {
		t.Fatalf("permanent rejection status = %s, want failed (dead-letter)", st)
	}

	store2 := newFakeStore(tr("00000000-0000-0000-0000-000000000005", time.Now()))
	writer2 := &fakeWriter{err: errors.New("ch down")}
	svc2 := NewService(store2, writer2, "o")
	svc2.SetTuning(time.Second, 10, 10, 10)
	if _, err := svc2.RunOnce(context.Background()); err == nil {
		t.Fatal("transient error must surface")
	}
	if st := store2.statuses()["00000000-0000-0000-0000-000000000005"]; st != "pending" {
		t.Fatalf("transient status = %s, want pending (retry)", st)
	}
}

// 重试上限：超过 maxAttempts 的行进入 failed，不会永久停留在 inflight。
func TestMaxAttemptsDeadLetters(t *testing.T) {
	store := newFakeStore(tr("00000000-0000-0000-0000-000000000006", time.Now()))
	writer := &fakeWriter{err: errors.New("ch down")}
	svc := NewService(store, writer, "o")
	svc.SetTuning(time.Second, 10, 10, 3)
	for i := 0; i < 3; i++ {
		_, _ = svc.RunOnce(context.Background())
	}
	if st := store.statuses()["00000000-0000-0000-0000-000000000006"]; st != "pending" {
		t.Fatalf("status = %s, want pending before exhaustion", st)
	}
	_, _ = svc.RunOnce(context.Background()) // MarkExhausted pass
	if st := store.statuses()["00000000-0000-0000-0000-000000000006"]; st != "failed" {
		t.Fatalf("status = %s, want failed after max attempts", st)
	}
}