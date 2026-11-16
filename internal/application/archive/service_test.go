package archive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeStore struct {
	rows     []Transition
	cursor   int
	advanced []string
}

func (f *fakeStore) NextTransitions(_ context.Context, limit int) ([]Transition, error) {
	remaining := f.rows[f.cursor:]
	if len(remaining) > limit {
		remaining = remaining[:limit]
	}
	return remaining, nil
}

func (f *fakeStore) AdvanceCursor(_ context.Context, _ time.Time, id string) error {
	f.advanced = append(f.advanced, id)
	for i, r := range f.rows {
		if r.TransitionID == id {
			f.cursor = i + 1
			return nil
		}
	}
	return errors.New("unknown cursor id")
}

type fakeWriter struct {
	batches int
	rows    int
	err     error
}

func (f *fakeWriter) InsertTransitions(_ context.Context, rows []Transition) error {
	if f.err != nil {
		return f.err
	}
	f.batches++
	f.rows += len(rows)
	return nil
}

func tr(id string, at time.Time) Transition {
	return Transition{TenantID: "t", TransitionID: id, InstanceID: "i", At: at}
}

// P9-CH-01：按游标分批归档；写入成功才推进；CH 失败时不推进游标。
func TestRunOnceBatchesAndCursor(t *testing.T) {
	base := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	store := &fakeStore{rows: []Transition{
		tr("11111111-1111-1111-1111-111111111111", base),
		tr("22222222-2222-2222-2222-222222222222", base.Add(time.Second)),
		tr("33333333-3333-3333-3333-333333333333", base.Add(2*time.Second)),
	}}
	writer := &fakeWriter{}
	svc := NewService(store, writer)
	svc.SetTuning(time.Second, 2, 10)
	svc.now = func() time.Time { return base.Add(time.Minute) }

	before := testutil.ToFloat64(writtenTotal)
	n, err := svc.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || writer.rows != 3 || writer.batches != 2 {
		t.Fatalf("archived n=%d writer rows=%d batches=%d, want 3/3/2", n, writer.rows, writer.batches)
	}
	if len(store.advanced) != 2 {
		t.Fatalf("cursor advances = %d, want 2", len(store.advanced))
	}
	if got := testutil.ToFloat64(writtenTotal) - before; got != 3 {
		t.Fatalf("written_total delta = %v, want 3", got)
	}

	// idempotent when no rows remain
	n, err = svc.RunOnce(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("empty run n=%d err=%v", n, err)
	}
}

func TestWriterFailureDoesNotAdvance(t *testing.T) {
	store := &fakeStore{rows: []Transition{
		tr("11111111-1111-1111-1111-111111111111", time.Now()),
	}}
	writer := &fakeWriter{err: errors.New("ch down")}
	svc := NewService(store, writer)
	svc.SetTuning(time.Second, 10, 10)

	before := testutil.ToFloat64(errorTotal)
	if _, err := svc.RunOnce(context.Background()); err == nil {
		t.Fatal("writer error must surface")
	}
	if len(store.advanced) != 0 {
		t.Fatalf("cursor advanced despite CH failure: %v", store.advanced)
	}
	_ = before // errorTotal increments in Run, not RunOnce
}
