package history

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"
)

type fakeStore struct {
	rows  []Transition
	calls int
	lastF time.Time
	lastT time.Time
	err   error
}

func (f *fakeStore) ListTransitionsRange(_ context.Context, _ string, from, to time.Time, limit int) ([]Transition, error) {
	f.calls++
	f.lastF, f.lastT = from, to
	if f.err != nil {
		return nil, f.err
	}
	var out []Transition
	for _, r := range f.rows {
		if !r.At.Before(from) && r.At.Before(to) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.After(out[j].At) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func trAt(id string, at time.Time) Transition {
	return Transition{TransitionID: id, ToState: "firing", At: at}
}

// P10-003：冷热边界相对 NOW（不是用户的 to）—— 查"很久以前的短窗口"必须走 CH。
func TestOldShortWindowRoutesToArchive(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	pg := &fakeStore{}
	from := now.Add(-48 * 24 * time.Hour)
	to := from.Add(24 * time.Hour) // a 1-day window, long outside the PG window
	ch := &fakeStore{rows: []Transition{trAt("cold-1", from.Add(time.Hour))}}
	svc := NewService(pg, ch, 168*time.Hour)
	svc.SetClock(func() time.Time { return now })
	res, err := svc.Query(context.Background(), "t", from, to, 10)
	if err != nil {
		t.Fatal(err)
	}
	if pg.calls != 0 {
		t.Fatalf("PG called %d times for a fully-cold window", pg.calls)
	}
	if !res.Archived || res.Coverage != CoverageFull || len(res.Transitions) != 1 {
		t.Fatalf("res = %+v, want archived CH result", res)
	}
}

// 跨边界：PG 热区 + CH 冷区合并、按时间倒序、transition_id 去重。
func TestCrossBoundaryMergeAndDedup(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	hotStart := now.Add(-168 * time.Hour)
	dup := trAt("dup-1", hotStart.Add(-time.Hour)) // exists in both (mid-archival)
	pg := &fakeStore{rows: []Transition{
		trAt("hot-new", now.Add(-time.Hour)),
		trAt("hot-old", hotStart.Add(time.Hour)),
		trAt("dup-1", hotStart.Add(-time.Hour)), // same id in PG and CH
	}}
	ch := &fakeStore{rows: []Transition{
		dup,
		trAt("cold-1", now.Add(-200*time.Hour)),
	}}
	svc := NewService(pg, ch, 168*time.Hour)
	svc.SetClock(func() time.Time { return now })

	res, err := svc.Query(context.Background(), "t", now.Add(-300*time.Hour), now, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Archived || res.Coverage != CoverageFull {
		t.Fatalf("coverage = %+v", res)
	}
	ids := map[string]int{}
	for _, r := range res.Transitions {
		ids[r.TransitionID]++
	}
	if ids["dup-1"] != 1 {
		t.Fatalf("dup-1 appeared %d times (dedup by transition_id)", ids["dup-1"])
	}
	// Newest-first ordering across the merge.
	for i := 1; i < len(res.Transitions); i++ {
		if res.Transitions[i].At.After(res.Transitions[i-1].At) {
			t.Fatalf("merge not sorted newest-first: %+v", res.Transitions)
		}
	}
	// Cold query upper bound is the hot boundary, not `to`.
	if !ch.lastT.Equal(hotStart) {
		t.Fatalf("archive upper bound = %s, want hot boundary %s", ch.lastT, hotStart)
	}
}

// 无归档：只能回答热区，必须显式告知 coverage（archived=false ≠ 完整）。
func TestNoArchiveReportsPGWindowOnly(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	pg := &fakeStore{rows: []Transition{trAt("hot", now.Add(-time.Hour))}}
	svc := NewService(pg, nil, 168*time.Hour)
	svc.SetClock(func() time.Time { return now })

	res, err := svc.Query(context.Background(), "t", now.Add(-30*24*time.Hour), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if res.Archived {
		t.Fatal("archived must be false without an archive")
	}
	if res.Coverage != CoveragePGWindow {
		t.Fatalf("coverage = %q, want %q", res.Coverage, CoveragePGWindow)
	}
	if len(res.Transitions) != 1 {
		t.Fatalf("transitions = %d, want the hot row", len(res.Transitions))
	}
}

// 截断：limit 用满时给出 truncated + next_from 续查锚点。
func TestTruncatedProvidesCursor(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	pg := &fakeStore{rows: []Transition{
		trAt("a", now.Add(-time.Hour)),
		trAt("b", now.Add(-2*time.Hour)),
		trAt("c", now.Add(-3*time.Hour)),
	}}
	svc := NewService(pg, nil, 168*time.Hour)
	svc.SetClock(func() time.Time { return now })

	res, err := svc.Query(context.Background(), "t", now.Add(-4*time.Hour), now, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || res.NextFrom == nil || !res.NextFrom.Equal(now.Add(-2*time.Hour)) {
		t.Fatalf("truncation = %+v, want next_from at the oldest returned row", res)
	}
}

func TestArchiveErrorPropagates(t *testing.T) {
	now := time.Now()
	pg := &fakeStore{}
	ch := &fakeStore{err: errors.New("ch down")}
	svc := NewService(pg, ch, time.Hour)
	svc.SetClock(func() time.Time { return now })
	if _, err := svc.Query(context.Background(), "t", now.Add(-48*time.Hour), now.Add(-24*time.Hour), 10); err == nil {
		t.Fatal("archive error must surface")
	}
}
