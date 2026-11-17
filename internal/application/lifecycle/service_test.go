package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

// P10-004 failure matrix: PG rows are deleted ONLY for a day whose archive
// queue is fully done AND whose ClickHouse unique count matches PG.

type fakePG struct {
	days    []DayStats
	deleted map[string]int64
	calls   []string
}

func (f *fakePG) TransitionDaysBefore(context.Context, time.Time) ([]DayStats, error) {
	return f.days, nil
}
func (f *fakePG) DeleteTransitionsForDay(_ context.Context, day time.Time) (int64, error) {
	if f.deleted == nil {
		f.deleted = map[string]int64{}
	}
	k := day.UTC().Format("2006-01-02")
	f.calls = append(f.calls, k)
	n := f.deleted[k]
	if n == 0 {
		n = 1
	}
	return n, nil
}

type fakeArchive struct {
	counts map[string]int64
	err    error
}

func (f *fakeArchive) CountTransitionsByDay(context.Context, time.Time, time.Time) (map[string]int64, error) {
	return f.counts, f.err
}

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func TestReconcileVerifiedOnly(t *testing.T) {
	pg := &fakePG{days: []DayStats{
		{Day: day(2026, 10, 1), Total: 10, ArchivedDone: 10}, // verified

		{Day: day(2026, 10, 2), Total: 5, ArchivedDone: 4}, // queue incomplete
		{Day: day(2026, 10, 3), Total: 7, ArchivedDone: 7}, // CH short
		{Day: day(2026, 10, 4), Total: 3, ArchivedDone: 3}, // CH outage
	}, deleted: map[string]int64{"2026-10-01": 10}}
	ch := &fakeArchive{counts: map[string]int64{"2026-10-01": 10, "2026-10-02": 4, "2026-10-03": 6},
		err: nil}
	// Make CH fail only for the 4th day: simplest is a per-call error, so run
	// a second service for that case.
	svc := NewService(pg, ch, 30*24*time.Hour)
	svc.now = func() time.Time { return day(2026, 11, 17) }

	rep, err := svc.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Verified != 1 || rep.Unverified != 3 || rep.Deleted != 10 {
		t.Fatalf("report = %+v", rep)
	}
	if len(pg.calls) != 1 || pg.calls[0] != "2026-10-01" {
		t.Fatalf("deleted days = %v, want only the verified day", pg.calls)
	}
	if rep.Days[1].Reason == "" || rep.Days[2].Reason == "" {
		t.Fatalf("unverified days lack reasons: %+v", rep.Days)
	}
}

func TestReconcileCHFailureKeepsRows(t *testing.T) {
	pg := &fakePG{days: []DayStats{{Day: day(2026, 10, 1), Total: 4, ArchivedDone: 4}}}
	svc := NewService(pg, &fakeArchive{err: errors.New("ch down")}, time.Hour)
	svc.now = func() time.Time { return day(2026, 11, 17) }
	rep, err := svc.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Deleted != 0 || len(pg.calls) != 0 || rep.Unverified != 1 {
		t.Fatalf("CH outage deleted data: report=%+v calls=%v", rep, pg.calls)
	}
}

func TestReconcileWithoutArchiveNeverDeletes(t *testing.T) {
	pg := &fakePG{days: []DayStats{{Day: day(2026, 10, 1), Total: 1, ArchivedDone: 1}}}
	svc := NewService(pg, nil, time.Hour)
	rep, err := svc.RunOnce(context.Background())
	if err != nil || rep.Deleted != 0 || len(pg.calls) != 0 {
		t.Fatalf("archive-less reconcile deleted: rep=%+v calls=%v err=%v", rep, pg.calls, err)
	}
}

func TestReconcileEmptyDay(t *testing.T) {
	pg := &fakePG{days: []DayStats{{Day: day(2026, 10, 1)}}}
	svc := NewService(pg, &fakeArchive{counts: map[string]int64{}}, time.Hour)
	svc.now = func() time.Time { return day(2026, 11, 17) }
	rep, _ := svc.RunOnce(context.Background())
	if rep.Deleted != 0 || len(pg.calls) != 0 {
		t.Fatalf("empty day deleted: %+v", rep)
	}
}
