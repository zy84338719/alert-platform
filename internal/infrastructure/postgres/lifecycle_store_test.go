package postgres

import (
	"context"
	"testing"
	"time"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// P10-004: day bucketing + deletion operate on UTC days and remove only the
// requested day's transitions (queue rows included).
func TestTransitionDaysAndDelete(t *testing.T) {
	store, pool := newStoreTest(t)
	ctx := context.Background()
	d1 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

	mustApply(t, store, ctx, canonical("lc-1", d1))
	mustApply(t, store, ctx, func() *eventv1.AlertEvent {
		ev := canonical("lc-2", d1.Add(time.Hour))
		ev.Source.ExternalRuleId = "rule-2"
		return ev
	}())
	mustApply(t, store, ctx, func() *eventv1.AlertEvent {
		ev := canonical("lc-3", d2)
		ev.Source.ExternalRuleId = "rule-3"
		return ev
	}())
	// Archive queue rows exist (pending). Mark the first day done.
	if _, err := pool.Exec(ctx, `
		UPDATE transition_archive_queue SET status='done'
		WHERE transition_at >= $1 AND transition_at < $1 + interval '1 day'`, d1.Truncate(24*time.Hour)); err != nil {
		t.Fatal(err)
	}

	days, err := store.TransitionDaysBefore(ctx, d2.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(days) != 2 {
		t.Fatalf("days = %+v", days)
	}
	if days[0].Day.UTC().Format("2006-01-02") != "2026-10-01" || days[0].Total != 2 || days[0].ArchivedDone != 2 {
		t.Fatalf("day1 = %+v", days[0])
	}
	if days[1].ArchivedDone != 0 {
		t.Fatalf("day2 queueDone = %d, want 0 (still pending)", days[1].ArchivedDone)
	}

	n, err := store.DeleteTransitionsForDay(ctx, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || n != 2 {
		t.Fatalf("delete: n=%d err=%v", n, err)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM alert_transitions`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 1 {
		t.Fatalf("transitions left = %d, want 1 (day 2 untouched)", left)
	}
	var queueLeft int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM transition_archive_queue`).Scan(&queueLeft); err != nil {
		t.Fatal(err)
	}
	if queueLeft != 1 {
		t.Fatalf("queue rows left = %d, want 1 (done rows of day1 deleted)", queueLeft)
	}
}
