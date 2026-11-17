// Package lifecycle implements the PG cold-data lifecycle (P10-004):
// per-day reconciliation between PostgreSQL (authoritative hot store) and the
// ClickHouse transition archive, and ONLY-AFTER-VERIFIED deletion of old PG
// rows.
//
// Safety rule: a day is deleted only when (a) every transition of that day has
// a `done` archive-queue row AND (b) ClickHouse holds at least as many unique
// transition ids as PG for that day. A failed/missing archive write, a pending
// backfill or a CH outage therefore always keeps the PG rows.
package lifecycle

import (
	"context"
	"sync"
	"time"

	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oekit/logx"
)

// DayStats is one UTC day of PG transitions.
type DayStats struct {
	Day time.Time
	// Total is the PG transition count for the day.
	Total int64
	// ArchivedDone is how many of them have a done archive-queue row.
	ArchivedDone int64
}

// PG is the authoritative store port.
type PG interface {
	// TransitionDaysBefore groups transitions strictly older than cutoff by
	// UTC day, with their archived-done counts.
	TransitionDaysBefore(ctx context.Context, cutoff time.Time) ([]DayStats, error)
	// DeleteTransitionsForDay removes one verified UTC day (and its done queue
	// rows) in one transaction.
	DeleteTransitionsForDay(ctx context.Context, day time.Time) (int64, error)
}

// Archive is the ClickHouse archive port.
type Archive interface {
	// CountTransitionsByDay returns unique transition ids per UTC day
	// ("YYYY-MM-DD") in [from, to).
	CountTransitionsByDay(ctx context.Context, from, to time.Time) (map[string]int64, error)
}

// DayReport is the reconciliation outcome for one day.
type DayReport struct {
	Day       string
	PG        int64
	QueueDone int64
	Archived  int64
	Verified  bool
	Deleted   int64
	Reason    string
}

// Report is one reconciliation pass.
type Report struct {
	Days       []DayReport
	Verified   int
	Unverified int
	Deleted    int64
}

var (
	deletedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "lifecycle", Name: "transitions_deleted_total",
		Help: "Verified PG transitions deleted after ClickHouse reconciliation.",
	})
	verifiedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "lifecycle", Name: "days_verified_total",
		Help: "UTC days whose archive was verified and PG rows were eligible for deletion.",
	})
	unverifiedGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "lifecycle", Name: "days_unverified",
		Help: "Old UTC days still retained because archive reconciliation did not pass.",
	})
)

var registerOnce sync.Once

func registerMetrics() {
	registerOnce.Do(func() {
		for _, c := range []prometheus.Collector{deletedTotal, verifiedTotal, unverifiedGauge} {
			if err := oekitmetrics.Registry.Register(c); err != nil {
				if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
					panic(err)
				}
			}
		}
	})
}

// Service runs the daily reconciliation.
type Service struct {
	pg        PG
	archive   Archive // nil = archive not configured: NEVER delete PG rows
	retention time.Duration
	interval  time.Duration
	now       func() time.Time
}

// NewService builds the reconciler. retention is the minimum PG age; the
// caller must keep it >= the history hot window to avoid deleting data that
// queries still expect from PG.
func NewService(pg PG, archive Archive, retention time.Duration) *Service {
	registerMetrics()
	if retention <= 0 {
		retention = 30 * 24 * time.Hour
	}
	return &Service{pg: pg, archive: archive, retention: retention,
		interval: 24 * time.Hour, now: time.Now}
}

// SetTuning overrides the loop cadence (tests/wiring).
func (s *Service) SetTuning(interval time.Duration) {
	if interval > 0 {
		s.interval = interval
	}
}

// Run reconciles until ctx is done.
func (s *Service) Run(ctx context.Context) error {
	logx.CtxInfo(ctx, "lifecycle reconciler started",
		"retention", s.retention.String(), "interval", s.interval.String())
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			rep, err := s.RunOnce(ctx)
			if err != nil {
				logx.CtxError(ctx, "lifecycle reconcile failed; no PG data deleted", logx.ErrKey, err)
				continue
			}
			logx.CtxInfo(ctx, "lifecycle reconcile",
				"verified_days", rep.Verified, "unverified_days", rep.Unverified,
				"deleted", rep.Deleted)
		}
	}
}

// RunOnce reconciles every old day and deletes only verified ones.
func (s *Service) RunOnce(ctx context.Context) (Report, error) {
	var rep Report
	if s.archive == nil {
		// No archive backend: keep everything (the matrix says "verified
		// archive first"; without one there is no proof).
		return rep, nil
	}
	cutoff := s.now().Add(-s.retention)
	days, err := s.pg.TransitionDaysBefore(ctx, cutoff)
	if err != nil {
		return rep, err
	}
	for _, d := range days {
		dr := DayReport{Day: d.Day.UTC().Format("2006-01-02"), PG: d.Total, QueueDone: d.ArchivedDone}
		from := d.Day.UTC().Truncate(24 * time.Hour)
		counts, cerr := s.archive.CountTransitionsByDay(ctx, from, from.Add(24*time.Hour))
		if cerr != nil {
			// CH unavailable: keep the day, report it, continue with others.
			dr.Reason = "archive count failed: " + cerr.Error()
			rep.Days = append(rep.Days, dr)
			rep.Unverified++
			continue
		}
		dr.Archived = counts[dr.Day]
		switch {
		case d.Total == 0:
			dr.Reason = "empty day"
		case d.ArchivedDone < d.Total:
			dr.Reason = "archive queue not fully done"
		case dr.Archived < d.Total:
			dr.Reason = "clickhouse count below PG count"
		default:
			dr.Verified = true
			n, derr := s.pg.DeleteTransitionsForDay(ctx, from)
			if derr != nil {
				dr.Reason = "delete failed: " + derr.Error()
			} else {
				dr.Deleted = n
				rep.Deleted += n
				deletedTotal.Add(float64(n))
				verifiedTotal.Inc()
				rep.Verified++
			}
		}
		if !dr.Verified {
			rep.Unverified++
		}
		rep.Days = append(rep.Days, dr)
	}
	unverifiedGauge.Set(float64(rep.Unverified))
	return rep, nil
}
