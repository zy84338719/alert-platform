// Package ingest metrics (P9-OBS-02). Metric names follow the platform
// convention alert_platform_<area>_<metric>; labels are low-cardinality only
// (reason strings), never event/tenant/source identifiers.
package ingest

import (
	"time"

	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	eventsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "ingest", Name: "events_total",
		Help: "Canonical events durably enqueued by the ingest API.",
	})
	rejectedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "ingest", Name: "rejected_total",
		Help: "Rejected ingest requests by reason (auth, validation, publish).",
	}, []string{"reason"})
	batchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "alert_platform", Subsystem: "ingest", Name: "batch_duration_seconds",
		Help:    "PushBatch handling latency including broker ack.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})
)

func init() {
	registerMetrics(eventsTotal, rejectedTotal, batchDuration)
}

// registerMetrics tolerates AlreadyRegistered (multiple test binaries import
// the package but each process registers once; keeps panics for real errors).
func registerMetrics(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := oekitmetrics.Registry.Register(c); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
				panic(err)
			}
		}
	}
}

// RecordAccepted counts successfully enqueued events.
func RecordAccepted(n int) { eventsTotal.Add(float64(n)) }

// RecordRejected counts one rejected request. reason ∈ unauthenticated |
// disabled | registry | invalid | forbidden | publish | unavailable.
func RecordRejected(reason string) { rejectedTotal.WithLabelValues(reason).Inc() }

// ObserveBatch records PushBatch latency.
func ObserveBatch(d time.Duration) { batchDuration.Observe(d.Seconds()) }
