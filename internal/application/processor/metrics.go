// Package processor metrics (P9-OBS-03): event outcomes and transaction
// latency. Low-cardinality labels only (outcome strings).
package processor

import (
	"sync"
	"time"

	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	eventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "processor", Name: "events_total",
		Help: "Processed canonical events by outcome.",
	}, []string{"outcome"})
	durationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "alert_platform", Subsystem: "processor", Name: "duration_seconds",
		Help:    "ApplyEvent transaction latency (PG commit included).",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	})
)

// registerOnce: the kafka package (consumer) imports processor, and alert-api
// imports kafka for its producer — the API binary must not expose idle
// processor metrics. Registration happens when the worker constructs the
// processor service.
var registerOnce sync.Once

func registerMetrics() {
	registerOnce.Do(func() {
		for _, c := range []prometheus.Collector{eventsTotal, durationSeconds} {
			if err := oekitmetrics.Registry.Register(c); err != nil {
				if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
					panic(err)
				}
			}
		}
	})
}

func observeOutcome(outcome string) { eventsTotal.WithLabelValues(outcome).Inc() }

func observeDuration(d time.Duration) { durationSeconds.Observe(d.Seconds()) }
