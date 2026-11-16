// Package collector metrics (P9-OBS-05): SSE gap markers surfaced as a
// counter — every gap means events were lost while disconnected (HT-004).
package collector

import (
	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var sseGapTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "alert_platform", Subsystem: "edge", Name: "sse_gap_total",
	Help: "SSE gap markers observed (events lost while disconnected).",
})

func init() {
	if err := oekitmetrics.Registry.Register(sseGapTotal); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			panic(err)
		}
	}
}
