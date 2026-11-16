// Package sseclient metrics (P9-OBS-05): reconnect counter for the SSE
// stream. One low-cardinality counter per process suffices — the URL is a
// deployment constant and must not become a label.
package sseclient

import (
	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var reconnectTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "alert_platform", Subsystem: "edge", Name: "reconnect_total",
	Help: "SSE stream reconnects after a transient disconnect.",
})

func init() {
	if err := oekitmetrics.Registry.Register(reconnectTotal); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			panic(err)
		}
	}
}
