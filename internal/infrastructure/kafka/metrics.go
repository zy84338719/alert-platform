// Package kafka consumer metrics (P9-OBS-03): consumer lag and DLQ
// quarantines. topic/group are low-cardinality deployment constants.
package kafka

import (
	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	consumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "consumer", Name: "lag",
		Help: "Consumer group lag reported by the Kafka client.",
	}, []string{"topic", "group"})
	dlqQuarantined = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "dlq", Name: "quarantined_total",
		Help: "Poison messages quarantined into the DLQ.",
	}, []string{"topic"})
)

func init() {
	for _, c := range []prometheus.Collector{consumerLag, dlqQuarantined} {
		if err := oekitmetrics.Registry.Register(c); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
				panic(err)
			}
		}
	}
}
