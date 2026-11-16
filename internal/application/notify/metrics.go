// Package notify metrics (P9-OBS-04): delivery attempts, queue depth, lease
// renewals and the persisted authority generation that gates delivery.
package notify

import (
	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	attempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "notify", Name: "attempt_total",
		Help: "Notification delivery attempts by channel type and result.",
	}, []string{"channel_type", "result"})
	pendingGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "notify", Name: "pending",
		Help: "Pending/retry notification tasks in the current authority generation.",
	})
	leaseRenewFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "notify", Name: "lease_renew_failures_total",
		Help: "Pre-send lease renewals that failed (stale ownership or DB error).",
	}, []string{"reason"})
	authorityGeneration = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "alert_platform", Subsystem: "notify", Name: "authority_generation",
		Help: "Current persisted notification-authority generation.",
	})
)

func init() {
	for _, c := range []prometheus.Collector{attempts, pendingGauge, leaseRenewFailures, authorityGeneration} {
		if err := oekitmetrics.Registry.Register(c); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
				panic(err)
			}
		}
	}
}

// attemptResult maps a SendResult onto the low-cardinality result label.
func attemptResult(res SendResult) string {
	if res.Sent {
		return "sent"
	}
	if res.FailureCode != "" {
		return res.FailureCode
	}
	return "unknown"
}
