package edge

import (
	"sync"

	oekitmetrics "github.com/oekit/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// Edge-side rejection metrics (AUD-016). Both events were previously
// log-only: an operator could not alert on a disk-full edge or on a poison
// batch pinning the WAL head.
var (
	walRejectedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "edge", Name: "wal_rejected_total",
		Help: "Vendor batches refused because the edge WAL could not accept them (disk full / io error).",
	})
	uploadPoisonTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alert_platform", Subsystem: "edge", Name: "upload_poison_total",
		Help: "Batches permanently rejected by the center (4xx), kept in the WAL for operator replay.",
	}, []string{"status"})
)

var registerOnce sync.Once

func registerMetrics() {
	registerOnce.Do(func() {
		for _, c := range []prometheus.Collector{walRejectedTotal, uploadPoisonTotal} {
			if err := oekitmetrics.Registry.Register(c); err != nil {
				if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
					panic(err)
				}
			}
		}
	})
}

// RecordWALRejected counts one refused vendor batch (EDGE-001 self-alert).
func RecordWALRejected() {
	registerMetrics()
	walRejectedTotal.Inc()
}

// RecordPoisonBatch counts one center-side 4xx rejection. The record stays in
// the WAL (never silently dropped) but no longer surprises an operator who
// only watches logs (AUD-007/AUD-016).
func RecordPoisonBatch(status int) {
	registerMetrics()
	uploadPoisonTotal.WithLabelValues(httpStatusClass(status)).Inc()
}

func httpStatusClass(status int) string {
	switch {
	case status >= 400 && status < 500:
		return "4xx"
	case status >= 500:
		return "5xx"
	default:
		return "other"
	}
}
