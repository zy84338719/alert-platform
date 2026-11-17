package main

import (
	"fmt"
	"strings"
)

// workerProfile splits the single worker binary into run roles (P10-008).
// Scaling the Kafka processor must NOT multiply the singleton maintenance
// loops (archive/retention/telemetry) — each profile starts only its own
// tasks, and every singleton additionally holds a DB lease.
type workerProfile struct {
	Processor   bool // Kafka consumer group member
	Notifier    bool // relay + notifier + escalation, DB-leased/idempotent
	Maintenance bool // health scheduler + retention + telemetry + archive
}

// parseProfile accepts all|processor|notifier|maintenance. Unknown values are
// a startup error: silently falling back to "all" would reintroduce the
// duplicate-loop bug on a typo.
func parseProfile(raw string) (workerProfile, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "all":
		return workerProfile{Processor: true, Notifier: true, Maintenance: true}, nil
	case "processor":
		return workerProfile{Processor: true}, nil
	case "notifier":
		return workerProfile{Notifier: true}, nil
	case "maintenance":
		return workerProfile{Maintenance: true}, nil
	default:
		return workerProfile{}, fmt.Errorf("unknown WORKER_PROFILE %q (want all|processor|notifier|maintenance)", raw)
	}
}
