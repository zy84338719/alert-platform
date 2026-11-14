// Package domain groups the DOMAIN layer subpackages (docs/02 §3): the pure
// business rules of the platform — alert state machine, incidents, routing
// computation.
//
// Dependency rule: domain code imports NOTHING internal and the Go standard
// library only. Hertz/OEKit/Kafka/PG types must never appear here; enforced
// by scripts/check_deps.py (BASE-001).
package domain
