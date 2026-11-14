// Package infrastructure groups the INFRASTRUCTURE layer subpackages
// (docs/02 §3): Kafka, PostgreSQL, HTTP server, and observability
// implementations. OEKit/Hertz usage lives here (bootstrap/logx/tracing/
// metrics), never in domain or application.
//
// Dependency rule: infrastructure code may import internal/application,
// internal/domain, and generated IDL types under gen/ — it IMPLEMENTS the
// ports those layers declare; enforced by scripts/check_deps.py (BASE-001).
package infrastructure
