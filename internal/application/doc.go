// Package application groups the APPLICATION layer subpackages (docs/02 §3):
// use-case orchestration, transaction boundaries, and policy invocation.
//
// Dependency rule: application code may import internal/domain (and generated
// IDL types under gen/) but must NOT import internal/adapter or
// internal/infrastructure. Outbound needs are declared as ports (interfaces)
// here or in domain, realized by adapter/infrastructure; enforced by
// scripts/check_deps.py (BASE-001).
package application
