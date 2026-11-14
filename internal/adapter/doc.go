// Package adapter groups the ADAPTER layer subpackages (docs/02 §3): vendor
// protocol parsing/mapping (DeepFlow, Alertmanager, HUATUO) on the inbound
// side, and realization of application/domain ports on the outbound side.
//
// Dependency rule: adapter code may import internal/application,
// internal/domain, and generated IDL types under gen/; enforced by
// scripts/check_deps.py (BASE-001). Vendor-native structures never cross into
// the domain — they stop at the adapter boundary (CanonicalEvent, docs/07 §1).
package adapter
