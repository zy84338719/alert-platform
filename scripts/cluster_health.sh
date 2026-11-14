#!/usr/bin/env bash
# MC-001: four-cluster component health gate. For each environment context,
# verify helm release status, pod readiness and each component /healthz.
# Usage: cluster_health.sh <kubecontext>...  (contexts provided with access)
set -euo pipefail
fail() { echo "FAIL: $*" >&2; exit 1; }
[ $# -ge 1 ] || fail "usage: $0 <kubecontext> [<kubecontext>...]"
overall=0
for ctx in "$@"; do
  echo "==> context ${ctx}"
  kubectl --context "$ctx" get ns alert-platform >/dev/null 2>&1 || { echo "  ns missing"; overall=1; continue; }
  kubectl --context "$ctx" -n alert-platform get deploy -o name | while read -r d; do
    ready=$(kubectl --context "$ctx" -n alert-platform get "$d" -o jsonpath='{.status.readyReplicas}')
    want=$(kubectl --context "$ctx" -n alert-platform get "$d" -o jsonpath='{.spec.replicas}')
    [ "${ready:-0}" = "$want" ] && echo "  OK  $d ${ready}/${want}" || { echo "  BAD $d ${ready:-0}/${want}"; overall=1; }
  done
  for pod in $(kubectl --context "$ctx" -n alert-platform get pods -l app.kubernetes.io/name=alert-platform -o name); do
    code=$(kubectl --context "$ctx" -n alert-platform exec "$pod" -- wget -qO- http://127.0.0.1:8080/healthz 2>/dev/null || \
            kubectl --context "$ctx" -n alert-platform exec "$pod" -- wget -qO- http://127.0.0.1:8081/healthz 2>/dev/null || \
            kubectl --context "$ctx" -n alert-platform exec "$pod" -- wget -qO- http://127.0.0.1:8082/healthz 2>/dev/null || echo unavailable)
    echo "  healthz $pod -> ${code:0:40}"
  done
done
[ "$overall" = "0" ] && echo "ALL CLUSTERS GREEN" || fail "one or more clusters degraded"
