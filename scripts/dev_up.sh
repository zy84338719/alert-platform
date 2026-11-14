#!/usr/bin/env bash
# BASE-002 acceptance demo: start all three binaries, verify /healthz 200 on
# each, then prove trace_id correlation by driving one X-Trace-Id through
# edge -> API (real HTTP) and worker (local), asserting the same id appears
# in all three logs.
#
# Usage: scripts/dev_up.sh [up|down|trace]
#   up     (default) build, start, health-check, run trace demo, print result
#   down   stop everything started by this script
#   trace  re-run only the trace demo against already-running processes
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/.dev/bin"
LOG="$ROOT/.dev/logs"
PIDF="$ROOT/.dev/pids"
mkdir -p "$BIN" "$LOG" "$PIDF"
cd "$ROOT"

# Trace ids must satisfy the hmw whitelist: [0-9a-zA-Z-], length <= 64.
TRACE_ID="dev-$(date +%s)-${RANDOM}${RANDOM}"

log() { printf '[dev_up] %s\n' "$*"; }

die() {
  printf '[dev_up] FAIL: %s\n' "$*" >&2
  echo "----- log tails -----" >&2
  for f in "$LOG"/*.log; do
    [ -f "$f" ] || continue
    echo "== $f ==" >&2
    tail -n 25 "$f" >&2 || true
  done
  exit 1
}

stop_all() {
  for f in "$PIDF"/*.pid; do
    [ -f "$f" ] || continue
    pid="$(cat "$f" 2>/dev/null || true)"
    if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; fi
    rm -f "$f"
  done
  # Best-effort cleanup of stragglers from earlier runs.
  pkill -f "$BIN/alert-" 2>/dev/null || true
}

start_one() {
  name="$1"
  "$BIN/$name" -config etc/config.yaml >"$LOG/$name.log" 2>&1 &
  echo $! >"$PIDF/$name.pid"
  log "started $name (pid $!)"
}

wait_health() { # wait_health <name> <port>
  name="$1"; port="$2"
  i=0
  while [ "$i" -lt 60 ]; do
    code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/healthz" || echo 000)"
    if [ "$code" = "200" ]; then log "$name /healthz -> 200"; return 0; fi
    i=$((i + 1))
    sleep 0.25
  done
  die "$name /healthz did not return 200 (last=$code)"
}

run_trace_demo() {
  log "trace demo: TRACE_ID=$TRACE_ID"
  # 1) edge /trace-demo forwards to API, propagating X-Trace-Id (real HTTP hop).
  curl -s -H "X-Trace-Id: $TRACE_ID" "http://127.0.0.1:8082/trace-demo" >/dev/null \
    || die "edge /trace-demo request failed"
  # 2) worker local request with the same id.
  curl -s -H "X-Trace-Id: $TRACE_ID" "http://127.0.0.1:8081/version" >/dev/null \
    || die "worker /version request failed"
  sleep 1
  ok=1
  for name in alert-edge alert-api alert-worker; do
    hits="$(grep -c "$TRACE_ID" "$LOG/$name.log" 2>/dev/null || true)"
    [ -n "$hits" ] || hits=0
    if [ "$hits" -gt 0 ]; then
      log "$name log contains TRACE_ID ($hits hits)"
    else
      log "$name log MISSING TRACE_ID"
      ok=0
    fi
  done
  [ "$ok" -eq 1 ] || die "trace_id not present in all three component logs"
  log "TRACE CHAIN OK: $TRACE_ID observed in alert-api + alert-worker + alert-edge logs"
}

case "${1:-up}" in
  down)
    stop_all
    log "stopped"
    ;;
  trace)
    run_trace_demo
    ;;
  up)
    stop_all || true
    log "building binaries..."
    GOWORK=off go build -o "$BIN/alert-api" ./cmd/alert-api
    GOWORK=off go build -o "$BIN/alert-worker" ./cmd/alert-worker
    GOWORK=off go build -o "$BIN/alert-edge" ./cmd/alert-edge
    start_one alert-api
    wait_health alert-api 8080
    start_one alert-worker
    wait_health alert-worker 8081
    start_one alert-edge
    wait_health alert-edge 8082
    run_trace_demo
    log "all three binaries up; logs in $LOG; stop with: scripts/dev_up.sh down"
    ;;
  *)
    echo "usage: $0 [up|down|trace]" >&2
    exit 2
    ;;
esac
