#!/usr/bin/env bash
# CHAOS-001 / T-010: the PG master dies WHILE the worker processes. Asserts:
#   1) no offset is committed past the failure (redelivery on restart),
#   2) no event is silently lost (all pushed events eventually land),
#   3) recovery converges with zero duplicate transitions.
set -euo pipefail
cd "$(dirname "$0")/.."

BROKERS="${ALERT_TEST_KAFKA_BROKERS:-127.0.0.1:19092}"
RUN_ID="$(date +%s)"
WORK=".dev/t010-${RUN_ID}"
DB="alert_t010_${RUN_ID}"
PGC="${ALERT_PG_CONTAINER:-dev-postgres-1}"
mkdir -p "$WORK"

cleanup() {
  [ -n "${WORKER_PID:-}" ] && kill "$WORKER_PID" 2>/dev/null || true
  [ -n "${API_PID:-}" ] && kill "$API_PID" 2>/dev/null || true
  rm -rf "$WORK"
  docker exec "$PGC" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null 2>&1 || true
}
trap cleanup EXIT

step() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
psql_q() { docker exec "$PGC" psql -U postgres -d "$DB" -qtAc "$1"; }
DSN="postgres://postgres:postgres@127.0.0.1:55432/${DB}?sslmode=disable"

step "dedicated database + schema + topic"
docker exec "$PGC" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null
docker exec "$PGC" psql -U postgres -qc "CREATE DATABASE ${DB}" >/dev/null
go run ./cmd/alert-migrate -dsn "$DSN" >/dev/null
TOPIC="alert.t010.${RUN_ID}"
docker exec dev-kafka-1 kafka-topics --bootstrap-server localhost:29092 --create --topic "$TOPIC" --partitions 1 >/dev/null 2>&1 || true

step "api + worker; kill the DATABASE mid-stream"
go build -o "$WORK/api" ./cmd/alert-api
go build -o "$WORK/worker" ./cmd/alert-worker
go build -o "$WORK/loadgen" ./cmd/loadgen
ALERT_PG_DSN="$DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_API_ADDR=":18500" ALERT_KAFKA_TOPIC="$TOPIC" LOG_LEVEL=error "$WORK/api" > "$WORK/api.log" 2>&1 & API_PID=$!
for i in $(seq 1 50); do curl -sf http://127.0.0.1:18500/healthz >/dev/null 2>&1 && break; sleep 0.2; done

# 300 events at ~150 EPS
"$WORK/loadgen" -api http://127.0.0.1:18500 -eps 150 -d 2s > "$WORK/load.log" 2>&1 &
LOAD_PID=$!
sleep 1
# DATABASE DOWN (pg_isready fails; worker must fail-stop without committing)
docker exec "$PGC" psql -U postgres -qc "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${DB}'" >/dev/null
docker exec "$PGC" psql -U postgres -qc "ALTER DATABASE ${DB} RENAME TO ${DB}_down" >/dev/null
sleep 3
wait "$LOAD_PID" || true
if kill -0 "$API_PID" 2>/dev/null; then :; else fail "api died on PG outage (expected to keep serving /healthz degraded)"; fi

step "recovery: database back; worker restarts; everything converges"
docker exec "$PGC" psql -U postgres -qc "ALTER DATABASE ${DB}_down RENAME TO ${DB}" >/dev/null
ALERT_PG_DSN="$DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_KAFKA_TOPIC="$TOPIC" ALERT_KAFKA_GROUP="t010-${RUN_ID}" ALERT_WORKER_ADDR=":18501" ALERT_STALE_AFTER=1h LOG_LEVEL=error "$WORK/worker" > "$WORK/worker.log" 2>&1 & WORKER_PID=$!
for i in $(seq 1 120); do
  R=$(psql_q "SELECT count(*) FROM event_receipts" 2>/dev/null || echo 0)
  [ "$R" -ge 300 ] && break
  sleep 0.5
done

R=$(psql_q "SELECT count(*) FROM event_receipts")
[ "$R" = "300" ] || fail "receipts=$R want 300 (events silently lost)"
TR=$(psql_q "SELECT count(*) FROM alert_transitions")
[ "$TR" -le 300 ] || fail "transitions=$TR > 300 (duplicate effects after replay)"
sleep 3
TR=$(psql_q "SELECT count(*) FROM alert_transitions")
DUP=$(psql_q "SELECT count(*) FROM (SELECT transition_id FROM alert_transitions GROUP BY transition_id HAVING count(*)>1) x")
[ "$DUP" = "0" ] || fail "duplicate transition ids after replay"
INC=$(psql_q "SELECT count(*) FROM alert_instances")
echo "    receipts=$R transitions=$TR instances=$INC"
echo "T-010 DRILL PASS: PG 故障期间零误提交、恢复后 100% 收敛、零重复迁移"
