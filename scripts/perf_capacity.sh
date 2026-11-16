#!/usr/bin/env bash
# PERF-001 capacity run (local scale, real processes): steady 100 EPS for 2
# minutes then a 2000 EPS burst for 60s, against the local compose stack
# (PG 55432 / Kafka 19092). Reports push + end-to-end percentiles and error
# rates; PG IO note included. Full 10-minute burst belongs on the perf
# cluster — this local run pins the pipeline shape.
set -euo pipefail
cd "$(dirname "$0")/.."

PG_DSN="${ALERT_TEST_PG_DSN:-postgres://postgres:postgres@127.0.0.1:55432/alert_platform?sslmode=disable}"
PG_CONTAINER="${ALERT_PG_CONTAINER:-dev-postgres-1}"
BROKERS="${ALERT_TEST_KAFKA_BROKERS:-127.0.0.1:19092}"
RUN_ID="$(date +%s)"
# Durations overridable for long-run evidence (PERF-003: 10min steady).
STEADY_DURATION="${STEADY_DURATION:-120s}"
BURST_DURATION="${BURST_DURATION:-60s}"
DRAIN_WAIT="${DRAIN_WAIT:-5}"
DB="alert_perf_${RUN_ID}"
WORK=".dev/perf-${RUN_ID}"
mkdir -p "$WORK"

cleanup() {
  [ -n "${WORKER_PID:-}" ] && kill "$WORKER_PID" 2>/dev/null || true
  [ -n "${API_PID:-}" ] && kill "$API_PID" 2>/dev/null || true
  rm -rf "$WORK"
  docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null 2>&1 || true
}
trap cleanup EXIT

DSN_BASE="${PG_DSN%%\?*}"; TARGET_DSN="${DSN_BASE%/*}/${DB}?sslmode=disable"
echo "==> disposable perf database + schema"
docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null
docker exec "$PG_CONTAINER" psql -U postgres -qc "CREATE DATABASE ${DB}" >/dev/null
go run ./cmd/alert-migrate -dsn "$TARGET_DSN" >/dev/null
# P7-P0-03: /ingest/v1 is credential-gated. Register the perf source and its
# token so the load generator drives a REAL authorized push path (random
# unauthenticated identities would 401/403 and void the numbers).
PERF_TENANT="11112222-0000-0000-0000-000000000001"
PERF_SOURCE="11112222-0000-0000-0000-0000000000a1"
PERF_TOKEN="aps_perf_${RUN_ID}"
docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qc "INSERT INTO alert_sources (tenant_id, source_id, name, source_type, vendor_version, adapter_version, cluster_id, enabled, health_status) VALUES ('$PERF_TENANT','$PERF_SOURCE','perf-loadgen','deepflow','7.2.4','v1','perf',true,'unknown')" >/dev/null
docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qc "INSERT INTO source_credentials (tenant_id, source_id, token_hash) VALUES ('$PERF_TENANT','$PERF_SOURCE', encode(sha256('$PERF_TOKEN'::bytea),'hex'))" >/dev/null
echo "==> perf source registered: tenant=$PERF_TENANT source=$PERF_SOURCE"
docker exec dev-kafka-1 kafka-topics --bootstrap-server localhost:29092 --create --topic "alert.perf.${RUN_ID}" --partitions 3 >/dev/null 2>&1 || true
# Quarantine + lifecycle targets (compose Kafka has auto-create disabled).
for t in alert.dlq.v1 alert.lifecycle.v1; do
  docker exec dev-kafka-1 kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic "$t" --partitions 3 >/dev/null 2>&1 || true
done

go build -o "$WORK/api" ./cmd/alert-api
go build -o "$WORK/worker" ./cmd/alert-worker
go build -o "$WORK/loadgen" ./cmd/loadgen

echo "==> start api + worker (single worker replica, local scale)"
ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_API_ADDR=":18400" \
  ALERT_KAFKA_TOPIC="alert.perf.${RUN_ID}" LOG_LEVEL=error "$WORK/api" > "$WORK/api.log" 2>&1 & API_PID=$!
ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_KAFKA_TOPIC="alert.perf.${RUN_ID}" \
  ALERT_KAFKA_GROUP="perf-${RUN_ID}" ALERT_WORKER_ADDR=":18401" ALERT_STALE_AFTER=1h LOG_LEVEL=error \
  "$WORK/worker" > "$WORK/worker.log" 2>&1 & WORKER_PID=$!
for i in $(seq 1 50); do curl -sf "http://127.0.0.1:18400/healthz" >/dev/null 2>&1 && break; sleep 0.2; done

echo "==> steady 100 EPS for ${STEADY_DURATION}"
"$WORK/loadgen" -api http://127.0.0.1:18400 -eps 100 -d "$STEADY_DURATION" -pg "$TARGET_DSN" \
  -token "$PERF_TOKEN" -tenant "$PERF_TENANT" -source "$PERF_SOURCE" | tee "$WORK/steady.txt"
echo "==> burst 2000 EPS for ${BURST_DURATION} (local scale; full 10min on perf cluster)"
"$WORK/loadgen" -api http://127.0.0.1:18400 -eps 2000 -d "$BURST_DURATION" -burst -pg "$TARGET_DSN" \
  -token "$PERF_TOKEN" -tenant "$PERF_TENANT" -source "$PERF_SOURCE" | tee "$WORK/burst.txt"

echo "==> post-run invariants (drain wait ${DRAIN_WAIT}s)"
sleep "$DRAIN_WAIT"
docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qtAc "SELECT 'instances='||count(*) FROM alert_instances"
docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qtAc "SELECT 'receipts='||count(*) FROM event_receipts"
docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qtAc "SELECT 'transitions='||count(*) FROM alert_transitions"
docker exec dev-kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group "perf-${RUN_ID}" 2>/dev/null | head -4 || true

echo "PERF LOCAL RUN COMPLETE (artifacts: $WORK)"
