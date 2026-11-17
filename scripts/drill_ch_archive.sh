#!/usr/bin/env bash
# P9-CH-01 acceptance drill (2026-11-17): REAL ClickHouse container + real
# PostgreSQL/Kafka + real api/worker processes. Proves the full chain:
#   ingest -> processor -> alert_transitions (PG) -> archive task ->
#   ClickHouse insert -> GET /history/v1/transitions (archived=true).
#
# Prereq: make dev-deps (PG 55432 / Kafka 19092), Docker able to pull
# clickhouse/clickhouse-server:24.8-alpine (cached offline after first run).
# Overrides: ALERT_CH_IMAGE, ALERT_PG_CONTAINER (dev-postgres-1),
#            ALERT_KAFKA_CONTAINER (dev-kafka-1).
set -euo pipefail
cd "$(dirname "$0")/.."

PG_DSN="${ALERT_TEST_PG_DSN:-postgres://postgres:postgres@127.0.0.1:55432/alert_platform?sslmode=disable}"
PG_CONTAINER="${ALERT_PG_CONTAINER:-dev-postgres-1}"
KAFKA_CONTAINER="${ALERT_KAFKA_CONTAINER:-dev-kafka-1}"
BROKERS="${ALERT_TEST_KAFKA_BROKERS:-127.0.0.1:19092}"
CH_IMAGE="${ALERT_CH_IMAGE:-clickhouse/clickhouse-server:24.8-alpine}"
CH_NAME="ap-ch-e2e-$$"
CH_PORT=18123
CH_USER=default
API_ADDR=":18100"
WORKER_ADDR=":18101"
RUN_ID="$(date +%s)"
CH_PASS="chpass_${RUN_ID}"
DB="alert_ch_${RUN_ID}"
CH_DB="alert_archive_${RUN_ID}"
TOPIC="alert.ch.${RUN_ID}"
WORK=".dev/ch-${RUN_ID}"
mkdir -p "$WORK"

cleanup() {
  [ -n "${WORKER_PID:-}" ] && kill "$WORKER_PID" 2>/dev/null || true
  [ -n "${API_PID:-}" ] && kill "$API_PID" 2>/dev/null || true
  if [ "${KEEP_WORK:-0}" = "1" ]; then :; else docker rm -f "$CH_NAME" >/dev/null 2>&1 || true; fi
  if [ "${KEEP_WORK:-0}" = "1" ]; then :; else docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null 2>&1 || true; fi
  if [ "${KEEP_WORK:-0}" = "1" ]; then echo "logs kept: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

step() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
psql_q() { docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qtAc "$1"; }
ch_q() { curl -sS -u "${CH_USER}:${CH_PASS}" "http://127.0.0.1:${CH_PORT}/?database=${CH_DB}" --data-binary "$1"; }

step "real ClickHouse container ($CH_IMAGE)"
# 24.x images disable network access for a passwordless default user, so the
# drill pins credentials (also exercises ALERT_CLICKHOUSE_USER/PASSWORD).
docker run -d --rm --name "$CH_NAME" -p "127.0.0.1:${CH_PORT}:8123" \
  -e CLICKHOUSE_USER="$CH_USER" -e CLICKHOUSE_PASSWORD="$CH_PASS" "$CH_IMAGE" >/dev/null
for i in $(seq 1 60); do
  curl -sf -u "${CH_USER}:${CH_PASS}" "http://127.0.0.1:${CH_PORT}/?query=SELECT%201" >/dev/null 2>&1 && break
  sleep 1
done
curl -sf -u "${CH_USER}:${CH_PASS}" "http://127.0.0.1:${CH_PORT}/?query=SELECT%201" >/dev/null || fail "ClickHouse did not become ready"
curl -sS -u "${CH_USER}:${CH_PASS}" "http://127.0.0.1:${CH_PORT}/" --data-binary "CREATE DATABASE IF NOT EXISTS ${CH_DB}" >/dev/null
curl -sS -u "${CH_USER}:${CH_PASS}" "http://127.0.0.1:${CH_PORT}/?database=${CH_DB}" \
  --data-binary @deploy/clickhouse/0001_transitions.sql >/dev/null
echo "    database=${CH_DB} table=alert_transitions"

step "disposable PG database + migrations"
docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null
docker exec "$PG_CONTAINER" psql -U postgres -qc "CREATE DATABASE ${DB}" >/dev/null
DSN_BASE="${PG_DSN%%\?*}"; TARGET_DSN="${DSN_BASE%/*}/${DB}?sslmode=disable"
go run ./cmd/alert-migrate -dsn "$TARGET_DSN" >/dev/null

step "kafka topics"
for t in "$TOPIC" alert.dlq.v1 alert.lifecycle.v1; do
  docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:29092 \
    --create --if-not-exists --topic "$t" --partitions 1 >/dev/null 2>&1 || true
done

step "seed source + credentials + platform token"
TENANT="eeee0000-0000-0000-0000-0000000000c1"
SOURCE="eeee0000-0000-0000-0000-0000000000c2"
SRC_TOKEN="aps_ch_${RUN_ID}"
USR_TOKEN="usr_ch_${RUN_ID}"
psql_q "INSERT INTO alert_sources (tenant_id, source_id, name, source_type, vendor_version, adapter_version, cluster_id, enabled, health_status)
        VALUES ('$TENANT','$SOURCE','ch-e2e','deepflow','7.2.4','v1','c1',true,'unknown')" >/dev/null
psql_q "INSERT INTO source_credentials (tenant_id, source_id, token_hash)
        VALUES ('$TENANT','$SOURCE', encode(sha256('${SRC_TOKEN}'::bytea),'hex'))" >/dev/null
psql_q "INSERT INTO platform_tokens (tenant_id, actor_id, role, token_hash)
        VALUES ('$TENANT','ch-drill','platform_admin', encode(sha256('platform-token/v1|${USR_TOKEN}'::bytea),'hex'))" >/dev/null

step "build + start api & worker (archive enabled, PG window 1m)"
go build -o "$WORK/api" ./cmd/alert-api
go build -o "$WORK/worker" ./cmd/alert-worker
ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_KAFKA_TOPIC="$TOPIC" \
  ALERT_API_ADDR="$API_ADDR" ALERT_HISTORY_PG_WINDOW=1m \
  ALERT_CLICKHOUSE_URL="http://127.0.0.1:${CH_PORT}" ALERT_CLICKHOUSE_DB="$CH_DB" \
  ALERT_CLICKHOUSE_USER="$CH_USER" ALERT_CLICKHOUSE_PASSWORD="$CH_PASS" \
  LOG_LEVEL="${DRILL_LOG_LEVEL:-error}" "$WORK/api" > "$WORK/api.log" 2>&1 & API_PID=$!
ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_KAFKA_TOPIC="$TOPIC" \
  ALERT_KAFKA_GROUP="ch-e2e-${RUN_ID}" ALERT_WORKER_ADDR="$WORKER_ADDR" \
  ALERT_CLICKHOUSE_URL="http://127.0.0.1:${CH_PORT}" ALERT_CLICKHOUSE_DB="$CH_DB" \
  ALERT_CLICKHOUSE_USER="$CH_USER" ALERT_CLICKHOUSE_PASSWORD="$CH_PASS" \
  ALERT_NOTIFY_MODE=off ALERT_STALE_AFTER=1h LOG_LEVEL="${DRILL_LOG_LEVEL:-error}" \
  "$WORK/worker" > "$WORK/worker.log" 2>&1 & WORKER_PID=$!
for i in $(seq 1 50); do
  curl -sf "http://127.0.0.1${API_ADDR}/healthz" >/dev/null 2>&1 && break
  sleep 0.2
done
curl -sf "http://127.0.0.1${API_ADDR}/healthz" >/dev/null || fail "api did not become healthy (see $WORK/api.log)"

sim() { # sim <signal> <event_key>
  curl -sf -X POST "http://127.0.0.1${API_ADDR}/ingest/v1/events:batch" \
    -H 'Content-Type: application/json' -H "Authorization: Bearer ${SRC_TOKEN}" \
    --data-binary "{\"batch_id\":\"ch-${RUN_ID}-$2\",\"events\":[{
      \"kind\":1,\"signal\":$1,\"severity\":1,\"alert_name\":\"CH-E2E\",
      \"event_key\":\"$2\",\"resource\":{\"resource_type\":\"pod\",\"resource_uid\":\"ch-pod-1\",\"cluster_id\":\"c1\"},
      \"source\":{\"source_id\":\"$SOURCE\",\"external_rule_id\":\"rule-ch\"}}]}" >/dev/null
}

step "ingest firing -> resolved"
sim 1 "ek-ch-firing"
sleep 2
sim 2 "ek-ch-resolved"

step "wait for processor transitions (PG) and archive (ClickHouse)"
for i in $(seq 1 40); do
  n=$(psql_q "SELECT count(*) FROM alert_transitions")
  [ "$n" = "2" ] && break
  sleep 1
done
[ "$(psql_q "SELECT count(*) FROM alert_transitions")" = "2" ] || fail "PG transitions != 2"
for i in $(seq 1 40); do
  n=$(ch_q "SELECT count() FROM alert_transitions FINAL" | tr -d '[:space:]')
  [ "$n" = "2" ] && break
  sleep 1
done
CH_COUNT=$(ch_q "SELECT count() FROM alert_transitions FINAL" | tr -d '[:space:]')
[ "$CH_COUNT" = "2" ] || fail "ClickHouse rows = ${CH_COUNT}, want 2 (archiver did not catch up; see $WORK/worker.log)"
echo "    PG=2 CH=${CH_COUNT}"

step "late/replayed event is archived (P10-001: no event-time cursor)"
NOW=$(date +%s); OLD=$((NOW-7200))
curl -sf -X POST "http://127.0.0.1${API_ADDR}/ingest/v1/events:batch" \
  -H 'Content-Type: application/json' -H "Authorization: Bearer ${SRC_TOKEN}" \
  --data-binary "{\"batch_id\":\"ch-late-${RUN_ID}\",\"events\":[{
    \"kind\":1,\"signal\":1,\"severity\":1,\"alert_name\":\"CH-Late\",
    \"event_key\":\"ek-ch-late\",\"occurred_at\":{\"seconds\":${OLD}},
    \"starts_at\":{\"seconds\":${OLD}},
    \"resource\":{\"resource_type\":\"pod\",\"resource_uid\":\"ch-pod-late\",\"cluster_id\":\"c1\"},
    \"source\":{\"source_id\":\"$SOURCE\",\"external_rule_id\":\"rule-ch-late\"}}]}" >/dev/null
for i in $(seq 1 40); do
  n=$(psql_q "SELECT count(*) FROM alert_transitions")
  [ "$n" = "3" ] && break
  sleep 1
done
[ "$(psql_q "SELECT count(*) FROM alert_transitions")" = "3" ] || fail "late event transition missing in PG"
for i in $(seq 1 40); do
  n=$(ch_q "SELECT count() FROM alert_transitions FINAL" | tr -d '[:space:]')
  [ "$n" = "3" ] && break
  sleep 1
done
[ "$(ch_q "SELECT count() FROM alert_transitions FINAL" | tr -d '[:space:]')" = "3" ] || fail "late event not archived (event-time cursor would lose it)"
echo "    late event archived: PG=3 CH=3"

step "CH write idempotency: re-insert the same rows, FINAL view stays 3"
LATE_JSON=$(ch_q "SELECT * FROM alert_transitions FINAL WHERE transition_at < toDateTime(${OLD}+60) FORMAT JSONEachRow" | head -1)
[ -n "$LATE_JSON" ] || fail "could not read a row for the duplicate test"
curl -sf -u "${CH_USER}:${CH_PASS}" "http://127.0.0.1:${CH_PORT}/?database=${CH_DB}&query=INSERT%20INTO%20alert_transitions%20FORMAT%20JSONEachRow&wait_end_of_query=1" --data-binary "$LATE_JSON" >/dev/null
sleep 1
DUP_COUNT=$(ch_q "SELECT count() FROM alert_transitions FINAL" | tr -d '[:space:]')
[ "$DUP_COUNT" = "3" ] || fail "duplicate insert changed FINAL count to ${DUP_COUNT} (ReplacingMergeTree dedup broken)"
echo "    duplicate insert collapsed: FINAL count still 3"

step "history API routes to CH archive (PG window 1m, wide window)"
NOW=$(date +%s); FROM=$((NOW-10800))
RESP=$(curl -sf -H "Authorization: Bearer ${USR_TOKEN}" \
  "http://127.0.0.1${API_ADDR}/history/v1/transitions?from=${FROM}&to=${NOW}&limit=10") || fail "history API failed"
echo "$RESP" | python3 -c '
import json,sys
body=json.load(sys.stdin)
data=body.get("data", body)
archived=data.get("archived")
assert archived is True, "archived flag = %r, want True" % (archived,)
rows=data.get("transitions") or []
assert len(rows)==3, "transitions=%d, want 3 (incl. late)" % len(rows)
states={r["to_state"] for r in rows}
assert states=={"firing","resolved"}, "states=%r" % (states,)
assert data.get("coverage")=="full", "coverage=%r" % (data.get("coverage"),)
print("    archived=true transitions=3 states=", sorted(states))'

echo "CH ARCHIVE E2E PASS: ingest -> PG -> archiver -> ClickHouse -> history API"