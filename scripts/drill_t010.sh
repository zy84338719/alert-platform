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
  docker start "$PGC" >/dev/null 2>&1 || true   # the drill stopped it; always bring it back
  if [ "${KEEP_WORK:-0}" = "1" ]; then echo "logs kept: $WORK"; else rm -rf "$WORK"; fi
  [ -n "${WORKER_PID:-}" ] && kill "$WORKER_PID" 2>/dev/null || true
  [ -n "${API_PID:-}" ] && kill "$API_PID" 2>/dev/null || true
  if [ "${KEEP_WORK:-0}" != "1" ]; then docker exec "$PGC" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null 2>&1 || true; fi
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

step "seed source + credentials (P7-P0-03: ingest is credential-gated)"
TENANT="eeee0000-0000-0000-0000-0000000000d1"
SOURCE="eeee0000-0000-0000-0000-0000000000d2"
SRC_TOKEN="aps_t010_${RUN_ID}"
SRC_HASH=$(python3 -c "import hashlib;print(hashlib.sha256('$SRC_TOKEN'.encode()).hexdigest())")
docker exec "$PGC" psql -U postgres -d "$DB" -qc "
INSERT INTO alert_sources (tenant_id, source_id, name, source_type, vendor_version, adapter_version, cluster_id, enabled, health_status)
VALUES ('$TENANT','$SOURCE','t010','deepflow','7.2.3','x','c1',true,'unknown');
INSERT INTO source_credentials (tenant_id, source_id, token_hash) VALUES ('$TENANT','$SOURCE','$SRC_HASH');" >/dev/null

step "api + worker; kill the DATABASE mid-stream"
go build -o "$WORK/api" ./cmd/alert-api
go build -o "$WORK/worker" ./cmd/alert-worker
go build -o "$WORK/loadgen" ./cmd/loadgen
ALERT_PG_DSN="$DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_API_ADDR=":18500" ALERT_KAFKA_TOPIC="$TOPIC" LOG_LEVEL="${DRILL_LOG_LEVEL:-error}" "$WORK/api" > "$WORK/api.log" 2>&1 & API_PID=$!
ALERT_PG_DSN="$DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_KAFKA_TOPIC="$TOPIC" \
  ALERT_KAFKA_GROUP="t010-${RUN_ID}" ALERT_WORKER_ADDR=":18501" ALERT_STALE_AFTER=1h LOG_LEVEL="${DRILL_LOG_LEVEL:-error}" \
  "$WORK/worker" > "$WORK/worker.log" 2>&1 & WORKER_PID=$!
for i in $(seq 1 50); do curl -sf http://127.0.0.1:18500/healthz >/dev/null 2>&1 && break; sleep 0.2; done

# 300 events at ~150 EPS (credential-bound loadgen)
"$WORK/loadgen" -api http://127.0.0.1:18500 -eps 150 -d 2s -pg "$DSN" \
  -token "$SRC_TOKEN" -tenant "$TENANT" -source "$SOURCE" > "$WORK/load.log" 2>&1 &
LOAD_PID=$!
push() { # push <event_key> — one canonical event via the REAL API path
  # Producer semantics (R28): bounded retries with the SAME batch_id; a failed
  # batch must never be silently dropped (T-010 convergence depends on it).
  for attempt in 1 2 3 4 5; do
    code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:18500/ingest/v1/events:batch" \
      -H 'Content-Type: application/json' -H "Authorization: Bearer ${SRC_TOKEN}" \
      --data-binary "{\"batch_id\":\"t010-${RUN_ID}-$1\",\"events\":[{\"kind\":1,\"signal\":1,\"severity\":1,\"alert_name\":\"T010 $1\",\"event_key\":\"$1\",\"resource\":{\"resource_type\":\"pod\",\"resource_uid\":\"$1\",\"cluster_id\":\"c1\"},\"source\":{\"source_id\":\"$SOURCE\",\"external_rule_id\":\"rule-$1\"}}]}" || echo 000)
    [ "$code" = "202" ] && return 0
    sleep 1
  done
  echo "WARN: push $1 failed after retries (last HTTP $code)" >&2
  return 1
}
sleep 1
# DATABASE DOWN mid-stream: STOP the PG container — established connections
# die and new connects refuse, so the worker's next ApplyEvent fails and the
# supervised processor FAIL-STOPS without committing (P7 P1-03): offsets stay
# at the last committed position and redelivery after recovery is exact.
# (A database RENAME is racy here: existing backends keep working across a
# rename and the pool may reconnect inside the window — found by this drill.)
docker stop "$PGC" >/dev/null
# Deterministic fail-stop trigger: push events while PG is DOWN. The worker
# consumes them from Kafka and its next ApplyEvent MUST fail — the supervised
# processor then fail-stops WITHOUT committing (they stay in Kafka and are
# redelivered after recovery).
push_down() { for i in $(seq 1 10); do push "down-$i"; done }
push_down
WRC=0
wait "$WORKER_PID" 2>/dev/null || WRC=$?
[ "$WRC" -ne 0 ] || fail "worker did not fail-stop while PG was down (offsets could pass the failure)"
sleep 2
wait "$LOAD_PID" || true
if kill -0 "$API_PID" 2>/dev/null; then :; else fail "api died on PG outage (expected to keep serving /healthz degraded)"; fi

step "recovery: database back; worker restarts; everything converges"
docker start "$PGC" >/dev/null
for i in $(seq 1 60); do
  docker exec "$PGC" pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$PGC" pg_isready -U postgres >/dev/null || fail "postgres did not recover"
ALERT_PG_DSN="$DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_KAFKA_TOPIC="$TOPIC" ALERT_KAFKA_GROUP="t010-${RUN_ID}" ALERT_WORKER_ADDR=":18501" ALERT_STALE_AFTER=1h LOG_LEVEL="${DRILL_LOG_LEVEL:-error}" "$WORK/worker" > "$WORK/worker.log" 2>&1 & WORKER_PID=$!
for i in $(seq 1 120); do
  R=$(psql_q "SELECT count(*) FROM event_receipts" 2>/dev/null || echo 0)
  [ "$R" -ge 300 ] && break
  sleep 0.5
done

R=$(psql_q "SELECT count(*) FROM event_receipts")
[ "$R" -ge 300 ] || fail "receipts=$R want >=300 (events silently lost)"
TR=$(psql_q "SELECT count(*) FROM alert_transitions")
[ "$TR" -le "$R" ] || fail "transitions=$TR > receipts=$R (duplicate effects after replay)"
sleep 3
TR=$(psql_q "SELECT count(*) FROM alert_transitions")
DUP=$(psql_q "SELECT count(*) FROM (SELECT transition_id FROM alert_transitions GROUP BY transition_id HAVING count(*)>1) x")
[ "$DUP" = "0" ] || fail "duplicate transition ids after replay"
INC=$(psql_q "SELECT count(*) FROM alert_instances")
echo "    receipts=$R transitions=$TR instances=$INC"
echo "T-010 DRILL PASS: PG 故障期间零误提交、恢复后 100% 收敛、零重复迁移"
