#!/usr/bin/env bash
# PROC-008 v0.2 acceptance: replay a Kafka history segment through the real
# api+worker processes and assert the idempotency/ordering invariants named by
# docs/13:
#   T-005  1000 same-source firing redeliveries -> exactly 1 transition
#   T-006  older firing after a newer valid recovery -> no rollback, no reopen
#   T-007  identical batch replay (offset-commit-failure equivalent) -> no
#          duplicate transitions/domain events
# Prereq: make dev-deps (PG 55432 / Kafka 19092) and migrated schema.
set -euo pipefail
cd "$(dirname "$0")/.."

PG_DSN="${ALERT_TEST_PG_DSN:-postgres://postgres:postgres@127.0.0.1:55432/alert_platform?sslmode=disable}"
# psql runs inside the compose PG container (host machines need not have psql).
PG_CONTAINER="${ALERT_PG_CONTAINER:-dev-postgres-1}"
BROKERS="${ALERT_TEST_KAFKA_BROKERS:-127.0.0.1:19092}"
API_ADDR=":18080"
TENANT="eeee0000-0000-0000-0000-0000000000aa"
SOURCE="eeee0000-0000-0000-0000-0000000000bb"
RUN_ID="$(date +%s)"
DB="alert_replay_${RUN_ID}"
# Target DSN: same server, disposable database, no TLS (compose PG).
DSN_BASE="${PG_DSN%%\?*}"
TARGET_DSN="${DSN_BASE%/*}/${DB}?sslmode=disable"
WORK=".dev/replay-${RUN_ID}"
mkdir -p "$WORK"

cleanup() {
  [ -n "${WORKER_PID:-}" ] && kill "$WORKER_PID" 2>/dev/null || true
  [ -n "${API_PID:-}" ] && kill "$API_PID" 2>/dev/null || true
  rm -rf "$WORK"
  # Drop the disposable database (best effort; leftover named alert_replay_*).
  docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null 2>&1 || true
}
trap cleanup EXIT

step() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

psql_q() { docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qtAc "$1"; }

step "disposable database + schema"
docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null
docker exec "$PG_CONTAINER" psql -U postgres -qc "CREATE DATABASE ${DB}" >/dev/null
go run ./cmd/alert-migrate -dsn "$TARGET_DSN" >/dev/null

step "create run topic alert.replay.${RUN_ID}"
docker exec dev-kafka-1 kafka-topics --bootstrap-server localhost:29092 --create \
  --topic "alert.replay.${RUN_ID}" --partitions 1 >/dev/null
for t in alert.dlq.v1 alert.lifecycle.v1; do
  docker exec dev-kafka-1 kafka-topics --bootstrap-server localhost:29092 \
    --create --if-not-exists --topic "$t" --partitions 3 >/dev/null 2>&1 || true
done

step "build + start api & worker against ${DB}"
go build -o "$WORK/api" ./cmd/alert-api
go build -o "$WORK/worker" ./cmd/alert-worker
ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" \
  ALERT_API_ADDR="$API_ADDR" ALERT_KAFKA_TOPIC="alert.replay.${RUN_ID}" \
  ALERT_KAFKA_GROUP="replay-${RUN_ID}" LOG_LEVEL=info \
  "$WORK/api" > "$WORK/api.log" 2>&1 & API_PID=$!
ALERT_PG_DSN="${PG_DSN%/*}/${DB}" ALERT_KAFKA_BROKERS="$BROKERS" \
  ALERT_KAFKA_TOPIC="alert.replay.${RUN_ID}" ALERT_KAFKA_GROUP="replay-${RUN_ID}" \
  ALERT_WORKER_ADDR=":18081" ALERT_STALE_AFTER=1h LOG_LEVEL=info \
  "$WORK/worker" > "$WORK/worker.log" 2>&1 & WORKER_PID=$!

for i in $(seq 1 50); do
  curl -sf "http://127.0.0.1${API_ADDR}/healthz" >/dev/null 2>&1 && break
  sleep 0.2
done
curl -sf "http://127.0.0.1${API_ADDR}/healthz" >/dev/null || fail "api did not become healthy (see $WORK/api.log)"

step "seed source row + credential (P7-P0-03: /ingest/v1 is credential-gated)"
psql_q "INSERT INTO alert_sources (tenant_id, source_id, name, source_type, vendor_version, adapter_version, cluster_id)
        VALUES ('${TENANT}','${SOURCE}','replay','deepflow','7.2.3','x','c1') ON CONFLICT DO NOTHING" >/dev/null
SRC_TOKEN="aps_replay_${RUN_ID}"
psql_q "INSERT INTO source_credentials (tenant_id, source_id, token_hash)
        VALUES ('${TENANT}','${SOURCE}', encode(sha256('${SRC_TOKEN}'::bytea),'hex'))" >/dev/null

emit() { # emit <json-file> -> POST canonical batch (credential-bound)
  curl -sf -X POST "http://127.0.0.1${API_ADDR}/ingest/v1/events:batch" \
    -H 'Content-Type: application/json' -H "Authorization: Bearer ${SRC_TOKEN}" \
    --data-binary @"$1" >/dev/null
}

step "T-005: 1000 same-source firing redeliveries in one logical episode"
python3 - "$WORK" "$TENANT" "$SOURCE" <<'EOF'
import json, sys, datetime
work, tenant, source = sys.argv[1], sys.argv[2], sys.argv[3]
def ts(dt): return {"seconds": int(dt.timestamp()), "nanos": 0}
base = datetime.datetime(2026, 11, 14, 10, 0, 0, tzinfo=datetime.timezone.utc)
events = []
for i in range(1000):
    events.append({
        "event_id": f"evt-{i}", "transport_id": f"tp-{i}",
        "tenant_id": tenant,
        "source": {"source_id": source, "source_type": 1, "external_rule_id": "rule-replay"},
        "resource": {"cluster_id": "c1", "resource_type": "pod", "resource_uid": "pod-replay"},
        "kind": 1, "signal": 1, "severity": 1, "alert_name": "ReplayFiring",
        "occurred_at": ts(base), "received_at": ts(base),
        "starts_at": ts(base), "event_key": f"ek-{i}",
    })
json.dump({"batch_id": "replay-t5", "events": events}, open(f"{work}/t5.json", "w"))
EOF
emit "$WORK/t5.json"

step "T-006: newer recovery, then an OLDER firing straggler"
python3 - "$WORK" "$TENANT" "$SOURCE" <<'EOF'
import json, sys, datetime
work, tenant, source = sys.argv[1], sys.argv[2], sys.argv[3]
def ts(dt): return {"seconds": int(dt.timestamp()), "nanos": 0}
base = datetime.datetime(2026, 11, 14, 10, 0, 0, tzinfo=datetime.timezone.utc)
resolved_at = base + datetime.timedelta(minutes=10)
old_firing = base + datetime.timedelta(minutes=5)
events = [
    {"event_id": "evt-r", "transport_id": "tp-r", "tenant_id": tenant,
     "source": {"source_id": source, "source_type": 1, "external_rule_id": "rule-replay"},
     "resource": {"cluster_id": "c1", "resource_type": "pod", "resource_uid": "pod-replay"},
     "kind": 1, "signal": 2, "alert_name": "ReplayFiring",
     "occurred_at": ts(resolved_at), "received_at": ts(resolved_at),
     "ends_at": ts(resolved_at), "event_key": "ek-r"},
    {"event_id": "evt-o", "transport_id": "tp-o", "tenant_id": tenant,
     "source": {"source_id": source, "source_type": 1, "external_rule_id": "rule-replay"},
     "resource": {"cluster_id": "c1", "resource_type": "pod", "resource_uid": "pod-replay"},
     "kind": 1, "signal": 1, "severity": 1, "alert_name": "ReplayFiring",
     "occurred_at": ts(old_firing), "received_at": ts(old_firing),
     "event_key": "ek-o"},
]
json.dump({"batch_id": "replay-t6", "events": events}, open(f"{work}/t6.json", "w"))
EOF
emit "$WORK/t6.json"

step "T-007: replay BOTH batches verbatim (offset-commit-failure equivalent)"
emit "$WORK/t5.json"
emit "$WORK/t6.json"

step "wait for the worker to drain both batches (twice)"
for i in $(seq 1 100); do
  receipts=$(psql_q "SELECT count(*) FROM event_receipts")
  [ "$receipts" -ge 1002 ] && break
  sleep 0.3
done
sleep 3  # let in-flight processing settle

step "assert invariants"
transitions=$(psql_q "SELECT count(*) FROM alert_transitions")
instances=$(psql_q "SELECT count(*) FROM alert_instances")
state=$(psql_q "SELECT monitor_state FROM alert_instances LIMIT 1")
outbox=$(psql_q "SELECT count(*) FROM domain_outbox")
receipts=$(psql_q "SELECT count(*) FROM event_receipts")

echo "    instances=$instances transitions=$transitions state=$state outbox=$outbox receipts=$receipts"
[ "$instances" = "1" ] || fail "T-005: instances=$instances want 1"
[ "$transitions" = "2" ] || fail "T-005/T-006: transitions=$transitions want 2 (one firing + one resolved)"
[ "$state" = "resolved" ] || fail "T-006: state=$state — older firing rolled back the episode"
[ "$outbox" = "2" ] || fail "T-007: domain events=$outbox want 2 (replay created no duplicates)"
[ "$receipts" = "1002" ] || fail "receipts=$receipts want 1002 (second replay fully absorbed)"

echo "REPLAY v0.2 PASS: T-005/T-006/T-007 invariants hold through real api+worker processes"
