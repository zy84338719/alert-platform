#!/usr/bin/env bash
# MC-003 / T-011 drill (local scale): the CENTER goes dark; the edge keeps
# accepting vendor pushes (WAL buffers); the center returns; every buffered
# event must arrive (100% 补传), waterlines visible, and a WORKER restart
# loses no in-flight task.
set -euo pipefail
cd "$(dirname "$0")/.."

PG_DSN="${ALERT_TEST_PG_DSN:-postgres://postgres:postgres@127.0.0.1:55432/alert_platform?sslmode=disable}"
PG_CONTAINER="${ALERT_PG_CONTAINER:-dev-postgres-1}"
BROKERS="${ALERT_TEST_KAFKA_BROKERS:-127.0.0.1:19092}"
RUN_ID="$(date +%s)"
DB="alert_t011_${RUN_ID}"
WORK=".dev/t011-${RUN_ID}"
mkdir -p "$WORK"

TENANT="dddd1111-0000-0000-0000-0000000000aa"
SOURCE_ID="dddd1111-0000-0000-0000-0000000000bb"
SRC_TOKEN="aps_t011_src"
SRC_HASH=$(python3 -c "import hashlib;print(hashlib.sha256('$SRC_TOKEN'.encode()).hexdigest())")
# P8-P0-02: the edge uploads via its UPLINK identity (X-Edge-Id + token),
# scoped to the allowed source set — not via a raw unregistered key.
EDGE_TOKEN="aps_edge-uplink-t011"
EDGE_HASH=$(python3 -c "import hashlib;print(hashlib.sha256('$EDGE_TOKEN'.encode()).hexdigest())")
TOPIC="alert.t011.${RUN_ID}"

cleanup() {
  [ -n "${WORKER_PID:-}" ] && kill "$WORKER_PID" 2>/dev/null || true
  [ -n "${CENTER_PID:-}" ] && kill "$CENTER_PID" 2>/dev/null || true
  [ -n "${EDGE_PID:-}" ] && kill "$EDGE_PID" 2>/dev/null || true
  rm -rf "$WORK"
  docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null 2>&1 || true
}
trap cleanup EXIT

step() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
psql_q() { docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qtAc "$1"; }
DSN_BASE="${PG_DSN%%\?*}"; TARGET_DSN="${DSN_BASE%/*}/${DB}?sslmode=disable"

step "一次性库 + 来源 + topic"
docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null
docker exec "$PG_CONTAINER" psql -U postgres -qc "CREATE DATABASE ${DB}" >/dev/null
go run ./cmd/alert-migrate -dsn "$TARGET_DSN" >/dev/null
docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qc "
INSERT INTO alert_sources (tenant_id, source_id, name, source_type, vendor_version, adapter_version, cluster_id, last_seen_at, health_status)
VALUES ('$TENANT','$SOURCE_ID','t011','deepflow','7.2.3','x','prod-studio', now(), 'ok');
INSERT INTO source_credentials (tenant_id, source_id, token_hash) VALUES ('$TENANT','$SOURCE_ID','$SRC_HASH');
INSERT INTO edge_uplink_credentials (edge_id, tenant_id, token_hash, allowed_source_ids)
VALUES ('edge-t011','$TENANT','$EDGE_HASH', ARRAY['$SOURCE_ID']::uuid[]);" >/dev/null
docker exec dev-kafka-1 kafka-topics --bootstrap-server localhost:29092 --create --topic "$TOPIC" --partitions 1 >/dev/null 2>&1 || true

step "启动 center(api+worker) + edge(gateway)"
go build -o "$WORK/api" ./cmd/alert-api
go build -o "$WORK/worker" ./cmd/alert-worker
go build -o "$WORK/edge" ./cmd/alert-edge

start_center() {
  ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_API_ADDR=":18300" \
    ALERT_KAFKA_TOPIC="$TOPIC" ALERT_EDGE_API_KEY="edge-key-t011" LOG_LEVEL=warn \
    "$WORK/api" > "$WORK/api.log" 2>&1 & CENTER_PID=$!
}
start_center
ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_KAFKA_TOPIC="$TOPIC" \
  ALERT_KAFKA_GROUP="t011-${RUN_ID}" ALERT_WORKER_ADDR=":18301" ALERT_STALE_AFTER=1h LOG_LEVEL=warn \
  "$WORK/worker" > "$WORK/worker.log" 2>&1 & WORKER_PID=$!
cat > "$WORK/edge-sources.json" <<EOF
{"sources":[{"token_hash":"$SRC_HASH","tenant_id":"$TENANT","source_id":"$SOURCE_ID","cluster_id":"prod-studio","type":"deepflow"}]}
EOF
ALERT_EDGE_ADDR=":18302" ALERT_EDGE_MODE=gateway ALERT_EDGE_WAL_DIR="$WORK/wal" \
  ALERT_EDGE_SOURCES_FILE="$WORK/edge-sources.json" ALERT_API_ORIGIN="http://127.0.0.1:18300" \
  ALERT_EDGE_API_KEY="edge-key-t011" ALERT_HEARTBEAT_SOURCE_ID="$SOURCE_ID" \
  ALERT_EDGE_UPLINK_TOKEN="$EDGE_TOKEN" ALERT_EDGE_ID="edge-t011" \
  ALERT_HUATUO_TENANT_ID="$TENANT" LOG_LEVEL=warn \
  "$WORK/edge" > "$WORK/edge.log" 2>&1 & EDGE_PID=$!
for p in 18300 18301 18302; do
  for i in $(seq 1 50); do curl -sf "http://127.0.0.1:$p/healthz" >/dev/null 2>&1 && break; sleep 0.2; done
done
curl -sf "http://127.0.0.1:18302/healthz" >/dev/null || fail "edge unhealthy"

step "断网：停掉 center（30min 的本地等价），edge 继续接单"
kill "$CENTER_PID" 2>/dev/null; wait "$CENTER_PID" 2>/dev/null || true
sleep 1

push() { # push <n>
  curl -sf -X POST "http://127.0.0.1:18302/ingest/external/deepflow/v7_2/events" \
    -H "Authorization: Bearer $SRC_TOKEN" -H 'Content-Type: application/json' \
    -d "{\"schema_version\":\"deepflow.bridge.v1\",\"source_id\":\"$SOURCE_ID\",\"external_event_id\":\"t011-$1\",\"event_state\":0,\"policy_id\":\"p$1\",\"event_level\":3,\"alert_name\":\"T011 Alert $1\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"target_key\":\"pod-t011-$1\",\"cluster_id\":\"prod-studio\"}" >/dev/null
}
N=25
for i in $(seq 1 $N); do push "$i"; done
BYTES=$(curl -s "http://127.0.0.1:18302/edge/stats" | python3 -c "import json,sys;d=json.load(sys.stdin).get('data',{});print(d.get('wal_pending',''))")
[ "$BYTES" = "$N" ] || fail "edge buffered $BYTES, want $N (waterline visible)"

step "worker 重启（中心实例重启不丢任务）"
kill "$WORKER_PID" 2>/dev/null; wait "$WORKER_PID" 2>/dev/null || true
ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_KAFKA_TOPIC="$TOPIC" \
  ALERT_KAFKA_GROUP="t011-${RUN_ID}" ALERT_WORKER_ADDR=":18301" ALERT_STALE_AFTER=1h LOG_LEVEL=warn \
  "$WORK/worker" > "$WORK/worker2.log" 2>&1 & WORKER_PID=$!

step "恢复 center；edge 必须补传 100%"
start_center
for i in $(seq 1 120); do
  RECEIVED=$(psql_q "SELECT count(*) FROM alert_instances WHERE name LIKE 'T011%'")
  [ "$RECEIVED" = "$N" ] && break
  sleep 0.5
done
RECEIVED=$(psql_q "SELECT count(*) FROM alert_instances WHERE name LIKE 'T011%'")
[ "$RECEIVED" = "$N" ] || fail "补传不完整：received=$RECEIVED want=$N"
sleep 3
PENDING=$(curl -s "http://127.0.0.1:18302/edge/stats" | python3 -c "import json,sys;print(json.load(sys.stdin).get('data',{}).get('wal_pending',''))")
[ "$PENDING" = "0" ] || fail "edge 仍有 $PENDING 条未确认"

step "中心实例重启后无重复迁移"
TRANS=$(psql_q "SELECT count(*) FROM alert_transitions WHERE event_id LIKE 't011-%'")
[ "$TRANS" = "$N" ] || fail "transitions=$TRANS want=$N（重启期间重复投递未被吸收）"

echo "T-011 DRILL PASS: 断网缓冲 ${N}/${N} 补传、水位可见、worker 重启零丢失零重复"
