#!/usr/bin/env bash
# MVP-001 (v0.3 E2E closed loop): DeepFlow 样本 -> 归一化 -> 通知(mock 渠道)
# -> 人工 ACK(监控状态不变) -> 恢复 -> Resolved -> 通知与审计全链路一条不缺。
# Plus NOTI-004 smoke: the embedded console serves.
set -euo pipefail
cd "$(dirname "$0")/.."

PG_DSN="${ALERT_TEST_PG_DSN:-postgres://postgres:postgres@127.0.0.1:55432/alert_platform?sslmode=disable}"
PG_CONTAINER="${ALERT_PG_CONTAINER:-dev-postgres-1}"
BROKERS="${ALERT_TEST_KAFKA_BROKERS:-127.0.0.1:19092}"
API_ADDR=":18099"
RUN_ID="$(date +%s)"
DB="alert_mvp_${RUN_ID}"
WORK=".dev/mvp-${RUN_ID}"
mkdir -p "$WORK"

TENANT="ffff0000-0000-0000-0000-0000000000aa"
SOURCE_ID="ffff0000-0000-0000-0000-0000000000bb"
SRC_TOKEN="aps_mvp_source_token_$(date +%s)"
USR_TOKEN="usr_mvp_admin_token_$(date +%s)"
TOPIC="alert.mvp.${RUN_ID}"

cleanup() {
  [ -n "${WORKER_PID:-}" ] && kill "$WORKER_PID" 2>/dev/null || true
  [ -n "${API_PID:-}" ] && kill "$API_PID" 2>/dev/null || true
  [ -n "${HOOK_PID:-}" ] && kill "$HOOK_PID" 2>/dev/null || true
  rm -rf "$WORK"
  docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null 2>&1 || true
}
trap cleanup EXIT

step() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
psql_q() { docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qtAc "$1"; }
DSN_BASE="${PG_DSN%%\?*}"; TARGET_DSN="${DSN_BASE%/*}/${DB}?sslmode=disable"

step "mock 渠道接收器 (captures every delivery)"
cat > "$WORK/mock_hook.py" <<'PYEOF'
import http.server, json, sys, threading
work = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        with open(f"{work}/deliveries.jsonl", "a") as f:
            f.write(json.dumps({"at": __import__("time").time(), "body": body.decode()}) + "\n")
        self.send_response(200); self.end_headers(); self.wfile.write(b'{"code":0}')
    def log_message(self, *a): pass
srv = http.server.HTTPServer(("127.0.0.1", 18199), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
import signal; signal.pause()
PYEOF
python3 "$WORK/mock_hook.py" "$WORK" &
HOOK_PID=$!
sleep 0.5

step "一次性数据库 + schema + 平台令牌 + 来源凭证"
docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null
docker exec "$PG_CONTAINER" psql -U postgres -qc "CREATE DATABASE ${DB}" >/dev/null
go run ./cmd/alert-migrate -dsn "$TARGET_DSN" >/dev/null
SRC_HASH=$(python3 -c "import hashlib;print(hashlib.sha256('$SRC_TOKEN'.encode()).hexdigest())")
USR_HASH=$(python3 -c "import hashlib;print(hashlib.sha256(('platform-token/v1|'+'''$USR_TOKEN''').encode()).hexdigest())")
docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qc "
  INSERT INTO alert_sources (tenant_id, source_id, name, source_type, vendor_version, adapter_version, cluster_id, last_seen_at, health_status)
  VALUES ('$TENANT','$SOURCE_ID','df-mvp','deepflow','7.2.3','x','prod-studio', now(), 'ok');
  INSERT INTO source_credentials (tenant_id, source_id, token_hash) VALUES ('$TENANT','$SOURCE_ID','$SRC_HASH');
  INSERT INTO platform_tokens (tenant_id, actor_id, role, token_hash) VALUES ('$TENANT','mvp-admin','platform_admin','$USR_HASH');
" >/dev/null

docker exec dev-kafka-1 kafka-topics --bootstrap-server localhost:29092 --create --topic "$TOPIC" --partitions 1 >/dev/null 2>&1 || true

step "api + worker（允许私有渠道端点：本机 mock）"
go build -o "$WORK/api" ./cmd/alert-api
go build -o "$WORK/worker" ./cmd/alert-worker
ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_API_ADDR="$API_ADDR" \
  ALERT_KAFKA_TOPIC="$TOPIC" ALERT_ALLOW_PRIVATE_CHANNELS=true LOG_LEVEL=warn "$WORK/api" > "$WORK/api.log" 2>&1 & API_PID=$!
ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_KAFKA_TOPIC="$TOPIC" \
  ALERT_KAFKA_GROUP="mvp-${RUN_ID}" ALERT_WORKER_ADDR=":18198" \
  ALERT_ALLOW_PRIVATE_CHANNELS=true ALERT_STALE_AFTER=1h LOG_LEVEL=warn "$WORK/worker" > "$WORK/worker.log" 2>&1 & WORKER_PID=$!
for i in $(seq 1 50); do curl -sf "http://127.0.0.1${API_ADDR}/healthz" >/dev/null 2>&1 && break; sleep 0.2; done
curl -sf "http://127.0.0.1${API_ADDR}/healthz" >/dev/null || fail "api unhealthy"

step "NOTI-004 冒烟：内嵌控制台可服务"
curl -sf "http://127.0.0.1${API_ADDR}/console" | grep -q 'alert-platform' || fail "console html missing"
curl -sf "http://127.0.0.1${API_ADDR}/console/app.js" >/dev/null || fail "console js missing"

step "创建渠道（webhook -> 本机 mock）"
CH_RESP=$(curl -s -w '\n%{http_code}' -X POST "http://127.0.0.1${API_ADDR}/channels/v1" \
  -H "Authorization: Bearer $USR_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"name\":\"mock-hook\",\"type\":\"webhook\",\"webhook_url\":\"http://127.0.0.1:18199/hook\",\"is_default\":true}")
CH_CODE=$(echo "$CH_RESP" | tail -1)
case "$CH_CODE" in 200|201) ;; *) fail "channel create -> $CH_CODE: $CH_RESP";; esac

step "① DeepFlow firing 样本 -> 归一化 -> 202"
cat > "$WORK/firing.json" <<EOF
{"schema_version":"deepflow.bridge.v1","source_id":"$SOURCE_ID","source_version":"7.2.3",
 "external_event_id":"mvp-evt-1","event_state":0,"policy_id":"mvp-policy-1","event_level":1,
 "alert_name":"MVP Latency Critical","occurred_at":"$(date -u +%Y-%m-%dT%H:%M:%SZ)",
 "target_key":"pod-mvp-1","cluster_id":"prod-studio","labels":{"ns":"mvp"},"tenant_id":"forged-tenant"}
EOF
curl -sf -X POST "http://127.0.0.1${API_ADDR}/ingest/external/deepflow/v7_2/events" \
  -H "Authorization: Bearer $SRC_TOKEN" -H 'Content-Type: application/json' \
  --data-binary @"$WORK/firing.json" | grep -q durable_batch_id || fail "firing push rejected"

step "② 等 firing 通知到达 mock 渠道（含 policy_version）"
for i in $(seq 1 60); do [ -f "$WORK/deliveries.jsonl" ] && grep -q 'firing' "$WORK/deliveries.jsonl" && break; sleep 0.5; done
[ -f "$WORK/deliveries.jsonl" ] && grep -q 'firing' "$WORK/deliveries.jsonl" || fail "firing notification never delivered"
grep -q 'policy_version=' "$WORK/deliveries.jsonl" || fail "message missing policy_version"

step "③ 人工 ACK：expected_version 正确；监控状态不变"
INC_JSON=$(curl -sf "http://127.0.0.1${API_ADDR}/incidents/v1" -H "Authorization: Bearer $USR_TOKEN")
# responses use the unified envelope {code,data,message}; protobuf field names
# are snake_case over the wire (docs/07).
INC_ID=$(echo "$INC_JSON" | python3 -c "import json,sys;d=json.load(sys.stdin)['data']['incidents'];print(d[0]['incident_id'])")
INC_VER=$(echo "$INC_JSON" | python3 -c "import json,sys;d=json.load(sys.stdin)['data']['incidents'];print(d[0]['version'])")
ACK_RESP=$(curl -s -w '\n%{http_code}' -X POST "http://127.0.0.1${API_ADDR}/incidents/v1/${INC_ID}/acknowledge" \
  -H "Authorization: Bearer $USR_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"incident_id\":\"${INC_ID}\",\"idempotency_key\":\"mvp-ack-1\",\"expected_version\":${INC_VER}}")
ACK_CODE=$(echo "$ACK_RESP" | tail -1)
case "$ACK_CODE" in 200|201) ;; *) fail "ack -> $ACK_CODE: $ACK_RESP";; esac
MON=$(psql_q "SELECT monitor_state FROM alert_instances LIMIT 1")
[ "$MON" = "firing" ] || fail "ACK changed monitor state: $MON"
# wrong version -> 409
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1${API_ADDR}/incidents/v1/${INC_ID}/acknowledge" \
  -H "Authorization: Bearer $USR_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"incident_id\":\"${INC_ID}\",\"idempotency_key\":\"mvp-ack-2\",\"expected_version\":1}")
[ "$CODE" = "409" ] || fail "stale expected_version returned $CODE, want 409"

step "④ 恢复样本（state=1, level=5, 同 external_event_id）-> Resolved"
cat > "$WORK/resolved.json" <<EOF
{"schema_version":"deepflow.bridge.v1","source_id":"$SOURCE_ID","source_version":"7.2.3",
 "external_event_id":"mvp-evt-1","event_state":1,"policy_id":"mvp-policy-1","event_level":5,
 "alert_name":"MVP Latency Critical","occurred_at":"$(date -u +%Y-%m-%dT%H:%M:%SZ)",
 "target_key":"pod-mvp-1","cluster_id":"prod-studio","labels":{"ns":"mvp"}}
EOF
curl -sf -X POST "http://127.0.0.1${API_ADDR}/ingest/external/deepflow/v7_2/events" \
  -H "Authorization: Bearer $SRC_TOKEN" -H 'Content-Type: application/json' \
  --data-binary @"$WORK/resolved.json" >/dev/null || fail "recovery push rejected"

for i in $(seq 1 60); do grep -q 'resolved' "$WORK/deliveries.jsonl" 2>/dev/null && break; sleep 0.5; done
grep -q 'resolved' "$WORK/deliveries.jsonl" || fail "resolved notification never delivered"
MON=$(psql_q "SELECT monitor_state FROM alert_instances LIMIT 1")
[ "$MON" = "resolved" ] || fail "monitor state = $MON, want resolved"
INC_ST=$(psql_q "SELECT status FROM incidents LIMIT 1")
[ "$INC_ST" = "acknowledged" ] || fail "recovery auto-closed incident: $INC_ST"

step "⑤ 通知审计与操作审计齐全"
ATTEMPTS=$(psql_q "SELECT count(*) FROM notification_attempts")
[ "$ATTEMPTS" -ge 2 ] || fail "notification attempts = $ATTEMPTS, want >=2"
SENT=$(psql_q "SELECT count(*) FROM notification_outbox WHERE status='sent'")
[ "$SENT" -ge 2 ] || fail "sent notifications = $SENT, want >=2"
ACK_AUDIT=$(psql_q "SELECT count(*) FROM audit_logs WHERE operation='incident.ack'")
[ "$ACK_AUDIT" -ge 1 ] || fail "ack missing from audit"
CH_AUDIT=$(psql_q "SELECT count(*) FROM audit_logs WHERE operation='channel.create'")
[ "$CH_AUDIT" -ge 1 ] || fail "channel.create missing from audit"
TENANT_CHECK=$(psql_q "SELECT count(*) FROM alert_instances WHERE tenant_id <> '$TENANT'")
[ "$TENANT_CHECK" = "0" ] || fail "forged tenant leaked into instances"

echo
echo "MVP-001 E2E PASS: push->notify->ack->recover->resolved->audit 全链路闭环"
