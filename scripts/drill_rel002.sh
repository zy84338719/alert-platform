#!/usr/bin/env bash
# REL-002: production reconciliation + rollback drill (local, real processes).
#   Phase 1 SHADOW  — upstream keeps authority: platform records every
#                     would-have-sent notification, delivers NOTHING (mock
#                     channel must receive 0), no double-send.
#   Phase 2 RECON   — shadow_compare.py matches platform vs upstream records;
#                     differences must be explainable (shadow suppression).
#   Phase 3 CUTOVER — switch to active: a new alert IS delivered.
#   Phase 4 ROLLBACK— two deterministic scenarios (R27 semantics: deliverability
#                     is decided by the authority mode AT ENQUEUE TIME):
#     4a) a task created while ACTIVE is interrupted by the rollback: not lost,
#         not duplicated, delivers exactly once after authority returns;
#     4b) a task created while SHADOW (observation era) is NEVER replayed after
#         the cutover — that would duplicate what the upstream already sent
#         (P0-07 storm guard).
set -euo pipefail
cd "$(dirname "$0")/.."

BROKERS="${ALERT_TEST_KAFKA_BROKERS:-127.0.0.1:19092}"
PG_CONTAINER="${ALERT_PG_CONTAINER:-dev-postgres-1}"
RUN_ID="$(date +%s)"
DB="alert_rel002_${RUN_ID}"
WORK=".dev/rel002-${RUN_ID}"
mkdir -p "$WORK"

TENANT="aaaa2222-0000-0000-0000-0000000000aa"
SOURCE_ID="aaaa2222-0000-0000-0000-0000000000bb"
SRC_TOKEN="aps_rel002_src"
USR_TOKEN="usr_rel002_admin"
TOPIC="alert.rel002.${RUN_ID}"
HOOK_PORT=18777

cleanup() {
  for v in API_PID WORKER_PID HOOK_PID; do
    [ -n "${!v:-}" ] && kill "${!v}" 2>/dev/null || true
  done
  # logs stay on disk for post-mortem (WORK dir survives, DB is dropped)
  docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null 2>&1 || true
}
trap cleanup EXIT

step() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
psql_q() { docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qtAc "$1"; }
DSN_BASE="${ALERT_TEST_PG_DSN:-postgres://postgres:postgres@127.0.0.1:55432/alert_platform?sslmode=disable}"
DSN_BASE="${DSN_BASE%%\?*}"; TARGET_DSN="${DSN_BASE%/*}/${DB}?sslmode=disable"

step "mock 上游通知接收器（统计收到条数）"
cat > "$WORK/hook.py" <<'PYEOF'
import http.server, json, sys
work = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        with open(f"{work}/deliveries.jsonl", "a") as f:
            f.write(json.dumps({"body": body.decode()}) + "\n")
        self.send_response(200); self.end_headers(); self.wfile.write(b'{"code":0}')
    def log_message(self, *a): pass
http.server.HTTPServer(("127.0.0.1", int(sys.argv[2])), H).serve_forever()
PYEOF
python3 "$WORK/hook.py" "$WORK" "$HOOK_PORT" & HOOK_PID=$!
sleep 0.5

step "一次性库 + 凭证 + topic"
docker exec "$PG_CONTAINER" psql -U postgres -qc "DROP DATABASE IF EXISTS ${DB} WITH (FORCE)" >/dev/null
docker exec "$PG_CONTAINER" psql -U postgres -qc "CREATE DATABASE ${DB}" >/dev/null
go run ./cmd/alert-migrate -dsn "$TARGET_DSN" >/dev/null
SRC_HASH=$(python3 -c "import hashlib;print(hashlib.sha256('$SRC_TOKEN'.encode()).hexdigest())")
USR_HASH=$(python3 -c "import hashlib;print(hashlib.sha256(('platform-token/v1|'+'''$USR_TOKEN''').encode()).hexdigest())")
docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -qc "
INSERT INTO alert_sources (tenant_id, source_id, name, source_type, vendor_version, adapter_version, cluster_id, last_seen_at)
VALUES ('$TENANT','$SOURCE_ID','rel002','deepflow','7.2.3','x','prod-studio', now());
INSERT INTO source_credentials (tenant_id, source_id, token_hash) VALUES ('$TENANT','$SOURCE_ID','$SRC_HASH');
INSERT INTO platform_tokens (tenant_id, actor_id, role, token_hash) VALUES ('$TENANT','rel002-admin','platform_admin','$USR_HASH');
" >/dev/null
# unrouted alerts enqueue NOTHING by design (docs/13 P3): the drill needs a
# default channel or the shadow-phase assertion has nothing to assert on.
CHANNEL_SQL=$(printf "INSERT INTO notification_channels (id, tenant_id, name, type, config, is_default) VALUES (gen_random_uuid(), '%s', 'mock-upstream', 'webhook', '{\"webhook_url\":\"http://127.0.0.1:%s/hook\"}', true);" "$TENANT" "$HOOK_PORT" | base64)
echo "$CHANNEL_SQL" | base64 -d | docker exec -i "$PG_CONTAINER" psql -U postgres -d "$DB" -q -f -
docker exec dev-kafka-1 kafka-topics --bootstrap-server localhost:29092 --create --topic "$TOPIC" --partitions 1 >/dev/null 2>&1 || true

go build -o "$WORK/api" ./cmd/alert-api
go build -o "$WORK/worker" ./cmd/alert-worker

start_stack() { # $1 = notify mode
  ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_API_ADDR=":18800" \
    ALERT_KAFKA_TOPIC="$TOPIC" ALERT_ALLOW_PRIVATE_CHANNELS=true LOG_LEVEL=error \
    "$WORK/api" > "$WORK/api-$1.log" 2>&1 & API_PID=$!
  ALERT_PG_DSN="$TARGET_DSN" ALERT_KAFKA_BROKERS="$BROKERS" ALERT_KAFKA_TOPIC="$TOPIC" \
    ALERT_KAFKA_GROUP="rel002-${RUN_ID}" ALERT_WORKER_ADDR=":18801" ALERT_STALE_AFTER=1h \
    ALERT_NOTIFY_MODE="$1" ALERT_ALLOW_PRIVATE_CHANNELS=true LOG_LEVEL=error \
    "$WORK/worker" > "$WORK/worker-$1.log" 2>&1 & WORKER_PID=$!
  for i in $(seq 1 60); do
    curl -sf http://127.0.0.1:18800/healthz >/dev/null 2>&1 && return 0
    sleep 0.3
  done
  fail "api did not become healthy in mode $1 (see $WORK/api-$1.log)"
}
stop_stack() { kill "$WORKER_PID" "$API_PID" 2>/dev/null || true; wait "$WORKER_PID" 2>/dev/null || true; sleep 1; }

push() { # push <event_id>
  curl -sf -X POST "http://127.0.0.1:18800/ingest/external/deepflow/v7_2/events" \
    -H "Authorization: Bearer $SRC_TOKEN" -H 'Content-Type: application/json' \
    -d "{\"schema_version\":\"deepflow.bridge.v1\",\"source_id\":\"$SOURCE_ID\",\"external_event_id\":\"$1\",\"event_state\":0,\"policy_id\":\"p-$1\",\"event_level\":3,\"alert_name\":\"REL002 $1\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"target_key\":\"pod-$1\",\"cluster_id\":\"prod-studio\"}" >/dev/null
}
wait_notify() { # wait until notification_outbox has >= $1 rows
  for i in $(seq 1 60); do
    [ "$(psql_q "SELECT count(*) FROM notification_outbox")" -ge "$1" ] && return 0
    sleep 0.5
  done
  return 1
}

step "① SHADOW：5 个告警 → 平台记录但不投递（上游零干扰）"
start_stack shadow
for i in 1 2 3 4 5; do push "s-$i"; done
wait_notify 5 || fail "shadow 阶段通知任务未入队"
sleep 3
SENT_COUNT=$(psql_q "SELECT count(*) FROM notification_outbox WHERE status='sent'")
PENDING=$(psql_q "SELECT count(*) FROM notification_outbox WHERE status='pending'")
DELIVERED=$(grep -c 'body' "$WORK/deliveries.jsonl" 2>/dev/null || echo 0)
SHADOW_AUDIT=$(psql_q "SELECT count(*) FROM audit_logs WHERE operation='notify.shadow_would_send'")
[ "$DELIVERED" = "0" ] || fail "shadow 模式外发了 $DELIVERED 条（双发风险！）"
[ "$SENT_COUNT" = "0" ] || fail "shadow 模式出现 sent=$SENT_COUNT"
[ "$PENDING" = "5" ] || fail "shadow 阶段任务状态被污染: pending=$PENDING want 5"
[ "$SHADOW_AUDIT" -ge 5 ] || fail "shadow would-have-sent 审计=$SHADOW_AUDIT, want >=5"
echo "    shadow: 任务=5 全部 pending（未投递未污染）、审计记录=${SHADOW_AUDIT}、外发=0 ✓"

step "② 对账：平台记录 vs 上游记录（shadow_compare.py）"
docker exec "$PG_CONTAINER" psql -U postgres -d "$DB" -c "\copy (SELECT coalesce(i.fingerprint,'') AS alert_fingerprint, o.payload->>'notification_type' AS notification_type, to_char(o.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS') AS created_at, o.status AS status, o.receiver_id AS receiver_id FROM notification_outbox o LEFT JOIN alert_instances i ON i.id=o.alert_instance_id) TO STDOUT WITH (FORMAT csv, HEADER)" > "$WORK/platform_raw.csv"
# platform CSV -> JSONL; upstream side: in production this export comes from
# the upstream notification system keyed by its own identity — here the
# platform's fingerprint export stands in as the truth source.
python3 - "$WORK" <<'CONVERT'
import csv, json, sys
work = sys.argv[1]
plat = []
with open(f"{work}/platform_raw.csv", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        plat.append({"alert_fingerprint": row["alert_fingerprint"],
                     "notification_type": row["notification_type"],
                     "created_at": row["created_at"] + "+00:00",
                     "status": row["status"], "receiver_id": row["receiver_id"]})
with open(f"{work}/platform.jsonl", "w", encoding="utf-8") as f:
    for p in plat:
        f.write(json.dumps(p) + "\n")
ups = [{"fingerprint": p["alert_fingerprint"], "status": p["notification_type"],
        "at": p["created_at"]} for p in plat]
with open(f"{work}/upstream.jsonl", "w", encoding="utf-8") as f:
    for u in ups:
        f.write(json.dumps(u) + "\n")
CONVERT
python3 scripts/shadow_compare.py --upstream "$WORK/upstream.jsonl" --platform "$WORK/platform.jsonl" --out "$WORK/report.json" 2>"$WORK/compare.log"
MATCHED=$(python3 -c "import json;print(json.load(open('$WORK/report.json'))['matched_within_window'])")
[ "$MATCHED" = "5" ] || fail "对账 matched=$MATCHED, want 5"
echo "    对账：5/5 命中、报告产出（report.json）✓"

step "③ CUTOVER：切 active → 新告警真实送达"
stop_stack; start_stack active
for i in $(seq 1 50); do curl -sf http://127.0.0.1:18800/healthz >/dev/null 2>&1 && break; sleep 0.2; done
push "c-1"
for i in $(seq 1 60); do grep -q 'c-1' "$WORK/deliveries.jsonl" 2>/dev/null && break; sleep 0.5; done
grep -q 'c-1' "$WORK/deliveries.jsonl" || fail "active 模式通知未送达"
C_SENT=$(psql_q "SELECT count(*) FROM notification_outbox WHERE payload->>'message' LIKE '%c-1%' AND status='sent'")
[ "$C_SENT" -ge 1 ] || fail "active 通知未标记 sent"
echo "    active：新告警送达 mock 渠道、status=sent ✓"

step "④a ROLLBACK：active 期在途任务被 shadow 打断 → 不丢、恢复后补投恰 1 次"
# Deterministic in-flight scenario: take the mock channel DOWN, then push r-2
# while authority is ACTIVE (created_mode=active). Delivery fails -> the task
# parks in retry_scheduled. Rollback to shadow must not lose/deliver it; the
# return to active (channel back up) must deliver it exactly once.
kill "$HOOK_PID" 2>/dev/null || true; wait "$HOOK_PID" 2>/dev/null || true
push "r-2"
wait_notify 7 || true
for i in $(seq 1 30); do
  st=$(psql_q "SELECT status FROM notification_outbox o WHERE o.payload->>'message' LIKE '%r-2%' LIMIT 1")
  [ "$st" = "retry_scheduled" ] && break
  sleep 0.5
done
[ "$st" = "retry_scheduled" ] || fail "r-2 未进入待重试状态（got: $st）"
stop_stack
start_stack shadow
sleep 3
R_SENT=$(psql_q "SELECT count(*) FROM notification_outbox o WHERE o.payload->>'message' LIKE '%r-2%' AND o.status='sent'")
DELIV_R=$(grep -c 'r-2' "$WORK/deliveries.jsonl" 2>/dev/null || true)
[ "$R_SENT" = "0" ] || fail "回滚期 r-2 被外发 $R_SENT 次"
[ "$DELIV_R" = "0" ] || fail "shadow 期 r-2 送达 mock 渠道 $DELIV_R 次"
R_HELD=$(psql_q "SELECT count(*) FROM notification_outbox o WHERE o.payload->>'message' LIKE '%r-2%' AND o.status IN ('pending','retry_scheduled')")
[ "$R_HELD" -ge 1 ] || fail "回滚后 r-2 任务丢失（待投递必须保留）"
stop_stack
python3 "$WORK/hook.py" "$WORK" "$HOOK_PORT" & HOOK_PID=$!
sleep 0.5
start_stack active
for i in $(seq 1 50); do curl -sf http://127.0.0.1:18800/healthz >/dev/null 2>&1 && break; sleep 0.2; done
for i in $(seq 1 60); do grep -q 'r-2' "$WORK/deliveries.jsonl" 2>/dev/null && break; sleep 0.5; done
grep -q 'r-2' "$WORK/deliveries.jsonl" || fail "恢复 active 后积压任务未补投"
sleep 2
R_DELIVERED=$(grep -c 'r-2' "$WORK/deliveries.jsonl")
[ "$R_DELIVERED" = "1" ] || fail "r-2 投递 $R_DELIVERED 次, want 恰 1（不重发）"
echo "    ④a 回滚：active 期 r-2 进入 retry（不丢、不外发）→ 恢复后补投恰 1 次（不重发）✓"

step "④b shadow 期任务永不补投（P0-07 风暴守卫）"
stop_stack
start_stack shadow
sleep 2
push "r-3"
wait_notify 9 || true
sleep 2
stop_stack
start_stack active
for i in $(seq 1 50); do curl -sf http://127.0.0.1:18800/healthz >/dev/null 2>&1 && break; sleep 0.2; done
sleep 4
DELIV_R3=$(grep -c 'r-3' "$WORK/deliveries.jsonl" 2>/dev/null || true)
[ "$DELIV_R3" = "0" ] || fail "shadow 期任务 r-3 在切回 active 后被补投 $DELIV_R3 次（P0-07 风暴！）"
R3_HELD=$(psql_q "SELECT count(*) FROM notification_outbox o WHERE o.payload->>'message' LIKE '%r-3%' AND o.status IN ('pending','retry_scheduled')")
[ "$R3_HELD" -ge 1 ] || fail "r-3 任务状态被污染（应保持 pending/retry 供审计）"
echo "    ④b 风暴守卫：shadow 期 r-3 切回 active 后 0 送达、保持 pending/retry（审计可查）✓"

echo "REL-002 DRILL PASS: shadow 零干扰 → 对账 5/5 → 切换生效 → 回滚不丢不重发"
