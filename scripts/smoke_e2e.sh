#!/usr/bin/env bash
# BASE-007/008 acceptance smoke: real processes + resident dependencies.
# 1) dev deps (compose) healthy; 2) migrate; 3) start api/worker/edge;
# 4) POST one canonical event with X-Trace-Id -> expect 202+durable_batch_id;
# 5) worker consumes -> PG has receipt+instance+transition;
# 6) same trace_id in api AND worker logs (Kafka header propagation);
# 7) retry the same batch_id -> receipts stay at 1 (transport idempotency).
#
# Prereq: make dev-deps (compose up). Override with env:
#   SMOKE_PG_DSN (default dev compose DSN), SMOKE_KAFKA (default 127.0.0.1:19092)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PG_DSN="${SMOKE_PG_DSN:-postgres://postgres:postgres@127.0.0.1:55432/alert_platform?sslmode=disable}"
KAFKA="${SMOKE_KAFKA:-127.0.0.1:19092}"
LOG=".dev/logs"; PIDF=".dev/pids"; BIN=".dev/bin"
mkdir -p "$LOG" "$PIDF" "$BIN"

log() { printf '[smoke] %s\n' "$*"; }
die() { printf '[smoke] FAIL: %s\n' "$*" >&2; exit 1; }
psql() { docker exec dev-postgres-1 psql -U postgres -d alert_platform -tAc "$1"; }

log "step 1/7: dependencies health (compose)"
docker compose -f deploy/dev/docker-compose.yaml ps --status running | grep -q postgres || die "dev postgres not running (make dev-deps)"
docker compose -f deploy/dev/docker-compose.yaml ps --status running | grep -q kafka || die "dev kafka not running (make dev-deps)"

# P7/R20: the consumer quarantines contract-violating events into the DLQ and
# the relay publishes lifecycle events — both topics must exist locally
# (compose Kafka has auto-create disabled).
KAFKA_CONTAINER="${SMOKE_KAFKA_CONTAINER:-dev-kafka-1}"
for t in alert.events alert.dlq.v1 alert.lifecycle.v1; do
  docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:29092 \
    --create --if-not-exists --topic "$t" --partitions 3 >/dev/null 2>&1 || true
done

log "step 2/7: migrate"
GOWORK=off go run ./cmd/alert-migrate -dsn "$PG_DSN" || die "migrate failed"

# P7-P0-03: /ingest/v1 is credential-gated; register the smoke source so the
# canonical POST exercises the REAL authorized path instead of 401.
SMOKE_TENANT="11111111-1111-1111-1111-111111111111"
SMOKE_SRC="22222222-2222-2222-2222-222222222222"
SMOKE_TOKEN="aps_smoke_$(date +%s)"
psql "INSERT INTO alert_sources (tenant_id, source_id, name, source_type, vendor_version, adapter_version, cluster_id, enabled, health_status)
      VALUES ('$SMOKE_TENANT','$SMOKE_SRC','smoke','deepflow','7.2.4','x','c1',true,'unknown') ON CONFLICT DO NOTHING" >/dev/null
psql "INSERT INTO source_credentials (tenant_id, source_id, token_hash)
      VALUES ('$SMOKE_TENANT','$SMOKE_SRC', encode(sha256('$SMOKE_TOKEN'::bytea),'hex')) ON CONFLICT DO NOTHING" >/dev/null

bash scripts/dev_up.sh down >/dev/null 2>&1 || true
for n in alert-api alert-worker alert-edge; do
  GOWORK=off go build -o "$BIN/$n" "./cmd/$n"
done
TID="smoke-$(date +%s)"
log "step 3/7: start binaries (trace=$TID)"
ALERT_KAFKA_BROKERS="$KAFKA" "$BIN/alert-api" -config etc/config.yaml >"$LOG/alert-api.log" 2>&1 &
echo $! >"$PIDF/alert-api.pid"
ALERT_KAFKA_BROKERS="$KAFKA" ALERT_PG_DSN="$PG_DSN" "$BIN/alert-worker" -config etc/config.yaml >"$LOG/alert-worker.log" 2>&1 &
echo $! >"$PIDF/alert-worker.pid"
"$BIN/alert-edge" -config etc/config.yaml >"$LOG/alert-edge.log" 2>&1 &
echo $! >"$PIDF/alert-edge.pid"
sleep 3
for port in 8080 8081 8082; do
  code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/healthz" || echo 000)"
  [ "$code" = "200" ] || die "healthz on :$port = $code"
done
log "healthz 200 on api/worker/edge"

log "step 4/7: POST /ingest/v1/events:batch"
TENANT="$SMOKE_TENANT"
SRC="$SMOKE_SRC"
BODY='{"batch_id":"smoke-batch-1","events":[{"tenant_id":"'$TENANT'","source":{"source_id":"'$SRC'","source_type":1,"external_rule_id":"dp-rule-7"},"resource":{"cluster_id":"c1","resource_type":"pod","resource_uid":"uid-smoke-1"},"kind":1,"signal":1,"severity":1,"alert_name":"smoke-cpu-high","event_key":"smoke-ek-1"}]}'
resp="$(curl -s -X POST http://127.0.0.1:8080/ingest/v1/events:batch \
  -H 'Content-Type: application/json' -H "X-Trace-Id: $TID" -H "Authorization: Bearer $SMOKE_TOKEN" -d "$BODY")"
echo "$resp" | grep -q '"durable_batch_id":"smoke-batch-1"' || die "expected 202 envelope with durable_batch_id, got: $resp"
log "202 accepted (durable_batch_id=smoke-batch-1)"

log "step 5/7: worker consumption -> PG"
sleep 4
[ "$(psql "SELECT count(*) FROM event_receipts WHERE transport_id='smoke-batch-1:0'")" = "1" ] || die "receipt missing"
state="$(psql "SELECT i.monitor_state FROM alert_instances i WHERE i.name='smoke-cpu-high'")"
[ "$state" = "firing" ] || die "instance state = $state, want firing"
[ "$(psql "SELECT count(*) FROM alert_transitions t JOIN alert_instances i ON i.id=t.alert_instance_id WHERE i.name='smoke-cpu-high'")" = "1" ] || die "transition missing"
log "PG: receipt + firing instance + transition present"

log "step 6/7: trace_id correlation api<->worker (Kafka header propagation)"
api_hits="$(grep -c "$TID" "$LOG/alert-api.log" || true)"
worker_hits="$(grep -c "$TID" "$LOG/alert-worker.log" || true)"
[ "${api_hits:-0}" -gt 0 ] || die "trace_id missing in api log"
[ "${worker_hits:-0}" -gt 0 ] || die "trace_id missing in worker log (Kafka header propagation broken)"
log "trace_id present in api($api_hits) and worker($worker_hits) logs"

log "step 7/7: duplicate batch retry is transport-idempotent"
curl -s -X POST http://127.0.0.1:8080/ingest/v1/events:batch \
  -H 'Content-Type: application/json' -H "X-Trace-Id: $TID" -H "Authorization: Bearer $SMOKE_TOKEN" -d "$BODY" >/dev/null
sleep 3
[ "$(psql "SELECT count(*) FROM event_receipts WHERE transport_id='smoke-batch-1:0'")" = "1" ] || die "duplicate batch created extra receipts"
log "retry kept receipts at 1 (deterministic transport_id)"

log "SMOKE PASS: API 202 -> Kafka -> worker -> PG(firing) with trace correlation and idempotent retry"
