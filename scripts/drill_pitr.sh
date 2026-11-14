#!/usr/bin/env bash
# CHAOS-002: PITR to an arbitrary point in time. Single container, TWO
# postgres instances (primary :5432, restored clone :5433) — everything runs
# as the postgres user inside, no volumes/cp/permission complications.
# Timeline: base backup -> phase A rows -> target ts -> phase B rows ->
#           crash (immediate stop) -> clone restores from base+archive to ts
#           -> assert phase A present, phase B absent.
set -euo pipefail
cd "$(dirname "$0")/.."
NAME="alert-pitr-$$"
step() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
cleanup() {
  docker rm -f "$NAME" "$NAME-restore" >/dev/null 2>&1 || true
  docker volume rm "$VOL_ARCH" "$VOL_CLONE" >/dev/null 2>&1 || true
}
[ "${DRILL_KEEP:-0}" = "1" ] || trap cleanup EXIT

VOL_ARCH="${NAME}-arch"; VOL_CLONE="${NAME}-clone"
docker volume create "$VOL_ARCH" >/dev/null; docker volume create "$VOL_CLONE" >/dev/null
step "throwaway PG 16 with WAL archiving (archive+clone on shared volumes)"
docker run -d --name "$NAME" -e POSTGRES_PASSWORD=pit -p 16543:5432 \
  -v "$VOL_ARCH:/archive" -v "$VOL_CLONE:/clone" \
  postgres:16-alpine \
  -c wal_level=replica -c archive_mode=on \
  -c archive_command='test ! -f /archive/%f && cp %p /archive/%f' >/dev/null
for i in $(seq 1 60); do
  docker exec "$NAME" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 0.5
done
docker exec -u root "$NAME" sh -c "chown postgres:postgres /archive /clone"
# the archiver retries; force it to pick up archived segments promptly
docker exec "$NAME" psql -U postgres -c "SELECT pg_switch_wal()" >/dev/null
sleep 2

PSQL="docker exec $NAME psql -U postgres -qtAc"
step "schema + base backup"
$PSQL "CREATE DATABASE app" >/dev/null
docker exec "$NAME" psql -U postgres -d app -qc "CREATE TABLE facts(id serial primary key, note text, at timestamptz default now())" >/dev/null
docker exec -u postgres "$NAME" pg_basebackup -U postgres -D /clone -Fp -Xs -P >/dev/null 2>&1 || fail "base backup"

step "phase A rows, then the recovery target timestamp"
docker exec "$NAME" psql -U postgres -c "SELECT pg_switch_wal()" >/dev/null; sleep 2
docker exec "$NAME" psql -U postgres -d app -qc "INSERT INTO facts(note) SELECT 'phaseA-'||g FROM generate_series(1,10) g" >/dev/null
docker exec "$NAME" psql -U postgres -d app -qc "CHECKPOINT" >/dev/null
sleep 5
TS=$($PSQL "SELECT clock_timestamp()")
sleep 5
step "phase B rows (target: $TS)"
docker exec "$NAME" psql -U postgres -c "SELECT pg_switch_wal()" >/dev/null; sleep 2
docker exec "$NAME" psql -U postgres -d app -qc "INSERT INTO facts(note) SELECT 'phaseB-'||g FROM generate_series(1,10) g" >/dev/null
docker exec "$NAME" psql -U postgres -c "SELECT pg_switch_wal()" >/dev/null
sleep 5   # let the archiver land the segments carrying phase A and B

step "crash the primary; restore the clone to the target time"
# recovery config goes in BEFORE the crash (the clone volume survives it).
docker exec "$NAME" sh -c "cat >> /clone/postgresql.auto.conf <<CONF
port = 5433
restore_command = 'cp /archive/%f %p'
recovery_target_time = '${TS}'
recovery_target_action = 'promote'
CONF"
# PG12+: archive recovery (and thus recovery_target_*) only engages when the
# recovery.signal marker exists — crash recovery alone ignores them.
docker exec -u postgres "$NAME" touch /clone/recovery.signal
# PID 1 is postgres: stopping it ends the container — the outcome check is
# "container exited", then the clone container takes over the volumes.
docker exec -u postgres "$NAME" sh -c 'pg_ctl -D "$PGDATA" -m immediate stop' >/dev/null 2>&1 || true
sleep 1
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME-restore" -e POSTGRES_PASSWORD=pit -e PGDATA=/clone \
  -v "$VOL_CLONE:/clone" -v "$VOL_ARCH:/archive" -p 16544:5433 \
  postgres:16-alpine -c port=5433 -c archive_mode=off >/dev/null || fail "restore start"
for i in $(seq 1 90); do
  docker exec "$NAME-restore" pg_isready -U postgres -p 5433 >/dev/null 2>&1 && break; sleep 0.5
done

step "assert point-in-time semantics"
A=$(docker exec "$NAME-restore" psql -U postgres -p 5433 -d app -qtAc "SELECT count(*) FROM facts WHERE note LIKE 'phaseA-%'")
B=$(docker exec "$NAME-restore" psql -U postgres -p 5433 -d app -qtAc "SELECT count(*) FROM facts WHERE note LIKE 'phaseB-%'")
if [ "$A" != "10" ]; then
  docker logs "$NAME-restore" 2>&1 | grep -iE 'restored log|recovery|target|redo|cp:' | tail -12 || true
  fail "phase A rows = $A, want 10 (committed data lost)"
fi
[ "$B" = "0" ] || fail "phase B rows = $B, want 0 (replayed past the target time)"
echo "PITR DRILL PASS: 恢复到 ${TS}——A 段 10/10 在、B 段 0 在（任意时间点恢复语义成立）"
