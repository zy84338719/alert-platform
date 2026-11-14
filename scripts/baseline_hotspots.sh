#!/usr/bin/env bash
# BUF-002: quantify the three P5 hotspot baselines — Kafka consumer lag,
# PG lock waits and process RSS — so later optimization rounds have numbers.
# Produces a timestamped report on stdout (archive under docs/evidence/).
set -euo pipefail
cd "$(dirname "$0")/.."
echo "hotspot baseline @ $(date -u +%FT%TZ)"
echo "--- kafka consumer lag"
if docker exec dev-kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 --list >/dev/null 2>&1; then
  docker exec dev-kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 --all-groups --describe 2>/dev/null | awk 'NR==1 || /alert/' | head -12 || echo "  (no groups)"
else
  echo "  (dev kafka unreachable)"
fi
echo "--- pg lock waits (waiting sessions)"
docker exec dev-postgres-1 psql -U postgres -qtAc "
  SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock';" 2>/dev/null || echo "  (pg unreachable)"
docker exec dev-postgres-1 psql -U postgres -qtAc "
  SELECT 'row_lock_waits='||sum(row_lock_waits) FROM pg_stat_user_tables WHERE relname='alert_instances';" 2>/dev/null || true
echo "--- process rss (alert-* binaries)"
ps -axo rss,comm | grep -E 'alert-(api|worker|edge)' | awk '{printf "  %.1f MiB %s\n", $1/1024, $2}' || echo "  (none running)"
