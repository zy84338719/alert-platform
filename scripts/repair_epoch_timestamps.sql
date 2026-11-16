-- Repair rows written before the R25 timestamp fix: a missing protobuf
-- occurred_at/received_at was stored as 1970-01-01 (nil Timestamp.AsTime())
-- instead of "now". These rows also poison the ClickHouse archive (partition
-- 197001 is TTL-deleted immediately), so repair PG before the next archive run.
--
-- Usage (review the detection output first, then run the repair):
--   psql "$DSN" -f scripts/repair_epoch_timestamps.sql
--
-- The true time is unrecoverable; created_at/received_at are the best available
-- proxies (row insert time). Rows are only touched when the suspicious column
-- is before 2000-01-01 — no legitimate alert timestamp is that old.

\echo '--- detection (before) ---'
SELECT 'alert_transitions.transition_at' AS column, count(*)
FROM alert_transitions WHERE transition_at < '2000-01-01'
UNION ALL
SELECT 'alert_instances.starts_at', count(*) FROM alert_instances WHERE starts_at < '2000-01-01'
UNION ALL
SELECT 'alert_instances.last_observed_at', count(*) FROM alert_instances WHERE last_observed_at < '2000-01-01'
UNION ALL
SELECT 'alert_instances.last_transition_at', count(*) FROM alert_instances WHERE last_transition_at < '2000-01-01'
UNION ALL
SELECT 'event_history.occurred_at', count(*) FROM event_history WHERE occurred_at < '2000-01-01';

BEGIN;

UPDATE alert_transitions
SET transition_at = created_at
WHERE transition_at < '2000-01-01';

UPDATE alert_instances
SET starts_at = CASE WHEN starts_at < '2000-01-01' THEN created_at ELSE starts_at END,
    last_observed_at = CASE WHEN last_observed_at < '2000-01-01' THEN created_at ELSE last_observed_at END,
    last_transition_at = CASE WHEN last_transition_at < '2000-01-01' THEN created_at ELSE last_transition_at END
WHERE starts_at < '2000-01-01'
   OR last_observed_at < '2000-01-01'
   OR last_transition_at < '2000-01-01';

UPDATE event_history
SET occurred_at = received_at
WHERE occurred_at < '2000-01-01';

COMMIT;

\echo '--- detection (after) ---'
SELECT 'alert_transitions.transition_at' AS column, count(*)
FROM alert_transitions WHERE transition_at < '2000-01-01'
UNION ALL
SELECT 'alert_instances.starts_at', count(*) FROM alert_instances WHERE starts_at < '2000-01-01'
UNION ALL
SELECT 'alert_instances.last_observed_at', count(*) FROM alert_instances WHERE last_observed_at < '2000-01-01'
UNION ALL
SELECT 'alert_instances.last_transition_at', count(*) FROM alert_instances WHERE last_transition_at < '2000-01-01'
UNION ALL
SELECT 'event_history.occurred_at', count(*) FROM event_history WHERE occurred_at < '2000-01-01';