-- 0011 down: drop the active-episode partial unique index.
BEGIN;

DROP INDEX IF EXISTS uq_alert_instance_active_fingerprint;

COMMIT;