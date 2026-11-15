-- 0009 down: drop the authority ledger and the task generation stamp.
BEGIN;

DROP INDEX IF EXISTS notification_outbox_authority_generation_idx;
ALTER TABLE notification_outbox DROP COLUMN IF EXISTS authority_generation;
DROP TABLE IF EXISTS notification_authority;

COMMIT;