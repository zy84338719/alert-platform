-- 0013 down: drop the creation-mode stamp.
BEGIN;

ALTER TABLE notification_outbox DROP COLUMN IF EXISTS created_mode;

COMMIT;