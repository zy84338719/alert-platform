BEGIN;
ALTER TABLE notification_outbox DROP COLUMN IF EXISTS lease_token;
COMMIT;
