-- 0015 down: drop the shadow-audit unique index.
BEGIN;

DROP INDEX IF EXISTS uq_audit_shadow_task;

COMMIT;