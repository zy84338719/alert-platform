-- 0016 down: drop the escalation stage uniqueness.
BEGIN;

DROP INDEX IF EXISTS uq_incident_escalation_stage;

COMMIT;