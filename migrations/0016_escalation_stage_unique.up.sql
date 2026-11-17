-- 0016 (AUD-008): escalation/correlation multi-replica safety.
--
-- incident_escalations had only an id PK: two worker replicas scanning the
-- same candidate could both insert stage N (double page when the default
-- channel changed between their resolutions). Dedupe existing rows first
-- (keep the earliest notified_at per (incident_id, stage)) then enforce
-- UNIQUE (incident_id, stage).
BEGIN;

DELETE FROM incident_escalations a
USING incident_escalations b
WHERE a.incident_id = b.incident_id
  AND a.stage = b.stage
  AND a.ctid > b.ctid;

CREATE UNIQUE INDEX IF NOT EXISTS uq_incident_escalation_stage
    ON incident_escalations (incident_id, stage);

COMMIT;