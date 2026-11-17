-- 0015 (AUD-002/AUD-012): notification lifecycle guarantees.
--
-- 1) Shadow delivery audit becomes idempotent at the DATABASE level: two
--    shadow replicas racing on the same task could previously both pass the
--    check-then-insert and write duplicate would-have-sent rows. A partial
--    unique index makes INSERT ... ON CONFLICT DO NOTHING the dedup ledger.
-- 2) Terminal-state support needs no new columns: attempts + status already
--    exist; max-effort dead-lettering is enforced in FinishNotifyTask.
BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS uq_audit_shadow_task
    ON audit_logs ((details->>'shadow_task'))
    WHERE operation = 'notify.shadow_would_send';

COMMIT;