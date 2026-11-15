-- 0011 (P8 · P1-05): one ACTIVE episode per alert identity. Two concurrent
-- Firing events with different transport ids could both miss the active row
-- (ReadCommitted) and insert separate episodes; the (tenant_id, fingerprint,
-- episode_key) unique constraint cannot stop that because each side mints its
-- own episode_key. ApplyEvent now takes a per-identity advisory lock; this
-- partial unique index is the database-level backstop.
--
-- Active = pending | firing | stale (resolved rows are history and may repeat).
BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_instance_active_fingerprint
    ON alert_instances (tenant_id, fingerprint)
    WHERE monitor_state IN ('pending','firing','stale');

COMMIT;