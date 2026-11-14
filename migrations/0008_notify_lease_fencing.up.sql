-- 0008 (P7 · P1-02): notification lease fencing. Every claim mints a unique
-- token; completion updates are conditional on it, so a stale worker whose
-- lease expired (task re-claimed elsewhere) can never overwrite the current
-- owner's delivery state — its late result lands in notification_attempts
-- only (auditable, non-authoritative).
BEGIN;

ALTER TABLE notification_outbox ADD COLUMN IF NOT EXISTS lease_token UUID;

COMMIT;
