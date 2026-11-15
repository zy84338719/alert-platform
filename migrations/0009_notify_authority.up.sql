-- 0009 (P8 · P0-01): persistent notification authority. P7 kept the shadow→
-- active cutoff in process memory (shadowCutoff); a normal Kubernetes rollout
-- (env change + Pod restart) starts a fresh process in the default active mode,
-- so shadow-era pending tasks were replayed as a delivery backlog.
--
-- The authority row is the single source of truth for delivery ownership:
--   mode        — active | shadow | off (mirrors ALERT_NOTIFY_MODE)
--   generation  — monotonically increasing era id; bumped on every mode change
--                 and on first initialization after this migration
--   effective_at— when the current generation became authoritative
--   initialized — false only for the migration seed; the first SyncNotifyAuthority
--                 always opens a new generation (conservative: pre-authority
--                 pending tasks are never auto-replayed) and flips it to true.
--
-- notification_outbox.authority_generation stamps the era a task was enqueued
-- in; the claim only picks the CURRENT generation. Rows created before this
-- migration keep generation 0 and are never claimed — they predate the
-- authority ledger and may be shadow-era observations (no storm on upgrade).
BEGIN;

CREATE TABLE IF NOT EXISTS notification_authority (
    id           INT PRIMARY KEY DEFAULT 1,
    mode         TEXT NOT NULL DEFAULT 'active',
    generation   BIGINT NOT NULL DEFAULT 1,
    effective_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    initialized  BOOLEAN NOT NULL DEFAULT false,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT notification_authority_singleton_ck CHECK (id = 1),
    CONSTRAINT notification_authority_mode_ck CHECK (mode IN ('active','shadow','off'))
);

INSERT INTO notification_authority (id, mode, generation, initialized)
VALUES (1, 'active', 1, false)
ON CONFLICT (id) DO NOTHING;

ALTER TABLE notification_outbox
    ADD COLUMN IF NOT EXISTS authority_generation BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS notification_outbox_authority_generation_idx
    ON notification_outbox (authority_generation, status, next_attempt_at);

COMMIT;