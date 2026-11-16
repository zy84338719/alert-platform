-- 0013 (R27): deliverability by CREATION MODE, not by era id. P8-P0-01 scoped
-- claims to the current authority generation, which stranded ACTIVE-era tasks
-- whenever authority oscillated (active→shadow→active): the REL-002 drill
-- phase 4 requires those tasks to deliver exactly once after the return.
--
-- created_mode records the authority mode at enqueue time:
--   'active'  — deliverable whenever the notifier runs in active mode
--   'shadow'  — a shadow-era observation, NEVER auto-replayed
--   'off'     — created while delivery was intentionally off, never claimed
--   ''        — pre-ledger rows (before 0009): never claimed (conservative)
--
-- authority_generation stays as an audit/metrics era marker only.
BEGIN;

ALTER TABLE notification_outbox
    ADD COLUMN IF NOT EXISTS created_mode TEXT NOT NULL DEFAULT '';

COMMIT;