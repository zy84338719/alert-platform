-- 0014 down: restore the (superseded) archive cursor and drop the queue.
BEGIN;

DROP INDEX IF EXISTS idx_transitions_tenant_time;
DROP INDEX IF EXISTS idx_archive_queue_failed;
DROP INDEX IF EXISTS idx_archive_queue_claim;
DROP TABLE IF EXISTS transition_archive_queue;
DROP TABLE IF EXISTS component_leases;

CREATE TABLE IF NOT EXISTS archive_cursors (
    name       TEXT PRIMARY KEY,
    last_at    TIMESTAMPTZ NOT NULL,
    last_id    UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;