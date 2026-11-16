-- 0012 (P9 · CH-01): archive cursor for the PG -> ClickHouse transition
-- archiver. The cursor lives OUTSIDE the hot tables: archiving reads committed
-- rows by (transition_at, transition_id) and advances after a successful CH
-- insert, so a CH outage only pauses archiving — alerting never blocks.
BEGIN;

CREATE TABLE IF NOT EXISTS archive_cursors (
    name       TEXT PRIMARY KEY,
    last_at    TIMESTAMPTZ NOT NULL,
    last_id    UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;