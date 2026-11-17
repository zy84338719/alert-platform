-- 0014 (AUD-001/002/010): transactional archive queue + component leases.
--
-- WHY: the old archiver scanned transitions by EVENT time with a high-water
-- cursor. Late/out-of-order/edge-replay rows (transition_at older than the
-- cursor) were never scanned again — permanently lost from the archive. A
-- cursor over event time cannot be made safe without commit-order semantics.
--
-- NEW MODEL: every transition enqueues ONE queue row inside the SAME PG
-- transaction that inserts the transition (ApplyEvent / reconcile). The
-- archiver claims queue rows (SKIP LOCKED), writes ClickHouse idempotently
-- (ReplacingMergeTree on transition_id order key), then marks them done. Late
-- arrivals simply create their queue row when they land.
--
-- component_leases is the generic single-writer lease used by archive (and by
-- retention/telemetry/relay in the Worker Profile work) so N worker replicas
-- do not run N copies of the same singleton loop.
BEGIN;

CREATE TABLE IF NOT EXISTS transition_archive_queue (
    transition_id UUID PRIMARY KEY,
    tenant_id     UUID NOT NULL,
    instance_id   UUID NOT NULL,
    from_state    TEXT NOT NULL DEFAULT '',
    to_state      TEXT NOT NULL,
    event_id      TEXT NOT NULL DEFAULT '',
    transition_at TIMESTAMPTZ NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    attempts      INT NOT NULL DEFAULT 0,
    lease_until   TIMESTAMPTZ,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT archive_queue_status_ck CHECK (status IN ('pending','inflight','done','failed'))
);
CREATE INDEX IF NOT EXISTS idx_archive_queue_claim
    ON transition_archive_queue (status, transition_at)
    WHERE status IN ('pending','inflight');
CREATE INDEX IF NOT EXISTS idx_archive_queue_failed
    ON transition_archive_queue (updated_at DESC)
    WHERE status = 'failed';

-- AUD-010: history range queries scan by (tenant, time) and had no index;
-- archive scans by event time had no index either.
CREATE INDEX IF NOT EXISTS idx_transitions_tenant_time
    ON alert_transitions (tenant_id, transition_at DESC);

CREATE TABLE IF NOT EXISTS component_leases (
    name       TEXT PRIMARY KEY,
    owner      UUID NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One-time backfill: existing transitions become pending archive rows.
-- Large tables: run this migration off-peak (the archiver drains at a
-- configurable rate).
INSERT INTO transition_archive_queue
    (transition_id, tenant_id, instance_id, from_state, to_state, event_id, transition_at)
SELECT transition_id, tenant_id, alert_instance_id, COALESCE(from_state,''), to_state, event_id, transition_at
FROM alert_transitions
ON CONFLICT (transition_id) DO NOTHING;

-- The event-time cursor is superseded by the queue; keep no dead state.
DROP TABLE IF EXISTS archive_cursors;

COMMIT;