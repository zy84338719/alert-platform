-- 0005 (MC-002): edge buffer waterline reporting. Edges heartbeat their WAL
-- state to the center; the console surfaces LastSeen/Buffer alongside
-- alert_sources.health_status.
BEGIN;

CREATE TABLE IF NOT EXISTS edge_status (
    source_id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    wal_bytes BIGINT NOT NULL DEFAULT 0,
    wal_pending BIGINT NOT NULL DEFAULT 0,
    generation BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
