-- 0007 (P7 · P0-05): event history — one row per processed canonical event
-- observation, so OCCURRENCE-class events (e.g. HUATUO kernel anomalies) are
-- queryable from the event history / incident workbench instead of leaving
-- only an invisible receipt. Stateful events are persisted too (audit of the
-- observation stream); monitor state stays authoritative in alert_instances.
BEGIN;

CREATE TABLE IF NOT EXISTS event_history (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    source_id UUID NOT NULL,
    event_id TEXT NOT NULL,
    transport_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    signal TEXT NOT NULL,
    severity TEXT,
    alert_name TEXT NOT NULL,
    cluster_id TEXT,
    entity_type TEXT,
    entity_uid TEXT,
    resource_name TEXT,
    namespace TEXT,
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    annotations JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_payload JSONB,
    occurred_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT event_history_kind_ck CHECK (kind IN ('occurrence','stateful')),
    CONSTRAINT uq_event_history_transport UNIQUE (tenant_id, source_id, transport_id)
);
CREATE INDEX IF NOT EXISTS idx_event_history_time
    ON event_history(tenant_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_event_history_entity
    ON event_history(tenant_id, entity_type, entity_uid, occurred_at DESC);

COMMIT;
