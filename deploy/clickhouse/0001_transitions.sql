-- P10-002 / AUD-013: transition archive schema.
--
-- Engine: ReplacingMergeTree(version=_inserted_at) — the archive writer is
-- at-least-once (crash between CH ack and the PG queue mark re-inserts the
-- batch), and duplicates collapse on the ORDER BY key. Queries read with
-- FINAL so unmerged duplicates are invisible.
--
-- TTL is on INSERT time (_inserted_at), NOT event time: backfilled/repair rows
-- with old transition_at must not evaporate on arrival (AUD-013).
--
-- Run inside the database selected by ALERT_CLICKHOUSE_DB (default "default").
CREATE TABLE IF NOT EXISTS alert_transitions
(
    tenant_id     UUID,
    transition_id UUID,
    instance_id   UUID,
    from_state    LowCardinality(String),
    to_state      LowCardinality(String),
    event_id      String,
    transition_at DateTime64(3, 'UTC'),
    _inserted_at  DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(_inserted_at)
PARTITION BY toYYYYMM(transition_at)
ORDER BY (tenant_id, transition_at, transition_id)
TTL toDateTime(_inserted_at) + INTERVAL 365 DAY;
