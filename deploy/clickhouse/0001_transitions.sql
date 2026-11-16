-- P9-CH-01: ClickHouse archive schema. Run inside the database selected by
-- ALERT_CLICKHOUSE_DB (default "default").
--
-- Retention: 365 days at the CH layer (TTL); PG keeps its own retention
-- window. Adjust TTL to the platform's audit requirement.
CREATE TABLE IF NOT EXISTS alert_transitions
(
    tenant_id     UUID,
    transition_id UUID,
    instance_id   UUID,
    from_state    LowCardinality(String),
    to_state      LowCardinality(String),
    event_id      String,
    transition_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transition_at)
ORDER BY (tenant_id, transition_at, transition_id)
TTL toDateTime(transition_at) + INTERVAL 365 DAY;