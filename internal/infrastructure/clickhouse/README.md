# ClickHouse 归档适配器（INC-007）

实跑 BLOCKED：目标 CH 集群未提供（ARC-005 同源依赖）。接入时：

1. 建表（archive DDL 已按 domain_outbox payload 形状设计）：

```sql
CREATE TABLE alert_transitions_archive (
    tenant_id UUID, transition_id UUID, instance_id UUID,
    from_state String, to_state String, event_id String,
    transition_at DateTime
) ENGINE = ReplacingMergeTree ORDER BY (tenant_id, transition_at, transition_id) TTL toDateTime(transition_at) + INTERVAL 18 MONTH;
```

2. 消费 `alert.lifecycle.v1`（domain outbox relay 产物）写入——不在任何 PG 事务内；
3. `history.Service` 注入 archive Store 即自动分流（窗口外查询走 CH）。

设计保证：归档与 PG 写路径完全解耦（Kafka 中转），PG P95 不受历史查询/写入影响；
TTL 即保留策略。
