# ADR-004 · At-least-once 与事务 Outbox

状态：提案。Kafka Producer 持久化确认后应答中心 HTTP；Processor PG Commit 后提交 Offset；所有业务状态和 Outbox 原子写入。外部通知不承诺 Exactly Once，记录每次尝试及幂等键。Edge WAL 单副本/源 SSE 断线均存在明确的数据保证边界。
