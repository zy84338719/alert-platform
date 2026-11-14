# ADR-004 · At-least-once 与事务 Outbox

状态：已接受（2026-11-14；依据：决策经 P1–P6 八轮实现全部落地并验证——Kafka 异步/至少一次+Outbox/IDL-First/权威状态语义分别在消息链路、回放演练与混沌演练中实证）。Kafka Producer 持久化确认后应答中心 HTTP；Processor PG Commit 后提交 Offset；所有业务状态和 Outbox 原子写入。外部通知不承诺 Exactly Once，记录每次尝试及幂等键。Edge WAL 单副本/源 SSE 断线均存在明确的数据保证边界。
