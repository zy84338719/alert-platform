# P9 ClickHouse 归档 E2E Drill（2026-11-17）

真实 ClickHouse 引擎 + 真实 PostgreSQL/Kafka + 真实 api/worker 进程的全链路验收：
`ingest → processor → alert_transitions(PG) → archive 任务 → ClickHouse → GET /history/v1/transitions(archived=true)`。

## 环境与执行

- ClickHouse：`clickhouse/clickhouse-server:24.8-alpine`（本地 Docker，真实引擎，非 mock）
- PG 55432 / Kafka 19092（dev compose）
- 一次性数据库/主题/CH database，脚本结束自动清理
- 复现：`bash scripts/drill_ch_archive.sh`（`KEEP_WORK=1` 保留日志）
  - 覆盖 `ALERT_CLICKHOUSE_URL/DB/USER/PASSWORD`、`ALERT_HISTORY_PG_WINDOW=1m`

## 结果（2026-11-17）

```
PG=2 CH=2
history API: archived=true transitions=2 states=['firing','resolved']
CH ARCHIVE E2E PASS: ingest -> PG -> archiver -> ClickHouse -> history API
```

- `archive_cursors` 复合游标推进正常，重启不重复归档。
- 归档失败不推进游标（单测覆盖），CH 不可用不影响告警写入。

## Drill 发现并修复的问题

1. **P0：缺失 protobuf 时间戳被写成 1970（已修复 + 回归测试）**
   `(*timestamppb.Timestamp)(nil).AsTime()` 返回 `1970-01-01` 而不是 `time.Time{}`，
   旧实现把它当作"已知时间"，`orNow` 回退失效 → `alert_transitions.transition_at`、
   `alert_instances.starts_at/last_observed_at`、`event_history` 全部落 1970。
   ClickHouse 分区 `197001` 立即被 TTL 删除，表现为"归档成功但查不到"。
   修复：`store.go` 新增 `eventTime()`（nil/epoch-zero → 零值 Time），全部事件时间入口替换；
   回归测试 `TestMissingEventTimestampsFallBackToNow`。
2. **DDL TTL 类型不合法**：ClickHouse 24.8 拒绝 `DateTime64` 作为 TTL 表达式
   （`BAD_TTL_EXPRESSION`）→ 改为 `TTL toDateTime(transition_at) + INTERVAL 365 DAY`。
3. **CH 24.x 默认用户网络禁用**：未设置 `CLICKHOUSE_USER/PASSWORD` 时容器禁止 default 用户网络访问；
   drill 固定账号密码，同时覆盖 `ALERT_CLICKHOUSE_USER/PASSWORD` 路径。

## 限制

- 本 drill 使用真实 ClickHouse 引擎但非目标生产集群；集群级 CH（HA/容量/TTL 策略）仍属 INC-007 挂账。
- 历史读路由的 PG 窗口在生产使用 `ALERT_HISTORY_PG_WINDOW=168h`（默认），drill 用 1m 以便命中原归档行。