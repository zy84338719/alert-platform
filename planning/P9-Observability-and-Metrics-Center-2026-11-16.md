# P9 可观测性与"指标中心"任务书 · 2026-11-16

> 来源：2026-11-16 剩余工作盘点（指标只有骨架、CH 只有接口、控制台无指标页）。
> 执行要求沿用 P7/P8 协议：测试先行、独立 commit、`golangci-lint` 零发现、IDL 变更走 buf breaking、
> 不得以合成测试替代真实环境验收。

---

## 0. 现状与差距（实测）

`docs/08 §3` 承诺 14 个指标名，代码实际注册 4 个，且命名与文档不一致：

| 已实现 | 实际注册名 | 文档承诺名 |
| --- | --- | --- |
| Edge WAL 字节 | `alert_platform_edge_wal_bytes` | `alert_edge_buffer_bytes` |
| Edge WAL 待传数 | `alert_platform_edge_wal_pending_records` | `alert_edge_buffer_events` |
| 保留清理 | `alert_platform_retention_receipts_deleted_total` | （未在文档列出） |
| 保留留存 | `alert_platform_retention_receipts_retained_total` | （未在文档列出） |

完全缺失：接入 total/rejected、processor duration、consumer lag、transition conflict、
notification attempt/pending/failed、edge reconnect/sse gap、业务 gauges、source heartbeat age、
DLQ 隔离、authority generation、lease renew 失败、vendor 业务拒绝。

非指标缺口：PrometheusRule 自身告警、独立兜底通知通道（docs/08 明确要求）；
ClickHouse 仅有接口；控制台无指标查询/看板页。

## 1. 命名与基数红线（OBS-01）

- 统一命名：`alert_platform_<area>_<metric>`（area ∈ ingest/processor/consumer/notify/edge/source/alert/incident）。
- **禁止高基数标签**：event_id / incident_id / pod uid / source uid / channel_id 不得作为 label；
  允许的低基数 label：`reason`、`result`、`channel_type`、`topic`、`group`。
- 每个指标必须有 Help；Counter 以 `_total` 结尾；耗时用 Histogram（秒）。
- `docs/08 §3` 与实现同步更新（本次以实际注册名为准，保留旧名作为兼容说明）。

## 2. 任务清单

### P9-OBS-02 接入指标（ingest + API handler）
- `alert_platform_ingest_events_total`：成功入队的 canonical 事件数。
- `alert_platform_ingest_rejected_total{reason}`：reason ∈ `unauthenticated|disabled|forbidden|invalid|publish|registry`。
- `alert_platform_ingest_batch_duration_seconds`：PushBatch 处理耗时（含 Kafka ack）。
- 实现位置：`internal/application/ingest` 定义与导出记录函数；`http/v1` handler/middleware 调用。
- 验收：伪造 401/403/400 请求 → 对应 reason 计数 +1；成功批次 → events_total +N。
- 测试：`prometheus/testutil` 断言计数增量。

### P9-OBS-03 消费与处理指标
- `alert_platform_processor_duration_seconds`：ApplyEvent 事务耗时。
- `alert_platform_processor_events_total{outcome}`：outcome ∈ `applied|duplicate|permanent|transient`。
- `alert_platform_consumer_lag{topic,group}`：Gauge，来自 `kafka.ReaderStats().Lag`。
- `alert_platform_dlq_quarantined_total{topic}`：隔离消息数（R20 毒消息链路的可观测面）。
- 验收：DLQ 测试断言 quarantined +1、lag gauge 可读；transient 错误 outcome=transient。

### P9-OBS-04 通知指标
- `alert_platform_notify_attempt_total{channel_type,result}`：result ∈ `sent|retryable|permanent|ssrf`。
- `alert_platform_notify_pending`：Gauge，当前纪元 pending+retry 任务数（30s 采样，避免每 tick COUNT）。
- `alert_platform_notify_lease_renew_failures_total{reason}`：reason ∈ `stale|error`。
- `alert_platform_notify_authority_generation`：Gauge，InitAuthority 后设置。
- 验收：active 投递一次 → attempt{sent}=1；stale 续租 → renew_failures{stale}=1；
  1000 条 shadow 无投递 → attempt 不增。

### P9-OBS-05 Edge/SSE 指标
- `alert_platform_edge_sse_gap_total`：collector 收到 gap 标记时 +1。
- `alert_platform_edge_reconnect_total`：SSE 断线重连次数（sseclient 重连路径）。
- `alert_platform_edge_wal_bytes` / `_pending_records`：保留，docs 同步说明旧名映射。
- 验收：模拟断流 → gap/reconnect 计数增长；现有 WAL 指标不回归。

### P9-OBS-06 业务 Gauges（周期采样）
- `alert_platform_alert_active_instances`、`alert_platform_incident_open`、
  `alert_platform_source_heartbeat_age_seconds{source}`（source 为注册来源，基数受控）。
- 采样位置：worker 新增 metrics sampler（30s，非关键任务，失败只记日志）。
- 验收：造 3 个活跃实例/1 个 open incident → gauge=3/1；来源失联 → age 增长。

### P9-OBS-07 自身告警与兜底通道
- Helm 增加可选 `PrometheusRule`（默认关闭）：IngestRejectRate、ConsumerLag、NotifyFailureRate、
  DLQGrowing、AuthorityStale、WorkerReadinessDown。
- 独立兜底通道：设计文档 + 最小实现（独立 webhook，**不经过 notification_outbox**），
  避免"通知链路故障时无法告警"。默认关闭，显式配置启用。
- 验收：rule 渲染矩阵；兜底通道单测（HTTP 200/非 200 语义、脱敏、超时）。

### P9-MC-01 产品内"指标中心"（Phase 2）
- **数据源决策（2026-11-16）：A. Prometheus HTTP API 代理**（与 OBS 指标同源、无需新存储；
  B/C 保留为后续备选）。
- 后端：`GET /metrics/v1/query`（白名单指标 + 时间范围 + 步长，租户/权限复用现有 RBAC）。
- 前端：控制台新增"指标中心"页（趋势图、TopN、告警叠加）；不引入 Vue3 工程化前用原生 JS 实现。
- 验收：白名单外指标 403；查询超时/无数据有明确 UI 状态；页面截图入 docs/evidence。

### P9-CH-01 ClickHouse 归档（Phase 3）
- 代码面：CH Writer 接口 + 批量写入 + DDL（events/transitions 按 tenant/time 分区）+ 查询路由；
  无 CH 环境时以接口单测 + 契约测试覆盖，真实写入验证挂 INC-007。
- 验收：无 CH 时归档关闭不影响主链路；CH 不可用时不阻塞事务（异步 + 有界队列）。

### P9-INT-01 真实集成（BLOCKED，外部输入）
- DeepFlow v7.2.4 真实样本（ARC-002）、HUATUO 联调、通知沙箱（NOTI-002）、
  四集群（MC-001/005）、CH 实跑（INC-007）、长时压测（P8-012）。
- 代码侧已就绪，本任务书不重复实现，仅跟踪。

## 3. 执行顺序

```
第一批（本轮）：OBS-02 → OBS-03 → OBS-04 → OBS-05
第二批：OBS-06 → OBS-07
第三批：MC-01（依赖 OBS + 数据源评审）
第四批：CH-01（可与 MC-01 并行）
挂账：INT-01（外部输入）
```

## 4. 关闭门槛

1. docs/08 §3 中每个指标名都能在 `/metrics` 实测输出中找到（名称/类型/标签一致）。
2. `golangci-lint` 0 issues；新增代码均有指标断言测试。
3. 指标基数审查：无 event/incident/pod/channel uid 标签。
4. OBS-07 兜底通道不依赖 notification_outbox（代码级证明 + 单测）。

## 5. 状态跟踪

| ID | 任务 | 状态 | Commit | 证据 |
| --- | --- | --- | --- | --- |
| P9-OBS-02 | 接入指标 | DONE | `1a71339` | TestIngestMetrics（计数/直方图/失败不误记） |
| P9-OBS-03 | 消费/处理指标 | DONE | `1a71339` | TestHandleMetricsOutcomes + DLQ 计数断言 |
| P9-OBS-04 | 通知指标 | DONE | `1a71339` | TestNotifyMetrics + 多副本/租约联动 |
| P9-OBS-05 | Edge/SSE 指标 | DONE | `1a71339` | TestSSEGapMetricIncrements + 重连计数 |
| P9-OBS-06 | 业务 Gauges | DONE | `77b0ec7` | TestSampleBusinessGauges（active/incident/heartbeat+序列清理） |
| P9-OBS-07 | 自身告警+兜底通道 | DONE | `77b0ec7` | TestSelfAlerter×3 + scripts/test_chart_rules.py（5 规则/selector/extraRules/docs 一致） |
| P9-MC-01 | 产品内指标中心 | DONE | `77b0ec7` | 数据源=Prometheus API；metricsquery/promclient/handler 测试 + 控制台截图 docs/evidence/console-metrics.png |
| P9-CH-01 | ClickHouse 归档 | DONE（真实 CH drill 通过） | `d3d0dca` + 本轮 | 迁移 0012/客户端/路由/DDL；`scripts/drill_ch_archive.sh` 真实 CH 24.8 全链路 PASS（INGEST→PG→CH→history API）；drill 发现 1970 时间戳 P0 已修复 |
| P9-INT-01 | 真实集成 | BLOCKED | — | 外部输入 |

## 6. 集群验收（2026-11-16）

rpi-k3s `ap-dev` REVISION 5→6→7（镜像 0.5.0→0.5.2-dev）：迁移 12 应用；P9 指标经 `/metrics` 暴露；
临时 Prometheus 抓取 api/edge 均为 up；指标中心对接真实 Prometheus 返回真实序列；
控制台真实数据截图 `docs/evidence/console-metrics-real.png`。
详见 `docs/evidence/P9-cluster-verification-2026-11-16.md`（限制：无 ClickHouse/无 worker）。

---

*盘点来源：2026-11-16 对 docs/08 §3 与实际 metrics 注册的逐一对照。*