# P9 集群验收（rpi-k3s · 2026-11-16）

覆盖：P9 指标在真实集群的暴露、ClickHouse 归档迁移、指标中心对接真实 Prometheus。
应用侧单测/集成证据见 `planning/P9-Observability-and-Metrics-Center-2026-11-16.md` 与 DEVELOPMENT_LOG R22/R23。

## 部署轨迹（release `ap-dev`）

| REVISION | 镜像 | 内容 |
| --- | --- | --- |
| 5 | `alert-platform:0.5.0-dev` | P9 第一批（OBS-02..05）+ MC-01 代码；migrate 到 schema 12 |
| 6 | `0.5.1-dev` | telemetry/archive 指标改惰性注册（修复 API 显示恒 0 业务 gauges） |
| 7 | `0.5.2-dev` | processor 指标同样惰性注册（kafka 包导入导致 API 注册） |

迁移：`schema_migrations = 12|f`，`archive_cursors` 表存在（P9-CH-01）。

## 指标暴露

`kubectl exec` 抓取（REVISION 7）：

- API `/metrics`：`alert_platform_ingest_events_total`、`ingest_rejected_total{reason}`、
  `ingest_batch_duration_seconds_*` 等 15 个 ingest 序列。
- Edge `/metrics`：`edge_wal_bytes`、`edge_wal_pending_records`、`edge_reconnect_total`、`edge_sse_gap_total`。
- **惰性注册修复验证**：API 进程中 `alert_platform_(processor|notify|archive|alert_active|incident_open)*`
  序列数为 **0**（此前因 postgres/kafka 导入类型而误注册恒 0 指标）。

## 真实 Prometheus 抓取 + 指标中心查询

临时 Pod `prom-verify`（prom/prometheus:v2.53.0，静态配置抓 api Service 与 edge Pod）：

- `/-/ready` = Ready；targets：`alert-platform-api` **up**、`alert-platform-edge` **up**。
- `alert_platform_edge_wal_bytes` 可查（labels 含 job/instance/component）。
- 触发 10 次未认证 ingest 后：
  `alert_platform_ingest_rejected_total{reason="unauthenticated"}` = **10**。
- 本地 alert-api + `ALERT_PROMETHEUS_URL` 经 port-forward 指向该 Prometheus：
  `GET /metrics/v1/query?metric=ingest_rejected_rate` 返回真实序列
  `reason=unauthenticated`，value ≈ **0.0252 req/s**；控制台"指标"页截图
  `docs/evidence/console-metrics-real.png`（最新 0.014）。

## 其他路由检查

| 检查 | 结果 |
| --- | --- |
| `GET /history/v1/transitions` 未认证 | 401（路由存在且鉴权生效） |
| `GET /metrics/v1/catalog` 未配置 Prometheus | 503 `metrics backend not configured` |
| 归档任务 | 未配置 `ALERT_CLICKHOUSE_URL` → worker 不启动 archive 任务（设计行为） |

## 限制（诚实注记）

- 集群无 ClickHouse，**归档写入/读取未做真实 CH 验收**（代码面与 HTTP 契约测试已覆盖，
  真实链路挂 INC-007）。
- 集群 worker 仍 disabled（无 Kafka），`telemetry`/`notify`/`archive` 指标在 worker 侧未实机抓取。
- 临时 Prometheus/ConfigMap/port-forward/演示令牌均已清理；集群保留 REVISION 7（0.5.2-dev）。
---

## 后续：R28 升级（2026-11-17，REVISION 8 / schema 13）

- 镜像 `alert-platform:0.6.0-dev`（含 R27 created_mode 迁移 0013 + processor 超时 + 凭证缓存）经节点导入法
  部署，`helm upgrade` → **REVISION 8**；api/edge 滚动成功。
- `schema_migrations = 13|f`；`notification_outbox.created_mode` 列存在。
- 运行时检查：`/healthz` + `/readyz` 200；ingest 指标 15 条；`/history/v1/transitions` 与
  `/metrics/v1/catalog` 均 401（鉴权生效）；`notification_authority` 种子 `active gen=1 initialized=false`
  （worker 仍 disabled，符合设计）。
- 同轮本地回归：`smoke_e2e.sh` PASS（修复持久库固定 id 与依赖自愈）、`replay_v02.sh` PASS。
