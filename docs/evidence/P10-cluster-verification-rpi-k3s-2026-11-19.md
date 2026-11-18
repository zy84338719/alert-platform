# P10 / R29 集群验证：rpi-k3s REVISION 11（0.6.3-dev，schema 16）

- 日期：2026-11-19
- 目标：`~/.kube/rpi-k3s.yaml`，namespace `alert-platform`，release `ap-dev`
- 镜像：`alert-platform:0.6.3-dev`（arm64，节点本地导入，`IfNotPresent`）
- 变更：P10-001..008 + AUD-001..016 修复批次（main@b4d9d4a + 集群适配）

## 1. 发布结果

| 项 | 结果 |
|---|---|
| Helm | **REVISION 11 / deployed**（chart 0.3.0） |
| 镜像 | api/edge/migrate 全部 `alert-platform:0.6.3-dev` |
| Pod | api 1/1 Running、edge 1/1 Running、migrate Completed、dev-pg Running |
| 节点 | 全部钉在 `rpi4-011`（镜像导入节点） |
| schema | **16**（新建表 `transition_archive_queue`、`component_leases` 均存在） |
| readiness | `/healthz` 200、`/readyz` 200 |

> 首次升级（REVISION 9）暴露出真实部署依赖：新增的 Kafka readiness 检查让无 Kafka 的 dev 集群
> API 一直 503。生产默认保持 fail-closed；dev values 显式设置
> `ALERT_KAFKA_READINESS=false`（启动告警）后 REVISION 10/11 正常。集群仍无 Kafka，
> ingest 只验证到鉴权层。

## 2. 验收检查（port-forward api :18080）

| 检查 | 结果 |
|---|---|
| `GET /healthz` / `/readyz` | 200 / 200 |
| 安全响应头（`/console`） | `X-Frame-Options: DENY`、`X-Content-Type-Options: nosniff`、CSP 含 `frame-ancestors 'none'` |
| `GET /metrics/v1/catalog` 无凭证 | 401 |
| `GET /history/v1/transitions` 无凭证 | 401 |
| `GET /channels/v1` 无凭证 | 401 |
| `POST /edge/v1/heartbeat` 无凭证 | 401 |
| oncall token `GET /channels/v1` | 200（channels:read 拆分生效） |
| oncall token `GET /history/v1/transitions` | 200（alerts:read） |
| oncall token `GET /metrics/v1/query?metric=ingest_events_rate` | 503（本集群未配置 Prometheus backend；本地 stub Prometheus 已验证 403 平台越权） |

指标中心分级/租户 matcher 与响应 `{timestamp,value}` 形状在本地真实 API + stub Prometheus 下验证：

- admin catalog 包含 platform + tenant 指标；
- oncall catalog 仅 3 个 tenant 指标；
- oncall 查询平台指标 → 403；查询 `active_instances` → Prometheus 收到
  `alert_platform_alert_active_instances{tenant="1111..."}`。

## 3. 本地 E2E（同日）

- `scripts/smoke_e2e.sh`：**PASS**（schema 16，202→Kafka→PG firing，trace 关联，重复批次幂等）。
- `scripts/drill_ch_archive.sh`：**PASS**（真实 CH 24.8：PG=3/CH=3、迟到事件归档、重复插入 FINAL 仍 3、
  history `archived=true`）。
- `scripts/drill_rel002.sh`：**PASS**（shadow 零外发、对账 5/5、cutover、回滚不丢不重发，
  含 P10-005 authority fencing 路径）。

## 4. 限制

1. 集群无 Kafka → worker disabled；processor/notifier 链路仍需 Kafka 环境复验（P11）。
2. 集群无 Prometheus → 指标中心接口 503（预期 fail-closed）；分类/隔离逻辑由本地 stub 验证。
3. 镜像节点导入法仍需每次升级重跑（本文件所用 0.6.3-dev 已导入 rpi4-011）。