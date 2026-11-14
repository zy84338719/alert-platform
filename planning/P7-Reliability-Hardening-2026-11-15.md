# P7 可靠性加固任务书 · 2026-11-15（Reliability Hardening）

> 本文档是**可直接下发给 Coding AI 的迭代任务书**，源自 2026-11-15 对 main 分支的实现评审。
> 每轮 AI 迭代 = 从"任务清单"中领取 **一个** 编号任务，按"实施要求"完整交付（失败测试 → 修复 → 回归证据 → 文档同步），不得跨任务混合提交。

---

## 0. 背景与定位

- 项目已完成 P5/P6 阶段功能覆盖（232 文件、29 个 Go 测试文件、三服务 + Edge + 控制台），但**核心链路语义正确性与故障安全性未达生产验收**。
- 静态评审估分：工程实现完整度 **7.5/10**，生产可靠性准备度 **3.5/10**。
- 本轮**不新增业务功能、不整体重构**，只做 Reliability Hardening。
- 当前结论：**不建议直接切换生产告警链路**；全部 P0 关闭前不得申请切生产。

## 1. 实施要求（每项任务必须遵守）

1. **测试先行**：每项问题先提交能稳定复现问题的**失败测试**，再改代码，最后以回归测试通过作为关闭条件。禁止修改测试断言来掩盖问题。
2. **独立提交**：每项任务使用独立分支或独立 Commit（建议 `rh-<id>-<slug>`），禁止混入大规模无关重构。
3. **兼容性**：保留已有 API 兼容性；必须变更 IDL 时执行 `buf breaking` 检查并附结果。
4. **交付四件套**：每个任务完成后给出 —— ①根因分析 ②变更文件清单 ③测试证据（含命令与输出摘要）④风险与回滚说明。
5. **测试范围**：执行 `go test ./...`、`go test -race ./...`，以及相关 PostgreSQL/Kafka 集成测试与必要的故障注入（崩溃/断网/跳跃 ACK）。
6. **真实性红线**：不允许以合成测试代替真实 DeepFlow 或 HUATUO 的最终集成验收（合成 fixture 只作为回归门槛，真实验收另挂 ARC-002 / HUATUO 真实样本任务）。
7. **文档同步**：每完成一项，同步更新 `planning/backlog.csv`、相关 ADR、`planning/DEVELOPMENT_LOG.md`。
8. **合并流程**：完成代码评审后再申请合并 main，不得直接部署生产告警。

---

## 2. 任务清单

### P0-01 修复 Alertmanager 事件身份 / 幂等键冲突

- **现象**：Alertmanager Adapter 将告警 Fingerprint 直接作为 `EventKey`，而 `event_receipts` 有 `(tenant_id, source_id, source_event_key)` 唯一索引 → 同一 Fingerprint 的 Firing 与 Resolved 共用幂等键，恢复事件触发唯一冲突后被 `ApplyEvent` 当重复观察忽略，**恢复消息丢失、状态机无法正确转换**。
- **涉及文件**：
  - `internal/adapter/translate/alertmanager.go`
  - `internal/infrastructure/postgres/store.go`（`ApplyEvent` / `event_receipts` 写入）
  - `migrations/0001_initial.up.sql`（唯一索引定义，必要时新增增量迁移）
- **修复方向**：将**逻辑告警身份**（fingerprint）与**每次事件观察身份**（observation key）彻底分离。一个告警周期内允许多次 Firing 观察 + 一次 Resolved 状态变化；重复投递用独立的稳定观察键去重（如 fingerprint + startsAt/alertname/labels 派生的稳定哈希 + 事件类型）。
- **失败测试（先行）**：真实 PostgreSQL 回归：`Firing → Firing(重复观察) → Resolved → 下一周期 Firing`，断言 Resolved 被持久化且状态正确转换、下一周期 Firing 不被误判为重复。
- **验收**：上述链路全部正确；重复投递幂等不变；`buf breaking` 通过（如涉 IDL）。
- **风险**：历史 `event_receipts` 数据的兼容（旧 key 语义）；需评估是否双读旧键。

### P0-02 重构 Edge WAL ACK 与 Compact

- **现象**：
  1. Uploader 对单条记录 HTTP 4xx（非 429）记错后 `continue`，后续记录成功却调用 `Ack(rec.Seq)`；而 WAL 的 `Ack` 是**最高序号水位**，`Compact` 清掉 ≤ 水位的所有记录 → 被拒记录（未成功上传）连同已成功记录一起被删除，**断网缓冲告警丢失**。
  2. `Compact` 先删旧分段再移新文件，中间崩溃丢失已接收数据（非原子、无目录 fsync）。
- **涉及文件**：
  - `internal/application/edge/service.go`（Uploader）
  - `internal/infrastructure/wal/wal.go`（`Ack` / `Compact`）
- **修复方向**：改为**连续 ACK 前缀**（只推进连续成功前缀的水位）或逐记录确认位图；`Compact` 采用可恢复的原子切换（写新 → fsync → rename → fsync 目录），崩溃后可恢复。
- **失败测试（先行）**：① 跳跃 ACK（100 拒/101 成功 → 100 不被清）；② Compact 中途崩溃注入 + 重启恢复；③ 断网补传端到端（已确认事件无丢失）。
- **验收**：已确认事件在任何 ACK 顺序/崩溃场景下零丢失；现有 T-011 断网补传用例保持通过。
- **风险**：水位语义变更影响现有 WAL 格式/恢复逻辑，需保留旧 WAL 兼容读取或迁移说明。

### P0-03 加固内部 Ingest 鉴权与租户/来源绑定

- **现象**：`POST /ingest/v1/events:batch` 直接信任 CanonicalEvent 中自报的 `tenant_id`/`source_id`；路由中间件返回 `nil`，全局中间件只有日志/恢复/Trace，**无应用层身份鉴权** → 未授权调用可伪造任意租户告警。
- **涉及文件**：
  - `internal/adapter/inbound/http/v1/ingest_service.go`
  - `internal/adapter/inbound/http/router/v1/middleware.go`
- - **修复方向**：为 Edge 内部上传建立独立身份认证 + 来源授权，绑定可信租户/来源范围。优先 mTLS 身份映射或独立短期凭证；**服务端权威覆盖 payload 自报身份**（认证身份与 payload 不一致时 403）。
- **失败测试（先行）**：未认证 → 401；凭证与 payload 租户/来源不匹配（越权/跨租户伪造）→ 403；合法 Edge 凭证 → 2xx 且入库身份取自凭证。
- **验收**：安全矩阵用例全绿；不影响已有合法来源链路（DeepFlow/Alertmanager Adapter 回归通过）。
- **风险**：现有 Edge 客户端需同步升级凭证配置；部署侧证书/密钥分发流程需更新 docs/12。

### P0-04 重做 HUATUO 协议适配（POST SSE + CloudEvents）

- **现象**：
  1. SSE Client 用 `HTTP GET`，而官方 `/v1/events/watch` 是 **HTTP POST + JSON 请求体**，且按部署配置需 Bearer Token。
  2. Collector 把 SSE 数据当顶层 `kind`/`node` 解析，官方实际是 **CloudEvents 1.0 JSON**（应取 `id`、`source`、`type`、`time`、`data`）。
  3. 心跳看门狗用固定定时器关连接，**不刷新最后活跃时间** → 正常收流也被周期性强制重连。
  4. 现有测试用自建 GET SSE 模拟服务，只证明模拟可跑，不证明与真实 HUATUO 兼容。
- **涉及文件**：
  - `internal/infrastructure/sseclient/client.go`
  - `internal/application/collector/service.go`
- **修复方向**：按官方 OpenAPI 重写 Watch 请求（POST + JSON body + Bearer），CloudEvents 1.0 解码（`id/source/type/time/data`），心跳/断线重连以"最后活跃时间"为基准刷新；提供**真实样本契约测试**。
- **失败测试（先行）**：以官方真实样本 payload 为 fixture 的契约测试（当前实现必须失败）；心跳保活测试（持续收流不被强制断开）。
- **验收**：POST SSE 握手、CloudEvents 解码、认证、心跳、断线重连、事件唯一性验证全部通过；真实 HUATUO 环境联调另行安排（不以合成样本替代最终验收）。
- **风险**：官方协议细节以 docs.huatuo.tech 的 OpenAPI 为准，实现前先固化契约样本入库 `contracts/external/huatuo/`。

### P0-05 HUATUO 事件身份与 Occurrence 持久化

- **现象**：
  1. `appendFiring` 生成的 resource 只有 `cluster_id`/`resource_type`，缺 `EntityUID`，而 `fingerprint.go` 要求 EntityUID 非空否则 `incomplete alert identity` → HUATUO 告警进 Kafka 后在 Processor 侧失败。
  2. `ApplyEvent` 对 `OCCURRENCE` 只写 `event_receipts` 即提交，无可查询的原始事件记录 → 一次性内核异常不出现在事件历史/Incident 工作台。
- **涉及文件**：
  - `internal/application/collector/service.go`
  - `internal/domain/alert/fingerprint.go`
  - `internal/infrastructure/postgres/store.go`（`ApplyEvent` OCCURRENCE 分支）
- **修复方向**：把 CloudEvent 的节点/容器身份映射到 `ResourceRef`（含稳定 EntityUID，如节点 UID/容器 ID）；Occurrence 单独持久化到事件历史，并通过明确的事件策略触发 Incident 或 Stateful Alert。
- **失败测试（先行）**：HUATUO 事件 → Fingerprint 成功生成（不再 incomplete）；OCCURRENCE 处理后可在事件历史/工作台查询到。
- **验收**：HUATUO 链路事件可形成有效告警且可追溯；与 P0-04 的契约样本联动回归。
- **风险**：指纹输入变化会影响告警归并身份，注意版本前缀与历史指纹兼容。

### P0-06 修复通知静默发送前复核

- **现象**：`SuppressNotifyAtSend` 用 `LIMIT 1` 查静默规则 —— 多条规则时第一条不匹配即返回 `false`（漏抑制）；且 SQL 只查 `ends_at > now()` 未限定开始时间，**未生效的静默规则被当作有效**。
- **涉及文件**：`internal/infrastructure/postgres/store.go`（`SuppressNotifyAtSend`，约 L809-L835）
- **修复方向**：一次取全部有效候选（或 SQL 内完整匹配，含 `starts_at <= now() < ends_at`），任意一条命中即抑制。
- **失败测试（先行）**：① 两条规则第一条不匹配、第二条匹配 → 必须抑制；② 尚未开始的规则 → 不抑制；③ 多规则 + 边界时间。
- **验收**：多静默命中矩阵全绿。
- **风险**：低；注意匹配语义（matcher 字段）与现有静默匹配器实现保持一致。

### P0-07 Shadow → Active 切换禁止补发历史通知

- **现象**：Shadow 模式只记录 `would-have-sent` 不改任务状态；切换 Active 后历史 `pending` 任务被当成待发送任务批量投递 → **通知风暴**。
- **涉及文件**：`internal/application/notify/service.go`
- **修复方向**：引入通知所有权生效时间 + 任务的 `delivery_mode_at_creation`（或等价标记），区分"历史观察任务"与"允许真实投递"的任务；切 Active 时不自动补发 Shadow 历史任务。
- **失败测试（先行）**：shadow 期间产生 pending 任务 → 切 active → 断言历史任务**不发送**（或仅按显式策略），新任务正常投递。
- **验收**：切换无历史风暴；`ALERT_NOTIFY_MODE` 三态测试（TestNotifyModes）保持通过并扩展。
- **风险**：与既有"幂等键含 transition/stage"语义联动，勿破坏 active→shadow 回滚的"不丢不重发"。

### P1-01 Kafka 分区键与批量部分成功语义

- **现象**：
  1. 入队 key 为 `tenant_id|source_id` → 大来源热点分区，Consumer Group 扩容失效。
  2. `WriteMessages(ctx, msgs...)` 被解释为"整批原子写入"，但 kafka-go 同步批量写入**可能部分成功**，错误含逐条结果 —— 文档/接口语义/测试与实际不符。
- **涉及文件**：
  - `internal/application/ingest/service.go`（key 计算）
  - `internal/infrastructure/kafka/producer.go`
- **修复方向**：分区键改为服务端计算的 `tenant + fingerprint`（同一告警逻辑身份保序）；批量写入语义改为"至少一次 + 消费端幂等"，部分成功可安全整批重试，接口注释与测试同步修正。
- **失败测试（先行）**：① 同 fingerprint 同分区/同序；② 部分成功错误路径重试不丢不重（消费端幂等断言）。
- **验收**：语义与文档一致；压测下无单分区热点（可选 PERF 验证）。
- **风险**：分区键变更影响消息顺序假设，需评估滚动升级期间新旧 key 混布的乱序窗口。

### P1-02 通知 Lease Fencing（完成时所有权校验）

- **现象**：`FinishNotifyTask` 只按任务 ID 更新状态，不校验 Lease Token/Attempt 所有权 → 超时后任务被他 Worker 重领，旧 Worker 迟到结果覆盖新结果。
- **涉及文件**：`internal/infrastructure/postgres/store.go`（任务 claim/finish 相关）；如需加列则新增迁移。
- **修复方向**：增加 `lease_token`/`attempt_id` 列，所有完成操作做**所有权条件更新**（WHERE token 匹配）；迟到响应只写审计，不覆盖当前状态。
- **失败测试（先行）**：模拟双 Worker 竞争 + 迟到完成 → 断言新状态不被旧结果覆盖、迟到行为有审计。
- **验收**：并发/超时场景矩阵通过（`-race`）。
- **风险**：任务表 schema 变更（增量迁移，可回滚）。

### P1-03 Worker 后台任务故障传播

- **现象**：`cmd/alert-worker/main.go` 把 Relay/Notifier/Health/Retention/Escalation 作为 goroutine 启动，异常退出只打日志，主消费进程继续跑且 Readiness 仍报健康。
- **涉及文件**：`cmd/alert-worker/main.go`
- **修复方向**：`errgroup.WithContext` 管理关键任务，按任务类型定义重试或 fail-stop；关键消费者不可用时 Readiness 报异常。
- **失败测试（先行）**：注入某关键任务 panic/退出 → 断言进程按策略处置且 Readiness 变为不健康。
- **验收**：故障传播矩阵通过；不影响正常滚动升级探针行为。
- **风险**：探针语义变更需与 Helm values 中探针配置一致（与 P1-04 联动检查）。

### P1-04 Helm NetworkPolicy 按组件拆分

- **现象**：`network-policy.yaml` 用单一 Pod Selector 选中同 Release 全部组件，Egress 主要只放行 Kafka/PostgreSQL/kube-system，且 `networkPolicy.enabled` 默认 `true` → 默认安装即阻断：通知 Worker 访问飞书/钉钉、Edge 访问跨集群中央 API、其他必要外呼。
- **涉及文件**：
  - `deploy/helm/alert-platform/templates/network-policy.yaml`
  - `deploy/helm/alert-platform/values.yaml`
- **修复方向**：拆成 API / Worker / Edge 三套独立 NetworkPolicy，按实际通信图配置白名单（含外部通知渠道 egress、Edge→中央 API）。
- **失败测试（先行）**：连通性矩阵测试（允许/拒绝均需有断言证据）。
- **验收**：默认安装下通知、Edge 上报、跨集群链路均连通；非白名单流量被拒。
- **风险**：不同集群（Calico/Cilium）行为差异，需在 rpi-k3s 测试集群实测。

### P1-05 DeepFlow v7.2.4 基线验证

- **现象**：DeepFlow Latest 已是 **v7.2.4**（2026-11-12 发布），仓库仍以 `contracts/external/deepflow/v7.2.3` 为主适配目录。
- **涉及文件**：`contracts/external/deepflow/`、DeepFlow adapter（`internal/adapter/translate/` 一族）
- **修复方向**：保留稳定 `deepflow/v7_2` Adapter 族，新增 v7.2.4 Golden Fixtures 与差异兼容测试；**不得只改版本字符串充当升级**。
- **验收**：v7.2.4 真实 Payload 映射与契约测试通过；与 ARC-002（真实样本采集）联动。
- **风险**：低；注意枚举/字段增删差异。

---

## 3. 执行顺序与依赖

```
第一批（阻断项，先行）:  P0-01 → P0-02 → P0-03
第二批（HUATUO 链路）:   P0-04 → P0-05（005 依赖 004 的契约样本）
第三批（通知）:          P0-06 → P0-07 → P1-02（Lease）
第四批（平台化）:        P1-01、P1-03、P1-04、P1-05（相互独立，可并行）
```

- 每批完成后跑全量 `go test ./...` + `-race` + 集成回归，再进入下一批。
- P1-05 与 ARC-002（真实 DeepFlow 样本）、P0-04 与 HUATUO 真实联调属于**外部输入依赖**，代码侧完成后挂起等真实环境。

## 4. 关闭门槛（下次评审硬性项）

以下四项为下次评审的硬性门槛，全部关闭后才讨论 Incident 智能关联、ClickHouse 性能优化、多集群生产落地：

1. **Alertmanager 恢复链路**（P0-01）：Firing→Resolved→新周期回归通过。
2. **Edge WAL 数据完整性**（P0-02）：跳跃 ACK + 崩溃注入零丢失。
3. **内部 Ingest 鉴权**（P0-03）：401/403/跨租户伪造全拒。
4. **HUATUO 真实协议**（P0-04+P0-05）：POST SSE + CloudEvents + 资源身份 + 真实样本。

> 同步挂账（P6-GoNoGo §4 五项放行条件不变）：真实 DeepFlow 样本、真实通知沙箱、四集群实装、ClickHouse 归档实跑、生产 10min 压测复跑。

## 5. 任务状态跟踪（AI 迭代时更新本表）

| ID | 任务 | 状态 | 分支/Commit | 测试证据 | 备注 |
| --- | --- | --- | --- | --- | --- |
| P0-01 | Alertmanager 事件身份 | TODO | | | 先失败测试 |
| P0-02 | Edge WAL ACK/Compact | TODO | | | 先失败测试 |
| P0-03 | Ingest 鉴权 | TODO | | | 先失败测试 |
| P0-04 | HUATUO POST SSE/CloudEvents | TODO | | | 依赖真实契约样本 |
| P0-05 | HUATUO 身份/Occurrence 持久化 | TODO | | | 依赖 P0-04 |
| P0-06 | 静默复核漏匹配 | TODO | | | |
| P0-07 | Shadow 历史通知风暴 | TODO | | | |
| P1-01 | Kafka 分区/批量语义 | TODO | | | |
| P1-02 | 通知 Lease Fencing | TODO | | | |
| P1-03 | Worker 故障传播 | TODO | | | |
| P1-04 | NetworkPolicy 拆分 | TODO | | | rpi-k3s 实测 |
| P1-05 | DeepFlow v7.2.4 | TODO | | | 依赖 ARC-002 |

---

*评审来源：2026-11-15 main 分支实现评审（静态审查 + CI 记录 + P6 Go/No-Go 材料对照）。估分与结论见评审正文。*
