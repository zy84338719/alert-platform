# P8 生产正确性与真实集成任务书 · 2026-11-15（Production Correctness & Integration）

> 本文档是**可直接下发给 Coding AI 的迭代任务书**，源自 2026-11-15 第二轮实现评审（对照 [P7-Reliability-Hardening-2026-11-15.md](P7-Reliability-Hardening-2026-11-15.md) 逐项复核后的新增发现）。
> 每轮 AI 迭代 = 从"任务清单"中领取 **一个** 编号任务，按"实施要求"完整交付（失败测试 → 修复 → 回归证据 → 文档同步），不得跨任务混合提交。
> **本轮不扩展 Incident 智能关联与 AI 功能**，只做生产正确性（Production Correctness）与真实集成（Integration）。

---

## 0. 背景与定位

- P7 的 12 项可靠性整改已全量关闭（14 commits，race + buf + chart + rpi-k3s 双绿），方向正确，**不需要大规模重构**。
- 但第二轮评审在新实现中发现 **4 项 P0 生产阻断**（通知权切换、Edge 上行身份、通知成功判定、SSRF 竞态）和 **6 项 P1/P2 工程问题**。
- P7 全部标记 DONE ≠ 生产验收完成。CI 成功只证明自动化检查通过，不等于数据丢失/故障场景已排除。
- 当前结论：**可进入生产前整改阶段，暂不建议切换生产告警权威**。P0-01～P0-04 全部关闭 + 真实集成门槛达成后，才开始小范围生产 Shadow 验证。

## 1. 实施要求（每项任务必须遵守）

1. **测试先行**：每项问题先提交能稳定复现问题的**失败测试**，再改代码，最后以回归测试通过作为关闭条件。禁止修改测试断言来掩盖问题。
2. **独立提交**：每项任务使用独立分支或独立 Commit（建议 `p8-<id>-<slug>`），禁止混入大规模无关重构。
3. **兼容性**：保留已有 API 兼容性；必须变更 IDL 时执行 `buf breaking` 检查并附结果。schema 变更用增量迁移、可回滚。
4. **交付四件套**：①根因分析 ②变更文件清单 ③测试证据（命令与输出摘要）④风险与回滚说明。
5. **测试范围**：`go test ./...`、`go test -race ./...`、相关 PostgreSQL/Kafka 集成测试与故障注入（重启/并发/竞态/DNS）。
6. **真实性红线**：合成 fixture 只作回归门槛；DeepFlow v7.2.4、HUATUO、多集群、真实渠道的最终验收必须以真实环境 E2E 报告为准，不得以合成测试替代。
7. **文档同步**：每完成一项，同步更新 `planning/backlog.csv`、相关 ADR、`planning/DEVELOPMENT_LOG.md`，并回填本文件 §6 状态表。
8. **合并流程**：完成代码评审后再申请合并 main，不得直接部署生产告警。

---

## 2. P0 任务清单（生产阻断，优先关闭）

### P8-P0-01 Shadow 权威状态持久化（跨进程重启失效）

- **现象**：Shadow→Active 的历史通知抑制依赖**内存字段** `shadowCutoff`（`internal/application/notify/service.go:89`）。`NewService` 默认 `ModeActive`，`SetMode` 仅在"当前对象已处于 Shadow 再切 Active"时记录 cutoff（`service.go:104-108`）。而 K8s 真实切换流程是改 `ALERT_NOTIFY_MODE` 环境变量后重启 Pod —— 新 Pod 启动即 active、`shadowCutoff` 为零值 → **历史 pending 任务被领取发送，通知风暴在正常部署场景下复现**（P7 的 P0-07 修复在跨进程场景失效）。
- **涉及文件**：
  - `internal/application/notify/service.go`（`shadowCutoff` / `SetMode` / `ClaimNotifyTasks` 参数链）
  - `internal/infrastructure/postgres/store.go`、`lease_store.go`（领取条件）
  - 新增迁移（Notification Authority 状态表或配置持久化）
- **修复方向**：将 Notification Authority 状态持久化到 PostgreSQL，记录 `mode`、`generation`、`effective_at`；创建任务时记录所属 Generation；新 Worker 只允许发送**当前授权 Generation** 的任务，不依赖进程内时间。
- **失败测试（先行）**：**跨进程切换验收**——Shadow 模式产生 1,000 条任务 → 停止 Worker → 配置切 Active → 启动新 Worker → 断言历史任务发送数为 **0**，新产生任务正常发送。
- **验收**：上述场景全绿；P7 的 TestNotifyModes 三态与 active→shadow 回滚"不丢不重发"保持通过。
- **风险**：任务表/领取 SQL 语义变更；注意与幂等键（含 transition/stage）联动，勿破坏现有回滚语义。

### P8-P0-02 Edge 多来源上行身份真正接入启动配置

- **现象**：Uploader 已支持 `SourceTokens map[string]string`（`internal/application/edge/service.go:63-66`），但 `cmd/alert-edge/main.go:81` 创建 Uploader 时**只传 `EdgeKey: cfg.EdgeAPIKey`，未传 `SourceTokens`**。中央 Ingest 按每个注册 Source 的凭证校验 SourceID（P7 的 P0-03 机制），导致一个 Edge 同时接入 DeepFlow + Alertmanager 时：统一 Token 则其他来源 401；独立 EdgeKey 不是注册来源凭证则全部 401。
- **涉及文件**：
  - `cmd/alert-edge/main.go`
  - `internal/application/edge/service.go`（Uploader 配置）
  - `internal/adapter/inbound/http/v1/ingest_service.go`、`router/v1/middleware.go`（中央校验）
  - Edge/中央配置 schema（`etc/`、docs/12）
- **修复方向**：
  1. Edge 增加独立的**上行身份配置**；
  2. 支持 `edge_id → tenant_id → allowed_source_ids` 授权关系；
  3. 中央校验每个 Batch 的 Source 范围（凭证身份权威覆盖 payload 自报）；
  4. "上游 Webhook 凭证"与"Edge→中心凭证"分开管理；
  5. **禁止**以在一个 Edge 中复制多个来源明文 Token 作为长期方案。
- **失败测试（先行）**：双来源（DeepFlow + Alertmanager）同 Edge E2E；凭证轮换；越权 SourceID 拒绝（403）。
- **验收**：双来源上传各自凭证正确、轮换无缝、越权全拒。
- **风险**：现有 Edge 部署配置需迁移；凭证分发流程同步更新 docs/12。

### P8-P0-03 飞书/钉钉业务响应校验（HTTP 200 ≠ 发送成功）

- **现象**：`internal/infrastructure/channel/channel.go` 的 `vendorCheck()`（L165-166）是**占位实现**（`return res`），且响应 Body 已被前序读取逻辑丢弃。HTTP 200 + `{"code": 非零}` 被 `classify()` 判成功 → Outbox 进入 sent，**平台记录"已成功"但机器人实际拒绝了消息**。
- **涉及文件**：`internal/infrastructure/channel/channel.go`（`Send` / `vendorCheck` / body 读取保留）
- **修复方向**：分别解析飞书、钉钉响应 Body，校验业务成功码（飞书 `code==0`、钉钉 `errcode==0` 等，以官方文档为准）；失败按可重试/不可重试分类；保留**脱敏**后的错误信息到 attempt 审计。
- **失败测试（先行）**：HTTP 200 + 业务错误码 → 断言判定为失败且 Outbox **不进入 sent**；正常业务成功 → sent；错误信息脱敏断言。
- **验收**：三态（业务成功/业务失败可重试/业务失败终态）矩阵全绿。
- **说明**：即便校验通过，也只证明渠道 API 接受了请求，不等于用户已读 —— 文档语义需与此一致。

### P8-P0-04 SSRF 防护 DNS 解析与实际连接的竞态（DNS Rebinding）

- **现象**：`channel.go` 的 `guardedDial()`（L67 附近）先 `LookupIPAddr` 校验 IP，随后 `DialContext(ctx, network, addr)` 仍用**原始域名** —— Dialer 会再次解析，实际连接 IP 可能与校验过的不同 → 可控 DNS 的通知目标可 DNS Rebinding 绕过防护。
- **涉及文件**：`internal/infrastructure/channel/channel.go`（`guardedDial` 及调用处）
- **修复方向**：将**已校验的 IP 直接作为 Dial 目标**（`net.JoinHostPort(ip, port)`），同时保留正确 TLS ServerName 与 HTTP Host（SNI/Host 不受影响）；覆盖 IPv4、IPv6、重定向、代理、多 A/AAAA 记录场景。不能仅依赖 URL 字符串检查。
- **失败测试（先行）**：DNS Rebinding 安全测试（解析校验返回合法 IP、实际解析指向内网 IP 的模拟）→ 断言连接被拒；IPv6/多记录/重定向矩阵。
- **验收**：P7 的 SSRF 双层防护矩阵保持通过 + 新增 rebinding 用例全绿。
- **风险**：代理/拨号层交互（oekit/自研 dialer），注意回归既有 169.254.169.254 用例。

---

## 3. P1/P2 任务清单（工程加固）

### P8-P1-01 Worker Readiness 任务检查注册过晚

- **现象**：`platform/service.go` 启动时遍历 `runtimeChecks` 注册健康检查，而 `cmd/alert-worker/main.go:103` 的 `RegisterRuntimeCheck("background-tasks", ...)` 在传给 `RunServiceWith` 的 `run()` 内才执行 —— 注册发生在健康检查注册表构建**之后**，background-tasks 很可能根本没进注册表。
- **涉及文件**：`cmd/alert-worker/main.go`、`internal/platform/service.go`（或 platform 的动态检查注册表）
- **修复方向**：在启动 HTTP Server 前创建 TaskGroup 并注册 ReadinessCheck；或让运行时健康检查直接读取动态注册表。
- **失败测试（先行）**：主动终止非关键后台任务 → 断言 `/readyz` 返回不健康。
- **验收**：故障注入下 Readiness 如实反映；正常滚动升级探针行为不变（与 Helm 探针配置一致）。

### P8-P1-02 NetworkPolicy 默认仍阻断外部通知

- **现象**：已拆分 API/Worker/Edge 三套策略，但 `deploy/helm/alert-platform/values.yaml:61` `workerExtraEgress: []` 默认为空 —— 默认安装仍不允许 Worker 访问飞书/钉钉外部地址；多集群下 Edge→中央 API 常不在同一 Namespace，默认 Edge Egress 也覆盖不了。
- **涉及文件**：`deploy/helm/alert-platform/values.yaml`、`deploy/helm/alert-platform/templates/network-policy.yaml`
- **修复方向**：把实际外部依赖（通知渠道 egress、Edge→中央 API）做成**显式部署配置与验证项**（文档 + values 注释示例）；Cilium 环境可评估 FQDN Policy，但**不要误认为标准 K8s NetworkPolicy 支持域名白名单**。
- **失败测试（先行）**：组件允许/拒绝流量矩阵（含外部通知出口、跨 Namespace/跨集群中央 API）。
- **验收**：默认配置下需显式声明外部依赖即可连通；非白名单被拒；rpi-k3s 实测证据。

### P8-P1-03 静默检查失败时仍继续发送

- **现象**：`internal/application/notify/service.go:203` 附近，`SuppressNotifyAtSend` 返回错误时只记日志，随后照常 `sender.Send(...)` —— 静默若为强约束，这是绕过抑制策略的行为。
- **涉及文件**：`internal/application/notify/service.go`
- **修复方向**：校验失败转为**可重试投递错误**（暂停投递并重试），不默认发送；极端 P0 是否绕过静默由**显式紧急通知策略配置**决定，不能在数据库异常时自动放行。
- **失败测试（先行）**：注入静默查询错误 → 断言消息**未发送**且任务进入可重试状态；显式紧急策略开启 → 才允许绕过（如有该配置）。
- **验收**：失败-不发送矩阵全绿。

### P8-P1-04 通知批量领取 + 逐条串行发送导致 Lease 提前过期

- **现象**：默认一次领取 50 条、Lease 60s、渠道 HTTP Timeout 10s，逐条串行发送 —— 极端下一批次执行远超 60s，后段任务 Lease 过期被其他 Worker 重领。Lease Fencing（P7 的 P1-02）只防迟到结果覆盖 DB，**挡不住两名 Worker 同时向外部渠道发同一消息**（第三方渠道无幂等键时重复投递）。
- **涉及文件**：`internal/application/notify/service.go`（领取批次/发送循环）、`lease_store.go`（续租）
- **修复方向**：有限并发的**逐任务领取**或**可续租机制**（heartbeat 续约）；每渠道**速率限制**；对无幂等键的第三方渠道，文档明确其**至少一次投递**语义。
- **失败测试（先行）**：模拟慢渠道（>60s 批次）→ 断言无重复投递（或明确记录 at-least-once 风险且有审计）。
- **验收**：慢渠道场景无双发；速率限制生效；`-race` 通过。

### P8-P1-05 同一 Fingerprint 并发创建缺少数据库级互斥

- **现象**：`ApplyEvent`（`internal/infrastructure/postgres/store.go`）在 ReadCommitted 下查活跃 AlertInstance **不加 Fingerprint 锁** —— 两个不同 Transport ID 的 Firing 并发进入、都看不到活跃实例时，各自生成 Episode ID 插入；唯一约束 `(tenant_id, fingerprint, episode_key)` 因 episode_key 不同**拦不住两个活跃周期并存**。Kafka 分区串行只是缓解，DB 不应依赖上游串行化保证聚合一致性。
- **涉及文件**：`internal/infrastructure/postgres/store.go`（`ApplyEvent`）、新增迁移（部分唯一索引）
- **修复方向**：按 `tenant + fingerprint` 的事务级互斥（`SELECT ... FOR UPDATE` / advisory lock）；**部分唯一索引**限制同一 Fingerprint 同时只存在一个活跃 Episode。
- **失败测试（先行）**：高并发（两个 Transport 并发 Firing）→ 断言只产生一个活跃 Episode；DB 约束直接插入双活跃 → 被拒。
- **验收**：并发矩阵 + 约束测试全绿；现有 alertmanager_lifecycle / 状态机回归通过。

### P8-P1-06（P2）HUATUO 聚合器内存长期增长

- **现象**：`internal/application/occurrence/aggregator.go` 的 `events map[string][]time.Time` / `fired map[string]time.Time` 只清理当前 Key 的旧时间，**长期不出现的 Key 永不删除** —— 大量不同容器 ID/节点标识时 Map 随运行时间增长。
- **涉及文件**：`internal/application/occurrence/aggregator.go`
- **修复方向**：过期淘汰（窗口外键定期清理）、Key 数量配额、内存水位指标；多 Collector 副本共同计算时再考虑把聚合状态迁到中心分区消费者（本轮不做）。
- **失败测试（先行）**：注入大量一次性 Key + 时间推进 → 断言 Map 尺寸受控（淘汰生效）。
- **验收**：长时运行内存有界；现有 aggregator_test 回归通过。

---

## 4. 附加回归（本轮评审遗留的小项）

- **P8-REG-01 Alertmanager 重复 Firing 不应被 Receipt 提前过滤掉观察时间刷新**：`amObservationKey`（`internal/adapter/translate/alertmanager.go:154-161`）已解决 Firing/Resolved 幂等冲突，但同周期重复 Firing 会被 Receipt 过滤，**无法刷新 `alert_instances.last_observed_at`**。补回归测试：同一告警持续 Firing 10 分钟 → 断言最后观察时间持续更新，且**不重复创建通知**。（必要时改为：Receipt 记录去重但观察时间仍更新。）

---

## 5. 生产交付能力跟踪（非 Bug，发布门槛）

以下能力不计入缺陷，但**未达标前不得切换生产告警权威**，按独立外部输入任务跟踪：

| 能力 | 当前状态 | 生产验收要求 | 依赖 |
| --- | --- | --- | --- |
| DeepFlow v7.2.4 | 版本目录 + 合成 Fixtures 已有 | 真实推送 Payload 与恢复生命周期验证 | ARC-002 / 真实数据源 |
| HUATUO | CloudEvents 代码已完善 | 真实 HUATUO 接口联调 | 真实环境 + 契约样本 |
| 多集群 | Helm + 部分测试环境部署 | 四套目标集群真实端到端验证 | SRE / kubeconfig |
| ClickHouse | 接口与规划 | 历史写入、查询、保留策略实跑 | CH 集群 |
| 通知渠道 | HTTP 实现已有 | 飞书/钉钉真实沙箱成功 + 失败测试 | 沙箱账号 |
| Kafka/PG 高可用 | 部分故障测试 | 多副本真实故障演练 | 性能/故障集群 |
| 性能 | 本地压测报告 | 2,000 events/s、≥10 分钟稳定性验证 | 性能集群 |
| 管理控制台 | 原生 JS 界面 | 若坚持原规划，需落实 Vue3 工程化 | 产品决策 |

---

## 6. 执行顺序与依赖

```
第一批（P0 阻断，串行关闭）:  P8-P0-01 → P8-P0-02 → P8-P0-03 → P8-P0-04
第二批（通知与聚合加固）:      P8-P1-03 → P8-P1-04 → P8-P1-06 → P8-REG-01
第三批（DB 与平台）:          P8-P1-05 → P8-P1-01 → P8-P1-02（P1-02 需 rpi-k3s 实测）
第四批（真实集成，外部输入）:  §5 表全部 —— 代码侧就绪后挂起等真实环境
```

- 每批完成后跑全量 `go test ./...` + `-race` + 集成回归，再进入下一批。
- §5 的真实集成是**发布门槛**，AI 代码侧完成后必须挂起，不得以合成测试关闭。

## 7. 关闭门槛（下次评审硬性项）

1. **Shadow 权威跨进程持久化**（P8-P0-01）：重启切换历史任务发送数为 0。
2. **Edge 多来源上行身份**（P8-P0-02）：双来源 E2E + 越权拒绝。
3. **通知业务响应校验**（P8-P0-03）：200+业务错误码不得入 sent。
4. **SSRF DNS 绑定**（P8-P0-04）：DNS Rebinding 测试不过不了闸门。
5. **真实集成**（§5）：DeepFlow v7.2.4 真实 Payload、HUATUO 真实联调、真实渠道沙箱 —— 完成后才申请**小范围生产 Shadow 验证**。

## 8. 任务状态跟踪（AI 迭代时更新本表）

**执行状态（2026-11-15）：P0×4 + P1×6 + REG-01 全量关闭（11 commits，p8-*）；§5 真实集成仍为发布门槛（BLOCKED，外部输入）。**
验收基线：`go test ./...` 与 `go test -race ./...`（PG 55432 / Kafka 19092 resident deps）双绿；`buf lint/build` 通过（零 IDL 变更）；chart 策略矩阵含 P8-P1-02 断言通过。

| ID | 任务 | 状态 | Commit | 测试证据 |
| --- | --- | --- | --- | --- |
| P8-P0-01 | Shadow 权威持久化 | DONE | `8098c20` | 1000 任务跨进程重启零补发（TestShadowAuthoritySurvivesRestart）+ PG generation 隔离矩阵（TestClaimNotifyTasksGenerationIsolation） |
| P8-P0-02 | Edge 多来源上行身份 | DONE | `1fb6982` | 双来源上传+越权 403+禁用源收缩+轮换（TestEdgeUplinkCredentialStore / TestIngestEdgeUplinkMatrix）+ 上传头选择（TestUploaderUplinkCredentialSelection） |
| P8-P0-03 | 飞书/钉钉业务码校验 | DONE | `64b1042` | 200+业务错误码/限流/空体/非 JSON 矩阵（TestVendorBusinessCodeValidation） |
| P8-P0-04 | SSRF DNS 绑定 | DONE | `64b1042` | IP 钉扎拨号 + 混合解析拒绝（TestGuardedDialPinsValidatedIP / TestGuardedDialRejectsBlockedRebind） |
| P8-P1-01 | Readiness 注册时机 | DONE | `f5ae236` | 晚注册 503 / 健康 200（TestLateRuntimeCheckIsObserved） |
| P8-P1-02 | NetworkPolicy 默认出口 | DONE | `c8bacb5` | chart 矩阵：默认公网放行+私网排除、enabled=false 回退、edgeCentralNamespace 跨 ns |
| P8-P1-03 | 静默失败不放行 | DONE | `6b982ee` | DB 错误→retryable 且 sender 未调用（TestSilenceCheckFailureDoesNotSend） |
| P8-P1-04 | Lease 续租/限速 | DONE | `a1419cd` | PG 续租任务不被重领/旧 token fencing（TestRenewNotifyLeaseProtectsFromReclaim）+ 续租顺序/stale 不发送/限速间隔 |
| P8-P1-05 | Fingerprint 并发互斥 | DONE | `bef3bf3` | 8 并发 Firing 单活跃 episode + 第二活跃行被部分唯一索引拒绝（TestConcurrentFiringSameFingerprintSingleEpisode） |
| P8-P1-06 | 聚合器内存淘汰 | DONE | `ca5e608` | 过期淘汰 + Key 配额驱逐（TestAggregatorPrunesExpiredKeys / TestAggregatorKeyCapEvictsOldest） |
| P8-REG-01 | 重复 Firing 刷新观察时间 | DONE | `30c419b` | 持续 firing 观察时间前进、通知=1、历史=1（TestRepeatFiringRefreshesObservation） |
| §5 | 真实集成（发布门槛） | BLOCKED | — | 外部输入；代码侧就绪，待真实 DeepFlow/HUATUO/集群/渠道沙箱 |

---

*评审来源：2026-11-15 第二轮 main 分支实现评审（对照 P7 全部 12 项修复复核 + 新代码边界审查，base e2a1c532 → 4956b7d2，16 commits / 74 文件）。综合结论：可进入生产前整改阶段，暂不切换生产告警权威。*
