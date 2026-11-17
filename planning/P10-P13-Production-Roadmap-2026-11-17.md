# P10–P13 演进路线（生产收敛专项）

- 日期：2026-11-17 · 评审对象：`main@065daaf`（R26）
- 来源：外部架构评审（2026-11-17），**8 项断言已逐条对照代码核实，全部成立**（3×P0、5×P1）。
- 结论：项目已从"功能开发 + P7/P8 可靠性整改"进入"生产前收敛 + 平台能力建设"。下一阶段以正确性收敛为主，**P10 不新增页面和业务功能**。
- 总口径：约 8–12 周，3 Go 并行；P10≈19 人日（见任务表）；P11–P13 任务在各自启动前细化入 backlog。

---

## 一、评审断言核实结论（2026-11-17，逐条对码）

| # | 评审断言 | 级别 | 核实结论 | 代码证据（@065daaf） |
|---|---|---|---|---|
| 1 | 归档游标用事件时间，迟到事件永久漏归档 | P0 | **成立** | `internal/infrastructure/postgres/archive_store.go:43` `WHERE (transition_at, transition_id) > ($1,$2)` —— `transition_at` 是告警状态转换的**事件时间**；Edge 弱网补传的迟到行（事件时间早于游标）永远不会被再次扫描 |
| 2 | 多副本归档无幂等，崩溃/并发重复写入 | P0 | **成立** | `internal/application/archive/service.go` `Run/RunOnce` 无任何租约，每副本都跑（`cmd/alert-worker/main.go` 装配）；CH 表为普通 `MergeTree`（`deploy/clickhouse/0001`），无去重键；CH 写成功→游标推进前崩溃 = 重复归档 |
| 3 | 冷热路由以查询终点 `to` 计算边界 | P0 | **成立** | `internal/application/history/service.go:45` `oldestPossible := to.Add(-pgWindow)` —— 查 10-01→10-02（已出 PG 保留窗）会被误判为热数据走 PG 返回空；跨边界查询只能整段二选一，无法合并 |
| 4 | 指标中心无租户隔离 | P1 | **成立** | `internal/application/metricsquery/service.go:61-100` 全部 catalog 表达式为平台级全局 PromQL（无 tenant 维度）；`internal/adapter/inbound/http/v1/metrics_center.go:31,45` 仅要求 `PermAlertsRead` —— 普通租户用户可见平台全局吞吐/错误率/来源健康 |
| 5 | 通知权威滚动升级期间旧实例可继续领取发送 | P1 | **成立** | committed 代码：`ClaimNotifyTasks` 只比对**任务**的 `authority_generation` 与 worker **缓存**的 generation；`notify/service.go:178-189` `ensureAuthority` 在 `!authorityDirty && generation>0` 时跳过 re-sync —— 旧实例启动后永不复查 DB 当前 authority，DB 切 shadow 后仍按旧纪元领取发送 |
| 6 | PG 数据生命周期不完整 | P1 | **成立** | `internal/application/retention/service.go` 仅清理 receipts/notifications/audit；`event_history`、已结束 instance 周期数据无生命周期；CH 归档仅覆盖 `alert_transitions`；无归档对账，不存在"对账通过才清 PG" |
| 7 | 新增 HTTP API 脱离 IDL | P1 | **成立** | `proto/alert/` 下无 `metrics`/`history` 包；`cmd/alert-api/main.go:116-119` 手写注册 `/metrics/v1/catalog`、`/metrics/v1/query`、`/history/v1/transitions` |
| 8 | 单 worker 二进制七类后台任务无执行所有权 | P1 | **成立** | `cmd/alert-worker/main.go` 同一进程装配 processor/notifier/relay/escalation/retention/telemetry/archive；Kafka processor 有 consumer group，其余靠 DB/自身循环 —— 扩 processor 副本 = 同时扩全部后台任务 |

**与在途 R27 的关系（重要）**：工作区在途的 R27（迁移 0013 + `created_mode` 领取语义）修复的是另一个已确认缺陷——authority 振荡（active→shadow→active）时 ACTIVE 纪元任务被"只认当前 generation"的旧语义**永久搁浅**（REL-002 drill phase 4 要求返回后补投）。R27 把交付资格改为"入队时的 authority mode"，但 **claim 时仍不校验 DB 当前 authority**（且移除了 SQL 中的 generation 比对），断言 5 的 fencing 缺口在 R27 之后依然存在。故 P10-005 的方案必须与 R27 语义**正交组合**：交付资格看 `created_mode`，执行资格（fencing）看 claim 时的 DB 当前 authority。

---

## 二、P10：数据一致性与发布阻断修复（W26–W27，17 人日）

> **2026-11-17 完成**：P10-001..008 全部落地（见 `planning/DEVELOPMENT_LOG.md` R29 / `planning/backlog.csv` DONE）。
> 对应实现：0014 归档队列+租约、ReplacingMergeTree+FINIAL、冷热合并分页、lifecycle 对账清理、
> claim/renew DB fencing、指标分级+tenant matcher、proto metrics/history + hz 路由、WORKER_PROFILE+singleton 租约。

| 任务 | 开发内容 | 验收标准 | 依赖 | 人日 | Owner |
|---|---|---|---|---|---|
| **P10-001 归档游标重构** | `transition_at` 事件时间游标改为提交序来源：首选复用 domain outbox，新增 `TransitionArchiveRequested` 事件由独立 Archive Consumer 按 `transition_id` 幂等消费；若采用提交序扫描，必须处理并发事务晚提交的游标跳跃（提交序来源 + 可重扫窗口 + 唯一去重）。迁移自 0014 起（0013 已被 R27 占用） | 迟到事件、乱序事件、Edge 补传均不漏归档（drill 扩展迟到/补传用例） | — | 3 | Backend-Core |
| **P10-002 CH 归档幂等 + 单写者** | 明确归档语义 = At-Least-Once + Idempotent Write：PG 租约（或 advisory lock）保证单写者；稳定批次 ID；CH 侧按 `transition_id` 去重（ReplacingMergeTree/版本列或查询侧去重）；崩溃恢复后重复写入对账为 0 | 并发双 worker + `kill -9` 恢复场景，CH 中 `transition_id` 无重复业务结果 | P10-001 | 2 | Backend-Core |
| **P10-003 历史冷热查询修复** | 以当前时间与归档水位（而非查询终点 `to`）判断冷热边界；跨边界 = PG 查热区 + CH 查冷区，按 `transition_id` 去重、时间排序、统一分页；`archived` 来源标记正确 | 历史范围 / 近期范围 / 跨冷热窗口三组用例结果完整（对齐 drill 数据） | P10-001/P10-002 | 2 | Backend-Core |
| **P10-004 数据生命周期矩阵与对账清理** | 按"活跃 instance 不清 / transition 已归档且超窗才清 / 原始 event 短期 / 审计按合规"建矩阵（入 docs/05）；归档水位 + PG/CH 按天对账；**对账通过后才允许清 PG 旧数据**，绝不"CH 写成功一次即删" | 对账报表 + only-after-verified 清理用例通过 | P10-001/P10-002 | 3 | Platform |
| **P10-005 通知 Authority DB 侧 Fencing** | claim SQL 原子校验 DB 当前 authority（JOIN `notification_authority`，active worker 要求当前 mode=active）；发送前（RenewNotifyLease）二次复核；周期性 re-sync 补上 `ensureAuthority` 的缓存盲区；定义 Drain/Fencing 语义；与 R27 `created_mode` 交付语义正交组合 | Active→Shadow 滚动升级期间旧实例停止领取（双实例竞争矩阵）；shadow→active 返回后搁浅任务正常补投（REL-002 phase 4 不回归） | R27（0013） | 2 | Backend-Core |
| **P10-006 指标中心租户隔离** | 指标分级：平台运维指标仅 Platform Admin；租户业务指标绑定 `tenant_id` 并在**查询侧强制注入** matcher（不靠前端筛选）；集群/来源指标按授权资源范围；明确租户数上限、指标基数预算与聚合规则（**不简单给所有高基数指标加 tenant 标签**） | 普通租户不可见平台吞吐/错误率与未授权来源（权限矩阵用例）；admin 视图不受损 | — | 3 | Security |
| **P10-007 新增 HTTP API IDL 化** | `proto/alert/metrics/v1` + `proto/alert/history/v1` 正式定义；buf 生成 handler/router/错误结构，替换 `main.go` 手写路由；docs/07 登记契约 | buf lint + 兼容性测试通过 | — | 2 | Backend-Governance |
| **P10-008 Worker Profile 拆分** | `WORKER_PROFILE`（processor/notifier/all 等）拆启动装配角色，同一代码仓不拆服务；retention/telemetry/archive 先上单实例租约（PG advisory lock）；chart 镜像/参数矩阵 | 5 processor 副本不再连带 5 套归档/清理/采样；单实例任务双副本竞争矩阵通过 | P8-005 | 2 | Platform |

P10 合计 **19 人日**（评审口径 2–3 周 × 3 Go，含联调与缺陷修复）。

## 三、P11：分布式能力与容量验证（W28–W29，任务启动前细化）

解读 R26 压测口径：100 EPS×600s 稳态 0 错误（e2e P95 170ms）证明稳态健康；2000 EPS 已验证的是**短时接收能力**（P95 50.7s 为单 Worker 消费积压排水），**不是满足 3s SLO 的持续处理能力**。

| 场景 | 通过标准 |
|---|---|
| Processor 扩容 1/2/3/5/8 副本 | 测量吞吐、PG 锁竞争、扩容效率曲线 |
| 长时峰值 2000 EPS × 600s | 事件完整、延迟满足预先定义的 SLO |
| Kafka 故障（broker/分区迁移/Rebalance） | 状态准确、恢复后无业务丢失 |
| PG 故障（主备切换/连接池耗尽/慢事务） | 安全重试、不重复开告警周期 |
| Edge 弱网（30 分钟断网→恢复→限速补传） | WAL 与中央入库完整对账 |
| 通知渠道故障（429/500/超时/慢响应） | 租约安全、重试准确 |
| 四集群（home/dev/prod-studio/prod-tencent） | 来源、身份、缓冲、通知全链路 |

完成后才能定：Pod 副本、Kafka 分区、PG 连接池、CPU/内存 Requests、HPA 阈值。前置：P10-008（Worker Profile 拆分，见下）。

## 四、P12：告警治理（W30–W31，任务启动前细化）

正确性稳定后的业务深化，仍用**确定性规则**，不引入 AI 自动控制告警生命周期（OAgentKit 保留为后续可选诊断组件）：

- **确定性关联引擎 V2**：Cluster/Node UID/Pod UID/Workload UID/Service + 时间窗口 + DeepFlow 网络关系关联多来源告警；保留原始证据；支持人工解除关联。
- **策略治理中心**：规则版本、模拟执行、批量校验、静默冲突检测、通知升级、策略灰度；每次变更可预览受影响告警。
- **告警质量分析**：重复告警率、通知抑制率、无人确认率、MTTA/MTTR、按来源误报率、关联降噪效果。

## 五、P13：生产灰度与 V1.0 发布（W32–W33，证据清单）

**证据齐备才定 V1.0 日期**（当前不给确定日期）：

- [ ] DeepFlow v7.2.4 真实告警与恢复事件（推送为 DeepFlow 企业版能力，需真实联调，不能 Mock 替代）
- [ ] HUATUO 真实 SSE/CloudEvents 数据
- [ ] 飞书/钉钉真实沙箱发送与错误反馈
- [ ] 四集群 Edge 补传及网络故障演练
- [ ] Kafka/PostgreSQL 多副本高可用验证
- [ ] CH 归档完整性与历史数据恢复验证
- [ ] Shadow 实际对账 + 通知权威切换与回滚演练
- [ ] Worker 在真实集群启用（P9 集群验证仅覆盖 api/edge 指标面，**不能等同分布式部署验收**）

## 六、优先级总表

| 方向 | 时点 | 判断 |
|---|---|---|
| 修复归档/冷热查询/通知权威/租户隔离（P10） | **立即** | 生产前正确性阻断项 |
| Worker Profile 拆分（P10-008，随 P10 收尾） | 下一期前 | `WORKER_PROFILE` 启动装配拆角色（同仓不拆服务）；retention/telemetry/archive 单实例租约；为 P11 扩容实验前置 |
| 2000 EPS 长稳 + 容量基线（P11） | 下一期 | 生产容量决策依据 |
| 真实 DeepFlow/HUATUO/渠道联调（P13，可并行） | 下一期并行 | 发布必需 |
| Incident 关联/策略治理/质量分析（P12） | 正确性稳定后 | 业务价值高 |
| Vue3 正式前端工程 | 按产品投入 | 非首要可靠性瓶颈 |
| OAgentKit 智能诊断 | V1.0 后 | 不进核心链路 |
| Kitex 微服务拆分 | 暂缓 | 缺少必须拆分的同步调用场景；核心数据面继续走 Kafka 异步 |
