# 分布式统一告警平台 · 项目交接包（2026-11-12）

> **本包定位**：给研发团队启动项目的 **PRD / 技术架构 / IDL 契约 / 数据设计 / 20 周排期 / Backlog / 测试验收包**，附有可测试的 Go 状态机参考实现。**不是已经实现的完整生产系统**，也没有宣称能够一键部署告警平台。

## 关键决策

- **目标**：面向 DeepFlow、Prometheus/Alertmanager、HUATUO 的统一告警汇聚、生命周期、Incident 关联及可靠通知。
- **语言/框架**：Go + CloudWeGo Hertz；按需引入 OEKit 基础能力；Kitex 在出现独立同步 RPC 需求时再引入；OAgentKit **不接入核心链路**。
- **契约**：IDL First，Protobuf v1；Buf lint / breaking；Hertz HTTP 层由 hz 根据 IDL 生成，注意固定版本。
- **DeepFlow**：适配族 `deepflow/v7_2`。截至 2026-11-12 复核，v7.2.3（2026-10-15）之后 **v7.2.4 已于当日发布并成为 Latest**；开源仓库的官方告警 proto/枚举已取证（见 `docs/06`），企业版推送格式与 Jinja 变量**必须在真实目标实例上核验**，W1 决策目标小版本。
- **消息与存储**：Kafka 用于传输与可重放窗口；PostgreSQL 负责状态、唯一约束、事务和 Outbox；ClickHouse 用于后期历史分析；Edge 负责本地缓冲。
- **交付节奏**：W9 MVP、W12 多集群、W17 Incident、W20 生产发布（基于 3 Go + 1 FE + 1 QA + 0.5 SRE（W18–W20 升配 1.0）+ 0.5 PM 的人员假设；backlog 已含集成缓冲，Go 峰值周利用率 93%，容量模型见 `planning/README.md`）。

## 项目组应该先读什么

1. `docs/00-交付说明与待确认项.md` — 能做什么 / 不能做什么 / 必须确认什么。
2. `docs/01-产品需求与范围.md` — PRD 和使用场景。
3. `docs/02-总体架构.md` — 服务拓扑和部署边界。
4. `docs/03-事件模型与状态机.md` — 业务正确性的核心。
5. `docs/04-消息可靠性与一致性.md` — ACK、Offset、Outbox、重试。
6. `docs/05-存储与数据库.md` — 表结构和读写策略。
7. `docs/06-DeepFlow-v7.2.3-适配.md` — 最新版适配边界。
8. `docs/07-IDL与代码生成规范.md` — Proto、Buf、hz、兼容性。
9. `docs/08-安全运维与部署.md` — HA、认证、隔离、指标。
10. `docs/09-测试与验收.md` — 合约、E2E、压测、混沌测试。
11. `docs/10-研发排期与里程碑.md` — 20 周阶段与闸门。
12. `docs/13-分阶段任务验收标准.md` — 80 个任务的逐条二值验收标准与证据要求。
13. `planning/backlog.csv` — 可导入工单系统的任务清单。
14. `planning/week_schedule.csv` — 全量周级计划。

## 目录结构

```text
proto/                  Protobuf IDL（含 Hertz api 注解扩展）
contracts/external/     DeepFlow Bridge JSON Schema 和合成示例
migrations/             PostgreSQL 基础迁移 SQL（初稿；上线前审计）
internal/domain/        标准库 Go 状态机参考实现 + 单元测试
api/                    API 合约和响应/错误约定
planning/               Backlog、20 周排期、团队分工、准入清单
docs/                   PRD、架构、决策与测试验收
scripts/                交付包结构检查脚本
.github/workflows/      参考 CI 门禁
```

## 即刻可执行的命令

在本包根目录：

```bash
python3 scripts/validate_package.py
GOWORK=off go test ./...
# 装有 buf 后：
buf lint
buf build
# 在有 main 基线的 Git 仓库中：
buf breaking --against '.git#branch=main'
# 安装 protoc-gen-go 后：
buf generate
```

`go test` 只覆盖自包含的领域示例，并不表示实现了 Kafka、数据库、Hertz 或通知链路。`buf` 工具在制作交付包的环境中**未安装，因此 Buf 编译/生成仍需在团队 CI 环境执行**。

### 关于 hz

`proto/alert/ingest/v1/ingest.proto` 包含 `(api.post)` 注解，`proto/api/api.proto` 是提供最小 Hertz 方法注解的本地声明。需要由项目工程师使用**固定版本**的 `hz new` / `hz update` 集成 Hertz 路由；不要手工修改生成代码。参见 `docs/07-IDL与代码生成规范.md`。

## 实施前的必填清单

- [ ] 确认实际 Go module / 仓库地址，并替换所有 `github.com/example/alert-platform` 占位。
- [ ] 确认 DeepFlow v7.2.3 的部署形态、授权能力、原生推送 Payload 和 Jinja 模板字段。
- [ ] 确认 Kafka 和 PostgreSQL 是否由平台团队提供已有生产集群。
- [ ] 确认通知渠道及 P0/P1 值班升级需求。
- [ ] 确定数据保留、审计合规、租户隔离与跨区域数据出境要求。
- [ ] 按 `docs/09-测试与验收.md` 设立阶段准入测试。

## 证据来源（复核日期 2026-11-12）

- DeepFlow Releases：https://github.com/deepflowio/deepflow/releases （v7.2.4 于 2026-11-12 成为 Latest；v7.2.3 发布于 2026-10-15）
- DeepFlow 官方告警 proto：https://github.com/deepflowio/deepflow/blob/main/message/alert_event.proto （`event_id`/`state`/`start_time`/`end_time`/`policy_id` 等字段）
- DeepFlow 官方枚举：https://github.com/deepflowio/deepflow/tree/main/server/querier/db_descriptions/clickhouse/tag/enum （`event_level` 1–6、`state` 0/1、`policy_app_type`）
- DeepFlow 告警表描述：https://github.com/deepflowio/deepflow/tree/main/server/querier/db_descriptions/clickhouse/tag/event （`alert_event`/`alert_record` 资源标签体系）
- DeepFlow Push Endpoint：https://blog.deepflow.io/docs/guide/ee-tenant/alert/push-endpoint/ （公开文档位于企业版租户告警章节，支持类型随授权核实）
- CloudWeGo hz Protobuf：https://www.cloudwego.io/zh/docs/hertz/tutorials/toolkit/usage-protobuf/
- Buf breaking：https://buf.build/docs/breaking/usage/
- OEKit：https://oekit.github.io/
- OAgentKit：https://oagentkit.github.io/

**请勿将 `contracts/external/deepflow/v7.2.3/fixtures/SYNTHETIC-bridge-event.json` 当作 DeepFlow 官方真实 Payload。**
