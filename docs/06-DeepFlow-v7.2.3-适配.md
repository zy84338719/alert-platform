# 06 · DeepFlow 适配规范与风险（适配族 deepflow/v7_2）

## 1. 已核实与未核实（复核日期 2026-11-12）

**版本事实**（GitHub Releases）：

- v7.2.3 发布于 2026-10-15；**v7.2.4 于 2026-11-12（本包定稿当天）发布并成为 Latest**。
- 因此 W1 必须显式决策目标小版本：锁定已验证的 7.2.3，还是跟随 7.2.4（需重新采样）。适配族目录 `deepflow/v7_2/` 覆盖 7.2.x，小版本差异以 fixtures 增量消化（R12）。

**开源仓库代码证据**（`deepflowio/deepflow` main 分支，2026-11-12 取证）：

- `message/alert_event.proto`：官方 `AlertEvent` / `AlertRecord` protobuf 定义。`AlertEvent` 关键字段：`event_id`(string)、`state`(uint32)、`start_time`/`end_time`/`duration`/`alert_time`、`policy_id`/`policy_type`/`alert_policy`、`event_level`(uint32)、`target_tags`、`tag_str/tag_int` 数组、`org_id`/`user_id`/`team_id`、`trigger_threshold`、`metric_value(_str)`/`metric_unit`、`custom_tag_*`、`_target_uid`、`_query_region`。`AlertRecord.event_id` 可回链聚合事件。
- 告警落库链路：`server/ingester/event/dbwriter/alert_event_writer.go` 写入 ClickHouse `event.alert_event` / `event.alert_record`；querier 侧有对应表描述与枚举（`server/querier/db_descriptions/clickhouse/tag/event/`）。
- **官方枚举**（`tag/enum/event_level.*`、`tag/enum/state.*`、`tag/enum/policy_app_type.*`）：
  - `event_level`：1=Critical(致命) 2=Error(错误) 3=Warn(警告) 4=NoData(无数据) 5=Recovered(恢复) 6=Info(信息)
  - `state`：0=Ongoing(持续中) 1=Ended(已结束)
  - `policy_app_type`：1=系统阈值 3=自定义阈值 4=业务阈值 5=业务组合
- 资源标签体系（`tag/event/alert_event` 描述文件）：`pod_cluster/pod_ns/pod_node/pod_service/pod/pod_group`、`auto_instance/auto_service`、`host/chost/vpc/subnet/lb/rds/redis` 等，可映射平台 `ResourceRef`。
- **开源仓库没有对外推送（Push/Webhook）告警的代码**；公开文档的 Push Endpoint 位于企业版租户告警章节。即：数据模型有代码证据，**推送通道与 Jinja 变量仍是企业版行为，未核实**。

**仍未核实**：目标实例的企业版授权、推送 Payload 实际结构、Jinja 可用变量、推送失败重试行为、`event_level`/`state` 在推送模板中的暴露形式。

参考：

- https://github.com/deepflowio/deepflow/releases/tag/v7.2.3 （2026-10-15）
- https://github.com/deepflowio/deepflow/releases/tag/v7.2.4 （2026-11-12，Current Latest）
- https://github.com/deepflowio/deepflow/blob/main/message/alert_event.proto
- https://github.com/deepflowio/deepflow/tree/main/server/querier/db_descriptions/clickhouse/tag/event
- https://blog.deepflow.io/docs/guide/ee-tenant/alert/push-endpoint/ （企业版文档；支持类型随授权核实）

因此 **绝不把本包 JSON Schema 和合成样例声称为 DeepFlow 官方原生格式**；但等级/状态/身份字段的语义以上述开源枚举与 proto 为准，不再是猜测。

## 2. 推荐模式

DeepFlow 企业版告警 Push -> `POST /ingest/external/deepflow/v7_2/events` -> Bridge Schema 校验 -> Adapter -> `alert.event.v1.AlertEvent` -> Edge 本地持久化 -> 中央 Kafka。

HTTP 优先，Kafka 原生直推作为可选实现。避免向 DeepFlow 暴露中央 Kafka 生产权限。

## 3. Bridge Schema

目录：`contracts/external/deepflow/v7.2.3/bridge.schema.json`（适配族 7.2.x 共用，小版本样本差异放 `fixtures/`）。

**P7/P1-05（2026-11-15）：v7.2.4 基线落地。** 取证（gh api）：
- `compare/v7.2.3...v7.2.4` 11 个变更文件均为 agent dispatcher / agent_config 文档与测试，**零** alert 路径（message/、clickhouse tag/enum/）变更；
- `message/alert_event.proto` 双 tag sha256 **逐字节一致**（`2658e869…57`）。
结论：v7.2.4 是运行时基线升级、payload 契约不变；`contracts/external/deepflow/v7.2.4/`（mapping/bridge.schema/golden fixtures）+ 跨版本契约等价测试（TestDeepFlowV723V724ContractParity / TestDeepFlowV724Baseline）为升级证据，适配族保持 `deepflow/v7_2` 不变。真实 Payload 验收仍挂 ARC-002。

Schema 描述的是本项目希望 DeepFlow 自定义 Jinja Push 最终输出的 JSON，而非 DeepFlow 内置结构。真实接入时先获取原始样本与模板变量清单，再完成 `mapping.yaml`。字段语义现在可以对照官方 proto 命名：

| Bridge 字段 | 是否必须 | 官方对应（证据） | 说明 |
| --- | --- | --- | --- |
| `schema_version` | 是 | — | `deepflow.bridge.v1` |
| `source_id` | 是 | — | 注册的 DeepFlow 实例 |
| `source_version` | 是 | — | 实际推送实例版本（7.2.3/7.2.4） |
| `policy_id` | 是 | `policy_id` + `policy_type`/`policy_app_type` | 策略唯一标识 |
| `event_level` | 是 | `event_level`（1–6，官方枚举） | 1/2/3/4/5/6，映射见 §4 |
| `alert_name` | 是 | `alert_policy` | 可读名称 |
| `occurred_at` | 是 | `alert_time`/`time` | RFC3339，注意秒级精度与时区 |
| `target_key` | 是 | `_target_uid` / `target_tags` | 目标稳定身份，不能只有瞬时 IP |
| `external_event_id` | 强烈建议 | `event_id`(string) | **官方聚合事件自带稳定 ID，恢复关联首选键** |
| `event_state` | 强烈建议 | `state`（0=Ongoing 1=Ended） | 配合 `start_time/end_time/duration` 判定生命周期 |
| `starts_at`/`ends_at` | 建议 | `start_time`/`end_time` | episode 对齐证据 |
| `labels` | 否 | `tag_str_*`/`custom_tag_*` | 维度标签，限定 key、数量和大小 |
| `details_url` | 否 | — | DeepFlow 控制台跳转 |

## 4. 等级与事件语义（官方枚举背书）

| 官方 event_level | 官方含义 | 平台 Signal / Severity |
| --- | --- | --- |
| 1 | Critical 致命 | FIRING / CRITICAL |
| 2 | Error 错误 | FIRING / ERROR |
| 3 | Warn 警告 | FIRING / WARNING |
| 4 | NoData 无数据 | NO_DATA / ——（不自动认为 resolved） |
| 5 | Recovered 恢复 | RESOLVED |
| 6 | Info 信息 | INFO / INFO（默认不打扰值班） |

**恢复关联键优先级**：`event_id`（同一聚合事件的 Ongoing/Ended）> `policy_id + target_key(+维度)` + `start_time` 对齐 > 仅名称匹配（**禁止**，T-016）。`state=1(Ended)` 且 `end_time` 与活动 episode 的 `starts_at` 对得上才允许关闭；对不上时进入待对账，不盲关。

部分 DeepFlow 事件可能是 occurrence（如 `policy_app_type` 组合告警的瞬时触发），需按源的告警策略类型识别，不能只用等级判断是否 Stateful。

## 5. W1 合约采样门槛

1. **决策目标小版本**：7.2.3（已验证基线）vs 7.2.4（Latest），并确认目标部署的授权模式。
2. 收集真实样本覆盖 `event_level` 1–6 与 `state` 0/1 的组合（Critical/Warn/NoData/Recovered 至少各一条）。
3. 采集模板可用变量及其空值行为，记录时间字段时区与精度（`time` 为 uint32 秒）。
4. 测试源端推送失败、重试、重放、批次格式和幂等字段（`event_id` 是否随重发保持不变）。
5. 将脱敏样本存入 `fixtures/REAL-*.json` 并标明来源、版本、采集时间和脱敏步骤。
6. 更新 `mapping.yaml` 与 `bridge.schema.json`；为所有字段建立契约测试。

## 6. 无企业版原生告警推送时的后备方案

若目标部署不提供 Push 能力，按优先级：

1. 通过 DeepFlow 导出的指标由 Prometheus 规则计算告警，经 Alertmanager Webhook 接入（**不是** DeepFlow 原生告警行为的等价替代）。
2. 经 querier SQL API 周期拉取 `event.alert_event`（开源代码已证实该表存在，见 §1）。这违反 docs/00“不直接读取数据库”的默认边界，启用前需单独 ADR 评审拉取周期、时间戳边界与重复语义。

该问题应在 W1 作为明确阻塞风险上报（R01），不能留到 W4 才发现。
