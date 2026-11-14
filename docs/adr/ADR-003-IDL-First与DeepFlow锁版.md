# ADR-003 · Protobuf IDL First，DeepFlow 适配族 v7_2 锁版

状态：已接受（2026-11-14；依据：IDL-First 工作流经 R7–R14 全部服务的 buf/hz 生成与 breaking 基线实跑验证，锁版决策由 ARC-002 采样挂账跟踪）（2026-11-12 修订）。内部事件和服务契约使用 Protobuf v1，Buf build/lint/breaking；Hertz hz 根据 API IDL 生成。DeepFlow 适配锁定**适配族 `deepflow/v7_2`**：以 v7.2.3 为已验证基线（2026-10-15 发布），v7.2.4 于 2026-11-12 成为 Latest——W1 必须显式决策目标小版本，小版本差异用 fixtures 增量消化而非新开适配族。字段语义以开源仓库官方证据为准：`message/alert_event.proto`（`event_id`/`state`/`start_time`/`end_time`/`policy_id`）与 querier 枚举（`event_level` 1–6、`state` 0=Ongoing/1=Ended）。企业授权/真实 Jinja 推送字段必须以目标环境和实际样本核对；不得把合成 Bridge 数据当作官方协议。
