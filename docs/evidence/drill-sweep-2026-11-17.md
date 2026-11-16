# 能力回归 Drill Sweep（2026-11-17）

用当前最新二进制把仓库自带可靠性 drill 全部重跑，验证 P7–P9 改动没有破坏既有能力，
同时修复 drill 自身在鉴权/fail-stop 语义变更后的陈旧假设。**全部 PASS。**

## 结果总览

| Drill | 能力 | 结果 | 本轮发现/修复 |
| --- | --- | --- | --- |
| REL-002 | 通知权 shadow→active 切换/对账/回滚 | **PASS** | **P0：active 期任务被 generation 语义搁浅** → created_mode 修复 |
| T-011 | Edge 断网 WAL 缓冲 100% 补传 | **PASS** | drill 陈旧（edge 上传 key 未注册）→ 升级为 P8 上行身份路径 |
| T-010 | PG 故障：零误提交/100% 收敛/零重复 | **PASS** | 认证依赖 PG、pgxpool 无限阻塞、loadgen 丢弃失败批次 → 三处修复 |
| PITR | 任意时间点恢复 | **PASS** | — |
| MVP-001 | push→notify→ack→recover→resolved→audit | **PASS** | — |
| 兜底自告警 | 关键任务 fail-stop → 独立 webhook | **PASS** | 真进程实测送达 |

## 本轮发现并修复的代码缺陷

1. **P0 · 通知可投递性语义（REL-002 phase 4）**
   P8-P0-01 用"当前 generation"限定 claim，active→shadow→active 往返会把 **active 期创建的
   合法任务搁浅**（永不投递）。修复：迁移 0013 `notification_outbox.created_mode` —— 入队时记录
   授权模式；claim 只投 `created_mode='active'`。结果：
   - shadow 期任务永不补投（P0-07 风暴守卫保持）；
   - active 期任务被 shadow 打断后，回 active 补投恰一次（REL-002 本意）。
   回归：`TestClaimNotifyTasksCreatedModeIsolation`、`TestShadowToActiveNoHistoryReplay` 等。

2. **P1 · PG 全断时 processor 挂起而非 fail-stop**
   `pgxpool.Acquire` 在数据库不可达时可无限等待，P1-03 的关键任务 fail-stop 被架空。
   修复：`processor.Handle` 对 `ApplyEvent` 加 15s 上限（`storeCallTimeout`），错误必然浮出。

3. **P1 · 接入认证依赖 PG，故障期间推送被 500 拒绝**
   P7-P0-03 后 ingest 每次认证查 `source_credentials`，PG 故障 → fail-closed 500 → 事件进不了
   Kafka，"断库期间继续接入"能力丧失。修复：`sourcereg.CachedStore` 正缓存 30s +
   管理写（Create/SetEnabled/Revoke）立即清空；负结果不缓存。PG 故障窗口内接入继续入队。

4. **P2 · loadgen 丢弃失败批次**
   生产语义要求批次重试（同 batch_id）；loadgen 现在对失败批次有界重试 5 次，T-010 收敛前提成立。

## Drill 自身修复（陈旧假设）

- REL-002 phase 4 原脚本假设"shadow 期任务切回 active 后补投"，与 P0-07 风暴守卫冲突；
  已重写为两个确定性场景：4a active 期在途任务（渠道临时下线 → retry）不丢不重发；
  4b shadow 期任务永不补投且保持 pending 供审计。
- T-011 edge 上行 key 未注册（P7 后 401）→ 改用 `edge_uplink_credentials` 上行身份
  （X-Edge-Id + token + allowed_source_ids），顺带真实进程验证 P8-P0-02。
- T-010 worker 在故障前未启动（断言空转）、DB rename 混沌存在竞态（既有连接跨 rename 存活）、
  loadgen 无凭证 → 改为凭证注入 + 容器级 stop/start 混沌 + 停机期间推事件触发确定 fail-stop +
  有界重试生产者。

## 复现

```bash
make dev-deps
bash scripts/drill_rel002.sh
bash scripts/drill_t011.sh
bash scripts/drill_t010.sh
bash scripts/drill_pitr.sh
bash scripts/e2e_mvp.sh
```

## 限制

- 以上均为本地单节点规模；四集群/生产容量演练仍属外部挂账（MC-001/P8-012）。
- T-010 的 `docker stop dev-postgres-1` 会短暂中断本地常驻依赖，脚本 cleanup 会恢复。