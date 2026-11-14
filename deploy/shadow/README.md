# Shadow 上线 Runbook（MVP-002）

> 状态：**工具与流程就绪；实跑 BLOCKED**——等待首个真实集群接入（上游 DeepFlow/Alertmanager
> 生产实例与通知流导出权限）。属 docs/13 MVP-002 验收的外部依赖项，已在 backlog 标注。

## 1. 影子上线定义

平台与上游通知系统**并行接收同一告警流**，但只有上游持有通知权（平台渠道处于 disabled 或
指向回环采集端点）。运行 ≥3 天后产出对账差异报告；差异全部可解释才允许进入切换评估。

## 2. 步骤

1. **接入镜像**：在目标集群部署 alert-edge（gateway 模式），上游 Webhook 同时（或经镜像
   分流）指向 edge 与原通知链路。上游保持通知权。
2. **平台侧回环**：租户默认渠道指向平台自身的 `shadow-sink`（一个只记录不外发的
   webhook 渠道；本地可用 `scripts/e2e_mvp.sh` 中的 mock 接收器充当）。
3. **导出两侧记录**：
   - 平台侧：`SELECT idempotency_key, created_at, status, notification_type, receiver_id,
     alert_instance_id FROM notification_outbox WHERE created_at > now()-interval '3 days'`
     导出为 JSONL（补一列 alert_fingerprint：join alert_instances）。
   - 上游侧：从上游通知系统导出同期投递记录（fingerprint/时间/状态/接收人）为 JSONL。
4. **对账**：`python3 scripts/shadow_compare.py --upstream up.jsonl --platform plat.jsonl
   --out report.json`。差异必须逐条归因：静默（平台拦、上游发）/ 延迟窗口 / 渠道重试 /
   规则差异。不可解释差异 = 不得切换。
5. **回滚演练（必做 1 次）**：影子上线期间任意时刻执行——停掉 edge 或在 edge 配置
   `enabled=false` 的来源 → 确认上游通知不受任何影响；重新启用 → 缓冲补传完整。

## 3. 判定

- 上游通知零干扰：影子上线前后上游投递成功率无变化；
- 差异报告产出且全部可解释；
- 回滚演练 1 次成功。

满足三项后 MVP-002 的 BLOCKED 解除，进入评审（MVP-003）。

## 4. 已知边界

- Shadow 期间平台通知一律不外发（渠道 disabled 或回环），杜绝双发；
- SSE/HUATUO 流不在 shadow 范围（P4 才接入）；
- 对账工具按 (fingerprint, type, 时间窗) 匹配，窗口默认 120s 可调。
