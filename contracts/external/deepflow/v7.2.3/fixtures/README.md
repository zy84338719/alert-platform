# DeepFlow Fixture 说明

`SYNTHETIC-bridge-event.json` 只是我们建议的 **Jinja Push 输出契约** 的人工合成样例，**不是 DeepFlow v7.2.3 原生告警 Payload**。

请在 W1 收集真实脱敏样本并命名 `REAL-<level>-<case>.json`，附元数据：DeepFlow 版本/授权类型、采集时间、策略类别、源端推送配置、变量映射、重试行为。

通过真实数据核验前，Adapter 任务不得标记 DONE。
