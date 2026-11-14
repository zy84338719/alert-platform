# DeepFlow v7.2.4 Golden Fixtures（SYNTHETIC · P7/P1-05）

字段结构与 v7.2.3 golden 一致 —— 这是**取证结论而非偷懒**：

- `gh api compare/v7.2.3...v7.2.4`：11 个变更文件全部是 agent dispatcher 与
  agent_config 文档/迁移测试，**零** alert 路径（message/、clickhouse tag/enum/）变更。
- `message/alert_event.proto` 双 tag sha256 **逐字节一致**
  （2658e869…57）。

因此 v7.2.4 基线 = 运行时升级，payload 契约不变；adapter 族保持
`deepflow/v7_2` 不动（禁止只改版本字符串充当升级——本目录 + 跨版本契约
等价测试才是升级证据）。真实 Payload 验收仍挂 ARC-002（真实样本采集），
合成样本只作回归门槛。
