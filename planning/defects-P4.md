# P4 联调缺陷台账（BUF-001）

> R7–R12 开发过程中由测试/演练抓出并已修复的缺陷全集。定级：S1=可致数据错误/丢失，S2=功能不可用，S3=体验/噪声。全部关闭；未关闭项为外部依赖（见状态列）。

| # | 级别 | 缺陷 | 发现方式 | 状态 |
| --- | --- | --- | --- | --- |
| D-01 | S1 | WAL `Stats()` 持锁调用 `Pending()` 自死锁——中心宕机后 edge 全部 vendor push 挂起（无响应不落盘） | T-011 演练实跑 | 已修（pendingLocked 内联） |
| D-02 | S1 | DeepFlow event_key 直接用官方 event_id：恢复事件（同聚合 ID、state=1）被 receipts 当重复吞掉，告警永不 Resolved | MVP E2E | 已修（event_id#state） |
| D-03 | S1 | hz 生成 `{param}` 路由段被 Hertz 按字面量注册：全部参数路由 404（详情/ACK/启停用） | 路由探针测试 | 已修（merge 脚本归一 `:param`） |
| D-04 | S1 | receipt `ON CONFLICT (transport)` 单层幂等：同 event_key 换 transport_id 重投撞唯一索引报错进 DLQ | 代码审查（R8 重写） | 已修（ON CONFLICT DO NOTHING 双层） |
| D-05 | S2 | 对账把 `last_transition_at` 覆写为服务器时钟：紧随其后的真实 firing 被"乱序保护"误杀，stale 无法恢复 | PG 集成测试 | 已修（保留来源权威锚点） |
| D-06 | S2 | FindByTokenHash 在 SQL 过滤 enabled：禁用源报 401（应 403），语义错误 | ING 集成测试 | 已修 |
| D-07 | S2 | ListAlerts 过滤器拼接漏占位符（`AND severity` 裸列名） | 查询集成测试 | 已修 |
| D-08 | S2 | FinishNotifyTask 重试分支多余 nil 参数：PG 无法推断参数类型，429 重试记录失败 | 通知管线测试 | 已修 |
| D-09 | S2 | 控制台 app.js 正则转义错误→整脚本不加载；`body` 变量遮蔽参数 TDZ；统一信封未解包 | 浏览器实测 | 已修（+no-store） |
| D-10 | S3 | hz 多 IDL 生成同包 Register 冲突；master IDL 方案 shadowing/unused-import 失效 | R7 工具链 | 已修（顺序执行+合并脚本） |
| D-11 | S3 | 旧进程占用端口致"修复不生效"假象（go run 子进程未随父进程终止） | 演练排查 | 流程规避（pkill 清场写入脚本） |

外部依赖未关闭项：见 backlog BLOCKED 行（ARC-002 真实样本、NOTI-002 沙箱、MVP-002/MC-001/EDGE-003 集群通道）。
