# 11 · 接口目录、响应与错误契约

## 外部来源

- `POST /ingest/external/alertmanager/v2/webhook`：AM 原生 JSON，按 `alerts[]` 单条映射。仅以注册来源身份处理。
- `POST /ingest/external/deepflow/v7_2/events`：DeepFlow v7.2.3 自定义 Push Bridge JSON；真实模板和 Payload 未验证前不宣称兼容。
- HUATUO SSE 属于 Edge 主动订阅，非中心对外 Webhook。

## Edge 与中心

- `POST /ingest/v1/events:batch`：Protobuf `PushBatchRequest`，按整批校验/持久化返回 202，包含 durable batch ID。**响应不携带逐条接受计数**（`accepted_count` 已 reserved，见 docs/04 §6）；逐事件 ACK 若引入必须是新接口。

## 管理接口（规划）

- `GET/POST /api/v1/sources`
- `GET /api/v1/alerts`；`GET /api/v1/alerts/{id}`
- `GET /api/v1/incidents`；`GET /api/v1/incidents/{id}`
- `POST /api/v1/incidents/{id}:ack`；`POST ...:close`
- `GET/POST /api/v1/policies`
- `GET/POST /api/v1/silences`
- `GET/POST /api/v1/notification-routes`
- `GET /api/v1/notification-attempts`
- `GET /api/v1/edges/health`

## HTTP 约定

- `202`：已可靠入队，不代表处理成功或通知送达。
- `400`：格式或语义错误；`401/403`：认证/授权错误；`409`：版本冲突或重复请求语义冲突。
- `413`：Payload 超限；`429`：来源限流，可重试且尊重 Retry-After。
- `503`：暂不可用且没有可靠 ACK；上游根据协议重试。
- 返回包含 `code/message/request_id/details` 的结构化错误，不泄露令牌及内部 SQL。

## 幂等约束

- 外部源 Webhook 由来源协议提取事件去重键。
- Edge 上传统一使用 `transport_id`，网络重试不变。
- Incident ACK/Close 支持 `Idempotency-Key` 与 `expected_version`，冲突时 409 并要求刷新。
- 前端不得依据 HTTP 202 推测通知已成功。
