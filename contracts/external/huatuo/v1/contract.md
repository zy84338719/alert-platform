# HUATUO `/v1/events/watch` 契约（P0-04 固化 · 2026-11-15）

> 来源：官方文档 docs.huatuo.tech（ccfos/huatuo，文档许可 CC BY 4.0），
> 抓取自 `/en/latest/best-practice/events-watch/` 与
> `/en/latest/reference/instant-observability/`（2026-11-15）。
> fixtures 中的 payload 为 **SYNTHETIC**：字段结构与官方一致，值为合成 ——
> 真实环境联调仍以 ARC-002/HUATUO 真实样本任务为准，不以本目录替代最终验收。

## 1. 订阅握手

`POST /v1/events/watch`（**不是 GET**），持久连接收流：

```bash
curl -s -N -X POST http://<node-ip>:19704/v1/events/watch \
-H "Authorization: Bearer <node-token>" \
-H "Content-Type: application/json" \
-H "Accept: text/event-stream" \
-H "Cache-Control: no-cache" \
-H "Connection: keep-alive" \
-d '{"filters": {"tracer_name": "^memory_oom_kill$"}}'
```

请求体（全部可选，正则，AND 语义）：

```json
{"filters": {"tracer_name": "", "hostname": "", "container_hostname": "",
 "container_host_namespace": "", "container_qos": "", "region": ""}}
```

认证：`Authorization: Bearer <node-token>` 请求头（无 URL token 参数）。
连接数超限返回 HTTP 429。

## 2. 事件流格式

SSE `data:` 行 = **CloudEvents 1.0** JSON 对象（每条一个）：

| 字段 | 值 |
| --- | --- |
| `specversion` | 恒为 `"1.0"` |
| `id` | UUID v4，事件唯一 ID |
| `source` | `/huatuo/{hostname}/{tracer_name}` |
| `type` | 恒为 `"tech.huatuo.kernel.event"` |
| `datacontenttype` | 恒为 `"application/json"` |
| `time` | RFC 3339 纳秒精度 UTC |
| `data` | WatchEventData（见下） |

## 3. WatchEventData（`data` 字段）

| 字段 | 说明 |
| --- | --- |
| `hostname` | 节点主机名 |
| `region` | 节点区域 |
| `observed_timestamp` | 用户空间观察时间，ISO 8601 含纳秒与时区 |
| `uploaded_timestamp` | 上传时间 |
| `tracer_name` | **事件名**（如 `memory_oom_kill`、`sched_tick`、`softlockup`） |
| `tracer_id` / `tracer_run_type` / `tracer_type` | 追踪标识（`tracer_type` 恒为 `event`） |
| `container_id` / `container_hostname` / `container_host_namespace` / `container_type` / `container_qos` | 容器关联（仅有关联时出现） |
| `tracer_data` | 事件专属数据（如 OOM 的 trigger/victim 结构） |

官方**没有** `severity` 字段 —— 平台侧一律 INFO（OCCURRENCE），
突发升级（promote）时由聚合器赋 ERROR（现状语义不变）。

## 4. 心跳

服务端周期写 SSE 注释行 `: ping`（`EventStreamKeepAliveIntervalSeconds`，
默认 30s，建议低于上游 LB 空闲超时）；连续 3 次写失败关闭连接。
客户端必须把 `: ping` 计入"最后活跃时间"——按固定定时器强断会周期性重连。

## 5. 平台侧映射（P0-04/P0-05 实现契约）

| CloudEvent / WatchEventData | CanonicalEvent |
| --- | --- |
| `id` | transport_id / event_id / event_key（观察身份；CE id 稳定唯一 → 重投递幂等） |
| `data.tracer_name` | alert_name 尾段 + external_rule_id（`huatuo-<tracer_name>`） |
| `data.observed_timestamp`（缺省回退 `time`） | occurred_at |
| `data.hostname` | 节点身份：resource_uid（无容器时）、resource_name |
| `data.container_id` | 容器身份：resource_uid（有容器时，稳定实体 UID） |
| `data.container_hostname` / `container_host_namespace` | resource_name / namespace |
| `data.region` / `hostname` / 容器字段 | labels |
| `data.tracer_data`（原样） | raw_payload |

## 6. fixtures

- `fixtures/SYNTHETIC-cloudevent-oom.json`：单条 memory_oom_kill CloudEvent（含容器身份）。
- `fixtures/SYNTHETIC-cloudevent-schedtick.json`：节点级事件（无容器身份）。
- `fixtures/SYNTHETIC-sse-stream.txt`：完整 SSE 流样本（心跳注释 + 三条事件）。
