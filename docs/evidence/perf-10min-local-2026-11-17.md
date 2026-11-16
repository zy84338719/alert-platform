# P9 10 分钟长稳压测（本地 · 2026-11-17）

## 环境

- 本地真实链路：`loadgen → alert-api（ingest/Kafka）→ alert-worker（processor/notify/relay/…）→ PG 55432 / Kafka 19092`
- 单 worker 副本（本地规模上限 ~450 EPS；生产按 P8 容量阈值扩副本）
- 执行：`STEADY_DURATION=600s BURST_DURATION=60s DRAIN_WAIT=300 bash scripts/perf_capacity.sh`
  （脚本已参数化：`STEADY_DURATION` / `BURST_DURATION` / `DRAIN_WAIT`）

## 结果（EXIT=0，全程 0 错误）

| 阶段 | 指标 | 数值 |
| --- | --- | --- |
| **稳态 100 EPS × 600s** | push P50/P95/P99 | 13.1 / **14.7** / 16.9 ms |
| | 端到端（push→receipt）P50/P95/P99 | 82 / **170** / 210 ms（SLO ≤3s，余量 ~17×） |
| | 批次 / 错误 | 1200 / **0** |
| **突发 2000 EPS × 60s** | push P50/P95/P99 | 7.4 / 18.5 / 19.4 ms |
| | 批次 / 错误 | 2400 / **0** |
| | 端到端 P95 | 50.7s（单副本消费滞后，符合预期；300s 排水后残差仅 ~1.7k） |
| **不变量** | receipts / transitions | 171570 / 110821 |

## 结论

- **10 分钟连续负载零错误、P95 无劣化**（较 2 分钟短跑 e2e P95 230ms → 170ms，属正常抖动范围）。
- P8/R19–R25 的全部变更（authority generation 过滤、发送前续租、advisory lock、事件历史写入、
  指标埋点）在长稳负载下无性能回退。
- 剩余：2,000 EPS 持续 10 分钟的全容量与多副本场景仍属生产/性能集群（PERF-003 挂账不变）。

## 复现

```bash
STEADY_DURATION=600s BURST_DURATION=60s DRAIN_WAIT=300 \
  bash scripts/perf_capacity.sh
```