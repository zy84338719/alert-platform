# P8 集群验收（rpi-k3s · 2026-11-15）

覆盖 P8-P1-02（NetworkPolicy 默认通知出口）与 P8 运行时/迁移的集群级安装验证。
应用侧单元/集成证据见任务书 §8（`planning/P8-Production-Correctness-2026-11-15.md`）。

## 环境与版本

- 集群：rpi-k3s，7 节点全 arm64（k3s v1.37.0+k3s1），`export KUBECONFIG=~/.kube/rpi-k3s.yaml`
- Release：`ap-dev`（namespace `alert-platform`）
  - **REVISION 3**（12:39）：仅 chart/NetworkPolicy 更新，旧镜像，Pod 零重启
  - **REVISION 4**（12:43）：导入 P8 镜像后完整升级（新二进制 + 迁移 0009–0011）
- 镜像：`docker.io/library/alert-platform:0.4.0-dev`（`docker buildx --platform linux/arm64` 本机构建，
  31MB tar，经节点导入法进入 rpi4-011 containerd `k8s.io` 命名空间）
- values：`deploy/helm/environments/values-dev.yaml`（image.tag 已更新 0.4.0-dev；worker 因集群无 Kafka 保持 disabled）

## 迁移与运行时

| 项 | 证据 |
| --- | --- |
| migrate hook | `schema version 11`（日志），`schema_migrations` = `11\|f`（非 dirty） |
| 0009 | `notification_authority` 表存在；种子行 `active gen=1 initialized=false`（worker 未启用，首个 worker 启动时才开新纪元） |
| 0010 | `edge_uplink_credentials` 表存在 |
| 0011 | 部分唯一索引 `uq_alert_instance_active_fingerprint` 存在 |
| api（新二进制） | `/healthz` 200 `{"status":"ok","closed":false}`；`/readyz` 200 |
| edge（新二进制） | StatefulSet 滚动后 Running；日志 `HTTP server listening on [::]:8082` |

## NetworkPolicy 连通矩阵（REVISION 3 期间实测，策略未再变化）

临时 Pod `p8-nettest-worker`（标签 `instance=ap-dev, component=worker`，镜像同版本，测后删除）。

| # | 源 | 目标 | 期望 | 实测 |
| --- | --- | --- | --- | --- |
| W1 | worker | 同 ns api:8080 `/healthz` | allow | **200 `{"status":"ok"}`** |
| W2 | worker | DNS `kubernetes.default.svc.cluster.local` | allow | **解析成功 10.45.0.1** |
| W3 | worker | kube-system:53/TCP | allow | **open** |
| W4 | worker | kube-system:9153 | deny | **立即失败（exit 1）** |
| W5 | worker | LAN 10.10.10.32:22（10/8 排除） | deny | **立即失败（exit 1）** |
| W6 | worker | 公网 1.1.1.1:443 | allow（策略） | **包已放行，上游超时**（集群无外网） |
| E1 | edge | 同 ns api:8080 | allow | **200** |
| E2 | edge | dev-pg:5432 | deny | **立即失败** |
| E3 | edge | 公网 1.1.1.1:443 | deny | **立即失败** |
| A1 | api | dev-pg:5432（同 ns） | allow | **连通** |
| A2 | api | 公网 1.1.1.1:443（无公网规则） | deny | **立即失败** |

判定依据：策略拒绝=立即失败（无 `Operation timed out`）；策略放行但上游不可达=超时（W6）。

## Edge 上行鉴权矩阵（REVISION 4，真实 api Pod + 真实 PG）

DBA 方式预置：tenant `1111…`、两个 enabled 来源（S1 deepflow / S2 alertmanager）、
edge 凭证 `edge-e2e`（allowed=[S1]，token `aps_cluster-uplink` 仅存 SHA-256）。测后已清理。

| # | 请求 | 期望 | 实测 |
| --- | --- | --- | --- |
| ① | 无 Authorization | 401 | **401 Unauthorized** |
| ② | edge token + `X-Edge-Id` + 事件来源 S1 | 认证通过（集群无 Kafka → 5xx） | **500 Internal Server Error**（鉴权/身份绑定已过，卡在 publish） |
| ③ | edge token + 事件来源 S2（allowed 集外） | 403 | **403 Forbidden** |
| ④ | edge token + payload 跨租户 | 403 | **403 Forbidden** |
| ⑤ | 未知 edge token | 401 | **401 Unauthorized** |

②的 500 是集群缺少 Kafka（producer 无法 publish），恰好证明请求已穿过
`IngestAuthMiddleware → enforceEdgeIngestIdentity` 并进入 Ingest Service——
鉴权语义在真实 API 进程 + 真实 PostgreSQL 上成立。

## 限制与后续

1. 集群无公网出口（ungoverned pod 直连 1.1.1.1 亦超时）→ worker 通知出口的真实外呼需在有外网
   环境复验（飞书/钉钉沙箱，与 P8-012 / NOTI-002 同源挂账）。
2. 集群无 Kafka → ingest 只能验证到鉴权/身份绑定层；Kafka 消费链路（processor/notifier）待 Kafka 接入后复跑。
3. worker 处于 disabled → `notification_authority.initialized` 保持 false；worker 启用后首次启动会开新纪元，
   属 P8-P0-01 设计行为。
4. P8-012（真实 DeepFlow/HUATUO/多集群/CH/渠道沙箱/压测）仍未完成，本次仅关闭"集群安装与策略"层证据。

## 复现

```bash
export KUBECONFIG=~/.kube/rpi-k3s.yaml
# 镜像（本机 arm64）
docker buildx build --platform linux/arm64 -t docker.io/library/alert-platform:0.4.0-dev --load .
docker save docker.io/library/alert-platform:0.4.0-dev -o /tmp/ap-image.tar
# 节点导入（特权 pod + nsenter 宿主 ctr；manifest 见 docs/12 §节点导入法）
kubectl -n alert-platform cp /tmp/ap-image.tar image-import:/scratch/ap-image.tar
kubectl -n alert-platform exec image-import -- nsenter -t 1 -m -- \
  ctr --address /run/k3s/containerd/containerd.sock -n k8s.io images import \
  "/var/lib/kubelet/pods/$(kubectl -n alert-platform get pod image-import -o jsonpath='{.metadata.uid}')/volumes/kubernetes.io~empty-dir/scratch/ap-image.tar"
# 升级（REVISION 4）
helm upgrade ap-dev deploy/helm/alert-platform -n alert-platform -f deploy/helm/environments/values-dev.yaml
```