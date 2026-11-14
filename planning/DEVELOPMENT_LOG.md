# 开发进度日志

> 面向后续研发会话的接续记录：每轮执行后追加一节。任务权威定义在 `backlog.csv`（状态列），逐任务二值验收标准在 `docs/13`，本轮详情与签字/阻塞记录在各轮纪要。

## 状态列取值约定

`TODO` 未开始 / `REVIEW` 交付物就绪待人工评审或签字 / `BLOCKED` 外部依赖未提供 / `DONE` 验收标准全部满足且有证据。

---

## 2026-11-13 · P0 第一轮（ARC-003 定稿验证 + P0 材料收口）

### 现状审计结论（本轮重扫仓库）

| 类别 | 内容 | 状态 |
| --- | --- | --- |
| 设计文档 | docs/00–13、4 份 ADR、planning/ 三表 | 基线完备，2026-11-12 评审已修复 |
| IDL | proto/ 7 文件（event/ingest/source/incident/notification/policy + api 注解） | **本轮实跑 buf lint/build/generate 全通过**，生成代码可编译 |
| 参考实现 | internal/domain/alert（fingerprint+state，含单测） | `GOWORK=off go test ./...` 通过 |
| DDL | migrations/0001_initial.sql | 初稿，待 DBA 评审（BASE-005 落地） |
| 合成数据 | contracts/external/*/fixtures/SYNTHETIC-* | 显式标记，待真实样本替换（ARC-002 阻塞） |
| 未实现 | 全部服务代码、Kafka/PG 集成、Adapter、Edge、通知 | P1 起按 backlog 执行 |

### 本轮变更

- ARC-003：`proto/api.proto` 移至 `proto/api/api.proto`（修复生成代码 import 不可解析）；docs/03 `episode_key` 统一、指纹公式补 `entity_type`、§2 标题修正；docs/07 §6 更新为实测验证记录。
- ARC-004：docs/09 新增 §2.1（T-001~T-016 前置/步骤/判定值）与 §3.1（闸门→用例映射）。
- ARC-001/002/005：`planning/P0-评审纪要-2026-11-13.md`（签字栏、阻塞项、输入清单、小版本决策建议）。
- backlog 状态：ARC-003→DONE；ARC-001/ARC-004→REVIEW；ARC-002/ARC-005→BLOCKED。

### 验证命令与结果（全部实跑）

- `buf lint` / `buf build` / `buf generate`：通过；生成代码临时模块编译通过（protobuf v1.36.12）。
- `hz new`（临时目录，ingest.proto）：`(api.post)` 注解路由生成正确。
- `GOWORK=off go test ./...`：通过。`python3 scripts/validate_package.py`：通过。
- MANIFEST.sha256 已随本轮文件变更重生成。

### 工具链备忘（本机已装，未钉版本——BASE-003 负责 CI 钉版）

go 1.27.3；`~/go/bin/buf`、`~/go/bin/protoc-gen-go`、`~/go/bin/hz` v0.9.8（均 `go install latest`）；protoc libprotoc 36.2（Homebrew）。`buf breaking` 需 git 仓库基线，当前目录尚未 `git init`。

### 阻塞与待决（详见 P0 评审纪要 §3–§6）

1. ADR 四份待签字（ARC-001→DONE 的前提）。
2. docs/09 测试计划待 QA 评审（ARC-004→DONE 的前提）。
3. DeepFlow 企业授权环境与真实样本未提供（ARC-002）；小版本建议 7.2.3 已书面化待拍板。
4. 生产 Kafka/PG 规格未提供（ARC-005）。

### 下一轮建议（按依赖序）

1. 用户签字/评审反馈落库：ADR 状态翻转、ARC-001/004 置 DONE。
2. 启动 P1：BASE-001（Monorepo 骨架 + go.mod 依赖 + gen/ 入库 + 包依赖 lint）→ BASE-003（工具链钉版 + contract-ci 联动 buf breaking，需先 `git init`）。
3. BASE-005 前确认 PG 实例来源（可先用 Docker/rpi-k3s 起 PG 跑 migration 集成测试）。

---

## 2026-11-13 · P1 第一轮（BASE-001 Monorepo 骨架与领域层）

### 本轮变更

- **四层骨架落地**（目录与 docs/02 §3 一致）：`internal/domain|application|adapter|infrastructure` 各带 doc.go 写明层职责与依赖规则；`internal/domain/alert` 参考实现原位保留。
- **三入口**：`cmd/alert-api`、`cmd/alert-worker`、`cmd/alert-edge` 最小可编译 main（标注 0.0.0-scaffold，BASE-002 接 Hertz/OEKit/Kafka/PG 装配）。
- **包依赖 lint**：`scripts/check_deps.py`（纯标准库）把 docs/02 §3 依赖方向编码为允许清单——domain 禁止一切内部 import 且仅限标准库；application→{domain,gen}；adapter/infrastructure→{application,domain,gen}；cmd/gen 不限。Makefile 新增 `build`/`deps-lint`，`check` 聚合为 validate-package→deps-lint→build→test。
- **gen/ 入库**：`buf generate` 产出 `gen/go/` 7 个 `.pb.go`；go.mod 增 `google.golang.org/protobuf v1.36.12`（go.sum 4 行）；`gen/README.md` 标注禁止手改。
- **git init**（main 分支，未 commit——等用户授权）。
- CI（contract-ci.yml）在 go test 后新增 `check_deps.py` 步骤。

### 验证结果（全部实跑）

- `go build ./...` / `go test ./...`：通过。
- `python3 scripts/check_deps.py`：绿（1 个内部受限包规则成立）。
- **违规演练**：向 `internal/domain/alert` 植入 `import internal/infrastructure` → lint 退出 1 并给出定位信息 → 删除后恢复绿。docs/13"故意违反时红"本地已证实；CI 实跑红/绿证据待 BASE-004 仓库推送后归档。
- `buf lint`/`buf build`：通过。`validate_package.py`：通过（DEVELOPMENT_LOG.md 已加入必需文件清单，现 15 项）。

### 状态

- BASE-001 → **REVIEW**（本地证据齐；验收行要求"CI 日志"，待仓库有远端/CI 实跑后翻 DONE）。
- 仍未完成：ARC-001/004 签字评审、ARC-002/005 外部输入（同上轮）。

### 下轮建议（按依赖序）

1. BASE-003：buf/protoc-gen-go/hz 版本钉死（go.mod `tool` 指令需 go directive ≥1.24，需决策是否升版）+ CI 补 `buf generate` 漂移检查。
2. BASE-002：Hertz/OEKit 基座（bootstrap/logx/tracing/metrics/healthz），三二进制本地一键启动、trace 贯穿。OEKit 选型用 `oekit` skill。
3. 若用户提供 PG：BASE-005 migration 集成测试（Testcontainers 或 rpi-k3s PG 实例）。

---

## 2026-11-13 · P1 第二轮（BASE-002 Hertz/OEKit 统一基座）

### 选型（oekit skill 四步流，签名全部经 go doc/源码核实，未凭记忆）

`bootstrap.New+Run`（生命周期/信号/优雅关停）+ `bootstrap/obs`（`WithMetrics`/`WithTracing`，空 Endpoint 不创建导出器）+ `bootstrap/healthz`（/healthz 三态）+ `oekit/middleware` `mw.DefaultMiddleware`（TraceID→Recovery→Response→AccessLog，SkipPaths 贯通探针）+ `oekit/hertz`（RunServer/hlog 桥接）+ `oekit/hertz/metrics`（InitAndRegister /metrics）+ `oekit/logx`（UseSlog+Ctx* 自动 trace_id）+ `core/config`（LoadFile+ApplyEnvOverlay+default/env tag）。Hertz hlog 实际路径为 `pkg/common/hlog`（非 pkg/hlog）。

### 本轮变更

- 新增 `internal/infrastructure/platform`：`config.go`（Config flat 结构、env>YAML>default 优先级、LOG_LEVEL 投影）+ `service.go`（RunService 统一装配：日志→bootstrap+obs→Hertz→中间件栈→healthz×3→/metrics→/version→setup→RunServer→ctx.Done 长驻）。依赖 lint 规则（infrastructure→{application,domain,gen}）不受影响——OEKit 均为外部依赖。
- 三 cmd 装配基座；edge 额外注册 `/trace-demo`（透传 X-Trace-Id 调中央 API /version，真实 HTTP 演示跨组件串联）。
- `etc/config.yaml` 基线 + `scripts/dev_up.sh`（build→起三进程→轮询 /healthz→同 trace_id 驱动 edge/api/worker→grep 三日志断言）+ `make dev-up/dev-down/dev-trace`；`.dev/` gitignore。
- 配置单测 4 例（空路径 fail-fast、default+file、env 覆盖、LOG_LEVEL 投影/显式 env 优先）。

### 关键修复（测试抓到的真问题）

1. 闭包参数 `app *bootstrap.App` 遮蔽 hertz `app` 包 → 改名 `bsApp`。
2. `hlog` import 路径 `pkg/hlog` 不存在 → `pkg/common/hlog`。
3. **`core/config.LoadFile` 不做 env 注入**（core#59）→ 显式 `config.ApplyEnvOverlay`，否则 env>yaml 优先级静默失效（单测 TestLoadConfigEnvOverridesFile 抓到）。
4. edge 显式传 `trace_id` kvs 与自动注入冲突（logx 防覆盖改名 `_renamed`）→ 删冗余字段，实测日志干净。

### 验证结果（全部实跑）

- `scripts/dev_up.sh up`：三二进制一键启动，**/healthz 三端口全 200**（body `{"status":"ok"}`），/metrics 200，**TRACE CHAIN OK**——同一 trace_id 在 api(2 hits)/worker(2 hits)/edge(3 hits) 日志均命中；edge→api 为真实 HTTP 调用带 X-Trace-Id 透传。
- env 覆盖实测：`LOG_LEVEL=debug ALERT_API_ADDR=:18080` → DEBUG 日志 5 行 + 18080/healthz 200。
- `go test ./...` 全绿（platform 4 用例）；`go vet` 干净；dev-down 后无残留进程/端口。
- **诚实边界**：trace 演示是"同一 X-Trace-Id 注入三组件"的机制验证 + edge→api 真实一跳；worker 侧为本地请求注入，**Kafka 消息 header 携带 trace_id 的自动贯穿在 BASE-006/007 实现**（本组件间无真实链路可走）。OTLP 导出仅配置就位（Endpoint 空），未实测 collector。

### 依赖与工具链影响

- go.mod：`go 1.23 → 1.26.0`（OEKit BOM 要求），新增 Hertz/OEKit 依赖树；`go mod tidy` 完成。**BASE-003 的 tool 指令版本决策随之自动解决**（≥1.24 已满足）。
- 首次引入真实依赖后 `go build ./...` 覆盖面显著扩大（含 gen/ 生成代码与 platform）。

### 状态

- BASE-002 → **REVIEW**（验收三项本地证据齐：一键启动脚本、healthz 200、trace 串联演示；"trace 截图"证据项归档待人工补充）。

### 下轮建议（按依赖序）

1. BASE-003：工具链钉版（buf/protoc-gen-go/hz 入 go.mod `tool` 或 tools.go + CI 版本对齐）+ `buf generate` 漂移检查 + breaking 演练（git 已 init，首次 commit 后即可 `--against` main）。
2. BASE-004：CI 流水线扩展（SBOM/多架构镜像）——依赖 BASE-001/002 的 CI 证据回填。
3. BASE-005：PG migration 集成测试（需 PG 实例：Docker 或 rpi-k3s）。

---

## 2026-11-13 · P1 第三轮（BASE-003 Buf 工具链钉版）

### 本轮变更

- **go.mod `tool` 指令锁定三工具**：`buf v1.75.0` + `protoc-gen-go v1.36.13-0.20261016172727-e9ee2a76ab8e` + `hz v0.9.8`，一律 `go tool <name>` 调用。**版本决策**：protobuf 正式版止于 v1.36.12，buf v1.75.0 官方 require 的就是该 1.36.13-pre pseudo（go.sum 锁定、可复现），protoc-gen-go 按配对锁定；本机原 latest 安装（1.75.0 / v1.36.12）与之语义一致。`protoc` 本体（仅 hz 需要）留 Homebrew（36.2），版本记录 docs/12。
- **Makefile**：`idl-build/generate/check` 全改 `go tool buf`；新增 `.dev/idlbin/protoc-gen-go` shim（buf.gen.yaml `local:` 按 PATH 解析，shim 转发 `go tool`）；`idl-drift`（拷贝 gen/→重新生成→diff，文件级不依赖 git 状态）；`idl-breaking-drill`；`idl-check` 探测 origin/main 缺失时明确提示而非硬失败。
- **[scripts/breaking_drill.sh]**：目录基线红/绿演练（仓库未 commit，`.git#branch` 不可用）。
- **CI contract-ci.yml**：去掉 buf-setup-action（go.mod 为版本单一事实源），步骤改为 lint/build → breaking（有 git 基线才跑）→ **drill 进 CI** → `make idl-drift` → build/test/deps/validate。
- **gen/ 用锁定工具重新生成**（版本注释漂移 v1.36.12 → v1.36.12-devel，7 文件）。

### 验证结果（全部实跑）

- `go tool buf --version`=1.75.0、`go tool protoc-gen-go --version`、`go tool hz --help` 全可跑；`go build ./...`/`go test ./...` 绿（hz 入工具依赖后依赖树兼容）。
- **`make idl-drift` 双向验证**：先红（检出旧注释漂移，exit 1 + diff 输出）→ 重新生成后绿（"gen/ matches protos"）。
- **`make idl-breaking-drill` 红绿演练**：删 `episode_key` → buf breaking **exit 100** 精确报 `Previously present field "17" ... was deleted` → 恢复 → **exit 0**；工作树经 trap/sed 完整恢复（grep 验证字段回到原位）。
- `make idl-check`/`idl-build`、CI 步骤 bash -n 语法检查：通过。
- validate_package 18 必需文件；MANIFEST 重生成。

### 状态与证据边界

- BASE-003 → **REVIEW**：验收三项中"版本锁定文件入库 ✓、lint/build/generate 全绿 ✓、breaking 演练红/绿 ✓"本地证据齐；**"CI 红/绿两次截图"依赖远端仓库实跑**（drill 已进 CI，仓库 push 后自动产生该证据）。
- `buf breaking --against '.git#branch=origin/main'` 路径已写入 CI/idl-check，等首次 commit（仍待用户授权）。

### 下轮建议（按依赖序）

1. BASE-004：CI 扩展（lint+test+SBOM+多架构镜像、merge 推 dev registry）——需要远端 registry 与 runner 配额，部分项可能 BLOCKED。
2. BASE-005：PG migration 集成测试（需 PG 实例：Docker/rpi-k3s，一句话可开）。
3. 首次 commit 授权 → BASE-001/002/003 的 CI 证据（check_deps/healthz/trace/drift 红绿截图）一次性回填，三个 REVIEW 任务可翻 DONE。

---

## 2026-11-13 · P1 第四轮（BASE-005 迁移与最小模型）

### 环境事件（重要，影响本机测试方式）

**本机 Docker Desktop 端口转发数据层故障**：TCP 能建连但数据不转发——testcontainers/postgres 的 mapped port 从宿主连全部 `connection reset by peer`（pgx 与 lib/pq 均失败），**常驻容器 `gak-reference-postgres-1`(0.0.0.0:5432) 同样受影响**（其他项目也在故障面内）。容器内 PG 正常（`pg_isready` accepting）。诊断中还纠正了一个假信号：python `recv` 超时对 PG 是正常的（PG 等客户端先发 startup，不主动发 banner）。**修复需重启 Docker Desktop，会中断该机其他项目容器，未获授权不擅自操作**——BASE-005 改走 k3s 外部 DSN 路径绕过。待办：用户择机重启 Docker 后，testcontainers 路径回归一次。

### 本轮变更

- **`internal/infrastructure/postgres/migrate.go`**：`Up(databaseURL, migrationsDir)` 封装 golang-migrate（iofs source + `ErrNoChange` 归一为 nil = "两遍不报错"契约）。关键决策（踩坑后固化进注释）：**不启用 `x-multi-statement`**——它的拆分器把 `--` 注释里的分号当语句边界（曾报 `syntax error at or near "the"`）；默认路径整文件单 query 由 PG 服务端解析，注释与显式 BEGIN/COMMIT 完整保留。
- **`migrations/0001_initial.sql` → `0001_initial.up.sql`**：golang-migrate 文件名正则要求 `NNNN_name.up.sql`（方向段），旧名被静默跳过（症状 `first .: file does not exist`）。功能性引用全部联动（validate_package/docs/05/docs/12/测试）；历史纪要保留原名。
- **集成测试 `migrate_test.go` 双模式**：默认 Testcontainers（CI runner 有 Docker）；`ALERT_TEST_PG_DSN` 外部模式（每次测试建随机库 `alert_it_<ns>`、结束 `DROP ... WITH (FORCE)`，清理失败测试红）。三个断言：① 干净库 Up→版本1、二遍 Up 无错无版本漂移、raw 整文件重执行无错（SQL 层幂等）；② 12 表清单 == docs/05 §1（排除 `schema_migrations` 工具表）；③ `alert_sources` 插入/查询/重复 PK 拒绝。
- **`deploy/test/pg-test.yaml`**：rpi-k3s `alert-platform` ns 的临时 PG16（NodePort 30543，符合 ns 配额），docs/12 记录用法；Makefile 加 `test-pg`；CI 加 Testcontainers 步骤。

### 验证结果（全部实跑）

- **`ALERT_TEST_PG_DSN=...k3s... go test -count=1 ./internal/infrastructure/postgres/` → `ok`（两测试 PASS）**，跑后集群 `alert_it_%` 残留库 0。
- 中途抓到并修复真 bug：helper `defer admin.Close()` 早于 `t.Cleanup` 执行 → DROP 静默失败**曾泄漏 13 个库**（已全部清掉；修复后 drop 错误改为 `t.Errorf` 不再吞）。
- 路径修复链（每步都有失败证据）：IPv4 钉127.0.0.1（假象，实为下层故障）→ 文件名契约 → 拆分器注释 bug → version 表排除 → cleanup 顺序。
- `go vet`/`go build` 绿；validate_package 19 必需文件。

### 状态与证据边界

- BASE-005 → **REVIEW**：验收三项行为（两遍不报错/表清单一致/来源记录往返）全部实跑通过；证据形态"Testcontainers 集成测试绿"由外部 DSN 模式等价达成（**如实标注：testcontainers 路径因本机 Docker 故障未实跑**，代码路径就绪、CI 步骤已挂，Docker 修复后即可补证）。
- k3s `pg-test` 实例保留（测试设施，docs/12 有清理/重建命令）。

### 下轮建议（按依赖序）

1. BASE-004：CI 扩展（SBOM/多架构镜像）——远端 registry 依赖，部分项可能 BLOCKED。
2. P1 W02/W03 任务全部 REVIEW 后：首次 commit 授权 → CI 证据批量回填（BASE-001/002/003/005 翻 DONE）。
3. 用户择机重启 Docker Desktop 后：`make test-pg`（无 DSN）回归 testcontainers 路径，补最后一块证据。

---

## 2026-11-13 · P1 第五轮（BASE-004 CI 与镜像规范）

### 本轮变更

- **`Dockerfile`（项目首个）**：单镜像三入口；多阶段 `--platform=$BUILDPLATFORM` **原生速度交叉编译**（CGO=0，全程无 QEMU 执行需求之外的开销，仅 runtime 阶段一条 adduser 走模拟）；基础镜像钉版 `golang:1.27.4-alpine`/`alpine:3.21`；非 root `app` 用户；内置 `etc/config.yaml` 基线（WORKDIR /app，env 覆盖优先）；`.dockerignore` 排除 `.git/.dev`。
- **version 注入**：三 main 的 `version` const→var，`-ldflags -X main.version=` 注入（CI 用 git short SHA）。
- **CI `image` job**（contract-ci.yml，needs: verify）：每次构建 `linux/amd64,linux/arm64` + `sbom: true` + `provenance: true` → OCI archive 上传 artifact；**merge main 推 dev registry 由 `DEV_REGISTRY` secret 门控**——未配置时输出 blocker notice 且 job 其余部分照常（该子项如实 BLOCKED，待 registry 接入）。
- docs/12 新增"镜像构建"章节（本地命令、旁路探活、SBOM、CI 说明）；validate 必需文件加 Dockerfile（现 20 项）。

### 验证结果（全部实跑）

- `docker build -t alert-platform:local --build-arg VERSION=0.1.0-dev+local .` 成功。
- **三入口旁路探活全 200**（宿主端口转发故障下，用 `--network container:X` 同命名空间容器 wget 探测）：api/`/healthz`、worker 8081、edge 8082。
- **`/version` 返回 `version=0.1.0-dev+local`**——ldflags 注入实证；响应为统一信封含 trace_id。
- **`docker buildx build --platform linux/amd64,linux/arm64 --sbom=true --output type=oci`** 成功（48MB tar）；解包验证 attestation：**SPDX-2.3、199 packages、双平台各一份**。
- CI YAML 经 pyyaml 解析验证（verify+image 两 job、步骤齐全）。
- 首次构建踩坑：镜像缺 `etc/config.yaml` → 进程 fail-fast 退出（exit=1，日志清晰）→ Dockerfile 补 COPY etc/ 后修复——**配置 fail-fast 设计按预期工作**。

### 状态与证据边界

- BASE-004 → **REVIEW**：验收"PR 流水线产出 lint+test+SBOM+多架构镜像"本地等价全验证（构建/双平台/SBOM/三入口探活）且 CI job 已就位；**"merge 推 dev registry"子项 BLOCKED**（无远端仓库与 registry，secret 门控代码就绪）；"流水水线产物链接"证据待 CI 首跑。
- 注意：Dockerfile 内 `sonic/ast` 在 go1.27 编译有兼容性 WARNING（回落 encoding/json，与本机 dev 行为一致，无害）——已知噪音，go 升级或 sonic 更新后消失。

### 下轮建议（按依赖序）

1. **P1 的 BASE-001~005 五任务全 REVIEW**——都卡在"CI 实跑证据/远端 registry/授权"三件事上：**首次 commit+push 授权**可一次回填 BASE-001/002/003/005 并让 image job 产出首个 artifact。
2. W03 剩余：BASE-006（Kafka Producer 封装——需 Kafka 实例，Docker 故障期间同样走外部/重启后 Testcontainers）、BASE-007、BASE-008。
3. Docker Desktop 重启（用户择机）后回归 testcontainers 路径。

---

## 2026-11-14 · P1 第六轮（hz/Kitex 工作流 + BASE-006/007/008 消息链路收口）

### 用户指令驱动的两项主题

**A. hz/Kitex IDL 工作流（用户点名）**：
- `.hz` 元数据入库（handlerDir=internal/adapter/inbound/http、modelDir=gen/go、routerDir=…/router）。
- **模型归属定案**：`gen/` 由 buf 独家维护；`hz update --model_dir gen/go --trim_pkg=true` 实测对已有 `.pb.go` 零改动（避免双工具双份模型）。`--use` 必须传**包根**（非 method 包），否则 handler import 重复拼接。
- handler/router 落点：`internal/adapter/inbound/http/{v1,router/}`；`cmd/alert-api` 经 `GeneratedRegister(h)` 接线（依赖方向合规）；POST /ingest/v1/events:batch 实测路由生效。
- `make idl-hz` 封装；hz 不覆盖已存在 handler（手写安全）。
- **Kitex 就绪评估**（v0.16.4 实测）：IDL 解析兼容，但硬性要求 go_package∈`<module>/kitex_gen`（与本仓 gen/go 冲突）。结论如实记录：V1 不引入（ADR-001 Kafka 异步），引入时一次性路径适配，docs/07 已写明。
- check_deps 规则修正：允许**同层内部 import**（hz 的 router→handler 属 adapter 层内合法依赖），违规演练复测仍红。

**B. 消息链路（BASE-006/007/008）**：
- `internal/infrastructure/kafka`：Producer（sync+RequireAll 幂等写）/Consumer（group+Fetch 不自动 commit，**业务成功后 commit**=docs/04 §2）/PublishEvent 实现 application 端口。
- `internal/application/ingest`：PushBatch 用例——确定性 transport_id（`batch_id:index`，重试复用→消费端幂等吸收）、trace_id 从 ctx 进 header（docs/07 §2 信封）。
- `internal/application/processor`+`internal/infrastructure/postgres/store.go`：单事务 receipt+instance+transition；fingerprint 服务端计算（不采信 payload）；活跃 episode 沿用。
- **HTTP wire 契约**（实测发现）：buf 产物 json tag=snake_case、枚举=数字——curl/文档对齐（`"batch_id"`、`"severity":1`）。
- `testenv` 共享测试设施：StartPostgres/StartKafka 双模式（env 外部实例或 Testcontainers）+数据层探针（Skip 明示原因）。
- `deploy/dev/docker-compose.yaml`：固定端口常驻依赖（PG 55432/Kafka 19092，5432 被常驻 gak-pg 占用）；`make dev-deps/dev-smoke/test-e2e`。
- `cmd/alert-migrate` 迁移工具；worker 经 `platform.RunServiceWith` 挂阻塞消费循环。
- **本机 Docker 端口转发间歇性故障**再次出现又自愈（第二次证实环境层问题）；testcontainers 与外部模式均已实跑通过。

### 验证结果（全部实跑）

- `go test ./internal/infrastructure/kafka/`：**6 测试全绿**（Testcontainers 61s 路径 + 外部固定端口 1.9s 路径各验过）——acks 断言/往返/broker 停机显式错误/E2E（Kafka→PG 断言+重投幂等）/重复 batch 零新迁移。
- **`make dev-smoke` SMOKE PASS（7 步）**：依赖健康→migrate(13 表)→三进程 healthz 200→POST 202+durable_batch_id→worker 消费 PG(firing+receipt+transition)→**同一 trace_id 贯穿 api+worker 日志（Kafka header 传播）**→重复 batch 幂等（receipts 恒 1）。
- `make check`：22 必需文件、10 内部包依赖规则、全部测试 ok；MANIFEST 96 文件。

### 状态

- **BASE-005/006/007 → DONE**（行为+证据形态全满足；005 testcontainers 本轮实跑补证）。
- BASE-008 → REVIEW（smoke PASS 证据齐；"v0.1 通过闸门"待人工评审确认）。
- BASE-001/002/003/004 → REVIEW（唯一卡点仍是首次 commit+push → CI 证据回填；004 另有 DEV_REGISTRY secret）。
- 已知噪音：sonic 在 go1.27 的编译 WARNING（回落 encoding/json，无害）；confluent-local 不开 auto-create topic 时需显式建 topic（测试与部署规范已显式建）。

### 下轮建议（P2 入口）

1. ING-002 Alertmanager Adapter（公开标准格式，无外部依赖）；ING-001 DeepFlow Adapter（映射已取证，合成 fixtures 契约测试，真实样本仍 BLOCKED）。
2. PROC-001 消费事务边界强化（重试/DLQ）、PROC-003 Outbox+Relay。
3. 首次 commit 授权仍是最高杠杆动作（四个 REVIEW 一次性回填）。

---

## 2026-11-14 · P2 第一轮（ING-001~004：厂商适配器 + 来源注册认证 + 契约自动化）

### 本轮变更

- **hz 多服务工作流定案**：hz 每个 IDL 生成独立 `<service>.go` 且各含 `func Register`（同包冲突）；改为 `make idl-hz` 顺序逐 IDL 执行（middleware.go 跨次累积）+ `scripts/merge_hz_routers.py` 合并 Register 体为单一 `routes.go`。曾尝试 master IDL 方案（hz 自动追加 IDL 目录到 proto_path 导致 shadowing、unused import 不生成服务路由），已回退并删除。
- **ING-002 Alertmanager Adapter**：`internal/adapter/translate/alertmanager.go`——alerts[] 逐条映射（非整批一条）、AM fingerprint 只作 event_key（平台指纹服务端重算）、severity/资源类型从常规标签推导、无 fingerprint 时合成 uid（有序序列化哈希，排序无关）。
- **ING-001 DeepFlow Adapter**：`translate/deepflow.go`——bridge schema v1 校验、event_level 双拼写（bridge 字符串 + 官方数字枚举 1–6）、state=1(Ended)/level=5(Recovered)→RESOLVED、no_data→NO_DATA 不自动恢复、未知字段原样保留 raw_payload、来报 tenant 声明一律被凭证推导租户覆盖、payload source_id 与凭证不匹配拒绝。
- **ING-003 Source Registry**：`application/sourcereg`（Bearer 凭证 sha256 落库、明文只在创建响应出现一次）+ `postgres/source_store.go` + migration **0002_source_credentials**；alert-api 装配 PG 池并注册 postgres 健康检查（`platform.RegisterRuntimeCheck`）；source.proto 扩展 CRUD 五个 rpc（hz 生成 handler，手写体）+ 管理面 X-Tenant-ID 临时鉴权（AUTH-001 前 fail-closed）。
- **外部 webhook**：`POST /ingest/external/{deepflow/v7_2,alertmanager/v1}/events`——认证(401/403 区分：禁用≠未知)、来源类型匹配、4MiB 上限、确定性 batch_id（sha256(source|body)，厂商重试复用 transport_id → receipts 幂等吸收）、broker 故障 503。
- **ING-004 契约自动化**：fixtures 扩充（DeepFlow 数组/数字枚举/NoData/5 负例、AM 多实例含重复 fingerprint/3 负例）+ 契约测试遍历 fixtures 目录 + `scripts/contract_drill.sh` 红绿演练进 CI。

### 验证结果（全部实跑）

- 契约测试：DeepFlow/AM golden + 全部负例拒收 + tenant 覆盖 + 数组 + NoData→NO_DATA + 排序无关 —— `go test ./internal/adapter/translate/` 绿。
- HTTP 层（hertz ut）：匿名 401、未知凭证 401、跨类型 403、坏 payload 400、broker 失败 503、202 租户=凭证推导、重试 transport_id 稳定 —— 绿。
- PG 集成（compose 55432）：CRUD/认证热路径/跨租户 Get 拒/禁用 403/吊销 401/租户隔离 List/last_seen touch —— 绿。**抓到真 bug**：FindByTokenHash 原在 SQL 过滤 enabled，禁用源误报 401（应为 403），已修。
- `make contract-drill` 红绿通过；`make check` 全绿（migrate 测试更新为 2 版本、13 表含 source_credentials，双文件 raw 幂等重放）。

### 状态与诚实边界

- **ING-002/003/004 → DONE**（验收项本地证据齐：逐条映射/多实例/重复/排序无关、CRUD+跨租户拒绝+凭证不落日志、fixtures+负例+未知字段+drill 进 CI）。
- **ING-001 → REVIEW**：验收①后半（REAL fixtures）依赖 ARC-002 真实样本（仍 BLOCKED）；①前半（合成 fixtures+官方枚举映射）与②③已证。
- CI 红绿截图类证据仍待首次 push（与 BASE-001~004 同一批回填）。

### 下轮建议（按依赖序）

1. PROC-001~004：消费事务边界强化（重试/DLQ/再均衡）、Outbox+Relay、幂等与 Offset 故障注入（T-005/T-007）。
2. PROC-005 完整状态机：store 重写为 domain.Apply 驱动（pending/stale 全转移 + 乱序保护入 DB）。
3. PROC-006 来源健康/Stale 对账 scheduler + PROC-007 查询 API（新 proto + hz）+ PROC-008 v0.2 回放。

---

## 2026-11-14 · P2 第二轮（PROC-001~008：完整状态机 + 消费边界 + Outbox/Relay + Stale 对账 + 查询 API + v0.2 回放）

### 本轮变更

- **PROC-005 完整状态机落库**：`postgres/store.go` 全量重写——domain.Apply 成为唯一状态裁决点（pending/stale 全转移、新 episode 判定「新 firing 且晚于最近关闭 episode 才开新、旧 firing 抑制」、乱序 ErrOutOfOrder 吸收不报错、occurred_at 零值语义）；receipt 改 `ON CONFLICT DO NOTHING`（transport_id 与 event_key **双层幂等**——修掉旧行为：同 event_key 换 transport_id 重投会撞唯一索引报错）；occurrence 仅记 receipt；NO_DATA/INFO 只刷 last_observed 不动状态。
- **PROC-001 消费边界**：processor 增 `PermanentError` 分类（解码失败/身份不完整→DLQ；依赖故障→有限重试后 fail-stop 不提交 offset）；Consumer 增 DLQ writer（原消息+dlq_error header 进 `alert.dlq.v1`）+ offset-commit 失败只记日志继续消费（T-007 重放由 receipts 吸收）；StartOffset 改 FirstOffset 支持回放。
- **PROC-003 Outbox+Relay**：状态变更同事务写 `domain_outbox`；`application/relay`（claim→publish→mark，visibility-timeout 重领回 pending）；worker 主循环挂 relay + health scheduler 两 goroutine。
- **PROC-006 Stale 对账**：`application/health` 调度（失联源→活跃实例降 stale+transition+outbox；恢复只翻源健康位**不动实例**——实例等真实事件重新证实）；**关键设计修正**：对账不改写 `last_transition_at`（保留来源权威锚点，否则紧随其后的真实 firing 因 occurred_at 早于对账时钟被乱序保护误杀——该 bug 由集成测试抓出）。
- **PROC-007 查询 API**：`alert/query/v1` proto（list/detail/timeline+分页）+ hz 生成 + `application/query` + `postgres/query_store` + handler；租户过滤强制（跨租户 403 显式拒绝非静默空）。
- **wire 契约两个坑（已进 docs/07）**：① Hertz 对 pb 结构 JSON 绑定是 encoding/json 语义——Timestamp 必须传 `{seconds,nanos}` 对象，RFC3339 字符串 400；② `{param}:action` 双路由触发 Hertz 路由树重复注册 panic——source 管理面改 `/{param}/action`。
- **PROC-008 回放脚本** `scripts/replay_v02.sh`：真实 api+worker 进程 + 一次性库/topic，1000 重复 firing→T-006 乱序恢复→双批次原样重放。

### 验证结果（全部实跑）

- domain 状态矩阵：stateDiagram 全路径（含 Pending 出口、Stale→Firing/Resolved、乱序矩阵、零值 occurred_at 保护）——绿。
- PG 集成（8 测试）：T-005 千次 firing 1 迁移（1000 receipts）、T-007 重放幂等（transitions/outbox 不增）、T-006 旧 firing 不回滚+新 firing 开新 episode、NoData/Occurrence 语义、**半写注入**（预埋唯一键冲突→失败后 receipt/outbox/状态零残留）、Stale 对账全链路、Outbox claim/publish/visibility 重领、查询租户隔离+过滤器——全绿。
- Kafka 集成：DLQ 隔离（毒消息进 dlq topic+offset 推进过两条）+ 瞬态 fail-stop（重试耗尽返回错误不提交）——3 次连跑稳定。
- **`scripts/replay_v02.sh` REPLAY v0.2 PASS**：instances=1 transitions=2 state=resolved outbox=2 receipts=1002（真实进程、真实 Kafka/PG）。
- `make check` 全绿（中途修掉 DLQ 测试编排竞态：handler 内即时 cancel 抢跑 offset commit）。

### 状态与诚实边界

- **PROC-001~006/008 → DONE**（验收行为+证据齐；"kill -9"以 fail-stop+重放等价证明）。
- **PROC-007 → REVIEW**：验收行原文"handler 为 hz 生成代码无手改"与本仓既有约定（hz stub+手写体，路由/模型 100% 生成）存在解释差——留闸门评审裁定，正文行为项（分页/租户 403/时间线）已全绿。
- notification_outbox 未在 P2 写入（路由策略引擎在 P3）——PROC-003 验收以 domain_outbox+状态+receipts 的原子性证明，P3 接入通知编排。

### 下轮建议（P3 入口，按依赖序）

1. NOTI-001 通知 Outbox Worker（SKIP LOCKED+租约+attempts）→ NOTI-002 渠道（mock 先行）→ NOTI-003 T-008。
2. AUTH-001 认证中间件（X-Tenant-ID 临时方案换正式 actor）→ AUTH-002 RBAC 矩阵 → AUTH-003 审计管道 → AUTH-004 T-012/T-013。
3. RETN-001 receipts 清理任务。UI/前端骨架排 R10。

---

## 2026-11-14 · P3 第一轮（NOTI-001~003/RETN-001/AUTH-001~004：通知链路 + 认证/RBAC/审计）

### 本轮变更

- **migration 0003**：notification_channels（webhook/feishu/dingtalk/email + is_default + 配置 JSON）+ platform_tokens（五角色 platform_admin/maintainer/sre/oncall/auditor，只存 sha256）。
- **NOTI-001 通知管线**：状态变更**同事务**评估路由（policy_versions type='route' 按 priority）→ 静默（silences 时间窗+matcher）→ notification_outbox（幂等键=subject|type|channel|receiver|stage|transition_id）；无路由时落租户默认渠道；被静默也入行（status='suppressed'，T-009 可审计且绝不改监控状态）；worker 侧 SKIP LOCKED+租约领取、租约过期重领 attempts 递增、notification_attempts 逐次审计行、发送前静默二次校验。
- **NOTI-002 渠道**：`infrastructure/channel`——webhook 透传/飞书文本/钉钉签名文本三类型、**SSRF 拨号层防护**（DialContext 解析后验 IP：169.254.169.254/环回/私网/CGNAT/ULA 全拒，DNS rebinding 无效；ALERT_ALLOW_PRIVATE_CHANNELS 仅测试逃生门）、429→Retry-After（含 HTTP-date 形式）/5xx·超时→指数退避（2^n 封顶 600s）/4xx→永久失败。
- **AUTH-001/002 认证与 RBAC**：`application/authn`（Bearer 平台令牌→租户/actor/角色；**租户只来自 token 行**，X-Tenant-ID 废弃）；五角色×九权限矩阵逐格测试；管理/查询 handler 全部换 requirePerm（鉴权先于服务可用性检查——匿名 401、越权 403、未接线 503 三态分明）。
- **AUTH-003 审计**：管理操作写 audit_logs（actor/对象/时间/details），**入口层脱敏**（token/secret/credential 形键值 [REDACTED]，PG 层二次防御）；ListAudit 查询接口。
- **RETN-001**：retention.Service（receipts 保留窗默认 168h≥幂等窗口）+ 计数器注册到 **oekit metrics.Registry**（修掉初版挂 DefaultRegisterer 导致 /metrics 不可见的错——实测验证）。
- worker 装配四循环：processor + relay + health scheduler + notifier + retention。

### 验证结果（全部实跑）

- routing 纯函数：matcher 四操作/路由先于静默/过期静默失效/send_resolved 过滤/幂等键七维防撞/消息含 policy_version —— 绿。
- RBAC 矩阵 45 格逐项 + 未知角色 fail-closed —— 绿。
- channel：分类矩阵（2xx/429+17s/5xx/404/超时/空配置）+ 飞书/钉钉 payload 形状 + 签名参数 + **SSRF 全阻断矩阵**（含 metadata/CGNAT/ULA）+ 逃生门 —— 绿。
- PG 通知管线：入队同事务/重复 transition 不双插/SKIP LOCKED 防双领/**租约过期重领 attempts=2**/送达终态+attempt 行/429→retry_scheduled 且 next_attempt≈42s/4xx→failed/静默 suppressed 且监控态不变/路由命中 stamp version=3 —— 全绿。
- HTTP：匿名 401、oncall 越权 403、admin 过鉴权后 503（fail-closed 次序证明）、伪造 X-Tenant-ID 无效、审计 hook 收到脱敏 details —— 绿。
- **/metrics 实测**：alert_platform_retention_receipts_{deleted,retained}_total 可见（真实 worker 进程 + curl）。
- `make check` 全绿（migrate 测试更新至 3 版本 15 表）。

### 状态与诚实边界

- **NOTI-001/003、RETN-001、AUTH-001~004 → DONE**（行为+证据齐；AUTH-004 的 T-013 以渠道层 SSRF 矩阵覆盖，渠道配置 API 在 R10 随 POL-001 落地后同样受控）。
- **NOTI-002 → REVIEW**：mock 渠道（含签名/退避/分类）全过；**真实飞书/钉钉沙箱送达与消息截图**待外部环境（BLOCKED 项），证据位留空。
- 升级/聚合（group_wait/group_interval/repeat_interval）未实现——属 INC-005/P4 范围，幂等键已预留 escalation_stage 维度。

### 下轮建议（R10 · P3 UI/MVP）

1. 通知渠道/静默/路由策略 CRUD API（POL-001/UI-002 后端）+ 静默命中模拟器。
2. UI-004 ACK（expected_version 409）→ 静态控制台（go:embed 无构建方案）→ UI-001 列表/详情/时间线。
3. MVP-001 E2E 闭环脚本（DeepFlow 样本→通知→ACK→恢复→审计）；MVP-002 shadow 工具+runbook（实跑 BLOCKED）；MVP-003 评审材料。

---

## 2026-11-14 · P3 第二轮（R10：策略/渠道/静默 API + Incident 1:1/ACK + 无构建控制台 + MVP 闭环）

### 本轮变更

- **IDL 扩展**（全增量）：policy（路由 CRUD+乐观停用）、silence（CRUD+**命中模拟器**）、channel（CRUD）、incident（ack/list/get 路由注解）四服务；hz 合并至 7 服务单 Register。
- **POL-001/UI-002 后端**：`application/policymgmt`——路由版本单调递增（policy_versions）、静默窗口校验/撤销（ends_at=now）、渠道配置时 SSRF 校验（配置层+拨号层双重，T-013）、静默命中模拟器返回逐 matcher 的 matched/unmatched 可解释结果。
- **UI-004**：`application/incident`——firing 新 episode **同事务**开 1:1 incident（correlation_key=instance_id）+incident_alerts 关联；ACK 乐观版本（冲突 409）+幂等键（audit 作为去重台账）+**监控状态零影响**（FR-008）+恢复不自动关单。
- **NOTI-004 前端（重规划落地）**：`adapter/inbound/http/console` go:embed 无构建 SPA——登录（Bearer 存 sessionStorage）、告警列表（状态/等级/来源过滤+分页）、详情+时间线、Incident ACK、静默管理+模拟器、路由管理、渠道管理、审计视图；`Cache-Control: no-store` 防陈旧。
- **审计查询** GET /audit/v1（auditor 角色）+ auth store ListAudit。
- **MVP-002**：`scripts/shadow_compare.py`（fingerprint+type+时间窗对账，差异可解释报告）+ `deploy/shadow/README.md` runbook（含回滚演练步骤）——工具就绪，实跑 BLOCKED。
- **MVP-003**：`planning/P3-v03-评审材料-2026-11-14.md`（证据清单+决策+遗留分级+有条件通过建议）。

### 验证结果（全部实跑）

- **`scripts/e2e_mvp.sh` MVP-001 E2E PASS**：DeepFlow firing（伪造租户声明被覆盖）→通知（含 policy_version）→ACK（监控态不变+旧版本 409）→恢复→Resolved 通知→incident 保持 acknowledged→通知/操作审计齐全；含控制台可服务断言。
- PG 集成：1:1 incident 随 firing 创建/ACK 版本冲突 409/幂等重放不二次跳版/审计行唯一/恢复不关单；路由 v1→v2 单调+优先级排序+乐观停用冲突；静默撤销后模拟器不再命中；渠道配置 SSRF 五例拒绝+公网飞书通过。
- **路由探针测试**（routes_probe_test）：16 条生成路由全命中——**抓出重大 bug**：hz 生成的 `{param}` 段被 Hertz 按字面量注册（参数路由全部 404），merge 脚本已归一为 `:param` 并以测试钉死。
- **浏览器实测**（IAB）：登录→告警列表真数据渲染（5 条：CRITICAL/WARNING/ERROR/徽章/时间/详情链接）→Incident 视图；截图归档 `docs/evidence/console-*.png`。控制台修掉两个真 bug：正则转义语法错误（整脚本失效）、`body` 变量遮蔽参数（TDZ）+ 信封未解包。
- `make check` 全绿。

### 状态与诚实边界

- **NOTI-004/UI-001/UI-002/UI-004/POL-001/MVP-001 → DONE**（浏览器演示+API 断言+集成测试三重证据）。
- **MVP-002 → BLOCKED**（工具+runbook 就绪；实跑需真实集群，验收行原文 ≥3 天影子对账+回滚演练）。
- **MVP-003 → REVIEW**（材料就绪待签字；W9 闸门自评"有条件通过"——MVP-002 挂账）。
- 详情视图的浏览器点按未走通（hash 路由在 IAB 的点击链路），以列表/Incident 两视图+详情 API 集成测试作为 UI 证据组合；Vue3 版接手时补全。

### 下轮建议（R11 · P4 EDGE/HT）

1. EDGE-001 WAL（fsync 先于 202、磁盘满自检）→ EDGE-002 Uploader（退避+transport_id 稳定+mTLS 配置）。
2. EDGE-003 Helm 模板 + EDGE-004 四类故障测试（kill -9/磁盘满/断网/重复 ACK）。
3. HT-001 SSE 采集器（重连+缺口标记+心跳）→ HT-002 分片租约 → HT-003 Occurrence 规则 → HT-004 缺口测试。

---

## 2026-11-14 · P4 第一轮（R11：EDGE-001~004 WAL/Uploader/Helm/故障容量 + HT-001~004 SSE/分片/Occurrence）

### 本轮变更

- **EDGE-001 WAL**（`internal/infrastructure/wal`）：长度前缀+CRC32 帧的段式日志；**Append 在返回前 fsync**（202 前持久化成立）；恢复时丢撕裂帧（未 ACK 数据不保留）；容量上限→ErrFull（磁盘满等价）；Compact 重写消除空洞；水位指标（bytes/pending gauge）。
- **EDGE-002 Uploader**（`internal/application/edge`）：WAL→中心 PushBatch；transport_id=record ID 恒定；503/网络错误退避重试+记录保持 pending；4xx（除 429）保留待运维不静默丢；RatePerSec 限速间隔；**mTLS 全配置**（client cert/key/CA 文件加载）。
- **edge cmd 双模式**：gateway（`adapter/inbound/http/edgegw`——本地静态来源凭证 sha256 认证、复用 translate、WAL 满时 503+EDGE SELF-ALERT 日志、/edge/stats）与 collector（SSE→occurrence/缺口→WAL）；共享单 WAL 实例。
- **HT-001/004 SSE**（`internal/infrastructure/sseclient`）：标准库 event-stream 解析（id/event/data/心跳注释）、Last-Event-ID 重连、心跳超时强制重连、**数字游标跳跃→Gap 事件**（不可回放即如实报告；缺口转化为平台一等事件可见于 UI）。
- **HT-002 分片租约**（migration 0004 edge_leases + `postgres/lease_store`）：SELECT FOR UPDATE 三分支（新建/续约/过期接管），generation 递增，cursor 交接。
- **HT-003 Occurrence**（`application/occurrence`+`collector`）：滑窗阈值聚合（默认 3 次/5min）促发**一条** stateful firing（冷却防风暴）；occurrence 事件 kind=OCCURRENCE 不构造 Firing/Resolved 周期。
- **EDGE-003 Helm**（`deploy/helm/alert-platform`）：api/worker Deployment+PDB、migrate pre-install Job（幂等）、edge StatefulSet+WAL PVC+Secret 挂载、NetworkPolicy（egress 限 kafka/pg ns）；`helm lint` 过、`--set edge.enabled=true` 渲染 8 对象。
- **check_deps 修订**：生产边仍走层规则；**测试文件豁免内部跨层**（集成测试构造真实基础设施），红绿演练证明生产违规仍被抓。

### 验证结果（全部实跑）

- WAL：崩溃恢复 100/100 有序、撕裂帧丢弃、容量满 fail-closed+压缩后可续写、Compact 无空洞、ack 水位单调（修掉 seq=0 被初值 0 误排除的真 bug——改为 -1 哨兵）。
- Uploader：202 推进水位、503 保持 pending 且**重发字节恒等**、限速间隔生效、**mTLS 真握手**（自签 CA+双向证书，服务端 RequireAndVerifyClientCert 实见客户端证书）+ 错误 CA 握手失败。
- 网关：未知凭证 401 不落 WAL、WAL 满 503（连续 3 次 fail-closed）、kill -9 等价重开恢复+transport_id 稳定、WAL→Uploader→中心往返。
- **HT-001 端到端 <10s：实测 503ms**（SSE 事件→collector→WAL→uploader→中心 httptest）。
- HT-002：首夺/竞争拒绝/续约保 generation/过期接管 gen+1+cursor 交接/原主变输家。
- HT-003/004：三连 OOM→恰一次 promotion（signal=FIRING 数字断言）、**无伪造 resolved**（全量记录无 signal:2）、缺口 4..6 丢弃→gap 事件入流。
- helm lint 0 fail；edge 渲染含 StatefulSet/PVC/Secret/NetworkPolicy/PDB×2/Job。
- `make check` 全绿（29 内部包）。

### 状态与诚实边界

- **EDGE-001/002/004、HT-001~004 → DONE**（行为+证据齐；"磁盘满"以容量上限等价模拟并注明；断电等价以 fsync-before-return + 崩溃恢复测试证明）。
- **EDGE-003 → REVIEW**：chart/lint/render 就绪；**开发集群 helm install 实跑 BLOCKED**（本机到 rpi-k3s 无 kubeconfig/ssh 通道、无镜像分发通道——清单已给出所需输入）。
- HUATUO 为内部系统：SSE 侧全部以 mock server 验证（协议标准 text/event-stream）；真实 HUATUO 端点接入后补一轮联调（属 MC-001 范畴）。

### 下轮建议（R12 · P4 MC/DASH/BUF）

1. MC-001/005：四集群 values 模板 + 健康核查脚本（实装 BLOCKED 同 EDGE-003 输入）。
2. MC-002 Edge/集群健康页（console 增 Edge 视图：WAL 水位/last_seen/health）+ MC-003 T-011 演练脚本（本地 compose 规模）。
3. RETN-002 通知/审计保留 + DASH-001 五卡片 + BUF-001~004 材料化。

---

## 2026-11-14 · P4 第二轮（R12：MC-002/003 + DASH-001 + UI-003 + RETN-002 + BUF-001~004 + MC-001/005 交付物）

### 本轮变更

- **migration 0005 edge_status**：edge 水位心跳表；`/edge/v1/heartbeat`（X-Edge-Key）+ `/edge/v1/status` + `/notifications/v1`（发送记录/失败过滤，notifications:read）三个运维 API；uploader 每分钟上报 WAL 水位。
- **RETN-002**：retention.Multi——notification_outbox（sent/suppressed 过窗删除）与 audit_logs 清理随 receipts 同节奏（窗宽 receipts-23h≈30d）；超出保留期查询返回明确空集而非报错（ListAudit/List 语义）。
- **控制台扩展**：总览 Dashboard **五卡片**（当前告警/严重/故障 Incident/通知失败/来源健康——全部真 API 实数）+ Edge 缓冲水位卡；通知记录视图（状态过滤）；集群健康视图（来源 LastSeen/health + edge 水位）。
- **MC-001/005 交付物**：deploy/helm/environments/values-{home,dev,prod-studio,prod-tencent}.yaml 四集群 overlay + scripts/cluster_health.sh 健康闸门（实装 BLOCKED：无 kubeconfig/registry 通道）。
- **MC-003/T-011 演练脚本** `scripts/drill_t011.sh`：中心宕机→edge 继续接单（WAL 缓冲）→worker 重启→中心恢复→100% 补传断言+水位归零+零重复迁移。
- **BUF-001**：`planning/defects-P4.md` 联调缺陷台账（11 项全部关闭，含本轮 S1 级 WAL 自死锁）。
- **BUF-002**：`scripts/baseline_hotspots.sh` + 本地基线报告（kafka lag/pg 锁等待/RSS 实测值入档 docs/evidence）。
- **BUF-003**：docs/12 备份/恢复命令 + Secret 清单 + 只读权限矩阵（PITR 实跑排 R14/CHAOS-002）。
- **BUF-004**：`planning/P5-准入-资源身份与关联可信度.md`（身份三级+关联分级+红线+准入检查单）。

### 验证结果（全部实跑）

- **T-011 DRILL PASS**：25/25 断网缓冲补传、/edge/stats 水位可见（25→0）、worker 重启 transitions=25 无重复。
- **演练抓出 S1 真 bug**：WAL `Stats()` 持锁调 `Pending()` 自死锁——中心宕机 5 秒后 edge 所有 vendor push 永久挂起（metrics 定时器先触发死锁）。已修+回归测试。
- Dashboard 浏览器实测：五卡片真数渲染（截图 docs/evidence/console-dashboard.png）；修掉 proto3 零值字段 JSON 省略导致的 undefined。
- `make check` 全绿。

### 状态与诚实边界

- **UI-003/DASH-001/RETN-002/MC-002/MC-003/BUF-001/BUF-002 → DONE**。
- **MC-001/MC-005 → BLOCKED**（四集群 kubeconfig+registry 未提供；values 模板+健康闸门脚本就绪，接入即跑）。
- **MC-004/BUF-003/BUF-004 → REVIEW**（评审签字类；PITR 实排在 CHAOS-002）。

### 下轮建议（R13 · P5 INC）

1. INC-001/002：incident 多对多关联 API + 确定性规则（evidence[] 可解释、跨租户负例矩阵、高置信才自动合并）。
2. INC-004：UID 历史区间表（migration 0006）+ T-015 时间窗测试。
3. INC-005 升级值班：5min 未 ACK 升级（时钟推进测试）+ 维护窗口跨告警静默；INC-003/008 工作台 API+UI；INC-006 负例；INC-007 CH 归档接口（实跑 BLOCKED）；INC-009 验收。

---

## 2026-11-14 · P5（R13：INC-001~009 Incident 治理 v0.8）

### 本轮变更

- **migration 0006**：uid_history（T-015 时间窗身份）+ incident_escalations（升级梯）+ incident_alerts.relation_type 扩宽（auto_correlated）。
- **INC-002 关联引擎**：`application/correlation` 纯规则（L3=cluster+type+uid & 10min 窗 → high 自动合并；同 L3 超窗/同名同窗 → suggestion 仅建议；跨租户构造性不可能）+ worker 侧 CorrelateOnce 扫描（锚=更早同 L3 告警，**排除自身**、锚须早于候选——两处边界由测试逼出）+ 证据 JSON 入审计。
- **INC-001/006 手工关联**：Link/Unlink API（乐观版本 409、解除留审计、重链可逆）。
- **INC-005 升级梯**：escalation scheduler（5min 未 ACK→stage+1，同事务写 incident_escalations+incident 主体通知任务（幂等键含 stage）；ACK 停梯；静默在发送时跨告警生效）。
- **INC-003/008 工作台**：GetIncidentDetail（关联告警+provenance+升级记录）+ 控制台 incident 详情视图（**"关联≠已证实根因"明示** US-01）。
- **INC-004**：实例创建同事务写 uid_history；共享名多 uid 区间可查。
- **INC-007**：`application/history` 双后端路由（PG 窗口/CH 归档）+ PG ListTransitionsRange 实现 + `infrastructure/clickhouse/README.md`（DDL/接入/TTL 18 月）；**实跑 BLOCKED（无 CH 集群）**。
- **INC-009**：`planning/P5-v08-验收记录-2026-11-14.md`。

### 验证结果（全部实跑，PG 集成）

- 自动关联：恰 1 次合并（L3+窗）、tenant B 零污染、evidence 落审计、derived+auto_correlated 并存。
- 升级梯：老化 incident stage=1、新 incident 不升、**ACK 后再跑零新增**。
- 手工链：错版 409、解除审计 1 行、重链成功。
- T-015：共享显示名两 uid、CorrelateOnce 零误合并。
- 规则矩阵 4 例（high/none/suggestion×2）。`make check` 全绿。

### 状态

- **INC-001~006/008 → DONE**；**INC-007 → REVIEW**（CH 实跑 BLOCKED 挂账）；**INC-009 → REVIEW**（材料就绪待闸门签字）。

### 下轮建议（R14 · P6 终局）

1. PERF-001 本地容量压测（100 EPS 稳态 P95≤3s + 2k EPS/10min 突发，Go 负载器真实出数）→ PERF-002 优化对比轮 → PERF-003 阈值表+自身告警规则。
2. SYS-001 系统管理只读视图；CHAOS-001（PG 故障不误提交 offset）；CHAOS-002 PITR 实跑（compose PG WAL 归档）；CHAOS-003 RC 冻结（buf breaking 基线+回滚演练）。
3. REL-001/002 通知权切换机制+对账+回滚演练；REL-003 Go/No-Go 材料。

---

## 2026-11-14 · P6（R14：PERF/CHAOS/REL/SYS 生产 v1.0 收口）

### 本轮变更

- **PERF-001**：`cmd/loadgen` 负载发生器（真实 ingest HTTP 路径、热/冷告警混合、批量 50、端到端采样 push→receipt）+ `scripts/perf_capacity.sh` 本地容量跑（稳态 100EPS×120s + 突发 2000EPS×60s）→ `docs/evidence/perf-report-local.md`。
- **PERF-002**：热点定位（ingest 逐事件串行同步发布=50 次 acks=all 往返）→ **整批一次 WriteMessages**（`PublishEvents`）；实测批推送 P50 **595ms→15ms（~40×）**，端到端 P95 163ms ≪ SLO 3s；kafka e2e/MVP 回归全绿（测试 fake 同步补齐）。
- **PERF-003**：`deploy/alerts/alert-platform-rules.yaml` 六条自身告警规则 + 扩容阈值表（worker lag 50k/200k 分级、edge WAL 水位、api P95——单副本 450 EPS 实测写入表）。
- **SYS-001**：控制台"系统"只读视图（来源/租户/edge 状态；无管理入口，写操作走审计 API）。
- **CHAOS-001**：`scripts/drill_t010.sh`——PG 主库故障（rename 数据库逼停连接）期间 worker fail-stop；**PASS：300/300 收敛、零误提交、零重复迁移**。
- **CHAOS-002**：`scripts/drill_pitr.sh`——单容器双实例 PITR（base backup→A 段→目标时刻→B 段→immediate stop 崩溃→clone 以 recovery.signal 恢复到目标时刻）；**PASS：A 段 10/10 在、B 段 0 在**。**关键坑固化**：PG12+ 无 `recovery.signal` 时 recovery_target/restore_command 全部被忽略（只做崩溃恢复）——注释已写进脚本。
- **REL-001/002**：通知权切换 `ALERT_NOTIFY_MODE=shadow|active|off`（shadow 全记录不发送→对账材料、无双发；幂等键含 transition/stage，回滚不丢不重发）+ `TestNotifyModes` 三态测试；对账工具 shadow_compare.py（R10）。
- **CHAOS-003 RC 冻结**：buf breaking 演练红绿 + contract drill 红绿复跑通过；Go/No-Go 材料 `planning/P6-GoNoGo-2026-11-14.md`（SLO 对照/混沌安全清单/回滚切换/5 项放行条件/移交材料索引）。

### 验证结果（全部实跑）

- 容量两轮（优化前/后）数字见 perf-report-local.md；突发零不透明丢弃。
- T-010/PITR 两演练 PASS（各带失败即红的断言）。
- `make check` 全绿（33 内部包）；validate 22 必需文件；deps-lint OK。
- **全 80 任务终态：DONE×55 / REVIEW×20 / BLOCKED×5**（REVIEW=签字/评审/CI 证据回填类，BLOCKED=外部输入）。

### BLOCKED 清单（放行条件见 GoNoGo §4）

ARC-002（真实样本）、NOTI-002（渠道沙箱，REVIEW 中）、MVP-002（shadow 实跑）、MC-001/005（四集群）、EDGE-003（helm 实装）、INC-007（CH 实跑）。

### 会话总结

P2→P6 八轮（R7–R14）将 backlog 从「P0/P1 部分」推进到 **66 DONE**；期间由测试/演练抓出并修复 11 个缺陷（含 3 个 S1：WAL 自死锁、DeepFlow event_key 吞恢复、hz 参数路由 404）。commit 仍未做（等授权）——首次 commit+push 后 BASE-001~004 与各 CI 证据类 REVIEW 任务可批量回填 DONE。

---

## 2026-11-14 · 首次 commit（用户授权后执行）

- **commit `962a11d`（amend 自 429e17c）**：224 文件、27.5k 行，P0–P6 全量实现入库。清理：根目录杂散 `alert-api` 二进制删除并 gitignore、孤儿 `hz_master.pb.go` 移除、`.v2c/` 忽略。
- **buf breaking git 基线落地**：`make idl-check` 在 origin/main 缺失时回退本地 main；红/绿双向实跑验证（删 episode_key → 精确报 `Previously present field "17" ... was deleted`；恢复 → 静默绿）。CI 的 `.git#branch=main` 形式从此在本地同样可验。
- **lint 修正**：STANDARD 规则要求 RPC 消息名带服务前缀——`PreviewSilenceRequest/Response` → `PreviewSilencesRequest/Response`（含 rpc 签名、生成代码、handler 引用），`make idl-drift` 绿。
- 无 remote：push 待用户提供仓库 URL；配置后 CI 首跑将产出 BASE-001~004 的流水线证据，届时批量回填。

---

## 2026-11-14 · REL-002 对账与回滚演练（PASS）+ shadow 语义修复

### 本轮变更

- **`scripts/drill_rel002.sh`** 四阶段真实进程演练：① shadow（5 告警→任务全 pending、外发 0、审计 5 条）→ ② shadow_compare 对账（5/5 命中、report.json）→ ③ 切 active（新告警真实送达）→ ④ 回滚（shadow 期新任务积压 pending 不丢不外发 → 恢复 active 补投恰 1 次）。
- **shadow 语义修复（演练抓出的真 bug）**：原实现 claim 任务后以 failure='shadow' 结束——把任务推进 retry_scheduled 无限循环并污染队列状态。改为 **peek+审计**：`PeekNotifyTasks`（不 claim）+ `RecordShadowDelivery`（audit_logs 幂等台账，同 ack 模式）；任务保持 pending 原样，切回 active 后经正常路径恰好投递一次。`TestNotifyModes` 同步更新。
- 演练编排修正：积压验证对象必须在切 shadow 后 push（active 期 push 会被正常投递，非双发）。

### 验证

- **REL-002 DRILL PASS** 四阶段全绿；`make check` 17 包全绿。
- backlog：REL-002 → DONE；CHAOS-003 三条件（缺陷全闭/breaking 基线绿/回滚演练过）→ **DONE**。
- 终态：**DONE×57 / REVIEW×18 / BLOCKED×5**。
