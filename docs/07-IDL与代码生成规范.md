# 07 · IDL First、Protobuf、Hertz hz 与兼容性管理

## 1. 契约层级

- 供应商层：DeepFlow/Alertmanager/HUATUO 原生协议不受本项目控制，由 Adapter + fixtures 固定映射。
- 交换层：`proto/alert/event/v1` 的 Protobuf 是中央事件和内部边缘上传的权威模型。
- 管理 API：`proto/alert/{source,incident,notification,policy,ingest}/v1` 用 IDL 描述。
- Domain：Go 领域类型和规则为独立实体，不能把生成的 `.pb.go` 当成 DDD Aggregate。

## 2. 命名约束

- 包名采用 `alert.<bounded_context>.v1`，目录与包名层级对应。
- 任何字段删除先 `reserved` 字段号及名称，不允许重用 field number。
- 枚举第一个值为 `*_UNSPECIFIED = 0`，新增值需要评估未知值容错。
- 数字时间优先使用 `google.protobuf.Timestamp`，持续时长使用 `Duration` 或显式单位后缀。
- Kafka 的编解码统一 Protobuf，消息 Header 附 `schema_name`, `schema_version`, `transport_id`, `trace_id`。
- 重要的业务组合约束由 Validator 执行；IDL 类型校验不能替代语义校验。

## 3. 工具与命令

工具版本自 BASE-003 起锁定在 **go.mod `tool` 指令**（`buf v1.75.0`、`protoc-gen-go v1.36.13-0.2026…`（与 buf 配对）、`hz v0.9.8`），一律经 `go tool` 调用——本地与 CI 同一套二进制，无需全局安装：

```bash
go tool buf lint
go tool buf build
go tool buf generate          # Makefile 封装为 make idl-generate（protoc-gen-go shim 解析 PATH）
make idl-drift                # 重新生成并 diff gen/，漂移即红（CI 同步执行）
make idl-breaking-drill       # 目录基线红/绿演练（首次 commit 前 .git#branch 不可用）
go tool buf breaking --against '.git#branch=origin/main'   # 有基线后（idl-check 自动探测）
GOWORK=off go test ./...
```

`buf.gen.yaml` 的 `local: protoc-gen-go` 按 PATH 解析，Makefile 生成 `.dev/idlbin/protoc-gen-go` shim 转发到 `go tool protoc-gen-go`，保证生成器与锁定版本一致。

### hz 工作流（2026-11-14 实测落地，IDL→Hertz handler/router）

工程使用**自定义 DDD 布局**，通过 `.hz` 元数据（已入库）声明 hz 生成落点：

```
handlerDir: internal/adapter/inbound/http
modelDir: gen/go
routerDir: internal/adapter/inbound/http/router
```

- **模型归属**：`gen/` 由 buf 独家维护；`--model_dir gen/go --trim_pkg=true` 使 hz 的模型路径与 go_package 对齐——**对已存在的 `.pb.go` 一律跳过**（实测 `gen/` 零改动），杜绝双工具双份模型。
- **handler/router 落点**：handler stub 生成到 `internal/adapter/inbound/http/v1/`（inbound adapter 职责：Bind+调 application 层）；router 生成到 `internal/adapter/inbound/http/router/`（`GeneratedRegister`，新 RPC 追加到 INSERT_POINT）。
- **接线**：`cmd/alert-api` 在 setup 回调调用 `httpadapter.GeneratedRegister(h)`——依赖方向 cmd→adapter→(application/domain) 合规。
- **命令**（`make idl-hz` 封装；`protoc` 本体仅 hz 需要）：

```bash
go tool hz update --module github.com/example/alert-platform \
  -I proto --idl proto/alert/ingest/v1/ingest.proto \
  --model_dir gen/go --trim_pkg=true \
  --use github.com/example/alert-platform/gen/go
```

- `--use` 指向 **buf 生成包的根**（不是 method 包），handler import 才会正确解析；`hz update` **不覆盖已存在的 handler**（手写实现安全），新增 RPC 会生成新 stub。
- 新增管理 API（source/incident/notification service）时：先在 IDL 补 `(api.post/get)` 路由注解，再 `make idl-hz`；同一 IDL 服务于 buf（模型）与 hz（路由/handler）。
- **多服务工作流（2026-11-14 PROC-007 起）**：hz 每个 IDL 生成独立 `<service>.go` 且各含 `func Register`（同包冲突）——`make idl-hz` 顺序逐 IDL 执行后由 `scripts/merge_hz_routers.py` 合并出唯一 `routes.go`。**路由注解不得使用 `{param}:action` 形式**（Hertz 路由树把同前缀的两条 `{source_id}:xxx` 视为重复注册而 panic），用 `/{param}/action` 子路径。
- **HTTP wire 时间戳（实测坑）**：Hertz 对生成结构体的 JSON 绑定走 encoding/json 语义，`google.protobuf.Timestamp` 字段必须传 **`{"seconds":..,"nanos":..}` 对象**，RFC3339 字符串会被 400 拒绝（`scripts/replay_v02.sh` 生成器按此约定）；snake_case 字段名与数字枚举不变。

### Kitex 就绪评估（2026-11-14，v0.16.4 实测）

`kitex` 对本仓 IDL **解析兼容**（service 定义可读），但硬性要求 go_package 落于 `<module>/kitex_gen`，与本仓 `gen/go` 约定冲突。结论：**V1 不引入 Kitex 运行时**（ADR-001 以 Kafka 异步避免跨进程同步 RPC）；当 docs/02 §5 拆分边界触发器发生时，引入工作为一次性 go_package 重定向（或 `-module` 匹配的独立 IDL 副本），模型来源不变。此结论如实记录，不宣称"零成本接入"。

## 4. API 和事件兼容

- Protobuf：Buf `STANDARD` lint、`FILE` breaking；禁止字段编号复用。
- HTTP JSON：同步测试 JSON 字段名、`null/absent` 差异、枚举默认值、错误码、幂等请求行为。
- Kafka：至少保留一个兼容消费者版本进行回放验证，必要时升级 `v2` Topic。
- DeepFlow：每个验证过的 v7.2.x 协议样本有 fixture；升级 vendor version 必须重新采样。

## 5. PR 强制检查

- IDL 更改是否新增/修改/删除字段，是否评估业务含义？
- `buf build`、`buf lint`、`buf breaking` 是否通过？
- Go/TS 生成代码是否重新生成，是否混入手写改动？
- Kafka 老消息是否还能被新 Consumer 消费？
- 原生 Adapter fixtures 是否包含本次字段变化？
- 是否更新 API 文档、ADR 和 SchemaVersion？

## 6. 工具链验证记录与已知限制

**2026-11-13 本地验证（ARC-003）**：安装 `buf`（go install latest）、`protoc-gen-go`、`protoc`（Homebrew libprotoc 36.2）、`hz` v0.9.8 后，以下命令全部通过：

- `buf lint`、`buf build`：通过（api 注解文件按 buf.yaml ignore 豁免 lint）。
- `buf generate`：生成 7 个 `.pb.go`；在临时模块中 `go mod tidy && go build ./...` 编译通过。
- `hz new -I proto -idl proto/alert/ingest/v1/ingest.proto`（临时目录试生成）：`(api.post)` 注解正确产出 `_v1.POST("/events:batch")` 路由注册。
- 修复：`api.proto` 从 `proto/` 根移至 `proto/api/`（`paths=source_relative` 下 go_package `gen/go/api` 与源路径不一致会导致生成代码内部 import 不可解析）；`docs/03` 的 `episode_id` 统一为 `episode_key`、指纹公式补 `entity_type`。

**2026-11-13 BASE-003 工具链钉版**：`go.mod` `tool` 指令锁定 `buf v1.75.0` + `protoc-gen-go v1.36.13-0.20261016172727-e9ee2a76ab8e`（buf v1.75.0 的官方配对，protobuf 止于 v1.36.12 正式版）+ `hz v0.9.8`；全部经 `go tool` 调用。实测：

- `go tool buf lint/build/generate` 全绿；`gen/` 用锁定版本重新生成（`make idl-drift` 绿——首次跑时正确检出旧注释漂移并阻断）。
- **breaking 红绿演练**（`make idl-breaking-drill`）：删除 `episode_key` 字段 → exit 100 精确报字段删除；恢复 → exit 0。仓库未 commit，演练用目录基线；CI 同时具备 `.git#branch=origin/main` 路径（有基线后自动生效）。
- 生成代码经 `go build ./...` 与 `go test` 验证（protobuf runtime 升至与 buf 配对版本）。

已知限制：

- `buf breaking --against '.git#branch=origin/main'` 等待仓库首次 commit（`git init` 已完成、未 commit）；`make idl-check` 会探测并给出明确提示，本地演练走目录基线。
- "CI 红/绿两次截图"证据项依赖远端仓库实跑；本地等价演练（drill + idl-drift 双向）已完成，CI 步骤已就位。
- `protoc` 本体（仅 `go tool hz` 需要）由 Homebrew 管理（libprotoc 36.2），不进 go.mod；版本记录于 docs/12。
- ~~`gen/` 生成代码尚未入库~~（BASE-001 完成；BASE-003 后由锁定工具重新生成并纳入漂移检查；`gen/README.md` 标注禁止手改）。
