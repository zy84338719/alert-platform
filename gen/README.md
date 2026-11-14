# gen/ — 生成代码，禁止手改

`gen/go/` 下的 `.pb.go` 全部由 `make idl-generate`（`buf generate`，配置见 `buf.gen.yaml`）生成。

- 修改契约：编辑 `proto/` 下 IDL → `make idl-generate` 重新生成。
- 任何对 `gen/` 的手写改动都会在下次生成时丢失，且违反 docs/07 IDL First 规范。
- 生成代码不作为 DDD Aggregate 使用，Domain 类型独立维护（docs/07 §1）。
