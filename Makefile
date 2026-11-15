.PHONY: test check build deps-lint dev-up dev-down dev-trace dev-deps dev-deps-down dev-smoke test-e2e idl-build idl-check idl-generate idl-drift idl-breaking-drill idl-hz validate-package

build:
	GOWORK=off go build ./...

# BASE-002: local one-shot start + /healthz + trace_id chain demo.
dev-up:
	bash scripts/dev_up.sh up

dev-down:
	bash scripts/dev_up.sh down

dev-trace:
	bash scripts/dev_up.sh trace

# BASE-006/007/008: resident dev dependencies (fixed ports) + full smoke.
dev-deps:
	docker compose -f deploy/dev/docker-compose.yaml up -d

dev-deps-down:
	docker compose -f deploy/dev/docker-compose.yaml down

# BASE-008: end-to-end smoke (real processes, real Kafka/PG, trace correlation).
dev-smoke:
	bash scripts/smoke_e2e.sh

# Integration tests against the resident dev deps (fast, no container churn):
#   ALERT_TEST_PG_DSN=postgres://postgres:postgres@127.0.0.1:55432/alert_platform?sslmode=disable
#   ALERT_TEST_KAFKA_BROKERS=127.0.0.1:19092
test-e2e:
	ALERT_TEST_PG_DSN="postgres://postgres:postgres@127.0.0.1:55432/alert_platform?sslmode=disable" \
	ALERT_TEST_KAFKA_BROKERS="127.0.0.1:19092" \
	GOWORK=off go test -count=1 -v ./internal/infrastructure/kafka/ ./internal/infrastructure/postgres/

test:
	GOWORK=off go test ./...

# BASE-005: PG integration tests. Without ALERT_TEST_PG_DSN they use
# Testcontainers (needs working Docker); with it they target any disposable
# PostgreSQL — e.g. the rpi-k3s instance from deploy/test/pg-test.yaml:
#   ALERT_TEST_PG_DSN=postgres://postgres:test@10.10.10.20:30543/postgres?sslmode=disable \
#     go test -count=1 ./internal/infrastructure/postgres/
test-pg:
	GOWORK=off go test -count=1 -v ./internal/infrastructure/postgres/

# golangci-lint quality gate (pinned v2.15.0 in CI): zero-finding baseline
# established 2026-11-15 (errcheck/staticcheck/govet/ineffassign/unused).
lint:
	golangci-lint run --timeout=5m

# BASE-001: enforce docs/02 §3 layer dependency direction.
deps-lint:
	python3 scripts/check_deps.py

validate-package:
	python3 scripts/validate_package.py

# ---------------------------------------------------------------------------
# IDL toolchain (BASE-003): all tools are version-locked in go.mod `tool`
# directives and invoked via `go tool` — no global installs, CI runs the same
# commands. The protoc-gen-go shim exists because buf.gen.yaml's `local:`
# entry is resolved against PATH, while the real binary is `go tool`-managed.
IDL_BIN := .dev/idlbin

$(IDL_BIN)/protoc-gen-go:
	@mkdir -p $(IDL_BIN)
	@printf '#!/bin/sh\nexec go tool protoc-gen-go "$$@"\n' > $@ && chmod +x $@

idl-build:
	GOWORK=off go tool buf build

idl-generate: $(IDL_BIN)/protoc-gen-go
	PATH="$(CURDIR)/$(IDL_BIN):$$PATH" GOWORK=off go tool buf generate

# Generate-code drift check: snapshot the checked-in gen/, regenerate, then
# compare. File-level diff (not git diff) so semantics hold before the first
# commit too; CI runs the same target.
idl-drift: $(IDL_BIN)/protoc-gen-go
	@rm -rf .dev/gen-baseline && cp -r gen .dev/gen-baseline
	@$(MAKE) --no-print-directory idl-generate
	@if diff -r .dev/gen-baseline gen >/dev/null; then \
		echo "OK: gen/ matches protos (no codegen drift)"; \
	else \
		echo "FAIL: gen/ drifted from protos — run make idl-generate and commit"; \
		diff -r .dev/gen-baseline gen | head -20; \
		exit 1; \
	fi

# Breaking-change drill (docs/13 BASE-003): directory baseline so it works
# on the un-committed repo; CI uses the .git#branch form once a baseline
# exists (see idl-check).
idl-breaking-drill:
	bash scripts/breaking_drill.sh

# hz (BASE-004/ING): regenerate Hertz handler/router stubs from IDL.
# Models stay in gen/ — owned by buf; `--use` points handlers at buf packages
# and `--trim_pkg` keeps hz from re-nesting the model path. Existing handler
# bodies are never overwritten (hz keeps handwritten code).
# hz (BASE-004/ING): regenerate Hertz handler/router stubs from IDL.
# Models stay in gen/ — owned by buf. hz emits one <service>.go router file
# per IDL, each with its own `func Register` in the same package, so we run it
# once per service IDL (middleware.go accumulates hooks across runs) and merge
# the Register bodies into routes.go. Existing handler bodies are never
# overwritten (hz keeps handwritten code).
IDL_FILES := ingest source query policy silence channel incident
idl-hz:
	@for svc in $(IDL_FILES); do \
		PATH="$(CURDIR)/$(IDL_BIN):$$PATH" GOWORK=off go tool hz update \
			--module github.com/example/alert-platform \
			-I proto \
			--idl proto/alert/$$svc/v1/$$svc.proto \
			--model_dir gen/go \
			--trim_pkg=true \
			--use github.com/example/alert-platform/gen/go || exit 1; \
	done
	python3 scripts/merge_hz_routers.py internal/adapter/inbound/http/router/v1 $(IDL_FILES)

idl-check:
	GOWORK=off go tool buf lint
	GOWORK=off go tool buf build
	@if git rev-parse --verify origin/main >/dev/null 2>&1; then \
		go tool buf breaking --against '.git#branch=origin/main'; \
	elif git rev-parse --verify main >/dev/null 2>&1; then \
		go tool buf breaking --against '.git#branch=main'; \
	else \
		echo "skip breaking: no git baseline yet; local drill: make idl-breaking-drill"; \
	fi

check: validate-package deps-lint build test
