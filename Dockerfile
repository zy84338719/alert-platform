# syntax=docker/dockerfile:1.7
# Single image for all three binaries (docs/02 §2): the entry binary is
# selected at run time (`docker run ... alert-platform:dev alert-worker` or
# K8s `command: ["alert-worker"]`).
#
# Multi-arch by construction: compilation runs on $BUILDPLATFORM at native
# speed and cross-compiles (CGO off) to $TARGETARCH — no QEMU needed for the
# build stage; the runtime stage only COPYs artifacts.
# Pinned base images (BASE-004); bump deliberately, not implicitly.
ARG GO_IMAGE=golang:1.27.4-alpine
ARG RUN_IMAGE=alpine:3.21

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
WORKDIR /src

# Warm the module cache in its own layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# TARGETOS/TARGETARCH are injected by buildx; default to the build host so a
# plain `docker build` keeps working.
ARG TARGETOS=linux
ARG TARGETARCH=arm64
# VERSION lands in each binary's `version` var via -X (set from git by CI).
ARG VERSION=dev

# Explicit binary list (go build -o dir/ ./cmd/... proved fragile under
# buildx attestation builds: RUN exited 0 with an empty /out/ on the CI
# runner). `ls` after build fails the stage loudly if anything is missing.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/alert-api ./cmd/alert-api \
    && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/alert-worker ./cmd/alert-worker \
    && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/alert-edge ./cmd/alert-edge \
    && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/alert-migrate ./cmd/alert-migrate \
    && ls -la /out/ \
    && test -x /out/alert-api -a -x /out/alert-worker -a -x /out/alert-edge

FROM ${RUN_IMAGE} AS runtime
# Non-root runtime user (docs/08 security baseline).
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/alert-api /out/alert-worker /out/alert-edge /out/alert-migrate /usr/local/bin/

# Ship the dev config baseline so the image runs out of the box; deployments
# override via env (env > YAML > default, platform.Config) or by mounting a
# ConfigMap over /app/etc/config.yaml and passing -config.
WORKDIR /app
COPY --chown=app:app etc/ etc/
# alert-migrate's default -dir is the relative path "migrations" (CWD=/app).
COPY --chown=app:app migrations/ migrations/

USER app
# Default entry; override per component (alert-worker / alert-edge).
# Health probing: K8s uses httpGet probes (no in-image tooling needed);
# locally, probe from a sibling container on the same network namespace.
ENTRYPOINT ["/usr/local/bin/alert-api"]
