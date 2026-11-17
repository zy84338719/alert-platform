package platform

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/oekit/bootstrap"
	"github.com/oekit/bootstrap/healthz"
	"github.com/oekit/bootstrap/obs"
	oekithertz "github.com/oekit/hertz"
	hmetrics "github.com/oekit/hertz/metrics"
	"github.com/oekit/logx"
	mw "github.com/oekit/middleware"
	"github.com/oekit/tracing"
)

// probePaths are skipped by the standard middleware stack (TraceID still
// applies upstream via hmw semantics; probes stay unwrapped so Prometheus and
// K8s can parse raw bodies — see mw.StackConfig.SkipPaths).
var probePaths = []string{"/healthz", "/livez", "/readyz", "/metrics"}

// SetupFn registers component-specific routes/tasks on the Hertz engine.
type SetupFn func(h *server.Hertz)

// RunFn is the component's blocking main loop; it receives the app context
// and must return promptly once that context is cancelled (graceful shutdown).
type RunFn func(ctx context.Context) error

// RegisterRuntimeCheck registers a dependency/background health check on the
// shared healthz registry immediately (P8-P1-01). The probe reads the registry
// live on every request, so registrations made from inside the component run
// loop (e.g. the worker's background task group) are observed. Earlier this
// only filled a local map that RunServiceWith snapshotted BEFORE run(), so a
// check registered during run() never reached /healthz or /readyz — the worker
// kept reporting healthy after a background task had died.
func RegisterRuntimeCheck(name string, check func(ctx context.Context) error) {
	healthz.RegisterCheck(name, check)
}

// RunService wires the shared base (logging, health, metrics, tracing,
// bootstrap lifecycle) and serves until the process is asked to stop.
func RunService(cfg *Config, component, version string, setup SetupFn) error {
	return RunServiceWith(cfg, component, version, setup, nil)
}

// RunServiceWith additionally runs a blocking component loop after the HTTP
// base is up (oekithertz.RunServer runs the server in the background).
func RunServiceWith(cfg *Config, component, version string, setup SetupFn, run RunFn) error {
	// Structured JSON logging; LOG_LEVEL (projected by LoadConfig) applies.
	logx.UseSlog(component)
	warnInsecureConfig(cfg, component)
	// Framework (hlog) logs route through the same logx/slog pipeline.
	hlog.SetSilentMode(true)
	hlog.SetLogger(oekithertz.NewLogger(logx.NewSlogLogger(component)))

	opts := []bootstrap.BootstrapperOption{
		bootstrap.WithServiceName(component),
		obs.WithMetrics(), // Prometheus registry + runtime metrics (local pull)
	}
	if cfg.OTLPEndpoint != "" {
		opts = append(opts, obs.WithTracing(tracing.Config{
			ServiceName: component,
			Endpoint:    cfg.OTLPEndpoint,
			Insecure:    cfg.OTLPInsecure,
		}))
	}
	b, err := bootstrap.New(opts...)
	if err != nil {
		return err
	}
	return b.Run(func(bsApp *bootstrap.App) error {
		addr, err := cfg.AddrFor(component)
		if err != nil {
			return err
		}
		h := server.New(
			server.WithHostPorts(addr),
			server.WithExitWaitTime(10*time.Second),
			// Explicit body bound (AUD-016): the framework default (4MB) was
			// implicit and did not match the ingest per-event cap. 8MB fits a
			// useful batch while keeping an attacker's request bounded.
			server.WithMaxRequestBodySize(MaxRequestBodyBytes),
		)

		// Standard stack: TraceID -> Recovery -> Response -> AccessLog.
		// Recovery is mandatory (Hertz has no default panic handler).
		h.Use(mw.DefaultMiddleware(mw.StackConfig{SkipPaths: probePaths})...)
		// AUD-016: browser hardening for every response (console + API).
		h.Use(SecurityHeaders())

		// Health endpoints (bootstrap/healthz contract: all checks pass -> 200,
		// any hard failure -> 503, soft dependency degraded -> 200 + "degraded").
		// Base-002 ships a self check; cmd wiring adds runtime dependency checks
		// (PG/Kafka/background tasks) via RegisterRuntimeCheck (BASE-005/006,
		// ING-003) — healthz reads its registry live, so late registrations from
		// inside run() (P8-P1-01) are surfaced too.
		healthz.RegisterCheck("self", func(ctx context.Context) error { return nil })
		registerProbe := func(path string) {
			h.GET(path, func(ctx context.Context, c *app.RequestContext) {
				if path == "/livez" {
					// Liveness: process is up by definition (handler running).
					c.JSON(http.StatusOK, map[string]string{"status": "alive"})
					return
				}
				status, body := healthz.Handle(ctx) // ctx, not c (timeout semantics)
				c.Data(status, "application/json; charset=utf-8", []byte(body))
			})
		}
		registerProbe("/healthz")
		registerProbe("/livez")
		registerProbe("/readyz")

		// Prometheus scrape endpoint.
		hmetrics.InitAndRegister(component, h)

		// Component routes; /version is the shared trace-correlation endpoint.
		h.GET("/version", func(ctx context.Context, c *app.RequestContext) {
			logx.CtxInfo(ctx, "version request", "component", component, "version", version)
			c.JSON(http.StatusOK, map[string]string{
				"service":   component,
				"version":   version,
				"startedAt": time.Now().UTC().Format(time.RFC3339),
			})
		})
		if setup != nil {
			setup(h)
		}

		// RunServer registers shutdown with bootstrap; a bind failure triggers
		// graceful shutdown instead of a "live" zombie process.
		oekithertz.RunServer(bsApp, component, h)
		if run != nil {
			// Component main loop (e.g. alert-worker consumption) runs on the
			// primary goroutine; it returns when the app context is cancelled.
			if err := run(bsApp.Context()); err != nil && bsApp.Context().Err() == nil {
				return err
			}
		}
		<-bsApp.Context().Done()
		return nil
	})
}

// MaxRequestBodyBytes is the explicit HTTP request-body bound (AUD-016). It is
// intentionally larger than ingest's per-event cap (256KB): one request may
// carry several events, but the body can never be unbounded.
const MaxRequestBodyBytes = 8 << 20

// warnInsecureConfig surfaces production-hostile defaults without blocking
// local/dev startup (AUD-016): plaintext OTLP export and a TLS-less PG DSN
// are acceptable on a laptop, not in production.
func warnInsecureConfig(cfg *Config, component string) {
	ctx := context.Background()
	if cfg.OTLPEndpoint != "" && cfg.OTLPInsecure {
		logx.CtxWarn(ctx, "OTLP tracing is configured INSECURE (plaintext gRPC); set OTLP_INSECURE=false in production",
			"component", component, "endpoint", cfg.OTLPEndpoint)
	}
	if strings.Contains(cfg.PGDSN, "sslmode=disable") {
		logx.CtxWarn(ctx, "PG DSN disables TLS (sslmode=disable); require verify-full in production",
			"component", component)
	}
}
