package platform

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	oekithertz "github.com/oekit/hertz"
	hmetrics "github.com/oekit/hertz/metrics"
	"github.com/oekit/bootstrap"
	"github.com/oekit/bootstrap/healthz"
	"github.com/oekit/bootstrap/obs"
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

// runtimeChecks holds dependency health checks registered by cmd wiring
// before RunService (e.g. the API's PG ping). They join /healthz + /readyz.
var runtimeChecks = map[string]func(ctx context.Context) error{}
var runtimeChecksMu sync.Mutex

// RegisterRuntimeCheck adds a dependency health check surfaced on /healthz.
func RegisterRuntimeCheck(name string, check func(ctx context.Context) error) {
	runtimeChecksMu.Lock()
	defer runtimeChecksMu.Unlock()
	runtimeChecks[name] = check
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
			Insecure:    true,
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
		)

		// Standard stack: TraceID -> Recovery -> Response -> AccessLog.
		// Recovery is mandatory (Hertz has no default panic handler).
		h.Use(mw.DefaultMiddleware(mw.StackConfig{SkipPaths: probePaths})...)

		// Health endpoints (bootstrap/healthz contract: all checks pass -> 200,
		// any hard failure -> 503, soft dependency degraded -> 200 + "degraded").
		// Base-002 ships a self check; cmd wiring adds runtime dependency
		// checks (PG/Kafka) via RegisterRuntimeCheck (BASE-005/006, ING-003).
		healthz.RegisterCheck("self", func(ctx context.Context) error { return nil })
		runtimeChecksMu.Lock()
		for name, check := range runtimeChecks {
			healthz.RegisterCheck(name, check)
		}
		runtimeChecksMu.Unlock()
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
