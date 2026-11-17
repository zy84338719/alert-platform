// Command alert-api is the central HTTP entry: ingest, query and management
// APIs on CloudWeGo Hertz (docs/02 §2). BASE-006 wires the durable Kafka
// producer behind POST /ingest/v1/events:batch; ING-001/002/003 add the
// vendor webhooks (DeepFlow bridge / Alertmanager) authenticated against the
// PG-backed source registry.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/example/alert-platform/internal/adapter/inbound/http/console"
	httpadapter "github.com/example/alert-platform/internal/adapter/inbound/http/router"
	httpv1 "github.com/example/alert-platform/internal/adapter/inbound/http/v1"
	"github.com/example/alert-platform/internal/application/authn"
	"github.com/example/alert-platform/internal/application/history"
	"github.com/example/alert-platform/internal/application/incident"
	"github.com/example/alert-platform/internal/application/ingest"
	"github.com/example/alert-platform/internal/application/metricsquery"
	"github.com/example/alert-platform/internal/application/policymgmt"
	"github.com/example/alert-platform/internal/application/query"
	"github.com/example/alert-platform/internal/application/sourcereg"
	"github.com/example/alert-platform/internal/infrastructure/clickhouse"
	"github.com/example/alert-platform/internal/infrastructure/kafka"
	"github.com/example/alert-platform/internal/infrastructure/platform"
	"github.com/example/alert-platform/internal/infrastructure/postgres"
	"github.com/example/alert-platform/internal/infrastructure/promclient"
)

// version is injected at image build time via -ldflags "-X main.version=..." (Dockerfile/CI).
var version = "0.1.0-dev"

func main() {
	cfgPath := flag.String("config", "etc/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := platform.LoadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// AUD-016: per-identity ingest rate limiting (authenticated tenant/source
	// or edge; IP fallback for unauthenticated noise).
	if cfg.IngestRatePerSec > 0 {
		httpv1.SetIngestLimiter(httpv1.NewIngestLimiter(float64(cfg.IngestRatePerSec), float64(cfg.IngestBurst)))
	}
	defer httpv1.SetIngestLimiter(nil)

	// Durable producer: Publish returns only after acks=all (docs/04 §2).
	producer := kafka.NewProducer(strings.Split(cfg.KafkaBrokers, ","), cfg.KafkaTopic)
	defer func() { _ = producer.Close() }()
	httpv1.SetIngest(ingest.NewService(producer))

	// Source registry (ING-003): pgxpool connects lazily; a down PG fails the
	// vendor-webhook auth path (and readyz) instead of the whole process.
	pool, err := postgres.OpenPool(context.Background(), cfg.PGDSN, cfg.PGPoolMaxConns,
		platform.ParseDurationOr(cfg.PGStatementTimeout, 5*time.Second),
		platform.ParseDurationOr(cfg.PGLockTimeout, 2*time.Second),
		platform.ParseDurationOr(cfg.PGIdleTxTimeout, 30*time.Second))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer pool.Close()
	authnStore := postgres.NewAuthnStore(pool)
	// P9-R29: read-through credential cache — ingest auth must survive a PG
	// outage (positive lookups cached 30s; management writes invalidate).
	httpv1.SetSourceRegistry(sourcereg.NewService(sourcereg.NewCachedStore(postgres.NewSourceStore(pool), 30*time.Second)))
	queryStore := postgres.NewQueryStore(pool)
	httpv1.SetQuerySvc(query.NewService(queryStore))
	// P9-CH-01: history reads hit PG inside the retention window and the
	// ClickHouse archive beyond it (nil archive = PG-only).
	var historyArchive history.Store
	if cfg.ClickhouseURL != "" {
		historyArchive = clickhouse.New(cfg.ClickhouseURL, cfg.ClickhouseDB, cfg.ClickhouseUser, cfg.ClickhousePassword)
	}
	pgWindow, err := time.ParseDuration(cfg.HistoryPGWindow)
	if err != nil || pgWindow <= 0 {
		pgWindow = 7 * 24 * time.Hour
	}
	httpv1.SetHistorySvc(history.NewService(queryStore, historyArchive, pgWindow))
	policySvc := policymgmt.NewService(postgres.NewPolicyStore(pool))
	policySvc.SetAllowPrivate(cfg.AllowPrivateChannels) // dev/test escape hatch (T-013)
	httpv1.SetPolicySvc(policySvc)
	incidentStore := postgres.NewIncidentStore(pool)
	httpv1.SetIncidentSvc(incident.NewService(incidentStore))
	httpv1.LinkStore = incidentStore
	httpv1.SetAuthn(authn.NewService(authnStore))
	httpv1.SetAuditHook(func(ctx context.Context, tenantID, actor, operation, objectType, objectID string, details map[string]any) {
		_ = authnStore.WriteAudit(ctx, postgres.AuditEntry{
			TenantID: tenantID, ActorID: actor, Operation: operation,
			ObjectType: objectType, ObjectID: objectID, Details: details,
		})
	})
	platform.RegisterRuntimeCheck("postgres", func(ctx context.Context) error {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return pool.Ping(pingCtx)
	})
	// AUD-013: ingest accepts events into Kafka, so broker reachability is a
	// readiness dependency — a live PG alone must not report "ready" while
	// every publish fails.
	platform.RegisterRuntimeCheck("kafka", func(ctx context.Context) error {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return producer.Ping(pingCtx)
	})

	// P9-MC-01: the metrics center needs a Prometheus HTTP API; empty URL
	// leaves the endpoints fail-closed (503) with a clear message.
	if cfg.PrometheusURL != "" {
		httpv1.SetMetricsQuery(metricsquery.NewService(promclient.New(cfg.PrometheusURL)))
	} else {
		httpv1.SetMetricsQuery(nil)
	}

	setup := func(h *server.Hertz) {
		httpadapter.GeneratedRegister(h)
		httpv1.RegisterExternalWebhooks(h)
		httpv1.SetAuditList(authnStore.ListAudit)
		httpv1.RegisterAuditAPI(h)
		httpv1.SetOpsStore(postgres.NewStore(pool))
		httpv1.SetEdgeAPIKey(cfg.EdgeAPIKey)
		httpv1.RegisterPlatformAPI(h)
		// P9-CH-01: PG window + optional ClickHouse archive.
		console.Register(h)
	}
	if err := platform.RunService(cfg, platform.ComponentAPI, version, setup); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
