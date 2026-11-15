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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/alert-platform/internal/adapter/inbound/http/console"
	httpadapter "github.com/example/alert-platform/internal/adapter/inbound/http/router"
	httpv1 "github.com/example/alert-platform/internal/adapter/inbound/http/v1"
	"github.com/example/alert-platform/internal/application/authn"
	"github.com/example/alert-platform/internal/application/incident"
	"github.com/example/alert-platform/internal/application/ingest"
	"github.com/example/alert-platform/internal/application/policymgmt"
	"github.com/example/alert-platform/internal/application/query"
	"github.com/example/alert-platform/internal/application/sourcereg"
	"github.com/example/alert-platform/internal/infrastructure/kafka"
	"github.com/example/alert-platform/internal/infrastructure/platform"
	"github.com/example/alert-platform/internal/infrastructure/postgres"
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

	// Durable producer: Publish returns only after acks=all (docs/04 §2).
	producer := kafka.NewProducer(strings.Split(cfg.KafkaBrokers, ","), cfg.KafkaTopic)
	defer func() { _ = producer.Close() }()
	httpv1.SetIngest(ingest.NewService(producer))

	// Source registry (ING-003): pgxpool connects lazily; a down PG fails the
	// vendor-webhook auth path (and readyz) instead of the whole process.
	pool, err := pgxpool.New(context.Background(), cfg.PGDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer pool.Close()
	authnStore := postgres.NewAuthnStore(pool)
	httpv1.SetSourceRegistry(sourcereg.NewService(postgres.NewSourceStore(pool)))
	httpv1.SetQuerySvc(query.NewService(postgres.NewQueryStore(pool)))
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

	setup := func(h *server.Hertz) {
		httpadapter.GeneratedRegister(h)
		httpv1.RegisterExternalWebhooks(h)
		httpv1.SetAuditList(authnStore.ListAudit)
		httpv1.RegisterAuditAPI(h)
		httpv1.SetOpsStore(postgres.NewStore(pool))
		httpv1.SetEdgeAPIKey(cfg.EdgeAPIKey)
		httpv1.RegisterPlatformAPI(h)
		console.Register(h)
	}
	if err := platform.RunService(cfg, platform.ComponentAPI, version, setup); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
