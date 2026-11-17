// Command alert-worker runs the consume-side loops (docs/02 §2):
//   - processor: canonical events Kafka -> PG transaction -> offset commit
//     (BASE-007; PROC-001 adds retry/DLQ policy);
//   - relay: committed domain_outbox rows -> Kafka lifecycle topic (PROC-003);
//   - scheduler: source-health stale reconciliation (PROC-006).
//
// P1-03: every loop runs under a supervised task group — an unexpected exit
// or panic in a CRITICAL loop (processor/relay/notifier/escalation) fails the
// process (fail-stop, platform restarts) instead of only logging while the
// consumer kept running; auxiliary loops (health/retention) degrade the
// readiness probes via the background-tasks runtime check. All loops stop on
// app-context cancellation (graceful shutdown).
package main

import (
	"context"

	"flag"
	"fmt"
	"github.com/google/uuid"
	"os"
	"strings"
	"time"

	oekitmetrics "github.com/oekit/metrics"

	"github.com/example/alert-platform/internal/application/archive"
	"github.com/example/alert-platform/internal/application/escalation"
	"github.com/example/alert-platform/internal/application/health"
	"github.com/example/alert-platform/internal/application/notify"
	"github.com/example/alert-platform/internal/application/processor"
	"github.com/example/alert-platform/internal/application/relay"
	"github.com/example/alert-platform/internal/application/retention"
	"github.com/example/alert-platform/internal/application/telemetry"
	"github.com/example/alert-platform/internal/application/worker"
	"github.com/example/alert-platform/internal/infrastructure/channel"
	"github.com/example/alert-platform/internal/infrastructure/clickhouse"
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
	profile, err := parseProfile(cfg.WorkerProfile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// The processor consume loop blocks the primary goroutine; relay and the
	// health scheduler run beside it. Everything stops with the app context.
	run := func(ctx context.Context) error {
		pool, err := postgres.OpenPool(ctx, cfg.PGDSN, cfg.PGPoolMaxConns,
			platform.ParseDurationOr(cfg.PGStatementTimeout, 5*time.Second),
			platform.ParseDurationOr(cfg.PGLockTimeout, 2*time.Second),
			platform.ParseDurationOr(cfg.PGIdleTxTimeout, 30*time.Second))
		if err != nil {
			return fmt.Errorf("pg pool: %w", err)
		}
		defer pool.Close()

		brokers := strings.Split(cfg.KafkaBrokers, ",")

		store := postgres.NewStore(pool)
		// P1-03 supervision: critical loops fail-stop, auxiliary loops degrade
		// readiness. The readiness view joins /healthz + /readyz.
		// P9-OBS-07: independent self-alert channel (never via notification_outbox).
		selfAlerts := channel.NewSelfAlerter(cfg.FallbackWebhook, cfg.FallbackAllowPrivate)
		group := worker.NewGroup()

		// processor: Kafka -> PG (offsets after commit; DLQ for poison).
		// Consumer-group scaling only: this profile starts no singleton loops.
		if profile.Processor {
			processorSvc := processor.NewService(store)
			consumer := kafka.NewConsumer(brokers, cfg.KafkaTopic, cfg.KafkaGroupID, cfg.KafkaDLQTopic)
			defer func() { _ = consumer.Close() }()
			group.Go(worker.Task{Name: "processor", Critical: true, Run: func(ctx context.Context) error {
				return consumer.Run(ctx, processorSvc.Handle)
			}})
		}

		// notifier: relay (leased, ordered) + notifier (leased) + escalation
		// (single-flight). Safe to run on several replicas.
		if profile.Notifier {
			relaySvc := relay.NewService(store, kafka.NewProducer(brokers, cfg.KafkaLifecycleTopic), uuid.NewString())
			relaySvc.SetTuning(0, 0, 0, cfg.OutboxMaxAttempts)
			group.Go(worker.Task{Name: "relay", Critical: true, Run: relaySvc.Run})

			notifySvc := notify.NewService(store, &channelSenderAdapter{
				sender: channel.NewHTTPSender(cfg.AllowPrivateChannels),
			})
			notifySvc.SetMode(cfg.NotifyMode) // REL-001: active|shadow|off
			notifySvc.SetMaxAttempts(cfg.NotifyMaxAttempts)
			// P8-P0-01: establish the persisted authority generation BEFORE any
			// producer enqueues tasks — a fresh Pod must know which era owns
			// delivery, or a shadow→active rollout would replay history.
			if err := notifySvc.InitAuthority(ctx); err != nil {
				return fmt.Errorf("notify authority: %w", err)
			}
			group.Go(worker.Task{Name: "notifier", Critical: true, Run: notifySvc.Run})

			escSvc := escalation.NewService(store, store)
			escSvc.SetOwner(uuid.NewString()) // single-flight across replicas
			group.Go(worker.Task{Name: "escalation", Critical: true, Run: escSvc.Run})
		}

		// maintenance: singleton loops. Every one additionally holds a DB
		// lease, so even an accidental second maintenance replica is safe.
		if profile.Maintenance {
			healthSvc := health.NewService(store)
			healthSvc.SetTuning(mustDuration(cfg.StaleAfter), mustDuration(cfg.ReconcileInterval))
			group.Go(worker.Task{Name: "health-scheduler", Critical: false, Run: healthSvc.Run})

			retentionSvc := retention.NewService(retention.Multi(store, store),
				mustDurationOr(cfg.RetentionReceipts, 7*24*time.Hour),
				mustDurationOr(cfg.RetentionInterval, time.Hour),
				oekitmetrics.Registry)
			retentionSvc.SetLeaser(store)
			retentionSvc.SetOwner(uuid.NewString())
			group.Go(worker.Task{Name: "retention", Critical: false, Run: retentionSvc.Run})

			telemetrySvc := telemetry.NewService(store)
			telemetrySvc.SetLeaser(store)
			telemetrySvc.SetOwner(uuid.NewString())
			group.Go(worker.Task{Name: "telemetry", Critical: false, Run: telemetrySvc.Run})

			if cfg.ClickhouseURL != "" {
				archiveSvc := archive.NewService(postgres.NewArchiveStore(pool), clickhouse.New(
					cfg.ClickhouseURL, cfg.ClickhouseDB, cfg.ClickhouseUser, cfg.ClickhousePassword),
					uuid.NewString()) // single-writer lease owner
				archiveSvc.SetTuning(mustDurationOr(cfg.ArchiveInterval, 10*time.Second),
					cfg.ArchiveBatch, cfg.ArchiveMaxBatches, cfg.ArchiveMaxAttempts)
				group.Go(worker.Task{Name: "archive", Critical: false, Run: archiveSvc.Run})
			}
		}
		platform.RegisterRuntimeCheck("background-tasks", group.ReadinessCheck())
		platform.RegisterRuntimeCheck("postgres", func(ctx context.Context) error {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			return pool.Ping(pingCtx)
		})

		runErr := group.Run(ctx)
		if runErr != nil && ctx.Err() == nil {
			// Best effort, detached from the (cancelled) component context:
			// the process is about to exit, the alarm must still leave.
			nctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			selfAlerts.Notify(nctx, platform.ComponentWorker, "critical background task exited: "+runErr.Error())
			cancel()
		}
		return runErr
	}

	if err := platform.RunServiceWith(cfg, platform.ComponentWorker, version, nil, run); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func mustDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0 // service falls back to its defaults
	}
	return d
}

func mustDurationOr(s string, fallback time.Duration) time.Duration {
	if d := mustDuration(s); d > 0 {
		return d
	}
	return fallback
}

// channelSenderAdapter bridges infrastructure/channel results onto the
// application port type (the layers must not share concrete types).
type channelSenderAdapter struct{ sender *channel.HTTPSender }

func (a *channelSenderAdapter) Send(ctx context.Context, channelType string, cfg, payload []byte) notify.SendResult {
	r := a.sender.Send(ctx, channelType, cfg, payload)
	return notify.SendResult{
		Sent: r.Sent, HTTPStatus: r.HTTPStatus, RetryAfter: r.RetryAfter,
		FailureCode: r.FailureCode, Err: r.Err,
	}
}
