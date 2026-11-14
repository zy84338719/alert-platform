// Command alert-worker runs the consume-side loops (docs/02 §2):
//   - processor: canonical events Kafka -> PG transaction -> offset commit
//     (BASE-007; PROC-001 adds retry/DLQ policy);
//   - relay: committed domain_outbox rows -> Kafka lifecycle topic (PROC-003);
//   - scheduler: source-health stale reconciliation (PROC-006).
//
// All three stop on app-context cancellation (graceful shutdown).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	oekitmetrics "github.com/oekit/metrics"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/alert-platform/internal/application/escalation"
	"github.com/example/alert-platform/internal/application/health"
	"github.com/example/alert-platform/internal/application/notify"
	"github.com/example/alert-platform/internal/application/processor"
	"github.com/example/alert-platform/internal/application/relay"
	"github.com/example/alert-platform/internal/application/retention"
	"github.com/example/alert-platform/internal/infrastructure/channel"
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

	// The processor consume loop blocks the primary goroutine; relay and the
	// health scheduler run beside it. Everything stops with the app context.
	run := func(ctx context.Context) error {
		pool, err := pgxpool.New(ctx, cfg.PGDSN)
		if err != nil {
			return fmt.Errorf("pg pool: %w", err)
		}
		defer pool.Close()

		brokers := strings.Split(cfg.KafkaBrokers, ",")

		// processor: Kafka -> PG (offsets after commit; DLQ for poison).
		store := postgres.NewStore(pool)
		processorSvc := processor.NewService(store)
		consumer := kafka.NewConsumer(brokers, cfg.KafkaTopic, cfg.KafkaGroupID, cfg.KafkaDLQTopic)
		defer consumer.Close()

		// relay: domain_outbox -> lifecycle topic.
		relaySvc := relay.NewService(store, kafka.NewProducer(brokers, cfg.KafkaLifecycleTopic))

		// scheduler: source-health stale reconciliation.
		healthSvc := health.NewService(store)
		healthSvc.SetTuning(mustDuration(cfg.StaleAfter), mustDuration(cfg.ReconcileInterval))

		// notifier: notification_outbox -> channels (NOTI-001/002). The
		// adapter converts infrastructure results to the application type.
		notifySvc := notify.NewService(store, &channelSenderAdapter{
			sender: channel.NewHTTPSender(cfg.AllowPrivateChannels),
		})
		notifySvc.SetMode(cfg.NotifyMode) // REL-001: active|shadow|off

		// retention: receipts cleanup (RETN-001) + notifications/audit (RETN-002);
		// counters join the registry the /metrics endpoint serves.
		retentionSvc := retention.NewService(retention.Multi(store, store),
			mustDurationOr(cfg.RetentionReceipts, 7*24*time.Hour),
			mustDurationOr(cfg.RetentionInterval, time.Hour),
			oekitmetrics.Registry)

		go func() {
			if err := relaySvc.Run(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "relay:", err)
			}
		}()
		go func() {
			if err := healthSvc.Run(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "health scheduler:", err)
			}
		}()
		go func() {
			if err := notifySvc.Run(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "notifier:", err)
			}
		}()
		go func() {
			if err := retentionSvc.Run(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "retention:", err)
			}
		}()
		// escalation ladder + auto-correlation (INC-002/005).
		escSvc := escalation.NewService(store, store)
		go func() {
			if err := escSvc.Run(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "escalation:", err)
			}
		}()

		return consumer.Run(ctx, processorSvc.Handle)
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
