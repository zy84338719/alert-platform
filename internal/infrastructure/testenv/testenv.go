// Package testenv provides shared integration-test infrastructure: disposable
// PostgreSQL/Kafka via Testcontainers, or externally provisioned instances via
// environment variables, with loud skips instead of red failures when the
// local Docker data path is unhealthy (see DEVELOPMENT_LOG 2026-11-13).
package testenv

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq" // maintenance connections for CREATE/DROP DATABASE
	"github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// RequireDocker skips the test when the Docker daemon is unreachable.
func RequireDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("docker unavailable, skipping Testcontainers test: %v", err)
	}
}

func probeOrSkip(t *testing.T, what string, ping func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ping(ctx); err != nil {
		t.Skipf("%s data path unhealthy (port-forward fault or slow start; "+
			"see DEVELOPMENT_LOG 2026-11-13): %v", what, err)
	}
}

// StartPostgres returns a DSN to a disposable database. Modes:
//
//   - ALERT_TEST_PG_DSN set: create a throwaway database on that instance
//     (e.g. the rpi-k3s pg-test NodePort) and drop it on cleanup.
//   - otherwise: Testcontainers postgres:16-alpine.
//
// Skips loudly when the container data path does not carry data.
func StartPostgres(t *testing.T) string {
	t.Helper()
	if ext := os.Getenv("ALERT_TEST_PG_DSN"); ext != "" {
		return externalPostgres(t, ext)
	}
	RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pg, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("alert_platform_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		termCtx, termCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer termCancel()
		_ = pg.Terminate(termCtx)
	})
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	// Docker Desktop publishes the mapped port on IPv4 loopback only, while
	// "localhost" may resolve to ::1 first — pin the loopback address.
	dsn = strings.Replace(dsn, "@localhost:", "@127.0.0.1:", 1)
	probeOrSkip(t, "postgres (testcontainers)", func(ctx context.Context) error {
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return err
		}
		defer pool.Close()
		return pool.Ping(ctx)
	})
	return dsn
}

// externalPostgres connects to a maintenance DSN (database part ignored),
// creates a uniquely named database for this test run, and returns a DSN
// pointing at it. Dropped on cleanup (PG13+ FORCE); drop failures turn the
// test red so leaks never stay silent.
func externalPostgres(t *testing.T, maintenanceDSN string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	u, err := url.Parse(maintenanceDSN)
	if err != nil {
		t.Fatalf("ALERT_TEST_PG_DSN is not a URL: %v", err)
	}
	maintenance := *u
	maintenance.Path = "/postgres" // maintenance database for CREATE/DROP

	admin, err := sql.Open("postgres", maintenance.String())
	if err != nil {
		t.Fatalf("open maintenance connection: %v", err)
	}
	// Do NOT defer-close here: t.Cleanup runs after this helper returns, and
	// the DROP below needs the still-open pool (closing first made every
	// cleanup fail silently and leaked 13 databases before this was caught).

	dbName := fmt.Sprintf("alert_it_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+dbName); err != nil {
		_ = admin.Close()
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() {
		defer func() { _ = admin.Close() }()
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		if _, err := admin.ExecContext(cctx,
			"DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)"); err != nil {
			t.Errorf("drop test database %s: %v", dbName, err) // surface leaks
		}
	})

	testDB := *u
	testDB.Path = "/" + dbName
	return testDB.String()
}

// StartKafka returns broker addresses of a disposable single-node Kafka.
// Mode: ALERT_TEST_KAFKA_BROKERS (comma-separated) or Testcontainers.
// Skips loudly when the container data path does not carry data.
func StartKafka(t *testing.T) []string {
	t.Helper()
	if ext := os.Getenv("ALERT_TEST_KAFKA_BROKERS"); ext != "" {
		return strings.Split(ext, ",")
	}
	RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0") // module starter script targets this image
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() {
		termCtx, termCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer termCancel()
		_ = kc.Terminate(termCtx)
	})
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("brokers: %v", err)
	}
	for i, b := range brokers {
		brokers[i] = strings.Replace(b, "localhost", "127.0.0.1", 1)
	}
	probeOrSkip(t, "kafka (testcontainers)", func(ctx context.Context) error {
		return probeKafka(ctx, brokers[0])
	})
	return brokers
}

// probeKafka verifies the broker answers protocol requests (not just TCP).
func probeKafka(ctx context.Context, broker string) error {
	client := &kafka.Client{Addr: kafka.TCP(broker), Timeout: 10 * time.Second}
	_, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	return err
}
