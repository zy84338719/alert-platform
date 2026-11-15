package kafka

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/application/ingest"
	"github.com/example/alert-platform/internal/application/processor"
	"github.com/example/alert-platform/internal/infrastructure/postgres"
	"github.com/example/alert-platform/internal/infrastructure/testenv"
)

// TestIngestPipelineE2E is the BASE-007 acceptance path: ingest service (real
// Kafka, durable producer) -> processor (consumer semantics) -> PG store
// (receipts + instance + transition), with the receipts layer absorbing a
// redelivered message.
func TestIngestPipelineE2E(t *testing.T) {
	brokers := testenv.StartKafka(t)
	dsn := testenv.StartPostgres(t)
	dir := e2eMigrations(t)
	if _, err := postgres.Up(dsn, dir); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pg pool: %v", err)
	}
	defer pool.Close()

	topic := fmt.Sprintf("it_e2e_%d", time.Now().UnixNano())
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	if _, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// 1) Ingest: durable producer via the application service.
	producer := NewProducer(brokers, topic)
	defer func() { _ = producer.Close() }()
	ingestSvc := ingest.NewService(producer)

	batchID := "e2e-batch-1"
	ev := canonicalEvent()
	if _, err := ingestSvc.PushBatch(ctx, batchID, []*eventv1.AlertEvent{ev}); err != nil {
		t.Fatalf("PushBatch: %v", err)
	}

	// 2) Processor: consume one message and apply it to PG.
	processorSvc := processor.NewService(postgres.NewStore(pool))
	msg := fetchOne(t, brokers, topic)
	if err := processorSvc.Handle(ctx, msg); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// 3) PG assertions: receipt + firing instance + one transition.
	tenantID := ev.GetTenantId()
	sourceID := ev.GetSource().GetSourceId()
	var receipts, instances, transitions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_receipts
		WHERE tenant_id=$1 AND source_id=$2 AND transport_id=$3`,
		tenantID, sourceID, ev.GetTransportId()).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("receipts = %d err=%v, want 1", receipts, err)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM alert_instances
		WHERE tenant_id=$1`, tenantID).Scan(&instances); err != nil || instances != 1 {
		t.Fatalf("instances = %d err=%v, want 1", instances, err)
	}
	if err := pool.QueryRow(ctx, `SELECT i.monitor_state FROM alert_instances i
		WHERE i.tenant_id=$1`, tenantID).Scan(&state); err != nil || state != "firing" {
		t.Fatalf("state = %q err=%v, want firing", state, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM alert_transitions t
		JOIN alert_instances i ON i.id=t.alert_instance_id WHERE i.tenant_id=$1`,
		tenantID).Scan(&transitions); err != nil || transitions != 1 {
		t.Fatalf("transitions = %d err=%v, want 1", transitions, err)
	}

	// 4) Redelivery: the same transport_id must be absorbed by receipts.
	dup, err := postgres.NewStore(pool).ApplyEvent(ctx, ev)
	if err != nil {
		t.Fatalf("redelivered apply: %v", err)
	}
	if !dup {
		t.Fatal("redelivered event must be reported as duplicate")
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM alert_instances
		WHERE tenant_id=$1`, tenantID).Scan(&instances); err != nil || instances != 1 {
		t.Fatalf("instances after redelivery = %d err=%v, want still 1", instances, err)
	}
}

// TestIngestDuplicateBatchNoNewTransitions proves batch retries reuse the
// deterministic transport_id: a full second PushBatch of the same batch_id
// yields duplicate receipts, no new instances/transitions.
func TestIngestDuplicateBatchNoNewTransitions(t *testing.T) {
	brokers := testenv.StartKafka(t)
	dsn := testenv.StartPostgres(t)
	if _, err := postgres.Up(dsn, e2eMigrations(t)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pg pool: %v", err)
	}
	defer pool.Close()

	topic := fmt.Sprintf("it_dup_%d", time.Now().UnixNano())
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	if _, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := NewProducer(brokers, topic)
	defer func() { _ = producer.Close() }()
	ingestSvc := ingest.NewService(producer)
	ev := canonicalEvent()
	for i := 0; i < 2; i++ { // original + client retry of the same batch
		if _, err := ingestSvc.PushBatch(ctx, "dup-batch", []*eventv1.AlertEvent{ev}); err != nil {
			t.Fatalf("PushBatch #%d: %v", i, err)
		}
	}

	processorSvc := processor.NewService(postgres.NewStore(pool))
	for i := 0; i < 2; i++ {
		msg := fetchOne(t, brokers, topic)
		if err := processorSvc.Handle(ctx, msg); err != nil {
			t.Fatalf("handle #%d: %v", i, err)
		}
	}

	var receipts, instances int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_receipts
		WHERE tenant_id=$1 AND transport_id=$2`,
		ev.GetTenantId(), ev.GetTransportId()).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("receipts = %d err=%v, want 1 (retry reuses transport_id)", receipts, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM alert_instances
		WHERE tenant_id=$1`, ev.GetTenantId()).Scan(&instances); err != nil || instances != 1 {
		t.Fatalf("instances = %d err=%v, want 1", instances, err)
	}
}

func fetchOne(t *testing.T, brokers []string, topic string) processor.IncomingMessage {
	t.Helper()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   500 * time.Millisecond,
	})
	defer func() { _ = reader.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	hdrs := map[string]string{}
	for _, h := range msg.Headers {
		hdrs[h.Key] = string(h.Value)
	}
	return processor.IncomingMessage{Key: string(msg.Key), Headers: hdrs, Value: msg.Value}
}

func canonicalEvent() *eventv1.AlertEvent {
	return &eventv1.AlertEvent{
		EventId:   "ev-e2e-1",
		TenantId:  uuid.NewString(),
		Source:    &eventv1.SourceRef{SourceId: uuid.NewString(), ExternalRuleId: "rule-1"},
		Resource:  &eventv1.ResourceRef{ClusterId: "c1", ResourceType: "pod", ResourceUid: "uid-1"},
		Signal:    eventv1.EventSignal_EVENT_SIGNAL_FIRING,
		Severity:  eventv1.Severity_SEVERITY_CRITICAL,
		AlertName: "e2e-test-alert",
		EventKey:  "ek-e2e-1",
		Labels:    map[string]string{"team": "sre"},
	}
}

func e2eMigrations(t *testing.T) string {
	t.Helper()
	dir, err := filepathAbs("../../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func filepathAbs(p string) (string, error) {
	return filepath.Abs(p)
}
