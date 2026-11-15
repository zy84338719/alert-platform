package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/example/alert-platform/internal/infrastructure/testenv"
)

// TestProducerRequiresAcksAll asserts the acks=all contract at the unit level
// (docs/13 BASE-006 "configuration assertion"): durability before response.
func TestProducerRequiresAcksAll(t *testing.T) {
	p := NewProducer([]string{"127.0.0.1:9092"}, "unused-topic")
	defer func() { _ = p.Close() }()
	if p.RequiredAcks() != kafka.RequireAll {
		t.Fatalf("RequiredAcks = %v, want RequireAll (acks=all)", p.RequiredAcks())
	}
}

// TestProducerPublishConsumeRoundTrip writes through the real broker and
// reads back, asserting key/value/header preservation end to end.
func TestProducerPublishConsumeRoundTrip(t *testing.T) {
	brokers := testenv.StartKafka(t)
	topic := fmt.Sprintf("it_producer_%d", time.Now().UnixNano())

	// Explicit topic creation: production manages topics deliberately (no
	// reliance on broker auto-create, which confluent-local disables).
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	if _, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	p := NewProducer(brokers, topic)
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	value := []byte(`{"batch_id":"it-1","events":[]}`)
	headers := []kafka.Header{
		{Key: HeaderSchemaName, Value: []byte("alert.ingest.v1.PushBatchRequest")},
		{Key: HeaderSchemaVersion, Value: []byte("1")},
		{Key: HeaderTransportID, Value: []byte("tr-it-1")},
		{Key: HeaderTraceID, Value: []byte("trace-it-1")},
	}
	if err := p.Publish(ctx, "tenant-a|source-1", headers, value); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Read back through an independent consumer path.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   500 * time.Millisecond,
	})
	defer func() { _ = reader.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readCancel()
	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if string(msg.Key) != "tenant-a|source-1" || string(msg.Value) != string(value) {
		t.Fatalf("round-trip mismatch: key=%q value=%q", msg.Key, msg.Value)
	}
	got := map[string]string{}
	for _, h := range msg.Headers {
		got[h.Key] = string(h.Value)
	}
	for _, want := range headers {
		if got[want.Key] != string(want.Value) {
			t.Fatalf("header %s = %q, want %q", want.Key, got[want.Key], want.Value)
		}
	}
}

// TestProducerBrokerDownFailsLoudly pins the BASE-006 error semantics: a
// downed broker must surface as an error to the caller (retriable), never as
// a silent drop.
func TestProducerBrokerDownFailsLoudly(t *testing.T) {
	p := NewProducer([]string{"127.0.0.1:1"}, "unused") // nothing listens here
	defer func() { _ = p.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Publish(ctx, "k", nil, []byte("v")); err == nil {
		t.Fatal("publish to unreachable broker must fail, not drop silently")
	}
}
