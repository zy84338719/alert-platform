package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/application/processor"
	"github.com/example/alert-platform/internal/infrastructure/testenv"
)

// PROC-001: poison messages are quarantined to the DLQ and the offset
// commits; a transient failure exhausts retries and fails the consumer
// WITHOUT committing (redelivery preserves at-least-once).
func TestConsumerDLQAndFailStop(t *testing.T) {
	brokers := testenv.StartKafka(t)
	ctx := context.Background()

	topic := "dlq-test-events"
	dlqTopic := "dlq-test-dlq"
	createTopic(t, brokers, topic)
	createTopic(t, brokers, dlqTopic)

	// One malformed (permanent) + one well-formed event.
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}
	if err := w.WriteMessages(ctx,
		kafka.Message{Key: []byte("k"), Value: []byte("not-protobuf-at-all")},
		kafka.Message{Key: []byte("k"), Value: mustProtoEvent(t, "tp-ok-1")},
	); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	waitForMessages(t, brokers, topic, 2)

	consumer := NewConsumer(brokers, topic, "dlq-test-group", dlqTopic)
	defer func() { _ = consumer.Close() }()

	// Run returns on cancellation; the handler cancels once both messages
	// have been seen, so Run's error is the graceful nil.
	runCtx, cancel := context.WithCancel(ctx)
	applied := 0
	handlerDone := make(chan struct{})
	var runErr error
	go func() {
		runErr = consumer.Run(runCtx, func(ctx context.Context, msg processor.IncomingMessage) error {
			applied++
			if string(msg.Value) == "not-protobuf-at-all" {
				return processor.Permanent(context.Canceled) // any permanent error
			}
			if applied >= 2 {
				// Delay the cancel so the consumer's offset commit for THIS
				// message (immediately after the handler returns) runs on a
				// live context; a hard cancel here would skip the commit and
				// the group would replay message 2.
				time.AfterFunc(700*time.Millisecond, cancel)
			}
			return nil
		})
		close(handlerDone)
	}()
	select {
	case <-handlerDone:
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("consumer did not finish both messages")
	}
	if runErr != nil {
		t.Fatalf("consumer run: %v", runErr)
	}
	if applied != 2 {
		t.Fatalf("handler invocations = %d, want 2", applied)
	}

	// DLQ holds exactly the poison message with the failure header attached.
	dlqReader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: dlqTopic, Partition: 0, MaxWait: 200 * time.Millisecond})
	defer func() { _ = dlqReader.Close() }()
	if err := dlqReader.SetOffset(kafka.FirstOffset); err != nil {
		t.Fatal(err)
	}
	msg, err := dlqReader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read dlq: %v", err)
	}
	if string(msg.Value) != "not-protobuf-at-all" {
		t.Fatalf("dlq payload = %q", msg.Value)
	}
	foundErr := false
	for _, h := range msg.Headers {
		if h.Key == "dlq_error" {
			foundErr = true
		}
	}
	if !foundErr {
		t.Fatal("dlq message missing dlq_error header")
	}

	// Offsets committed past BOTH messages (poison quarantined, not skipped
	// silently): committed offset == next offset after message 2.
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
			Topics: map[string][]int{topic: {0}}, GroupID: "dlq-test-group",
		})
		if err == nil && len(resp.Topics) > 0 && len(resp.Topics[topic]) > 0 {
			if resp.Topics[topic][0].CommittedOffset >= 2 {
				break // both messages committed (poison via DLQ path)
			}
			if resp.Topics[topic][0].CommittedOffset < 0 {
				resp.Topics[topic][0].CommittedOffset = 0
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("committed offset never reached 2 (quarantine must commit): %+v", resp.Topics)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitForMessages blocks until the topic exposes n messages (producer ack is
// not visibility; readers need the broker to serve them).
func waitForMessages(t *testing.T, brokers []string, topic string, n int) {
	t.Helper()
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := client.ListOffsets(context.Background(), &kafka.ListOffsetsRequest{
			Topics: map[string][]kafka.OffsetRequest{topic: {kafka.LastOffsetOf(0)}},
		})
		if err == nil {
			if offs := resp.Topics[topic]; len(offs) > 0 && offs[0].LastOffset >= int64(n)-1 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("topic %s never reached %d messages", topic, n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Transient failures exhaust retries and fail the consumer without committing.
func TestConsumerTransientFailStop(t *testing.T) {
	brokers := testenv.StartKafka(t)
	ctx := context.Background()
	topic := "failstop-events"
	createTopic(t, brokers, topic)
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte("k"), Value: mustProtoEvent(t, "tp-fail-1")}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	consumer := NewConsumer(brokers, topic, "failstop-group", "")
	// Shrink the retry envelope so the test stays fast.
	consumer.retries = 1
	consumer.backoff = 10 * time.Millisecond
	defer func() { _ = consumer.Close() }()

	attempts := 0
	err := consumer.Run(ctx, func(context.Context, processor.IncomingMessage) error {
		attempts++
		return context.DeadlineExceeded // transient
	})
	if err == nil {
		t.Fatal("transient failure must fail the consumer (fail-stop, no commit)")
	}
	if attempts != 2 { // initial + 1 retry
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func createTopic(t *testing.T, brokers []string, name string) {
	t.Helper()
	conn, err := kafka.DialLeader(context.Background(), "tcp", brokers[0], name, 0)
	if err != nil {
		t.Fatalf("create topic %s: %v", name, err)
	}
	_ = conn.Close()
	// Topic creation is asynchronous on the broker; wait for it to appear in
	// metadata so the immediately following writes cannot race it.
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := client.Metadata(context.Background(), &kafka.MetadataRequest{Topics: []string{name}})
		if err == nil && len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("topic %s never became ready", name)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// mustProtoEvent serializes a well-formed canonical event (protobuf wire
// format, as the ingest producer would).
func mustProtoEvent(t *testing.T, transportID string) []byte {
	t.Helper()
	ev := &eventv1.AlertEvent{
		EventId:     transportID,
		TransportId: transportID,
		TenantId:    "dddd0000-0000-0000-0000-000000000001",
		Source: &eventv1.SourceRef{
			SourceId: "dddd0000-0000-0000-0000-000000000002", SourceType: eventv1.SourceType_SOURCE_TYPE_DEEPFLOW,
			ExternalRuleId: "r1",
		},
		Resource:  &eventv1.ResourceRef{ClusterId: "c1", ResourceType: "pod", ResourceUid: "u1"},
		Kind:      eventv1.EventKind_EVENT_KIND_STATEFUL,
		Signal:    eventv1.EventSignal_EVENT_SIGNAL_FIRING,
		Severity:  eventv1.Severity_SEVERITY_CRITICAL,
		AlertName: "DLQTest",
		EventKey:  "ek-" + transportID,
	}
	raw, err := proto.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
