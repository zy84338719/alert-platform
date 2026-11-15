package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/oekit/logx"
	"github.com/segmentio/kafka-go"

	"github.com/example/alert-platform/internal/application/processor"
)

// Consumer wraps a consumer-group reader with the docs/04 §2 offset rule:
// the business side (PG transaction) succeeds first, the offset commits
// after. Deduplication happens in the store (receipts), so redelivery after
// a rebalance or crash is safe by construction.
//
// Failure policy (PROC-001):
//   - permanent errors (processor.IsPermanent): publish the original message
//     to the DLQ topic, then commit — poison messages never wedge the group;
//   - transient errors: bounded in-process retries with backoff, then the
//     consumer RETURNS (fail-stop). The offset stays uncommitted, K8s
//     restarts the pod and redelivery resumes — no unbounded uncommitted lag.
type Consumer struct {
	reader  *kafka.Reader
	dlq     *kafka.Writer
	retries int
	backoff time.Duration
}

// NewConsumer builds a consumer-group reader for one topic. dlqTopic may be
// empty to disable quarantine (tests); retries defaults to 3, backoff 500ms.
func NewConsumer(brokers []string, topic, groupID, dlqTopic string) *Consumer {
	c := &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     groupID, // group mode: FetchMessage does not auto-commit
			MinBytes:    1,
			MaxBytes:    10e6,
			MaxWait:     500 * time.Millisecond,
			StartOffset: kafka.FirstOffset, // replay from the beginning on a fresh group (PROC-008)
		}),
		retries: 3,
		backoff: 500 * time.Millisecond,
	}
	if dlqTopic != "" {
		c.dlq = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        dlqTopic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 10 * time.Millisecond,
		}
	}
	return c
}

// Run consumes until ctx is done.
func (c *Consumer) Run(ctx context.Context, handle func(context.Context, processor.IncomingMessage) error) error {
	logx.CtxInfo(ctx, "consumer loop started",
		"topic", c.reader.Config().Topic, "group", c.reader.Config().GroupID)
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			return fmt.Errorf("fetch: %w", err)
		}
		hdrs := make(map[string]string, len(msg.Headers))
		for _, h := range msg.Headers {
			hdrs[h.Key] = string(h.Value)
		}
		in := processor.IncomingMessage{
			Key: string(msg.Key), Headers: hdrs, Value: msg.Value,
			Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset,
		}

		if err := c.handleWithRetry(ctx, in, handle); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Transient failure exhausted retries: fail-stop WITHOUT
			// committing — redelivery after restart preserves at-least-once.
			return fmt.Errorf("handle topic=%s partition=%d offset=%d (uncommitted, will replay): %w",
				msg.Topic, msg.Partition, msg.Offset, err)
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Business side committed, offset did not: replay is expected and
			// absorbed by receipts (T-007). Keep consuming.
			logx.CtxError(ctx, "offset commit failed after applied event; replay will be deduplicated",
				logx.ErrKey, err, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
		}
	}
}

// handleWithRetry applies the bounded retry policy for transient failures and
// quarantines permanent failures into the DLQ (returning nil so the offset
// commits).
func (c *Consumer) handleWithRetry(ctx context.Context, in processor.IncomingMessage, handle func(context.Context, processor.IncomingMessage) error) error {
	var lastErr error
	for attempt := 0; ; attempt++ {
		err := handle(ctx, in)
		if err == nil {
			return nil
		}
		lastErr = err
		if processor.IsPermanent(err) {
			return c.quarantine(ctx, in, err)
		}
		if attempt >= c.retries {
			// Exhausted: fail-stop immediately — sleeping one more backoff
			// window would only delay the restart that resumes redelivery.
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.backoff << uint(attempt)):
		}
	}
	return lastErr
}

// quarantine publishes the original message to the DLQ with the failure
// reason in the headers. If the DLQ publish itself fails, the error is
// returned transient — the message is retried, never dropped.
func (c *Consumer) quarantine(ctx context.Context, in processor.IncomingMessage, cause error) error {
	logx.CtxError(ctx, "quarantining poison message to DLQ", logx.ErrKey, cause, "key", in.Key)
	if c.dlq == nil {
		return fmt.Errorf("permanent failure and no DLQ configured: %w", cause)
	}
	headers := make([]kafka.Header, 0, len(in.Headers)+1)
	for k, v := range in.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	headers = append(headers, kafka.Header{Key: "dlq_error", Value: []byte(cause.Error())})
	// Forensics: the original coordinates are NOT recoverable from the DLQ
	// topic itself, so they ride along as headers.
	headers = append(headers,
		kafka.Header{Key: "original_topic", Value: []byte(in.Topic)},
		kafka.Header{Key: "original_partition", Value: []byte(strconv.Itoa(in.Partition))},
		kafka.Header{Key: "original_offset", Value: []byte(strconv.FormatInt(in.Offset, 10))},
	)
	if err := c.dlq.WriteMessages(ctx, kafka.Message{
		Key:     []byte(in.Key),
		Headers: headers,
		Value:   in.Value,
	}); err != nil {
		return fmt.Errorf("dlq publish: %w", err)
	}
	return nil // quarantined: caller commits the offset
}

// Close releases the reader and the DLQ writer.
func (c *Consumer) Close() error {
	if c.dlq != nil {
		if err := c.dlq.Close(); err != nil {
			return err
		}
	}
	return c.reader.Close()
}
