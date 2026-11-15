package kafka

// P1-01 (rh-p1-01) 失败测试①：分区键从 tenant|source 改为 tenant+fingerprint
// —— 同一逻辑告警保序（同 key 同分区），不同逻辑告警在多分区 topic 上扩散，
// 大来源不再把 Consumer Group 扩容钉死在单一热点分区。

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/segmentio/kafka-go"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/application/ingest"
	"github.com/example/alert-platform/internal/application/processor"
	"github.com/example/alert-platform/internal/infrastructure/postgres"
	"github.com/example/alert-platform/internal/infrastructure/testenv"
)

func ptEvent(rule, uid string) *eventv1.AlertEvent {
	return &eventv1.AlertEvent{
		EventId:     "evt-" + rule + "-" + uid,
		TransportId: "tp-" + rule + "-" + uid,
		TenantId:    "22222222-2222-2222-2222-222222222222",
		Source: &eventv1.SourceRef{
			SourceId:       "cccc0000-0000-0000-0000-000000000003",
			SourceType:     eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER,
			ExternalRuleId: rule,
		},
		Resource: &eventv1.ResourceRef{
			ClusterId: "c1", ResourceType: "pod", ResourceUid: uid,
		},
		Kind:       eventv1.EventKind_EVENT_KIND_STATEFUL,
		Signal:     eventv1.EventSignal_EVENT_SIGNAL_FIRING,
		Severity:   eventv1.Severity_SEVERITY_CRITICAL,
		AlertName:  rule,
		OccurredAt: timestamppb.New(time.Now()),
		ReceivedAt: timestamppb.New(time.Now()),
		EventKey:   "ek-" + rule + "-" + uid,
	}
}

func TestPartitionKeySpreadsLogicalAlerts(t *testing.T) {
	brokers := testenv.StartKafka(t)
	ctx := context.Background()

	topic := fmt.Sprintf("it_pkey_%d", time.Now().UnixNano())
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	if _, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 16, ReplicationFactor: 1}},
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := NewProducer(brokers, topic)
	defer func() { _ = producer.Close() }()
	ingestSvc := ingest.NewService(producer)

	// 同一来源的 20 个不同逻辑告警（不同指纹）+ 同一逻辑告警的 3 次观察。
	var events []*eventv1.AlertEvent
	for i := 0; i < 20; i++ {
		events = append(events, ptEvent(fmt.Sprintf("rule-%02d", i), fmt.Sprintf("pod-%02d", i)))
	}
	if _, err := ingestSvc.PushBatch(ctx, "pkey-batch", events); err != nil {
		t.Fatalf("PushBatch: %v", err)
	}
	for i := 0; i < 3; i++ {
		repeat := ptEvent("rule-00", "pod-00")
		repeat.TransportId = fmt.Sprintf("tp-repeat-%d", i)
		repeat.EventId = repeat.TransportId
		if _, err := ingestSvc.PushBatch(ctx, "pkey-repeat-"+fmt.Sprint(i), []*eventv1.AlertEvent{repeat}); err != nil {
			t.Fatalf("PushBatch repeat %d: %v", i, err)
		}
	}

	// 消费全部 23 条，按 transport_id 记录分区。
	partOf := map[string]int32{}
	deadline := time.Now().Add(40 * time.Second)
	for len(partOf) < 23 && time.Now().Before(deadline) {
		for p := 0; p < 16; p++ {
			consumer := kafka.NewReader(kafka.ReaderConfig{
				Brokers: brokers, Topic: topic, Partition: p,
				MinBytes: 1, MaxBytes: 10e6,
				StartOffset: kafka.FirstOffset,
			})
			for {
				readCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
				msg, err := consumer.ReadMessage(readCtx)
				cancel()
				if err != nil {
					break
				}
				for _, h := range msg.Headers {
					if h.Key == "transport_id" {
						partOf[string(h.Value)] = int32(p)
					}
				}
			}
			_ = consumer.Close()
		}
	}
	if len(partOf) < 23 {
		t.Fatalf("consumed %d of 23 messages", len(partOf))
	}

	// ① 扩散：20 个不同逻辑告警不得全部落在同一分区（旧 key 全部同分区）。
	used := map[int32]bool{}
	for i := 0; i < 20; i++ {
		used[partOf[fmt.Sprintf("tp-rule-%02d-pod-%02d", i, i)]] = true
	}
	if len(used) < 2 {
		t.Fatalf("20 logical alerts landed on %d partition(s) — per-source key pins the whole source to one hotspot partition (P1-01)", len(used))
	}

	// ② 保序：同一逻辑告警（rule-00 的 4 条观察）必须同分区同 key。
	base := partOf["tp-rule-00-pod-00"]
	for i := 0; i < 3; i++ {
		if got := partOf[fmt.Sprintf("tp-repeat-%d", i)]; got != base {
			t.Fatalf("repeat observation %d on partition %d, want %d — same logical alert must stay ordered", i, got, base)
		}
	}
}

// P1-01 (rh-p1-01) 语义测试②：批量写入是至少一次而非原子 —— 部分成功（部分
// 分区已 ack）后错误路径整批重试不丢不重，消费端 receipts 幂等吸收。
// 实现注记：kafka-go 对超 broker message.max.bytes 的消息在客户端预校验、
// 整批拒绝（错误码 10），size 类违规观测不到部分成功；部分成功由端口级
// 故障注入确定性构造（模拟其它分区 ack、本分区失败）。
type partialPub struct {
	next  *Producer
	calls int
}

func (p *partialPub) PublishEvents(ctx context.Context, msgs []ingest.BatchMessage) error {
	p.calls++
	if p.calls <= 3 {
		// 模拟部分成功：第一条（其分区）已持久化，其余分区失败。
		if err := p.next.PublishEvents(ctx, msgs[:1]); err != nil {
			return err
		}
		return fmt.Errorf("kafka: simulated partial failure on the remaining partitions (call %d)", p.calls)
	}
	return p.next.PublishEvents(ctx, msgs)
}

func (p *partialPub) PublishEvent(ctx context.Context, key string, headers [][2]string, value []byte) error {
	return p.next.PublishEvent(ctx, key, headers, value)
}

func TestBatchPartialSuccessRetryIdempotent(t *testing.T) {
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

	topic := fmt.Sprintf("it_pbatch_%d", time.Now().UnixNano())
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	if _, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 4, ReplicationFactor: 1}},
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := NewProducer(brokers, topic)
	defer func() { _ = producer.Close() }()
	svc := ingest.NewService(&partialPub{next: producer})

	small := ptEvent("rule-ok", "pod-ok")
	other := ptEvent("rule-other", "pod-other")
	// 同一批两次 PushBatch 重试（相同 batch_id → 相同 transport_id）：
	// 每次"部分成功"都把 small 持久化一次，然后报错。
	for i := 0; i < 3; i++ {
		if _, err := svc.PushBatch(ctx, "pb-batch", []*eventv1.AlertEvent{small, other}); err == nil {
			if i < 3 {
				t.Fatalf("attempt %d must fail (simulated partial failure)", i+1)
			}
		} else if i == 2 {
			// 第 3 次之后注入器放行 → 整批成功
			if _, err := svc.PushBatch(ctx, "pb-batch", []*eventv1.AlertEvent{small, other}); err != nil {
				t.Fatalf("final attempt: %v", err)
			}
			break
		}
	}

	// 消费端处理全部已投递消息（small 至少 4 次 on the wire）。
	procSvc := processor.NewService(postgres.NewStore(pool))
	consumed := 0
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := fetchOneTimeout(t, brokers, topic, 2*time.Second)
		if err != nil {
			break
		}
		if err := procSvc.Handle(ctx, msg); err != nil {
			t.Fatalf("processor handle: %v", err)
		}
		consumed++
	}

	// 不丢不重：small 经历 4 次 on the wire，receipts 恰好 1；每个事件各一个
	// instance（无重复效果）；消费条数 = 消息数（无静默丢弃）。
	var smallReceipts, otherReceipts, instances int
	tenantID := small.GetTenantId()
	sourceID := small.GetSource().GetSourceId()
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_receipts WHERE tenant_id=$1 AND source_id=$2 AND transport_id=$3`,
		tenantID, sourceID, small.GetTransportId()).Scan(&smallReceipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_receipts WHERE tenant_id=$1 AND source_id=$2 AND transport_id=$3`,
		tenantID, sourceID, other.GetTransportId()).Scan(&otherReceipts); err != nil {
		t.Fatal(err)
	}
	if smallReceipts != 1 || otherReceipts != 1 {
		t.Fatalf("receipts small=%d other=%d, want 1/1 (partial-success retry dedup, consumed=%d)", smallReceipts, otherReceipts, consumed)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM alert_instances WHERE tenant_id=$1`, tenantID).Scan(&instances); err != nil || instances != 2 {
		t.Fatalf("instances = %d err=%v, want 2 (each event applied exactly once)", instances, err)
	}
}

func fetchOneTimeout(t *testing.T, brokers []string, topic string, timeout time.Duration) (processor.IncomingMessage, error) {
	t.Helper()
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: "pbatch-consumer",
		MinBytes: 1, MaxBytes: 10e6, MaxWait: 200 * time.Millisecond,
	})
	defer func() { _ = consumer.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	msg, err := consumer.ReadMessage(ctx)
	if err != nil {
		return processor.IncomingMessage{}, err
	}
	hdrs := map[string]string{}
	for _, h := range msg.Headers {
		hdrs[h.Key] = string(h.Value)
	}
	return processor.IncomingMessage{Key: string(msg.Key), Headers: hdrs, Value: msg.Value}, nil
}
