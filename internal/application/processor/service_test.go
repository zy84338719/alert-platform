package processor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	domainalert "github.com/example/alert-platform/internal/domain/alert"
)

type fakeStore struct{ err error }

func (f *fakeStore) ApplyEvent(context.Context, *eventv1.AlertEvent) (bool, error) {
	return false, f.err
}

func validWireEvent(t *testing.T) []byte {
	t.Helper()
	b, err := proto.Marshal(&eventv1.AlertEvent{
		TransportId: "p-poison-1",
		EventId:     "e-1",
		TenantId:    "tenant-1",
		Source:      &eventv1.SourceRef{SourceId: "src-1"},
		Signal:      eventv1.EventSignal_EVENT_SIGNAL_FIRING,
		Severity:    eventv1.Severity_SEVERITY_CRITICAL,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// R20 失败测试：契约违例必须归类为 Permanent（DLQ 隔离 + commit），
// 依赖故障保持 transient（offset 不提交、重投）。
// 修复前只有 ErrIncompleteIdentity 是 Permanent；缺 signal 等校验错误
// 以普通 error 冒泡 → consumer 不 DLQ → processor 关键任务 fail-stop，
// 重启后同一条消息再次崩溃（crash loop，单条毒消息打死整个 worker）。
func TestHandlePermanentClassification(t *testing.T) {
	wire := validWireEvent(t)
	handle := func(storeErr error) error {
		return NewService(&fakeStore{err: storeErr}).Handle(context.Background(), IncomingMessage{
			Value:   wire,
			Headers: map[string]string{"trace_id": "trace-r20"},
		})
	}

	t.Run("contract violation is permanent", func(t *testing.T) {
		err := handle(fmt.Errorf("%w: missing signal", domainalert.ErrInvalidEvent))
		if !IsPermanent(err) {
			t.Fatalf("err = %v, want Permanent (DLQ quarantine)", err)
		}
		if !errors.Is(err, domainalert.ErrInvalidEvent) {
			t.Fatalf("wrapped sentinel lost: %v", err)
		}
	})

	t.Run("incomplete identity is permanent", func(t *testing.T) {
		err := handle(fmt.Errorf("apply: %w", domainalert.ErrIncompleteIdentity))
		if !IsPermanent(err) {
			t.Fatalf("err = %v, want Permanent", err)
		}
	})

	t.Run("infrastructure error stays transient", func(t *testing.T) {
		err := handle(errors.New("pg: connection refused"))
		if err == nil {
			t.Fatal("infrastructure error must surface")
		}
		if IsPermanent(err) {
			t.Fatalf("infrastructure error classified Permanent (would DLQ a replayable event): %v", err)
		}
	})

	t.Run("decode failure is permanent", func(t *testing.T) {
		err := NewService(&fakeStore{}).Handle(context.Background(), IncomingMessage{Value: []byte{0xff, 0x00, 0xff}})
		if !IsPermanent(err) {
			t.Fatalf("decode failure = %v, want Permanent", err)
		}
	})
}

// P9-OBS-03：处理结果与事务耗时都有指标（applied/permanent/transient）。
func TestHandleMetricsOutcomes(t *testing.T) {
	wire := validWireEvent(t)
	ctx := context.Background()
	outcome := func(name string) float64 { return testutil.ToFloat64(eventsTotal.WithLabelValues(name)) }
	handle := func(storeErr error) {
		t.Helper()
		_ = NewService(&fakeStore{err: storeErr}).Handle(ctx, IncomingMessage{Value: wire})
	}

	before := outcome("applied")
	handle(nil)
	if got := outcome("applied") - before; got != 1 {
		t.Fatalf("applied delta = %v, want 1", got)
	}
	before = outcome("permanent")
	handle(fmt.Errorf("%w: missing signal", domainalert.ErrInvalidEvent))
	if got := outcome("permanent") - before; got != 1 {
		t.Fatalf("permanent delta = %v, want 1", got)
	}
	before = outcome("transient")
	handle(errors.New("pg down"))
	if got := outcome("transient") - before; got != 1 {
		t.Fatalf("transient delta = %v, want 1", got)
	}

	var hist dto.Metric
	if err := durationSeconds.Write(&hist); err != nil {
		t.Fatal(err)
	}
	if hist.GetHistogram().GetSampleCount() == 0 {
		t.Fatal("processor duration histogram recorded no observations")
	}
}

// realFingerprintStore exercises the REAL domain fingerprint (no injected
// sentinel) — the regression that AUD-003 found: the processor's Permanent
// classification relied on errors.Is, which a fresh errors.New broke.
type realFingerprintStore struct{}

func (realFingerprintStore) ApplyEvent(_ context.Context, ev *eventv1.AlertEvent) (bool, error) {
	_, err := domainalert.Fingerprint(domainalert.Identity{
		TenantID: ev.GetTenantId(), SourceID: ev.GetSource().GetSourceId(),
		RuleID: ev.GetSource().GetExternalRuleId(), ClusterID: ev.GetResource().GetClusterId(),
		EntityUID: ev.GetResource().GetResourceUid(),
	})
	return false, err
}

// AUD-003（#12）：身份不完整消息必须经真实 Fingerprint 路径被判 Permanent
// （进 DLQ），而不是 transient crash-loop。
func TestHandleIncompleteIdentityViaRealFingerprint(t *testing.T) {
	wire, err := proto.Marshal(&eventv1.AlertEvent{
		TransportId: "aud003-1", EventId: "e1", TenantId: "t1",
		Source:   &eventv1.SourceRef{SourceId: "s1"},    // no external_rule_id
		Resource: &eventv1.ResourceRef{ClusterId: "c1"}, // no resource_uid
		Signal:   eventv1.EventSignal_EVENT_SIGNAL_FIRING,
		Severity: eventv1.Severity_SEVERITY_CRITICAL,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = NewService(realFingerprintStore{}).Handle(context.Background(), IncomingMessage{Value: wire})
	if !IsPermanent(err) {
		t.Fatalf("err = %v, want Permanent (DLQ quarantine)", err)
	}
	if !errors.Is(err, domainalert.ErrIncompleteIdentity) {
		t.Fatalf("sentinel lost: %v", err)
	}
}
