package metricsquery

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeClient struct {
	series []Series
	expr   string
	err    error
}

func (f *fakeClient) QueryRange(_ context.Context, expr string, _, _ time.Time, _ time.Duration) ([]Series, error) {
	f.expr = expr
	return f.series, f.err
}

// P9-MC-01：白名单与窗口校验 —— 任意 PromQL 不可注入，越界参数被拒绝。
func TestQueryWhitelistAndBounds(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	client := &fakeClient{series: []Series{{Labels: map[string]string{"reason": "invalid"},
		Points: [][2]float64{{float64(now.Unix()), 1.5}}}}}
	svc := NewService(client)

	if len(Catalog()) == 0 {
		t.Fatal("catalog must not be empty")
	}

	t.Run("unknown metric", func(t *testing.T) {
		if _, err := svc.Query(ctx, "attacker_metric()", now.Add(-time.Hour), now, time.Minute); !errors.Is(err, ErrUnknownMetric) {
			t.Fatalf("err = %v, want ErrUnknownMetric", err)
		}
	})
	t.Run("end before start", func(t *testing.T) {
		if _, err := svc.Query(ctx, "consumer_lag", now, now.Add(-time.Minute), time.Minute); !errors.Is(err, ErrInvalidRange) {
			t.Fatalf("err = %v, want ErrInvalidRange", err)
		}
	})
	t.Run("range too wide", func(t *testing.T) {
		if _, err := svc.Query(ctx, "consumer_lag", now.Add(-48*time.Hour), now, time.Minute); !errors.Is(err, ErrInvalidRange) {
			t.Fatalf("err = %v, want ErrInvalidRange", err)
		}
	})
	t.Run("step too small", func(t *testing.T) {
		if _, err := svc.Query(ctx, "consumer_lag", now.Add(-time.Hour), now, time.Second); !errors.Is(err, ErrInvalidRange) {
			t.Fatalf("err = %v, want ErrInvalidRange", err)
		}
	})
	t.Run("too many points", func(t *testing.T) {
		if _, err := svc.Query(ctx, "consumer_lag", now.Add(-24*time.Hour), now, 15*time.Second); !errors.Is(err, ErrInvalidRange) {
			t.Fatalf("err = %v, want ErrInvalidRange", err)
		}
	})

	t.Run("catalog query uses server-side expression", func(t *testing.T) {
		res, err := svc.Query(ctx, "ingest_rejected_rate", now.Add(-time.Hour), now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if client.expr == "" || client.expr == "ingest_rejected_rate" {
			t.Fatalf("client received caller-controlled expression %q", client.expr)
		}
		if res.Metric != "ingest_rejected_rate" || len(res.Series) != 1 || res.StepSeconds != 60 {
			t.Fatalf("result = %+v", res)
		}
	})

	t.Run("client error propagates", func(t *testing.T) {
		bad := NewService(&fakeClient{err: errors.New("prometheus down")})
		if _, err := bad.Query(ctx, "consumer_lag", now.Add(-time.Hour), now, time.Minute); err == nil {
			t.Fatal("upstream error must propagate")
		}
	})
}
