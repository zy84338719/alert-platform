package worker

// P1-03 (rh-p1-03) 失败测试：后台任务故障传播 —— 关键任务退出/panic 必须
// fail-stop（整组随 ctx 取消、Run 返回错误、进程可被重启），非关键任务退出
// 不杀进程但必须让 Readiness 变不健康；正常取消是干净关停。

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCriticalTaskFailureStopsGroup(t *testing.T) {
	g := NewGroup()
	g.Go(Task{Name: "processor", Critical: true, Run: func(ctx context.Context) error {
		<-ctx.Done() // 主消费者：阻塞直到被 fail-stop 取消
		return ctx.Err()
	}})
	g.Go(Task{Name: "relay", Critical: true, Run: func(ctx context.Context) error {
		time.Sleep(20 * time.Millisecond)
		return errors.New("kafka writer closed unexpectedly")
	}})

	start := time.Now()
	err := g.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "relay") {
		t.Fatalf("Run err = %v, want the relay failure", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("fail-stop took %s — the worker kept running with a dead critical task", elapsed)
	}
}

func TestTaskPanicIsFailStop(t *testing.T) {
	g := NewGroup()
	g.Go(Task{Name: "notifier", Critical: true, Run: func(ctx context.Context) error {
		time.Sleep(20 * time.Millisecond)
		panic("nil map write in notifier loop")
	}})
	err := g.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "panic") || !strings.Contains(err.Error(), "notifier") {
		t.Fatalf("Run err = %v, want the notifier panic surfaced", err)
	}
}

func TestNonCriticalFailureDegradesReadiness(t *testing.T) {
	g := NewGroup()
	g.Go(Task{Name: "processor", Critical: true, Run: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}})
	g.Go(Task{Name: "retention", Critical: false, Run: func(ctx context.Context) error {
		time.Sleep(20 * time.Millisecond)
		return errors.New("retention sweep failed")
	}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	// 进程不退（非关键任务失败），但 Readiness 必须报异常。
	if err := g.ReadinessCheck()(ctx); err == nil || !strings.Contains(err.Error(), "retention") {
		t.Fatalf("ReadinessCheck err = %v, want the retention failure surfaced", err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run after cancel = %v, want nil (clean shutdown)", err)
	}
}

func TestCleanShutdownRecordsNoFailure(t *testing.T) {
	g := NewGroup()
	g.Go(Task{Name: "processor", Critical: true, Run: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if err := g.ReadinessCheck()(context.Background()); err != nil {
		t.Fatalf("ReadinessCheck after clean shutdown = %v, want nil", err)
	}
}
