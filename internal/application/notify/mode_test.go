package notify

import (
	"context"
	"testing"
	"time"
)

type modeStore struct {
	BaseStore
	claimed  []Task
	finished []Finish
	shadowed []string
}

type Finish struct {
	ID         string
	Sent       bool
	Failure    string
	AttemptNo  int
}

// mode drill (REL-001): active delivers; shadow records suppressed attempts
// (no send, auditable); off leaves tasks untouched.
func TestNotifyModes(t *testing.T) {
	newSvc := func(mode string) (*Service, *modeStore, *captureSender) {
		st := &modeStore{}
		sender := &captureSender{}
		svc := NewService(st, sender)
		svc.SetMode(mode)
		return svc, st, sender
	}

	// ACTIVE: delivered.
	svc, st, sender := newSvc(ModeActive)
	st.tasks = []Task{{ID: "t1", Attempts: 1, Payload: []byte(`{}`)}}
	svc.DeliverOnce(context.Background())
	if sender.sent != 1 || len(st.finished) != 1 || !st.finished[0].Sent {
		t.Fatalf("active mode: sender=%d finished=%+v", sender.sent, st.finished)
	}

	// SHADOW: recorded to the audit trail, tasks NOT claimed and NOT mutated —
	// the queue stays pending so a later switch to active delivers exactly once.
	svc, st, sender = newSvc(ModeShadow)
	st.tasks = []Task{{ID: "t2", Attempts: 1, Payload: []byte(`{}`)}}
	svc.DeliverOnce(context.Background())
	if sender.sent != 0 {
		t.Fatalf("shadow mode sent %d notifications (double-send risk!)", sender.sent)
	}
	if len(st.finished) != 0 {
		t.Fatalf("shadow mode mutated delivery state: %+v", st.finished)
	}
	if len(st.shadowed) != 1 || st.shadowed[0] != "t2" {
		t.Fatalf("shadow audit records = %v, want [t2]", st.shadowed)
	}
	if len(st.claimed) != 0 {
		t.Fatalf("shadow mode claimed tasks: %v", st.claimed)
	}

	// OFF: nothing claimed, nothing sent.
	svc, st, sender = newSvc(ModeOff)
	st.tasks = []Task{{ID: "t3", Attempts: 1, Payload: []byte(`{}`)}}
	svc.DeliverOnce(context.Background())
	if sender.sent != 0 || len(st.claimed) != 0 || len(st.finished) != 0 {
		t.Fatalf("off mode: claimed=%d finished=%d", len(st.claimed), len(st.finished))
	}
	_ = time.Now
}

// captureSender counts sends.
type captureSender struct{ sent int }

func (c *captureSender) Send(context.Context, string, []byte, []byte) SendResult {
	c.sent++
	return SendResult{Sent: true, HTTPStatus: 200}
}

// BaseStore is a minimal in-memory notify.Store for the mode drill.
type BaseStore struct {
	tasks []Task
}

func (b *BaseStore) ClaimNotifyTasks(_ context.Context, limit int, _ time.Duration, createdAfter time.Time) ([]Task, error) {
	// Model the real store's P0-07 cutoff: shadow-era tasks (enqueued before
	// the ownership moment) are never claimed.
	out := make([]Task, 0, len(b.tasks))
	for _, t := range b.tasks {
		if !createdAfter.IsZero() && !t.CreatedAt.IsZero() && !t.CreatedAt.After(createdAfter) {
			continue
		}
		out = append(out, t)
	}
	b.tasks = out[len(out):]
	if len(out) > limit {
		b.tasks = out[limit:]
		return out[:limit], nil
	}
	return out, nil
}

func (m *modeStore) ClaimNotifyTasks(ctx context.Context, limit int, lease time.Duration, createdAfter time.Time) ([]Task, error) {
	tasks, err := m.BaseStore.ClaimNotifyTasks(ctx, limit, lease, createdAfter)
	m.claimed = append(m.claimed, tasks...)
	return tasks, err
}

func (m *modeStore) PeekNotifyTasks(_ context.Context, limit int) ([]Task, error) {
	out := m.tasks
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *modeStore) RecordShadowDelivery(_ context.Context, taskID string) (bool, error) {
	m.shadowed = append(m.shadowed, taskID)
	return true, nil
}

func (m *modeStore) FinishNotifyTask(_ context.Context, id string, sent bool, _ time.Duration, attemptNo int, _ int, failureCode, _ string) error {
	m.finished = append(m.finished, Finish{ID: id, Sent: sent, Failure: failureCode, AttemptNo: attemptNo})
	return nil
}

func (m *BaseStore) SuppressNotifyAtSend(context.Context, string, map[string]string) (bool, error) {
	return false, nil
}

// P0-07 (rh-p0-07) 失败测试：shadow→active 切换不得补发 shadow 历史任务 ——
// shadow 期间产生的 pending 任务（历史观察任务）在新所有权生效后不被 claim，
// 新任务正常投递；从未进入 shadow 的部署（cutoff 零值）保持全量 claim 兼容。
func TestShadowToActiveNoHistoryReplay(t *testing.T) {
	ctx := context.Background()
	st := &modeStore{}
	sender := &captureSender{}
	svc := NewService(st, sender)

	// ① shadow 期间：历史任务被记录、不被 claim。
	svc.SetMode(ModeShadow)
	shadowEra := time.Now().Add(-time.Second)
	st.tasks = []Task{{ID: "shadow-hist", Attempts: 1, Payload: []byte(`{}`), CreatedAt: shadowEra}}
	svc.DeliverOnce(ctx)
	if sender.sent != 0 || len(st.claimed) != 0 {
		t.Fatalf("shadow phase: sent=%d claimed=%d", sender.sent, len(st.claimed))
	}

	// ② 切 active：所有权生效时间 = 切换时刻。
	svc.SetMode(ModeActive)

	// ③ 新任务正常投递；shadow 历史任务不被补发。
	st.tasks = []Task{
		{ID: "shadow-hist", Attempts: 1, Payload: []byte(`{}`), CreatedAt: shadowEra},
		{ID: "fresh-task", Attempts: 1, Payload: []byte(`{}`), CreatedAt: time.Now()},
	}
	svc.DeliverOnce(ctx)
	if sender.sent != 1 {
		t.Fatalf("sent = %d, want 1 (only the fresh task)", sender.sent)
	}
	if len(st.claimed) != 1 || st.claimed[0].ID != "fresh-task" {
		t.Fatalf("claimed = %v, want [fresh-task] — shadow history must not replay (notification storm)", st.claimed)
	}
}

// 从未进入 shadow 的部署：cutoff 零值 → 全量 claim（行为兼容）。
func TestActiveWithoutShadowClaimsAll(t *testing.T) {
	ctx := context.Background()
	st := &modeStore{}
	sender := &captureSender{}
	svc := NewService(st, sender) // 默认 active，从未 shadow
	st.tasks = []Task{
		{ID: "old-backlog", Attempts: 1, Payload: []byte(`{}`), CreatedAt: time.Now().Add(-time.Hour)},
		{ID: "new", Attempts: 1, Payload: []byte(`{}`), CreatedAt: time.Now()},
	}
	svc.DeliverOnce(ctx)
	if sender.sent != 2 || len(st.claimed) != 2 {
		t.Fatalf("sent=%d claimed=%d, want 2/2 (no shadow cutoff engaged)", sender.sent, len(st.claimed))
	}
}
