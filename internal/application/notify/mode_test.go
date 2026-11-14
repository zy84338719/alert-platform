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

func (b *BaseStore) ClaimNotifyTasks(_ context.Context, limit int, _ time.Duration) ([]Task, error) {
	out := b.tasks
	b.tasks = nil
	if len(out) > limit {
		b.tasks = out[limit:]
		return out[:limit], nil
	}
	return out, nil
}

func (m *modeStore) ClaimNotifyTasks(ctx context.Context, limit int, lease time.Duration) ([]Task, error) {
	tasks, err := m.BaseStore.ClaimNotifyTasks(ctx, limit, lease)
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
