package notify

import (
	"context"
	"errors"
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
	ID        string
	Sent      bool
	Failure   string
	AttemptNo int
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
	st.tasks = []Task{{ID: "t1", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1}}
	svc.DeliverOnce(context.Background())
	if sender.sent != 1 || len(st.finished) != 1 || !st.finished[0].Sent {
		t.Fatalf("active mode: sender=%d finished=%+v", sender.sent, st.finished)
	}

	// SHADOW: recorded to the audit trail, tasks NOT claimed and NOT mutated —
	// the queue stays pending so a later switch to active delivers exactly once.
	svc, st, sender = newSvc(ModeShadow)
	st.tasks = []Task{{ID: "t2", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1}}
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
	st.tasks = []Task{{ID: "t3", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1}}
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

// BaseStore is a minimal in-memory notify.Store for the mode drill. It models
// the persisted authority ledger across Service instances: authMode/authGen
// survive a "process restart" (new Service on the same store), which is the
// P8-P0-01 scenario Kubernetes actually exercises.
type BaseStore struct {
	tasks    []Task
	authMode string
	authGen  int64
}

// SyncNotifyAuthority mirrors the real store: first initialization always
// opens a new generation (pre-authority tasks are never adopted); a mode
// change opens the next one.
func (b *BaseStore) SyncNotifyAuthority(_ context.Context, desiredMode string) (Authority, error) {
	if b.authGen == 0 {
		b.authGen = 1
		b.authMode = desiredMode
	} else if b.authMode != desiredMode {
		b.authGen++
		b.authMode = desiredMode
	}
	return Authority{Mode: b.authMode, Generation: b.authGen}, nil
}

func (b *BaseStore) ClaimNotifyTasks(_ context.Context, limit int, _ time.Duration, generation int64) ([]Task, error) {
	// Model the real store's generation scoping: older-era tasks are never
	// claimed (no history replay), only the current generation is deliverable.
	var out, rest []Task
	for _, t := range b.tasks {
		if t.AuthorityGeneration == generation && len(out) < limit {
			out = append(out, t)
			continue
		}
		rest = append(rest, t)
	}
	b.tasks = rest
	return out, nil
}

func (m *modeStore) ClaimNotifyTasks(ctx context.Context, limit int, lease time.Duration, generation int64) ([]Task, error) {
	tasks, err := m.BaseStore.ClaimNotifyTasks(ctx, limit, lease, generation)
	m.claimed = append(m.claimed, tasks...)
	return tasks, err
}

func (m *modeStore) PeekNotifyTasks(_ context.Context, limit int, generation int64) ([]Task, error) {
	var out []Task
	for _, t := range m.tasks {
		if t.AuthorityGeneration != generation {
			continue
		}
		if len(out) < limit {
			out = append(out, t)
		}
	}
	return out, nil
}

func (m *modeStore) RecordShadowDelivery(_ context.Context, taskID string) (bool, error) {
	m.shadowed = append(m.shadowed, taskID)
	return true, nil
}

func (m *modeStore) FinishNotifyTask(_ context.Context, id, _ string, sent bool, _ time.Duration, attemptNo int, _ int, failureCode, _ string) error {
	m.finished = append(m.finished, Finish{ID: id, Sent: sent, Failure: failureCode, AttemptNo: attemptNo})
	return nil
}

func (m *BaseStore) SuppressNotifyAtSend(context.Context, string, map[string]string) (bool, error) {
	return false, nil
}

// P8-P0-01 失败测试（同进程）：shadow→active 切换不得补发 shadow 历史任务 ——
// shadow 期间产生的 pending 任务属于旧 authority generation，所有权切换
// （新 generation 生效）后不被 claim；新 generation 的任务正常投递。
func TestShadowToActiveNoHistoryReplay(t *testing.T) {
	ctx := context.Background()
	st := &modeStore{}
	sender := &captureSender{}
	svc := NewService(st, sender)

	// ① shadow 期间：历史任务被记录、不被 claim。
	svc.SetMode(ModeShadow)
	st.tasks = []Task{{ID: "shadow-hist", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1}}
	svc.DeliverOnce(ctx) // 首次初始化 → generation 1 (shadow)
	if sender.sent != 0 || len(st.claimed) != 0 {
		t.Fatalf("shadow phase: sent=%d claimed=%d", sender.sent, len(st.claimed))
	}
	if st.authMode != ModeShadow || st.authGen != 1 {
		t.Fatalf("authority after init = %s/%d, want shadow/1", st.authMode, st.authGen)
	}

	// ② 切 active：持久化 authority 进入新 generation（2），历史任务留在 gen1。
	svc.SetMode(ModeActive)

	// ③ 新 generation 任务正常投递；shadow 历史任务不被补发。
	st.tasks = append(st.tasks,
		Task{ID: "shadow-hist-2", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1},
		Task{ID: "fresh-task", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 2},
	)
	svc.DeliverOnce(ctx)
	if st.authGen != 2 {
		t.Fatalf("generation after shadow→active = %d, want 2", st.authGen)
	}
	if sender.sent != 1 {
		t.Fatalf("sent = %d, want 1 (only the fresh task)", sender.sent)
	}
	if len(st.claimed) != 1 || st.claimed[0].ID != "fresh-task" {
		t.Fatalf("claimed = %v, want [fresh-task] — shadow history must not replay (notification storm)", st.claimed)
	}
}

// 从未进入 shadow 的部署：首次初始化（gen1）的任务照常投递（行为兼容），
// 但 authority ledger 之前（gen0）的任务永不被领取（保守：不冒险补发）。
func TestActiveWithoutShadowClaimsAll(t *testing.T) {
	ctx := context.Background()
	st := &modeStore{}
	sender := &captureSender{}
	svc := NewService(st, sender) // 默认 active，从未 shadow
	st.tasks = []Task{
		{ID: "old-backlog", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1},
		{ID: "new", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1},
		{ID: "pre-authority", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 0},
	}
	svc.DeliverOnce(ctx)
	if sender.sent != 2 || len(st.claimed) != 2 {
		t.Fatalf("sent=%d claimed=%d, want 2/2 (gen1 claimed, pre-authority gen0 never)", sender.sent, len(st.claimed))
	}
}

// P8-P0-01 核心验收：跨进程重启 ——
// Shadow 模式产生 1,000 条任务 → 停止 Worker → 配置切 Active → 启动新 Worker
// （新 Service 实例、同一持久化 store）→ 历史任务发送数为 0，新任务正常发送。
// P7 的内存 shadowCutoff 在真实 K8s 部署（改 env 后重启 Pod）下失效：新进程
// 默认 active、cutoff 零值 → 1000 条历史通知被补发；本测试锁定该回归。
func TestShadowAuthoritySurvivesRestart(t *testing.T) {
	ctx := context.Background()
	st := &modeStore{} // authority ledger 随 store 持久化（模拟 PostgreSQL）

	// 进程 1：shadow 模式产生 1000 条任务并记录观察。
	sender1 := &captureSender{}
	svc1 := NewService(st, sender1)
	svc1.SetMode(ModeShadow)
	for i := 0; i < 1000; i++ {
		st.tasks = append(st.tasks, Task{ID: "shadow-hist", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1})
	}
	svc1.DeliverOnce(ctx)
	if sender1.sent != 0 {
		t.Fatalf("shadow phase sent %d, want 0", sender1.sent)
	}

	// 进程 1 停止；运维改 ALERT_NOTIFY_MODE=active 并启动新 Pod。
	sender2 := &captureSender{}
	svc2 := NewService(st, sender2) // 构造函数默认 active —— 与真实重启一致
	svc2.SetMode(ModeActive)
	svc2.DeliverOnce(ctx)

	if sender2.sent != 0 || len(st.claimed) != 0 {
		t.Fatalf("RESTART REPLAY: sent=%d claimed=%d, want 0/0 — shadow history must never be replayed after a Pod restart",
			sender2.sent, len(st.claimed))
	}
	if st.authGen != 2 || st.authMode != ModeActive {
		t.Fatalf("authority = %s/%d, want active/2", st.authMode, st.authGen)
	}

	// 新 generation（2）的任务正常发送。
	st.tasks = append(st.tasks, Task{ID: "fresh-after-restart", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 2})
	svc2.DeliverOnce(ctx)
	if sender2.sent != 1 {
		t.Fatalf("fresh task after restart: sent=%d, want 1", sender2.sent)
	}
}

// silenceErrStore simulates the silence re-check hitting a DB error.
type silenceErrStore struct{ *modeStore }

func (s *silenceErrStore) SuppressNotifyAtSend(context.Context, string, map[string]string) (bool, error) {
	return false, errors.New("pg unavailable")
}

// P8-P1-03 失败测试：静默复核失败必须 fail-closed ——
// 修复前 DB 错误只记日志然后照常 Send（静默被绕过，通知在系统降级时外发）。
// 修复后：不发送、任务以 retryable 重新调度；是否允许紧急绕过必须是显式策略，
// 不能由数据库异常自动决定。
func TestSilenceCheckFailureDoesNotSend(t *testing.T) {
	ms := &modeStore{}
	st := &silenceErrStore{modeStore: ms}
	sender := &captureSender{}
	svc := NewService(st, sender) // 默认 active
	ms.tasks = []Task{{ID: "s1", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1}}

	svc.DeliverOnce(context.Background())
	if sender.sent != 0 {
		t.Fatalf("silence check failure sent %d notifications, want 0 (fail-closed)", sender.sent)
	}
	if len(ms.finished) != 1 {
		t.Fatalf("finished = %+v, want one retryable outcome", ms.finished)
	}
	f := ms.finished[0]
	if f.Sent || f.Failure != "retryable" || f.ID != "s1" {
		t.Fatalf("outcome = %+v, want not-sent retryable for s1", f)
	}
}
