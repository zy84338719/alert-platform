package notify

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

type modeStore struct {
	BaseStore
	mu       sync.Mutex
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
	if sender.count() != 1 || len(st.finished) != 1 || !st.finished[0].Sent {
		t.Fatalf("active mode: sender=%d finished=%+v", sender.count(), st.finished)
	}

	// SHADOW: recorded to the audit trail, tasks NOT claimed and NOT mutated —
	// the queue stays pending so a later switch to active delivers exactly once.
	svc, st, sender = newSvc(ModeShadow)
	st.tasks = []Task{{ID: "t2", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1}}
	svc.DeliverOnce(context.Background())
	if sender.count() != 0 {
		t.Fatalf("shadow mode sent %d notifications (double-send risk!)", sender.count())
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
	if sender.count() != 0 || len(st.claimed) != 0 || len(st.finished) != 0 {
		t.Fatalf("off mode: claimed=%d finished=%d", len(st.claimed), len(st.finished))
	}
	_ = time.Now
}

// captureSender counts sends (delivery is concurrent since P8-P1-04).
type captureSender struct {
	mu   sync.Mutex
	sent int
}

func (c *captureSender) Send(context.Context, string, []byte, []byte) SendResult {
	c.mu.Lock()
	c.sent++
	c.mu.Unlock()
	return SendResult{Sent: true, HTTPStatus: 200}
}

func (c *captureSender) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sent
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
	m.mu.Lock()
	m.claimed = append(m.claimed, tasks...)
	m.mu.Unlock()
	return tasks, err
}

func (b *BaseStore) RenewNotifyLease(context.Context, string, string, time.Duration) error {
	return nil
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
	m.mu.Lock()
	m.shadowed = append(m.shadowed, taskID)
	m.mu.Unlock()
	return true, nil
}

func (m *modeStore) FinishNotifyTask(_ context.Context, id, _ string, sent bool, _ time.Duration, attemptNo int, _ int, failureCode, _ string) error {
	m.mu.Lock()
	m.finished = append(m.finished, Finish{ID: id, Sent: sent, Failure: failureCode, AttemptNo: attemptNo})
	m.mu.Unlock()
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
	if sender.count() != 0 || len(st.claimed) != 0 {
		t.Fatalf("shadow phase: sent=%d claimed=%d", sender.count(), len(st.claimed))
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
	if sender.count() != 1 {
		t.Fatalf("sent = %d, want 1 (only the fresh task)", sender.count())
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
	if sender.count() != 2 || len(st.claimed) != 2 {
		t.Fatalf("sent=%d claimed=%d, want 2/2 (gen1 claimed, pre-authority gen0 never)", sender.count(), len(st.claimed))
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
	if sender1.count() != 0 {
		t.Fatalf("shadow phase sent %d, want 0", sender1.count())
	}

	// 进程 1 停止；运维改 ALERT_NOTIFY_MODE=active 并启动新 Pod。
	sender2 := &captureSender{}
	svc2 := NewService(st, sender2) // 构造函数默认 active —— 与真实重启一致
	svc2.SetMode(ModeActive)
	svc2.DeliverOnce(ctx)

	if sender2.count() != 0 || len(st.claimed) != 0 {
		t.Fatalf("RESTART REPLAY: sent=%d claimed=%d, want 0/0 — shadow history must never be replayed after a Pod restart",
			sender2.count(), len(st.claimed))
	}
	if st.authGen != 2 || st.authMode != ModeActive {
		t.Fatalf("authority = %s/%d, want active/2", st.authMode, st.authGen)
	}

	// 新 generation（2）的任务正常发送。
	st.tasks = append(st.tasks, Task{ID: "fresh-after-restart", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 2})
	svc2.DeliverOnce(ctx)
	if sender2.count() != 1 {
		t.Fatalf("fresh task after restart: sent=%d, want 1", sender2.count())
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
	if sender.count() != 0 {
		t.Fatalf("silence check failure sent %d notifications, want 0 (fail-closed)", sender.count())
	}
	if len(ms.finished) != 1 {
		t.Fatalf("finished = %+v, want one retryable outcome", ms.finished)
	}
	f := ms.finished[0]
	if f.Sent || f.Failure != "retryable" || f.ID != "s1" {
		t.Fatalf("outcome = %+v, want not-sent retryable for s1", f)
	}
}

// staleRenewStore reports that another worker owns the task at renewal time.
type staleRenewStore struct{ *modeStore }

func (s *staleRenewStore) RenewNotifyLease(context.Context, string, string, time.Duration) error {
	return ErrStaleNotifyLease
}

// renewCountingStore counts renewal calls (P8-P1-04).
type renewCountingStore struct {
	*modeStore
	mu     sync.Mutex
	renews int
}

func (s *renewCountingStore) RenewNotifyLease(context.Context, string, string, time.Duration) error {
	s.mu.Lock()
	s.renews++
	s.mu.Unlock()
	return nil
}

func (s *renewCountingStore) renewsCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.renews
}

// P8-P1-04 失败测试：发送前必须续租 ——
// ① 每个任务在 Send 之前续租（旧批次串行发送可超出 60s lease，被其他 Worker
//
//	重领后重复外发）；② 续租被 fencing（任务已被重领）时绝不发送。
func TestLeaseRenewalBeforeSend(t *testing.T) {
	ctx := context.Background()
	ms := &modeStore{}
	rc := &renewCountingStore{modeStore: ms}
	sender := &captureSender{}
	svc := NewService(rc, sender)
	ms.tasks = []Task{
		{ID: "r1", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1},
		{ID: "r2", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1},
	}
	svc.DeliverOnce(ctx)
	if sender.count() != 2 || rc.renewsCount() != 2 {
		t.Fatalf("sent=%d renews=%d, want 2/2 (renew per task before send)", sender.count(), rc.renewsCount())
	}

	// ② 续租已被 fencing：不得发送，也不得覆盖状态。
	ms2 := &modeStore{}
	stale := &staleRenewStore{modeStore: ms2}
	sender2 := &captureSender{}
	svc2 := NewService(stale, sender2)
	ms2.tasks = []Task{{ID: "dup", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1}}
	svc2.DeliverOnce(ctx)
	if sender2.count() != 0 {
		t.Fatalf("stale renewal sent %d notifications, want 0 (double-page guard)", sender2.count())
	}
	if len(ms2.finished) != 0 {
		t.Fatalf("stale renewal finished the task: %+v", ms2.finished)
	}
}

// P8-P1-04 单渠道速率上限：2 条同渠道任务至少间隔 1/rate。
func TestChannelRateLimit(t *testing.T) {
	ms := &modeStore{}
	sender := &captureSender{}
	svc := NewService(ms, sender)
	svc.SetDeliveryTuning(1, 5) // serial + 5 sends/sec
	ms.tasks = []Task{
		{ID: "c1", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1, ChannelID: "ch-1"},
		{ID: "c2", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1, ChannelID: "ch-1"},
	}
	start := time.Now()
	svc.DeliverOnce(context.Background())
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("channel pacing not enforced: 2 sends in %s", elapsed)
	}
	if sender.count() != 2 {
		t.Fatalf("sent=%d, want 2", sender.count())
	}
}

// P8-P0-01 多副本安全：两个 worker 以同一模式启动时，第二个的 authority
// 同步不得 bump generation（否则第一个 worker 已领取/待领取的任务会被判为
// 旧纪元而搁浅）；同一纪元内任务可被任一副本领取。
func TestMultiWorkerSameModeSharesGeneration(t *testing.T) {
	ctx := context.Background()
	st := &modeStore{}
	a := NewService(st, &captureSender{})
	a.SetMode(ModeActive)
	if err := a.InitAuthority(ctx); err != nil {
		t.Fatal(err)
	}
	genAfterA := st.authGen

	b := NewService(st, &captureSender{})
	b.SetMode(ModeActive)
	if err := b.InitAuthority(ctx); err != nil {
		t.Fatal(err)
	}
	if st.authGen != genAfterA {
		t.Fatalf("second replica bumped generation %d → %d (pending tasks would be stranded)",
			genAfterA, st.authGen)
	}

	// 同纪元任务对两个副本都可见：A 领走一条后，B 仍能领到剩余一条。
	st.tasks = []Task{
		{ID: "mw-1", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: genAfterA},
		{ID: "mw-2", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: genAfterA},
	}
	a.SetTuning(0, 0, 1) // batch=1：A 只领一条
	if n := a.DeliverOnce(ctx); n != 1 {
		t.Fatalf("replica A claimed %d, want 1", n)
	}
	if n := b.DeliverOnce(ctx); n != 1 {
		t.Fatalf("replica B claimed %d, want 1 (same-era task must stay claimable)", n)
	}
}

// P9-OBS-04：投递尝试、authority generation 与续租失败都有指标。
func TestNotifyMetrics(t *testing.T) {
	ctx := context.Background()
	ms := &modeStore{}
	svc := NewService(ms, &captureSender{})
	ms.tasks = []Task{{ID: "nm-1", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1}}

	beforeSent := testutil.ToFloat64(attempts.WithLabelValues("", "sent"))
	svc.DeliverOnce(ctx)
	if got := testutil.ToFloat64(attempts.WithLabelValues("", "sent")) - beforeSent; got != 1 {
		t.Fatalf("notify attempt{sent} delta = %v, want 1", got)
	}
	var gauge dto.Metric
	if err := authorityGeneration.Write(&gauge); err != nil {
		t.Fatal(err)
	}
	if got := gauge.GetGauge().GetValue(); got != 1 {
		t.Fatalf("authority_generation = %v, want 1", got)
	}

	beforeStale := testutil.ToFloat64(leaseRenewFailures.WithLabelValues("stale"))
	ms2 := &modeStore{}
	svc2 := NewService(&staleRenewStore{modeStore: ms2}, &captureSender{})
	ms2.tasks = []Task{{ID: "nm-2", Attempts: 1, Payload: []byte(`{}`), AuthorityGeneration: 1}}
	svc2.DeliverOnce(ctx)
	if got := testutil.ToFloat64(leaseRenewFailures.WithLabelValues("stale")) - beforeStale; got != 1 {
		t.Fatalf("lease_renew_failures{stale} delta = %v, want 1", got)
	}
}
