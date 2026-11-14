// Package notify implements the notification outbox worker (NOTI-001):
// claim (SKIP LOCKED + lease) -> silence re-check -> send -> outcome record.
// Delivery is at-least-once with per-attempt audit rows; crashed senders'
// leases expire and tasks re-claim with an incremented attempts counter.
package notify

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/oekit/logx"
)

// Task is one claimed notification (application-level view).
type Task struct {
	ID          string
	TenantID    string
	ChannelID   string
	ReceiverID  string
	ChannelType string
	ChannelCfg  []byte
	Payload     []byte
	Attempts    int
	CreatedAt   time.Time // enqueue time (P0-07 shadow-history cutoff)
	LeaseToken  string    // P1-02 fencing token minted by the claim; empty = legacy
}

// ErrStaleNotifyLease marks a finish attempt from a worker that no longer
// owns the lease (expired + re-claimed elsewhere): the attempt is audited,
// the current owner's state is untouched.
var ErrStaleNotifyLease = errors.New("notify: stale lease — task was re-claimed; result audited only")

// Store is the notification persistence port (infrastructure/postgres).
type Store interface {
	// ClaimNotifyTasks claims due tasks; createdAfter (P0-07) excludes tasks
	// enqueued before the notification-ownership moment — the shadow→active
	// switch must never auto-replay shadow-era history. Zero time = no cutoff.
	ClaimNotifyTasks(ctx context.Context, limit int, lease time.Duration, createdAfter time.Time) ([]Task, error)
	// FinishNotifyTask records one attempt outcome. leaseToken (P1-02) fences
	// the completion: a stale worker's late result is audited but never
	// overwrites the current owner's state (ErrStaleNotifyLease).
	FinishNotifyTask(ctx context.Context, id, leaseToken string, sent bool, retryAfter time.Duration, attemptNo int, httpStatus int, failureCode, errMsg string) error
	SuppressNotifyAtSend(ctx context.Context, taskID string, labels map[string]string) (bool, error)
	// PeekNotifyTasks lists pending tasks WITHOUT claiming or mutating them
	// (REL-001 shadow mode: the would-have-sent trail must not disturb the
	// delivery queue state machine).
	PeekNotifyTasks(ctx context.Context, limit int) ([]Task, error)
	// RecordShadowDelivery writes one audited would-have-sent record; the
	// same task is only recorded once (idempotent per task).
	RecordShadowDelivery(ctx context.Context, taskID string) (bool, error)
}

// Sender delivers one payload (infrastructure/channel).
type Sender interface {
	Send(ctx context.Context, channelType string, cfg []byte, payload []byte) SendResult
}

// SendResult mirrors channel.SendResult without importing infrastructure.
type SendResult struct {
	Sent        bool
	HTTPStatus  int
	RetryAfter  time.Duration
	FailureCode string
	Err         string
}

// Delivery modes (REL-001 渐进切换): active sends; shadow records every
// delivery attempt but suppresses the actual send (the upstream system keeps
// notification authority — no double-send); off leaves tasks queued.
const (
	ModeActive = "active"
	ModeShadow = "shadow"
	ModeOff    = "off"
)

// Service runs the notify loop.
type Service struct {
	store    Store
	sender   Sender
	interval time.Duration
	lease    time.Duration
	batch    int
	mode     string
	// shadowCutoff (P0-07): the moment notification ownership last returned to
	// active from shadow. Tasks enqueued before it are shadow-era observations
	// and are never auto-claimed; zero = ownership never switched (claim all).
	shadowCutoff time.Time
}

// NewService builds the worker; defaults: tick 500ms, batch 50, lease 60s,
// mode active.
func NewService(store Store, sender Sender) *Service {
	return &Service{store: store, sender: sender, interval: 500 * time.Millisecond, lease: time.Minute, batch: 50, mode: ModeActive}
}

// SetMode switches delivery authority (REL-001): shadow keeps an auditable
// record of what WOULD have been sent, active pages for real. P0-07: leaving
// shadow for active moves the ownership cutoff forward — shadow-era pending
// tasks are historical observations, NOT a delivery backlog; auto-replaying
// them would storm every channel the upstream already notified (or the
// platform only observed). Fresh transitions after the switch deliver.
func (s *Service) SetMode(mode string) {
	switch mode {
	case ModeActive, ModeShadow, ModeOff:
		if mode == ModeActive && s.mode == ModeShadow {
			s.shadowCutoff = time.Now()
		}
		s.mode = mode
	}
}

// SetTuning overrides loop parameters (wiring/tests).
func (s *Service) SetTuning(interval, lease time.Duration, batch int) {
	if interval > 0 {
		s.interval = interval
	}
	if lease > 0 {
		s.lease = lease
	}
	if batch > 0 {
		s.batch = batch
	}
}

// Run delivers until ctx is done.
func (s *Service) Run(ctx context.Context) error {
	logx.CtxInfo(ctx, "notification worker started", "interval", s.interval.String())
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.deliverBatch(ctx)
		}
	}
}

// DeliverOnce processes one batch synchronously (tests/ops tooling).
func (s *Service) DeliverOnce(ctx context.Context) int {
	return s.deliverBatch(ctx)
}

func (s *Service) deliverBatch(ctx context.Context) int {
	if s.mode == ModeOff {
		return 0
	}
	if s.mode == ModeShadow {
		return s.shadowOnce(ctx)
	}
	tasks, err := s.store.ClaimNotifyTasks(ctx, s.batch, s.lease, s.shadowCutoff)
	if err != nil {
		if ctx.Err() == nil {
			logx.CtxError(ctx, "notify claim failed", logx.ErrKey, err)
		}
		return 0
	}
	for _, t := range tasks {
		s.deliverOne(ctx, t)
	}
	return len(tasks)
}

// shadowOnce records the would-have-sent trail WITHOUT claiming: tasks stay
// pending and untouched, so a later switch to active delivers them exactly
// once through the normal path (REL-001 无双发/不丢不重发).
func (s *Service) shadowOnce(ctx context.Context) int {
	tasks, err := s.store.PeekNotifyTasks(ctx, s.batch)
	if err != nil {
		if ctx.Err() == nil {
			logx.CtxError(ctx, "shadow peek failed", logx.ErrKey, err)
		}
		return 0
	}
	n := 0
	for _, t := range tasks {
		recorded, err := s.store.RecordShadowDelivery(ctx, t.ID)
		if err != nil {
			logx.CtxError(ctx, "shadow record failed", logx.ErrKey, err, "task", t.ID)
			continue
		}
		if recorded {
			n++
		}
	}
	if n > 0 {
		logx.CtxInfo(ctx, "shadow mode: would-have-sent recorded (tasks untouched)",
			"component", "alert-worker", "recorded", n)
	}
	return n
}

func (s *Service) deliverOne(ctx context.Context, t Task) {
	// Silence re-check at send time (docs/04 §5); the labels travel in the
	// routing payload snapshot taken at enqueue time.
	var p struct {
		Labels map[string]string `json:"labels"`
	}
	_ = json.Unmarshal(t.Payload, &p)
	if suppressed, err := s.store.SuppressNotifyAtSend(ctx, t.ID, p.Labels); err != nil {
		logx.CtxError(ctx, "silence re-check failed", logx.ErrKey, err, "task", t.ID)
	} else if suppressed {
		logx.CtxInfo(ctx, "notification suppressed by silence at send time", "task", t.ID)
		return
	}

	res := s.sender.Send(ctx, t.ChannelType, t.ChannelCfg, t.Payload)
	if err := s.store.FinishNotifyTask(ctx, t.ID, t.LeaseToken, res.Sent, res.RetryAfter, t.Attempts, res.HTTPStatus, res.FailureCode, res.Err); err != nil {
		if errors.Is(err, ErrStaleNotifyLease) {
			// P1-02: the lease expired and another worker re-claimed — this
			// result is a late duplicate; the attempt row was audited, the
			// current owner's state stays authoritative.
			logx.CtxWarn(ctx, "late notification result discarded (lease fenced)",
				"component", "alert-worker", "task", t.ID, logx.ErrKey, err)
			return
		}
		logx.CtxError(ctx, "notify finish failed (attempt row may be missing)", logx.ErrKey, err, "task", t.ID)
	}
	if res.Sent {
		logx.CtxInfo(ctx, "notification delivered",
			"component", "alert-worker", "task", t.ID, "channel_type", t.ChannelType,
			"attempt", t.Attempts, "http_status", res.HTTPStatus)
		return
	}
	outcome := "failed-" + res.FailureCode
	if res.RetryAfter > 0 {
		outcome = "retry-after-" + res.RetryAfter.String()
	}
	logx.CtxWarn(ctx, "notification attempt failed",
		"component", "alert-worker", "task", t.ID, "channel_type", t.ChannelType,
		"attempt", t.Attempts, "outcome", outcome, "http_status", res.HTTPStatus,
		logx.ErrKey, res.Err)
}
