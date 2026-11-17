// Package notify implements the notification outbox worker (NOTI-001):
// claim (SKIP LOCKED + lease) -> silence re-check -> send -> outcome record.
// Delivery is at-least-once with per-attempt audit rows; crashed senders'
// leases expire and tasks re-claim with an incremented attempts counter.
package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
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
	CreatedAt   time.Time // enqueue time (shadow-era diagnostics)
	LeaseToken  string    // P1-02 fencing token minted by the claim; empty = legacy
	// AuthorityGeneration is the ownership era the task was enqueued in
	// (audit/metrics marker only — deliverability is decided by CreatedMode).
	AuthorityGeneration int64
	// CreatedMode (R27) is the authority mode at enqueue time: deliverable
	// tasks are exactly the ones created while authority was ACTIVE. Shadow-era
	// observations and pre-ledger rows (empty mode) are never claimed.
	CreatedMode string
}

// ErrStaleNotifyLease marks a finish attempt from a worker that no longer
// owns the lease (expired + re-claimed elsewhere): the attempt is audited,
// the current owner's state is untouched.
// ErrAuthorityFenced means the DB authority is no longer active: a stale
// worker must stop claiming/sending even though its cached mode says active.
var ErrAuthorityFenced = errors.New("notification authority fenced")

var ErrStaleNotifyLease = errors.New("notify: stale lease — task was re-claimed; result audited only")

// Authority is the persisted notification-ownership state (P8-P0-01).
// Mode mirrors ALERT_NOTIFY_MODE; Generation is the ownership era: every mode
// change opens a new generation, and tasks are only ever claimed within the
// generation they were enqueued in. This survives process restarts — the
// shadow→active cutoff cannot live in memory because Kubernetes switches
// authority by changing env and rolling the Pod.
type Authority struct {
	Mode        string
	Generation  int64
	EffectiveAt time.Time
}

// Store is the notification persistence port (infrastructure/postgres).
type Store interface {
	// SyncNotifyAuthority reconciles the persisted ownership state with the
	// configured desired mode: same mode = no-op; different mode (or the first
	// initialization after the authority ledger was introduced) = new
	// generation, effective now. Returns the authoritative state.
	SyncNotifyAuthority(ctx context.Context, desiredMode string) (Authority, error)
	// ClaimNotifyTasks claims due tasks whose created_mode is ACTIVE
	// (R27): shadow-era observations and pre-ledger rows are never claimed,
	// but active-era tasks interrupted by a shadow switch deliver after the
	// authority returns (REL-002 phase 4).
	ClaimNotifyTasks(ctx context.Context, limit int, lease time.Duration) ([]Task, error)
	// FinishNotifyTask records one attempt outcome. leaseToken (P1-02) fences
	// the completion: a stale worker's late result is audited but never
	// overwrites the current owner's state (ErrStaleNotifyLease).
	FinishNotifyTask(ctx context.Context, id, leaseToken string, sent bool, retryAfter time.Duration, attemptNo int, httpStatus int, failureCode, errMsg string, maxAttempts int) error
	// RenewNotifyLease extends the current owner's lease before a send
	// (P8-P1-04); ErrStaleNotifyLease when the task was re-claimed by another
	// worker, ErrAuthorityFenced when the DB authority is no longer active —
	// the caller must NOT send in either case (duplicate page / stale era).
	RenewNotifyLease(ctx context.Context, id, leaseToken string, lease time.Duration) error
	// ReleaseNotifyClaim returns a fenced claim to pending without counting a
	// delivery failure (P10-005): if authority returns to active the task is
	// re-claimed exactly once (REL-002 phase 4).
	ReleaseNotifyClaim(ctx context.Context, id, leaseToken string) error
	// GetNotifyAuthority reads the CURRENT DB authority (mode/generation) for
	// periodic re-sync; execution fencing never trusts the cached copy.
	GetNotifyAuthority(ctx context.Context) (Authority, error)
	SuppressNotifyAtSend(ctx context.Context, taskID string, labels map[string]string) (bool, error)
	// PeekNotifyTasks lists pending/retry tasks WITHOUT claiming or mutating
	// them (REL-001 shadow mode: the would-have-sent trail must not disturb the
	// delivery queue state machine). AUD-012: the trail covers ALL pending
	// work — including active-era backlog from before an active→shadow switch
	// — not just the current generation.
	PeekNotifyTasks(ctx context.Context, limit int) ([]Task, error)
	// FailOrphanNotifyTasks dead-letters pending tasks whose channel is
	// missing or disabled: they can never be claimed (claim joins enabled
	// channels) yet used to sit pending forever, inflating the gauge.
	FailOrphanNotifyTasks(ctx context.Context) (int64, error)
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
	// generation (P8-P0-01) is the persisted ownership era this worker is
	// allowed to deliver in; tasks enqueued in older eras are shadow-era
	// observations and must never be auto-replayed, across process restarts.
	generation int64
	// authorityDirty marks that the desired mode changed and needs another
	// SyncNotifyAuthority before the next claim.
	authorityDirty bool
	// authoritySyncedAt throttles the read-only re-sync (P10-005): a stale
	// worker must notice an authority change made by another replica.
	authoritySyncedAt time.Time

	// P8-P1-04 delivery pacing: bounded parallelism keeps one batch well
	// inside the lease window, the per-channel limiter protects vendor
	// quotas, and every send renews its lease right before the HTTP call.
	concurrency int
	maxAttempts int     // dead-letter threshold for delivery attempts (AUD-002)
	channelRate float64 // sends/sec per channel; 0 = unlimited
	rateMu      sync.Mutex
	channelNext map[string]time.Time
	// pendingSampledAt throttles the pending-depth COUNT (P9-OBS-04).
	pendingSampledAt time.Time
	// orphanSweptAt throttles the disabled-channel dead-letter sweep.
	orphanSweptAt time.Time
}

// NewService builds the worker; defaults: tick 500ms, batch 50, lease 60s,
// concurrency 4, mode active (the persisted authority row remains the source
// of truth until InitAuthority reconciles it).
func NewService(store Store, sender Sender) *Service {
	registerMetrics()
	return &Service{
		store: store, sender: sender,
		interval: 500 * time.Millisecond, lease: time.Minute, batch: 50,
		mode: ModeActive, authorityDirty: true,
		concurrency: 4, maxAttempts: 15, channelNext: map[string]time.Time{},
	}
}

// SetMaxAttempts configures the delivery dead-letter threshold (AUD-002).
func (s *Service) SetMaxAttempts(n int) {
	if n > 0 {
		s.maxAttempts = n
	}
}

// SetDeliveryTuning configures batch parallelism and the per-channel send
// rate (P8-P1-04). concurrency<=0 keeps the default; channelRatePerSec<=0
// disables channel pacing.
func (s *Service) SetDeliveryTuning(concurrency int, channelRatePerSec float64) {
	if concurrency > 0 {
		s.concurrency = concurrency
	}
	s.channelRate = channelRatePerSec
}

// InitAuthority reconciles the persisted notification ownership with the
// configured mode and records the authoritative generation (P8-P0-01). Call it
// once at startup, BEFORE any producer enqueues tasks and before Run.
func (s *Service) InitAuthority(ctx context.Context) error {
	a, err := s.store.SyncNotifyAuthority(ctx, s.mode)
	if err != nil {
		return err
	}
	s.generation = a.Generation
	s.mode = a.Mode
	s.authorityDirty = false
	s.authoritySyncedAt = time.Now()
	authorityGeneration.Set(float64(a.Generation))
	logx.CtxInfo(ctx, "notification authority", "component", "alert-worker",
		"mode", s.mode, "generation", s.generation)
	return nil
}

// ensureAuthority lazily syncs the authority before a claim (defensive: Run
// and InitAuthority normally did it already; SetMode marks it dirty).
const authorityResyncEvery = 15 * time.Second

func (s *Service) ensureAuthority(ctx context.Context) bool {
	if s.authorityDirty || s.generation == 0 {
		if err := s.InitAuthority(ctx); err != nil {
			if ctx.Err() == nil {
				logx.CtxError(ctx, "notification authority sync failed", logx.ErrKey, err)
			}
			return false
		}
		return true
	}
	// Periodic READ-ONLY re-sync: another replica (rolling upgrade) may have
	// switched the DB authority. Adopt the DB value; never write back the
	// stale process-local mode (that would re-open the era on every tick).
	if time.Since(s.authoritySyncedAt) >= authorityResyncEvery {
		a, err := s.store.GetNotifyAuthority(ctx)
		if err != nil {
			if ctx.Err() == nil {
				logx.CtxError(ctx, "notification authority re-sync failed", logx.ErrKey, err)
			}
			return false
		}
		if a.Generation != s.generation || a.Mode != s.mode {
			logx.CtxWarn(ctx, "notification authority changed; adopting DB state",
				"component", "alert-worker", "mode", a.Mode, "generation", a.Generation)
			s.mode = a.Mode
			s.generation = a.Generation
			authorityGeneration.Set(float64(a.Generation))
		}
		s.authoritySyncedAt = time.Now()
	}
	return true
}

// SetMode switches delivery authority (REL-001): shadow keeps an auditable
// record of what WOULD have been sent, active pages for real. P8-P0-01: the
// switch is persisted as a new authority generation (SyncNotifyAuthority on
// the next tick), so historical pending tasks of earlier generations stay
// observations even after a full worker restart — auto-replaying them would
// storm every channel the upstream already notified.
func (s *Service) SetMode(mode string) {
	switch mode {
	case ModeActive, ModeShadow, ModeOff:
		if mode != s.mode {
			s.authorityDirty = true
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

// Run delivers until ctx is done. The authority sync happens up front: a
// worker that cannot establish which generation owns delivery must not start
// sending (fail-stop — the notifier is a critical supervised task).
func (s *Service) Run(ctx context.Context) error {
	if err := s.InitAuthority(ctx); err != nil {
		return fmt.Errorf("notify: notification authority unavailable: %w", err)
	}
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
	if !s.ensureAuthority(ctx) {
		return 0
	}
	s.samplePending(ctx)
	if s.mode == ModeOff {
		return 0
	}
	if s.mode == ModeShadow {
		return s.shadowOnce(ctx)
	}
	s.sweepOrphans(ctx)
	tasks, err := s.store.ClaimNotifyTasks(ctx, s.batch, s.lease)
	if err != nil {
		if ctx.Err() == nil {
			logx.CtxError(ctx, "notify claim failed", logx.ErrKey, err)
		}
		return 0
	}
	// P8-P1-04: deliver with bounded parallelism. Sequential delivery of a
	// 50-task batch at 10s timeouts can outlive the 60s lease; parallelism
	// keeps the batch well inside the window and the per-task lease renewal
	// in deliverOne covers the rest.
	s.deliverTasks(ctx, tasks)
	return len(tasks)
}

func (s *Service) deliverTasks(ctx context.Context, tasks []Task) {
	concurrency := s.concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, t := range tasks {
		wg.Add(1)
		sem <- struct{}{}
		go func(t Task) {
			defer wg.Done()
			defer func() { <-sem }()
			s.deliverOne(ctx, t)
		}(t)
	}
	wg.Wait()
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
	// P8-P1-04: refresh the lease immediately before doing work. A batch of
	// 50 tasks at 10s channel timeouts can outlive the 60s lease; without
	// renewal another worker re-claims mid-send and pages twice. If the
	// renewal is fenced (task already re-claimed), skip — sending now would
	// duplicate. A DB error also defers: the task stays leased and retries
	// after expiry rather than risking an unowned send.
	if err := s.store.RenewNotifyLease(ctx, t.ID, t.LeaseToken, s.lease); err != nil {
		if errors.Is(err, ErrAuthorityFenced) {
			// P10-005: the DB authority flipped (active→shadow/off) while this
			// task was claimed. Do NOT send; return the claim to pending so a
			// later active era can deliver it once.
			leaseRenewFailures.WithLabelValues("authority").Inc()
			if rerr := s.store.ReleaseNotifyClaim(ctx, t.ID, t.LeaseToken); rerr != nil && ctx.Err() == nil {
				logx.CtxError(ctx, "release fenced notification claim failed", logx.ErrKey, rerr, "task", t.ID)
			}
			logx.CtxWarn(ctx, "notification authority fenced before send; claim released",
				"component", "alert-worker", "task", t.ID)
			return
		}
		if errors.Is(err, ErrStaleNotifyLease) {
			leaseRenewFailures.WithLabelValues("stale").Inc()
			logx.CtxWarn(ctx, "notification lease lost before send; skipping duplicate",
				"component", "alert-worker", "task", t.ID)
			return
		}
		leaseRenewFailures.WithLabelValues("error").Inc()
		logx.CtxError(ctx, "notification lease renewal failed; delivery deferred (fail-closed)",
			logx.ErrKey, err, "task", t.ID)
		return
	}

	// Silence re-check at send time (docs/04 §5); the labels travel in the
	// routing payload snapshot taken at enqueue time.
	var p struct {
		Labels map[string]string `json:"labels"`
	}
	_ = json.Unmarshal(t.Payload, &p)
	if suppressed, err := s.store.SuppressNotifyAtSend(ctx, t.ID, p.Labels); err != nil {
		// P8-P1-03: silences are a hard constraint. When the re-check cannot
		// be evaluated (DB error) the task is rescheduled as retryable —
		// defaulting to "send" would bypass suppression exactly when the
		// system is degraded. An emergency override must be an explicit
		// policy decision, never an automatic fallback.
		logx.CtxError(ctx, "silence re-check failed; delivery deferred (fail-closed)",
			logx.ErrKey, err, "task", t.ID)
		if ferr := s.store.FinishNotifyTask(ctx, t.ID, t.LeaseToken, false, 0, t.Attempts, 0, "retryable",
			"silence re-check failed: "+err.Error(), s.maxAttempts); ferr != nil {
			logx.CtxError(ctx, "notify finish failed after silence error", logx.ErrKey, ferr, "task", t.ID)
		}
		return
	} else if suppressed {
		logx.CtxInfo(ctx, "notification suppressed by silence at send time", "task", t.ID)
		return
	}

	// P8-P1-04: per-channel pacing protects vendor quotas. Third-party channels
	// have no idempotency key — delivery stays at-least-once even with lease
	// fencing, which is why pacing and renewal reduce (not eliminate) dup risk.
	if !s.waitChannelSlot(ctx, t.ChannelID) {
		return
	}
	res := s.sender.Send(ctx, t.ChannelType, t.ChannelCfg, t.Payload)
	attempts.WithLabelValues(t.ChannelType, attemptResult(res)).Inc()
	// AUD-002: terminal outcomes are observable (failed counter) — the store
	// dead-letters on permanent/ssrf/max-attempts.
	if !res.Sent {
		switch {
		case res.FailureCode == "permanent":
			failedDeliveries.WithLabelValues("permanent").Inc()
		case res.FailureCode == "ssrf":
			failedDeliveries.WithLabelValues("ssrf").Inc()
		case t.Attempts >= s.maxAttempts:
			failedDeliveries.WithLabelValues("max_attempts").Inc()
		}
	}
	if err := s.store.FinishNotifyTask(ctx, t.ID, t.LeaseToken, res.Sent, res.RetryAfter, t.Attempts, res.HTTPStatus, res.FailureCode, res.Err, s.maxAttempts); err != nil {
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

// samplePending refreshes alert_platform_notify_pending every 30s when the
// store supports the depth counter (real PG; test doubles skip it).
func (s *Service) samplePending(ctx context.Context) {
	if time.Since(s.pendingSampledAt) < 30*time.Second {
		return
	}
	counter, ok := s.store.(interface {
		CountPendingNotifyTasks(ctx context.Context) (int64, error)
	})
	if !ok {
		return
	}
	s.pendingSampledAt = time.Now()
	n, err := counter.CountPendingNotifyTasks(ctx)
	if err != nil {
		if ctx.Err() == nil {
			logx.CtxError(ctx, "pending notification count failed", logx.ErrKey, err)
		}
		return
	}
	pendingGauge.Set(float64(n))
}

// sweepOrphans dead-letters pending work whose channel was deleted/disabled
// (they can never be claimed; AUD-002). Throttled to once a minute.
func (s *Service) sweepOrphans(ctx context.Context) {
	if time.Since(s.orphanSweptAt) < time.Minute {
		return
	}
	s.orphanSweptAt = time.Now()
	n, err := s.store.FailOrphanNotifyTasks(ctx)
	if err != nil {
		if ctx.Err() == nil {
			logx.CtxError(ctx, "orphan notification sweep failed", logx.ErrKey, err)
		}
		return
	}
	if n > 0 {
		failedDeliveries.WithLabelValues("channel_unavailable").Add(float64(n))
		logx.CtxWarn(ctx, "dead-lettered notifications with disabled/missing channels", "rows", n)
	}
}

// waitChannelSlot paces sends per channel (P8-P1-04). Returns false when the
// context ends while waiting. Zero rate = unlimited.
func (s *Service) waitChannelSlot(ctx context.Context, channelID string) bool {
	if s.channelRate <= 0 {
		return true
	}
	every := time.Duration(float64(time.Second) / s.channelRate)
	for {
		s.rateMu.Lock()
		now := time.Now()
		next, seen := s.channelNext[channelID]
		if !seen || !next.After(now) {
			s.channelNext[channelID] = now.Add(every)
			s.rateMu.Unlock()
			return true
		}
		wait := next.Sub(now)
		s.rateMu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}
