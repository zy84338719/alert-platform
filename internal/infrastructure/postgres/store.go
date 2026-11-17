package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/application/correlation"
	"github.com/example/alert-platform/internal/application/escalation"
	"github.com/example/alert-platform/internal/application/health"
	"github.com/example/alert-platform/internal/application/notify"
	"github.com/example/alert-platform/internal/application/ops"
	"github.com/example/alert-platform/internal/application/relay"
	"github.com/example/alert-platform/internal/application/routing"
	domainalert "github.com/example/alert-platform/internal/domain/alert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Store implements the application/processor.Store port on top of pgx: one
// transaction per canonical event (docs/04 §2 — business tables and
// idempotency records commit atomically; the transport offset commits after).
//
// State transitions are decided by the pure domain state machine
// (domain/alert.Apply) — PROC-005: pending/stale exits, episode reopen rules
// and out-of-order protection all live in ONE place, the database merely
// persists the decided snapshot plus the transition row and, in the same
// transaction, the domain_outbox entry (PROC-003). notification_outbox rows
// join the same transaction in P3, once the routing policy engine exists.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore builds the processor store over a pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// ApplyOutcome explains what one applied event did (logs/metrics; never
// changes caller behavior beyond duplicate handling).
type ApplyOutcome struct {
	Duplicate bool
	Reason    string
}

// ApplyEvent writes receipt + (instance + transition + domain_outbox) in one
// transaction. duplicate=true means the transport occurrence was already
// applied — callers treat it as success.
func (s *Store) ApplyEvent(ctx context.Context, ev *eventv1.AlertEvent) (bool, error) {
	out, err := s.applyEvent(ctx, ev)
	if err != nil {
		return false, err
	}
	return out.Duplicate, nil
}

func (s *Store) applyEvent(ctx context.Context, ev *eventv1.AlertEvent) (ApplyOutcome, error) {
	// Contract violations are Permanent (R20): the processor quarantines them
	// into the DLQ. Returning plain errors here made a single malformed event
	// fail-stop the critical processor task and crash-loop on replay.
	if ev.GetTransportId() == "" {
		return ApplyOutcome{}, fmt.Errorf("%w: missing transport_id", domainalert.ErrInvalidEvent)
	}
	if ev.GetTenantId() == "" || ev.GetSource() == nil || ev.GetSource().GetSourceId() == "" {
		return ApplyOutcome{}, fmt.Errorf("%w: missing tenant/source identity", domainalert.ErrInvalidEvent)
	}
	if ev.GetSignal() == eventv1.EventSignal_EVENT_SIGNAL_UNSPECIFIED {
		return ApplyOutcome{}, fmt.Errorf("%w: missing signal", domainalert.ErrInvalidEvent)
	}
	if ev.GetSignal() == eventv1.EventSignal_EVENT_SIGNAL_FIRING && ev.GetSeverity() == eventv1.Severity_SEVERITY_UNSPECIFIED {
		return ApplyOutcome{}, fmt.Errorf("%w: firing event missing severity", domainalert.ErrInvalidEvent)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ApplyOutcome{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1) Idempotency: the receipt IS the dedup record. ON CONFLICT DO UPDATE
	// covers BOTH uniqueness layers — transport_id and (tenant, source,
	// source_event_key) — and REFRESHES last_seen_at on duplicates (AUD-004:
	// with DO NOTHING the column stayed == first_seen, so retention deleted
	// receipts of long-running alerts after 7d and replayed events re-applied).
	// RETURNING (xmax = 0) distinguishes INSERT from CONFLICT UPDATE.
	// SAVEPOINT: a source_event_key collision raises 23505 inside this tx; we
	// roll back to the savepoint and refresh instead (the tx stays usable).
	if _, err := tx.Exec(ctx, `SAVEPOINT receipt_ins`); err != nil {
		return ApplyOutcome{}, fmt.Errorf("receipt savepoint: %w", err)
	}
	var inserted bool
	err = tx.QueryRow(ctx, `
		INSERT INTO event_receipts (tenant_id, source_id, transport_id, source_event_key)
		VALUES ($1, $2, $3, NULLIF($4,''))
		ON CONFLICT (tenant_id, source_id, transport_id)
		DO UPDATE SET last_seen_at = now()
		RETURNING (xmax = 0)`,
		ev.GetTenantId(), ev.GetSource().GetSourceId(), ev.GetTransportId(), ev.GetEventKey()).Scan(&inserted)
	if err != nil {
		// A different transport id with the same (tenant, source, event_key)
		// hits the OTHER unique index; PostgreSQL cannot infer two conflict
		// targets in one statement. Refresh and treat as duplicate.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && ev.GetEventKey() != "" {
			if _, rerr := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT receipt_ins`); rerr != nil {
				return ApplyOutcome{}, fmt.Errorf("receipt savepoint rollback: %w", rerr)
			}
			if _, uerr := tx.Exec(ctx, `
				UPDATE event_receipts SET last_seen_at = now()
				WHERE tenant_id=$1 AND source_id=$2 AND source_event_key=$3`,
				ev.GetTenantId(), ev.GetSource().GetSourceId(), ev.GetEventKey()); uerr != nil {
				return ApplyOutcome{}, fmt.Errorf("refresh receipt: %w", uerr)
			}
			inserted = false
		} else {
			return ApplyOutcome{}, fmt.Errorf("insert receipt: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `RELEASE SAVEPOINT receipt_ins`); err != nil {
		return ApplyOutcome{}, fmt.Errorf("receipt savepoint release: %w", err)
	}
	if !inserted {
		// P8-REG-01: a repeat FIRING observation deduped by a stable key
		// (Alertmanager amObservationKey) must still refresh liveness — the
		// receipt suppresses the state-machine input, but freezing
		// last_observed_at would let stale-reconciliation age out an alert
		// that is demonstrably still firing. No transition, no notification:
		// only the observation timestamp moves.
		if ev.GetSignal() == eventv1.EventSignal_EVENT_SIGNAL_FIRING {
			if fp, ferr := fingerprintOf(ev); ferr == nil {
				if lerr := tryIdentityLock(ctx, tx, ev.GetTenantId()+"|"+fp); lerr != nil {
					return ApplyOutcome{}, lerr
				}
				if _, uerr := tx.Exec(ctx, `
					UPDATE alert_instances
					SET last_observed_at = GREATEST(last_observed_at, $3), updated_at = now()
					WHERE tenant_id=$1 AND fingerprint=$2 AND monitor_state IN ('pending','firing','stale')`,
					ev.GetTenantId(), fp, orNow(eventTime(ev.GetReceivedAt()), time.Now)); uerr != nil {
					return ApplyOutcome{}, fmt.Errorf("refresh duplicate observation: %w", uerr)
				}
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return ApplyOutcome{}, fmt.Errorf("commit duplicate: %w", err)
		}
		return ApplyOutcome{Duplicate: true, Reason: "duplicate-observation"}, nil
	}

	// 1b) Event history (P0-05): every applied observation becomes queryable —
	// OCCURRENCE-class events (HUATUO kernel anomalies) were previously
	// receipt-only and invisible to the event history / incident workbench.
	// Same transaction as the receipt; duplicates never reach here.
	if err := insertEventHistory(ctx, tx, ev); err != nil {
		return ApplyOutcome{}, err
	}

	// 2) Occurrence events carry no lifecycle (docs/03 §4): receipt + history.
	if ev.GetKind() == eventv1.EventKind_EVENT_KIND_OCCURRENCE {
		if err := tx.Commit(ctx); err != nil {
			return ApplyOutcome{}, fmt.Errorf("commit: %w", err)
		}
		return ApplyOutcome{Reason: "occurrence-recorded"}, nil
	}

	// 3) Identity: server-computed fingerprint (payload fingerprints are
	// never trusted — docs/03 §2). Incomplete identity is a permanent
	// contract violation (adapters guarantee rule/cluster/uid): it fails the
	// event into the DLQ instead of keying everything under one digest.
	fingerprint, err := fingerprintOf(ev)
	if err != nil {
		return ApplyOutcome{}, err
	}
	// 3b) Serialize the whole identity mutation (P8-P1-05). Two Firing events
	// with different transport ids can race between "no active instance" and
	// the insert under ReadCommitted; both would generate their own episode
	// and the (tenant, fingerprint, episode_key) uniqueness would not fire.
	// The advisory lock is per (tenant, fingerprint) and released at commit;
	// the partial unique index (0011) is the database-level backstop.
	if err := tryIdentityLock(ctx, tx, ev.GetTenantId()+"|"+fingerprint); err != nil {
		return ApplyOutcome{}, err
	}

	// 4) Health-only signals: NO_DATA/INFO update observation freshness but
	// never change monitor state — NoData must NOT auto-resolve (docs/06 §4,
	// PROC-006).
	if ev.GetSignal() == eventv1.EventSignal_EVENT_SIGNAL_NO_DATA ||
		ev.GetSignal() == eventv1.EventSignal_EVENT_SIGNAL_INFO {
		if _, err := tx.Exec(ctx, `
			UPDATE alert_instances
			SET last_observed_at = GREATEST(last_observed_at, $3), updated_at = now()
			WHERE tenant_id=$1 AND fingerprint=$2 AND monitor_state IN ('pending','firing','stale')`,
			ev.GetTenantId(), fingerprint, orNow(eventTime(ev.GetOccurredAt()), time.Now)); err != nil {
			return ApplyOutcome{}, fmt.Errorf("health observation: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return ApplyOutcome{}, fmt.Errorf("commit: %w", err)
		}
		return ApplyOutcome{Reason: "health-observation"}, nil
	}

	signal := domainalert.SignalFiring
	if ev.GetSignal() == eventv1.EventSignal_EVENT_SIGNAL_RESOLVED {
		signal = domainalert.SignalResolved
	}
	receivedAt := orNow(eventTime(ev.GetReceivedAt()), time.Now)
	occurredAt := eventTime(ev.GetOccurredAt()) // zero = unknown (docs/03 §3)

	// 5) Load the active (pending/firing/stale) instance for this identity.
	row := tx.QueryRow(ctx, `
		SELECT id::text, episode_key, monitor_state, last_observed_at, last_transition_at, version
		FROM alert_instances
		WHERE tenant_id=$1 AND fingerprint=$2 AND monitor_state IN ('pending','firing','stale')
		ORDER BY updated_at DESC LIMIT 1`,
		ev.GetTenantId(), fingerprint)
	var activeID, activeEpisode, activeState string
	var activeObserved, activeTransition time.Time
	var activeVersion int64
	snapshot := domainalert.Snapshot{}
	err = row.Scan(&activeID, &activeEpisode, &activeState, &activeObserved, &activeTransition, &activeVersion)
	switch {
	case err == nil:
		snapshot = domainalert.Snapshot{
			EpisodeKey:       activeEpisode,
			State:            domainalert.State(activeState),
			LastObservedAt:   activeObserved,
			LastTransitionAt: activeTransition,
			Version:          uint64(activeVersion),
		}
	case errors.Is(err, pgx.ErrNoRows):
		// no active episode
	default:
		return ApplyOutcome{}, fmt.Errorf("lookup active instance: %w", err)
	}

	obs := domainalert.Observation{
		EpisodeKey: snapshot.EpisodeKey,
		Signal:     signal,
		OccurredAt: occurredAt,
		ReceivedAt: receivedAt,
	}
	if snapshot.EpisodeKey == "" {
		// First observation of a new episode — the domain machine only opens
		// episodes on FIRING; a stray RESOLVED with no active episode is
		// absorbed (receipt kept, no transition, no error): late recoveries
		// for already-closed episodes must not poison the stream.
		if signal == domainalert.SignalResolved {
			if err := tx.Commit(ctx); err != nil {
				return ApplyOutcome{}, fmt.Errorf("commit: %w", err)
			}
			return ApplyOutcome{Reason: "resolved-without-active-episode"}, nil
		}
		// Guard: a FIRING older than the latest closed episode of this
		// identity is an out-of-order straggler, not a new episode (T-006).
		var lastClose time.Time
		err := tx.QueryRow(ctx, `
			SELECT last_transition_at FROM alert_instances
			WHERE tenant_id=$1 AND fingerprint=$2 AND monitor_state='resolved'
			ORDER BY last_transition_at DESC LIMIT 1`,
			ev.GetTenantId(), fingerprint).Scan(&lastClose)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// never seen: genuinely new
		case err != nil:
			return ApplyOutcome{}, fmt.Errorf("lookup closed episodes: %w", err)
		default:
			if !occurredAt.IsZero() && occurredAt.Before(lastClose) {
				if err := tx.Commit(ctx); err != nil {
					return ApplyOutcome{}, fmt.Errorf("commit: %w", err)
				}
				return ApplyOutcome{Reason: "out-of-order-firing-suppressed"}, nil
			}
		}
		obs.EpisodeKey = uuid.NewString()
	}

	outcome, err := domainalert.Apply(snapshot, obs)
	if err != nil {
		if errors.Is(err, domainalert.ErrOutOfOrder) {
			// Absorb: receipt committed, state untouched (docs/03 §6).
			if err := tx.Commit(ctx); err != nil {
				return ApplyOutcome{}, fmt.Errorf("commit: %w", err)
			}
			return ApplyOutcome{Reason: "out-of-order-suppressed"}, nil
		}
		return ApplyOutcome{}, fmt.Errorf("state machine: %w", err)
	}

	if !outcome.Changed {
		// Repeat observation: freshness + latest severity/labels only; no
		// transition, no outbox (docs/03 §6 重复推送原则).
		if _, err := tx.Exec(ctx, `
			UPDATE alert_instances
			SET last_observed_at=$3, severity=COALESCE(NULLIF($4,''), severity), labels=$5,
			    version=version+1, updated_at=now()
			WHERE id=$1 AND episode_key=$2`,
			activeID, activeEpisode, orNow(occurredAt, time.Now), severityOrNull(ev), labelsJSON(ev.GetLabels())); err != nil {
			return ApplyOutcome{}, fmt.Errorf("update instance: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return ApplyOutcome{}, fmt.Errorf("commit: %w", err)
		}
		return ApplyOutcome{Reason: outcome.Reason}, nil
	}

	// State changed: persist snapshot + transition + domain outbox atomically.
	var instanceID string
	if snapshot.EpisodeKey == "" {
		instanceID = uuid.NewString()
		if err := s.insertInstance(ctx, tx, ev, fingerprint, instanceID, outcome.Next); err != nil {
			return ApplyOutcome{}, err
		}
		// 1:1 incident entity (UI-004/FR-008): every firing episode opens a
		// disposal incident in the SAME transaction; the alert's monitor
		// state and the incident's disposal state stay independent.
		if err := recordUIDHistory(ctx, tx, ev); err != nil {
			return ApplyOutcome{}, fmt.Errorf("uid history: %w", err)
		}
		incidentID := uuid.NewString()
		if _, err := tx.Exec(ctx, `
			INSERT INTO incidents (id, tenant_id, correlation_key, title, status, priority)
			VALUES ($1,$2,$3,$4,'open',$5)`,
			incidentID, ev.GetTenantId(), instanceID, ev.GetAlertName(), severityOr(ev, "warning")); err != nil {
			return ApplyOutcome{}, fmt.Errorf("insert incident: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO incident_alerts (tenant_id, incident_id, alert_instance_id, relation_type)
			VALUES ($1,$2,$3,'derived_1to1')`,
			ev.GetTenantId(), incidentID, instanceID); err != nil {
			return ApplyOutcome{}, fmt.Errorf("link incident: %w", err)
		}
	} else {
		instanceID = activeID
		if err := s.updateInstance(ctx, tx, ev, instanceID, outcome.Next); err != nil {
			return ApplyOutcome{}, err
		}
	}
	transitionID := uuid.NewString()
	transitionAt := orNow(occurredAt, time.Now)
	if _, err := tx.Exec(ctx, `
		INSERT INTO alert_transitions
			(transition_id, tenant_id, alert_instance_id, from_state, to_state, event_id, transition_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		transitionID, ev.GetTenantId(), instanceID, string(snapshot.State), string(outcome.Next.State),
		ev.GetEventId(), transitionAt); err != nil {
		return ApplyOutcome{}, fmt.Errorf("insert transition: %w", err)
	}
	// AUD-001: enqueue the archive write in the SAME transaction. Late/replayed
	// events queue here when they land — no event-time cursor to miss them.
	if _, err := tx.Exec(ctx, `
		INSERT INTO transition_archive_queue
			(transition_id, tenant_id, instance_id, from_state, to_state, event_id, transition_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (transition_id) DO NOTHING`,
		transitionID, ev.GetTenantId(), instanceID, string(snapshot.State), string(outcome.Next.State),
		ev.GetEventId(), transitionAt); err != nil {
		return ApplyOutcome{}, fmt.Errorf("enqueue transition archive: %w", err)
	}
	if err := insertDomainOutbox(ctx, tx, ev, map[string]any{
		"transition_id": transitionID,
		"instance_id":   instanceID,
		"episode_key":   outcome.Next.EpisodeKey,
		"fingerprint":   fingerprint,
		"from_state":    string(snapshot.State),
		"to_state":      string(outcome.Next.State),
		"event_id":      ev.GetEventId(),
		"alert_name":    ev.GetAlertName(),
		"severity":      ev.GetSeverity().String(),
		"signal":        string(signal),
	}, "alert.state_changed.v1", instanceID); err != nil {
		return ApplyOutcome{}, err
	}
	// Notification enqueue lives in the SAME transaction (docs/04 §2/§5):
	// state, domain event and notification tasks commit atomically. Suppressed
	// deliveries are still recorded (status='suppressed') so silencing is
	// auditable and provably never touches monitor state (T-009).
	if outcome.Next.State == domainalert.StateFiring || outcome.Next.State == domainalert.StateResolved {
		notificationType := "firing"
		if outcome.Next.State == domainalert.StateResolved {
			notificationType = "resolved"
		}
		if err := s.enqueueNotifications(ctx, tx, ev, instanceID, fingerprint, notificationType, transitionID); err != nil {
			return ApplyOutcome{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ApplyOutcome{}, fmt.Errorf("commit: %w", err)
	}
	return ApplyOutcome{Reason: outcome.Reason}, nil
}

func (s *Store) insertInstance(ctx context.Context, tx pgx.Tx, ev *eventv1.AlertEvent, fingerprint, instanceID string, next domainalert.Snapshot) error {
	endsAt := eventTime(ev.GetEndsAt())
	var endsAny any
	if !endsAt.IsZero() {
		endsAny = endsAt
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO alert_instances
			(id, tenant_id, source_id, fingerprint, episode_key, name, severity,
			 monitor_state, cluster_id, entity_type, entity_uid, labels,
			 starts_at, ends_at, last_observed_at, last_transition_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		instanceID, ev.GetTenantId(), ev.GetSource().GetSourceId(), fingerprint, next.EpisodeKey,
		ev.GetAlertName(), severityOr(ev, "warning"), string(next.State),
		ev.GetResource().GetClusterId(), ev.GetResource().GetResourceType(), ev.GetResource().GetResourceUid(),
		labelsJSON(ev.GetLabels()), orNow(eventTime(ev.GetStartsAt()), time.Now), endsAny,
		next.LastObservedAt, orNow(next.LastTransitionAt, time.Now))
	if err != nil {
		return fmt.Errorf("insert instance: %w", err)
	}
	return nil
}

func (s *Store) updateInstance(ctx context.Context, tx pgx.Tx, ev *eventv1.AlertEvent, instanceID string, next domainalert.Snapshot) error {
	// Only resolution closes an episode (ends_at); re-confirmation from stale
	// keeps the original boundaries.
	if next.State == domainalert.StateResolved {
		_, err := tx.Exec(ctx, `
			UPDATE alert_instances
			SET monitor_state=$3, severity=COALESCE(NULLIF($4,''), severity), labels=$5,
			    ends_at=$6, last_observed_at=$7, last_transition_at=$8,
			    version=$9, updated_at=now()
			WHERE id=$1 AND episode_key=$2`,
			instanceID, next.EpisodeKey, string(next.State), severityOrNull(ev), labelsJSON(ev.GetLabels()),
			orNow(eventTime(ev.GetEndsAt()), time.Now), next.LastObservedAt, orNow(next.LastTransitionAt, time.Now), next.Version)
		if err != nil {
			return fmt.Errorf("update instance: %w", err)
		}
		return nil
	}
	_, err := tx.Exec(ctx, `
		UPDATE alert_instances
		SET monitor_state=$3, severity=COALESCE(NULLIF($4,''), severity), labels=$5,
		    last_observed_at=$6, last_transition_at=$7,
		    version=$8, updated_at=now()
		WHERE id=$1 AND episode_key=$2`,
		instanceID, next.EpisodeKey, string(next.State), severityOrNull(ev), labelsJSON(ev.GetLabels()),
		next.LastObservedAt, orNow(next.LastTransitionAt, time.Now), next.Version)
	if err != nil {
		return fmt.Errorf("update instance: %w", err)
	}
	return nil
}

// insertDomainOutbox records a lifecycle event in the SAME transaction as the
// state change (PROC-003): the relay publishes only committed rows.
func insertDomainOutbox(ctx context.Context, tx pgx.Tx, ev *eventv1.AlertEvent, payload map[string]any, eventType, aggregateID string) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO domain_outbox (id, tenant_id, aggregate_type, aggregate_id, event_type, event_payload)
		VALUES ($1,$2,'alert_instance',$3,$4,$5)`,
		uuid.NewString(), ev.GetTenantId(), aggregateID, eventType, raw)
	if err != nil {
		return fmt.Errorf("insert domain outbox: %w", err)
	}
	return nil
}

// insertEventHistory persists one applied observation (P0-05): the event
// history / incident workbench can query every kernel anomaly instead of only
// seeing an invisible receipt. raw_payload keeps JSONB when the payload is
// valid JSON, else wraps the text. Caller: applyEvent, post-receipt — the
// row commits atomically with the rest of the transaction.
func insertEventHistory(ctx context.Context, tx pgx.Tx, ev *eventv1.AlertEvent) error {
	kind := "stateful"
	if ev.GetKind() == eventv1.EventKind_EVENT_KIND_OCCURRENCE {
		kind = "occurrence"
	}
	var rawAny any
	if raw := ev.GetRawPayload(); len(raw) > 0 {
		if json.Valid(raw) {
			rawAny = json.RawMessage(raw)
		} else {
			rawAny = map[string]string{"text": string(raw)}
		}
	}
	var occurred any
	if t := eventTime(ev.GetOccurredAt()); !t.IsZero() {
		occurred = t
	}
	sev := ev.GetSeverity().String()
	if ev.GetSeverity() == eventv1.Severity_SEVERITY_UNSPECIFIED {
		sev = ""
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO event_history
			(tenant_id, source_id, event_id, transport_id, kind, signal, severity,
			 alert_name, cluster_id, entity_type, entity_uid, resource_name, namespace,
			 labels, annotations, raw_payload, occurred_at, received_at)
		VALUES ($1,$2,NULLIF($3,''),$4,$5,$6,NULLIF($7,''),$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		ev.GetTenantId(), ev.GetSource().GetSourceId(), ev.GetEventId(), ev.GetTransportId(),
		kind, signalText(ev.GetSignal()), sev, ev.GetAlertName(),
		ev.GetResource().GetClusterId(), ev.GetResource().GetResourceType(), ev.GetResource().GetResourceUid(),
		ev.GetResource().GetResourceName(), ev.GetResource().GetNamespace(),
		labelsJSON(ev.GetLabels()), labelsJSON(ev.GetAnnotations()), rawAny,
		occurred, orNow(eventTime(ev.GetReceivedAt()), time.Now))
	if err != nil {
		return fmt.Errorf("insert event history: %w", err)
	}
	return nil
}

// signalText renders the signal enum without the verbose prefix (history rows
// are human-facing).
func signalText(sig eventv1.EventSignal) string {
	s := sig.String()
	return strings.ToLower(strings.TrimPrefix(s, "EVENT_SIGNAL_"))
}

// MarkStaleBySources is the PROC-006 reconciliation write: for every ACTIVE
// instance of the given stale sources, firing/pending -> stale with a
// transition + outbox row, in one transaction per source batch. Stale is a
// health statement — it never resolves anything.
func (s *Store) MarkStaleBySources(ctx context.Context, tenantID string, sourceIDs []string, reason string) (int, error) {
	if len(sourceIDs) == 0 {
		return 0, nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id::text, episode_key, monitor_state FROM alert_instances
		WHERE tenant_id=$1 AND source_id = ANY($2) AND monitor_state IN ('pending','firing')
		FOR UPDATE SKIP LOCKED`, tenantID, sourceIDs)
	if err != nil {
		return 0, err
	}
	type staleRow struct{ id, episode, from string }
	var victims []staleRow
	for rows.Next() {
		var r staleRow
		if err := rows.Scan(&r.id, &r.episode, &r.from); err != nil {
			rows.Close()
			return 0, err
		}
		victims = append(victims, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, v := range victims {
		transitionID := uuid.NewString()
		// monitor_state flips to stale but last_transition_at KEEPS the last
		// source-authored anchor: the out-of-order guard must compare incoming
		// events against source evidence, never against the reconcile clock
		// (a firing right after reconciliation has an older occurred_at than
		// server now and would be wrongly suppressed).
		if _, err := tx.Exec(ctx, `
			UPDATE alert_instances
			SET monitor_state='stale', version=version+1, updated_at=now()
			WHERE id=$1 AND episode_key=$2`, v.id, v.episode); err != nil {
			return 0, err
		}
		// AUD-005: the reconcile event id MUST be unique per transition —
		// a fixed "reconcile:source-silent" collided with
		// UNIQUE(alert_instance_id,event_id,to_state) on the second
		// stale->firing->stale cycle and rolled back the whole batch.
		reconcileEventID := "reconcile:" + reason + ":" + transitionID
		if _, err := tx.Exec(ctx, `
			INSERT INTO alert_transitions
				(transition_id, tenant_id, alert_instance_id, from_state, to_state, event_id, transition_at)
			VALUES ($1,$2,$3,$4,'stale',$5,now())`,
			transitionID, tenantID, v.id, v.from, reconcileEventID); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO transition_archive_queue
				(transition_id, tenant_id, instance_id, from_state, to_state, event_id, transition_at)
			VALUES ($1,$2,$3,$4,'stale',$5,now())
			ON CONFLICT (transition_id) DO NOTHING`,
			transitionID, tenantID, v.id, v.from, reconcileEventID); err != nil {
			return 0, err
		}
		raw, _ := json.Marshal(map[string]any{
			"transition_id": transitionID,
			"instance_id":   v.id,
			"episode_key":   v.episode,
			"from_state":    v.from,
			"to_state":      "stale",
			"reason":        reason,
		})
		if _, err := tx.Exec(ctx, `
			INSERT INTO domain_outbox (id, tenant_id, aggregate_type, aggregate_id, event_type, event_payload)
			VALUES ($1,$2,'alert_instance',$3,'alert.state_changed.v1',$4)`,
			uuid.NewString(), tenantID, v.id, raw); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(victims), nil
}

// SweepOutbox resolves claims abandoned by a crashed or timed-out relay.
// Rows under max attempts return to pending; rows at/over the limit enter the
// terminal 'failed' state instead of retrying forever (AUD-011).
func (s *Store) SweepOutbox(ctx context.Context, visibilityTimeout time.Duration, maxAttempts int) (int64, int64, error) {
	var requeued, dead int64
	err := s.pool.QueryRow(ctx, `
		WITH stale AS (
			SELECT id, attempts FROM domain_outbox
			WHERE status='publishing' AND next_attempt_at < now() - $1::bigint * interval '1 second'
		), failed AS (
			UPDATE domain_outbox d SET status='failed'
			FROM stale s WHERE d.id=s.id AND s.attempts >= $2
			RETURNING d.id
		), requeued AS (
			UPDATE domain_outbox d SET status='pending'
			FROM stale s WHERE d.id=s.id AND s.attempts < $2
			RETURNING d.id
		)
		SELECT (SELECT count(*) FROM failed), (SELECT count(*) FROM requeued)`,
		int64(visibilityTimeout.Seconds()), maxAttempts).Scan(&dead, &requeued)
	return requeued, dead, err
}

// ClaimOutboxRows returns up to limit pending domain events in created_at
// order, skipping rows that are not due yet and rows whose older same-key row
// is still pending/publishing. The NOT EXISTS guard preserves per-aggregate
// ordering: a newer lifecycle event can never overtake a failed/backing-off
// one (AUD-011). Claiming bumps attempts.
func (s *Store) ClaimOutboxRows(ctx context.Context, limit, maxAttempts int) ([]relay.OutboxRow, error) {
	rows, err := s.pool.Query(ctx, `
		WITH claimed AS (
			SELECT o.id FROM domain_outbox o
			WHERE o.status='pending'
			  AND o.attempts < $2
			  AND o.next_attempt_at <= now()
			  AND NOT EXISTS (
				SELECT 1 FROM domain_outbox e
				WHERE e.tenant_id = o.tenant_id
				  AND e.aggregate_id = o.aggregate_id
				  AND e.status IN ('pending','publishing')
				  AND (e.created_at, e.id) < (o.created_at, o.id)
			  )
			ORDER BY o.created_at, o.id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE domain_outbox d
		SET status='publishing', attempts=d.attempts+1, next_attempt_at=now()
		FROM claimed c WHERE d.id=c.id
		RETURNING d.id::text, d.tenant_id::text, d.aggregate_id, d.event_type,
		          d.event_payload, d.attempts`, limit, maxAttempts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []relay.OutboxRow
	for rows.Next() {
		var r relay.OutboxRow
		if err := rows.Scan(&r.ID, &r.TenantID, &r.AggregateID, &r.EventType, &r.Payload, &r.Attempts); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkOutboxPublished records successful publish (idempotent per row).
func (s *Store) MarkOutboxPublished(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE domain_outbox SET status='published', published_at=now() WHERE id=$1`, id)
	return err
}

// RequeueOutboxRows returns claimed-but-unpublished rows to pending after a
// publish failure. delay staggers the failed row's retry without unblocking
// newer same-key rows (they remain ordered behind it).
func (s *Store) RequeueOutboxRows(ctx context.Context, ids []string, delay time.Duration) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE domain_outbox
		SET status='pending', next_attempt_at = now() + $2::bigint * interval '1 millisecond'
		WHERE id = ANY($1::uuid[]) AND status='publishing'`,
		ids, delay.Milliseconds())
	return err
}

// OutboxStats reports the outbox depths (metrics / readiness).
func (s *Store) OutboxStats(ctx context.Context) (int64, int64, int64, error) {
	var pending, inflight, failed int64
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE status='pending'),
		       count(*) FILTER (WHERE status='publishing'),
		       count(*) FILTER (WHERE status='failed')
		FROM domain_outbox`).Scan(&pending, &inflight, &failed)
	return pending, inflight, failed, err
}

// StaleSources implements health.Store: enabled sources silent past cutoff.
func (s *Store) StaleSources(ctx context.Context, cutoff time.Time) ([]health.StaleSource, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id::text, source_id::text FROM alert_sources
		WHERE enabled AND last_seen_at IS NOT NULL AND last_seen_at < $1
		  AND health_status <> 'stale'`, cutoff)
	if err != nil {
		return nil, err
	}
	return scanSources(rows)
}

// RecoveredSources implements health.Store: sources marked stale that pushed
// again inside the window.
func (s *Store) RecoveredSources(ctx context.Context, cutoff time.Time) ([]health.StaleSource, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id::text, source_id::text FROM alert_sources
		WHERE health_status = 'stale' AND last_seen_at >= $1`, cutoff)
	if err != nil {
		return nil, err
	}
	return scanSources(rows)
}

// MarkSourceHealth flips the health marker (health.Store).
func (s *Store) MarkSourceHealth(ctx context.Context, tenantID, sourceID, status string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE alert_sources SET health_status=$3, updated_at=now()
		WHERE tenant_id=$1 AND source_id=$2`, tenantID, sourceID, status)
	return err
}

func scanSources(rows pgx.Rows) ([]health.StaleSource, error) {
	defer rows.Close()
	var out []health.StaleSource
	for rows.Next() {
		var r health.StaleSource
		if err := rows.Scan(&r.TenantID, &r.SourceID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func fingerprintOf(ev *eventv1.AlertEvent) (string, error) {
	identity := domainalert.Identity{
		TenantID:   ev.GetTenantId(),
		SourceID:   ev.GetSource().GetSourceId(),
		RuleID:     ev.GetSource().GetExternalRuleId(),
		ClusterID:  ev.GetResource().GetClusterId(),
		EntityType: ev.GetResource().GetResourceType(),
		EntityUID:  ev.GetResource().GetResourceUid(),
		Dimensions: ev.GetLabels(),
	}
	fp, err := domainalert.Fingerprint(identity)
	if err != nil {
		return "", fmt.Errorf("identity for %s: %w", ev.GetTransportId(), err)
	}
	return fp, nil
}

func orNow(t time.Time, now func() time.Time) time.Time {
	if t.IsZero() {
		return now()
	}
	return t
}

// eventTime converts a canonical-event protobuf timestamp to time.Time and
// maps nil/epoch-zero to the zero Time. This matters: (*Timestamp)(nil).AsTime()
// returns 1970-01-01, which is NOT time.Time.IsZero() — the old code then
// treated a missing occurred_at/received_at as "known" and wrote 1970 rows
// (found by the P9 ClickHouse archive drill: transitions landed in the 197001
// partition and were instantly TTL-deleted).
func eventTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil || (ts.GetSeconds() == 0 && ts.GetNanos() == 0) {
		return time.Time{}
	}
	return ts.AsTime()
}

// tryIdentityLock waits for the per-identity transaction lock, BOUNDED by the
// session lock_timeout (2s, AUD-006): a slow holder surfaces as a transient
// error instead of pinning a pool connection indefinitely. Waiting (rather
// than try-lock) keeps normal same-fingerprint concurrency working.
func tryIdentityLock(ctx context.Context, tx pgx.Tx, key string) error {
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return fmt.Errorf("identity lock: %w", err)
	}
	return nil
}

func severityOrNull(ev *eventv1.AlertEvent) any {
	if ev.GetSeverity() == eventv1.Severity_SEVERITY_UNSPECIFIED {
		return nil
	}
	return ev.GetSeverity().String()
}

func severityOr(ev *eventv1.AlertEvent, fallback string) string {
	if ev.GetSeverity() == eventv1.Severity_SEVERITY_UNSPECIFIED {
		return fallback
	}
	return ev.GetSeverity().String()
}

func labelsJSON(labels map[string]string) []byte {
	raw, err := json.Marshal(labels)
	if err != nil {
		return []byte("{}")
	}
	return raw
}

// enqueueNotifications resolves routing + silences INSIDE the state-change
// transaction and writes notification_outbox rows (NOTI-001/P3). Absence of
// any route or channel means no notification — recorded as no row, which the
// relay metrics surface as "unrouted".
func (s *Store) enqueueNotifications(ctx context.Context, tx pgx.Tx, ev *eventv1.AlertEvent, instanceID, fingerprint, notificationType, transitionID string) error {
	labels := ev.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	// Route policies, priority-ordered (policy_versions.spec mirrors routing.Route).
	rows, err := tx.Query(ctx, `
		SELECT version, spec FROM policy_versions
		WHERE tenant_id=$1 AND policy_type='route' AND enabled
		ORDER BY (spec->>'priority')::int ASC`, ev.GetTenantId())
	if err != nil {
		return fmt.Errorf("load routes: %w", err)
	}
	var routes []routing.Route
	for rows.Next() {
		var r routing.Route
		var spec []byte
		if err := rows.Scan(&r.Version, &spec); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal(spec, &struct {
			Priority  *int32              `json:"priority"`
			Matchers  *[]routing.Matcher  `json:"matchers"`
			Receivers *[]routing.Receiver `json:"receivers"`
		}{&r.Priority, &r.Matchers, &r.Receivers}); err != nil {
			rows.Close()
			return fmt.Errorf("route spec v%d: %w", r.Version, err)
		}
		routes = append(routes, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// Active silences (time-filtered in SQL; matcher eval in memory).
	srows, err := tx.Query(ctx, `
		SELECT id::text, matchers, starts_at, ends_at, version FROM silences
		WHERE tenant_id=$1 AND ends_at > now()`, ev.GetTenantId())
	if err != nil {
		return fmt.Errorf("load silences: %w", err)
	}
	var silences []routing.Silence
	for srows.Next() {
		var sl routing.Silence
		var matchers []byte
		if err := srows.Scan(&sl.ID, &matchers, &sl.StartsAt, &sl.EndsAt, &sl.Version); err != nil {
			srows.Close()
			return err
		}
		if err := json.Unmarshal(matchers, &sl.Matchers); err != nil {
			srows.Close()
			return fmt.Errorf("silence %s matchers: %w", sl.ID, err)
		}
		silences = append(silences, sl)
	}
	srows.Close()
	if err := srows.Err(); err != nil {
		return err
	}

	dec := routing.EvaluateForStateChange(routes, silences, labels, notificationType, time.Now())
	if len(dec.Receivers) == 0 {
		// Fallback: the tenant's default channel (no policy version).
		var channelID string
		if err := tx.QueryRow(ctx, `
			SELECT id::text FROM notification_channels
			WHERE tenant_id=$1 AND enabled AND is_default LIMIT 1`, ev.GetTenantId()).Scan(&channelID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil // unrouted: no channel, no notification
			}
			return fmt.Errorf("default channel: %w", err)
		}
		dec.Receivers = []routing.Receiver{{ChannelID: channelID, ReceiverID: "default", SendResolved: true}}
	}

	status := "pending"
	if dec.SilencedBy != "" {
		status = "suppressed"
	}
	msg := routing.RenderMessage(ev.GetAlertName(), severityOr(ev, "warning"), notificationType,
		strconv.FormatInt(dec.PolicyVersion, 10), ev.GetTenantId(), instanceID)
	for _, rcv := range dec.Receivers {
		key := routing.IdempotencyKey("alert_instance", instanceID, notificationType,
			rcv.ChannelID, rcv.ReceiverID, 0, transitionID)
		payload, _ := json.Marshal(map[string]any{
			"message":           msg,
			"notification_type": notificationType,
			"silenced_by":       dec.SilencedBy,
			"alert_name":        ev.GetAlertName(),
			"severity":          severityOr(ev, "warning"),
			"labels":            labels,
			"policy_version":    dec.PolicyVersion,
		})
		if _, err := tx.Exec(ctx, `
			INSERT INTO notification_outbox
				(id, tenant_id, subject_type, alert_instance_id, idempotency_key,
				 channel_id, receiver_id, policy_version, payload, status, authority_generation, created_mode)
			VALUES ($1,$2,'alert_instance',$3,$4,$5,$6,$7,$8,$9,
			        COALESCE((SELECT generation FROM notification_authority WHERE id=1), 0),
			        COALESCE((SELECT mode FROM notification_authority WHERE id=1), ''))
			ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
			uuid.NewString(), ev.GetTenantId(), instanceID, key,
			rcv.ChannelID, rcv.ReceiverID, dec.PolicyVersion, payload, status); err != nil {
			return fmt.Errorf("insert notification task: %w", err)
		}
	}
	return nil
}

// --- notification worker ports (NOTI-001) ---

// NotifyTask is one claimed notification_outbox row (application type).
type NotifyTask = notify.Task

// SyncNotifyAuthority reconciles the persisted notification-ownership state
// with the worker's configured mode (P8-P0-01). Semantics:
//   - same mode, already initialized: no-op (safe for every worker replica);
//   - mode changed: generation+1, effective now;
//   - first initialization after the 0009 migration (initialized=false): ALWAYS
//     opens a new generation and never adopts pre-authority tasks. The old
//     process kept its mode only in memory, so the DB cannot know whether those
//     pending rows are active-era deliveries or shadow-era observations; the
//     conservative choice is no replay (a one-off notification gap for a
//     backlog that normally drains in <1s beats a channel-wide storm).
func (s *Store) SyncNotifyAuthority(ctx context.Context, desiredMode string) (notify.Authority, error) {
	switch desiredMode {
	case notify.ModeActive, notify.ModeShadow, notify.ModeOff:
	default:
		return notify.Authority{}, fmt.Errorf("notify authority: invalid mode %q", desiredMode)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return notify.Authority{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var a notify.Authority
	var initialized bool
	err = tx.QueryRow(ctx, `
		SELECT mode, generation, effective_at, initialized
		FROM notification_authority WHERE id=1 FOR UPDATE`).
		Scan(&a.Mode, &a.Generation, &a.EffectiveAt, &initialized)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Ledger row missing (manual tampering): treat exactly like the
		// migration seed — insert uninitialized, then take the init path below.
		if _, err := tx.Exec(ctx, `
			INSERT INTO notification_authority (id, mode, generation, initialized)
			VALUES (1, 'active', 1, false) ON CONFLICT (id) DO NOTHING`); err != nil {
			return notify.Authority{}, err
		}
		if err := tx.QueryRow(ctx, `
			SELECT mode, generation, effective_at, initialized
			FROM notification_authority WHERE id=1 FOR UPDATE`).
			Scan(&a.Mode, &a.Generation, &a.EffectiveAt, &initialized); err != nil {
			return notify.Authority{}, err
		}
	case err != nil:
		return notify.Authority{}, err
	}

	if a.Mode != desiredMode || !initialized {
		a.Generation++
		if err := tx.QueryRow(ctx, `
			UPDATE notification_authority
			SET mode=$1, generation=$2, initialized=true, effective_at=now(), updated_at=now()
			WHERE id=1
			RETURNING mode, generation, effective_at`,
			desiredMode, a.Generation).Scan(&a.Mode, &a.Generation, &a.EffectiveAt); err != nil {
			return notify.Authority{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return notify.Authority{}, err
	}
	return a, nil
}

// ClaimNotifyTasks claims pending/retrying tasks with FOR UPDATE SKIP LOCKED
// plus a lease; expired leases (crashed senders) are re-claimed and their
// attempts counter keeps climbing — at-least-once with an audit trail.
// created_mode (R27) scopes the claim to tasks created while authority was
// ACTIVE: shadow-era observations and pre-ledger rows are never claimed, while
// active-era tasks interrupted by a shadow switch deliver after the return
// (REL-002 phase 4). authority_generation remains an audit marker only.
func (s *Store) ClaimNotifyTasks(ctx context.Context, limit int, lease time.Duration) ([]notify.Task, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		WITH claimed AS (
			SELECT o.id FROM notification_outbox o
			JOIN notification_channels c ON c.id = o.channel_id AND c.enabled
			WHERE ((o.status IN ('pending','retry_scheduled') AND o.next_attempt_at <= now()
			        AND (o.lease_until IS NULL OR o.lease_until < now()))
			    OR (o.status = 'sending' AND o.lease_until < now()))
			  AND o.created_mode = 'active'
			ORDER BY o.next_attempt_at
			LIMIT $1
			FOR UPDATE OF o SKIP LOCKED
		)
		UPDATE notification_outbox o
		SET status='sending', lease_owner=$2, lease_until=now()+$3::interval, attempts=attempts+1,
		    lease_token=gen_random_uuid(), updated_at=now()
		FROM claimed WHERE o.id = claimed.id
		RETURNING o.id::text, o.tenant_id::text, o.channel_id::text, o.receiver_id, o.payload, o.attempts,
			o.created_at, o.lease_token::text, o.authority_generation, o.created_mode,
			(SELECT type FROM notification_channels c WHERE c.id = o.channel_id),
			(SELECT config FROM notification_channels c WHERE c.id = o.channel_id)`,
		limit, "notify-worker", fmt.Sprintf("%d seconds", int(lease.Seconds())))
	if err != nil {
		return nil, err
	}
	var out []notify.Task
	for rows.Next() {
		var t notify.Task
		if err := rows.Scan(&t.ID, &t.TenantID, &t.ChannelID, &t.ReceiverID, &t.Payload, &t.Attempts, &t.CreatedAt, &t.LeaseToken, &t.AuthorityGeneration, &t.CreatedMode, &t.ChannelType, &t.ChannelCfg); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, tx.Commit(ctx)
}

// RenewNotifyLease extends the current owner's lease right before a send
// (P8-P1-04). Conditional on the lease token: a task re-claimed by another
// worker returns ErrStaleNotifyLease and the caller must skip the send.
func (s *Store) RenewNotifyLease(ctx context.Context, id, leaseToken string, lease time.Duration) error {
	var token any
	if leaseToken != "" {
		u, perr := uuid.Parse(leaseToken)
		if perr != nil {
			return fmt.Errorf("notify renew: invalid lease token %q: %w", leaseToken, perr)
		}
		token = u
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE notification_outbox
		SET lease_until = now() + ($3::bigint * interval '1 second'), updated_at = now()
		WHERE id=$1 AND ($2::uuid IS NULL OR lease_token=$2::uuid) AND status='sending'`,
		id, token, int64(lease.Seconds()))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 && leaseToken != "" {
		return notify.ErrStaleNotifyLease
	}
	return nil
}

// FinishNotifyTask records the delivery outcome; retryAfter>0 schedules the
// next attempt (429 Retry-After), otherwise exponential backoff applies.
// FinishNotifyTask records one attempt outcome, fenced by the claim's lease
// token (P1-02): the state update is conditional on ownership — a stale
// worker whose lease expired (task re-claimed elsewhere) only gets its attempt
// audited and receives ErrStaleNotifyLease, never overwriting the owner.
// An empty token (rows claimed by a pre-0008 binary mid-rollout) keeps the
// legacy by-id behavior.
func (s *Store) FinishNotifyTask(ctx context.Context, id, leaseToken string, sent bool, retryAfter time.Duration, attemptNo int, httpStatus int, failureCode, errMsg string, maxAttempts int) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO notification_attempts (id, tenant_id, task_id, attempt_no, http_status_code, failure_code, error_message)
		VALUES ($1, (SELECT tenant_id FROM notification_outbox WHERE id=$2), $2, $3, $4, $5, $6)`,
		uuid.NewString(), id, attemptNo, nullInt(httpStatus), nullStr(failureCode), nullStr(errMsg)); err != nil {
		return err
	}
	// Ownership-conditional update: only the current lease holder moves the
	// state machine; the attempt row above persists regardless (audit).
	var token any
	if leaseToken != "" {
		u, perr := uuid.Parse(leaseToken)
		if perr != nil {
			return fmt.Errorf("notify finish: invalid lease token %q: %w", leaseToken, perr)
		}
		token = u
	}
	var tag pgconn.CommandTag
	if sent {
		tag, err = tx.Exec(ctx, `
			UPDATE notification_outbox SET status='sent', lease_until=NULL, lease_token=NULL, updated_at=now()
			WHERE id=$1 AND ($2::uuid IS NULL OR lease_token=$2::uuid)`, id, token)
	} else if failureCode == "permanent" || failureCode == "ssrf" ||
		(maxAttempts > 0 && attemptNo >= maxAttempts) {
		// AUD-002: terminal outcomes — explicit permanent rejection, SSRF
		// (config error, retry cannot help), or attempts exhausted. Without
		// this a broken channel retried forever (attempts climbing, pending
		// gauge never draining).
		tag, err = tx.Exec(ctx, `
			UPDATE notification_outbox SET status='failed', lease_until=NULL, lease_token=NULL, updated_at=now()
			WHERE id=$1 AND ($2::uuid IS NULL OR lease_token=$2::uuid)`, id, token)
	} else {
		tag, err = tx.Exec(ctx, `
			UPDATE notification_outbox SET status='retry_scheduled',
				next_attempt_at = CASE WHEN $2::bigint > 0 THEN now()+($2||' seconds')::interval
				                       ELSE now() + least(power(2, attempts), 600) * interval '1 second' END,
				lease_until=NULL, updated_at=now()
			WHERE id=$1 AND ($3::uuid IS NULL OR lease_token=$3::uuid)`, id, int64(retryAfter.Seconds()), token)
	}
	if err != nil {
		return err
	}
	stale := tag.RowsAffected() == 0 && leaseToken != ""
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if stale {
		return notify.ErrStaleNotifyLease
	}
	return nil
}

// SuppressNotifyAtSend re-checks silences before the actual HTTP call
// (docs/04 §5: 发送前再次校验). Returns true when the task got suppressed.
func (s *Store) SuppressNotifyAtSend(ctx context.Context, taskID string, labels map[string]string) (bool, error) {
	// P0-06: load ALL time-active candidates and suppress when ANY matches —
	// the old LIMIT 1 returned the first arbitrary row and missed rules behind
	// a non-matching one; the window now enforces BOTH bounds (a future,
	// not-yet-started silence must never suppress). Matcher semantics are the
	// same routing.MatchAll used at enqueue time.
	rows, err := s.pool.Query(ctx, `
		SELECT sl.id::text, sl.matchers, sl.starts_at, sl.ends_at, sl.version
		FROM notification_outbox o
		JOIN silences sl ON sl.tenant_id = o.tenant_id
		WHERE o.id = $1 AND sl.starts_at <= now() AND sl.ends_at > now()`, taskID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var sl routing.Silence
		var matchers []byte
		if err := rows.Scan(&sl.ID, &matchers, &sl.StartsAt, &sl.EndsAt, &sl.Version); err != nil {
			return false, err
		}
		if err := json.Unmarshal(matchers, &sl.Matchers); err != nil {
			return false, err
		}
		if routing.MatchAll(sl.Matchers, labels) {
			_, err := s.pool.Exec(ctx, `
				UPDATE notification_outbox SET status='suppressed', lease_until=NULL, updated_at=now() WHERE id=$1`, taskID)
			return true, err
		}
	}
	return false, rows.Err()
}

func nullInt(i int) any {
	if i == 0 {
		return nil
	}
	return i
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// CleanupReceipts implements retention.Store (RETN-001): delete receipts
// whose last_seen_at precedes the cutoff, keep everything newer (the kept
// count is the in-window metric).
func (s *Store) CleanupReceipts(ctx context.Context, cutoff time.Time) (int64, int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM event_receipts WHERE last_seen_at < $1`, cutoff)
	if err != nil {
		return 0, 0, err
	}
	var retained int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM event_receipts`).Scan(&retained); err != nil {
		return tag.RowsAffected(), 0, err
	}
	return tag.RowsAffected(), retained, nil
}

// ReportEdgeStatus upserts an edge waterline heartbeat (MC-002).
func (s *Store) ReportEdgeStatus(ctx context.Context, tenantID, sourceID string, walBytes, walPending int64) error {
	// AUD-009: the (tenant, source) pair MUST be a registered source — the
	// heartbeat used to upsert any caller-provided identity under a shared
	// key, letting one edge zero another tenant's waterline.
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO edge_status (source_id, tenant_id, wal_bytes, wal_pending, updated_at)
		SELECT $1::uuid, $2::uuid, $3, $4, now()
		WHERE EXISTS (SELECT 1 FROM alert_sources s
		              WHERE s.tenant_id=$2::uuid AND s.source_id=$1::uuid)
		ON CONFLICT (source_id) DO UPDATE
		SET wal_bytes=EXCLUDED.wal_bytes, wal_pending=EXCLUDED.wal_pending, updated_at=now()`,
		sourceID, tenantID, walBytes, walPending)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ops.ErrSourceNotOwned
	}
	return nil
}

// ListEdgeStatus returns the tenant's edge waterlines (ops.Store).
func (s *Store) ListEdgeStatus(ctx context.Context, tenantID string) ([]ops.EdgeStatus, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT source_id::text, wal_bytes, wal_pending, updated_at FROM edge_status
		WHERE tenant_id=$1 ORDER BY updated_at DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ops.EdgeStatus
	for rows.Next() {
		var r ops.EdgeStatus
		if err := rows.Scan(&r.SourceID, &r.WalBytes, &r.WalPending, &r.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListNotificationTasks returns delivery records for the console (UI-003:
// 发送记录/失败过滤), tenant-scoped with an optional status filter.
func (s *Store) ListNotificationTasks(ctx context.Context, tenantID, status string, limit int) ([]ops.NotificationTask, error) {
	where := "tenant_id=$1"
	args := []any{tenantID}
	if status != "" {
		args = append(args, status)
		where += " AND status=$" + strconv.Itoa(len(args))
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	args = append(args, limit)
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, channel_id::text, receiver_id, status, attempts,
		       COALESCE(payload->>'notification_type',''), created_at, next_attempt_at,
		       COALESCE(payload->>'message','')
		FROM notification_outbox WHERE `+where+`
		ORDER BY created_at DESC LIMIT $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ops.NotificationTask
	for rows.Next() {
		var r ops.NotificationTask
		if err := rows.Scan(&r.ID, &r.ChannelID, &r.Receiver, &r.Status, &r.Attempts,
			&r.Type, &r.CreatedAt, &r.NextAttemptAt, &r.Message); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CleanupNotifications implements retention for delivered notifications and
// their attempt rows (RETN-002). Suppressed/failed rows follow the same
// window — audit lives in audit_logs, not the delivery queue.
func (s *Store) CleanupNotifications(ctx context.Context, olderThan time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM notification_outbox WHERE updated_at < $1 AND status IN ('sent','suppressed')`, olderThan)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// CleanupAudit removes audit rows past their retention (RETN-002); queries
// beyond the window return an explicit "outside retention" answer, not an error.
func (s *Store) CleanupAudit(ctx context.Context, olderThan time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM audit_logs WHERE created_at < $1`, olderThan)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// --- INC-002 auto-correlation + INC-005 escalation (P5) ---

// RecordUIDHistory writes the identity interval row on instance creation
// (INC-004: T-015 time-window identity).
func recordUIDHistory(ctx context.Context, tx pgx.Tx, ev *eventv1.AlertEvent) error {
	if ev.GetResource().GetResourceUid() == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO uid_history (tenant_id, cluster_id, entity_type, entity_uid, display_name, valid_from)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (tenant_id, cluster_id, entity_type, entity_uid, valid_from) DO NOTHING`,
		ev.GetTenantId(), ev.GetResource().GetClusterId(), ev.GetResource().GetResourceType(),
		ev.GetResource().GetResourceUid(), ev.GetResource().GetResourceName(),
		orNow(eventTime(ev.GetStartsAt()), time.Now))
	return err
}

// UnlinkedFiringAlerts returns recent firing instances that belong to no
// incident — the correlation candidates (INC-002 scan input).
func (s *Store) UnlinkedFiringAlerts(ctx context.Context, tenantID string, within time.Duration) ([]correlation.Alert, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT i.id::text, i.tenant_id::text, i.cluster_id, i.entity_type, i.entity_uid,
		       COALESCE(i.entity_uid,''), COALESCE(i.name,''), i.starts_at
		FROM alert_instances i
		WHERE i.tenant_id=$1 AND i.monitor_state='firing'
		  AND i.starts_at > now() - $2::interval
		  -- A candidate is an alert whose ONLY incident is its own solo 1:1
		  -- (no other alert shares that incident): merging moves it into the
		  -- anchor's incident without stranding anyone.
		  AND NOT EXISTS (
		    SELECT 1 FROM incident_alerts ia
		    JOIN incident_alerts ib ON ia.incident_id = ib.incident_id
		    WHERE ia.alert_instance_id = i.id AND ib.alert_instance_id <> i.id
		  )
		ORDER BY i.starts_at`, tenantID, secondsOf(within))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []correlation.Alert
	for rows.Next() {
		var a correlation.Alert
		if err := rows.Scan(&a.InstanceID, &a.TenantID, &a.ClusterID, &a.EntityType, &a.EntityUID,
			&a.EntityUID, &a.DisplayName, &a.StartedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// FindAnchor returns one incident-backed alert with the same L3 identity in
// the window (the auto-merge target), newest first.
func (s *Store) FindAnchor(ctx context.Context, cand correlation.Alert) (*correlation.Alert, string, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT i.id::text, i.tenant_id::text, i.cluster_id, i.entity_type, i.entity_uid,
		       COALESCE(i.entity_uid,''), COALESCE(i.name,''), i.starts_at, ia.incident_id::text
		FROM alert_instances i
		JOIN incident_alerts ia ON ia.alert_instance_id = i.id AND ia.tenant_id = i.tenant_id
		WHERE i.tenant_id=$1 AND i.monitor_state='firing'
		  AND i.cluster_id=$2 AND i.entity_type=$3 AND i.entity_uid=$4
		  AND i.id::text <> $6
		  AND i.starts_at <= $5::timestamptz
		  AND i.starts_at BETWEEN ($5::timestamptz - interval '10 minutes') AND ($5::timestamptz + interval '10 minutes')
		ORDER BY i.starts_at DESC, ia.incident_id DESC LIMIT 1`,
		cand.TenantID, cand.ClusterID, cand.EntityType, cand.EntityUID, cand.StartedAt, cand.InstanceID)
	var a correlation.Alert
	var incidentID string
	err := row.Scan(&a.InstanceID, &a.TenantID, &a.ClusterID, &a.EntityType, &a.EntityUID,
		&a.EntityUID, &a.DisplayName, &a.StartedAt, &incidentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	return &a, incidentID, nil
}

// AutoLink correlates an alert into the anchor's incident (relation_type=
// 'auto_correlated') with the evidence recorded in the audit trail, and
// RETIRES the alert's own solo 1:1 incident (merge semantics: one incident
// per correlated blast, not stranding empty shells) (INC-002).
func (s *Store) AutoLink(ctx context.Context, tenantID, incidentID, instanceID string, evidence []correlation.Evidence) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		DELETE FROM incident_alerts WHERE tenant_id=$1 AND alert_instance_id=$2 AND relation_type='derived_1to1'`,
		tenantID, instanceID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE incidents SET status='closed', close_reason='auto_merged', version=version+1, updated_at=now()
		WHERE tenant_id=$1 AND id NOT IN (SELECT incident_id FROM incident_alerts) AND status='open'`,
		tenantID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO incident_alerts (tenant_id, incident_id, alert_instance_id, relation_type)
		VALUES ($1,$2,$3,'auto_correlated')
		ON CONFLICT (incident_id, alert_instance_id) DO NOTHING`,
		tenantID, incidentID, instanceID); err != nil {
		return err
	}
	raw, _ := json.Marshal(map[string]any{"evidence": evidence, "instance_id": instanceID})
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_logs (id, tenant_id, actor_id, operation, object_type, object_id, details)
		VALUES ($1,$2,'system','incident.auto_correlate','incident',$3,$4::jsonb)`,
		uuid.NewString(), tenantID, incidentID, raw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// --- INC-005 escalation ---

// acquireComponentLease renews (or takes over) a named single-writer lease.
// False means another live owner holds it.
func (s *Store) acquireComponentLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error) {
	var got string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO component_leases (name, owner, expires_at)
		VALUES ($1, $2, now() + $3::bigint * interval '1 second')
		ON CONFLICT (name) DO UPDATE
		SET owner=EXCLUDED.owner, expires_at=EXCLUDED.expires_at, updated_at=now()
		WHERE component_leases.owner = $2::uuid OR component_leases.expires_at < now()
		RETURNING owner::text`, name, owner, int64(ttl.Seconds())).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return got == owner, nil
}

func (s *Store) releaseComponentLease(ctx context.Context, name, owner string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM component_leases WHERE name=$1 AND owner=$2::uuid`, name, owner)
	return err
}

// AcquireLease renews the escalation/correlation single-flight lease
// (AUD-008): even with N worker replicas, only one runs the scan per tick.
func (s *Store) AcquireLease(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	return s.acquireComponentLease(ctx, "escalation.correlate", owner, ttl)
}

// ReleaseLease drops the escalation single-flight lease.
func (s *Store) ReleaseLease(ctx context.Context, owner string) error {
	return s.releaseComponentLease(ctx, "escalation.correlate", owner)
}

// AcquireOutboxLease renews the relay's single-writer lease (AUD-011): one
// replica publishes at a time, so per-key created_at order is preserved.
func (s *Store) AcquireOutboxLease(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	return s.acquireComponentLease(ctx, "outbox.relay", owner, ttl)
}

// ReleaseOutboxLease drops the relay lease.
func (s *Store) ReleaseOutboxLease(ctx context.Context, owner string) error {
	return s.releaseComponentLease(ctx, "outbox.relay", owner)
}

// EscalatableIncidents returns open incidents whose last escalation (or
// creation) is older than every — the 5-minute un-ACK ladder.
func (s *Store) EscalatableIncidents(ctx context.Context, every time.Duration) ([]escalation.EscalationCandidate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT i.id::text, i.tenant_id::text, i.created_at,
		       COALESCE((SELECT max(e.stage) FROM incident_escalations e WHERE e.incident_id=i.id), 0),
		       COALESCE((SELECT a.alert_instance_id::text FROM incident_alerts a WHERE a.incident_id=i.id LIMIT 1), '')
		FROM incidents i
		WHERE i.status='open'
		  AND i.created_at < now() - $1::interval
		  AND COALESCE((SELECT max(e.notified_at) FROM incident_escalations e WHERE e.incident_id=i.id), i.created_at) < now() - $1::interval
		ORDER BY i.created_at
		LIMIT 200`,
		secondsOf(every))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []escalation.EscalationCandidate
	for rows.Next() {
		var c escalation.EscalationCandidate
		if err := rows.Scan(&c.IncidentID, &c.TenantID, &c.CreatedAt, &c.CurrentStage, &c.InstanceID); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// EscalationCandidate is one incident due for the next stage.
type EscalationCandidate struct {
	IncidentID   string
	TenantID     string
	CreatedAt    time.Time
	CurrentStage int
	InstanceID   string // anchor alert for routing/labels
}

// RecordEscalation writes the stage row and enqueues the escalation
// notification (subject=incident, escalation_stage in the idempotency key).
func (s *Store) RecordEscalation(ctx context.Context, cand escalation.EscalationCandidate, policyVersion int64) error {
	stage := cand.CurrentStage + 1
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// AUD-008: UNIQUE(incident_id, stage) + ON CONFLICT DO NOTHING — two
	// replicas scanning the same candidate must not page twice or duplicate
	// the audit row.
	tag, err := tx.Exec(ctx, `
		INSERT INTO incident_escalations (id, tenant_id, incident_id, stage, policy_version)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (incident_id, stage) DO NOTHING`,
		uuid.NewString(), cand.TenantID, cand.IncidentID, stage, policyVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// Another replica already escalated this stage: no page, stay idempotent.
		return tx.Commit(ctx)
	}
	// Notification: same transaction (docs/04 §2), incident-subject keyspace.
	var channelID string
	if err := tx.QueryRow(ctx, `
		SELECT id::text FROM notification_channels
		WHERE tenant_id=$1 AND enabled AND is_default LIMIT 1`, cand.TenantID).Scan(&channelID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tx.Commit(ctx) // no channel: escalation recorded, no page
		}
		return err
	}
	key := routing.IdempotencyKey("incident", cand.IncidentID, "escalation", channelID, "oncall", stage, cand.IncidentID)
	msg := routing.RenderMessage("incident escalation", "warning", fmt.Sprintf("stage-%d", stage),
		strconv.FormatInt(policyVersion, 10), cand.TenantID, cand.IncidentID)
	payload, _ := json.Marshal(map[string]any{
		"message": msg, "notification_type": "escalation", "stage": stage,
		"incident_id": cand.IncidentID, "policy_version": policyVersion,
	})
	if _, err := tx.Exec(ctx, `
		INSERT INTO notification_outbox
			(id, tenant_id, subject_type, incident_id, idempotency_key, channel_id,
			 receiver_id, policy_version, payload, status, authority_generation, created_mode)
		VALUES ($1,$2,'incident',$3,$4,$5,'oncall',$6,$7,'pending',
		        COALESCE((SELECT generation FROM notification_authority WHERE id=1), 0),
			        COALESCE((SELECT mode FROM notification_authority WHERE id=1), ''))
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
		uuid.NewString(), cand.TenantID, cand.IncidentID, key, channelID,
		policyVersion, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func secondsOf(d time.Duration) string {
	sec := int64(d.Seconds())
	if sec < 1 {
		sec = 1
	}
	return strconv.FormatInt(sec, 10)
}

// CorrelateOnce runs one INC-002 auto-merge pass over the tenant's unlinked
// firing alerts: L3 identity + time window -> merge into the anchor incident
// with recorded evidence; weaker evidence is left for the workbench.
func (s *Store) CorrelateOnce(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT tenant_id::text FROM alert_instances WHERE monitor_state='firing'`)
	if err != nil {
		return 0, err
	}
	var tenants []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return 0, err
		}
		tenants = append(tenants, t)
	}
	rows.Close()
	merged := 0
	for _, tenant := range tenants {
		cands, err := s.UnlinkedFiringAlerts(ctx, tenant, time.Hour)
		if err != nil {
			return merged, err
		}
		for _, cand := range cands {
			anchor, incidentID, err := s.FindAnchor(ctx, cand)
			if err != nil {
				return merged, err
			}
			if anchor == nil {
				continue
			}
			dec := correlation.SameTenantL3(cand, *anchor)
			if !dec.Merge {
				continue // suggestion-level evidence waits for a human (docs/13 INC-002)
			}
			if err := s.AutoLink(ctx, tenant, incidentID, cand.InstanceID, dec.Evidence); err != nil {
				return merged, err
			}
			merged++
		}
	}
	return merged, nil
}

// CountPendingNotifyTasks reports the current-era pending/retry depth
// (P9-OBS-04 gauge; sampled every 30s by the notify loop, not per tick).
func (s *Store) CountPendingNotifyTasks(ctx context.Context) (int64, error) {
	var n int64
	// AUD-002: only tasks the claim can actually deliver — disabled/missing
	// channels are excluded (the sweep dead-letters them separately).
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM notification_outbox o
		WHERE o.status IN ('pending','retry_scheduled') AND o.created_mode='active'
		  AND EXISTS (SELECT 1 FROM notification_channels c WHERE c.id=o.channel_id AND c.enabled)`).Scan(&n)
	return n, err
}

// FailOrphanNotifyTasks dead-letters pending/retry tasks whose channel is
// missing or disabled (AUD-002): the claim joins enabled channels, so these
// rows could never be delivered yet sat pending forever.
func (s *Store) FailOrphanNotifyTasks(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE notification_outbox o SET status='failed', lease_until=NULL, lease_token=NULL, updated_at=now()
		WHERE o.status IN ('pending','retry_scheduled')
		  AND NOT EXISTS (SELECT 1 FROM notification_channels c WHERE c.id=o.channel_id AND c.enabled)
		  AND o.updated_at < now() - interval '1 minute'`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ReplayNotifyTask resets a dead-lettered task for one more delivery attempt
// (AUD-002 manual replay). Tenant-scoped; returns found=true when reset.
func (s *Store) ReplayNotifyTask(ctx context.Context, tenantID, taskID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE notification_outbox
		SET status='pending', attempts=0, next_attempt_at=now(),
		    lease_until=NULL, lease_token=NULL, updated_at=now()
		WHERE id=$1 AND tenant_id=$2 AND status='failed'`, taskID, tenantID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// PeekNotifyTasks lists pending/retry tasks without claiming (notify shadow
// mode). AUD-012: the would-have-sent trail covers ALL pending work — the
// active-era backlog that existed before an active→shadow switch included —
// because those tasks WOULD be sent if authority were active now.
func (s *Store) PeekNotifyTasks(ctx context.Context, limit int) ([]notify.Task, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT o.id::text, o.tenant_id::text, o.channel_id::text, o.receiver_id, o.payload, o.attempts,
		       c.type, c.config
		FROM notification_outbox o
		JOIN notification_channels c ON c.id = o.channel_id AND c.enabled
		WHERE o.status IN ('pending','retry_scheduled') AND o.next_attempt_at <= now()
		ORDER BY o.next_attempt_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []notify.Task
	for rows.Next() {
		var t notify.Task
		if err := rows.Scan(&t.ID, &t.TenantID, &t.ChannelID, &t.ReceiverID, &t.Payload, &t.Attempts,
			&t.ChannelType, &t.ChannelCfg); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// RecordShadowDelivery writes one audited would-have-sent record; idempotent
// per task (the audit row IS the dedup ledger, same pattern as incident.ack).
func (s *Store) RecordShadowDelivery(ctx context.Context, taskID string) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var tenant string
	if err := tx.QueryRow(ctx, `SELECT tenant_id::text FROM notification_outbox WHERE id=$1`, taskID).Scan(&tenant); err != nil {
		return false, err
	}
	raw, _ := json.Marshal(map[string]any{"shadow_task": taskID})
	// AUD-012: INSERT ... ON CONFLICT DO NOTHING on the partial unique index
	// uq_audit_shadow_task (0015) — two shadow replicas racing the same task
	// can no longer double-write the trail.
	tag, err := tx.Exec(ctx, `
		INSERT INTO audit_logs (id, tenant_id, actor_id, operation, object_type, object_id, details)
		VALUES ($1,$2,'system','notify.shadow_would_send','notification',$3,$4::jsonb)
		ON CONFLICT ((details->>'shadow_task')) WHERE operation='notify.shadow_would_send' DO NOTHING`,
		uuid.NewString(), tenant, taskID, raw)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
