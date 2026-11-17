package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/alert-platform/internal/application/history"
	"github.com/example/alert-platform/internal/application/query"
	"github.com/google/uuid"
)

// QueryStore implements query.Store (PROC-007 read side).
type QueryStore struct {
	pool *pgxpool.Pool
}

// NewQueryStore builds the read-side store.
func NewQueryStore(pool *pgxpool.Pool) *QueryStore { return &QueryStore{pool: pool} }

const alertCols = `id::text, tenant_id::text, source_id, fingerprint, episode_key, name,
	severity, monitor_state, cluster_id, entity_type, entity_uid, labels,
	starts_at, ends_at, last_observed_at, last_transition_at, version`

func scanAlert(row pgx.Row) (*query.Alert, error) {
	var a query.Alert
	var labels []byte
	var startsAt, endsAt *time.Time
	if err := row.Scan(&a.InstanceID, &a.TenantID, &a.SourceID, &a.Fingerprint, &a.EpisodeKey,
		&a.Name, &a.Severity, &a.MonitorState, &a.ClusterID, &a.EntityType, &a.EntityUID,
		&labels, &startsAt, &endsAt, &a.LastObservedAt, &a.LastTransitionAt, &a.Version); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, query.ErrNotFound
		}
		return nil, err
	}
	if len(labels) > 0 {
		_ = json.Unmarshal(labels, &a.Labels)
	}
	if startsAt != nil {
		a.StartsAt = *startsAt
	}
	if endsAt != nil {
		a.EndsAt = *endsAt
	}
	return &a, nil
}

// offsetToken is the opaque page token: plain offset, validated on decode.
func offsetToken(tok string) int {
	if tok == "" {
		return 0
	}
	n, err := strconv.Atoi(tok)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// ListAlerts: tenant + optional filters, ordered most recently updated first.
func (s *QueryStore) ListAlerts(ctx context.Context, f query.AlertFilter) (query.Page[query.Alert], error) {
	where := "tenant_id=$1"
	args := []any{f.TenantID}
	eq := func(column string, val any) { // AND <column> = $n (skipped when empty)
		if v, ok := val.(string); !ok || v != "" {
			args = append(args, val)
			where += fmt.Sprintf(" AND %s = $%d", column, len(args))
		}
	}
	switch f.State {
	case "":
		where += " AND monitor_state IN ('pending','firing','stale')"
	default:
		eq("monitor_state", f.State)
	}
	eq("severity", f.Severity)
	eq("source_id", f.SourceID)
	eq("cluster_id", f.ClusterID)
	if f.Name != "" {
		args = append(args, f.Name+"%")
		where += " AND name LIKE $" + strconv.Itoa(len(args))
	}

	var total int64
	if err := s.pool.QueryRow(ctx,
		"SELECT count(*) FROM alert_instances WHERE "+where, args...).Scan(&total); err != nil {
		return query.Page[query.Alert]{}, err
	}

	offset := offsetToken(f.PageToken)
	args = append(args, f.PageSize, offset)
	rows, err := s.pool.Query(ctx, `
		SELECT `+alertCols+` FROM alert_instances
		WHERE `+where+`
		ORDER BY updated_at DESC, id
		LIMIT $`+strconv.Itoa(len(args)-1)+` OFFSET $`+strconv.Itoa(len(args)),
		args...)
	if err != nil {
		return query.Page[query.Alert]{}, err
	}
	defer rows.Close()
	page := query.Page[query.Alert]{TotalEstimate: total}
	for rows.Next() {
		a, err := scanAlert(rows)
		if err != nil {
			return query.Page[query.Alert]{}, err
		}
		page.Items = append(page.Items, *a)
	}
	if err := rows.Err(); err != nil {
		return query.Page[query.Alert]{}, err
	}
	if offset+len(page.Items) < int(total) {
		page.NextPageToken = strconv.Itoa(offset + len(page.Items))
	}
	return page, nil
}

// GetAlertByID: fetched unfiltered; the service enforces tenant ownership.
func (s *QueryStore) GetAlertByID(ctx context.Context, instanceID string) (*query.Alert, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+alertCols+` FROM alert_instances WHERE id=$1::uuid`, instanceID)
	return scanAlert(row)
}

// ListTransitions: newest first within the tenant + instance scope.
func (s *QueryStore) ListTransitions(ctx context.Context, tenantID, instanceID string, pageSize int, pageToken string) (query.Page[query.Transition], error) {
	// AUD-010: bind the uuid parameter directly — `alert_instance_id::text=$2`
	// is an expression predicate and defeated idx_transitions_instance.
	if _, err := uuid.Parse(instanceID); err != nil {
		return query.Page[query.Transition]{}, query.ErrNotFound
	}
	offset := offsetToken(pageToken)
	var total int64
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM alert_transitions WHERE tenant_id=$1 AND alert_instance_id=$2::uuid`,
		tenantID, instanceID).Scan(&total); err != nil {
		return query.Page[query.Transition]{}, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT transition_id::text, alert_instance_id::text, from_state, to_state, event_id, transition_at
		FROM alert_transitions
		WHERE tenant_id=$1 AND alert_instance_id=$2::uuid
		ORDER BY transition_at DESC, transition_id
		LIMIT $3 OFFSET $4`, tenantID, instanceID, pageSize, offset)
	if err != nil {
		return query.Page[query.Transition]{}, err
	}
	defer rows.Close()
	page := query.Page[query.Transition]{TotalEstimate: total}
	for rows.Next() {
		var t query.Transition
		var from *string
		if err := rows.Scan(&t.TransitionID, &t.InstanceID, &from, &t.ToState, &t.EventID, &t.TransitionAt); err != nil {
			return query.Page[query.Transition]{}, err
		}
		if from != nil {
			t.FromState = *from
		}
		page.Items = append(page.Items, t)
	}
	if err := rows.Err(); err != nil {
		return query.Page[query.Transition]{}, err
	}
	if offset+len(page.Items) < int(total) {
		page.NextPageToken = strconv.Itoa(offset + len(page.Items))
	}
	return page, nil
}

// ListTransitionsRange implements history.Store over the PG window.
func (s *QueryStore) ListTransitionsRange(ctx context.Context, tenantID string, from, to time.Time, limit int) ([]history.Transition, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT transition_id::text, alert_instance_id::text, COALESCE(from_state,''), to_state, event_id, transition_at
		FROM alert_transitions
		WHERE tenant_id=$1 AND transition_at >= $2 AND transition_at < $3
		ORDER BY transition_at DESC LIMIT $4`, tenantID, from, to, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []history.Transition
	for rows.Next() {
		var t history.Transition
		if err := rows.Scan(&t.TransitionID, &t.InstanceID, &t.FromState, &t.ToState, &t.EventID, &t.At); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ListEvents (P0-05) reads the event history, newest first, tenant-scoped.
func (s *QueryStore) ListEvents(ctx context.Context, f query.EventFilter) ([]query.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, transport_id, kind, signal, COALESCE(severity,''), alert_name,
			COALESCE(cluster_id,''), COALESCE(entity_type,''), COALESCE(entity_uid,''),
			COALESCE(namespace,''), labels, occurred_at
		FROM event_history
		WHERE tenant_id=$1
		  AND ($2::timestamptz IS NULL OR occurred_at >= $2)
		  AND ($3::timestamptz IS NULL OR occurred_at < $3)
		ORDER BY occurred_at DESC
		LIMIT $4`, f.TenantID, nullTime(f.From), nullTime(f.To), f.Limit)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	var out []query.Event
	for rows.Next() {
		var e query.Event
		var labels []byte
		if err := rows.Scan(&e.EventID, &e.TransportID, &e.Kind, &e.Signal, &e.Severity, &e.AlertName,
			&e.ClusterID, &e.EntityType, &e.EntityUID, &e.Namespace, &labels, &e.OccurredAt); err != nil {
			return nil, err
		}
		if len(labels) > 0 {
			_ = json.Unmarshal(labels, &e.Labels)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
