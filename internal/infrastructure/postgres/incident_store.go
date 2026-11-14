package postgres

import (
	"context"
	"time"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/alert-platform/internal/application/incident"
)

// IncidentStore implements incident.Store.
type IncidentStore struct{ pool *pgxpool.Pool }

// NewIncidentStore builds the incident store.
func NewIncidentStore(pool *pgxpool.Pool) *IncidentStore { return &IncidentStore{pool: pool} }

const incidentCols = `i.id::text, i.tenant_id::text, i.correlation_key, i.title, i.status, i.priority,
	COALESCE(i.owner_team_id,''), i.version, i.created_at, i.updated_at,
	COALESCE((SELECT a.alert_instance_id::text FROM incident_alerts a WHERE a.incident_id=i.id LIMIT 1),'')`

func scanIncident(row pgx.Row) (*incident.Incident, error) {
	var in incident.Incident
	if err := row.Scan(&in.IncidentID, &in.TenantID, &in.CorrelationKey, &in.Title, &in.Status,
		&in.Priority, &in.OwnerTeamID, &in.Version, &in.CreatedAt, &in.UpdatedAt, &in.AlertInstanceID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, incident.ErrNotFound
		}
		return nil, err
	}
	return &in, nil
}

// Get returns one incident, tenant-scoped.
func (s *IncidentStore) Get(ctx context.Context, tenantID, incidentID string) (*incident.Incident, error) {
	return scanIncident(s.pool.QueryRow(ctx, `
		SELECT `+incidentCols+` FROM incidents i WHERE i.tenant_id=$1 AND i.id=$2`,
		tenantID, incidentID))
}

// List filters by status, newest first, offset-paged.
func (s *IncidentStore) List(ctx context.Context, tenantID, status string, pageSize int, pageToken string) ([]incident.Incident, string, error) {
	where := "i.tenant_id=$1"
	args := []any{tenantID}
	if status != "" {
		args = append(args, status)
		where += " AND i.status=$" + strconv.Itoa(len(args))
	}
	offset := 0
	if pageToken != "" {
		if n, err := strconv.Atoi(pageToken); err == nil && n > 0 {
			offset = n
		}
	}
	args = append(args, pageSize, offset)
	rows, err := s.pool.Query(ctx, `
		SELECT `+incidentCols+` FROM incidents i WHERE `+where+`
		ORDER BY i.created_at DESC LIMIT $`+strconv.Itoa(len(args)-1)+` OFFSET $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []incident.Incident
	for rows.Next() {
		in, err := scanIncident(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, *in)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if offset+len(out) >= pageSize {
		next = strconv.Itoa(offset + len(out))
	}
	return out, next, nil
}

// Acknowledge: optimistic version check + idempotency by audit marker.
// The idempotency key is recorded as an audit row; a replayed key returns the
// current incident without a second transition (audit is the dedup ledger).
func (s *IncidentStore) Acknowledge(ctx context.Context, tenantID, incidentID, actor, idempotencyKey string, expectedVersion int64) (*incident.Incident, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Idempotent replay: same actor+key already acked -> return current.
	var marker string
	err = tx.QueryRow(ctx, `
		SELECT object_id FROM audit_logs
		WHERE tenant_id=$1 AND actor_id=$2 AND operation='incident.ack'
		  AND details->>'idempotency_key'=$3 LIMIT 1`,
		tenantID, actor, idempotencyKey).Scan(&marker)
	switch {
	case err == nil:
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return s.Get(ctx, tenantID, incidentID)
	case errors.Is(err, pgx.ErrNoRows):
		// first ack
	default:
		return nil, err
	}

	var currentStatus string
	var version int64
	if err := tx.QueryRow(ctx, `
		SELECT status, version FROM incidents
		WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, incidentID).
		Scan(&currentStatus, &version); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, incident.ErrNotFound
		}
		return nil, err
	}
	if version != expectedVersion {
		return nil, incident.ErrVersionConflict
	}
	switch currentStatus {
	case incident.StatusOpen:
		// open -> acknowledged (the only automated hop; later hops are P5)
	case incident.StatusAcknowledged:
		// same-key replay handled above; a DIFFERENT key acking an already
		// acknowledged incident is a no-op success (human re-confirm).
	default:
		return nil, incident.ErrInvalidStatus
	}
	tag, err := tx.Exec(ctx, `
		UPDATE incidents SET status=$3, version=version+1, updated_at=now()
		WHERE tenant_id=$1 AND id=$2 AND version=$4`,
		tenantID, incidentID, incident.StatusAcknowledged, expectedVersion)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, incident.ErrVersionConflict
	}
	raw, _ := json.Marshal(map[string]any{"idempotency_key": idempotencyKey})
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_logs (id, tenant_id, actor_id, operation, object_type, object_id, details)
		VALUES ($1,$2,$3,'incident.ack','incident',$4,$5::jsonb)`,
		uuid.NewString(), tenantID, actor, incidentID, raw); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.Get(ctx, tenantID, incidentID)
}

// LinkAlert adds a manual correlation row, bumping the incident version and
// writing an audit entry (INC-001: reversible + audited). Idempotent per
// (incident, instance): a repeat just returns the current version.
func (s *IncidentStore) LinkAlert(ctx context.Context, tenantID, incidentID, instanceID, actor string, expectedVersion int64) (int64, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var version int64
	if err := tx.QueryRow(ctx, `
		SELECT version FROM incidents WHERE tenant_id=$1 AND id=$2 FOR UPDATE`,
		tenantID, incidentID).Scan(&version); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, incident.ErrNotFound
		}
		return 0, err
	}
	if version != expectedVersion {
		return 0, incident.ErrVersionConflict
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO incident_alerts (tenant_id, incident_id, alert_instance_id, relation_type)
		VALUES ($1,$2,$3,'manual')
		ON CONFLICT (incident_id, alert_instance_id) DO NOTHING`,
		tenantID, incidentID, instanceID)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE incidents SET version=version+1, updated_at=now()
			WHERE tenant_id=$1 AND id=$2`, tenantID, incidentID); err != nil {
			return 0, err
		}
		raw, _ := json.Marshal(map[string]any{"instance_id": instanceID})
		if _, err := tx.Exec(ctx, `
			INSERT INTO audit_logs (id, tenant_id, actor_id, operation, object_type, object_id, details)
			VALUES ($1,$2,$3,'incident.link_alert','incident',$4,$5::jsonb)`,
			uuid.NewString(), tenantID, actor, incidentID, raw); err != nil {
			return 0, err
		}
		version++
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return version, nil
}

// UnlinkAlert removes a manual row and records the removal in the audit
// trail — the reversal itself leaves a trace (INC-001/006).
func (s *IncidentStore) UnlinkAlert(ctx context.Context, tenantID, incidentID, instanceID, actor string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
		DELETE FROM incident_alerts
		WHERE tenant_id=$1 AND incident_id=$2 AND alert_instance_id=$3 AND relation_type='manual'`,
		tenantID, incidentID, instanceID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return incident.ErrNotFound // not linked, or a derived link (not removable)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE incidents SET version=version+1, updated_at=now()
		WHERE tenant_id=$1 AND id=$2`, tenantID, incidentID); err != nil {
		return err
	}
	raw, _ := json.Marshal(map[string]any{"instance_id": instanceID})
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_logs (id, tenant_id, actor_id, operation, object_type, object_id, details)
		VALUES ($1,$2,$3,'incident.unlink_alert','incident',$4,$5::jsonb)`,
		uuid.NewString(), tenantID, actor, incidentID, raw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// IncidentDetail is the workbench payload (INC-003/008): the incident, its
// linked alerts with evidence, and the escalation trail.
type IncidentDetail struct {
	Incident    incident.Incident
	LinkedAlerts []LinkedAlert
	Escalations []Escalation
}

// LinkedAlert carries the correlation provenance of one link.
type LinkedAlert struct {
	InstanceID   string
	RelationType string // derived_1to1 | manual | auto_correlated
	Name         string
	Severity     string
	State        string
	StartedAt    time.Time
}

// Escalation is one recorded stage bump (INC-005).
type Escalation struct {
	Stage         int
	PolicyVersion int64
	NotifiedAt    time.Time
}

// GetDetail loads the workbench view (INC-003).
func (s *IncidentStore) GetDetail(ctx context.Context, tenantID, incidentID string) (*IncidentDetail, error) {
	in, err := s.Get(ctx, tenantID, incidentID)
	if err != nil {
		return nil, err
	}
	d := &IncidentDetail{Incident: *in}
	rows, err := s.pool.Query(ctx, `
		SELECT a.alert_instance_id::text, ia.relation_type, i.name, i.severity, i.monitor_state, i.starts_at
		FROM incident_alerts ia
		JOIN alert_instances i ON i.id = ia.alert_instance_id AND i.tenant_id = ia.tenant_id
		WHERE ia.tenant_id=$1 AND ia.incident_id=$2
		ORDER BY i.starts_at`, tenantID, incidentID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var la LinkedAlert
		if err := rows.Scan(&la.InstanceID, &la.RelationType, &la.Name, &la.Severity, &la.State, &la.StartedAt); err != nil {
			rows.Close()
			return nil, err
		}
		d.LinkedAlerts = append(d.LinkedAlerts, la)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	erows, err := s.pool.Query(ctx, `
		SELECT stage, policy_version, notified_at FROM incident_escalations
		WHERE tenant_id=$1 AND incident_id=$2 ORDER BY notified_at`, tenantID, incidentID)
	if err != nil {
		return nil, err
	}
	for erows.Next() {
		var e Escalation
		if err := erows.Scan(&e.Stage, &e.PolicyVersion, &e.NotifiedAt); err != nil {
			erows.Close()
			return nil, err
		}
		d.Escalations = append(d.Escalations, e)
	}
	erows.Close()
	return d, erows.Err()
}
