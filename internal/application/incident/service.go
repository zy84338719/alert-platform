// Package incident implements the 1:1 incident entity and its disposal
// workflow (UI-004, FR-008): a firing episode opens an incident, human ACK
// moves it open -> acknowledged with optimistic versioning. ACK NEVER touches
// the alert's monitor state (监控状态权威在上游/状态机，处置状态权威在人).
package incident

import (
	"context"
	"errors"
	"time"
)

// Errors surfaced as HTTP codes.
var (
	ErrNotFound        = errors.New("incident not found")
	ErrVersionConflict = errors.New("expected_version conflict (409)")
	ErrInvalidStatus   = errors.New("invalid status transition")
)

// Disposal statuses (docs/03 §5).
const (
	StatusOpen         = "open"
	StatusAcknowledged = "acknowledged"
	StatusMitigated    = "mitigated"
	StatusClosed       = "closed"
)

// Incident is the disposal entity.
type Incident struct {
	IncidentID      string
	TenantID        string
	CorrelationKey  string
	Title           string
	Status          string
	Priority        string
	OwnerTeamID     string
	Version         int64
	AlertInstanceID string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Store is the persistence port (infrastructure/postgres).
type Store interface {
	Get(ctx context.Context, tenantID, incidentID string) (*Incident, error)
	List(ctx context.Context, tenantID, status string, pageSize int, pageToken string) ([]Incident, string, error)
	// Acknowledge performs the open->acknowledged transition under an
	// optimistic version check; the SAME idempotency key replaying returns
	// the current state without bumping the version.
	Acknowledge(ctx context.Context, tenantID, incidentID, actor, idempotencyKey string, expectedVersion int64) (*Incident, error)
}

// Service wires the use cases.
type Service struct{ store Store }

// NewService builds the incident service.
func NewService(store Store) *Service { return &Service{store: store} }

// Get returns one incident (tenant-scoped).
func (s *Service) Get(ctx context.Context, tenantID, incidentID string) (*Incident, error) {
	return s.store.Get(ctx, tenantID, incidentID)
}

// List filters by disposal status (open incidents by default).
func (s *Service) List(ctx context.Context, tenantID, status string, pageSize int, pageToken string) ([]Incident, string, error) {
	switch status {
	case "", StatusOpen, StatusAcknowledged, StatusMitigated, StatusClosed:
	default:
		return nil, "", ErrInvalidStatus
	}
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 50
	}
	return s.store.List(ctx, tenantID, status, pageSize, pageToken)
}

// Acknowledge records the human ACK (expected_version guard, idempotent).
func (s *Service) Acknowledge(ctx context.Context, tenantID, incidentID, actor, idempotencyKey string, expectedVersion int64) (*Incident, error) {
	if idempotencyKey == "" {
		return nil, errors.New("idempotency_key is required")
	}
	return s.store.Acknowledge(ctx, tenantID, incidentID, actor, idempotencyKey, expectedVersion)
}

// Link/Unlink are the manual correlation operations (INC-001): reversible,
// audited by the caller, and version-guarded.
type LinkStore interface {
	LinkAlert(ctx context.Context, tenantID string, incidentID string, instanceID string, actor string, expectedIncidentVersion int64) (newVersion int64, err error)
	UnlinkAlert(ctx context.Context, tenantID string, incidentID string, instanceID string, actor string) error
}

// Link ties one alert instance to an incident under an optimistic version
// check; relinking after an unlink is explicitly allowed (reversible).
func (s *Service) LinkAlert(ctx context.Context, store LinkStore, tenantID, incidentID, instanceID, actor string, expectedVersion int64) (int64, error) {
	if _, err := s.Get(ctx, tenantID, incidentID); err != nil {
		return 0, err
	}
	return store.LinkAlert(ctx, tenantID, incidentID, instanceID, actor, expectedVersion)
}

// Unlink removes a manual link (derived_1to1 rows cannot be unlinked).
func (s *Service) UnlinkAlert(ctx context.Context, store LinkStore, tenantID, incidentID, instanceID, actor string) error {
	return store.UnlinkAlert(ctx, tenantID, incidentID, instanceID, actor)
}

// DetailStore provides the workbench view (INC-003).
type DetailStore interface {
	GetDetail(ctx context.Context, tenantID, incidentID string) (*Detail, error)
}

// Detail is the workbench payload at the application boundary.
type Detail struct {
	Incident     Incident
	LinkedAlerts []LinkedAlertView
	Escalations  []EscalationView
}

// LinkedAlertView carries correlation provenance.
type LinkedAlertView struct {
	InstanceID   string
	RelationType string
	Name         string
	Severity     string
	State        string
	StartedAt    time.Time
}

// EscalationView is one ladder record.
type EscalationView struct {
	Stage         int
	PolicyVersion int64
	NotifiedAt    time.Time
}

// GetDetail loads the workbench view.
func (s *Service) GetDetail(ctx context.Context, tenantID, incidentID string) (*Detail, error) {
	in, err := s.store.Get(ctx, tenantID, incidentID)
	if err != nil {
		return nil, err
	}
	ds, ok := s.store.(DetailStore)
	if !ok {
		return &Detail{Incident: *in}, nil
	}
	return ds.GetDetail(ctx, tenantID, incidentID)
}
