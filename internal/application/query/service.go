// Package query implements the read-side use cases (PROC-007): alert list /
// detail / state timeline with mandatory tenant scoping and keyset-safe
// pagination. Every store call receives the acting tenant — cross-tenant
// reads are Forbidden, never silently empty (FR-009).
package query

import (
	"context"
	"errors"
	"time"
)

// Errors surfaced by the inbound adapters.
var (
	ErrNotFound  = errors.New("alert instance not found")
	ErrForbidden = errors.New("cross-tenant access denied")
)

// AlertFilter is the list request (tenant is always the authenticated one).
type AlertFilter struct {
	TenantID  string
	State     string // pending|firing|stale|resolved; empty = active-only
	Severity  string
	SourceID  string
	ClusterID string
	Name      string
	PageSize  int
	PageToken string // opaque offset token from a previous response
}

// Alert is the read model of one alert instance row.
type Alert struct {
	InstanceID       string
	TenantID         string
	SourceID         string
	Fingerprint      string
	EpisodeKey       string
	Name             string
	Severity         string
	MonitorState     string
	ClusterID        string
	EntityType       string
	EntityUID        string
	Labels           map[string]string
	StartsAt         time.Time
	EndsAt           time.Time
	LastObservedAt   time.Time
	LastTransitionAt time.Time
	Version          int64
}

// Transition is one state-timeline entry.
type Transition struct {
	TransitionID string
	InstanceID   string
	FromState    string
	ToState      string
	EventID      string
	TransitionAt time.Time
}

// Page is a list result plus the continuation token.
type Page[T any] struct {
	Items        []T
	NextPageToken string
	TotalEstimate int64
}

// Store is the read-side persistence port (infrastructure/postgres).
type Store interface {
	ListAlerts(ctx context.Context, f AlertFilter) (Page[Alert], error)
	// GetAlertByID fetches without tenant filtering; the SERVICE enforces
	// tenant ownership so mismatches map to ErrForbidden (not a silent 404).
	GetAlertByID(ctx context.Context, instanceID string) (*Alert, error)
	ListTransitions(ctx context.Context, tenantID, instanceID string, pageSize int, pageToken string) (Page[Transition], error)
}

// Service wires the read use cases.
type Service struct{ store Store }

// NewService builds the query service.
func NewService(store Store) *Service { return &Service{store: store} }

// List returns one page of alerts for the acting tenant.
func (s *Service) List(ctx context.Context, f AlertFilter) (Page[Alert], error) {
	if f.TenantID == "" {
		return Page[Alert]{}, ErrForbidden
	}
	if f.PageSize <= 0 || f.PageSize > 200 {
		f.PageSize = 50
	}
	switch f.State {
	case "", "pending", "firing", "stale", "resolved":
	default:
		return Page[Alert]{}, errors.New("invalid state filter")
	}
	return s.store.ListAlerts(ctx, f)
}

// Get returns one alert; cross-tenant access is Forbidden (PROC-007).
func (s *Service) Get(ctx context.Context, tenantID, instanceID string) (*Alert, error) {
	a, err := s.store.GetAlertByID(ctx, instanceID)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if a.TenantID != tenantID {
		return nil, ErrForbidden
	}
	return a, nil
}

// Timeline returns the state transitions of one alert (tenant-scoped).
func (s *Service) Timeline(ctx context.Context, tenantID, instanceID string, pageSize int, pageToken string) (Page[Transition], error) {
	if _, err := s.Get(ctx, tenantID, instanceID); err != nil {
		return Page[Transition]{}, err
	}
	if pageSize <= 0 || pageSize > 500 {
		pageSize = 100
	}
	return s.store.ListTransitions(ctx, tenantID, instanceID, pageSize, pageToken)
}
