// Package metricsquery powers the console "metrics center" (P9-MC-01): a
// WHITELISTED catalog of platform metrics proxied to Prometheus. Callers can
// never inject arbitrary PromQL — the expression is chosen server-side from
// the catalog by name, so the read-only surface cannot be abused for scans or
// cross-tenant data access.
package metricsquery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Metric tiers (P10-006). Platform-tier metrics aggregate the whole
// installation; exposing them to a tenant user leaks cross-tenant volume,
// error rates and source inventory. Tenant-tier metrics carry a `tenant`
// label and are ALWAYS filtered server-side with the caller's tenant.
const (
	TierPlatform = "platform"
	TierTenant   = "tenant"
)

// Metric is one catalog entry.
type Metric struct {
	Name        string `json:"name"`
	Unit        string `json:"unit"`
	Description string `json:"description"`
	Tier        string `json:"tier"`
	expr        string // server-side PromQL template, never exposed
}

// Series is one labeled time series returned by Prometheus.
type Series struct {
	Labels map[string]string `json:"labels"`
	// Points are [unix_seconds, value] pairs (Prometheus matrix shape).
	Points [][2]float64 `json:"points"`
}

// Result is the normalized query_range response for the console.
type Result struct {
	Metric      string   `json:"metric"`
	Unit        string   `json:"unit"`
	Start       string   `json:"start"`
	End         string   `json:"end"`
	StepSeconds int      `json:"step_seconds"`
	Series      []Series `json:"series"`
}

// Client is the Prometheus HTTP API port (infrastructure/promclient).
type Client interface {
	QueryRange(ctx context.Context, expr string, start, end time.Time, step time.Duration) ([]Series, error)
}

// Validation errors surfaced as HTTP 400/404 by the adapter.
var (
	ErrUnknownMetric = errors.New("unknown metric")
	ErrInvalidRange  = errors.New("invalid time range")
	// ErrForbidden: a platform-tier metric was requested without the
	// platform-metrics permission.
	ErrForbidden = errors.New("metric requires platform scope")
)

// Limits keep one query cheap for Prometheus and the console.
const (
	maxRange    = 24 * time.Hour
	minStep     = 15 * time.Second
	maxStep     = time.Hour
	maxPoints   = 2000
	defaultStep = time.Minute
)

// catalog is the authoritative whitelist. Names are stable API surface.
var catalog = []Metric{
	{Name: "ingest_events_rate", Unit: "events/s", Description: "Accepted ingest events per second.",
		expr: `sum(rate(alert_platform_ingest_events_total[5m]))`},
	{Name: "ingest_rejected_rate", Unit: "req/s", Description: "Rejected ingest requests per second by reason.",
		expr: `sum by (reason) (rate(alert_platform_ingest_rejected_total[5m]))`},
	{Name: "ingest_batch_p95", Unit: "s", Description: "PushBatch P95 latency (broker ack included).",
		expr: `histogram_quantile(0.95, sum by (le) (rate(alert_platform_ingest_batch_duration_seconds_bucket[5m])))`},
	{Name: "processor_events_rate", Unit: "events/s", Description: "Processed events per second by outcome.",
		expr: `sum by (outcome) (rate(alert_platform_processor_events_total[5m]))`},
	{Name: "processor_p95", Unit: "s", Description: "ApplyEvent transaction P95 latency.",
		expr: `histogram_quantile(0.95, sum by (le) (rate(alert_platform_processor_duration_seconds_bucket[5m])))`},
	{Name: "consumer_lag", Unit: "messages", Description: "Consumer group lag by topic.",
		expr: `max by (topic) (alert_platform_consumer_lag)`},
	{Name: "dlq_quarantined_rate", Unit: "msg/s", Description: "Poison messages quarantined per second.",
		expr: `sum(rate(alert_platform_dlq_quarantined_total[5m]))`},
	{Name: "notify_attempt_rate", Unit: "attempts/s", Description: "Notification attempts per second by result.",
		expr: `sum by (result) (rate(alert_platform_notify_attempt_total[5m]))`},
	{Name: "notify_pending", Unit: "tasks", Description: "Pending/retry notification tasks (current generation).",
		expr: `alert_platform_notify_pending`},
	{Name: "notify_authority_generation", Unit: "generation", Description: "Persisted notification-authority generation.",
		expr: `alert_platform_notify_authority_generation`},
	{Name: "active_instances", Unit: "alerts", Description: "Alert instances pending/firing/stale.",
		Tier: TierTenant, expr: `alert_platform_alert_active_instances`},
	{Name: "open_incidents", Unit: "incidents", Description: "Incidents not yet closed.",
		Tier: TierTenant, expr: `alert_platform_incident_open`},
	{Name: "source_heartbeat_age", Unit: "s", Description: "Seconds since each source last pushed.",
		Tier: TierTenant, expr: `alert_platform_source_heartbeat_age_seconds`},
	{Name: "edge_wal_bytes", Unit: "bytes", Description: "Edge WAL size.",
		expr: `alert_platform_edge_wal_bytes`},
	{Name: "edge_reconnect_rate", Unit: "reconnects/s", Description: "SSE reconnects per second.",
		expr: `sum(rate(alert_platform_edge_reconnect_total[5m]))`},
	{Name: "edge_sse_gap_rate", Unit: "gaps/s", Description: "SSE gap markers per second (data loss windows).",
		expr: `sum(rate(alert_platform_edge_sse_gap_total[5m]))`},
	{Name: "edge_wal_rejected_rate", Unit: "batches/s", Description: "Vendor batches refused by the edge WAL (disk full/io error).",
		expr: `sum(rate(alert_platform_edge_wal_rejected_total[5m]))`},
	{Name: "edge_upload_poison_rate", Unit: "batches/s", Description: "Edge batches permanently rejected by the center (4xx).",
		expr: `sum(rate(alert_platform_edge_upload_poison_total[5m]))`},
	{Name: "archive_written_rate", Unit: "rows/s", Description: "Transitions written to the ClickHouse archive.",
		expr: `sum(rate(alert_platform_archive_written_total[5m]))`},
	{Name: "archive_lag", Unit: "s", Description: "Age of the most recently archived transition.",
		expr: `alert_platform_archive_lag_seconds`},
	{Name: "archive_error_rate", Unit: "errors/s", Description: "Archiver failures per second.",
		expr: `sum(rate(alert_platform_archive_errors_total[5m]))`},
}

var byName = func() map[string]Metric {
	m := make(map[string]Metric, len(catalog))
	for i := range catalog {
		if catalog[i].Tier == "" {
			catalog[i].Tier = TierPlatform
		}
		m[catalog[i].Name] = catalog[i]
	}
	return m
}()

// Service validates catalog lookups and range limits, then delegates.
type Service struct {
	client Client
}

// NewService builds the service over the Prometheus client port.
func NewService(client Client) *Service { return &Service{client: client} }

// Catalog lists every whitelisted metric (platform-admin view, no expressions).
func Catalog() []Metric { return append([]Metric(nil), catalog...) }

// CatalogFor filters the catalog by caller scope: a tenant user sees ONLY
// tenant-tier metrics. Platform admins see everything.
func CatalogFor(platform bool) []Metric {
	out := make([]Metric, 0, len(catalog))
	for _, e := range catalog {
		if platform || e.Tier == TierTenant {
			out = append(out, e)
		}
	}
	return out
}

// tenantMatcher appends a tenant label selector to a plain metric selector.
// Tenant IDs come from the authenticated identity (UUID), and the value is
// still escaped so no caller-controlled text can alter the PromQL.
func tenantMatcher(expr, tenantID string) string {
	escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(tenantID)
	return expr + `{tenant="` + escaped + `"}`
}

// Query resolves name against the catalog and validates the window.
// Tenant-tier metrics are filtered server-side with the caller's tenant;
// platform-tier metrics require platform scope (P10-006).
func (s *Service) Query(ctx context.Context, name, tenantID string, platform bool, start, end time.Time, step time.Duration) (Result, error) {
	entry, ok := byName[name]
	if !ok {
		return Result{}, fmt.Errorf("%w: %q", ErrUnknownMetric, name)
	}
	if entry.Tier == TierPlatform && !platform {
		return Result{}, fmt.Errorf("%w: %q", ErrForbidden, name)
	}
	expr := entry.expr
	if entry.Tier == TierTenant && !platform {
		if tenantID == "" {
			return Result{}, fmt.Errorf("%w: tenant identity required", ErrForbidden)
		}
		expr = tenantMatcher(expr, tenantID)
	}
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return Result{}, fmt.Errorf("%w: end must be after start", ErrInvalidRange)
	}
	window := end.Sub(start)
	if window > maxRange {
		return Result{}, fmt.Errorf("%w: range exceeds %s", ErrInvalidRange, maxRange)
	}
	if step <= 0 {
		step = defaultStep
	}
	if step < minStep {
		return Result{}, fmt.Errorf("%w: step below %s", ErrInvalidRange, minStep)
	}
	if step > maxStep {
		return Result{}, fmt.Errorf("%w: step above %s", ErrInvalidRange, maxStep)
	}
	if int(window/step) > maxPoints {
		return Result{}, fmt.Errorf("%w: more than %d points", ErrInvalidRange, maxPoints)
	}
	series, err := s.client.QueryRange(ctx, expr, start, end, step)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Metric: entry.Name, Unit: entry.Unit,
		Start: start.UTC().Format(time.RFC3339), End: end.UTC().Format(time.RFC3339),
		StepSeconds: int(step.Seconds()), Series: series,
	}, nil
}
