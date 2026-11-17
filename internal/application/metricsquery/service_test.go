package metricsquery

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type captureClient struct {
	exprs []string
}

func (c *captureClient) QueryRange(_ context.Context, expr string, _, _ time.Time, _ time.Duration) ([]Series, error) {
	c.exprs = append(c.exprs, expr)
	return nil, nil
}

// P10-006: tenant users only see tenant-tier metrics, and the query carries a
// server-injected tenant matcher; platform metrics are forbidden.
func TestTenantIsolation(t *testing.T) {
	cl := &captureClient{}
	svc := NewService(cl)
	now := time.Now()

	tenantCatalog := CatalogFor(false)
	for _, m := range tenantCatalog {
		if m.Tier != TierTenant {
			t.Fatalf("tenant catalog leaked %s (%s)", m.Name, m.Tier)
		}
	}
	adminCatalog := CatalogFor(true)
	if len(adminCatalog) <= len(tenantCatalog) {
		t.Fatalf("admin catalog = %d, tenant = %d", len(adminCatalog), len(tenantCatalog))
	}

	if _, err := svc.Query(context.Background(), "ingest_events_rate", "tenant-a", false, now.Add(-time.Hour), now, time.Minute); !errors.Is(err, ErrForbidden) {
		t.Fatalf("tenant platform query: %v, want ErrForbidden", err)
	}
	if _, err := svc.Query(context.Background(), "active_instances", "tenant-a", false, now.Add(-time.Hour), now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if len(cl.exprs) != 1 || !strings.Contains(cl.exprs[0], `{tenant="tenant-a"}`) {
		t.Fatalf("tenant matcher not injected: %v", cl.exprs)
	}

	// Admin querying a tenant metric sees the aggregate (no injected matcher).
	if _, err := svc.Query(context.Background(), "active_instances", "tenant-a", true, now.Add(-time.Hour), now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(cl.exprs[1], "tenant=") {
		t.Fatalf("admin query was tenant-filtered: %s", cl.exprs[1])
	}
	// Admin can query platform metrics.
	if _, err := svc.Query(context.Background(), "ingest_events_rate", "", true, now.Add(-time.Hour), now, time.Minute); err != nil {
		t.Fatal(err)
	}
}

// Tenant IDs are UUIDs, but escaping must still prevent PromQL injection if a
// future identity source permits other characters.
func TestTenantMatcherEscapesPromQL(t *testing.T) {
	expr := tenantMatcher("alert_platform_alert_active_instances", `t"}{job="evil`)
	// Every quote in the value is escaped, so the payload stays INSIDE the
	// tenant label literal and cannot close it / add selectors.
	want := `alert_platform_alert_active_instances{tenant="t\"}{job=\"evil"}`
	if expr != want {
		t.Fatalf("tenant matcher = %s, want %s", expr, want)
	}
}
