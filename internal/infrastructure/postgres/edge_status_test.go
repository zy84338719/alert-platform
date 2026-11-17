package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/example/alert-platform/internal/application/ops"
)

// AUD-009 integration: the waterline write must be bound to a REGISTERED
// (tenant, source) pair. Before this check a shared edge key could zero any
// tenant's buffer by claiming its source id in the heartbeat body.
func TestReportEdgeStatusBindsTenantSource(t *testing.T) {
	st, pool := newStoreTest(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO alert_sources (tenant_id, source_id, name, source_type,
			vendor_version, adapter_version, cluster_id)
		VALUES ($1::uuid, $2::uuid, 'df', 'deepflow', 'v1', 'v1', 'c1')`, tenantA, srcDF); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	// ① registered pair -> stored.
	if err := st.ReportEdgeStatus(ctx, tenantA, srcDF, 4096, 7); err != nil {
		t.Fatalf("own pair: %v", err)
	}
	var bytes, pending int64
	var tenantStr string
	if err := pool.QueryRow(ctx,
		`SELECT tenant_id::text, wal_bytes, wal_pending FROM edge_status WHERE source_id=$1::uuid`, srcDF).
		Scan(&tenantStr, &bytes, &pending); err != nil {
		t.Fatalf("read waterline: %v", err)
	}
	if tenantStr != tenantA || bytes != 4096 || pending != 7 {
		t.Fatalf("stored waterline = (%s,%d,%d)", tenantStr, bytes, pending)
	}

	// ② another tenant claiming the same source -> rejected, row untouched.
	if err := st.ReportEdgeStatus(ctx, tenantB, srcDF, 0, 0); !errors.Is(err, ops.ErrSourceNotOwned) {
		t.Fatalf("foreign tenant: want ErrSourceNotOwned got %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT tenant_id::text, wal_bytes FROM edge_status WHERE source_id=$1::uuid`, srcDF).
		Scan(&tenantStr, &bytes); err != nil {
		t.Fatalf("re-read waterline: %v", err)
	}
	if tenantStr != tenantA || bytes != 4096 {
		t.Fatalf("foreign write mutated the row: (%s,%d)", tenantStr, bytes)
	}

	// ③ unregistered source id -> rejected and no orphan row.
	const ghost = "cccc0000-0000-0000-0000-0000000000ff"
	if err := st.ReportEdgeStatus(ctx, tenantA, ghost, 1, 1); !errors.Is(err, ops.ErrSourceNotOwned) {
		t.Fatalf("ghost source: want ErrSourceNotOwned got %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM edge_status WHERE source_id=$1::uuid`, ghost).Scan(&n); err != nil {
		t.Fatalf("count ghost: %v", err)
	}
	if n != 0 {
		t.Fatalf("ghost waterline rows = %d", n)
	}
}
