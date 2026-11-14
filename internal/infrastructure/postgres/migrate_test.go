package postgres

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq" // raw multi-statement exec for the SQL-idempotency check

	"github.com/example/alert-platform/internal/infrastructure/testenv"
)

// migrationsDir is relative to this package (internal/infrastructure/postgres).
const migrationsDir = "../../../migrations"

// wantTables is the authoritative inventory from migrations/0001_initial.up.sql,
// covering every object class in docs/05 §1 (source registration, policy
// snapshots, current alerts, state transitions, incidents, notification tasks,
// delivery attempts, audit, plus receipts/silences/outboxes). If the DDL adds
// or removes a table, update docs/05 §1 and this list together — the test is
// the consistency check demanded by docs/13 BASE-005.
var wantTables = []string{
	"alert_sources",
	"alert_instances",
	"alert_transitions",
	"audit_logs",
	"domain_outbox",
	"event_receipts",
	"incident_alerts",
	"incidents",
	"notification_attempts",
	"notification_outbox",
	"policy_versions",
	"silences",
	"source_credentials", // 0002 (ING-003)
	"notification_channels", // 0003 (NOTI-002)
	"platform_tokens",       // 0003 (AUTH-001)
	"edge_status",           // 0005 (MC-002)
	"edge_leases",           // 0004 (HT-002)
	"uid_history",           // 0006 (INC-004)
	"event_history",         // 0007 (P0-05 事件历史)
	"incident_escalations",  // 0006 (INC-005)
}

func absMigrations(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(migrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "0001_initial.up.sql")); err != nil {
		t.Fatalf("migration files not found in %s: %v", dir, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "0002_source_credentials.up.sql")); err != nil {
		t.Fatalf("migration files not found in %s: %v", dir, err)
	}
	for _, name := range []string{
		"0003_channels_tokens.up.sql",
		"0004_edge_leases.up.sql",
		"0005_edge_status.up.sql",
		"0006_identity_history.up.sql",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("migration files not found in %s: %v", dir, err)
		}
	}
	return dir
}

// TestMigrateUpTwiceNoError is the BASE-005 acceptance core: a clean database
// migrates without error, a second run is a no-op without error, the SQL file
// itself is idempotent (IF NOT EXISTS), and the created table set matches
// docs/05 §1.
func TestMigrateUpTwiceNoError(t *testing.T) {
	dsn := testenv.StartPostgres(t)
	dir := absMigrations(t)

	v1, err := Up(dsn, dir)
	if err != nil {
		t.Fatalf("first Up on clean database: %v", err)
	}
	if v1 != 8 {
		t.Fatalf("version after first Up = %d, want 8 (0001..0008: P1-02 lease fencing)", v1)
	}

	v2, err := Up(dsn, dir)
	if err != nil {
		t.Fatalf("second Up must not error (idempotent no-op): %v", err)
	}
	if v2 != 8 {
		t.Fatalf("version after second Up = %d, want 8", v2)
	}

	// SQL-level idempotency: execute the whole file again, bypassing the
	// version table. Every statement is CREATE ... IF NOT EXISTS, so this
	// must succeed — proving the DDL itself (not only the runner) is re-runnable.
	raw, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open raw connection: %v", err)
	}
	defer raw.Close()
	stmt, err := os.ReadFile(filepath.Join(dir, "0001_initial.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	stmt2, err := os.ReadFile(filepath.Join(dir, "0002_source_credentials.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	stmt3, err := os.ReadFile(filepath.Join(dir, "0003_channels_tokens.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	stmts456 := []byte{}
	for _, name := range []string{"0004_edge_leases.up.sql", "0005_edge_status.up.sql", "0006_identity_history.up.sql"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		stmts456 = append(stmts456, []byte("\n")...)
		stmts456 = append(stmts456, b...)
	}
	stmt = append(stmt, []byte("\n")...)
	stmt = append(stmt, stmt2...)
	stmt = append(stmt, []byte("\n")...)
	stmt = append(stmt, stmt3...)
	stmt = append(stmt, stmts456...)
	if _, err := raw.Exec(string(stmt)); err != nil {
		t.Fatalf("raw re-execution of all migration files must be idempotent: %v", err)
	}

	// Table inventory vs docs/05 §1 (see wantTables).
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	rows, err := pool.Query(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema='public' AND table_type='BASE TABLE'
		   AND table_name <> 'schema_migrations'`) // golang-migrate's own version table, not a business table
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer rows.Close()
	got := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		got[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, name := range wantTables {
		want[name] = true
	}
	var missing, extra []string
	for name := range want {
		if !got[name] {
			missing = append(missing, name)
		}
	}
	for name := range got {
		if !want[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("table inventory drifted from docs/05 §1: missing=%v extra=%v", missing, extra)
	}
}

// TestAlertSourceRoundTrip covers the docs/13 BASE-005 evidence item "one
// source record insert/query": insert a registered source, read it back,
// and prove the primary key rejects a duplicate registration.
func TestAlertSourceRoundTrip(t *testing.T) {
	dsn := testenv.StartPostgres(t)
	dir := absMigrations(t)
	if _, err := Up(dsn, dir); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()

	tenantID := uuid.New()
	sourceID := uuid.New()
	const insert = `
		INSERT INTO alert_sources
			(tenant_id, source_id, name, source_type, vendor_version, adapter_version, cluster_id)
		VALUES ($1, $2, $3, 'deepflow', 'v7.2.3', 'v0.1.0', 'cluster-a')`
	if _, err := pool.Exec(ctx, insert, tenantID, sourceID, "deepflow-prod"); err != nil {
		t.Fatalf("insert source: %v", err)
	}

	var name, sourceType, vendorVersion string
	var enabled bool
	err = pool.QueryRow(ctx,
		`SELECT name, source_type, vendor_version, enabled
		 FROM alert_sources WHERE tenant_id=$1 AND source_id=$2`,
		tenantID, sourceID,
	).Scan(&name, &sourceType, &vendorVersion, &enabled)
	if err != nil {
		t.Fatalf("query source: %v", err)
	}
	if name != "deepflow-prod" || sourceType != "deepflow" || vendorVersion != "v7.2.3" || !enabled {
		t.Fatalf("round-trip mismatch: name=%q type=%q vendor=%q enabled=%v",
			name, sourceType, vendorVersion, enabled)
	}

	// Duplicate registration must be rejected by the composite primary key.
	if _, err := pool.Exec(ctx, insert, tenantID, sourceID, "deepflow-prod-dup"); err == nil {
		t.Fatal("duplicate (tenant_id, source_id) insert must fail on primary key")
	}
}
