package postgres

import (
	"context"
	"testing"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
	"github.com/example/alert-platform/internal/application/sourcereg"
	"github.com/example/alert-platform/internal/infrastructure/testenv"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ING-003 integration: CRUD + the auth hot path + tenant isolation, against a
// disposable database (external DSN or Testcontainers, see testenv).
func TestSourceRegistryStore(t *testing.T) {
	dsn := testenv.StartPostgres(t)
	if _, err := Up(dsn, absMigrations(t)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	store := NewSourceStore(pool)
	svc := sourcereg.NewService(store)

	tenantA := "aaaaaaaa-0000-0000-0000-000000000001"
	tenantB := "aaaaaaaa-0000-0000-0000-000000000002"

	created, token, err := svc.Create(ctx, &sourcereg.Source{
		TenantID: tenantA, Name: "df-prod", SourceType: eventv1.SourceType_SOURCE_TYPE_DEEPFLOW,
		VendorVersion: "7.2.3", ClusterID: "prod-studio",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if token == "" || created.SourceID == "" {
		t.Fatal("create must mint a token and a source id")
	}

	// Auth hot path derives tenant from the credential only.
	src, err := svc.Authenticate(ctx, token)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if src.TenantID != tenantA || src.SourceType != eventv1.SourceType_SOURCE_TYPE_DEEPFLOW {
		t.Fatalf("authenticate resolved wrong source: %+v", src)
	}

	// Cross-tenant read is rejected (T-012 read side).
	if _, err := svc.Get(ctx, tenantB, created.SourceID); err != sourcereg.ErrNotFound {
		t.Fatalf("cross-tenant Get: want ErrNotFound, got %v", err)
	}

	// Disabled sources stop authenticating with a distinct error.
	if _, err := svc.SetEnabled(ctx, tenantA, created.SourceID, false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if _, err := svc.Authenticate(ctx, token); err != sourcereg.ErrDisabled {
		t.Fatalf("authenticate on disabled source: want ErrDisabled, got %v", err)
	}
	if _, err := svc.SetEnabled(ctx, tenantA, created.SourceID, true); err != nil {
		t.Fatalf("re-enable: %v", err)
	}

	// Revocation makes the token permanently unauthenticated.
	if _, err := svc.RevokeCredential(ctx, tenantA, created.SourceID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := svc.Authenticate(ctx, token); err != sourcereg.ErrUnauthenticated {
		t.Fatalf("authenticate after revoke: want ErrUnauthenticated, got %v", err)
	}

	// List is tenant-scoped: tenant B never sees tenant A's sources.
	if _, _, err := svc.Create(ctx, &sourcereg.Source{
		TenantID: tenantB, Name: "am-b", SourceType: eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER,
		VendorVersion: "0.27", ClusterID: "prod-b",
	}); err != nil {
		t.Fatalf("seed tenant B: %v", err)
	}
	listA, err := svc.List(ctx, tenantA, true)
	if err != nil || len(listA) != 1 {
		t.Fatalf("list A: %v (%d)", err, len(listA))
	}
	listB, err := svc.List(ctx, tenantB, true)
	if err != nil || len(listB) != 1 {
		t.Fatalf("list B: %v (%d)", err, len(listB))
	}

	// Authenticate touches last_seen (feeds PROC-006 source health).
	tok, srcID := mustToken(t, svc, tenantB)
	if _, err := svc.Authenticate(ctx, tok); err != nil {
		t.Fatalf("auth tenant B: %v", err)
	}
	got, err := svc.Get(ctx, tenantB, srcID)
	if err != nil {
		t.Fatalf("get after touch: %v", err)
	}
	if got.LastSeenAt.IsZero() {
		t.Fatal("authenticate must touch last_seen_at")
	}
}

// mustToken mints a fresh source+token for one tenant (revocation in the main
// test consumed the first token).
func mustToken(t *testing.T, svc *sourcereg.Service, tenant string) (token, sourceID string) {
	t.Helper()
	src, token, err := svc.Create(context.Background(), &sourcereg.Source{
		TenantID: tenant, Name: "tmp", SourceType: eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER,
		ClusterID: "c",
	})
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	return token, src.SourceID
}

// P8-P0-02 失败测试（真实 PG）：Edge 上行凭证 —— 一个 edge 身份可覆盖多个
// 来源（DeepFlow + Alertmanager），allowed 集随来源禁用即时收缩，吊销后
// 401；轮换（新 token 行 + 旧行 revoked）后新 token 生效、旧 token 失效。
func TestEdgeUplinkCredentialStore(t *testing.T) {
	dsn := testenv.StartPostgres(t)
	if _, err := Up(dsn, absMigrations(t)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	store := NewSourceStore(pool)
	svc := sourcereg.NewService(store)

	const tenant = "aaaaaaaa-0000-0000-0000-0000000000e1"
	df, _, err := svc.Create(ctx, &sourcereg.Source{
		TenantID: tenant, Name: "df-edge", SourceType: eventv1.SourceType_SOURCE_TYPE_DEEPFLOW, ClusterID: "c1",
	})
	if err != nil {
		t.Fatal(err)
	}
	am, _, err := svc.Create(ctx, &sourcereg.Source{
		TenantID: tenant, Name: "am-edge", SourceType: eventv1.SourceType_SOURCE_TYPE_ALERTMANAGER, ClusterID: "c1",
	})
	if err != nil {
		t.Fatal(err)
	}

	const uplink = "aps_edge-uplink-1"
	if _, err := pool.Exec(ctx, `
		INSERT INTO edge_uplink_credentials (edge_id, tenant_id, token_hash, allowed_source_ids)
		VALUES ('edge-01', $1, $2, ARRAY[$3,$4]::uuid[])`,
		tenant, sourcereg.TokenHash(uplink), df.SourceID, am.SourceID); err != nil {
		t.Fatalf("insert edge credential: %v", err)
	}

	ident, err := svc.AuthenticateEdge(ctx, uplink)
	if err != nil {
		t.Fatalf("authenticate edge: %v", err)
	}
	if ident.EdgeID != "edge-01" || ident.TenantID != tenant || len(ident.AllowedSourceIDs) != 2 {
		t.Fatalf("edge identity = %+v", ident)
	}
	if !ident.Authorizes(df.SourceID) || !ident.Authorizes(am.SourceID) {
		t.Fatalf("allowed set must cover both sources: %v", ident.AllowedSourceIDs)
	}

	// 禁用 Alertmanager 来源：allowed 集立刻收缩（不能再替它上传）。
	if _, err := svc.SetEnabled(ctx, tenant, am.SourceID, false); err != nil {
		t.Fatal(err)
	}
	ident, err = svc.AuthenticateEdge(ctx, uplink)
	if err != nil {
		t.Fatalf("authenticate after disable: %v", err)
	}
	if ident.Authorizes(am.SourceID) || !ident.Authorizes(df.SourceID) {
		t.Fatalf("disabled source still authorized: %v", ident.AllowedSourceIDs)
	}

	// 轮换：先加新 token，再吊销旧 token —— 新 token 生效、旧 token 401。
	const rotated = "aps_edge-uplink-2"
	if _, err := pool.Exec(ctx, `
		INSERT INTO edge_uplink_credentials (edge_id, tenant_id, token_hash, allowed_source_ids)
		VALUES ('edge-01', $1, $2, ARRAY[$3]::uuid[])`,
		tenant, sourcereg.TokenHash(rotated), df.SourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE edge_uplink_credentials SET revoked_at=now()
		WHERE edge_id='edge-01' AND token_hash=$1`, sourcereg.TokenHash(uplink)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AuthenticateEdge(ctx, uplink); err != sourcereg.ErrUnauthenticated {
		t.Fatalf("revoked edge token: want ErrUnauthenticated, got %v", err)
	}
	ident, err = svc.AuthenticateEdge(ctx, rotated)
	if err != nil {
		t.Fatalf("rotated token: %v", err)
	}
	if ident.EdgeID != "edge-01" || !ident.Authorizes(df.SourceID) || ident.Authorizes(am.SourceID) {
		t.Fatalf("rotated identity = %+v", ident)
	}
}
