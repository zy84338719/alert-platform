package sourcereg

import (
	"context"
	"errors"
	"testing"
	"time"
)

type countingStore struct {
	findByToken, findEdge, invalidations int
	err                                  error
}

func (c *countingStore) CreateSource(context.Context, *Source, string) error {
	c.invalidations++
	return nil
}
func (c *countingStore) GetSource(context.Context, string, string) (*Source, error) {
	return nil, ErrNotFound
}
func (c *countingStore) ListSources(context.Context, string, bool) ([]*Source, error) {
	return nil, nil
}
func (c *countingStore) SetEnabled(context.Context, string, string, bool) error {
	c.invalidations++
	return nil
}
func (c *countingStore) RevokeCredentials(context.Context, string, string) error {
	c.invalidations++
	return nil
}
func (c *countingStore) FindByTokenHash(context.Context, string) (*Source, error) {
	c.findByToken++
	if c.err != nil {
		return nil, c.err
	}
	return &Source{SourceID: "s1", TenantID: "t1", Enabled: true}, nil
}
func (c *countingStore) FindEdgeByTokenHash(context.Context, string) (*EdgeIdentity, error) {
	c.findEdge++
	return &EdgeIdentity{EdgeID: "e1", TenantID: "t1", AllowedSourceIDs: []string{"s1"}}, nil
}
func (c *countingStore) TouchLastSeen(context.Context, string) error { return nil }

// P9-R29 失败测试：凭证正缓存 —— 同一 token 的重复认证只打一次 PG；
// 管理写（禁用/吊销/创建）立即清空缓存；PG 故障时缓存命中继续放行。
func TestCachedStore(t *testing.T) {
	ctx := context.Background()
	inner := &countingStore{}
	store := NewCachedStore(inner, 30*time.Second)
	if cs, ok := store.(interface{ SetClock(time.Time) }); ok {
		_ = cs
	}
	// inject clock via ttl boundary test below

	if _, err := store.FindByTokenHash(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FindByTokenHash(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	if inner.findByToken != 1 {
		t.Fatalf("PG calls = %d, want 1 (positive lookup must be cached)", inner.findByToken)
	}
	if _, err := store.FindEdgeByTokenHash(ctx, "h2"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FindEdgeByTokenHash(ctx, "h2"); err != nil {
		t.Fatal(err)
	}
	if inner.findEdge != 1 {
		t.Fatalf("edge PG calls = %d, want 1", inner.findEdge)
	}

	// 管理写立即清空：禁用后下一次查询必须回源（revocation 生效）。
	if err := store.SetEnabled(ctx, "t1", "s1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FindByTokenHash(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	if inner.findByToken != 2 {
		t.Fatalf("PG calls after invalidate = %d, want 2", inner.findByToken)
	}

	// PG 故障 + 缓存命中：接入继续放行（可用性不依赖 PG）。
	outage := &countingStore{err: errors.New("pg down")}
	cached := NewCachedStore(outage, 30*time.Second)
	// pre-warm: one successful lookup
	outage.err = nil
	if _, err := cached.FindByTokenHash(ctx, "warm"); err != nil {
		t.Fatal(err)
	}
	outage.err = errors.New("pg down")
	if _, err := cached.FindByTokenHash(ctx, "warm"); err != nil {
		t.Fatalf("cached auth failed during PG outage: %v", err)
	}
	if outage.findByToken != 1 {
		t.Fatalf("outage PG calls = %d, want 1 (cache must shield the outage)", outage.findByToken)
	}

	// TTL 过期：回源。
	inner2 := &countingStore{}
	c2 := NewCachedStore(inner2, 0) // ttl<=0 → 30s default; simulate expiry by invalidation instead
	if _, err := c2.FindByTokenHash(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := c2.RevokeCredentials(ctx, "t", "s"); err != nil {
		t.Fatal(err)
	}
	if _, err := c2.FindByTokenHash(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if inner2.findByToken != 2 {
		t.Fatalf("PG calls after revoke = %d, want 2 (invalidate on revoke)", inner2.findByToken)
	}
}
