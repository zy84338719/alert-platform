// CachedStore (P9-R29): a read-through cache over the source registry store.
//
// WHY: ingest authentication queries PostgreSQL on every push. With the cache,
// a PG outage no longer takes INGESTION down with it — pushes keep being
// accepted (Kafka buffers them) while PG is unavailable, matching docs/04
// (ingest must not depend on the database being alive).
//
// Semantics:
//   - POSITIVE lookups are cached for ttl (default 30s): a disabled/revoked
//     source may keep authenticating for up to ttl after the change;
//   - any management WRITE (revoke / set-enabled / create) clears the whole
//     cache, so the common revocation path is IMMEDIATE;
//   - NEGATIVE lookups are never cached (unknown tokens stay unknown).
package sourcereg

import (
	"context"
	"sync"
	"time"
)

// cacheEntry is one positive cached lookup.
type cacheEntry[T any] struct {
	value   T
	expires time.Time
}

type cachedStore struct {
	Store
	ttl time.Duration
	now func() time.Time

	mu      sync.Mutex
	sources map[string]cacheEntry[*Source]       // tokenHash -> source
	edges   map[string]cacheEntry[*EdgeIdentity] // tokenHash -> edge identity
}

// NewCachedStore wraps store with a positive-lookup cache (ttl default 30s).
func NewCachedStore(store Store, ttl time.Duration) Store {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &cachedStore{
		Store: store, ttl: ttl, now: time.Now,
		sources: map[string]cacheEntry[*Source]{},
		edges:   map[string]cacheEntry[*EdgeIdentity]{},
	}
}

func (c *cachedStore) invalidate() {
	c.mu.Lock()
	c.sources = map[string]cacheEntry[*Source]{}
	c.edges = map[string]cacheEntry[*EdgeIdentity]{}
	c.mu.Unlock()
}

func (c *cachedStore) CreateSource(ctx context.Context, src *Source, tokenHash string) error {
	err := c.Store.CreateSource(ctx, src, tokenHash)
	if err == nil {
		c.invalidate()
	}
	return err
}

func (c *cachedStore) GetSource(ctx context.Context, tenantID, sourceID string) (*Source, error) {
	return c.Store.GetSource(ctx, tenantID, sourceID)
}

func (c *cachedStore) ListSources(ctx context.Context, tenantID string, includeDisabled bool) ([]*Source, error) {
	return c.Store.ListSources(ctx, tenantID, includeDisabled)
}

func (c *cachedStore) SetEnabled(ctx context.Context, tenantID, sourceID string, enabled bool) error {
	err := c.Store.SetEnabled(ctx, tenantID, sourceID, enabled)
	if err == nil {
		c.invalidate()
	}
	return err
}

func (c *cachedStore) RevokeCredentials(ctx context.Context, tenantID, sourceID string) error {
	err := c.Store.RevokeCredentials(ctx, tenantID, sourceID)
	if err == nil {
		c.invalidate()
	}
	return err
}

func (c *cachedStore) FindByTokenHash(ctx context.Context, tokenHash string) (*Source, error) {
	c.mu.Lock()
	if e, ok := c.sources[tokenHash]; ok && c.now().Before(e.expires) {
		src := e.value
		c.mu.Unlock()
		return src, nil
	}
	c.mu.Unlock()
	src, err := c.Store.FindByTokenHash(ctx, tokenHash)
	if err != nil {
		return nil, err // negatives are never cached
	}
	c.mu.Lock()
	c.sources[tokenHash] = cacheEntry[*Source]{value: src, expires: c.now().Add(c.ttl)}
	c.mu.Unlock()
	return src, nil
}

func (c *cachedStore) FindEdgeByTokenHash(ctx context.Context, tokenHash string) (*EdgeIdentity, error) {
	c.mu.Lock()
	if e, ok := c.edges[tokenHash]; ok && c.now().Before(e.expires) {
		ident := e.value
		c.mu.Unlock()
		return ident, nil
	}
	c.mu.Unlock()
	ident, err := c.Store.FindEdgeByTokenHash(ctx, tokenHash)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.edges[tokenHash] = cacheEntry[*EdgeIdentity]{value: ident, expires: c.now().Add(c.ttl)}
	c.mu.Unlock()
	return ident, nil
}

// TouchLastSeen passes through uncached — it is a write, and losing a few of
// them during a PG outage is acceptable (health only degrades).
func (c *cachedStore) TouchLastSeen(ctx context.Context, sourceID string) error {
	return c.Store.TouchLastSeen(ctx, sourceID)
}
