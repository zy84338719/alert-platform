-- 0004 (HT-002): node-shard leases. One row per shard: whoever holds a live
-- lease is the ONLY valid subscriber for that HUATUO node partition; expired
-- leases are taken over after the grace period (replica scale-down).
BEGIN;

CREATE TABLE IF NOT EXISTS edge_leases (
    shard_key TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    cursor TEXT NOT NULL DEFAULT '',
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    generations BIGINT NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_edge_leases_expiry ON edge_leases(expires_at);

COMMIT;
