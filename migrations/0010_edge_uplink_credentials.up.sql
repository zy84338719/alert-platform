-- 0010 (P8 · P0-02): edge uplink identity. Multi-source edges previously had
-- to present one SOURCE credential per buffered source — a gateway fronting
-- DeepFlow + Alertmanager cannot do that with a single edge key, and copying
-- every source's plaintext token into the edge is not a durable design.
--
-- This table separates the "edge -> center" credential from the per-vendor
-- webhook credentials the edge accepts:
--   edge_id            — operator-facing identity of the edge (X-Edge-Id)
--   tenant_id          — the one tenant this edge may upload for
--   allowed_source_ids — the source subset this credential is scoped to
--   token_hash         — SHA-256 of the uplink bearer (plaintext never stored)
--   revoked_at         — rotation: add the new token row, then revoke the old.
BEGIN;

CREATE TABLE IF NOT EXISTS edge_uplink_credentials (
    edge_id            TEXT NOT NULL,
    tenant_id          UUID NOT NULL,
    token_hash         TEXT NOT NULL,
    allowed_source_ids UUID[] NOT NULL DEFAULT '{}',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at         TIMESTAMPTZ,
    PRIMARY KEY (edge_id, token_hash)
);

CREATE INDEX IF NOT EXISTS idx_edge_uplink_credentials_token
    ON edge_uplink_credentials(token_hash) WHERE revoked_at IS NULL;

COMMIT;