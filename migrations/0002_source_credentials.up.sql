-- 0002 (ING-003): per-source ingest credentials.
-- Only the SHA-256 of the bearer token is stored; the plaintext token is
-- returned exactly once at creation time and must never be logged.
BEGIN;

CREATE TABLE IF NOT EXISTS source_credentials (
    tenant_id UUID NOT NULL,
    source_id UUID NOT NULL,
    token_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, source_id, token_hash)
);
CREATE INDEX IF NOT EXISTS idx_source_credentials_token
    ON source_credentials(token_hash) WHERE revoked_at IS NULL;

COMMIT;
