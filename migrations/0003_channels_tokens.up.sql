-- 0003 (P3 · NOTI-002 / AUTH-001): notification channels + platform tokens.
BEGIN;

-- One row per configured delivery channel. config holds type-specific
-- settings (webhook URL, feishu/dingtalk hook secret); secrets never appear
-- in logs. SSRF validation happens at config time AND at send time.
CREATE TABLE IF NOT EXISTS notification_channels (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT channel_type_ck CHECK (type IN ('webhook','feishu','dingtalk','email')),
    CONSTRAINT uq_channel_name UNIQUE (tenant_id, name)
);
CREATE INDEX IF NOT EXISTS idx_channels_tenant ON notification_channels(tenant_id, enabled);

-- Platform bearer tokens for the management/query surface (AUTH-001/002).
-- Only the SHA-256 is stored; role drives the RBAC matrix. Sources use the
-- separate source_credentials table (different lifetime and blast radius).
CREATE TABLE IF NOT EXISTS platform_tokens (
    tenant_id UUID NOT NULL,
    actor_id TEXT NOT NULL,
    role TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, actor_id, token_hash),
    CONSTRAINT platform_role_ck CHECK (role IN ('platform_admin','maintainer','sre','oncall','auditor'))
);
CREATE INDEX IF NOT EXISTS idx_platform_tokens_hash ON platform_tokens(token_hash) WHERE revoked_at IS NULL;

COMMIT;
