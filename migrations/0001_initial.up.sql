-- Architecture draft / PostgreSQL 16+ proposal. DBA review required before production.
-- IDs are supplied by application services. All tenant-bound queries must enforce tenant_id.
BEGIN;

CREATE TABLE IF NOT EXISTS alert_sources (
    tenant_id UUID NOT NULL,
    source_id UUID NOT NULL,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    vendor_version TEXT NOT NULL,
    adapter_version TEXT NOT NULL,
    cluster_id TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen_at TIMESTAMPTZ,
    health_status TEXT NOT NULL DEFAULT 'unknown',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source_id),
    CONSTRAINT alert_sources_type_ck CHECK (source_type IN ('deepflow','alertmanager','huatuo','custom'))
);

-- One row per transport occurrence. Receipts enforce idempotency across Edge retransmits.
-- PK is transport-level; the partial unique index below additionally enforces
-- event-key idempotency ((tenant, source, source_event_key), see docs/03 §2)
-- for sources that provide a stable event id. Transport and event identity
-- are deliberately two different dedup layers and must not be conflated.
CREATE TABLE IF NOT EXISTS event_receipts (
    tenant_id UUID NOT NULL,
    source_id UUID NOT NULL,
    transport_id TEXT NOT NULL,
    source_event_key TEXT,
    first_received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source_id, transport_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_receipts_source_event_key
    ON event_receipts(tenant_id, source_id, source_event_key)
    WHERE source_event_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_receipts_last_seen ON event_receipts(last_seen_at);

-- Logical monitor state: one row per (fingerprint, episode).
CREATE TABLE IF NOT EXISTS alert_instances (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    source_id UUID NOT NULL,
    fingerprint TEXT NOT NULL,
    episode_key TEXT NOT NULL,
    name TEXT NOT NULL,
    severity TEXT NOT NULL,
    monitor_state TEXT NOT NULL,
    cluster_id TEXT,
    entity_type TEXT,
    entity_uid TEXT,
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    starts_at TIMESTAMPTZ,
    ends_at TIMESTAMPTZ,
    last_observed_at TIMESTAMPTZ NOT NULL,
    last_transition_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_alert_episode UNIQUE (tenant_id, fingerprint, episode_key),
    CONSTRAINT alert_state_ck CHECK (monitor_state IN ('pending','firing','resolved','stale'))
);
CREATE INDEX IF NOT EXISTS idx_alert_active
    ON alert_instances (tenant_id, monitor_state, severity, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_entity
    ON alert_instances (tenant_id, cluster_id, entity_uid, updated_at DESC);

CREATE TABLE IF NOT EXISTS alert_transitions (
    transition_id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    alert_instance_id UUID NOT NULL REFERENCES alert_instances(id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    event_id TEXT NOT NULL,
    transition_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_alert_transition_event UNIQUE (alert_instance_id, event_id, to_state)
);
CREATE INDEX IF NOT EXISTS idx_transitions_instance
    ON alert_transitions (tenant_id, alert_instance_id, transition_at DESC);

CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    correlation_key TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    owner_team_id TEXT,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at TIMESTAMPTZ,
    close_reason TEXT,
    CONSTRAINT incident_status_ck CHECK (status IN ('open','acknowledged','mitigated','closed'))
);
CREATE INDEX IF NOT EXISTS idx_incidents_active
    ON incidents(tenant_id,status,updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_incidents_correlation
    ON incidents(tenant_id,correlation_key,created_at DESC);

CREATE TABLE IF NOT EXISTS incident_alerts (
    tenant_id UUID NOT NULL,
    incident_id UUID NOT NULL REFERENCES incidents(id),
    alert_instance_id UUID NOT NULL REFERENCES alert_instances(id),
    relation_type TEXT NOT NULL DEFAULT 'associated',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(incident_id,alert_instance_id)
);

CREATE TABLE IF NOT EXISTS policy_versions (
    id UUID NOT NULL,
    tenant_id UUID NOT NULL,
    version BIGINT NOT NULL,
    policy_type TEXT NOT NULL,
    spec JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id,id,version)
);

CREATE TABLE IF NOT EXISTS silences (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    matchers JSONB NOT NULL,
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    created_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    -- version/updated_at make silence evaluation traceable the same way
    -- policy_version does for routing (docs/04 §5).
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT silence_valid_range_ck CHECK (ends_at > starts_at)
);

-- subject_* columns: notifications bind to an alert instance until the
-- minimal 1:1 Incident entity lands (W08+), then to the incident. The
-- idempotency key includes subject_type/subject_id so pre-incident and
-- post-incident notifications can never collide (docs/04 §5).
CREATE TABLE IF NOT EXISTS notification_outbox (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    subject_type TEXT NOT NULL DEFAULT 'alert_instance',
    alert_instance_id UUID REFERENCES alert_instances(id),
    incident_id UUID REFERENCES incidents(id),
    idempotency_key TEXT NOT NULL,
    channel_id UUID NOT NULL,
    receiver_id TEXT NOT NULL,
    policy_version BIGINT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id,idempotency_key),
    CONSTRAINT notify_subject_type_ck CHECK (subject_type IN ('alert_instance','incident')),
    CONSTRAINT notify_subject_presence_ck CHECK (
        (subject_type = 'alert_instance' AND alert_instance_id IS NOT NULL AND incident_id IS NULL)
        OR (subject_type = 'incident' AND incident_id IS NOT NULL)
    ),
    CONSTRAINT notify_status_ck CHECK (status IN ('pending','sending','sent','retry_scheduled','failed','suppressed'))
);
CREATE INDEX IF NOT EXISTS idx_notification_claim
    ON notification_outbox(status,next_attempt_at,lease_until)
    WHERE status IN ('pending','retry_scheduled','sending');

CREATE TABLE IF NOT EXISTS notification_attempts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    task_id UUID NOT NULL REFERENCES notification_outbox(id),
    attempt_no INT NOT NULL,
    http_status_code INT,
    failure_code TEXT,
    error_message TEXT,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(task_id,attempt_no)
);

-- Independent transactional domain events relay; do not publish to Kafka before DB commit.
CREATE TABLE IF NOT EXISTS domain_outbox (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_domain_outbox_pending
    ON domain_outbox(status,next_attempt_at)
    WHERE status='pending';

CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    actor_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_logs_tenant_time ON audit_logs(tenant_id,created_at DESC);

COMMIT;
-- TODO (before prod): partition history/audit, RLS enforcement decision, FK tenant checks,
-- cross-tenant FK guarantees, retention policy, PITR/backup, index selectivity and hot-row performance.
