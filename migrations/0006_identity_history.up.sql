-- 0006 (INC-004/005): resource identity history + escalation records.
BEGIN;

-- UID history: the time-interval map behind "same IP at different times is
-- a DIFFERENT object" (T-015). Written when instances are created; intervals
-- close when the source reports the uid gone (best-effort, refreshed).
CREATE TABLE IF NOT EXISTS uid_history (
    tenant_id UUID NOT NULL,
    cluster_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_uid TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    valid_from TIMESTAMPTZ NOT NULL,
    valid_until TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, cluster_id, entity_type, entity_uid, valid_from)
);
CREATE INDEX IF NOT EXISTS idx_uid_history_name
    ON uid_history(tenant_id, cluster_id, entity_type, display_name);

-- Escalation records (INC-005): every 5-min un-ACKed stage bump.
CREATE TABLE IF NOT EXISTS incident_escalations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    incident_id UUID NOT NULL REFERENCES incidents(id),
    stage INT NOT NULL,
    policy_version BIGINT NOT NULL,
    notified_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_escalations_incident
    ON incident_escalations(tenant_id, incident_id, notified_at DESC);

-- Manual correlation links (INC-001): reversible, audited.
-- incident_alerts gains relation_type values 'manual' (app-managed).
ALTER TABLE incident_alerts DROP CONSTRAINT IF EXISTS incident_alerts_relation_type_ck;
ALTER TABLE incident_alerts
    ADD CONSTRAINT incident_alerts_relation_type_ck
    CHECK (relation_type IN ('associated','derived_1to1','manual','suggested','auto_correlated'));

COMMIT;
