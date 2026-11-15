-- 0010 down: drop the edge uplink credential table.
BEGIN;

DROP INDEX IF EXISTS idx_edge_uplink_credentials_token;
DROP TABLE IF EXISTS edge_uplink_credentials;

COMMIT;