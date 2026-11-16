-- 0012 down: drop the archive cursor.
BEGIN;

DROP TABLE IF EXISTS archive_cursors;

COMMIT;