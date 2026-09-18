-- Records the status and answers with the one it replaced.
--
-- Every sub-statement of a `WITH` sees the same snapshot, so `before` reads the
-- row as it was even though `upsert` rewrites it. The upsert's own `WHERE`
-- leaves `changed_at` alone when the status did not move, and the insert is
-- conditional on the server still existing: a deleted one would otherwise fail
-- the foreign key and requeue the delivery forever.
WITH before AS (
    SELECT status FROM notify_server_state WHERE server = $1
), upsert AS (
    INSERT INTO notify_server_state (server, status, changed_at)
    SELECT $1, $2, $3
    WHERE EXISTS (SELECT 1 FROM orchestration_server WHERE id = $1)
    ON CONFLICT (server) DO UPDATE
        SET status = EXCLUDED.status, changed_at = EXCLUDED.changed_at
        WHERE notify_server_state.status <> EXCLUDED.status
    RETURNING server
)
SELECT (SELECT status FROM before) AS "previous?: ServerHealthStatus"
