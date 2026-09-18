-- The batch form of `observe_server_status.sql`: records every pod's status and
-- answers with the one it replaced. A pod that no longer exists is skipped by
-- the join and reads as a first sighting.
WITH given AS (
    SELECT * FROM UNNEST($1::text[], $2::text[]) AS t(pod, status)
), before AS (
    SELECT s.pod, s.status
    FROM notify_pod_state s
    JOIN given g ON g.pod = s.pod
), upsert AS (
    INSERT INTO notify_pod_state (pod, status, changed_at)
    SELECT g.pod, g.status, $3
    FROM given g
    JOIN orchestration_pod p ON p.id = g.pod
    ON CONFLICT (pod) DO UPDATE
        SET status = EXCLUDED.status, changed_at = EXCLUDED.changed_at
        WHERE notify_pod_state.status <> EXCLUDED.status
    RETURNING pod
)
SELECT g.pod AS "pod!: PodId", b.status AS "previous?: PodHealthStatus"
FROM given g
LEFT JOIN before b ON b.pod = g.pod
