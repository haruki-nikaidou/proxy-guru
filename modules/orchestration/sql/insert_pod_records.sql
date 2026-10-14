-- Writes the batch and reads back each row's labels in the same statement: the
-- pod's name, its canvas, and the canvas's name, so a notification can be
-- phrased without a second read (which could see a pod already moved).
--
-- A row whose pod no longer exists is skipped, not an error: a pod is deleted
-- the moment the graph is applied, while its server runs — and reports — the
-- revision that has it until the next one is applied. The pods that remain are
-- locked first (`FOR KEY SHARE`, the lock the foreign-key check takes anyway),
-- so a delete committed after this statement's snapshot is skipped here too
-- instead of failing that check, and a later one waits for this transaction.
WITH live AS (
    SELECT r.id, r.pod, r.status, r.message, r.report_time
    FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[])
         AS r(id, pod, status, message, report_time)
    JOIN orchestration_pod p ON p.id = r.pod
    FOR KEY SHARE OF p
), ins AS (
    INSERT INTO pod_health_record (id, pod, status, message, report_time)
    SELECT id, pod, status, message, report_time FROM live
    RETURNING id, pod, status, message, report_time
)
SELECT ins.id AS "id!: PodHealthRecordId", ins.pod AS "pod!: PodId",
       ins.status AS "status!: PodHealthStatus", ins.message AS "message!",
       ins.report_time AS "report_time!",
       p.name AS "pod_name!", p.canvas AS "canvas!: CanvasId", c.name AS "canvas_name!"
FROM ins
JOIN orchestration_pod p ON p.id = ins.pod
JOIN orchestration_canvas c ON c.id = p.canvas
