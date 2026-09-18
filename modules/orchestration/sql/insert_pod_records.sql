-- Writes the batch and reads back each row's labels in the same statement: the
-- pod's name, its canvas, and the canvas's name, so a notification can be
-- phrased without a second read (which could see a pod already moved).
WITH ins AS (
    INSERT INTO pod_health_record (id, pod, status, message, report_time)
    SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[])
    RETURNING id, pod, status, message, report_time
)
SELECT ins.id AS "id!: PodHealthRecordId", ins.pod AS "pod!: PodId",
       ins.status AS "status!: PodHealthStatus", ins.message AS "message!",
       ins.report_time AS "report_time!",
       p.name AS "pod_name!", p.canvas AS "canvas!: CanvasId", c.name AS "canvas_name!"
FROM ins
JOIN orchestration_pod p ON p.id = ins.pod
JOIN orchestration_canvas c ON c.id = p.canvas
