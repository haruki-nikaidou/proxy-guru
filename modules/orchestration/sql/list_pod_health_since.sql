SELECT id AS "id: PodHealthRecordId", pod AS "pod: PodId",
       status AS "status: PodHealthStatus", message, report_time
FROM (
    SELECT id, pod, status, message, report_time
    FROM pod_health_record
    WHERE pod = $1 AND report_time >= $2 AND report_time <= $3
    ORDER BY report_time DESC, id DESC
    LIMIT $4
) newest
ORDER BY report_time ASC, id ASC
