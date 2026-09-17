INSERT INTO pod_health_record (id, pod, status, message, report_time)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[])
RETURNING id AS "id: PodHealthRecordId", pod AS "pod: PodId",
          status AS "status: PodHealthStatus", message, report_time
