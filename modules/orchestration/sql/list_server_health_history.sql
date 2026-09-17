SELECT id AS "id: ServerHealthRecordId", server AS "server: ServerId",
       status AS "status: ServerHealthStatus", report_time,
       upload_bytes, download_bytes, current_connections, max_connections
FROM server_health_record
WHERE server = $1 AND report_time >= $2 AND report_time <= $3
ORDER BY report_time ASC, id ASC
