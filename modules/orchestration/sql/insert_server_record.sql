INSERT INTO server_health_record
    (id, server, status, report_time, upload_bytes, download_bytes,
     current_connections, max_connections)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id AS "id: ServerHealthRecordId", server AS "server: ServerId",
          status AS "status: ServerHealthStatus", report_time,
          upload_bytes, download_bytes, current_connections, max_connections
