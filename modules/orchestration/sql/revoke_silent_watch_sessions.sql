UPDATE orchestration_server
SET session_lease_until = NULL, watch_epoch = watch_epoch + 1
WHERE health_status = 'offline'
  AND session_lease_until > $1
  AND registered_at < $2
  AND (last_health_report_at IS NULL OR last_health_report_at < registered_at)
RETURNING id AS "id: ServerId"
