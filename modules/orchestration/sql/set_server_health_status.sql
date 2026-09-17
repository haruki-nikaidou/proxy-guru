UPDATE orchestration_server
SET health_status = $2,
    session_lease_until = CASE WHEN $3 THEN NULL ELSE session_lease_until END,
    watch_epoch = CASE WHEN $3 THEN watch_epoch + 1 ELSE watch_epoch END
WHERE id = $1
