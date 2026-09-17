UPDATE orchestration_server_config_view
SET desired = COALESCE($2, desired), derive_error = $3, invalid_pods = $4,
    waiting_for = $5,
    failed_revision = CASE WHEN $6 THEN NULL ELSE failed_revision END,
    apply_error = CASE WHEN $6 THEN NULL ELSE apply_error END
WHERE server = $1
