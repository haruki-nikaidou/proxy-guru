UPDATE orchestration_server_config_view
SET applied = $3, in_flight = NULL, apply_error = NULL,
    failed_revision = $4, failed_pods = $5, seq = seq + 1
WHERE server = $1 AND (in_flight ->> 'revision')::bigint = $2
RETURNING id AS "id: ServerConfigViewId"
