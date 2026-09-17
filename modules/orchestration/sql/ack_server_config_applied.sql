UPDATE orchestration_server_config_view
SET applied = in_flight, in_flight = NULL, apply_error = NULL,
    failed_revision = NULL, failed_pods = '[]', seq = seq + 1
WHERE server = $1 AND (in_flight ->> 'revision')::bigint = $2
RETURNING id AS "id: ServerConfigViewId"
