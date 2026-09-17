UPDATE orchestration_server_config_view
SET in_flight = NULL, apply_error = $3, failed_revision = $2, failed_pods = '[]',
    seq = seq + 1
WHERE server = $1 AND (in_flight ->> 'revision')::bigint = $2
RETURNING id AS "id: ServerConfigViewId"
