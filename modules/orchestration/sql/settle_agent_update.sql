UPDATE orchestration_server
SET agent_update_requested = NULL,
    agent_update_error = CASE WHEN $2::text IS NOT NULL THEN $2 ELSE NULL END
WHERE id = $1
  AND ($2::text IS NOT NULL
       OR (agent_update_requested IS NOT NULL AND agent_update_requested = $3))
