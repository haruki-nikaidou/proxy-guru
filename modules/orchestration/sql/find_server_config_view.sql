SELECT id           AS "id: ServerConfigViewId",
       server       AS "server: ServerId",
       desired      AS "desired: Json<ConfigSnapshot>",
       in_flight    AS "in_flight: Json<ConfigSnapshot>",
       applied      AS "applied: Json<ConfigSnapshot>",
       failed_revision,
       apply_error,
       failed_pods  AS "failed_pods: Json<Vec<PodFailure>>",
       derive_error,
       invalid_pods AS "invalid_pods: Json<Vec<InvalidPod>>",
       waiting_for  AS "waiting_for: Vec<ServerId>",
       seq
FROM orchestration_server_config_view
WHERE server = $1
