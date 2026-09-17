SELECT v.id           AS "id: ServerConfigViewId",
       v.server       AS "server: ServerId",
       v.desired      AS "desired: Json<ConfigSnapshot>",
       v.in_flight    AS "in_flight: Json<ConfigSnapshot>",
       v.applied      AS "applied: Json<ConfigSnapshot>",
       v.failed_revision,
       v.apply_error,
       v.failed_pods  AS "failed_pods: Json<Vec<PodFailure>>",
       v.derive_error,
       v.invalid_pods AS "invalid_pods: Json<Vec<InvalidPod>>",
       v.waiting_for  AS "waiting_for: Vec<ServerId>",
       v.seq
FROM orchestration_server_config_view v
JOIN orchestration_server s ON s.id = v.server
WHERE s.canvas = ANY($1)
ORDER BY v.server
