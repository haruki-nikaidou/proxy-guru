SELECT v.server                            AS "id: ServerId",
       s.refresh_key_generation,
       s.watch_epoch,
       (v.desired ->> 'revision')::bigint   AS desired_revision,
       (v.in_flight ->> 'revision')::bigint AS in_flight_revision,
       (v.applied ->> 'revision')::bigint   AS applied_revision,
       v.failed_revision
FROM orchestration_server_config_view v
JOIN orchestration_server s ON s.id = v.server
WHERE v.server = ANY($1)
