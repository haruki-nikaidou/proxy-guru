-- `IS DISTINCT FROM`: an empty `applied` or `failed_revision` must count as
-- "different", which `<>` against NULL would not.
--
-- The returned `in_flight` is the `desired` this statement just promoted, which
-- the `desired IS NOT NULL` predicate guarantees is there — hence `!`.
UPDATE orchestration_server_config_view v
SET in_flight = v.desired
FROM orchestration_server s
WHERE s.id = v.server AND v.server = $1
  AND v.in_flight IS NULL AND v.desired IS NOT NULL
  AND (v.desired ->> 'revision')::bigint IS DISTINCT FROM (v.applied ->> 'revision')::bigint
  AND (v.desired ->> 'revision')::bigint IS DISTINCT FROM v.failed_revision
  AND s.refresh_key_generation = $2 AND s.watch_epoch = $3
RETURNING v.in_flight AS "in_flight!: Json<ConfigSnapshot>"
