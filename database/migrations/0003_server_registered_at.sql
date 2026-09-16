-- When the worker session holding `refresh_key_generation` registered. The
-- liveness sweep measures a session that never reported from here: an offline
-- server whose lease is still renewed for a registration older than the offline
-- threshold, with no report since, is held by a stream no worker is behind.
ALTER TABLE orchestration_server
    ADD COLUMN registered_at timestamptz;

-- Until now only a registration wrote `observed_at`, so it is exactly the time of
-- the last one.
UPDATE orchestration_server SET registered_at = observed_at;
