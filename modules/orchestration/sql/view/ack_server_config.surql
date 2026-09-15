-- Unlike `take_in_flight.surql`, this update carries no
-- `(refresh_key_generation, watch_epoch)` predicate, deliberately: an ack arrives
-- on a unary RPC authenticated by the refresh-key digest, and re-registration
-- rotates that digest while incrementing `refresh_key_generation`
-- (`sql/server/register_worker_session.surql`, lines 2-3), so a fenced-out worker
-- cannot authenticate an ack at all. `take_in_flight` runs on behalf of a
-- long-lived watch stream whose credential was validated once, at stream open, so
-- it needs the predicate. The generation comparison in `AckConfig`
-- (`services/agent.rs`) is the fail-fast counterpart on this path.
--
-- Only this server's view row is written. `seq += 1` is what tells derivation
-- something changed; the canvas row is left alone so that N workers acking
-- one revision in the same instant never collide on it.
BEGIN TRANSACTION;
LET $ok = IF $error != NONE {
    UPDATE orchestration_server_config_view
        SET in_flight = NONE, apply_error = $error, failed_revision = $revision, failed_pods = [],
            seq += 1
        WHERE server = $server AND in_flight.revision = $revision RETURN AFTER
} ELSE IF $applied != NONE {
    -- Some pods failed: `applied` is the mix the worker runs (the revision for the
    -- pods that took it, each failed pod's previous shape), synthesised by the
    -- service. `failed_revision` is the acked revision, or NONE if nothing failed.
    UPDATE orchestration_server_config_view
        SET applied = $applied, in_flight = NONE, apply_error = NONE,
            failed_revision = $failed_revision, failed_pods = $failed_pods, seq += 1
        WHERE server = $server AND in_flight.revision = $revision RETURN AFTER
} ELSE {
    UPDATE orchestration_server_config_view
        SET applied = in_flight, in_flight = NONE, apply_error = NONE, failed_revision = NONE, failed_pods = [],
            seq += 1
        WHERE server = $server AND in_flight.revision = $revision RETURN AFTER
};
RETURN array::len($ok) > 0;
COMMIT TRANSACTION;
