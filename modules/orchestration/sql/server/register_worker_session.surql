BEGIN TRANSACTION;
-- `observed_address` is what the master saw this registration come from;
-- `reported_addresses` and the worker's build (`agent_version`, `agent_arch`)
-- are replaced only when the worker sent them (`??` keeps the previous value
-- otherwise, so an older worker cannot blank a known version).
LET $rotated = (UPDATE $server SET current_dynamic_refresh_key = $digest,
    refresh_key_generation += 1, session_lease_until = $lease_until, last_seen_at = $now,
    observed_address = $observed, observed_at = $now,
    reported_addresses = $reported ?? reported_addresses,
    agent_version = $agent_version ?? agent_version,
    agent_arch = $agent_arch ?? agent_arch
    WHERE session_lease_until = NONE OR session_lease_until <= $now
    RETURN AFTER);
IF array::len($rotated) > 0 {
    LET $v = (SELECT * FROM ONLY orchestration_server_config_view WHERE server = $server LIMIT 1);
    IF $running_revision > 0 {
        IF $v.desired.revision = $running_revision {
            UPDATE $v.id SET applied = desired, apply_error = NONE, failed_revision = NONE;
        } ELSE IF $v.in_flight.revision = $running_revision {
            UPDATE $v.id SET applied = in_flight, apply_error = NONE, failed_revision = NONE;
        } ELSE IF $v.applied.revision != $running_revision {
            UPDATE $v.id SET apply_error = "running revision " + <string>$running_revision + " unknown";
        };
    } ELSE {
        -- A worker running nothing (a fresh install, a wiped state directory,
        -- a replay that failed) is not running what an earlier session applied:
        -- forget that, so the stream hands the desired revision out again
        -- instead of treating the server as converged, and dependants stop
        -- counting on listeners nobody serves.
        UPDATE $v.id SET applied = NONE, apply_error = NONE, failed_revision = NONE, failed_pods = [];
    };
    UPDATE $v.id SET in_flight = NONE;
    fn::orchestration_touch($canvas);
};
RETURN $rotated[0];
COMMIT TRANSACTION;
