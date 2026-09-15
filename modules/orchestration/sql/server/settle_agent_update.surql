BEGIN TRANSACTION;
-- `$error` set: the attempt failed, the request is over and the reason is kept
-- for the dashboard. `$reported` equal to the requested version: the worker now
-- runs it, so the request is over and any stale reason goes with it. Anything
-- else leaves the row alone.
LET $requested = (SELECT VALUE agent_update_requested FROM ONLY $id);
IF $error != NONE {
    UPDATE $id SET agent_update_requested = NONE, agent_update_error = $error;
} ELSE IF $requested != NONE AND $reported = $requested {
    UPDATE $id SET agent_update_requested = NONE, agent_update_error = NONE;
};
COMMIT TRANSACTION;
