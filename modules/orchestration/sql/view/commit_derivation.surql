-- `$canvas` is always the root of the derived tree.
-- Statement 0 is BEGIN, 1 the LET, 2 the IF; the RETURN is statement 3.
BEGIN TRANSACTION;
-- `generation` fences against edits made since the pass read the canvas.
-- `derived_generation < $generation OR derived_view_seq < $view_seq` fences
-- against a second pass that read the same state, so only the first one to
-- arrive publishes its revisions. `$view_seq` is the sum of the tree's view
-- `seq` counters as the pass read them: a worker write that lands after the
-- read leaves the sum above the stamp, so the canvas reads as stale again.
LET $fenced = (UPDATE $canvas
    SET derived_generation = $generation, derived_view_seq = $view_seq
    WHERE generation = $generation
        AND (derived_generation < $generation OR derived_view_seq < $view_seq)
    RETURN AFTER);
IF array::len($fenced) > 0 {
    FOR $u IN $updates {
        -- `invalid_pods` is written on every branch: a pod breaking or being fixed
        -- must be visible even when the served config is byte-identical.
        IF $u.desired != NONE {
            UPDATE orchestration_server_config_view
                SET desired = $u.desired, derive_error = $u.derive_error,
                    invalid_pods = $u.invalid_pods, waiting_for = $u.waiting_for
                WHERE server = $u.server;
        } ELSE {
            UPDATE orchestration_server_config_view
                SET derive_error = $u.derive_error, invalid_pods = $u.invalid_pods,
                    waiting_for = $u.waiting_for
                WHERE server = $u.server;
        };
        IF $u.clear_failure {
            UPDATE orchestration_server_config_view
                SET failed_revision = NONE, apply_error = NONE
                WHERE server = $u.server
        };
    };
    -- Subcanvas rows carry no generation of their own; keep them level so a
    -- canvas that was imported mid-edit can never look stale on its own.
    UPDATE orchestration_canvas
        SET derived_generation = generation, derived_view_seq = $view_seq
        WHERE id IN fn::orchestration_tree($canvas, 0) AND id != $canvas;
};
RETURN array::len($fenced) > 0;
COMMIT TRANSACTION;
