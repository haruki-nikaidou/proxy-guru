-- Statement 0 is BEGIN, 1-2 the LETs; the RETURN is statement 3.
-- `UPDATE` (not `UPDATE ONLY`) on purpose: the fence is the `WHERE`, and a
-- refused claim has to come back as an empty result, not an error.
BEGIN TRANSACTION;
LET $id = type::record('orchestration_job_run', $job);
LET $claimed = IF record::exists($id) {
    (UPDATE $id SET last_run_at = $now, last_signal_tick = $tick
        WHERE last_signal_tick < $tick
          AND last_signal_tick <= $tick_not_before
        RETURN AFTER)
} ELSE {
    (CREATE $id CONTENT { last_run_at: $now, last_signal_tick: $tick }
        RETURN AFTER)
};
RETURN array::len($claimed) > 0;
COMMIT TRANSACTION;
