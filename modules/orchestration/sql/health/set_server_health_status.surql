-- Statement 0 is BEGIN; the RETURN is statement 4.
-- Going offline also hands the watch session back: the lease is dropped so the
-- next `Register` is accepted at once, and the epoch moves so the stream that
-- held it — possibly a zombie behind a proxy that never noticed its worker die —
-- fails its next renew and ends. Any other status leaves both alone.
BEGIN TRANSACTION;
LET $before = (SELECT VALUE health_status FROM ONLY $server);
LET $matched = UPDATE $server SET health_status = $status,
    session_lease_until = IF $status = 'offline' { NONE } ELSE { session_lease_until },
    watch_epoch = IF $status = 'offline' { watch_epoch + 1 } ELSE { watch_epoch }
    WHERE health_status != $status
      AND ($generation = NONE OR refresh_key_generation = $generation)
    RETURN AFTER;
LET $rec = IF array::len($matched) > 0 {
    (CREATE ONLY server_health_record SET server = $server, status = $status,
        report_time = $now, upload_bytes = 0, download_bytes = 0,
        current_connections = 0, max_connections = 0 RETURN AFTER)
} ELSE { NONE };
RETURN IF array::len($matched) > 0 {
    { record: $rec, canvas: $matched[0].canvas,
      previous_status: $before, nodes: [] }
} ELSE { NONE };
COMMIT TRANSACTION;
