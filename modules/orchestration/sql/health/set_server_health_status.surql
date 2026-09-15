-- Statement 0 is BEGIN; the RETURN is statement 4.
BEGIN TRANSACTION;
LET $before = (SELECT VALUE health_status FROM ONLY $server);
LET $matched = UPDATE $server SET health_status = $status
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
