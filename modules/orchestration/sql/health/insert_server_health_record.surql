-- Statement 0 is BEGIN; the RETURN is statement 5. `$before` is read ahead of
-- the UPDATE on purpose: a row written earlier in the same transaction is not
-- guaranteed to be visible to a later read.
BEGIN TRANSACTION;
LET $before = (SELECT VALUE health_status FROM ONLY $server);
LET $matched = UPDATE $server
    SET last_health_report_at = $report_time, last_seen_at = $report_time,
        health_status = $status
    WHERE refresh_key_generation = $generation RETURN AFTER;
LET $rec = IF array::len($matched) > 0 {
    (CREATE ONLY server_health_record SET server = $server, status = $status,
        report_time = $report_time, upload_bytes = $upload_bytes,
        download_bytes = $download_bytes,
        current_connections = $current_connections,
        max_connections = $max_connections RETURN AFTER)
} ELSE { NONE };
LET $node_rows = IF array::len($matched) > 0 AND array::len($nodes) > 0 {
    (INSERT INTO node_health_record $nodes RETURN AFTER)
} ELSE { [] };
RETURN IF array::len($matched) > 0 {
    { record: $rec, canvas: $matched[0].canvas,
      previous_status: $before, nodes: $node_rows }
} ELSE { NONE };
COMMIT TRANSACTION;
