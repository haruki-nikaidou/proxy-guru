-- Statement 0 is BEGIN, 1 the spec update, 2 the reshape, 3 the import sync,
-- 4 the touch; the RETURN is statement 5.
BEGIN TRANSACTION;
UPDATE $id SET spec = $spec;
fn::orchestration_reshape_ports($id, $new_ports);
IF $import_sync != NONE {
    fn::orchestration_reshape_ports($import_sync.node, $import_sync.ports);
};
fn::orchestration_touch_checked($canvas, $expected_root, $expected);
RETURN {
    node: (SELECT * FROM ONLY $id),
    ports: (SELECT * FROM orchestration_port WHERE owner = $id ORDER BY position)
};
COMMIT TRANSACTION;
