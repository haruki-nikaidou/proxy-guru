BEGIN TRANSACTION;
LET $ports = (SELECT VALUE id FROM orchestration_port WHERE owner = $id);
DELETE orchestration_edge_connection WHERE in IN $ports OR out IN $ports;
DELETE orchestration_port WHERE id IN $ports;
DELETE node_health_record WHERE node = $id;
DELETE relay_certificate WHERE pod = $id;
DELETE $id;
-- Retiring an export node drops its mirrored port (and that port's edge) on the
-- importing node.
IF $import_sync != NONE {
    fn::orchestration_reshape_ports($import_sync.node, $import_sync.ports);
};
fn::orchestration_touch_checked($canvas, $expected_root, $expected);
-- Retiring an import node makes its target a root again, with a derivation
-- pass of its own to run.
IF $frees != NONE {
    UPDATE $frees SET generation += 1;
};
COMMIT TRANSACTION;
