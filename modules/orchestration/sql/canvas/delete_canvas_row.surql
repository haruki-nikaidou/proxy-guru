-- Deletes a canvas tree. An imported canvas cannot be deleted: the service
-- reports the importer to the operator, the THROW is the race guard.
BEGIN TRANSACTION;
IF (SELECT VALUE id FROM orchestration_node WHERE spec.config.canvas = $id LIMIT 1)[0] != NONE {
    THROW "canvas is imported by another canvas";
};
LET $tree = fn::orchestration_tree($id, 0);
LET $servers = (SELECT VALUE id FROM orchestration_server WHERE canvas IN $tree);
LET $nodes = (SELECT VALUE id FROM orchestration_node WHERE canvas IN $tree);
LET $ports = (SELECT VALUE id FROM orchestration_port WHERE owner IN $nodes);
DELETE orchestration_edge_connection WHERE in IN $ports OR out IN $ports;
DELETE orchestration_port WHERE id IN $ports;
DELETE node_health_record WHERE node IN $nodes;
DELETE relay_certificate WHERE pod IN $nodes;
DELETE orchestration_node WHERE id IN $nodes;
DELETE orchestration_server_config_view WHERE server IN $servers;
DELETE server_health_record WHERE server IN $servers;
DELETE orchestration_server WHERE id IN $servers;
DELETE orchestration_canvas WHERE id IN $tree;
COMMIT TRANSACTION;
