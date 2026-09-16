-- Refuses while any pod is still placed on this server: a pod whose server is
-- gone belongs to nobody, and derivation could then neither publish nor report
-- it. The `spec.config.server` assertion cannot cover this — a field assertion
-- is not re-evaluated when its target is deleted — and a service-layer pre-check
-- runs in an earlier transaction, so the check belongs here. The server's own
-- universal pod is not a pod in that sense: it was created with the server and
-- goes with it, once nothing is bundled into or out of it.
BEGIN TRANSACTION;
LET $used = (SELECT VALUE id FROM orchestration_node
    WHERE spec.config.server = $id AND spec.type != "universal_pod");
IF array::len($used) > 0 {
    THROW "server still has " + <string> array::len($used) + " live pod(s)";
};
LET $universal = (SELECT VALUE id FROM orchestration_node
    WHERE spec.config.server = $id AND spec.type == "universal_pod");
LET $ports = (SELECT VALUE id FROM orchestration_port WHERE owner IN $universal);
IF array::len((SELECT VALUE id FROM orchestration_edge_connection WHERE in IN $ports OR out IN $ports)) > 0 {
    THROW "server's universal pod is still bundled; disconnect its bundles first";
};
DELETE orchestration_port WHERE id IN $ports;
DELETE node_health_record WHERE node IN $universal;
DELETE orchestration_node WHERE id IN $universal;
DELETE orchestration_server_config_view WHERE server = $id;
DELETE server_health_record WHERE server = $id;
DELETE $id;
fn::orchestration_touch_checked($canvas, $expected_root, $expected);
COMMIT TRANSACTION;
