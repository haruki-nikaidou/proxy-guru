-- Refuses while any pod is still placed on this server: a pod whose server is
-- gone belongs to nobody, and derivation could then neither publish nor report
-- it. The `spec.config.server` assertion cannot cover this — a field assertion
-- is not re-evaluated when its target is deleted — and a service-layer pre-check
-- runs in an earlier transaction, so the check belongs here.
BEGIN TRANSACTION;
LET $used = (SELECT VALUE id FROM orchestration_node WHERE spec.config.server = $id);
IF array::len($used) > 0 {
    THROW "server still has " + <string> array::len($used) + " live pod(s)";
};
DELETE orchestration_server_config_view WHERE server = $id;
DELETE server_health_record WHERE server = $id;
DELETE $id;
fn::orchestration_touch($canvas);
COMMIT TRANSACTION;
