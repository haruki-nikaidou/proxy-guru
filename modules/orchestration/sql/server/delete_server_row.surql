-- The cascade removes this server's ip records, so it must refuse while any pod
-- still listens on one of them: a pod whose ip record is gone belongs to no
-- server, and derivation could then neither publish nor report it. The
-- `spec.config.ip` assertion cannot cover this — a field assertion is not
-- re-evaluated when its target is deleted — and a service-layer pre-check runs
-- in an earlier transaction, so the check belongs here.
BEGIN TRANSACTION;
LET $used = (SELECT VALUE id FROM orchestration_node WHERE spec.config.ip.server = $id);
IF array::len($used) > 0 {
    THROW "server still has " + <string> array::len($used) + " live pod(s)";
};
DELETE orchestration_server_config_view WHERE server = $id;
DELETE server_health_record WHERE server = $id;
DELETE server_ip_record WHERE server = $id;
DELETE $id;
fn::orchestration_touch($canvas);
COMMIT TRANSACTION;
