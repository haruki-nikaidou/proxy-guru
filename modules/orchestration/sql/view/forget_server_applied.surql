BEGIN TRANSACTION;
UPDATE orchestration_server_config_view
    SET applied = NONE, in_flight = NONE, apply_error = NONE, failed_revision = NONE, failed_pods = []
    WHERE server = $server;
fn::orchestration_touch($canvas);
COMMIT TRANSACTION;
