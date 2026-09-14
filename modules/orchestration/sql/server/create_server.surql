BEGIN TRANSACTION;
LET $server = (CREATE ONLY orchestration_server CONTENT {
    canvas: $canvas, name: $name, icon: $icon, comment: $comment,
    position: $position, ipv6_resolve: $ipv6_resolve, log_level: $log_level,
    current_dynamic_refresh_key: NONE, refresh_key_generation: 0, watch_epoch: 0,
    session_lease_until: NONE, last_seen_at: NONE,
    override_v4: $override_v4, override_v6: $override_v6, extra_addresses: $extra_addresses,
    reported_addresses: NONE, observed_address: NONE, observed_at: NONE
});
CREATE orchestration_server_config_view CONTENT {
    server: $server.id, desired: NONE, in_flight: NONE, applied: NONE,
    failed_revision: NONE, apply_error: NONE, derive_error: NONE,
    invalid_pods: [], waiting_for: []
};
fn::orchestration_touch($canvas);
RETURN $server;
COMMIT TRANSACTION;
