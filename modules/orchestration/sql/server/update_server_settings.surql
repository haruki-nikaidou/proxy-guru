BEGIN TRANSACTION;
UPDATE $id SET name = $name, icon = $icon, comment = $comment,
    ipv6_resolve = $ipv6_resolve, log_level = $log_level,
    override_v4 = $override_v4, override_v6 = $override_v6, extra_addresses = $extra_addresses,
    agent_unit = $agent_unit
    RETURN AFTER;
fn::orchestration_touch_checked($canvas, $expected_root, $expected);
COMMIT TRANSACTION;
