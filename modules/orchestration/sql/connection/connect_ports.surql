BEGIN TRANSACTION;
LET $edge = (RELATE ONLY $source->orchestration_edge_connection->$target);
fn::orchestration_touch_checked($canvas, $expected_root, $expected);
RETURN $edge;
COMMIT TRANSACTION;
