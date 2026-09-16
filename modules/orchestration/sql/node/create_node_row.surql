-- Statement 0 is BEGIN, 1 the import guard, 2 the CREATE, 3 the port insert,
-- 4 the import target write, 5 the import sync, 6 the touch; the RETURN is
-- statement 7.
BEGIN TRANSACTION;
IF $spec.type == "canvas_import" {
    IF $spec.config.canvas IN array::concat([$canvas], fn::orchestration_ancestors($canvas, 0)) {
        THROW "a canvas cannot import itself or an ancestor";
    };
    IF (SELECT VALUE id FROM orchestration_node WHERE spec.config.canvas = $spec.config.canvas LIMIT 1)[0] != NONE {
        THROW "canvas is already imported";
    };
};
LET $node = (CREATE ONLY orchestration_node CONTENT {
    canvas: $canvas, name: $name, comment: $comment, spec: $spec,
    position: $position
});
LET $ports = (INSERT INTO orchestration_port
    (SELECT $node.id AS owner, kind, direction, key, position FROM $new_ports)
    RETURN AFTER);
-- The target stops being a root: its own pending derivation collapses into this
-- tree's. Writing its row also makes two concurrent imports of each other
-- conflict instead of forming a cycle. Written after the CREATE: the field
-- assertion on `spec.config.canvas` reads the target row, and older engines do
-- not see a row modified earlier in the same transaction.
IF $spec.type == "canvas_import" {
    IF $target_expected == NONE {
        UPDATE $spec.config.canvas SET derived_generation = generation;
    } ELSE {
        fn::orchestration_claim_root($spec.config.canvas, $target_expected);
    };
};
-- An export node's mirrored port on the importer is keyed by the export's
-- record key, which only exists now: swap the placeholder for it.
IF $import_sync != NONE {
    LET $synced = $import_sync.ports.map(|$p|
        IF $p.key == "pending-node" {
            { key: record::id($node.id), kind: $p.kind, direction: $p.direction, position: $p.position }
        } ELSE { $p });
    fn::orchestration_reshape_ports($import_sync.node, $synced);
};
fn::orchestration_touch_checked($canvas, $expected_root, $expected);
RETURN { node: $node, ports: $ports };
COMMIT TRANSACTION;
