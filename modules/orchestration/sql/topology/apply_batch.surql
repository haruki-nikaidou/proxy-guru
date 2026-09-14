-- One reconciling write: what a universal-node edit and the lane regeneration it
-- implies do to a canvas tree, in one transaction. Statement order is
-- load-bearing: rows are deleted first so a recreated lane never clashes with
-- the one it replaces, specs and ports are reshaped next, new lane nodes are
-- created with their ports, and only then are edges related — an edge may name
-- a port that did not exist when the batch was built, either on a node that was
-- just reshaped (`{ node, key }`) or on a lane node just created
-- (`{ lane_key, key }`), so endpoints are resolved here, inside the transaction.
BEGIN TRANSACTION;
DELETE orchestration_edge_connection WHERE id IN $delete_edges;
FOR $node IN $delete_nodes {
    LET $ports = (SELECT VALUE id FROM orchestration_port WHERE owner = $node);
    DELETE orchestration_edge_connection WHERE in IN $ports OR out IN $ports;
    DELETE orchestration_port WHERE id IN $ports;
    DELETE node_health_record WHERE node = $node;
    DELETE relay_certificate WHERE pod = $node;
    DELETE $node;
};
FOR $u IN $set_specs {
    UPDATE $u.node SET spec = $u.spec;
};
FOR $r IN $reshape {
    fn::orchestration_reshape_ports($r.node, $r.ports);
};
FOR $n IN $create_nodes {
    LET $node = (CREATE ONLY orchestration_node CONTENT {
        canvas: $n.canvas, name: $n.name, comment: $n.comment, spec: $n.spec,
        position: $n.position, lane: $n.lane
    });
    INSERT INTO orchestration_port
        (SELECT $node.id AS owner, kind, direction, key, position FROM $n.ports);
};
FOR $e IN $add_edges {
    LET $source = fn::orchestration_resolve_port($canvas, $e.source);
    LET $target = fn::orchestration_resolve_port($canvas, $e.target);
    IF $source == NONE OR $target == NONE {
        THROW "batch edge endpoint could not be resolved";
    };
    RELATE $source->orchestration_edge_connection->$target;
};
fn::orchestration_touch($canvas);
COMMIT TRANSACTION;
