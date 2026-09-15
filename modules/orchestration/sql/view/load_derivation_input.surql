-- One consistent read of everything a derivation pass needs, for the whole tree
-- containing $canvas. Only the root's generation counters fence the pass.
-- Statement order is load-bearing: `LoadCanvasDerivationInput` takes results by
-- index. 0 BEGIN, 1-2 LETs, 3 the root's counters, 4 the canvases, 5-8 the four
-- row reads `group_rows` expects, 9 the views, 10 the root.
BEGIN TRANSACTION;
LET $root = fn::orchestration_root($canvas);
LET $tree = fn::orchestration_tree($root, 0);
SELECT generation, derived_generation, derived_view_seq FROM ONLY $root;
SELECT * FROM orchestration_canvas WHERE id IN $tree;
SELECT * FROM orchestration_server WHERE canvas IN $tree;
SELECT * FROM orchestration_node WHERE canvas IN $tree;
SELECT * FROM orchestration_port WHERE owner.canvas IN $tree ORDER BY position;
SELECT * FROM orchestration_edge_connection WHERE in.owner.canvas IN $tree;
SELECT * FROM orchestration_server_config_view WHERE server.canvas IN $tree;
RETURN $root;
COMMIT TRANSACTION;
