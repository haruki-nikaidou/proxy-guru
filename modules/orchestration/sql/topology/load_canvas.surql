-- Everything one canvas *tree* is made of, in one consistent read. Any canvas
-- of the tree may be given; the read always covers the whole tree from its root.
-- Statement order is load-bearing: `group_rows` takes results by index.
-- Statement 0 is BEGIN, 1-2 the LETs; the canvases are statement 3, the four
-- row reads 4-7, and the root is returned at 8.
BEGIN TRANSACTION;
LET $root = fn::orchestration_root($canvas);
LET $tree = fn::orchestration_tree($root, 0);
SELECT * FROM orchestration_canvas WHERE id IN $tree;
SELECT * FROM orchestration_server WHERE canvas IN $tree;
SELECT * FROM orchestration_node WHERE canvas IN $tree;
SELECT * FROM orchestration_port WHERE owner.canvas IN $tree ORDER BY position;
SELECT * FROM orchestration_edge_connection WHERE in.owner.canvas IN $tree;
RETURN $root;
COMMIT TRANSACTION;
