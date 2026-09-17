WITH RECURSIVE tree (root, canvas, depth) AS (
    SELECT id, id, 0 FROM orchestration_canvas
  UNION ALL
    SELECT tree.root, c.id, tree.depth + 1
    FROM tree JOIN orchestration_canvas c ON c.parent = tree.canvas
    WHERE tree.depth < 32
), view_sum AS (
    SELECT tree.root, COALESCE(SUM(v.seq), 0) AS seq_sum
    FROM tree
    JOIN orchestration_server s ON s.canvas = tree.canvas
    JOIN orchestration_server_config_view v ON v.server = s.id
    GROUP BY tree.root
)
SELECT c.id AS "id: CanvasId" FROM orchestration_canvas c
LEFT JOIN view_sum ON view_sum.root = c.id
WHERE c.generation > c.derived_generation
   OR c.derived_view_seq < COALESCE(view_sum.seq_sum, 0)
ORDER BY c.id
