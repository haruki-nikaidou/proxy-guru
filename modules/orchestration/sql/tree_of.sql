WITH RECURSIVE tree (canvas, depth) AS (
    SELECT $1::text, 0
  UNION ALL
    SELECT c.id, tree.depth + 1
    FROM tree JOIN orchestration_canvas c ON c.parent = tree.canvas
    WHERE tree.depth < $2
)
-- Both columns are non-null by construction: the given canvas, then the ids of
-- the rows below it, with the depth that counts them.
SELECT canvas AS "canvas!: CanvasId", depth AS "depth!" FROM tree ORDER BY depth, canvas
