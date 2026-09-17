WITH RECURSIVE up (canvas, depth) AS (
    SELECT parent, 1 FROM orchestration_canvas WHERE id = $1 AND parent IS NOT NULL
  UNION ALL
    SELECT c.parent, up.depth + 1
    FROM up JOIN orchestration_canvas c ON c.id = up.canvas
    WHERE c.parent IS NOT NULL AND up.depth < $2
)
-- Both columns are non-null by construction: the walk only takes rows whose
-- `parent` is set, and the depth counts them.
SELECT canvas AS "canvas!: CanvasId", depth AS "depth!" FROM up ORDER BY depth
