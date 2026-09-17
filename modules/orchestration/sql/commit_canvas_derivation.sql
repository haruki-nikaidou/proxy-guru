UPDATE orchestration_canvas
SET derived_generation = $2, derived_view_seq = $3
WHERE id = $1 AND generation = $2
  AND (derived_generation < $2 OR derived_view_seq < $3)
RETURNING id AS "id: CanvasId"
