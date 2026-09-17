UPDATE orchestration_canvas
SET name = $2, description = $3,
    position_x = COALESCE($4, position_x), position_y = COALESCE($5, position_y)
WHERE id = $1
RETURNING id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
          position_x, position_y, generation, derived_generation
