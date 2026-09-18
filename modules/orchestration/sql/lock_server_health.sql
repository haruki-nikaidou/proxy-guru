-- The server row's health fields, locked for the transaction that reads them,
-- plus the two labels a notification needs — read here so nothing downstream
-- has to re-read a server that may since have moved.
SELECT canvas AS "canvas: CanvasId", name,
       (SELECT name FROM orchestration_canvas
        WHERE id = orchestration_server.canvas) AS "canvas_name!",
       health_status AS "health_status: ServerHealthStatus",
       refresh_key_generation
FROM orchestration_server
WHERE id = $1
FOR UPDATE
