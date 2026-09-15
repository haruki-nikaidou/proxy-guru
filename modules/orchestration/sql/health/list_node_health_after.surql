SELECT * FROM node_health_record
WHERE node = $node
  AND (report_time > $after
       OR (report_time = $after
           AND ($after_id = NONE OR id > $after_id)))
ORDER BY report_time ASC, id ASC LIMIT $limit;
