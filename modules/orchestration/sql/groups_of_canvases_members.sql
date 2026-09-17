SELECT m.group_id AS "group_id: GroupId", m.pod AS "pod: PodId",
       m.edge AS "edge: EdgeId", m.exit AS "exit: ExitId",
       m.server AS "server: ServerId"
FROM orchestration_group_member m
JOIN orchestration_group g ON g.id = m.group_id
WHERE g.canvas = ANY($1)
ORDER BY m.group_id, m.position
