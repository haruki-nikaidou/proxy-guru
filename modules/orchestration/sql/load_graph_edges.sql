-- Every edge leaving a pod of the given canvases.
SELECT e.id AS "id: EdgeId",
       e.source_pod AS "source_pod: PodId",
       e.target_pod AS "target_pod: PodId",
       e.target_exit AS "target_exit: ExitId",
       e.override_ip,
       e.override_port,
       e.ip_family AS "ip_family: IpFamily"
FROM orchestration_edge e
JOIN orchestration_pod p ON p.id = e.source_pod
WHERE p.canvas = ANY($1)
ORDER BY e.id
