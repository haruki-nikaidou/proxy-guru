-- The node model is gone: the master reads only the pod graph.
--
-- A database that still holds nodes but was never converted refuses this
-- migration, so no deploy can drop a canvas nobody converted. Convert with the
-- release that carries migration 0006 (`manage-tool orchestration
-- convert-graph`) first.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM orchestration_node)
       AND NOT EXISTS (SELECT 1 FROM orchestration_graph_state) THEN
        RAISE EXCEPTION 'the node model was never converted: run `manage-tool orchestration convert-graph` from the release with migration 0006 first';
    END IF;
END
$$;

DROP TABLE node_health_record;
DROP TABLE orchestration_edge_connection;
DROP TABLE orchestration_port;
DROP TABLE orchestration_node;

-- A relay leaf belongs to a pod again.
DELETE FROM relay_certificate WHERE pod NOT IN (SELECT id FROM orchestration_pod);
ALTER TABLE relay_certificate
    ADD CONSTRAINT relay_certificate_pod_fkey
    FOREIGN KEY (pod) REFERENCES orchestration_pod (id) ON DELETE CASCADE;
