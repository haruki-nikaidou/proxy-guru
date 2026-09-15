-- Canvases whose derivation is behind: an operator edit bumped `generation`, or
-- a worker wrote one of the tree's view rows (their `seq` sum moved past what
-- the last committed pass stamped). Subcanvas rows are levelled to the tree's
-- sum on every commit and their own subtree sums to no more, so only a root
-- reads as stale on its own.
SELECT VALUE id FROM orchestration_canvas
    WHERE generation > derived_generation
        OR derived_view_seq < math::sum(
            SELECT VALUE seq FROM orchestration_server_config_view
                WHERE server.canvas IN fn::orchestration_tree($parent.id, 0));
