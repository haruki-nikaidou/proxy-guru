-- Which of its target pod's addresses an edge dials. `auto` is the server's
-- effective address, IPv4 when it has one, as every edge dialed before; `v4`
-- and `v6` insist on that family: the pod's advertised address when it is of
-- it, else the server's address of it. An override address wins over all
-- three, and an edge into an exit dials the exit's destination as written.
ALTER TABLE orchestration_edge
    ADD COLUMN ip_family text NOT NULL DEFAULT 'auto'
        CHECK (ip_family IN ('auto', 'v4', 'v6'));
