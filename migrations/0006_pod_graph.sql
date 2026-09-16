-- The forwarding topology as a graph of pods.
--
-- A pod is one listener on one server and a vertex of a directed acyclic graph;
-- an edge sends a pod's traffic on to another pod (spoken in the protocol that
-- pod listens with) or to an exit; each pod's `route` is a tree over exactly its
-- own out-edges. Universal pods, bundles, splitters and aggregators are how the
-- dashboard draws this graph: the groups below are its drawing metadata and
-- nothing derives from them.
--
-- The node model (`orchestration_node`, `orchestration_port`,
-- `orchestration_edge_connection`, `node_health_record`) stays until
-- `manage-tool orchestration convert-graph` has filled these tables and the
-- master reads only them; migration 0007 drops it, and refuses to while nodes
-- exist that `orchestration_graph_state` does not record as converted.

-- Subcanvases nest by `parent`; a canvas sits at `position` on its parent.
ALTER TABLE orchestration_canvas
    ADD COLUMN parent     text REFERENCES orchestration_canvas (id) ON DELETE CASCADE,
    ADD COLUMN position_x bigint NOT NULL DEFAULT 0,
    ADD COLUMN position_y bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT orchestration_canvas_parent_not_self CHECK (parent IS NULL OR parent <> id);
CREATE INDEX orchestration_canvas_parent ON orchestration_canvas (parent);

-- What the server's worker reads beyond the config every worker reads, as it
-- reported on its last registration (`route_table`, `relay_confirm`).
ALTER TABLE orchestration_server
    ADD COLUMN capabilities text[] NOT NULL DEFAULT '{}';

CREATE TABLE orchestration_pod (
    id                      text PRIMARY KEY,
    canvas                  text NOT NULL REFERENCES orchestration_canvas (id) ON DELETE CASCADE,
    -- A server that still hosts pods cannot be deleted.
    server                  text NOT NULL REFERENCES orchestration_server (id),
    name                    text NOT NULL,
    comment                 text NOT NULL DEFAULT '',
    port                    integer NOT NULL CHECK (port BETWEEN 1 AND 65535),
    -- An IP literal; NULL listens on every address.
    bind_ip                 text,
    -- An IP literal dialers use instead of the server's address.
    advertise_ip            text,
    -- How traffic arrives: clients directly (raw or TLS-terminated), or relayed
    -- by other pods in the protocol named.
    ingress                 text NOT NULL CHECK (ingress IN
                                ('client_raw', 'client_tls', 'relay_tcp', 'relay_tls', 'relay_quic')),
    receive_proxy_protocol  text CHECK (receive_proxy_protocol IN ('v1', 'v2')),
    -- The ACME certificate a TLS client listener terminates with; an empty
    -- directory means the configured default.
    tls_sni                 text,
    tls_dns_provider        text REFERENCES dns_provider (id),
    tls_domain_id           text,
    tls_acme_directory      text,
    -- The route tree over the pod's out-edges (`guru_topology::Route`); NULL
    -- when it has none.
    route                   jsonb,
    CONSTRAINT orchestration_pod_proxy_on_clients CHECK (
        ingress IN ('client_raw', 'client_tls') OR receive_proxy_protocol IS NULL
    ),
    CONSTRAINT orchestration_pod_tls_shape CHECK (
        (ingress = 'client_tls') = (tls_sni IS NOT NULL)
        AND (tls_sni IS NULL) = (tls_dns_provider IS NULL)
        AND (tls_sni IS NULL) = (tls_domain_id IS NULL)
        AND (tls_sni IS NULL) = (tls_acme_directory IS NULL)
    )
);
CREATE INDEX orchestration_pod_canvas       ON orchestration_pod (canvas);
CREATE INDEX orchestration_pod_server       ON orchestration_pod (server);
CREATE INDEX orchestration_pod_dns_provider ON orchestration_pod (tls_dns_provider);

CREATE TABLE orchestration_exit (
    id                   text PRIMARY KEY,
    canvas               text NOT NULL REFERENCES orchestration_canvas (id) ON DELETE CASCADE,
    name                 text NOT NULL,
    comment              text NOT NULL DEFAULT '',
    -- `host:port`.
    destination          text NOT NULL,
    send_proxy_protocol  text CHECK (send_proxy_protocol IN ('v1', 'v2')),
    position_x           bigint NOT NULL DEFAULT 0,
    position_y           bigint NOT NULL DEFAULT 0
);
CREATE INDEX orchestration_exit_canvas ON orchestration_exit (canvas);

-- No uniqueness: two pods may be joined by several edges (dialing different
-- addresses of the same pod, say). NO ACTION everywhere: an edge is only
-- removed together with the route that names it, in the same write.
CREATE TABLE orchestration_edge (
    id             text PRIMARY KEY,
    source_pod     text NOT NULL REFERENCES orchestration_pod (id),
    target_pod     text REFERENCES orchestration_pod (id),
    target_exit    text REFERENCES orchestration_exit (id),
    -- Dial this host (an IP literal or a name) or port instead of the target
    -- pod's own.
    override_ip    text,
    override_port  integer CHECK (override_port BETWEEN 1 AND 65535),
    CONSTRAINT orchestration_edge_one_target CHECK (num_nonnulls(target_pod, target_exit) = 1)
);
CREATE INDEX orchestration_edge_source_pod  ON orchestration_edge (source_pod);
CREATE INDEX orchestration_edge_target_pod  ON orchestration_edge (target_pod);
CREATE INDEX orchestration_edge_target_exit ON orchestration_edge (target_exit);

-- How the dashboard draws the graph: a group of pods, edges, exits and servers
-- of some kind the dashboard defines, with whatever it needs to remember about
-- it (position, colour, the parameters of a batch edit). Nothing derives from it.
CREATE TABLE orchestration_group (
    id      text PRIMARY KEY,
    canvas  text NOT NULL REFERENCES orchestration_canvas (id) ON DELETE CASCADE,
    kind    text NOT NULL,
    name    text NOT NULL DEFAULT '',
    props   jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX orchestration_group_canvas ON orchestration_group (canvas);

CREATE TABLE orchestration_group_member (
    group_id  text NOT NULL REFERENCES orchestration_group (id) ON DELETE CASCADE,
    position  integer NOT NULL DEFAULT 0,
    pod       text REFERENCES orchestration_pod (id) ON DELETE CASCADE,
    edge      text REFERENCES orchestration_edge (id) ON DELETE CASCADE,
    exit      text REFERENCES orchestration_exit (id) ON DELETE CASCADE,
    server    text REFERENCES orchestration_server (id) ON DELETE CASCADE,
    CONSTRAINT orchestration_group_member_one CHECK (num_nonnulls(pod, edge, exit, server) = 1)
);
CREATE UNIQUE INDEX orchestration_group_member_pod    ON orchestration_group_member (group_id, pod)    WHERE pod IS NOT NULL;
CREATE UNIQUE INDEX orchestration_group_member_edge   ON orchestration_group_member (group_id, edge)   WHERE edge IS NOT NULL;
CREATE UNIQUE INDEX orchestration_group_member_exit   ON orchestration_group_member (group_id, exit)   WHERE exit IS NOT NULL;
CREATE UNIQUE INDEX orchestration_group_member_server ON orchestration_group_member (group_id, server) WHERE server IS NOT NULL;
CREATE INDEX orchestration_group_member_of_pod  ON orchestration_group_member (pod);
CREATE INDEX orchestration_group_member_of_edge ON orchestration_group_member (edge);

-- A pod's health history, as `node_health_record` was a node's.
CREATE TABLE pod_health_record (
    id           text PRIMARY KEY,
    pod          text NOT NULL REFERENCES orchestration_pod (id) ON DELETE CASCADE,
    status       text NOT NULL CHECK (status IN ('ready', 'deploying', 'failed')),
    message      text NOT NULL,
    report_time  timestamptz NOT NULL
);
CREATE INDEX pod_health_record_pod_time ON pod_health_record (pod, report_time);
CREATE INDEX pod_health_record_time     ON pod_health_record (report_time);

-- Exactly one row, keyed `current`, once the node model was converted: what the
-- conversion found, for the record.
CREATE TABLE orchestration_graph_state (
    id            text PRIMARY KEY CHECK (id = 'current'),
    converted_at  timestamptz NOT NULL,
    report        jsonb NOT NULL DEFAULT '{}'
);

-- A relay leaf belongs to a pod; pods keep the ids their nodes had, and the
-- foreign key moves to `orchestration_pod` once the node model is gone.
ALTER TABLE relay_certificate DROP CONSTRAINT relay_certificate_pod_fkey;
