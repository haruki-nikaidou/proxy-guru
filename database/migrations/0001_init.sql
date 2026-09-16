-- The whole schema, as one migration: the database this replaces was exported
-- and re-imported, so there is no earlier PostgreSQL state to migrate from.
--
-- Conventions
--   * Ids are 20-character [a-z0-9] keys minted by the application
--     (`db_types::random_key`), stored as text. That is the shape the previous
--     store minted, so imported rows and new rows are indistinguishable on the
--     wire, and the gRPC API keeps passing bare keys.
--   * Every enum is text with a CHECK; the Rust side is a `text_enum!`.
--   * Documents whose shape varies by row (`spec`, config snapshots) are jsonb.
--   * Foreign keys carry the delete behaviour the old delete transactions
--     implemented by hand: CASCADE where the transaction unconditionally deleted
--     the child rows, NO ACTION where it refused.

CREATE TABLE app_config (
    -- One row per config key, holding the whole serialised config struct; the
    -- key is the primary key, so a lookup is a point read.
    key      text  PRIMARY KEY,
    content  jsonb NOT NULL
);

CREATE TABLE auth_account (
    id             text PRIMARY KEY,
    email          text NOT NULL,
    password_hash  text NOT NULL,
    role           text NOT NULL CHECK (role IN ('admin', 'maintainer', 'observer')),
    CONSTRAINT auth_account_email_key UNIQUE (email)
);

CREATE TABLE auth_session (
    -- The session token itself is the key.
    id              text PRIMARY KEY,
    account_id      text NOT NULL REFERENCES auth_account (id) ON DELETE CASCADE,
    user_agent      text NOT NULL,
    created_at      timestamptz NOT NULL,
    last_active_at  timestamptz NOT NULL
);
CREATE INDEX auth_session_account ON auth_session (account_id);

CREATE TABLE api_key (
    id             text PRIMARY KEY,
    name           text NOT NULL,
    owner          text NOT NULL REFERENCES auth_account (id) ON DELETE CASCADE,
    secret_sha256  text NOT NULL,
    created_at     timestamptz NOT NULL,
    CONSTRAINT api_key_secret_sha256_key UNIQUE (secret_sha256)
);
CREATE INDEX api_key_owner ON api_key (owner);

-- Canvases nest: a `canvas_import` node embeds another canvas as one node, and
-- the unit of derivation is the whole tree. Only the root canvas of a tree
-- carries a meaningful `generation`: every mutating transaction bumps the root
-- (`entities::db::fence`), and a subcanvas row keeps
-- `generation == derived_generation`. Derivation is fenced by two counters: an
-- operator edit bumps `generation`; a worker never writes the canvas row at all,
-- its ack, registration and address report bump `seq` on its own view row, and
-- a derivation stamps the tree's `seq` sum as `derived_view_seq`.
CREATE TABLE orchestration_canvas (
    id                  text PRIMARY KEY,
    name                text NOT NULL,
    description         text NOT NULL,
    generation          bigint NOT NULL DEFAULT 0,
    derived_generation  bigint NOT NULL DEFAULT 0,
    derived_view_seq    bigint NOT NULL DEFAULT 0
);

CREATE TABLE orchestration_server (
    id                           text PRIMARY KEY,
    -- Deleting a canvas deletes the servers on it.
    canvas                       text NOT NULL REFERENCES orchestration_canvas (id) ON DELETE CASCADE,
    name                         text NOT NULL,
    icon                         text NOT NULL,
    comment                      text NOT NULL,
    position_x                   bigint NOT NULL,
    position_y                   bigint NOT NULL,
    ipv6_resolve                 text NOT NULL CHECK (ipv6_resolve IN ('required', 'preferred', 'tolerated', 'forbidden')),
    log_level                    text NOT NULL,
    -- The worker's session: rotated on registration, fenced by `refresh_key_generation`
    -- and `watch_epoch`; `session_lease_until` is the lease the config stream renews.
    current_dynamic_refresh_key  text,
    refresh_key_generation       bigint NOT NULL DEFAULT 0,
    watch_epoch                  bigint NOT NULL DEFAULT 0,
    session_lease_until          timestamptz,
    last_seen_at                 timestamptz,
    -- Health liveness is separate from the watch lease: only an accepted health
    -- report advances `last_health_report_at`. `health_status` is the denormalised
    -- current status so listings never scan history.
    last_health_report_at        timestamptz,
    health_status                text NOT NULL DEFAULT 'offline' CHECK (health_status IN ('online', 'degraded', 'offline')),
    -- Addresses: what the worker reported, what the master observed, and what the
    -- operator pinned. The effective address is computed, never stored.
    override_v4                  text,
    override_v6                  text,
    extra_addresses              text[] NOT NULL DEFAULT '{}',
    reported_addresses           jsonb,
    observed_address             text,
    observed_at                  timestamptz,
    -- The agent the worker last registered as, and the self-update bookkeeping.
    agent_version                text,
    agent_arch                   text,
    agent_unit                   text,
    agent_update_requested       text,
    agent_update_error           text,
    agent_key_digest             text,
    agent_key_issued_at          timestamptz
);
CREATE INDEX orchestration_server_canvas      ON orchestration_server (canvas);
CREATE INDEX orchestration_server_refresh_key ON orchestration_server (current_dynamic_refresh_key);
CREATE INDEX orchestration_server_agent_key   ON orchestration_server (agent_key_digest);

-- DNS providers answer ACME DNS-01 challenges. `api_secret` is encrypted with the
-- master key before it is stored and is never returned by the API.
CREATE TABLE dns_provider (
    id          text PRIMARY KEY,
    name        text NOT NULL,
    provider    text NOT NULL CHECK (provider IN ('cloudflare', 'vercel')),
    account_id  text NOT NULL,
    api_secret  text NOT NULL,
    created_at  timestamptz NOT NULL
);

CREATE TABLE orchestration_node (
    id          text PRIMARY KEY,
    canvas      text NOT NULL REFERENCES orchestration_canvas (id) ON DELETE CASCADE,
    name        text NOT NULL,
    comment     text NOT NULL,
    -- `NodeSpec`, a tagged enum: `{ "type": "pod", "config": { ... } }`. The
    -- shape differs per variant, hence a document.
    spec        jsonb NOT NULL,
    position_x  bigint NOT NULL,
    position_y  bigint NOT NULL,
    -- Three links lifted out of `spec` by the node write path (`NodeSpec::links`)
    -- so they can be indexed and foreign-keyed; `spec` remains the source of
    -- truth for everything else.
    --
    -- A canvas is imported at most once. NULL (every non-import node) is not
    -- part of the UNIQUE constraint. NO ACTION rather than RESTRICT because
    -- deleting a canvas tree deletes the importing nodes and the imported
    -- canvases in one statement, and NO ACTION is checked at the end of it.
    import_canvas     text REFERENCES orchestration_canvas (id),
    -- A pod's server. Existence is the database's job; "inside the pod's canvas
    -- tree" is the topology checker's (`ProblemKind::PodServerForeign`). Deleting
    -- a server that still has pods is refused here as well as by the service.
    pod_server        text REFERENCES orchestration_server (id),
    -- An Entry's DNS provider, from `spec.config.tls.dns_provider`.
    tls_dns_provider  text REFERENCES dns_provider (id),
    -- A node generated by the expansion of a universal node carries its lane:
    -- the universal node it belongs to (`group`), the entry pod whose channel it
    -- serves (`channel`), its role and, for a landing pod or a distributor's
    -- relay, the universal node on the other end of the bundle (`source`).
    -- `lane_key` is the deterministic identity the reconciler diffs by. Not
    -- foreign keys: the reconciler owns these rows' lifecycle and deletes them in
    -- its own order.
    lane_key      text,
    lane_group    text,
    lane_channel  text,
    lane_role     text CHECK (lane_role IN ('distribute', 'relay', 'landing', 'aggregate')),
    lane_source   text,
    lane_via      text,
    CONSTRAINT orchestration_node_import_canvas_key UNIQUE (import_canvas),
    CONSTRAINT orchestration_node_import_not_self CHECK (import_canvas IS NULL OR import_canvas <> canvas),
    CONSTRAINT orchestration_node_lane_shape CHECK (
        (lane_key IS NULL AND lane_group IS NULL AND lane_channel IS NULL
            AND lane_role IS NULL AND lane_source IS NULL AND lane_via IS NULL)
        OR (lane_key IS NOT NULL AND lane_group IS NOT NULL AND lane_channel IS NOT NULL
            AND lane_role IS NOT NULL)
    )
);
CREATE INDEX orchestration_node_canvas     ON orchestration_node (canvas);
CREATE INDEX orchestration_node_server     ON orchestration_node (pod_server);
CREATE INDEX orchestration_node_lane_key   ON orchestration_node (lane_key);
CREATE INDEX orchestration_node_lane_group ON orchestration_node (lane_group);
CREATE INDEX orchestration_node_spec_type  ON orchestration_node ((spec ->> 'type'));

CREATE TABLE orchestration_port (
    id         text PRIMARY KEY,
    owner      text NOT NULL REFERENCES orchestration_node (id) ON DELETE CASCADE,
    kind       text NOT NULL CHECK (kind IN ('derive_listen', 'derive_destination', 'bundle')),
    direction  text NOT NULL CHECK (direction IN ('input', 'output')),
    key        text NOT NULL,
    position   bigint NOT NULL,
    -- A node has one port per key; reshaping a node's ports is an upsert on it.
    CONSTRAINT orchestration_port_owner_key_key UNIQUE (owner, key)
);

CREATE TABLE orchestration_edge_connection (
    id           text PRIMARY KEY,
    -- A port carries at most one edge; the database, not only the topology
    -- checker, enforces it, so two concurrent connects cannot both persist an
    -- edge on one port. Deleting a port deletes its edge.
    source_port  text NOT NULL REFERENCES orchestration_port (id) ON DELETE CASCADE,
    target_port  text NOT NULL REFERENCES orchestration_port (id) ON DELETE CASCADE,
    CONSTRAINT orchestration_edge_connection_source_port_key UNIQUE (source_port),
    CONSTRAINT orchestration_edge_connection_target_port_key UNIQUE (target_port)
);

-- What a worker runs: one row per server holding three immutable snapshots
-- (`desired`, `in_flight`, `applied`). `seq` is bumped by every worker-driven
-- write to this row, the worker's share of the derivation fence.
CREATE TABLE orchestration_server_config_view (
    id               text PRIMARY KEY,
    server           text NOT NULL REFERENCES orchestration_server (id) ON DELETE CASCADE,
    desired          jsonb,
    in_flight        jsonb,
    applied          jsonb,
    failed_revision  bigint,
    apply_error      text,
    derive_error     text,
    -- `derive_error` is a server-level failure: the server produced no config at
    -- all. `invalid_pods` is the per-pod counterpart, `{ node, pod, listen, error }`
    -- each; `failed_pods` the pods the worker could not apply, `{ pod, tag, error }`.
    invalid_pods     jsonb  NOT NULL DEFAULT '[]',
    failed_pods      jsonb  NOT NULL DEFAULT '[]',
    waiting_for      text[] NOT NULL DEFAULT '{}',
    seq              bigint NOT NULL DEFAULT 0,
    CONSTRAINT orchestration_server_config_view_server_key UNIQUE (server)
);

-- Health history: one `server_health_record` per worker report (counters are
-- deltas since the previous report) and one `node_health_record` per node per
-- report; raw, never downsampled, trimmed by the retention cron.
CREATE TABLE server_health_record (
    id                   text PRIMARY KEY,
    server               text NOT NULL REFERENCES orchestration_server (id) ON DELETE CASCADE,
    status               text NOT NULL CHECK (status IN ('online', 'degraded', 'offline')),
    report_time          timestamptz NOT NULL,
    upload_bytes         bigint NOT NULL,
    download_bytes       bigint NOT NULL,
    current_connections  bigint NOT NULL,
    max_connections      bigint NOT NULL
);
CREATE INDEX server_health_record_server_time ON server_health_record (server, report_time);
CREATE INDEX server_health_record_time        ON server_health_record (report_time);

CREATE TABLE node_health_record (
    id           text PRIMARY KEY,
    node         text NOT NULL REFERENCES orchestration_node (id) ON DELETE CASCADE,
    status       text NOT NULL CHECK (status IN ('ready', 'deploying', 'failed')),
    message      text NOT NULL,
    report_time  timestamptz NOT NULL
);
CREATE INDEX node_health_record_node_time ON node_health_record (node, report_time);
CREATE INDEX node_health_record_time      ON node_health_record (report_time);

-- One public certificate per (sni, acme_directory), shared by every Entry that
-- asks for it. The account key and private key are encrypted with the master
-- key. `version` increments on every issuance and renewal; a config snapshot
-- pins the version it was derived against.
CREATE TABLE certificate (
    id                text PRIMARY KEY,
    sni               text NOT NULL,
    -- A provider with certificates cannot be deleted.
    dns_provider      text NOT NULL REFERENCES dns_provider (id),
    domain_id         text NOT NULL,
    acme_directory    text NOT NULL,
    status            text NOT NULL CHECK (status IN ('pending', 'issued', 'failed')),
    acme_account_key  text,
    private_key_pem   text,
    full_chain_pem    text,
    not_before        timestamptz,
    not_after         timestamptz,
    last_error        text,
    last_attempt_at   timestamptz,
    version           bigint NOT NULL DEFAULT 0,
    created_at        timestamptz NOT NULL,
    CONSTRAINT certificate_sni_acme_directory_key UNIQUE (sni, acme_directory)
);
CREATE INDEX certificate_dns_provider ON certificate (dns_provider);

-- The internal CA signs the leaf certificates relay links present to each other.
-- Exactly one row, keyed `master`, created by `manage-tool orchestration init-ca`.
CREATE TABLE internal_ca (
    id               text PRIMARY KEY CHECK (id = 'master'),
    certificate_pem  text NOT NULL,
    private_key_pem  text NOT NULL,
    not_after        timestamptz NOT NULL,
    created_at       timestamptz NOT NULL
);

-- The published `guru-worker` release. Exactly one row, keyed `current`.
CREATE TABLE orchestration_agent_release (
    id            text PRIMARY KEY CHECK (id = 'current'),
    version       text NOT NULL,
    sha256        text NOT NULL,
    arch          text NOT NULL,
    published_at  timestamptz NOT NULL
);

-- One leaf per pod whose listener is a TLS or QUIC relay, rotated by the
-- certificate cron. `version` plays the same role as on `certificate`.
CREATE TABLE relay_certificate (
    id               text PRIMARY KEY,
    pod              text NOT NULL REFERENCES orchestration_node (id) ON DELETE CASCADE,
    sni              text NOT NULL,
    private_key_pem  text NOT NULL,
    certificate_pem  text NOT NULL,
    not_before       timestamptz NOT NULL,
    not_after        timestamptz NOT NULL,
    version          bigint NOT NULL DEFAULT 0,
    CONSTRAINT relay_certificate_pod_key UNIQUE (pod)
);

-- One row per periodic job, keyed by the job name. A consumer claims a run with
-- one compare-and-set over both columns: `last_signal_tick` makes a redelivered
-- or backlogged signal a no-op, `last_run_at` enforces the configured interval.
CREATE TABLE orchestration_job_run (
    id                text PRIMARY KEY,
    last_run_at       timestamptz NOT NULL,
    last_signal_tick  timestamptz NOT NULL
);
