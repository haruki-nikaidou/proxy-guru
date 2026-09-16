//! The graph service: what a batch may change, what it is refused for, and what
//! the service fills in (ports) or cleans up (group members) on the way.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use kanau::processor::Processor;
use orchestration::entities::db::canvas::FindCanvasById;
use orchestration::entities::db::edge::EdgeTarget;
use orchestration::entities::db::graph::LoadCanvasGraph;
use orchestration::entities::db::group::{GroupEntity, GroupId, GroupMember};
use orchestration::entities::db::pod::{PodEntity, PodIngress};
use orchestration::entities::db::server::FindServerById;
use orchestration::services::OrchestrationError;
use orchestration::services::canvas::DeleteCanvas;
use orchestration::services::graph::{
    ApplyGraph, DEFAULT_POD_PORTS, GetGraph, GraphChange, GraphSubject, MoveItems,
};
use orchestration::services::server::DeleteServer;

fn problems(outcome: &orchestration::services::graph::ApplyOutcome) -> Vec<String> {
    outcome
        .diagnostics
        .iter()
        .filter(|d| d.error)
        .map(|d| d.problem.clone())
        .collect()
}

/// A pod put with port 0 gets a free port of its server: not one another pod
/// of the server listens on, and not one a config of the server still serves.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_pod_put_with_port_zero_gets_a_free_port(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let c = canvas(&w.db, "prod").await?;
    let s = server(&w.db, &c, "tokyo").await?;
    let mut change = GraphChange::default();
    for i in 0..20 {
        change
            .put_pods
            .push(pod(&c, &s, &format!("relay{i}"), 0, PodIngress::RelayTcp));
    }
    let outcome = w.apply(&c, change).await?;
    let ports: std::collections::HashSet<u16> = outcome.pods.iter().map(|p| p.port).collect();
    assert_eq!(ports.len(), 20, "no two pods share a port: {ports:?}");
    assert!(ports.iter().all(|p| DEFAULT_POD_PORTS.contains(p)));
    let graph = w
        .db
        .process(LoadCanvasGraph {
            canvas: c.id.clone(),
        })
        .await?;
    let stored: std::collections::HashSet<u16> = graph.pods.iter().map(|p| p.port).collect();
    assert_eq!(stored, ports, "the ports written are the ports answered");
    Ok(())
}

/// Ids are the caller's, but only in the shape every id has, only once per
/// batch, and never one another tree already uses.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn ids_must_be_well_formed_unique_and_this_trees(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let prod = canvas(&w.db, "prod").await?;
    let staging = canvas(&w.db, "staging").await?;
    let tokyo = server(&w.db, &prod, "tokyo").await?;
    let osaka = server(&w.db, &staging, "osaka").await?;
    w.apply(
        &staging,
        GraphChange {
            put_pods: vec![client(&staging, &osaka, "taken", 443, None)],
            ..GraphChange::default()
        },
    )
    .await?;

    let mut malformed = client(&prod, &tokyo, "x", 443, None);
    malformed.id = orchestration::utils::ids::pod_id("Not-A-Key");
    let refused = w
        .try_apply(
            &prod,
            GraphChange {
                put_pods: vec![
                    malformed,
                    client(&prod, &tokyo, "twice", 444, None),
                    client(&prod, &tokyo, "twice", 445, None),
                    client(&prod, &tokyo, "taken", 446, None),
                    // A server of another tree.
                    client(&prod, &osaka, "elsewhere", 447, None),
                ],
                delete_exits: vec![exit_id("ghost")],
                ..GraphChange::default()
            },
        )
        .await?;
    assert!(!refused.applied);
    let mut got = problems(&refused);
    got.sort();
    assert_eq!(
        got,
        [
            "duplicate_change",
            "id_in_use",
            "invalid_id",
            "unknown_exit",
            "unknown_server"
        ]
    );
    let taken = refused
        .diagnostics
        .iter()
        .find(|d| d.problem == "id_in_use")
        .unwrap();
    assert_eq!(taken.subjects, vec![GraphSubject::Pod(pod_id("taken"))]);
    let graph = w
        .db
        .process(LoadCanvasGraph {
            canvas: prod.id.clone(),
        })
        .await?;
    assert!(graph.pods.is_empty(), "nothing of a refused batch is written");
    Ok(())
}

/// An edge's ends are its identity: what it dials may change, where it runs
/// may not.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_edge_keeps_its_ends(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let c = canvas(&w.db, "prod").await?;
    let tokyo = server(&w.db, &c, "tokyo").await?;
    let osaka = server_at(&w.db, &c, "osaka", "198.51.100.10").await?;
    let entry = client(&c, &tokyo, "entry", 443, None);
    let hop = pod(&c, &osaka, "hop", 9443, PodIngress::RelayTcp);
    let origin = exit(&c, "origin", "10.0.0.5:8080");
    let dial = edge_to_pod("dial", &entry, &hop);
    let out = edge_to_exit("out", &hop, &origin);
    w.apply(
        &c,
        GraphChange {
            put_pods: vec![routed(entry.clone(), via(&dial)), routed(hop, via(&out))],
            put_exits: vec![origin.clone()],
            put_edges: vec![dial.clone(), out],
            ..GraphChange::default()
        },
    )
    .await?;

    let mut redialed = dial.clone();
    redialed.override_port = Some(19443);
    w.apply(
        &c,
        GraphChange {
            put_edges: vec![redialed],
            ..GraphChange::default()
        },
    )
    .await?;

    let mut moved = dial.clone();
    moved.target = EdgeTarget::Exit(origin.id.clone());
    let refused = w
        .try_apply(
            &c,
            GraphChange {
                put_edges: vec![moved],
                ..GraphChange::default()
            },
        )
        .await?;
    assert_eq!(problems(&refused), ["edge_ends_changed"]);
    let graph = w
        .db
        .process(LoadCanvasGraph {
            canvas: c.id.clone(),
        })
        .await?;
    let stored = graph.edges.iter().find(|e| e.id == dial.id).unwrap();
    assert_eq!(stored.override_port, Some(19443));
    assert_eq!(stored.target, dial.target);
    Ok(())
}

/// A pod deleted in a batch leaves the groups that held it; a batch of groups
/// alone does not bump the generation, and its members must exist.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn groups_follow_their_members(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let c = canvas(&w.db, "prod").await?;
    let s = server(&w.db, &c, "tokyo").await?;
    let a = client(&c, &s, "a", 443, None);
    let b = client(&c, &s, "b", 444, None);
    w.apply(
        &c,
        GraphChange {
            put_pods: vec![a.clone(), b.clone()],
            ..GraphChange::default()
        },
    )
    .await?;
    let generation = |w: &World| {
        let db = w.db.clone();
        let id = c.id.clone();
        async move {
            db.process(FindCanvasById { id })
                .await
                .unwrap()
                .unwrap()
                .generation
        }
    };
    let before = generation(&w).await;
    let group = GroupEntity {
        id: GroupId::from_key(key("rule")),
        canvas: c.id.clone(),
        kind: "rule".to_string(),
        name: "both".to_string(),
        props: serde_json::json!({"color": "#f80"}),
        members: vec![GroupMember::Pod(a.id.clone()), GroupMember::Pod(b.id.clone())],
    };
    w.apply(
        &c,
        GraphChange {
            put_groups: vec![group.clone()],
            ..GraphChange::default()
        },
    )
    .await?;
    assert_eq!(generation(&w).await, before, "drawing is not topology");

    let refused = w
        .try_apply(
            &c,
            GraphChange {
                put_groups: vec![GroupEntity {
                    members: vec![GroupMember::Pod(pod_id("ghost"))],
                    ..group.clone()
                }],
                ..GraphChange::default()
            },
        )
        .await?;
    assert_eq!(problems(&refused), ["unknown_group_member"]);

    w.apply(
        &c,
        GraphChange {
            delete_pods: vec![b.id.clone()],
            ..GraphChange::default()
        },
    )
    .await?;
    let graph = w
        .db
        .process(LoadCanvasGraph {
            canvas: c.id.clone(),
        })
        .await?;
    assert_eq!(graph.groups[0].members, vec![GroupMember::Pod(a.id)]);
    Ok(())
}

/// A TLS client pod names a DNS provider that exists, and pods sharing an SNI
/// agree on how its certificate is issued.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_tls_pod_must_name_a_provider_and_agree_on_its_certificate(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let c = canvas(&w.db, "prod").await?;
    let s = server(&w.db, &c, "tokyo").await?;
    let ghost = orchestration::utils::ids::dns_provider_id("ghost");
    let refused = w
        .try_apply(
            &c,
            GraphChange {
                put_pods: vec![tls_client(&c, &s, "edge", 443, &ghost, "a.example.com", "")],
                ..GraphChange::default()
            },
        )
        .await?;
    assert_eq!(problems(&refused), ["unknown_dns_provider"]);

    let provider = w
        .dns
        .process(orchestration::services::dns::CreateDnsProvider {
            actor: operator(),
            name: "cf".to_string(),
            provider: orchestration::entities::db::dns::DnsProvider::Cloudflare,
            account_id: String::new(),
            api_secret: "token".to_string(),
        })
        .await?;
    let other = w
        .dns
        .process(orchestration::services::dns::CreateDnsProvider {
            actor: operator(),
            name: "cf2".to_string(),
            provider: orchestration::entities::db::dns::DnsProvider::Cloudflare,
            account_id: String::new(),
            api_secret: "token".to_string(),
        })
        .await?;
    // The SNI is stored in the canonical form the certificate row is keyed by.
    let outcome = w
        .apply(
            &c,
            GraphChange {
                put_pods: vec![tls_client(&c, &s, "edge", 443, &provider.id, "A.Example.com", "")],
                ..GraphChange::default()
            },
        )
        .await?;
    assert!(matches!(
        &outcome.pods[0].ingress,
        PodIngress::ClientTls { tls, .. } if tls.sni == "a.example.com"
    ));
    w.db.process(orchestration::entities::db::certificate::EnsureCertificate {
        sni: "a.example.com".to_string(),
        dns_provider: provider.id.clone(),
        domain_id: "zone".to_string(),
        acme_directory: w.config.default_acme_directory.clone(),
        now: chrono::Utc::now(),
    })
    .await?;
    let refused = w
        .try_apply(
            &c,
            GraphChange {
                put_pods: vec![tls_client(&c, &s, "second", 8443, &other.id, "a.example.com", "")],
                ..GraphChange::default()
            },
        )
        .await?;
    assert_eq!(problems(&refused), ["certificate_issued_elsewhere"]);
    Ok(())
}

/// A canvas is not deleted while something outside it still leads into it, and
/// a server is not deleted while pods run on it.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn deletes_refuse_to_strand_what_depends_on_them(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let root = canvas(&w.db, "root").await?;
    let sub = subcanvas(&w.db, &root, "sub").await?;
    let tokyo = server(&w.db, &root, "tokyo").await?;
    let osaka = server_at(&w.db, &sub, "osaka", "198.51.100.10").await?;
    let entry = client(&root, &tokyo, "entry", 443, None);
    let hop = pod(&sub, &osaka, "hop", 9443, PodIngress::RelayTcp);
    let origin = exit(&sub, "origin", "10.0.0.5:8080");
    let dial = edge_to_pod("dial", &entry, &hop);
    let out = edge_to_exit("out", &hop, &origin);
    w.apply(
        &root,
        GraphChange {
            put_pods: vec![routed(entry.clone(), via(&dial)), routed(hop.clone(), via(&out))],
            put_exits: vec![origin],
            put_edges: vec![dial.clone(), out],
            ..GraphChange::default()
        },
    )
    .await?;

    let err = w
        .canvases
        .process(DeleteCanvas {
            actor: operator(),
            canvas: sub.id.clone(),
        })
        .await
        .expect_err("entry still dials into the subcanvas");
    assert!(
        matches!(&err, OrchestrationError::Conflict(m) if m.contains("entry")),
        "{err:?}"
    );
    let err = w
        .servers
        .process(DeleteServer {
            actor: operator(),
            server: tokyo.id.clone(),
        })
        .await
        .expect_err("tokyo still runs entry");
    assert!(matches!(err, OrchestrationError::Conflict(_)), "{err:?}");

    // Once the entry stops dialing it, the subcanvas goes with its server, pod,
    // exit and the edge leaving its pod.
    w.apply(
        &root,
        GraphChange {
            put_pods: vec![PodEntity {
                route: None,
                ..entry
            }],
            delete_edges: vec![dial.id],
            ..GraphChange::default()
        },
    )
    .await?;
    w.canvases
        .process(DeleteCanvas {
            actor: operator(),
            canvas: sub.id.clone(),
        })
        .await?;
    let view = w
        .graph
        .process(GetGraph {
            actor: operator(),
            canvas: root.id.clone(),
        })
        .await?;
    assert_eq!(view.rows.canvases.len(), 1);
    assert_eq!(view.rows.pods.len(), 1);
    assert!(view.rows.exits.is_empty() && view.rows.edges.is_empty());
    assert!(
        w.db.process(FindServerById { id: osaka.id })
            .await?
            .is_none()
    );
    Ok(())
}

/// Positions move without a fence or a generation bump, and only for items of
/// the tree the call names.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn items_move_without_touching_the_topology(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let root = canvas(&w.db, "root").await?;
    let sub = subcanvas(&w.db, &root, "sub").await?;
    let elsewhere = canvas(&w.db, "elsewhere").await?;
    let tokyo = server(&w.db, &root, "tokyo").await?;
    let foreign = server(&w.db, &elsewhere, "foreign").await?;
    let origin = exit(&sub, "origin", "10.0.0.5:8080");
    let entry = client(&root, &tokyo, "entry", 443, None);
    let out = edge_to_exit("out", &entry, &origin);
    w.apply(
        &root,
        GraphChange {
            put_pods: vec![routed(entry, via(&out))],
            put_exits: vec![origin.clone()],
            put_edges: vec![out],
            ..GraphChange::default()
        },
    )
    .await?;
    let before = w
        .db
        .process(LoadCanvasGraph {
            canvas: root.id.clone(),
        })
        .await?;
    w.graph
        .process(MoveItems {
            actor: operator(),
            canvas: sub.id.clone(),
            servers: vec![(tokyo.id.clone(), pos(1, 2)), (foreign.id.clone(), pos(9, 9))],
            exits: vec![(origin.id.clone(), pos(3, 4))],
            canvases: vec![(sub.id.clone(), pos(5, 6))],
        })
        .await?;
    let after = w
        .db
        .process(LoadCanvasGraph {
            canvas: root.id.clone(),
        })
        .await?;
    assert_eq!(after.generation(), before.generation());
    assert_eq!(after.servers[0].position, pos(1, 2));
    assert_eq!(after.exits[0].position, pos(3, 4));
    assert_eq!(
        after.canvases.iter().find(|c| c.id == sub.id).unwrap().position,
        pos(5, 6)
    );
    assert_eq!(
        w.db.process(FindServerById { id: foreign.id })
            .await?
            .unwrap()
            .position,
        pos(0, 0),
        "a server of another tree is not this call's to move"
    );
    Ok(())
}

/// A change computed against a tree that has moved on is refused before it is
/// checked, so it cannot land on top of an edit its author never saw.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_change_against_a_stale_generation_is_refused(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let c = canvas(&w.db, "prod").await?;
    let s = server(&w.db, &c, "tokyo").await?;
    let read = w
        .graph
        .process(GetGraph {
            actor: operator(),
            canvas: c.id.clone(),
        })
        .await?;
    w.apply(
        &c,
        GraphChange {
            put_pods: vec![client(&c, &s, "first", 443, None)],
            ..GraphChange::default()
        },
    )
    .await?;
    let err = w
        .graph
        .process(ApplyGraph {
            actor: operator(),
            canvas: c.id.clone(),
            change: GraphChange {
                put_pods: vec![client(&c, &s, "second", 444, None)],
                ..GraphChange::default()
            },
            dry_run: false,
            expected_generation: Some(read.rows.generation()),
        })
        .await
        .expect_err("the tree moved on");
    assert!(matches!(err, OrchestrationError::Conflict(_)), "{err:?}");
    Ok(())
}

/// A maintainer edits the graph; a machine credential may not.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn only_a_session_edits_the_graph(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let c = canvas(&w.db, "prod").await?;
    let s = server(&w.db, &c, "tokyo").await?;
    let change = GraphChange {
        put_pods: vec![client(&c, &s, "edge", 443, None)],
        ..GraphChange::default()
    };
    let err = w
        .graph
        .process(ApplyGraph {
            actor: machine(),
            canvas: c.id.clone(),
            change: change.clone(),
            dry_run: false,
            expected_generation: None,
        })
        .await
        .expect_err("api keys may not edit the workspace");
    assert!(matches!(err, OrchestrationError::Core(_)), "{err:?}");
    let outcome = w
        .graph
        .process(ApplyGraph {
            actor: maintainer(),
            canvas: c.id.clone(),
            change,
            dry_run: false,
            expected_generation: None,
        })
        .await?;
    assert!(outcome.applied);
    Ok(())
}
