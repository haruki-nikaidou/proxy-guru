//! Issue #6: mutating services validate the topology their edit *would* produce
//! and then write in a separate transaction. Two edits that each validate on
//! their own but jointly break an invariant must not both commit. The fenced
//! generation bump (`fn::orchestration_touch_checked`) is what stops them: only
//! the edit that still matches the generation it validated against lands.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use kanau::processor::Processor;
use orchestration::entities::db::connection::EdgeConnectionEntity;
use orchestration::entities::db::node::{NodeSpec, NodeWithPorts, RelayConfig, RelayProtocol};
use orchestration::entities::db::topology::LoadCanvasTopology;
use orchestration::services::edge::Connect;
use orchestration::services::node::CreateNode;
use orchestration::services::topology::{ProblemKind, ProblemSeverity, TopologyEdit, analyze};
use orchestration::utils::ids;

fn relay_spec() -> NodeSpec {
    NodeSpec::Relay(RelayConfig {
        protocol: RelayProtocol::TcpRaw,
        override_ip_address: None,
        override_port: None,
    })
}

async fn create(
    w: &World,
    canvas: &orchestration::entities::db::canvas::CanvasId,
    name: &str,
    spec: NodeSpec,
) -> NodeWithPorts {
    w.nodes
        .process(CreateNode {
            actor: operator(),
            canvas: canvas.clone(),
            name: name.to_string(),
            comment: String::new(),
            spec,
            position: pos0(),
            item_count: 0,
        })
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"))
}

fn add_edge(
    source: &orchestration::entities::db::port::PortId,
    target: &orchestration::entities::db::port::PortId,
) -> TopologyEdit {
    TopologyEdit::AddEdge {
        edge: EdgeConnectionEntity {
            id: ids::edge_id("pending"),
            source: source.clone(),
            target: target.clone(),
        },
    }
}

fn has_cycle(problems: &[orchestration::services::topology::TopologyProblem]) -> bool {
    problems
        .iter()
        .any(|p| p.severity == ProblemSeverity::Error && p.kind == ProblemKind::Cycle)
}

/// A relay that dials the pod feeding it closes a loop. Drawing either edge
/// alone is legal; drawing both is a cycle. Two operators who each validate
/// against the pre-state — one edge each — would both pass and, without the
/// fence, both commit, leaving a stored cycle no single request would accept.
/// The generation fence forces sequential consistency: exactly one lands, and
/// the stored canvas stays acyclic.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn concurrent_edits_that_jointly_form_a_cycle_do_not_both_commit(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let c = canvas(&w.db, "prod").await?;
    let s = server(&w.db, &c, "tokyo").await?;
    let pod = create(&w, &c.id, "pod", pod_spec(&s, 443)).await;
    let relay = create(&w, &c.id, "relay", relay_spec()).await;

    let pod_listen = port_of(&pod, "listen");
    let pod_dest = port_of(&pod, "destination");
    let relay_listen = port_of(&relay, "listen");
    let relay_dest = port_of(&relay, "destination");

    // The two edges an operator would draw: pod -> relay, and relay -> pod.
    let feed = (pod_listen.clone(), relay_listen.clone());
    let dial = (relay_dest.clone(), pod_dest.clone());

    // Each edge is legal on its own; together they are a cycle. This is the
    // invariant the pair jointly breaks, which no per-write DB guard catches.
    let topology =
        w.db.process(LoadCanvasTopology {
            canvas: c.id.clone(),
        })
        .await?;
    assert!(
        !has_cycle(&analyze(&topology.project(&[add_edge(&feed.0, &feed.1)]))),
        "pod -> relay alone is acyclic"
    );
    assert!(
        !has_cycle(&analyze(&topology.project(&[add_edge(&dial.0, &dial.1)]))),
        "relay -> pod alone is acyclic"
    );
    assert!(
        has_cycle(&analyze(&topology.project(&[
            add_edge(&feed.0, &feed.1),
            add_edge(&dial.0, &dial.1)
        ]))),
        "but both together form a cycle"
    );

    // Two concurrent requests, each validating against the same pre-state.
    let a = {
        let edges = w.edges.clone();
        tokio::spawn(async move {
            edges
                .process(Connect {
                    actor: operator(),
                    output_port: feed.0,
                    input_port: feed.1,
                })
                .await
        })
    };
    let b = {
        let edges = w.edges.clone();
        tokio::spawn(async move {
            edges
                .process(Connect {
                    actor: operator(),
                    output_port: dial.0,
                    input_port: dial.1,
                })
                .await
        })
    };
    let (a, b) = tokio::join!(a, b);
    let (a, b) = (a.unwrap(), b.unwrap());

    // Exactly one edit commits. The loser is rejected — either by the fence
    // (it validated against the pre-state and lost the race) or, if it read the
    // winner's write, by the cycle check itself. Both are correct outcomes.
    assert!(
        a.is_ok() ^ b.is_ok(),
        "exactly one of the jointly-invalid pair may commit: a={a:?} b={b:?}"
    );

    // The stored canvas is acyclic: the loser left nothing behind.
    let after =
        w.db.process(LoadCanvasTopology {
            canvas: c.id.clone(),
        })
        .await?;
    assert_eq!(after.edges.len(), 1, "only the winning edge persists");
    assert!(
        !has_cycle(&analyze(&after)),
        "the stored topology never became the cycle neither request would accept"
    );
    Ok(())
}
