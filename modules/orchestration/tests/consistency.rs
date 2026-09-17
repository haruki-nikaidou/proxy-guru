//! A graph edit is checked against the tree it *would* produce and then written
//! in a separate transaction. Two edits that each check on their own but jointly
//! break an invariant must not both commit. The fenced generation bump is what
//! stops them: only the edit that still matches the generation it was checked
//! against lands.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use kanau::processor::Processor;
use orchestration::entities::db::graph::LoadCanvasGraph;
use orchestration::entities::db::pod::PodIngress;
use orchestration::services::graph::{ApplyGraph, GraphChange};

/// Two relay pods that each dial the other close a loop. Drawing either edge
/// alone is legal; drawing both is a cycle. Two operators who each check against
/// the pre-state — one edge each — would both pass and, without the fence, both
/// commit, leaving a stored cycle no single request would accept.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn concurrent_edits_that_jointly_form_a_cycle_do_not_both_commit(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let c = canvas(&w.db, "prod").await?;
    let tokyo = server(&w.db, &c, "tokyo").await?;
    let osaka = server_at(&w.db, &c, "osaka", "198.51.100.10").await?;
    let a = pod(&c, &tokyo, "a", 9443, PodIngress::RelayTcp);
    let b = pod(&c, &osaka, "b", 9443, PodIngress::RelayTcp);
    w.apply(
        &c,
        GraphChange {
            put_pods: vec![a.clone(), b.clone()],
            ..GraphChange::default()
        },
    )
    .await?;

    let a_to_b = edge_to_pod("a to b", &a, &b);
    let b_to_a = edge_to_pod("b to a", &b, &a);
    let forward = GraphChange {
        put_pods: vec![routed(a.clone(), via(&a_to_b))],
        put_edges: vec![a_to_b.clone()],
        ..GraphChange::default()
    };
    let backward = GraphChange {
        put_pods: vec![routed(b.clone(), via(&b_to_a))],
        put_edges: vec![b_to_a.clone()],
        ..GraphChange::default()
    };

    // Each edge checks on its own; together they are a cycle.
    let dry = |change: GraphChange| {
        let graph = w.graph.clone();
        let canvas = c.id.clone();
        async move {
            graph
                .process(ApplyGraph {
                    actor: operator(),
                    canvas,
                    change,
                    dry_run: true,
                    expected_generation: None,
                })
                .await
                .unwrap()
        }
    };
    assert!(
        !dry(forward.clone())
            .await
            .diagnostics
            .iter()
            .any(|d| d.error)
    );
    assert!(
        !dry(backward.clone())
            .await
            .diagnostics
            .iter()
            .any(|d| d.error)
    );
    let both = GraphChange {
        put_pods: vec![
            routed(a.clone(), via(&a_to_b)),
            routed(b.clone(), via(&b_to_a)),
        ],
        put_edges: vec![a_to_b, b_to_a],
        ..GraphChange::default()
    };
    assert!(
        dry(both)
            .await
            .diagnostics
            .iter()
            .any(|d| d.error && d.problem == "cycle"),
        "but both together form a cycle"
    );

    // Two concurrent requests, each checked against the same pre-state.
    let spawn = |change: GraphChange| {
        let graph = w.graph.clone();
        let canvas = c.id.clone();
        tokio::spawn(async move {
            graph
                .process(ApplyGraph {
                    actor: operator(),
                    canvas,
                    change,
                    dry_run: false,
                    expected_generation: None,
                })
                .await
        })
    };
    let (first, second) = tokio::join!(spawn(forward), spawn(backward));
    let applied = |result: &Result<_, _>| {
        matches!(
            result,
            Ok(orchestration::services::graph::ApplyOutcome { applied: true, .. })
        )
    };
    let (first, second) = (first.unwrap(), second.unwrap());
    // Exactly one edit commits. The loser is refused — by the fence (it checked
    // against the pre-state and lost the race) or, if it read the winner's
    // write, by the cycle check itself. Both are correct outcomes.
    assert!(
        applied(&first) ^ applied(&second),
        "exactly one of the jointly-invalid pair may commit: {first:?} {second:?}"
    );

    let after =
        w.db.process(LoadCanvasGraph {
            canvas: c.id.clone(),
        })
        .await?;
    assert_eq!(after.edges.len(), 1, "only the winning edge persists");
    Ok(())
}
