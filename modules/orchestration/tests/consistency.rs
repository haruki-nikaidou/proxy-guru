//! A graph edit is checked against the tree it *would* produce and then written
//! in a separate transaction. Two edits that each check on their own but jointly
//! break an invariant must not both commit. The fenced generation bump is what
//! stops them: only the edit that still matches the generation it was checked
//! against lands.
//!
//! Concurrent writers must also never wait on each other in a circle. Every
//! transaction that writes a canvas tree's rows takes the tree's root row first
//! (`entities::db::fence`), so two of them queue on that one row. The tests
//! built on [`race`] force each interleaving that used to end in
//! `deadlock_detected`.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use base::db::Db;
use common::*;
use kanau::processor::Processor;
use orchestration::entities::db::canvas::{CanvasEntity, DeleteCanvasRow, FindCanvasById};
use orchestration::entities::db::graph::{ApplyGraphBatch, LoadCanvasGraph, MoveGraphItems};
use orchestration::entities::db::group::{GroupEntity, GroupId, GroupMember};
use orchestration::entities::db::pod::PodIngress;
use orchestration::entities::db::server::{
    DeleteServerRow, FindServerById, ReportedAddresses, UpdateReportedAddresses,
    UpdateServerSettings,
};
use orchestration::entities::db::view::{
    CommitCanvasDerivation, ConfigSnapshot, FindServerConfigView, ViewUpdate,
};
use orchestration::hooks::derive::DeriveCanvas;
use orchestration::services::graph::{ApplyGraph, GraphChange};
use orchestration::services::rollout::ForgetServerApplied;
use orchestration::services::server::DeleteServer;
use sqlx::PgPool;
use std::future::Future;
use std::time::Duration;
use tokio::task::JoinHandle;

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
    assert!(!dry(forward.clone()).await.diagnostics.iter().any(|d| d.error));
    assert!(!dry(backward.clone()).await.diagnostics.iter().any(|d| d.error));
    let both = GraphChange {
        put_pods: vec![routed(a.clone(), via(&a_to_b)), routed(b.clone(), via(&b_to_a))],
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
        matches!(result, Ok(orchestration::services::graph::ApplyOutcome { applied: true, .. }))
    };
    let (first, second) = (first.unwrap(), second.unwrap());
    // Exactly one edit commits. The loser is refused — by the fence (it checked
    // against the pre-state and lost the race) or, if it read the winner's
    // write, by the cycle check itself. Both are correct outcomes.
    assert!(
        applied(&first) ^ applied(&second),
        "exactly one of the jointly-invalid pair may commit: {first:?} {second:?}"
    );

    let after = w
        .db
        .process(LoadCanvasGraph {
            canvas: c.id.clone(),
        })
        .await?;
    assert_eq!(after.edges.len(), 1, "only the winning edge persists");
    Ok(())
}

// --- lock order -------------------------------------------------------------

/// Locks the config view row of the server bound as `$1`, the row a server
/// delete cascades to and a derivation commit rewrites.
const VIEW_ROW: &str =
    "SELECT 1 FROM orchestration_server_config_view WHERE server = $1 FOR UPDATE";

/// How long a racer may take to reach the lock it is expected to wait on.
const REACH_LOCK_WITHIN: Duration = Duration::from_secs(10);

/// A race keeps up to four connections open while its writers wait, and every
/// `#[sqlx::test]` pool in this binary draws on one shared budget of 20. Races
/// run side by side could spend it all and then wait on each other for a
/// connection until the pool times out, so only one runs at a time.
static ONE_RACE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Returns once `waiters` sessions of this test's database are waiting on a
/// lock, or once `task` has finished without having to wait.
async fn until_waiting<T>(pool: &PgPool, waiters: i64, task: &JoinHandle<T>) {
    let deadline = tokio::time::Instant::now() + REACH_LOCK_WITHIN;
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND wait_event_type = 'Lock'",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if waiting >= waiters || task.is_finished() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "never saw {waiters} session(s) waiting on a lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Forces the interleaving a lock-order deadlock needs.
///
/// `hold` locks one row (`$1` is `key`) in a transaction of its own. `first`
/// runs until it waits on that row, keeping every lock it took before it;
/// `second` runs until it waits too; only then is the row released. Two
/// writers whose lock orders disagree deadlock here every time, and PostgreSQL
/// aborts one of them after `deadlock_timeout` with `deadlock_detected`.
async fn race<A, B>(
    pool: &PgPool,
    hold: &'static str,
    key: &str,
    first: A,
    second: B,
) -> (A::Output, B::Output)
where
    A: Future + Send + 'static,
    A::Output: Send + 'static,
    B: Future + Send + 'static,
    B::Output: Send + 'static,
{
    let _alone = ONE_RACE_AT_A_TIME.lock().await;
    let mut holder = pool.begin().await.unwrap();
    sqlx::query(hold)
        .bind(key)
        .execute(&mut *holder)
        .await
        .unwrap();
    let first = tokio::spawn(first);
    until_waiting(pool, 1, &first).await;
    let second = tokio::spawn(second);
    until_waiting(pool, 2, &second).await;
    holder.commit().await.unwrap();
    let (first, second) = tokio::join!(first, second);
    (first.unwrap(), second.unwrap())
}

/// A derivation pass over `canvas`'s tree, as the consumer runs it.
fn deriving(
    w: &World,
    canvas: &CanvasEntity,
) -> impl Future<Output = Result<(), wakuwaku::Error>> + Send + 'static {
    let deriver = w.deriver.clone();
    let canvas = canvas.id.clone();
    async move { deriver.process(DeriveCanvas { canvas }).await }
}

/// The tree's derivation is level with every edit, the raced one included.
async fn assert_derived(db: &Db, canvas: &CanvasEntity) -> TestResult {
    let root = db
        .process(FindCanvasById {
            id: canvas.id.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(
        root.derived_generation, root.generation,
        "the derivation was redone after the write it raced"
    );
    Ok(())
}

/// Deleting a server while its canvas is re-derived, as seen live on
/// 2026-09-17. The delete cascaded to the server's view row before its fence
/// bumped the canvas; the derivation commit takes the canvas row first and the
/// view rows after it. Each waited on the other until PostgreSQL aborted one,
/// and a delete that lost reached the dashboard as `deadlock_detected`.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_server_deleted_while_its_canvas_derives_does_not_deadlock(pool: PgPool) -> TestResult {
    let w = world(pool.clone()).await?;
    let c = canvas(&w.db, "prod").await?;
    let s = server(&w.db, &c, "tokyo").await?;

    let delete = {
        let servers = w.servers.clone();
        let server = s.id.clone();
        async move {
            servers
                .process(DeleteServer {
                    actor: operator(),
                    server,
                })
                .await
        }
    };
    let (deleted, derived) = race(&pool, VIEW_ROW, s.id.as_str(), delete, deriving(&w, &c)).await;
    deleted.expect("the delete commits");
    derived.expect("the derivation is not aborted");

    assert!(w.db.process(FindServerById { id: s.id }).await?.is_none());
    assert_derived(&w.db, &c).await
}

/// A pass publishing a revision wrote each view row twice: the slots, then the
/// cleared failure. PostgreSQL re-checks a row's foreign key when a transaction
/// updates that row a second time, so the commit took KEY SHARE on the server
/// row after the view row, while a worker's address report locks the server
/// row before its view row. One statement per view row keeps derivation off the
/// server rows.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_derivation_commit_and_an_address_report_do_not_deadlock(pool: PgPool) -> TestResult {
    let sp = setup(pool.clone());
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    let generation = sp
        .process(FindCanvasById { id: c.id.clone() })
        .await?
        .unwrap()
        .generation;
    let now = chrono::Utc::now();
    let commit = CommitCanvasDerivation {
        canvas: c.id.clone(),
        generation,
        view_seq: 0,
        updates: vec![ViewUpdate {
            server: s.id.clone(),
            desired: Some(ConfigSnapshot {
                revision: 1,
                toml: "# revision 1".to_string(),
                created_at: now,
                forwardings: Vec::new(),
                certificates: Vec::new(),
            }),
            derive_error: None,
            invalid_pods: Vec::new(),
            waiting_for: Vec::new(),
            clear_failure: true,
        }],
    };
    let report = UpdateReportedAddresses {
        server: s.id.clone(),
        generation: s.refresh_key_generation,
        reported: ReportedAddresses {
            public_v4: Some("198.51.100.7".to_string()),
            public_v6: None,
            interfaces: Vec::new(),
            reported_at: now,
        },
    };
    let (committed, reported) = race(
        &pool,
        VIEW_ROW,
        s.id.as_str(),
        {
            let sp = sp.clone();
            async move { sp.process(commit).await }
        },
        {
            let sp = sp.clone();
            async move { sp.process(report).await }
        },
    )
    .await;
    assert!(
        committed.expect("the commit is not aborted"),
        "nothing moved the canvas, so the pass publishes"
    );
    assert!(
        reported.expect("the report is not aborted"),
        "and the report lands"
    );

    let view = sp
        .process(FindServerConfigView { server: s.id })
        .await?
        .unwrap();
    assert_eq!(view.desired.map(|d| d.revision), Some(1));
    assert!(view.failed_revision.is_none() && view.apply_error.is_none());
    assert_eq!(view.seq, 1, "the report moved the view's seq");
    Ok(())
}

/// Forgetting what a server runs cleared its view row and only then bumped the
/// canvas: the delete's order, and the same deadlock against a derivation.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_server_forgotten_while_its_canvas_derives_does_not_deadlock(pool: PgPool) -> TestResult {
    let w = world(pool.clone()).await?;
    let c = canvas(&w.db, "prod").await?;
    let s = server(&w.db, &c, "tokyo").await?;

    let forget = {
        let rollout = w.rollout.clone();
        let server = s.id.clone();
        async move {
            rollout
                .process(ForgetServerApplied {
                    actor: operator(),
                    server,
                })
                .await
        }
    };
    let (forgotten, derived) = race(&pool, VIEW_ROW, s.id.as_str(), forget, deriving(&w, &c)).await;
    forgotten.expect("the forget commits");
    derived.expect("the derivation is not aborted");
    assert_derived(&w.db, &c).await
}

/// Deleting a subcanvas locked the subcanvas, its servers and their view rows,
/// and bumped the tree's root last.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_subcanvas_deleted_while_its_tree_derives_does_not_deadlock(pool: PgPool) -> TestResult {
    let w = world(pool.clone()).await?;
    let root = canvas(&w.db, "prod").await?;
    let sub = subcanvas(&w.db, &root, "edge").await?;
    let s = server(&w.db, &sub, "tokyo").await?;

    let delete = {
        let db = w.db.clone();
        let id = sub.id.clone();
        async move { db.process(DeleteCanvasRow { id }).await }
    };
    let (deleted, derived) =
        race(&pool, VIEW_ROW, s.id.as_str(), delete, deriving(&w, &root)).await;
    deleted.expect("the delete commits");
    derived.expect("the derivation is not aborted");

    assert!(w.db.process(FindServerById { id: s.id }).await?.is_none());
    assert_derived(&w.db, &root).await
}

/// An edit and a delete of one server. Neither ever deadlocked on its own, but
/// they share one order: were either to lock the server row before the root
/// again while the other does not, this pair would.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_server_edited_while_it_is_deleted_does_not_deadlock(pool: PgPool) -> TestResult {
    let sp = setup(pool.clone());
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;

    let update = UpdateServerSettings {
        id: s.id.clone(),
        canvas: c.id.clone(),
        name: "tokyo-1".to_string(),
        icon: s.icon.clone(),
        comment: s.comment.clone(),
        ipv6_resolve: s.ipv6_resolve,
        log_level: s.log_level,
        quic: s.quic,
        override_v4: s.override_v4.clone(),
        override_v6: s.override_v6.clone(),
        extra_addresses: s.extra_addresses.clone(),
        agent_unit: None,
        fence: None,
    };
    let delete = DeleteServerRow {
        id: s.id.clone(),
        canvas: c.id.clone(),
        fence: None,
    };
    let (updated, deleted) = race(
        &pool,
        "SELECT 1 FROM orchestration_server WHERE id = $1 FOR UPDATE",
        s.id.as_str(),
        {
            let sp = sp.clone();
            async move { sp.process(update).await }
        },
        {
            let sp = sp.clone();
            async move { sp.process(delete).await }
        },
    )
    .await;
    assert_eq!(updated.expect("the edit commits").name, "tokyo-1");
    deleted.expect("the delete commits after it");
    assert!(sp.process(FindServerById { id: s.id }).await?.is_none());
    Ok(())
}

/// Moving items locked a server row, then an exit, then a subcanvas; deleting
/// that subcanvas locks it and then, by cascade, its servers. Positions bump
/// nothing, but a move takes the tree's root first all the same.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn items_moved_while_their_subcanvas_is_deleted_do_not_deadlock(pool: PgPool) -> TestResult {
    let sp = setup(pool.clone());
    let root = canvas(&sp, "prod").await?;
    let sub = subcanvas(&sp, &root, "edge").await?;
    let s = server(&sp, &sub, "tokyo").await?;
    let origin = exit(&root, "origin", "10.0.0.5:8080");
    insert_rows(&sp, &root, Vec::new(), vec![origin.clone()], Vec::new()).await?;

    let moves = MoveGraphItems {
        tree: vec![root.id.clone(), sub.id.clone()],
        servers: vec![(s.id.clone(), pos(10, 10))],
        exits: vec![(origin.id.clone(), pos(20, 20))],
        canvases: vec![(sub.id.clone(), pos(30, 30))],
    };
    let (moved, deleted) = race(
        &pool,
        "SELECT 1 FROM orchestration_exit WHERE id = $1 FOR UPDATE",
        origin.id.as_str(),
        {
            let sp = sp.clone();
            async move { sp.process(moves).await }
        },
        {
            let sp = sp.clone();
            let id = sub.id.clone();
            async move { sp.process(DeleteCanvasRow { id }).await }
        },
    )
    .await;
    moved.expect("the move commits");
    deleted.expect("the delete commits after it");
    assert!(sp.process(FindServerById { id: s.id }).await?.is_none());
    Ok(())
}

/// A batch of groups alone bumps nothing, yet its membership rows reference
/// servers: it deleted a group (cascading to a server's membership row) and
/// then inserted one naming that server (KEY SHARE on the server row), while a
/// server delete locks the server row and cascades to its membership rows.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn groups_rewritten_while_their_server_is_deleted_do_not_deadlock(
    pool: PgPool,
) -> TestResult {
    let sp = setup(pool.clone());
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    let group = |name: &str| GroupEntity {
        id: GroupId::from_key(key(name)),
        canvas: c.id.clone(),
        kind: "bundle".to_string(),
        name: name.to_string(),
        props: serde_json::json!({}),
        members: vec![GroupMember::Server(s.id.clone())],
    };
    let before = group("before");
    sp.process(ApplyGraphBatch {
        canvas: Some(c.id.clone()),
        insert_groups: vec![before.clone()],
        ..ApplyGraphBatch::default()
    })
    .await?;

    let regroup = ApplyGraphBatch {
        canvas: Some(c.id.clone()),
        delete_groups: vec![before.id.clone()],
        insert_groups: vec![group("after")],
        ..ApplyGraphBatch::default()
    };
    let delete = DeleteServerRow {
        id: s.id.clone(),
        canvas: c.id.clone(),
        fence: None,
    };
    let (regrouped, deleted) = race(
        &pool,
        "SELECT 1 FROM orchestration_canvas WHERE id = $1 FOR UPDATE",
        c.id.as_str(),
        {
            let sp = sp.clone();
            async move { sp.process(regroup).await }
        },
        {
            let sp = sp.clone();
            async move { sp.process(delete).await }
        },
    )
    .await;
    regrouped.expect("the groups commit");
    deleted.expect("the delete commits after them");
    assert!(sp.process(FindServerById { id: s.id }).await?.is_none());
    Ok(())
}
