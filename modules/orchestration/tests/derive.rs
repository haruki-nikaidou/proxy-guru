//! Derivation: the worker config every server of a tree compiles to, per what
//! its worker reads.
//!
//! Set `UPDATE_GOLDEN=1` to rewrite the files under `tests/golden/`.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use guru_topology::{Route, Sticky, Weighted};
use guru_worker_config::table::{Policy, Target};
use guru_worker_config::{Config, ForwardingTo, LoadBalanceStrategy, To};
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::db::graph::{GraphRows, LoadCanvasGraph};
use orchestration::entities::db::pod::{PodIngress, ProxyProtocolVersion};
use orchestration::entities::db::server::{QuicCongestion, ServerId, ServerQuic};
use orchestration::services::derive::{DerivationCertificates, DerivedConfig, derive_tree};
use orchestration::services::graph::GraphChange;

fn regenerating() -> bool {
    matches!(
        std::env::var("UPDATE_GOLDEN").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Compares against `tests/golden/<name>.toml` and re-parses the emitted text.
fn assert_golden(name: &str, toml: &str) {
    let path = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden"))
        .join(format!("{name}.toml"));
    if regenerating() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, toml).unwrap();
    } else {
        let expected = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert_eq!(toml, expected, "derived config for {name} changed");
    }
    Config::from_toml_str(toml)
        .unwrap_or_else(|e| panic!("emitted config for {name} does not parse: {e}"));
}

/// The mobile entry of the fabric: `entry` on `mobile` fails over from a
/// weighted balance over two relays on `gcore` (QUIC) to a relay on `aws` (plain
/// TCP), each relay exiting to `origin`. `mobile` and `gcore` run workers that
/// read route tables and confirm relays; `aws` runs an older one.
struct Fabric {
    graph: GraphRows,
    mobile: ServerId,
    gcore: ServerId,
    aws: ServerId,
}

async fn fabric(w: &World) -> Result<Fabric, Box<dyn std::error::Error>> {
    let c = canvas(&w.db, "prod").await?;
    let mobile = server_at(&w.db, &c, "mobile", "203.0.113.10").await?;
    let gcore = server_at(&w.db, &c, "gcore", "198.51.100.20").await?;
    let aws = server_at(&w.db, &c, "aws", "192.0.2.30").await?;
    for (server, capabilities) in [
        (&mobile, vec!["relay_confirm", "route_table"]),
        (&gcore, vec!["relay_confirm", "route_table"]),
        (&aws, vec![]),
    ] {
        sqlx::query("UPDATE orchestration_server SET capabilities = $2 WHERE id = $1")
            .bind(&server.id)
            .bind(&capabilities)
            .execute(w.db.db())
            .await?;
    }
    for (server, up_mbps, down_mbps) in [(&mobile, 100, 500), (&gcore, 1000, 50)] {
        sqlx::query("UPDATE orchestration_server SET quic = $2 WHERE id = $1")
            .bind(&server.id)
            .bind(sqlx::types::Json(ServerQuic {
                congestion: QuicCongestion::Brutal,
                up_mbps,
                down_mbps,
                stream_receive_window: 0,
                conn_receive_window: 0,
            }))
            .execute(w.db.db())
            .await?;
    }

    let entry = client(&c, &mobile, "entry", 443, Some(ProxyProtocolVersion::V2));
    let g1 = pod(&c, &gcore, "gcore-1", 7443, PodIngress::RelayQuic);
    let g2 = pod(&c, &gcore, "gcore-2", 7444, PodIngress::RelayQuic);
    let a1 = pod(&c, &aws, "aws-1", 8443, PodIngress::RelayTcp);
    let origin = exit(&c, "origin", "origin.example.com:443");
    let to_g1 = edge_to_pod("to-gcore-1", &entry, &g1);
    let mut to_g2 = edge_to_pod("to-gcore-2", &entry, &g2);
    to_g2.override_ip = Some("2001:db8::20".to_string());
    let to_a1 = edge_to_pod("to-aws-1", &entry, &a1);
    let outs: Vec<_> = [&g1, &g2, &a1]
        .iter()
        .map(|pod| edge_to_exit(&format!("{}-out", pod.name), pod, &origin))
        .collect();
    let entry = routed(
        entry,
        Route::Failover(vec![
            Route::Balance {
                members: vec![
                    Weighted {
                        weight: 2,
                        to: via(&to_g1),
                    },
                    Weighted {
                        weight: 1,
                        to: via(&to_g2),
                    },
                ],
                sticky: Some(Sticky::ClientIp),
            },
            via(&to_a1),
        ]),
    );
    let change = GraphChange {
        put_pods: vec![
            entry,
            routed(g1, via(&outs[0])),
            routed(g2, via(&outs[1])),
            routed(a1, via(&outs[2])),
        ],
        put_exits: vec![origin],
        put_edges: [vec![to_g1, to_g2, to_a1], outs].concat(),
        ..GraphChange::default()
    };
    w.apply(&c, change).await?;
    let graph =
        w.db.process(LoadCanvasGraph {
            canvas: c.id.clone(),
        })
        .await?;
    Ok(Fabric {
        graph,
        mobile: mobile.id,
        gcore: gcore.id,
        aws: aws.id,
    })
}

fn derived(
    graph: &GraphRows,
    certificates: &DerivationCertificates,
) -> std::collections::BTreeMap<ServerId, DerivedConfig> {
    derive_tree(graph, certificates, &OrchestrationConfig::default())
        .unwrap()
        .into_iter()
        .map(|(server, derived)| (server, derived.unwrap()))
        .collect()
}

fn issued() -> DerivationCertificates {
    DerivationCertificates::assumed()
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_worker_that_reads_route_tables_gets_one(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let f = fabric(&w).await?;
    let configs = derived(&f.graph, &issued());

    let mobile = &configs[&f.mobile];
    assert!(mobile.invalid.is_empty(), "{:?}", mobile.invalid);
    let [entry] = mobile.config.forwardings.as_slice() else {
        panic!("{:?}", mobile.config.forwardings);
    };
    assert_eq!(
        entry.tag,
        key("entry"),
        "a forwarding is tagged with its pod's id"
    );
    let To::Route(root) = &entry.to else {
        panic!("a route-table worker gets a route: {:?}", entry.to);
    };
    let group = |id: &str| entry.groups.iter().find(|g| g.id == id).unwrap();
    let Policy::Failover { members } = &group(root).policy else {
        panic!("{:?}", group(root));
    };
    let Policy::Balance {
        members: balance,
        sticky,
    } = &group(&members[0]).policy
    else {
        panic!("{:?}", group(&members[0]));
    };
    assert_eq!(*sticky, Some(Sticky::ClientIp));
    assert_eq!(
        balance.iter().map(|m| m.weight).collect::<Vec<_>>(),
        vec![2, 1]
    );
    let upstream = |id: &str| entry.upstreams.iter().find(|u| u.id == id).unwrap();
    let Target::Relay(to_g2) = &upstream(&balance[1].to).target else {
        panic!("{:?}", upstream(&balance[1].to));
    };
    assert_eq!(
        to_g2.destination,
        guru_worker_config::Remote::parse("[2001:db8::20]:7444")?,
        "the edge's override address wins"
    );
    assert!(to_g2.confirm, "both ends confirm relays");
    assert!(
        to_g2.quic.is_some(),
        "a QUIC hop pairs the two servers' rates"
    );
    let Target::Relay(to_a1) = &upstream(&members[1]).target else {
        panic!("{:?}", upstream(&members[1]));
    };
    assert!(
        !to_a1.confirm,
        "the older worker at the far end cannot confirm"
    );

    // The dependency record: the listeners the entry dials, by identity.
    let mut ports: Vec<i64> = mobile.forwardings[0]
        .points_at
        .iter()
        .map(|cap| cap.port)
        .collect();
    ports.sort_unstable();
    assert_eq!(ports, vec![7443, 7444, 8443]);

    assert_golden("fabric_mobile", &mobile.config.to_toml_string().unwrap());
    assert_golden(
        "fabric_gcore",
        &configs[&f.gcore].config.to_toml_string().unwrap(),
    );
    Ok(())
}

/// A worker that reads only trees gets its routes as trees; and a pod dialing
/// it from a route-table worker gets no confirmation asked of it.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_older_worker_gets_trees(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let mut f = fabric(&w).await?;
    // Make the entry's server an older worker too.
    for server in f.graph.servers.iter_mut() {
        if server.id == f.mobile {
            server.capabilities.clear();
        }
    }
    let configs = derived(&f.graph, &issued());
    let entry = &configs[&f.mobile].config.forwardings[0];
    let Some(ForwardingTo::LoadBalance(failover)) = entry.to.tree() else {
        panic!("{:?}", entry.to);
    };
    assert_eq!(failover.strategy, LoadBalanceStrategy::Fallback);
    let ForwardingTo::LoadBalance(balance) = &failover.members[0] else {
        panic!("{:?}", failover.members[0]);
    };
    assert_eq!(balance.strategy, LoadBalanceStrategy::IpHash);
    assert_eq!(
        balance.members.len(),
        3,
        "weight 2:1 becomes the heavier member twice"
    );
    assert!(entry.groups.is_empty() && entry.upstreams.is_empty());

    let aws = &configs[&f.aws].config.forwardings[0];
    assert!(matches!(aws.to.tree(), Some(ForwardingTo::Exit { .. })));
    assert_golden(
        "fabric_aws",
        &configs[&f.aws].config.to_toml_string().unwrap(),
    );
    Ok(())
}

/// Certificates are part of the input: without the internal CA the QUIC
/// relays and everything dialing them are invalid, and nothing else is.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_pod_without_its_material_is_invalid_alone(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let f = fabric(&w).await?;
    let configs = derived(&f.graph, &DerivationCertificates::default());

    let mobile = &configs[&f.mobile];
    assert!(mobile.config.forwardings.is_empty());
    assert_eq!(mobile.invalid.len(), 1);
    assert_eq!(mobile.invalid[0].pod, pod_id("entry"));
    assert_eq!(mobile.invalid[0].name, "entry");
    assert_eq!(mobile.invalid[0].listen, "[::]:443");
    assert!(
        mobile.invalid[0]
            .error
            .contains("internal CA not initialised"),
        "{}",
        mobile.invalid[0].error
    );
    let gcore = &configs[&f.gcore];
    assert_eq!(gcore.invalid.len(), 2, "{:?}", gcore.invalid);
    assert!(gcore.config.relay_ca.is_none());
    let aws = &configs[&f.aws];
    assert!(aws.invalid.is_empty(), "a plain TCP relay needs no CA");
    assert_eq!(aws.config.forwardings.len(), 1);
    Ok(())
}

/// A graph that does not check — something only a write around the service
/// could store — fails every server of the tree rather than half of it.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_graph_that_does_not_check_derives_nothing(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let c = canvas(&w.db, "prod").await?;
    let tokyo = server(&w.db, &c, "tokyo").await?;
    let a = pod(&c, &tokyo, "a", 9443, PodIngress::RelayTcp);
    let b = pod(&c, &tokyo, "b", 9444, PodIngress::RelayTcp);
    let ab = edge_to_pod("ab", &a, &b);
    let ba = edge_to_pod("ba", &b, &a);
    insert_rows(
        &w.db,
        &c,
        vec![routed(a, via(&ab)), routed(b, via(&ba))],
        Vec::new(),
        vec![ab, ba],
    )
    .await?;
    w.derive(&c.id).await?;
    let view = w.view(&tokyo.id).await?;
    assert!(view.desired.is_none(), "nothing is published");
    assert!(
        view.derive_error
            .as_deref()
            .is_some_and(|e| e.contains("does not check")),
        "{:?}",
        view.derive_error
    );
    Ok(())
}
