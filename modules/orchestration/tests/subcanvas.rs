//! Nested subcanvases through the services: import rules, derived import ports
//! that follow export edits, tree-scoped deletion and derivation.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use kanau::processor::Processor;
use orchestration::entities::db::canvas::{CanvasEntity, CanvasId, FindRootCanvas};
use orchestration::entities::db::connection::FindEdgeById;
use orchestration::entities::db::node::{
    CanvasExportAs, CanvasExportConfig, CanvasImportConfig, EntryConfig, ExitConfig,
    FindNodeWithPorts, NodeId, NodeSpec, NodeWithPorts, PodConfig,
};
use orchestration::entities::db::port::{PortDirection, PortId, PortKind};
use orchestration::entities::db::server::{ServerId, ServerIpv6Resolve};
use orchestration::services::OrchestrationError;
use orchestration::services::canvas::{
    CreateCanvas, DeleteCanvas, GetCanvas, GetCanvasTree, ListCanvases,
};
use orchestration::services::edge::Connect;
use orchestration::services::node::{CreateNode, ReplaceNodeSpec, RetireNode};
use orchestration::services::server::{AddressOverrides, CreateServer};
use orchestration::services::topology::ProblemKind;

async fn canvas(w: &World, name: &str) -> Result<CanvasEntity, Box<dyn std::error::Error>> {
    Ok(w.canvases
        .process(CreateCanvas {
            actor: operator(),
            name: name.to_string(),
            description: String::new(),
        })
        .await?)
}

async fn server(
    w: &World,
    canvas: &CanvasId,
    name: &str,
    ip: &str,
) -> Result<(ServerId, ServerId), Box<dyn std::error::Error>> {
    let server = w
        .servers
        .process(CreateServer {
            actor: operator(),
            canvas: canvas.clone(),
            name: name.to_string(),
            icon: String::new(),
            comment: String::new(),
            position: pos0(),
            ipv6_resolve: ServerIpv6Resolve::Tolerated,
            log_level: "info".to_string(),
            addresses: AddressOverrides {
                override_v4: Some(ip.to_string()),
                override_v6: None,
                extra_addresses: Vec::new(),
            },
        })
        .await?;
    Ok((server.id.clone(), server.id))
}

async fn create(
    w: &World,
    canvas: &CanvasId,
    name: &str,
    spec: NodeSpec,
) -> Result<NodeWithPorts, OrchestrationError> {
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
}

fn import(canvas: &CanvasId) -> NodeSpec {
    NodeSpec::CanvasImport(CanvasImportConfig {
        canvas: canvas.clone(),
    })
}

fn export_out() -> NodeSpec {
    NodeSpec::CanvasExport(CanvasExportConfig {
        kind: PortKind::DeriveDestination,
        direction: CanvasExportAs::OutputOutOfCanvas,
    })
}

fn pod(server: &ServerId, port: u16) -> NodeSpec {
    NodeSpec::Pod(PodConfig {
        server: server.clone(),
        port,
        bind_ip: None,
        advertise_ip: None,
    })
}

fn entry() -> NodeSpec {
    NodeSpec::Entry(EntryConfig {
        receive_proxy_protocol: None,
        tls: None,
    })
}

fn exit(destination: &str) -> NodeSpec {
    NodeSpec::Exit(ExitConfig {
        destination: destination.to_string(),
        pass_proxy_protocol: None,
    })
}

async fn connect(
    w: &World,
    output: &PortId,
    input: &PortId,
) -> Result<orchestration::entities::db::connection::EdgeConnectionEntity, OrchestrationError> {
    w.edges
        .process(Connect {
            actor: operator(),
            output_port: output.clone(),
            input_port: input.clone(),
        })
        .await
}

async fn retire(w: &World, node: &NodeId) -> Result<(), OrchestrationError> {
    w.nodes
        .process(RetireNode {
            actor: operator(),
            node: node.clone(),
        })
        .await
}

async fn ports_of(w: &World, node: &NodeId) -> Vec<(String, PortKind, PortDirection, i64)> {
    let row =
        w.db.process(FindNodeWithPorts { id: node.clone() })
            .await
            .unwrap()
            .expect("node exists");
    row.ports
        .iter()
        .map(|p| (p.key.clone(), p.kind, p.direction, p.position))
        .collect()
}

fn problem_kind(err: &OrchestrationError) -> Option<ProblemKind> {
    match err {
        OrchestrationError::Topology(e) => Some(e.first.kind),
        _ => None,
    }
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn creating_and_retiring_an_export_reshapes_the_import_ports(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let root = canvas(&w, "root").await?;
    let sub = canvas(&w, "sub").await?;
    let (_, ip) = server(&w, &root.id, "tokyo", "203.0.113.10").await?;
    let importer = create(&w, &root.id, "sub", import(&sub.id)).await?;
    assert!(importer.ports.is_empty(), "no exports yet, no ports");

    let e1 = create(&w, &sub.id, "e1", export_out()).await?;
    let e1_key = e1.node.id.to_string();
    assert_eq!(
        ports_of(&w, &importer.node.id).await,
        vec![(
            e1_key.clone(),
            PortKind::DeriveDestination,
            PortDirection::Output,
            0
        )],
        "the importer mirrors the new export"
    );

    let p = create(&w, &root.id, "pod", pod(&ip, 443)).await?;
    let en = create(&w, &root.id, "entry", entry()).await?;
    connect(&w, &port_of(&p, "listen"), &port_of(&en, "listen")).await?;
    let importer_now =
        w.db.process(FindNodeWithPorts {
            id: importer.node.id.clone(),
        })
        .await?
        .unwrap();
    let edge = connect(
        &w,
        &port_of(&importer_now, &e1_key),
        &port_of(&p, "destination"),
    )
    .await?;

    let e2 = create(&w, &sub.id, "e2", export_out()).await?;
    let ports = ports_of(&w, &importer.node.id).await;
    assert_eq!(ports.len(), 2);
    assert!(ports.iter().any(|p| p.0 == e1_key));
    assert!(
        w.db.process(FindEdgeById {
            id: edge.id.clone()
        })
        .await?
        .is_some(),
        "a surviving mirrored port keeps its edge"
    );

    retire(&w, &e2.node.id).await?;
    assert_eq!(ports_of(&w, &importer.node.id).await.len(), 1);
    assert!(
        w.db.process(FindEdgeById {
            id: edge.id.clone()
        })
        .await?
        .is_some()
    );

    retire(&w, &e1.node.id).await?;
    assert!(ports_of(&w, &importer.node.id).await.is_empty());
    assert!(
        w.db.process(FindEdgeById { id: edge.id }).await?.is_none(),
        "the dropped mirrored port took the parent edge with it"
    );
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn import_rules_are_enforced_by_the_service(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let root = canvas(&w, "root").await?;
    let sub = canvas(&w, "sub").await?;
    let third = canvas(&w, "third").await?;

    let err = create(&w, &root.id, "me", import(&root.id))
        .await
        .expect_err("self import");
    assert_eq!(problem_kind(&err), Some(ProblemKind::CanvasImportSelf));

    let importer = create(&w, &root.id, "sub", import(&sub.id)).await?;
    let err = create(&w, &sub.id, "root", import(&root.id))
        .await
        .expect_err("ancestor import");
    assert_eq!(problem_kind(&err), Some(ProblemKind::CanvasImportAncestor));

    let err = create(&w, &third.id, "sub-again", import(&sub.id))
        .await
        .expect_err("duplicate import");
    assert_eq!(problem_kind(&err), Some(ProblemKind::CanvasImportDuplicate));

    let err = create(
        &w,
        &root.id,
        "ghost",
        import(&orchestration::utils::ids::canvas_id("ghost")),
    )
    .await
    .expect_err("missing target");
    assert!(matches!(err, OrchestrationError::NotFound), "{err:?}");

    let err = w
        .nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: importer.node.id.clone(),
            spec: import(&third.id),
            item_count: 0,
        })
        .await
        .expect_err("an import target is immutable");
    assert!(matches!(err, OrchestrationError::Invalid(_)), "{err:?}");
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn deleting_an_imported_canvas_is_refused_until_its_import_is_retired(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let root = canvas(&w, "root").await?;
    let sub = canvas(&w, "sub").await?;
    let importer = create(&w, &root.id, "sub", import(&sub.id)).await?;

    let delete = |canvas: CanvasId| {
        w.canvases.process(DeleteCanvas {
            actor: operator(),
            canvas,
        })
    };
    let err = delete(sub.id.clone()).await.expect_err("still imported");
    assert!(matches!(err, OrchestrationError::Conflict(_)), "{err:?}");

    retire(&w, &importer.node.id).await?;
    delete(sub.id.clone()).await?;
    Ok(())
}

/// root -> sub -> subsub, each with a server and a node.
async fn three_levels(
    w: &World,
) -> Result<(CanvasEntity, CanvasEntity, CanvasEntity), Box<dyn std::error::Error>> {
    let root = canvas(w, "root").await?;
    let sub = canvas(w, "sub").await?;
    let subsub = canvas(w, "subsub").await?;
    create(w, &root.id, "sub", import(&sub.id)).await?;
    create(w, &sub.id, "subsub", import(&subsub.id)).await?;
    for (c, ip) in [
        (&root, "203.0.113.10"),
        (&sub, "203.0.113.20"),
        (&subsub, "203.0.113.30"),
    ] {
        let (_, ip) = server(w, &c.id, &c.name, ip).await?;
        let p = create(w, &c.id, "pod", pod(&ip, 443)).await?;
        let en = create(w, &c.id, "entry", entry()).await?;
        let ex = create(w, &c.id, "exit", exit("10.0.0.5:8080")).await?;
        connect(w, &port_of(&p, "listen"), &port_of(&en, "listen")).await?;
        connect(w, &port_of(&ex, "destination"), &port_of(&p, "destination")).await?;
    }
    Ok((root, sub, subsub))
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn deleting_a_root_deletes_its_whole_tree(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (root, _, _) = three_levels(&w).await?;
    w.canvases
        .process(DeleteCanvas {
            actor: operator(),
            canvas: root.id,
        })
        .await?;
    for table in [
        "orchestration_canvas",
        "orchestration_node",
        "orchestration_port",
        "orchestration_edge_connection",
        "orchestration_server",
        "orchestration_server_config_view",
    ] {
        let rows: Vec<String> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT id FROM {table}")))
                .fetch_all(w.db.db())
                .await?;
        assert!(rows.is_empty(), "{table} still holds {rows:?}");
    }
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_edit_in_the_innermost_canvas_bumps_only_the_servers_whose_path_crosses_it(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let root = canvas(&w, "root").await?;
    let sub = canvas(&w, "sub").await?;
    let subsub = canvas(&w, "subsub").await?;
    let (a, ip_a) = server(&w, &root.id, "a", "203.0.113.10").await?;
    let (b, ip_b) = server(&w, &root.id, "b", "203.0.113.20").await?;

    // Server b: a self-contained chain in the root.
    let pod_b = create(&w, &root.id, "shallow", pod(&ip_b, 443)).await?;
    let entry_b = create(&w, &root.id, "entry_b", entry()).await?;
    let exit_b = create(&w, &root.id, "exit_b", exit("10.0.0.9:8080")).await?;
    connect(&w, &port_of(&pod_b, "listen"), &port_of(&entry_b, "listen")).await?;
    connect(
        &w,
        &port_of(&exit_b, "destination"),
        &port_of(&pod_b, "destination"),
    )
    .await?;

    // Server a: destination crosses root -> sub -> subsub to an exit.
    let deep_exit = create(&w, &subsub.id, "exit_deep", exit("10.0.0.5:8080")).await?;
    let subsub_out = create(&w, &subsub.id, "subsub_out", export_out()).await?;
    connect(
        &w,
        &port_of(&deep_exit, "destination"),
        &port_of(&subsub_out, "export"),
    )
    .await?;
    let import_subsub = create(&w, &sub.id, "subsub", import(&subsub.id)).await?;
    let sub_out = create(&w, &sub.id, "sub_out", export_out()).await?;
    connect(
        &w,
        &port_of(&import_subsub, subsub_out.node.id.as_ref()),
        &port_of(&sub_out, "export"),
    )
    .await?;
    let import_sub = create(&w, &root.id, "sub", import(&sub.id)).await?;
    let pod_a = create(&w, &root.id, "deep", pod(&ip_a, 443)).await?;
    let entry_a = create(&w, &root.id, "entry_a", entry()).await?;
    connect(&w, &port_of(&pod_a, "listen"), &port_of(&entry_a, "listen")).await?;
    connect(
        &w,
        &port_of(&import_sub, sub_out.node.id.as_ref()),
        &port_of(&pod_a, "destination"),
    )
    .await?;

    w.derive(&root.id).await?;
    let rev = |view: orchestration::entities::db::view::ServerConfigViewEntity| {
        view.desired.expect("derived").revision
    };
    let a_before = rev(w.view(&a).await?);
    let b_before = rev(w.view(&b).await?);
    assert!(
        w.view(&a)
            .await?
            .desired
            .unwrap()
            .toml
            .contains("10.0.0.5:8080"),
        "a's path reaches the innermost exit"
    );

    let root_row =
        w.db.process(FindRootCanvas {
            canvas: sub.id.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(root_row.id.0, root.id.0);
    let gen_before = root_row.generation;

    w.nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: deep_exit.node.id.clone(),
            spec: exit("10.0.0.6:9090"),
            item_count: 0,
        })
        .await?;
    let root_row =
        w.db.process(FindRootCanvas {
            canvas: subsub.id.clone(),
        })
        .await?
        .unwrap();
    assert!(
        root_row.generation > gen_before,
        "an edit anywhere in the tree bumps the root"
    );

    // The hook is told about the innermost canvas and resolves the root itself.
    w.derive(&subsub.id).await?;
    let a_view = w.view(&a).await?;
    assert_eq!(a_view.desired.as_ref().unwrap().revision, a_before + 1);
    assert!(a_view.desired.unwrap().toml.contains("10.0.0.6:9090"));
    assert_eq!(
        rev(w.view(&b).await?),
        b_before,
        "b's path never crossed the edit"
    );
    let root_row =
        w.db.process(FindRootCanvas {
            canvas: root.id.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(root_row.generation, root_row.derived_generation);
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn list_and_tree(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (root, sub, subsub) = three_levels(&w).await?;

    let names = |canvases: Vec<CanvasEntity>| {
        let mut names: Vec<String> = canvases.into_iter().map(|c| c.name).collect();
        names.sort();
        names
    };
    let roots = w
        .canvases
        .process(ListCanvases {
            actor: operator(),
            include_subcanvases: false,
        })
        .await?;
    assert_eq!(names(roots), ["root"]);
    let all = w
        .canvases
        .process(ListCanvases {
            actor: operator(),
            include_subcanvases: true,
        })
        .await?;
    assert_eq!(names(all), ["root", "sub", "subsub"]);

    let tree = w
        .canvases
        .process(GetCanvasTree {
            actor: operator(),
            canvas: subsub.id.clone(),
        })
        .await?;
    assert_eq!(tree.canvas.id.0, root.id.0);
    assert_eq!(tree.children.len(), 1);
    assert_eq!(tree.children[0].canvas.id.0, sub.id.0);
    assert_eq!(tree.children[0].children.len(), 1);
    assert_eq!(tree.children[0].children[0].canvas.id.0, subsub.id.0);
    assert!(tree.children[0].children[0].children.is_empty());

    let contents = w
        .canvases
        .process(GetCanvas {
            actor: operator(),
            canvas: subsub.id.clone(),
        })
        .await?;
    let ancestors: Vec<String> = contents.ancestors.iter().map(|c| c.name.clone()).collect();
    assert_eq!(ancestors, ["root", "sub"]);
    assert!(contents.import_targets.is_empty());
    // subsub's own three nodes plus the universal pod its server was created
    // with.
    assert_eq!(contents.nodes.len(), 4, "only subsub's own nodes");
    assert_eq!(contents.servers.len(), 1);

    let contents = w
        .canvases
        .process(GetCanvas {
            actor: operator(),
            canvas: sub.id.clone(),
        })
        .await?;
    let targets: Vec<String> = contents
        .import_targets
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert_eq!(targets, ["subsub"]);
    Ok(())
}
