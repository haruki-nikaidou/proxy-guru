//! Transport contracts of the operator API that a client cannot infer from the
//! services alone.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use orchestration::rpc::OrchestrationGrpc;
use rpguru_sdk::orchestration as pb;
use rpguru_sdk::orchestration::orchestration_server::Orchestration;
use tonic::Request;

fn grpc(w: &World) -> OrchestrationGrpc {
    OrchestrationGrpc {
        canvases: w.canvases.clone(),
        servers: w.servers.clone(),
        nodes: w.nodes.clone(),
        edges: w.edges.clone(),
        rollout: w.rollout.clone(),
        health: w.health.clone(),
        dns: w.dns.clone(),
        certificates: w.certificates.clone(),
    }
}

/// A request carrying the identity the auth middleware would have injected.
fn as_operator<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.extensions_mut().insert(operator());
    request
}

async fn create_canvas(api: &OrchestrationGrpc, name: &str) -> pb::Canvas {
    api.create_canvas(as_operator(pb::CreateCanvasRequest {
        name: name.to_string(),
        description: String::new(),
    }))
    .await
    .unwrap()
    .into_inner()
    .canvas
    .unwrap()
}

/// `Node.import_target` is set on every reply that carries an import node, not
/// only on `GetCanvas`: a client must not need a second round trip to label the
/// node it just created or moved.
#[tokio::test]
async fn mutation_replies_carry_the_import_target() -> TestResult {
    let w = world().await?;
    let api = grpc(&w);
    let root = create_canvas(&api, "root").await;
    let sub = create_canvas(&api, "sub").await;

    let created = api
        .create_node(as_operator(pb::CreateNodeRequest {
            canvas_id: root.id.clone(),
            name: "sub".to_string(),
            comment: String::new(),
            spec: Some(pb::NodeSpec {
                spec: Some(pb::node_spec::Spec::CanvasImport(pb::CanvasImportConfig {
                    canvas_id: sub.id.clone(),
                })),
            }),
            position: None,
            item_count: 0,
        }))
        .await?
        .into_inner()
        .node
        .unwrap();
    let target = created
        .import_target
        .expect("CreateNode reply names the target");
    assert_eq!(
        (target.id.as_str(), target.name.as_str()),
        (sub.id.as_str(), "sub")
    );

    let moved = api
        .update_node_meta(as_operator(pb::UpdateNodeMetaRequest {
            node_id: created.id.clone(),
            name: "sub".to_string(),
            comment: "moved".to_string(),
            position: Some(pb::CanvasUiPosition { x: 5, y: 5 }),
        }))
        .await?
        .into_inner()
        .node
        .unwrap();
    let target = moved
        .import_target
        .expect("UpdateNodeMeta reply names the target");
    assert_eq!(target.id, sub.id);

    // A non-import node never carries one.
    let exit = api
        .create_node(as_operator(pb::CreateNodeRequest {
            canvas_id: root.id.clone(),
            name: "exit".to_string(),
            comment: String::new(),
            spec: Some(pb::NodeSpec {
                spec: Some(pb::node_spec::Spec::Exit(pb::ExitConfig {
                    destination: "10.0.0.5:8080".to_string(),
                    pass_proxy_protocol: pb::ProxyProtocolVersion::Unspecified.into(),
                })),
            }),
            position: None,
            item_count: 0,
        }))
        .await?
        .into_inner()
        .node
        .unwrap();
    assert!(exit.import_target.is_none());
    Ok(())
}
