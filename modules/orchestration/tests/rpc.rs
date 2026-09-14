//! Transport contracts of the operator API that a client cannot infer from the
//! services alone.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use base::services::config::ConfigStore;
use common::*;
use orchestration::rpc::OrchestrationGrpc;
use orchestration::services::config::OrchestrationConfigService;
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
        configs: OrchestrationConfigService {
            configs: ConfigStore { db: w.db.clone() },
        },
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

/// `ConnectPorts` takes a universal handle in place of a port id; the reply's
/// edge starts on the port the handle created.
#[tokio::test]
async fn connect_ports_accepts_universal_handles() -> TestResult {
    let w = world().await?;
    let api = grpc(&w);
    let root = create_canvas(&api, "root").await;
    let server = api
        .create_server(as_operator(pb::CreateServerRequest {
            canvas_id: root.id.clone(),
            name: "us".to_string(),
            icon: String::new(),
            comment: String::new(),
            position: None,
            ipv6_resolve: pb::Ipv6Resolve::Ipv6Tolerated.into(),
            log_level: "info".to_string(),
            override_v4: "198.51.100.1".to_string(),
            override_v6: String::new(),
            extra_addresses: Vec::new(),
        }))
        .await?
        .into_inner()
        .server
        .unwrap();
    let create = |name: &str, spec: pb::node_spec::Spec| {
        as_operator(pb::CreateNodeRequest {
            canvas_id: root.id.clone(),
            name: name.to_string(),
            comment: String::new(),
            spec: Some(pb::NodeSpec { spec: Some(spec) }),
            position: None,
            item_count: 0,
        })
    };
    let pod = api
        .create_node(create(
            "p0",
            pb::node_spec::Spec::Pod(pb::PodConfig {
                port: 10000,
                server_id: server.id.clone(),
                bind_ip: String::new(),
                advertise_ip: String::new(),
            }),
        ))
        .await?
        .into_inner()
        .node
        .unwrap();
    let ud = api
        .create_node(create(
            "fan",
            pb::node_spec::Spec::UniversalDistribute(pb::UniversalDistributeConfig {
                mode: pb::LoadBalanceMode::RoundRobin.into(),
                protocol: pb::RelayProtocol::RelayTcpRaw.into(),
            }),
        ))
        .await?
        .into_inner()
        .node
        .unwrap();
    assert!(ud.ports.is_empty(), "a distributor starts without ports");
    let destination = pod.ports.iter().find(|p| p.key == "destination").unwrap();
    let edge = api
        .connect_ports(as_operator(pb::ConnectRequest {
            output_port_id: String::new(),
            input_port_id: destination.id.clone(),
            output_handle: Some(pb::UniversalHandle {
                node_id: ud.id.clone(),
                group: pb::UniversalGroup::ChannelOut.into(),
            }),
            input_handle: None,
        }))
        .await?
        .into_inner()
        .edge
        .unwrap();
    assert_eq!(edge.target_port_id, destination.id);
    let canvas = api
        .get_canvas(as_operator(pb::GetCanvasRequest {
            canvas_id: root.id.clone(),
        }))
        .await?
        .into_inner();
    let ud_now = canvas.nodes.iter().find(|n| n.id == ud.id).unwrap();
    let chan = ud_now
        .ports
        .iter()
        .find(|p| p.id == edge.source_port_id)
        .expect("the edge starts on the created channel port");
    assert_eq!(chan.key, format!("chan:{}", pod.id));
    assert_eq!(chan.kind, i32::from(pb::PortKind::DeriveDestination));
    // The universal pod the server came with is reported with its fixed port.
    let up = canvas
        .nodes
        .iter()
        .find(|n| matches!(&n.spec, Some(pb::NodeSpec { spec: Some(pb::node_spec::Spec::UniversalPod(_)) })))
        .expect("the server's universal pod");
    assert_eq!(up.ports.len(), 1);
    assert_eq!(up.ports[0].kind, i32::from(pb::PortKind::Bundle));
    assert!(up.lane.is_none());

    // A missing end is an argument error, not a crash.
    let err = api
        .connect_ports(as_operator(pb::ConnectRequest {
            output_port_id: String::new(),
            input_port_id: destination.id.clone(),
            output_handle: None,
            input_handle: None,
        }))
        .await
        .expect_err("no port and no handle");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    Ok(())
}
