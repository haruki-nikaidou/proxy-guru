//! Record id ↔ wire string conversion.
//!
//! Ids cross the gRPC boundary as the bare record **key** (no `table:` prefix), so
//! every encode/decode in this module funnels through here instead of being
//! reinvented per handler.

use crate::entities::surreal::ca::RelayCertificateId;
use crate::entities::surreal::canvas::CanvasId;
use crate::entities::surreal::certificate::CertificateId;
use crate::entities::surreal::connection::EdgeConnectionId;
use crate::entities::surreal::dns::DnsProviderId;
use crate::entities::surreal::health::{NodeHealthRecordId, ServerHealthRecordId};
use crate::entities::surreal::node::NodeId;
use crate::entities::surreal::port::PortId;
use crate::entities::surreal::server::{ServerId, ServerIpRecordId};
use surrealdb::types::{RecordId, RecordIdKey};
use surrealdb_types::ToSql;

/// The bare key of a record id, as sent on the wire.
pub fn record_key(record: &RecordId) -> String {
    match &record.key {
        RecordIdKey::String(s) => s.clone(),
        RecordIdKey::Number(n) => n.to_string(),
        RecordIdKey::Uuid(u) => u.to_string(),
        other => other.to_sql(),
    }
}

macro_rules! decoder {
    ($name:ident, $ty:ident, $table:literal) => {
        pub fn $name(key: &str) -> $ty {
            $ty(RecordId::new($table, key))
        }
    };
}

decoder!(canvas_id, CanvasId, "orchestration_canvas");
decoder!(server_id, ServerId, "orchestration_server");
decoder!(server_ip_id, ServerIpRecordId, "server_ip_record");
decoder!(node_id, NodeId, "orchestration_node");
decoder!(port_id, PortId, "orchestration_port");
decoder!(edge_id, EdgeConnectionId, "orchestration_edge_connection");
decoder!(dns_provider_id, DnsProviderId, "dns_provider");
decoder!(certificate_id, CertificateId, "certificate");
decoder!(
    relay_certificate_id,
    RelayCertificateId,
    "relay_certificate"
);
decoder!(
    server_health_record_id,
    ServerHealthRecordId,
    "server_health_record"
);
decoder!(
    node_health_record_id,
    NodeHealthRecordId,
    "node_health_record"
);
