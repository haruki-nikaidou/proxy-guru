//! Row id ↔ wire string conversion.
//!
//! Ids cross the gRPC boundary as the bare key, so every decode in the RPC layer
//! funnels through here instead of being reinvented per handler.

use crate::entities::db::ca::RelayCertificateId;
use crate::entities::db::canvas::CanvasId;
use crate::entities::db::certificate::CertificateId;
use crate::entities::db::connection::EdgeConnectionId;
use crate::entities::db::dns::DnsProviderId;
use crate::entities::db::health::{NodeHealthRecordId, ServerHealthRecordId};
use crate::entities::db::node::NodeId;
use crate::entities::db::port::PortId;
use crate::entities::db::server::ServerId;

/// The bare key of an id, as sent on the wire.
///
/// Transitional: with text ids this is an identity copy of `id.0`, and new code
/// writes `id.to_string()` or compares ids directly. Kept so the call sites that
/// predate the PostgreSQL move compile unchanged until they are swept.
pub fn record_key(key: &str) -> String {
    key.to_owned()
}

macro_rules! decoder {
    ($name:ident, $ty:ident) => {
        pub fn $name(key: &str) -> $ty {
            $ty::from_key(key)
        }
    };
}

decoder!(canvas_id, CanvasId);
decoder!(server_id, ServerId);
decoder!(node_id, NodeId);
decoder!(port_id, PortId);
decoder!(edge_id, EdgeConnectionId);
decoder!(dns_provider_id, DnsProviderId);
decoder!(certificate_id, CertificateId);
decoder!(relay_certificate_id, RelayCertificateId);
decoder!(server_health_record_id, ServerHealthRecordId);
decoder!(node_health_record_id, NodeHealthRecordId);
