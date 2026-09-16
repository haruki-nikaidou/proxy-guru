//! Wire string → typed row id.
//!
//! Ids cross the gRPC boundary as bare text, so every decode funnels through
//! here instead of being reinvented per handler. The other direction needs no
//! helper: an id is `Display`.

use crate::entities::db::ca::RelayCertificateId;
use crate::entities::db::canvas::CanvasId;
use crate::entities::db::certificate::CertificateId;
use crate::entities::db::dns::DnsProviderId;
use crate::entities::db::edge::EdgeId;
use crate::entities::db::exit::ExitId;
use crate::entities::db::group::GroupId;
use crate::entities::db::health::{PodHealthRecordId, ServerHealthRecordId};
use crate::entities::db::pod::PodId;
use crate::entities::db::server::ServerId;

macro_rules! decoder {
    ($name:ident, $ty:ident) => {
        pub fn $name(key: &str) -> $ty {
            $ty::from_key(key)
        }
    };
}

decoder!(canvas_id, CanvasId);
decoder!(server_id, ServerId);
decoder!(pod_id, PodId);
decoder!(exit_id, ExitId);
decoder!(edge_id, EdgeId);
decoder!(group_id, GroupId);
decoder!(dns_provider_id, DnsProviderId);
decoder!(certificate_id, CertificateId);
decoder!(relay_certificate_id, RelayCertificateId);
decoder!(server_health_record_id, ServerHealthRecordId);
decoder!(pod_health_record_id, PodHealthRecordId);

/// Whether a client-chosen id has the shape every stored id has: 20 characters
/// of `[a-z0-9]`.
pub fn is_record_key(key: &str) -> bool {
    key.len() == 20
        && key
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}
