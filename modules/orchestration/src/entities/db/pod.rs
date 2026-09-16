//! Pods: the vertices of the forwarding graph.
//!
//! A pod is one listener on one server. Its ingress says how traffic arrives
//! (clients directly, or other pods relaying to it in one protocol) and its
//! route how that traffic leaves, over the pod's out-edges
//! ([`crate::entities::db::edge`]).

use crate::entities::db::canvas::CanvasId;
use crate::entities::db::dns::DnsProviderId;
use crate::entities::db::server::ServerId;
use base::db::Error;
use db_types::{table_record, text_enum};
use guru_topology::Route;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use sqlx::{FromRow, PgConnection, Row};

table_record!(PodId, "orchestration_pod");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodEntity {
    pub id: PodId,
    /// The canvas the pod is drawn on; its server may belong to an ancestor.
    pub canvas: CanvasId,
    pub server: ServerId,
    pub name: String,
    pub comment: String,
    pub port: u16,
    /// An IP literal; `None` listens on every address.
    pub bind_ip: Option<String>,
    /// An IP literal dialers use instead of the server's address.
    pub advertise_ip: Option<String>,
    pub ingress: PodIngress,
    /// The route over the pod's out-edges; `None` when it has none.
    pub route: Option<Route>,
}

impl PodEntity {
    /// The socket this pod binds, `[::]` standing for "all addresses".
    pub fn listen_display(&self) -> String {
        match self.bind_ip.as_deref() {
            None => format!("[::]:{}", self.port),
            Some(ip) if ip.contains(':') => format!("[{ip}]:{}", self.port),
            Some(ip) => format!("{ip}:{}", self.port),
        }
    }
}

/// How traffic arrives at a pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodIngress {
    /// Clients connect and their bytes are forwarded as they are.
    ClientRaw {
        receive_proxy_protocol: Option<ProxyProtocolVersion>,
    },
    /// Clients connect with TLS, terminated with the ACME certificate `tls` asks for.
    ClientTls {
        receive_proxy_protocol: Option<ProxyProtocolVersion>,
        tls: TlsConfig,
    },
    /// Other pods relay to it over plain TCP.
    RelayTcp,
    /// Other pods relay to it over TLS.
    RelayTls,
    /// Other pods relay to it over QUIC.
    RelayQuic,
}

impl PodIngress {
    pub fn kind(&self) -> IngressKind {
        match self {
            PodIngress::ClientRaw { .. } => IngressKind::ClientRaw,
            PodIngress::ClientTls { .. } => IngressKind::ClientTls,
            PodIngress::RelayTcp => IngressKind::RelayTcp,
            PodIngress::RelayTls => IngressKind::RelayTls,
            PodIngress::RelayQuic => IngressKind::RelayQuic,
        }
    }

    pub fn receive_proxy_protocol(&self) -> Option<ProxyProtocolVersion> {
        match self {
            PodIngress::ClientRaw {
                receive_proxy_protocol,
            }
            | PodIngress::ClientTls {
                receive_proxy_protocol,
                ..
            } => *receive_proxy_protocol,
            PodIngress::RelayTcp | PodIngress::RelayTls | PodIngress::RelayQuic => None,
        }
    }

    pub fn tls(&self) -> Option<&TlsConfig> {
        match self {
            PodIngress::ClientTls { tls, .. } => Some(tls),
            _ => None,
        }
    }

    /// Whether other pods reach this one by relay, and so may hold edges into it.
    pub fn is_relay(&self) -> bool {
        matches!(
            self,
            PodIngress::RelayTcp | PodIngress::RelayTls | PodIngress::RelayQuic
        )
    }

    /// Whether the listener needs a relay leaf certificate.
    pub fn needs_relay_certificate(&self) -> bool {
        matches!(self, PodIngress::RelayTls | PodIngress::RelayQuic)
    }
}

/// The stored spelling of a [`PodIngress`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngressKind {
    ClientRaw,
    ClientTls,
    RelayTcp,
    RelayTls,
    RelayQuic,
}
text_enum!(IngressKind {
    ClientRaw => "client_raw",
    ClientTls => "client_tls",
    RelayTcp => "relay_tcp",
    RelayTls => "relay_tls",
    RelayQuic => "relay_quic",
});

/// The ACME certificate a TLS client listener terminates with; the master
/// acquires it and ships it to the pod's worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsConfig {
    /// The SNI of the TLS certificate.
    pub sni: String,

    /// Which DNS provider answers the ACME DNS challenge.
    pub dns_provider: DnsProviderId,

    /// The identifier of the domain
    /// - Cloudflare: zone ID
    /// - vercel: domain SLD
    pub domain_id: String,

    /// The URL of the ACME directory; empty means the configured default.
    ///
    /// eg. <https://acme-staging-v02.api.letsencrypt.org/directory>
    pub acme_directory: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyProtocolVersion {
    V1,
    V2,
}
text_enum!(ProxyProtocolVersion {
    V1 => "v1",
    V2 => "v2",
});

impl From<ProxyProtocolVersion> for guru_worker_config::TcpProxyProtocol {
    fn from(value: ProxyProtocolVersion) -> Self {
        match value {
            ProxyProtocolVersion::V1 => guru_worker_config::TcpProxyProtocol::V1,
            ProxyProtocolVersion::V2 => guru_worker_config::TcpProxyProtocol::V2,
        }
    }
}

fn corrupt(column: &str, message: String) -> sqlx::Error {
    sqlx::Error::ColumnDecode {
        index: column.to_string(),
        source: message.into(),
    }
}

impl FromRow<'_, PgRow> for PodEntity {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let id: PodId = row.try_get("id")?;
        let port: i32 = row.try_get("port")?;
        let port = u16::try_from(port)
            .map_err(|_| corrupt("port", format!("pod {id}: port {port} out of range")))?;
        let receive_proxy_protocol: Option<ProxyProtocolVersion> =
            row.try_get("receive_proxy_protocol")?;
        let tls = match (
            row.try_get::<Option<String>, _>("tls_sni")?,
            row.try_get::<Option<DnsProviderId>, _>("tls_dns_provider")?,
            row.try_get::<Option<String>, _>("tls_domain_id")?,
            row.try_get::<Option<String>, _>("tls_acme_directory")?,
        ) {
            (Some(sni), Some(dns_provider), Some(domain_id), Some(acme_directory)) => {
                Some(TlsConfig {
                    sni,
                    dns_provider,
                    domain_id,
                    acme_directory,
                })
            }
            _ => None,
        };
        let ingress = match (row.try_get::<IngressKind, _>("ingress")?, tls) {
            (IngressKind::ClientRaw, None) => PodIngress::ClientRaw {
                receive_proxy_protocol,
            },
            (IngressKind::ClientTls, Some(tls)) => PodIngress::ClientTls {
                receive_proxy_protocol,
                tls,
            },
            (IngressKind::RelayTcp, None) => PodIngress::RelayTcp,
            (IngressKind::RelayTls, None) => PodIngress::RelayTls,
            (IngressKind::RelayQuic, None) => PodIngress::RelayQuic,
            (kind, _) => {
                return Err(corrupt(
                    "ingress",
                    format!("pod {id}: TLS columns do not match ingress {kind}"),
                ));
            }
        };
        let route: Option<Json<Route>> = row.try_get("route")?;
        Ok(PodEntity {
            id,
            canvas: row.try_get("canvas")?,
            server: row.try_get("server")?,
            name: row.try_get("name")?,
            comment: row.try_get("comment")?,
            port,
            bind_ip: row.try_get("bind_ip")?,
            advertise_ip: row.try_get("advertise_ip")?,
            ingress,
            route: route.map(|json| json.0),
        })
    }
}

/// Inserts a pod row as it is.
pub(crate) async fn insert_pod(conn: &mut PgConnection, pod: &PodEntity) -> Result<(), Error> {
    let tls = pod.ingress.tls();
    sqlx::query(
        "INSERT INTO orchestration_pod
             (id, canvas, server, name, comment, port, bind_ip, advertise_ip, ingress,
              receive_proxy_protocol, tls_sni, tls_dns_provider, tls_domain_id,
              tls_acme_directory, route)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
    )
    .bind(&pod.id)
    .bind(&pod.canvas)
    .bind(&pod.server)
    .bind(&pod.name)
    .bind(&pod.comment)
    .bind(i32::from(pod.port))
    .bind(&pod.bind_ip)
    .bind(&pod.advertise_ip)
    .bind(pod.ingress.kind())
    .bind(pod.ingress.receive_proxy_protocol())
    .bind(tls.map(|t| t.sni.as_str()))
    .bind(tls.map(|t| &t.dns_provider))
    .bind(tls.map(|t| t.domain_id.as_str()))
    .bind(tls.map(|t| t.acme_directory.as_str()))
    .bind(pod.route.as_ref().map(Json))
    .execute(conn)
    .await?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn stored_spellings_match_serde() {
        db_types::assert_text_enum_matches_serde!(IngressKind);
        db_types::assert_text_enum_matches_serde!(ProxyProtocolVersion);
    }
}
