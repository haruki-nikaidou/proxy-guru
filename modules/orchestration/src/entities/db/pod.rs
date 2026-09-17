//! Pods: the vertices of the forwarding graph.
//!
//! A pod is one listener on one server. Its ingress says how traffic arrives
//! (clients directly, or other pods relaying to it in one protocol) and its
//! route how that traffic leaves, over the pod's out-edges
//! ([`crate::entities::db::edge`]).

use crate::entities::db::canvas::CanvasId;
use crate::entities::db::dns::DnsProviderId;
use crate::entities::db::server::ServerId;
use crate::entities::db::tree;
use base::db::{Db, Error};
use db_types::{table_record, text_enum};
use guru_topology::Route;
use kanau::processor::Processor;
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use sqlx::types::Json;

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

/// One `orchestration_pod` row as the macros read it: one field per selected
/// column, validated into a [`PodEntity`] by the conversion below.
pub(crate) struct PodRow {
    pub(crate) id: PodId,
    pub(crate) canvas: CanvasId,
    pub(crate) server: ServerId,
    pub(crate) name: String,
    pub(crate) comment: String,
    pub(crate) port: i32,
    pub(crate) bind_ip: Option<String>,
    pub(crate) advertise_ip: Option<String>,
    pub(crate) ingress: IngressKind,
    pub(crate) receive_proxy_protocol: Option<ProxyProtocolVersion>,
    pub(crate) tls_sni: Option<String>,
    pub(crate) tls_dns_provider: Option<DnsProviderId>,
    pub(crate) tls_domain_id: Option<String>,
    pub(crate) tls_acme_directory: Option<String>,
    pub(crate) route: Option<Json<Route>>,
}

impl TryFrom<PodRow> for PodEntity {
    type Error = sqlx::Error;

    fn try_from(row: PodRow) -> Result<Self, Self::Error> {
        let id = row.id;
        let port = row.port;
        let port = u16::try_from(port)
            .map_err(|_| corrupt("port", format!("pod {id}: port {port} out of range")))?;
        let receive_proxy_protocol = row.receive_proxy_protocol;
        let tls = match (
            row.tls_sni,
            row.tls_dns_provider,
            row.tls_domain_id,
            row.tls_acme_directory,
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
        let ingress = match (row.ingress, tls) {
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
        Ok(PodEntity {
            id,
            canvas: row.canvas,
            server: row.server,
            name: row.name,
            comment: row.comment,
            port,
            bind_ip: row.bind_ip,
            advertise_ip: row.advertise_ip,
            ingress,
            route: row.route.map(|json| json.0),
        })
    }
}

/// Inserts a pod row as it is.
pub(crate) async fn insert_pod(conn: &mut PgConnection, pod: &PodEntity) -> Result<(), Error> {
    let tls = pod.ingress.tls();
    sqlx::query!(
        "INSERT INTO orchestration_pod
             (id, canvas, server, name, comment, port, bind_ip, advertise_ip, ingress,
              receive_proxy_protocol, tls_sni, tls_dns_provider, tls_domain_id,
              tls_acme_directory, route)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
        pod.id as _,
        pod.canvas as _,
        pod.server as _,
        pod.name,
        pod.comment,
        i32::from(pod.port),
        pod.bind_ip,
        pod.advertise_ip,
        pod.ingress.kind() as _,
        pod.ingress.receive_proxy_protocol() as _,
        tls.map(|t| t.sni.as_str()),
        tls.map(|t| &t.dns_provider) as _,
        tls.map(|t| t.domain_id.as_str()),
        tls.map(|t| t.acme_directory.as_str()),
        pod.route.as_ref().map(Json) as _
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Rewrites every column of an existing pod but its id and canvas.
pub(crate) async fn update_pod(conn: &mut PgConnection, pod: &PodEntity) -> Result<(), Error> {
    let tls = pod.ingress.tls();
    sqlx::query!(
        "UPDATE orchestration_pod
         SET server = $2, name = $3, comment = $4, port = $5, bind_ip = $6, advertise_ip = $7,
             ingress = $8, receive_proxy_protocol = $9, tls_sni = $10, tls_dns_provider = $11,
             tls_domain_id = $12, tls_acme_directory = $13, route = $14
         WHERE id = $1",
        pod.id as _,
        pod.server as _,
        pod.name,
        pod.comment,
        i32::from(pod.port),
        pod.bind_ip,
        pod.advertise_ip,
        pod.ingress.kind() as _,
        pod.ingress.receive_proxy_protocol() as _,
        tls.map(|t| t.sni.as_str()),
        tls.map(|t| &t.dns_provider) as _,
        tls.map(|t| t.domain_id.as_str()),
        tls.map(|t| t.acme_directory.as_str()),
        pod.route.as_ref().map(Json) as _
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// The distinct canvases the given pods are drawn on; the relay leaf rotation
/// cron re-derives these after re-issuing.
#[derive(Debug)]
pub struct ListCanvasesOfPods {
    pub pods: Vec<PodId>,
}

impl Processor<ListCanvasesOfPods> for Db {
    type Output = Vec<CanvasId>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCanvasesOfPods", skip_all, err)]
    async fn process(&self, input: ListCanvasesOfPods) -> Result<Self::Output, Self::Error> {
        if input.pods.is_empty() {
            return Ok(Vec::new());
        }
        Ok(sqlx::query_scalar!(
            r#"SELECT DISTINCT canvas AS "canvas: CanvasId" FROM orchestration_pod
               WHERE id = ANY($1) ORDER BY canvas"#,
            &input.pods as _
        )
        .fetch_all(self.db())
        .await?)
    }
}

/// One pod by id; `None` when there is no such row.
#[derive(Debug)]
pub struct FindPodById {
    pub id: PodId,
}

impl Processor<FindPodById> for Db {
    type Output = Option<PodEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindPodById", skip_all, err)]
    async fn process(&self, input: FindPodById) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_file_as!(PodRow, "sql/find_pod_by_id.sql", input.id as _)
                .fetch_optional(self.db())
                .await?
                .map(PodEntity::try_from)
                .transpose()?,
        )
    }
}

/// The root canvases of every tree holding a TLS or QUIC relay pod: the trees
/// whose derivation depends on the internal CA. `InitInternalCa` touches them
/// so pods reported invalid for lack of a CA get their first leaves.
#[derive(Debug)]
pub struct ListCanvasesWithRelayTls;

impl Processor<ListCanvasesWithRelayTls> for Db {
    type Output = Vec<CanvasId>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCanvasesWithRelayTls", skip_all, err)]
    async fn process(&self, _: ListCanvasesWithRelayTls) -> Result<Self::Output, Self::Error> {
        let mut conn = self.db().acquire().await?;
        let canvases: Vec<CanvasId> = sqlx::query_scalar!(
            r#"SELECT DISTINCT canvas AS "canvas: CanvasId" FROM orchestration_pod
               WHERE ingress IN ('relay_tls', 'relay_quic')"#
        )
        .fetch_all(&mut *conn)
        .await?;
        let mut roots = Vec::with_capacity(canvases.len());
        for canvas in &canvases {
            roots.push(tree::root_of(&mut conn, canvas).await?);
        }
        roots.sort();
        roots.dedup();
        Ok(roots)
    }
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
