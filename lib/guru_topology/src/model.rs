//! The graph a caller hands in.
//!
//! Field for field it follows the rows the control plane stores it in, and a
//! [`Route`] serializes to the document a pod row keeps:
//!
//! ```json
//! { "failover": [
//!     { "balance": [ { "weight": 2, "to": { "edge": "e1" } }, { "to": { "edge": "e2" } } ],
//!       "sticky": "client_ip" },
//!     { "edge": "e9" }
//! ] }
//! ```

use guru_worker_config::{QuicCongestion, QuicTuning, TcpProxyProtocol, TlsHostConfig};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;

pub use guru_worker_config::table::Sticky;

macro_rules! id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(id: impl Into<String>) -> Self {
                Self(id.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id!(
    /// A server's id. Opaque: the crate only compares and prints it.
    ServerId
);
id!(
    /// A pod's id. It is also the pod's worker tag, so it is unique per server
    /// whatever the pod is called.
    PodId
);
id!(
    /// An exit's id.
    ExitId
);
id!(
    /// An edge's id. Route leaves name edges by it.
    EdgeId
);

/// Everything the topology is made of, for one canvas tree.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Graph {
    pub servers: Vec<Server>,
    pub pods: Vec<Pod>,
    pub exits: Vec<Exit>,
    pub edges: Vec<Edge>,
}

/// A machine running a worker: the pods on it share its address, its QUIC
/// rates and what its worker understands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Server {
    pub id: ServerId,
    pub name: String,
    /// The address other servers dial this one on, as the control plane chose
    /// it (pinned, reported or observed). `None` while none is known. An edge
    /// of [`IpFamily::Auto`] dials it.
    pub dial_address: Option<IpAddr>,
    /// The server's IPv4 address, chosen the same way among its IPv4
    /// addresses: what an edge of [`IpFamily::V4`] dials.
    #[serde(default)]
    pub dial_v4: Option<Ipv4Addr>,
    /// The same for IPv6 and [`IpFamily::V6`].
    #[serde(default)]
    pub dial_v6: Option<Ipv6Addr>,
    #[serde(default)]
    pub quic: ServerQuic,
    #[serde(default)]
    pub capabilities: Capabilities,
}

impl Server {
    /// The address an edge of `family` dials this server on, if it has one.
    pub fn address_for(&self, family: IpFamily) -> Option<IpAddr> {
        match family {
            IpFamily::Auto => self.dial_address,
            IpFamily::V4 => self.dial_v4.map(IpAddr::V4),
            IpFamily::V6 => self.dial_v6.map(IpAddr::V6),
        }
    }
}

/// A server's side of every QUIC link it takes part in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerQuic {
    pub congestion: QuicCongestion,
    /// Send rate toward every QUIC peer, Mbit/s. `0` is unknown.
    pub up_mbps: u32,
    /// Receive rate from every QUIC peer, Mbit/s. `0` is unknown.
    pub down_mbps: u32,
    /// Per-stream receive window in bytes; `0` derives it from `down_mbps`.
    pub stream_receive_window: u64,
    /// Whole-connection receive window in bytes; `0` leaves it unlimited.
    pub conn_receive_window: u64,
}

impl ServerQuic {
    /// This server's side of a link with `peer`: it sends at the lower of its
    /// own up rate and the peer's down rate, and sizes its windows for the lower
    /// of its own down rate and the peer's up rate. Without a peer (the
    /// worker-wide default) its own numbers stand.
    pub fn tuning(&self, peer: Option<&ServerQuic>) -> QuicTuning {
        let (send_mbps, receive_mbps) = match peer {
            Some(peer) => (
                lower(self.up_mbps, peer.down_mbps),
                lower(self.down_mbps, peer.up_mbps),
            ),
            None => (self.up_mbps, self.down_mbps),
        };
        QuicTuning {
            congestion: self.congestion,
            send_mbps,
            receive_mbps,
            max_streams: 0,
            stream_receive_window: self.stream_receive_window,
            receive_window: self.conn_receive_window,
            send_window: 0,
        }
    }
}

/// The lower of two rates, where `0` means unknown and loses to any number.
pub(crate) fn lower(a: u32, b: u32) -> u32 {
    match (a, b) {
        (0, rate) | (rate, 0) => rate,
        (a, b) => a.min(b),
    }
}

/// What a server's worker understands beyond the tree form every worker reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Capabilities {
    /// Reads route tables (see [`guru_worker_config::table`]).
    pub route_table: bool,
    /// Confirms a relayed connection once its own next hop answered.
    pub relay_confirm: bool,
}

/// One listener on one server, and where its traffic may go.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pod {
    pub id: PodId,
    pub server: ServerId,
    /// Shown to people; never used to tell pods apart.
    pub name: String,
    pub port: u16,
    /// An IP literal; `None` listens on every address.
    pub bind_ip: Option<String>,
    /// An IP literal dialers use instead of the server's address.
    pub advertise_ip: Option<String>,
    pub ingress: Ingress,
    /// A tree over exactly this pod's out-edges; `None` when it has none.
    pub route: Option<Route>,
}

/// How traffic arrives at a pod. A pod has one listener, so one of these.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Ingress {
    /// Clients connect directly and their bytes are forwarded as they are.
    ClientRaw {
        receive_proxy_protocol: Option<TcpProxyProtocol>,
    },
    /// Clients connect with TLS, terminated with the ACME certificate issued for
    /// `sni` by `acme_directory`.
    ClientTls {
        receive_proxy_protocol: Option<TcpProxyProtocol>,
        sni: String,
        acme_directory: String,
    },
    /// Other pods relay to it over plain TCP.
    RelayTcp,
    /// Other pods relay to it over TLS, verified against the internal CA.
    RelayTls,
    /// Other pods relay to it over QUIC, verified against the internal CA.
    RelayQuic,
}

impl Ingress {
    pub fn is_client(&self) -> bool {
        matches!(self, Ingress::ClientRaw { .. } | Ingress::ClientTls { .. })
    }

    pub fn is_relay(&self) -> bool {
        !self.is_client()
    }

    /// Whether the pod learns the real client address: a relay hop always
    /// carries it in a PROXY v2 header, a client listener only when told to
    /// read one.
    pub fn knows_client_ip(&self) -> bool {
        match self {
            Ingress::ClientRaw {
                receive_proxy_protocol,
            }
            | Ingress::ClientTls {
                receive_proxy_protocol,
                ..
            } => receive_proxy_protocol.is_some(),
            Ingress::RelayTcp | Ingress::RelayTls | Ingress::RelayQuic => true,
        }
    }
}

/// A destination outside the fabric.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exit {
    pub id: ExitId,
    pub name: String,
    /// `host:port`.
    pub destination: String,
    pub send_proxy_protocol: Option<TcpProxyProtocol>,
}

/// A way a pod's traffic may go on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    pub id: EdgeId,
    pub source: PodId,
    pub target: EdgeTarget,
    /// Dial this host (an IP literal or a name) instead of the target pod's own
    /// address. Only meaningful toward a pod.
    pub override_ip: Option<String>,
    /// Dial this port instead of the target pod's.
    pub override_port: Option<u16>,
    /// Which of the target pod's addresses to dial when the edge names none.
    /// Only meaningful toward a pod.
    #[serde(default)]
    pub ip_family: IpFamily,
}

impl Edge {
    /// The host this edge dials `target`, a pod on `server`, at: its override
    /// address, else the address the pod advertises when that is of the edge's
    /// family, else the server's address of the edge's family. `None` when
    /// there is none of those.
    pub fn dial_host(&self, target: &Pod, server: &Server) -> Option<String> {
        if let Some(host) = &self.override_ip {
            return Some(host.clone());
        }
        let advertised = target
            .advertise_ip
            .as_ref()
            .filter(|host| match self.ip_family {
                IpFamily::Auto => true,
                family => host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| family.admits(address)),
            });
        match advertised {
            Some(host) => Some(host.clone()),
            None => server
                .address_for(self.ip_family)
                .map(|address| address.to_string()),
        }
    }
}

/// Which address family an edge dials the pod it leads to over.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum IpFamily {
    /// The server's dial address, whichever family that is: IPv4 when the
    /// server has one.
    #[default]
    Auto,
    /// IPv4 only.
    V4,
    /// IPv6 only.
    V6,
}

impl IpFamily {
    /// Whether an edge of this family may dial `address`.
    pub fn admits(self, address: IpAddr) -> bool {
        match self {
            IpFamily::Auto => true,
            IpFamily::V4 => address.is_ipv4(),
            IpFamily::V6 => address.is_ipv6(),
        }
    }
}

impl std::fmt::Display for IpFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            IpFamily::Auto => "auto",
            IpFamily::V4 => "IPv4",
            IpFamily::V6 => "IPv6",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeTarget {
    Pod(PodId),
    Exit(ExitId),
}

/// How deep a route may nest. Far beyond anything drawn by hand; it exists so
/// an absurd document is reported instead of walked.
pub const MAX_ROUTE_DEPTH: usize = 32;

/// How a pod chooses between its out-edges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawRoute", into = "RawRoute")]
pub enum Route {
    /// Use this edge.
    Edge(EdgeId),
    /// Spread connections over the members that are alive, by weight.
    Balance {
        members: Vec<Weighted>,
        sticky: Option<Sticky>,
    },
    /// Use the first member that is alive, in order.
    Failover(Vec<Route>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Weighted {
    #[serde(default = "one", skip_serializing_if = "is_one")]
    pub weight: u32,
    pub to: Route,
}

fn one() -> u32 {
    1
}

fn is_one(weight: &u32) -> bool {
    *weight == 1
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRoute {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    edge: Option<EdgeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    balance: Option<Vec<Weighted>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sticky: Option<Sticky>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failover: Option<Vec<Route>>,
}

impl TryFrom<RawRoute> for Route {
    type Error = String;

    fn try_from(raw: RawRoute) -> Result<Self, Self::Error> {
        if raw.sticky.is_some() && raw.balance.is_none() {
            return Err("`sticky` only applies to `balance`".to_string());
        }
        match (raw.edge, raw.balance, raw.failover) {
            (Some(edge), None, None) => Ok(Route::Edge(edge)),
            (None, Some(members), None) => Ok(Route::Balance {
                members,
                sticky: raw.sticky,
            }),
            (None, None, Some(members)) => Ok(Route::Failover(members)),
            _ => Err("a route is exactly one of `edge`, `balance` and `failover`".to_string()),
        }
    }
}

impl From<Route> for RawRoute {
    fn from(route: Route) -> Self {
        let mut raw = RawRoute {
            edge: None,
            balance: None,
            sticky: None,
            failover: None,
        };
        match route {
            Route::Edge(edge) => raw.edge = Some(edge),
            Route::Balance { members, sticky } => {
                raw.balance = Some(members);
                raw.sticky = sticky;
            }
            Route::Failover(members) => raw.failover = Some(members),
        }
        raw
    }
}

impl Route {
    /// How many levels the route nests, a lone edge being one. Iterative, so
    /// it can measure a route before anything recursive walks it.
    pub fn depth(&self) -> usize {
        let mut deepest = 0;
        let mut stack = vec![(self, 1usize)];
        while let Some((route, depth)) = stack.pop() {
            deepest = deepest.max(depth);
            let below = depth.saturating_add(1);
            match route {
                Route::Edge(_) => {}
                Route::Balance { members, .. } => {
                    stack.extend(members.iter().map(|m| (&m.to, below)));
                }
                Route::Failover(members) => stack.extend(members.iter().map(|m| (m, below))),
            }
        }
        deepest
    }

    /// Every node of the route, parents before children, siblings in order.
    pub fn nodes(&self) -> Vec<&Route> {
        let mut out = Vec::new();
        let mut stack = vec![self];
        while let Some(route) = stack.pop() {
            out.push(route);
            match route {
                Route::Edge(_) => {}
                Route::Balance { members, .. } => stack.extend(members.iter().rev().map(|m| &m.to)),
                Route::Failover(members) => stack.extend(members.iter().rev()),
            }
        }
        out
    }

    /// The edges the route uses, in order, repeats included.
    pub fn leaves(&self) -> Vec<&EdgeId> {
        self.nodes()
            .into_iter()
            .filter_map(|node| match node {
                Route::Edge(edge) => Some(edge),
                _ => None,
            })
            .collect()
    }
}

/// Which certificates exist, so compiling a pod that needs a missing one can
/// say so instead of emitting a listener that cannot start.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Certificates {
    /// The internal CA relay TLS and QUIC are verified against.
    pub internal_ca: bool,
    /// Issued ACME certificates, by `(sni, acme_directory)`.
    pub acme: BTreeMap<(String, String), CertificateRef>,
    /// Issued relay leaves, by the pod they belong to.
    pub relay: BTreeMap<PodId, CertificateRef>,
    /// Compile as though every certificate were issued, for callers that only
    /// need the shape (a protocol switch check must not be hidden by a pending
    /// certificate).
    pub assume_issued: bool,
}

impl Certificates {
    pub(crate) fn ca_usable(&self) -> bool {
        self.internal_ca || self.assume_issued
    }
}

/// A certificate a forwarding's listener references, at the version it was
/// compiled against.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CertificateRef {
    pub kind: CertificateKind,
    /// The id of the certificate row.
    pub key: String,
    pub version: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificateKind {
    /// Public ACME certificate, delivered as `certs/acme/<key>/{full_chain,key}.pem`.
    Acme,
    /// Relay leaf, delivered as `certs/relay/<key>/{full_chain,key}.pem`.
    Relay,
}

/// The SNI a relay listener presents and its dialers verify.
pub fn relay_sni(pod: &PodId) -> String {
    format!("{pod}.relay.guru.internal")
}

pub(crate) fn acme_host(certificate: &str) -> TlsHostConfig {
    TlsHostConfig {
        key: PathBuf::from(format!("certs/acme/{certificate}/key.pem")),
        full_chain: PathBuf::from(format!("certs/acme/{certificate}/full_chain.pem")),
    }
}

pub(crate) fn relay_host(pod: &PodId) -> TlsHostConfig {
    TlsHostConfig {
        key: PathBuf::from(format!("certs/relay/{pod}/key.pem")),
        full_chain: PathBuf::from(format!("certs/relay/{pod}/full_chain.pem")),
    }
}
