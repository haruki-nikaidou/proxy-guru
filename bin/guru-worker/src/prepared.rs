use crate::liveness::{self, Liveness};
use crate::stats::TagStats;
use guru_worker_config::table::{Policy, Target as TableTarget};
use guru_worker_config::{
    Forwarding, ForwardingTo, Ipv6Resolve, KeepAlive, ListenAs, LoadBalanceStrategy, QuicTuning,
    RelayHost, RelayProtocol, Remote, TcpProxyProtocol, To,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// Compiled ingest strategy for a listener (certs parsed once at apply time).
pub enum Ingest {
    Raw,
    Tls(openssl::ssl::SslAcceptor),
    RelayTcp,
    RelayTls(openssl::ssl::SslAcceptor),
    RelayQuic,
}

/// Where a connection may go, compiled once per apply from either form of a
/// forwarding's `to`.
pub enum Route {
    /// One next hop.
    Hop(Hop),
    /// Spread connections over the members that are alive, by weight.
    Balance(Balance),
    /// The first member that is alive, in order.
    Failover(Vec<Arc<Route>>),
}

pub struct Hop {
    pub target: HopTarget,
    /// What the worker knows about this hop, shared with every forwarding that
    /// dials the same thing.
    pub record: Arc<liveness::Hop>,
    /// What is dialed, for logs.
    pub label: String,
}

pub enum HopTarget {
    Exit {
        destination: Remote,
        ipv6_resolve: Ipv6Resolve,
        send_pp: Option<TcpProxyProtocol>,
        keepalive: KeepAlive,
    },
    Relay {
        protocol: RelayProtocol,
        destination: Remote,
        ipv6_resolve: Ipv6Resolve,
        sni: Option<String>,
        /// CA the relay peer is verified against; `None` means the system roots.
        relay_ca: Option<PathBuf>,
        keepalive: KeepAlive,
        /// This side of the link when `protocol` is QUIC; ignored otherwise.
        quic: QuicTuning,
        /// Ask the relay to confirm that its own next hop connected.
        confirm: bool,
    },
}

pub struct Balance {
    /// Each member and its weight (at least 1).
    pub members: Vec<(Arc<Route>, u32)>,
    pub pick: Pick,
    /// Mixed into the sticky hash, so balances nested in one another do not all
    /// choose alike. Derived from the tag and the group, so it survives reloads.
    pub salt: u64,
    /// Smooth weighted round robin's running weights, one per member.
    pub current: parking_lot::Mutex<Vec<i64>>,
}

pub enum Pick {
    /// Smooth weighted round robin.
    RoundRobin,
    /// Weighted random; the state of the generator.
    Random(AtomicU64),
    /// The same client address keeps the same member while it is alive.
    Sticky,
}

/// A ready-to-serve compiled form of one forwarding role.
pub struct PreparedForwarding {
    pub forwarding: Arc<Forwarding>,
    pub ingest: Ingest,
    pub route: Arc<Route>,
    pub quic_server: Option<quinn::ServerConfig>,
    /// Probing for the sockets this listener accepts; the dialed side carries its
    /// own copy in every hop of `route`.
    pub keepalive: KeepAlive,
    /// The tag's counters, shared with every earlier and later shape of the same tag.
    pub stats: Arc<TagStats>,
}

impl PreparedForwarding {
    /// `quic` is the worker's default side of a QUIC link; a forwarding that
    /// carries its own replaces it, listener and hop alike.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        f: &Forwarding,
        ipv6_resolve: Ipv6Resolve,
        relay_ca: Option<&Path>,
        keepalive: KeepAlive,
        quic: QuicTuning,
        stats: Arc<TagStats>,
        liveness: &Liveness,
    ) -> Result<PreparedForwarding, crate::BoxError> {
        let ingest = match &f.listen_as {
            ListenAs::Raw => Ingest::Raw,
            ListenAs::Tls(c) => Ingest::Tls(crate::tls::server_acceptor(c)?),
            ListenAs::Relay(RelayHost::Tcp) => Ingest::RelayTcp,
            ListenAs::Relay(RelayHost::TlsOverTcp(c)) => {
                Ingest::RelayTls(crate::tls::server_acceptor(c)?)
            }
            ListenAs::Relay(RelayHost::Quic(_)) => Ingest::RelayQuic,
        };
        let quic_server = match &f.listen_as {
            ListenAs::Relay(RelayHost::Quic(c)) => Some(crate::tls::quic_server_config(
                c,
                &keepalive,
                &f.quic.unwrap_or(quic),
            )?),
            _ => None,
        };
        let compiler = Compiler {
            ipv6_resolve,
            relay_ca,
            keepalive,
            quic,
            liveness,
            tag: &f.tag,
        };
        let route = match &f.to {
            To::Tree(tree) => compiler.tree(tree, "t"),
            To::Route(to) => compiler.table(f, to, &mut HashMap::new(), 0)?,
        };
        Ok(PreparedForwarding {
            forwarding: Arc::new(f.clone()),
            ingest,
            route,
            quic_server,
            keepalive,
            stats,
        })
    }
}

/// How deep a route table may nest before compiling it is refused; config
/// validation already rejects cycles, this only bounds the recursion.
const MAX_TABLE_DEPTH: usize = 64;

struct Compiler<'a> {
    ipv6_resolve: Ipv6Resolve,
    relay_ca: Option<&'a Path>,
    keepalive: KeepAlive,
    quic: QuicTuning,
    liveness: &'a Liveness,
    tag: &'a str,
}

impl Compiler<'_> {
    /// The inline tree: a `round_robin` group balances evenly, `random` picks at
    /// random, `ip_hash` keeps clients by address, and `fallback` fails over in
    /// order. All of them pass over dead members and try another on failure.
    fn tree(&self, to: &ForwardingTo, path: &str) -> Arc<Route> {
        match to {
            ForwardingTo::Exit {
                destination,
                send_proxy_protocol,
            } => self.exit(destination, *send_proxy_protocol),
            ForwardingTo::Relay {
                protocol,
                destination,
                sni,
                quic,
            } => self.relay(*protocol, destination, sni.clone(), *quic, false),
            ForwardingTo::LoadBalance(group) => {
                let members: Vec<Arc<Route>> = group
                    .members
                    .iter()
                    .enumerate()
                    .map(|(i, member)| self.tree(member, &format!("{path}.{i}")))
                    .collect();
                let pick = match group.strategy {
                    LoadBalanceStrategy::Fallback => return Arc::new(Route::Failover(members)),
                    LoadBalanceStrategy::RoundRobin => Pick::RoundRobin,
                    LoadBalanceStrategy::Random => Pick::Random(AtomicU64::new(seed())),
                    LoadBalanceStrategy::IpHash => Pick::Sticky,
                };
                self.balance(members.into_iter().map(|m| (m, 1)).collect(), pick, path)
            }
        }
    }

    /// The route table, from the id `id`. A group several others name is
    /// compiled once and shared.
    fn table<'f>(
        &self,
        f: &'f Forwarding,
        id: &'f str,
        built: &mut HashMap<&'f str, Arc<Route>>,
        depth: usize,
    ) -> Result<Arc<Route>, crate::BoxError> {
        if let Some(route) = built.get(id) {
            return Ok(route.clone());
        }
        if depth > MAX_TABLE_DEPTH {
            return Err(format!("route table nests deeper than {MAX_TABLE_DEPTH} at {id}").into());
        }
        let below = depth.saturating_add(1);
        let route = if let Some(upstream) = f.upstreams.iter().find(|u| u.id == id) {
            match &upstream.target {
                TableTarget::Exit(exit) => self.exit(&exit.destination, exit.send_proxy_protocol),
                TableTarget::Relay(relay) => self.relay(
                    relay.protocol,
                    &relay.destination,
                    relay.sni.clone(),
                    relay.quic,
                    relay.confirm,
                ),
            }
        } else if let Some(group) = f.groups.iter().find(|g| g.id == id) {
            match &group.policy {
                Policy::Balance { members, sticky } => {
                    let mut compiled = Vec::with_capacity(members.len());
                    for member in members {
                        compiled.push((self.table(f, &member.to, built, below)?, member.weight));
                    }
                    let pick = if sticky.is_some() {
                        Pick::Sticky
                    } else {
                        Pick::RoundRobin
                    };
                    self.balance(compiled, pick, id)
                }
                Policy::Failover { members } => {
                    let mut compiled = Vec::with_capacity(members.len());
                    for member in members {
                        compiled.push(self.table(f, member, built, below)?);
                    }
                    Arc::new(Route::Failover(compiled))
                }
            }
        } else {
            return Err(format!("route id {id} is not defined").into());
        };
        built.insert(id, route.clone());
        Ok(route)
    }

    fn exit(&self, destination: &Remote, send_pp: Option<TcpProxyProtocol>) -> Arc<Route> {
        let dialed = remote_text(destination);
        Arc::new(Route::Hop(Hop {
            record: self.liveness.hop(format!("exit|{dialed}")),
            label: format!("exit {dialed}"),
            target: HopTarget::Exit {
                destination: destination.clone(),
                ipv6_resolve: self.ipv6_resolve,
                send_pp,
                keepalive: self.keepalive,
            },
        }))
    }

    fn relay(
        &self,
        protocol: RelayProtocol,
        destination: &Remote,
        sni: Option<String>,
        own_quic: Option<QuicTuning>,
        confirm: bool,
    ) -> Arc<Route> {
        let dialed = remote_text(destination);
        let name = sni.as_deref().unwrap_or("");
        Arc::new(Route::Hop(Hop {
            record: self
                .liveness
                .hop(format!("relay|{protocol:?}|{dialed}|{name}")),
            label: format!("relay {protocol:?} {dialed}"),
            target: HopTarget::Relay {
                protocol,
                destination: destination.clone(),
                ipv6_resolve: self.ipv6_resolve,
                sni,
                relay_ca: self.relay_ca.map(Path::to_path_buf),
                keepalive: self.keepalive,
                quic: own_quic.unwrap_or(self.quic),
                confirm,
            },
        }))
    }

    fn balance(&self, members: Vec<(Arc<Route>, u32)>, pick: Pick, group: &str) -> Arc<Route> {
        let members: Vec<(Arc<Route>, u32)> = members
            .into_iter()
            .map(|(route, weight)| (route, weight.max(1)))
            .collect();
        Arc::new(Route::Balance(Balance {
            current: parking_lot::Mutex::new(vec![0; members.len()]),
            members,
            pick,
            salt: fnv1a(&[self.tag.as_bytes(), b"/", group.as_bytes()]),
        }))
    }
}

/// `host:port` as the config spells it.
pub fn remote_text(remote: &Remote) -> String {
    match remote {
        Remote::Domain(host, port) => format!("{host}:{port}"),
        Remote::Address(address) => address.to_string(),
    }
}

/// FNV-1a over the parts, stable across processes and versions.
pub fn fnv1a(parts: &[&[u8]]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in parts.iter().flat_map(|part| part.iter()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9_7F4A_7C15)
        | 1
}
