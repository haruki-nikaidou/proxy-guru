use crate::stats::TagStats;
use guru_worker_config::{
    Forwarding, ForwardingTo, Ipv6Resolve, KeepAlive, ListenAs, LoadBalanceStrategy, QuicTuning,
    RelayHost, RelayProtocol, Remote, TcpProxyProtocol,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};

/// Compiled ingest strategy for a listener (certs parsed once at apply time).
pub enum Ingest {
    Raw,
    Tls(openssl::ssl::SslAcceptor),
    RelayTcp,
    RelayTls(openssl::ssl::SslAcceptor),
    RelayQuic,
}

/// Compiled forwarding target tree (LB counters allocated once at apply time).
pub enum Target {
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
    },
    LoadBalance {
        members: Vec<Arc<Target>>,
        strategy: LoadBalanceStrategy,
        next: AtomicUsize,
        rng: AtomicU64,
    },
}

/// A ready-to-serve compiled form of one forwarding role.
pub struct PreparedForwarding {
    pub forwarding: Arc<Forwarding>,
    pub ingest: Ingest,
    pub target: Arc<Target>,
    pub quic_server: Option<quinn::ServerConfig>,
    /// Probing for the sockets this listener accepts; the dialed side carries its
    /// own copy in every leaf of `target`.
    pub keepalive: KeepAlive,
    /// The tag's counters, shared with every earlier and later shape of the same tag.
    pub stats: Arc<TagStats>,
}

impl PreparedForwarding {
    /// `quic` is the worker's default side of a QUIC link; a forwarding that
    /// carries its own replaces it, listener and hop alike.
    pub fn build(
        f: &Forwarding,
        ipv6_resolve: Ipv6Resolve,
        relay_ca: Option<&Path>,
        keepalive: KeepAlive,
        quic: QuicTuning,
        stats: Arc<TagStats>,
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
        let target = compile_target(&f.to, ipv6_resolve, relay_ca, keepalive, quic);
        Ok(PreparedForwarding {
            forwarding: Arc::new(f.clone()),
            ingest,
            target,
            quic_server,
            keepalive,
            stats,
        })
    }
}

fn compile_target(
    to: &ForwardingTo,
    ipv6_resolve: Ipv6Resolve,
    relay_ca: Option<&Path>,
    keepalive: KeepAlive,
    quic: QuicTuning,
) -> Arc<Target> {
    match to {
        ForwardingTo::Exit {
            destination,
            send_proxy_protocol,
        } => Arc::new(Target::Exit {
            destination: destination.clone(),
            ipv6_resolve,
            send_pp: *send_proxy_protocol,
            keepalive,
        }),
        ForwardingTo::Relay {
            protocol,
            destination,
            sni,
            quic: own,
        } => Arc::new(Target::Relay {
            protocol: *protocol,
            destination: destination.clone(),
            ipv6_resolve,
            sni: sni.clone(),
            relay_ca: relay_ca.map(Path::to_path_buf),
            keepalive,
            quic: own.unwrap_or(quic),
        }),
        ForwardingTo::LoadBalance(g) => {
            let members = g
                .members
                .iter()
                .map(|m| compile_target(m, ipv6_resolve, relay_ca, keepalive, quic))
                .collect();
            let seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x9E37_79B9_7F4A_7C15)
                | 1;
            Arc::new(Target::LoadBalance {
                members,
                strategy: g.strategy,
                next: AtomicUsize::new(0),
                rng: AtomicU64::new(seed),
            })
        }
    }
}
