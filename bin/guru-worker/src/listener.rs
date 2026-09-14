use crate::prepared::PreparedForwarding;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Accept loop for a TCP-family listener. Reads the latest hot-swapped config per accept.
/// Cancelling the token stops accepting; already-spawned connection tasks keep running.
pub async fn run_tcp(
    listener: tokio::net::TcpListener,
    cfg_rx: watch::Receiver<Arc<PreparedForwarding>>,
    token: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            accept = listener.accept() => match accept {
                Ok((stream, peer)) => {
                    let cfg = cfg_rx.borrow().clone();
                    // A dual-stack `[::]` listener reports IPv4 peers as
                    // `::ffff:a.b.c.d`; every consumer (PROXY headers, ip_hash,
                    // logs) wants the plain IPv4 address.
                    let peer = canonical(peer);
                    tokio::spawn(crate::pipe::handle_tcp_connection(stream, peer, cfg));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "accept error");
                }
            }
        }
    }
    // listener dropped here -> socket closed; already-spawned connection tasks keep running.
}

/// Accept loop for a QUIC listener. Pushes new server configs on reload and drains
/// existing connections on cancellation.
pub async fn run_quic(
    endpoint: quinn::Endpoint,
    mut cfg_rx: watch::Receiver<Arc<PreparedForwarding>>,
    token: CancellationToken,
) {
    let mut current = cfg_rx.borrow().clone();
    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            changed = cfg_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                current = cfg_rx.borrow_and_update().clone();
                endpoint.set_server_config(current.quic_server.clone());
            }
            incoming = endpoint.accept() => match incoming {
                Some(inc) => {
                    conns.spawn(handle_quic_connection(inc, current.clone()));
                }
                None => break, // endpoint closed
            }
        }
    }
    while conns.join_next().await.is_some() {}
    endpoint.wait_idle().await;
}

async fn handle_quic_connection(incoming: quinn::Incoming, cfg: Arc<PreparedForwarding>) {
    let conn = match incoming.await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "quic handshake failed");
            return;
        }
    };
    let remote = canonical(conn.remote_address());
    while let Ok((send, recv)) = conn.accept_bi().await {
        let joined = Box::new(tokio::io::join(recv, send));
        tokio::spawn(crate::pipe::handle_relay_quic_stream_logged(
            joined,
            remote,
            cfg.clone(),
        ));
    }
}

/// `::ffff:a.b.c.d` becomes `a.b.c.d`; anything else is returned unchanged.
pub fn canonical(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(addr.ip().to_canonical(), addr.port())
}

/// Whether an address is the IPv6 wildcard `[::]`, which we bind dual-stack.
fn is_v6_wildcard(addr: SocketAddr) -> bool {
    matches!(addr.ip(), std::net::IpAddr::V6(ip) if ip.is_unspecified())
}

/// Binds a `socket2` socket for `addr`. A `[::]` bind is made dual-stack
/// (`IPV6_V6ONLY` off) so one listener serves both families; the caller falls
/// back to `0.0.0.0` when the host has no IPv6. `SO_REUSEADDR` keeps a restart
/// from tripping over `TIME_WAIT`.
fn bind_socket(addr: SocketAddr, ty: Type, protocol: Protocol) -> std::io::Result<Socket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, ty, Some(protocol))?;
    socket.set_reuse_address(true)?;
    if is_v6_wildcard(addr) {
        socket.set_only_v6(false)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    Ok(socket)
}

/// Binds a TCP listener, dual-stack for a `[::]` address, falling back to the
/// IPv4 wildcard on a host with no IPv6.
pub fn bind_tcp(addr: SocketAddr) -> Result<tokio::net::TcpListener, crate::BoxError> {
    let addr = with_v6_fallback(addr, |addr| {
        let socket = bind_socket(addr, Type::STREAM, Protocol::TCP)?;
        socket.listen(1024)?;
        Ok(socket)
    })?;
    Ok(tokio::net::TcpListener::from_std(addr.into())?)
}

/// Binds a QUIC (UDP) endpoint serving the given config, dual-stack for `[::]`,
/// falling back to the IPv4 wildcard on a host with no IPv6.
pub fn bind_quic(
    addr: SocketAddr,
    server_cfg: quinn::ServerConfig,
) -> Result<quinn::Endpoint, crate::BoxError> {
    let socket = with_v6_fallback(addr, |addr| bind_socket(addr, Type::DGRAM, Protocol::UDP))?;
    Ok(quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        Some(server_cfg),
        socket.into(),
        std::sync::Arc::new(quinn::TokioRuntime),
    )?)
}

/// Runs `bind` for `addr`; if a `[::]` bind fails (a host without IPv6), retries
/// once on `0.0.0.0` and warns. The dedup key stays the configured address.
fn with_v6_fallback(
    addr: SocketAddr,
    bind: impl Fn(SocketAddr) -> std::io::Result<Socket>,
) -> Result<Socket, crate::BoxError> {
    match bind(addr) {
        Ok(socket) => Ok(socket),
        Err(error) if is_v6_wildcard(addr) => {
            let fallback = SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), addr.port());
            tracing::warn!(%addr, %error, "binding [::] failed; falling back to 0.0.0.0 (no IPv6?)");
            Ok(bind(fallback)?)
        }
        Err(error) => Err(error.into()),
    }
}
