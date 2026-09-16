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
                    if let Err(error) = crate::keepalive::apply_tcp(&stream, &cfg.keepalive) {
                        tracing::warn!(%peer, %error, "keepalive not set on accepted socket");
                    }
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
    // Connections outlive the accept loop only for as long as they carry a
    // stream: the dialer pools them and pings them, so a removed listener has
    // to close them itself or they stay up for good.
    let drain = CancellationToken::new();
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
                    conns.spawn(handle_quic_connection(inc, current.clone(), drain.clone()));
                }
                None => break, // endpoint closed
            }
        }
    }
    drain.cancel();
    while conns.join_next().await.is_some() {}
    endpoint.wait_idle().await;
}

/// Serves one accepted connection: a stream per proxied connection until `drain`
/// says the listener is going away, after which the connection is closed as
/// soon as its last stream ends.
async fn handle_quic_connection(
    incoming: quinn::Incoming,
    cfg: Arc<PreparedForwarding>,
    drain: CancellationToken,
) {
    let conn = match incoming.await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "quic handshake failed");
            return;
        }
    };
    let remote = canonical(conn.remote_address());
    // Streams are spawned on their own, as TCP connections are, so that a
    // listener forced down mid-transfer does not cut them; only their count is
    // kept, for the close below.
    let open = Arc::new(OpenStreams::default());
    loop {
        tokio::select! {
            _ = drain.cancelled() => break,
            accepted = conn.accept_bi() => match accepted {
                Ok((send, recv)) => {
                    let joined = Box::new(tokio::io::join(recv, send));
                    let guard = open.clone().enter();
                    let cfg = cfg.clone();
                    tokio::spawn(async move {
                        crate::pipe::handle_relay_quic_stream_logged(joined, remote, cfg).await;
                        drop(guard);
                    });
                }
                Err(_) => return, // the peer closed it, or it timed out
            }
        }
    }
    open.drained().await;
    conn.close(quinn::VarInt::from_u32(0), b"listener removed");
}

/// How many streams a connection is serving, and a way to wait for none.
#[derive(Default)]
struct OpenStreams {
    count: std::sync::atomic::AtomicUsize,
    changed: tokio::sync::Notify,
}

struct OpenStream(Arc<OpenStreams>);

impl OpenStreams {
    fn enter(self: Arc<Self>) -> OpenStream {
        self.count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        OpenStream(self)
    }

    async fn drained(&self) {
        loop {
            // Register before checking, so a drop between the two is not missed.
            let notified = self.changed.notified();
            if self.count.load(std::sync::atomic::Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for OpenStream {
    fn drop(&mut self) {
        self.0
            .count
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.0.changed.notify_waiters();
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
