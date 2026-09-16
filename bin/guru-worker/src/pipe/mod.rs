pub mod entry;
pub mod exit;
pub mod relay;
pub mod route;

use crate::BoxError;
use crate::pipe::relay::RelayStream;
use crate::prepared::PreparedForwarding;
use crate::stats::TagStats;
use guru_worker_config::TcpProxyProtocol;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

/// Any owned, boxable bidirectional stream (TCP, TLS-over-TCP, or a joined QUIC bi-stream).
pub trait AsyncRw: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncRw for T {}

/// Wraps a stream, draining a leftover byte prefix before delegating to the inner stream.
///
/// Needed because the proxy-protocol reader may over-read into the tunneled payload, and
/// because a PROXY header sent before TLS leaves ClientHello bytes buffered.
pub struct Prefixed<S> {
    prefix: std::io::Cursor<Vec<u8>>,
    inner: S,
}

impl<S> Prefixed<S> {
    pub fn new(prefix: Vec<u8>, inner: S) -> Self {
        Self {
            prefix: std::io::Cursor::new(prefix),
            inner,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Prefixed<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let pos = this.prefix.position() as usize;
        let data = this.prefix.get_ref();
        if pos < data.len() {
            let remaining = &data[pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            this.prefix.set_position(pos.saturating_add(n) as u64);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Prefixed<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// The PROXY v2 TLV type a dialer uses to ask a relay to confirm its own next hop
/// (in the range PROXY v2 leaves to applications). The value is a version byte and
/// the dialer's remaining patience in milliseconds, big-endian.
pub const CONFIRM_TLV: u8 = 0xE7;
const CONFIRM_VERSION: u8 = 1;
/// The byte a confirming relay answers with once its next hop connected.
pub const CONFIRM_OK: u8 = 0x00;
/// The byte a confirming relay answers with when none of its next hops did.
pub const CONFIRM_FAILED: u8 = 0x01;
/// What a relay keeps of the dialer's patience for the answer to travel back.
const CONFIRM_MARGIN: Duration = Duration::from_millis(300);
/// The least a relay gives its own attempts, however little the dialer has left.
const CONFIRM_FLOOR: Duration = Duration::from_millis(500);

/// A PROXY protocol header, read.
pub struct ProxyHeader {
    /// The true client address.
    pub source: SocketAddr,
    /// Bytes read past the header: the start of the tunneled payload.
    pub leftover: Vec<u8>,
    /// How long the dialer waits for a confirmation, when it asked for one.
    pub confirm: Option<Duration>,
}

/// Reads a PROXY protocol header (auto-detecting v1/v2), returning the source (client)
/// address and any bytes read past the header (the start of the tunneled payload).
pub async fn read_proxy_header<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> Result<(SocketAddr, Vec<u8>), BoxError> {
    let header = read_header(stream).await?;
    Ok((header.source, header.leftover))
}

/// [`read_proxy_header`] for a relay listener, which also learns whether the dialer
/// asked for a confirmation.
pub async fn read_header<S: AsyncRead + Unpin>(stream: &mut S) -> Result<ProxyHeader, BoxError> {
    use ppp::{HeaderResult, PartialResult};

    let mut buf: Vec<u8> = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err("eof before proxy protocol header".into());
        }
        buf.extend_from_slice(&chunk[..n]);
        let result = HeaderResult::parse(&buf);
        if result.is_incomplete() {
            continue;
        }
        match result {
            HeaderResult::V2(Ok(h)) => {
                let src = v2_source(&h.addresses).ok_or("unsupported proxy v2 address family")?;
                let confirm = h.tlvs().filter_map(Result::ok).find_map(|tlv| {
                    match (tlv.kind, tlv.value.as_ref()) {
                        (CONFIRM_TLV, [CONFIRM_VERSION, a, b, c, d]) => {
                            Some(Duration::from_millis(u64::from(u32::from_be_bytes([
                                *a, *b, *c, *d,
                            ]))))
                        }
                        _ => None,
                    }
                });
                let used = h.len();
                return Ok(ProxyHeader {
                    source: crate::listener::canonical(src),
                    leftover: buf[used..].to_vec(),
                    confirm,
                });
            }
            HeaderResult::V1(Ok(h)) => {
                let src = v1_source(&h.addresses).ok_or("unsupported proxy v1 address")?;
                let used = h.header.len();
                return Ok(ProxyHeader {
                    source: crate::listener::canonical(src),
                    leftover: buf[used..].to_vec(),
                    confirm: None,
                });
            }
            _ => return Err("malformed proxy protocol header".into()),
        }
    }
}

fn v2_source(a: &ppp::v2::Addresses) -> Option<SocketAddr> {
    match a {
        ppp::v2::Addresses::IPv4(x) => Some(SocketAddr::new(
            IpAddr::from(x.source_address),
            x.source_port,
        )),
        ppp::v2::Addresses::IPv6(x) => Some(SocketAddr::new(
            IpAddr::from(x.source_address),
            x.source_port,
        )),
        _ => None,
    }
}

fn v1_source(a: &ppp::v1::Addresses) -> Option<SocketAddr> {
    match a {
        ppp::v1::Addresses::Tcp4(x) => Some(SocketAddr::new(
            IpAddr::from(x.source_address),
            x.source_port,
        )),
        ppp::v1::Addresses::Tcp6(x) => Some(SocketAddr::new(
            IpAddr::from(x.source_address),
            x.source_port,
        )),
        ppp::v1::Addresses::Unknown => None,
    }
}

/// Writes a PROXY protocol header carrying the true client (`src`) address and the
/// connected backend/relay (`dst`) address.
pub async fn write_proxy_header<S: AsyncWrite + Unpin>(
    out: &mut S,
    version: TcpProxyProtocol,
    src: SocketAddr,
    dst: SocketAddr,
) -> Result<(), BoxError> {
    let bytes = match version {
        TcpProxyProtocol::V2 => build_v2(src, dst)?,
        TcpProxyProtocol::V1 => build_v1(src, dst),
    };
    tokio::io::AsyncWriteExt::write_all(out, &bytes).await?;
    Ok(())
}

/// Writes the PROXY v2 header a relay hop starts with; with `confirm`, it asks the
/// relay to confirm its own next hop and says how long this side will wait.
pub async fn write_relay_header<S: AsyncWrite + Unpin>(
    out: &mut S,
    src: SocketAddr,
    dst: SocketAddr,
    confirm: Option<Duration>,
) -> Result<(), BoxError> {
    let bytes = match confirm {
        None => build_v2(src, dst)?,
        Some(patience) => {
            use ppp::v2::{Builder, Command, Protocol, Version};
            let (src, dst) = same_family(src, dst);
            let addrs: ppp::v2::Addresses = (src, dst).into();
            let millis = u32::try_from(patience.as_millis()).unwrap_or(u32::MAX);
            let [a, b, c, d] = millis.to_be_bytes();
            Builder::with_addresses(Version::Two | Command::Proxy, Protocol::Stream, addrs)
                .write_tlv(CONFIRM_TLV, &[CONFIRM_VERSION, a, b, c, d])?
                .build()?
        }
    };
    out.write_all(&bytes).await?;
    Ok(())
}

/// A PROXY header carries one address family. A client and a destination of
/// different families (an IPv6 client relayed to an IPv4 hop, or the reverse)
/// are both expressed as IPv6, the IPv4 side as `::ffff:a.b.c.d`, which the
/// receiving side unmaps again in `read_proxy_header`.
fn same_family(src: SocketAddr, dst: SocketAddr) -> (SocketAddr, SocketAddr) {
    let (src, dst) = (
        crate::listener::canonical(src),
        crate::listener::canonical(dst),
    );
    if src.is_ipv4() == dst.is_ipv4() {
        return (src, dst);
    }
    let mapped = |addr: SocketAddr| match addr.ip() {
        IpAddr::V4(v4) => SocketAddr::new(IpAddr::V6(v4.to_ipv6_mapped()), addr.port()),
        IpAddr::V6(_) => addr,
    };
    (mapped(src), mapped(dst))
}

fn build_v2(src: SocketAddr, dst: SocketAddr) -> Result<Vec<u8>, BoxError> {
    use ppp::v2::{Builder, Command, Protocol, Version};
    let (src, dst) = same_family(src, dst);
    let addrs: ppp::v2::Addresses = (src, dst).into();
    Ok(Builder::with_addresses(Version::Two | Command::Proxy, Protocol::Stream, addrs).build()?)
}

fn build_v1(src: SocketAddr, dst: SocketAddr) -> Vec<u8> {
    let (src, dst) = same_family(src, dst);
    let proto = if src.is_ipv4() && dst.is_ipv4() {
        "TCP4"
    } else {
        "TCP6"
    };
    format!(
        "PROXY {} {} {} {} {}\r\n",
        proto,
        src.ip(),
        dst.ip(),
        src.port(),
        dst.port()
    )
    .into_bytes()
}

/// Splices two streams together until either side closes.
pub async fn splice(mut a: impl AsyncRw, mut b: impl AsyncRw) {
    match tokio::io::copy_bidirectional(&mut a, &mut b).await {
        Ok((a_to_b, b_to_a)) => {
            tracing::debug!(a_to_b, b_to_a, "connection closed");
        }
        Err(e) => {
            tracing::warn!(error = %e, "splice error");
        }
    }
}

/// A client-side stream whose traffic is credited to its tag as it flows: bytes read
/// from the client count as upload, bytes written to it as download. Counting per
/// poll rather than at close keeps a long-lived connection visible in every health
/// interval and keeps the bytes of a connection that ends in an error.
pub struct Counted<S> {
    inner: S,
    stats: Arc<TagStats>,
}

impl<S> Counted<S> {
    pub fn new(inner: S, stats: Arc<TagStats>) -> Self {
        Self { inner, stats }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Counted<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let polled = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &polled {
            let n = buf.filled().len().saturating_sub(before);
            this.stats.add_upload(n as u64);
        }
        polled
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Counted<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &polled {
            this.stats.add_download(*n as u64);
        }
        polled
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

pub enum TargetStream {
    Exit(TcpStream),
    Relay(RelayStream),
}

impl AsyncRead for TargetStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        match this {
            TargetStream::Exit(stream) => Pin::new(stream).poll_read(cx, buf),
            TargetStream::Relay(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TargetStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match this {
            TargetStream::Exit(stream) => Pin::new(stream).poll_write(cx, buf),
            TargetStream::Relay(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        match this {
            TargetStream::Exit(stream) => Pin::new(stream).poll_flush(cx),
            TargetStream::Relay(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        match this {
            TargetStream::Exit(stream) => Pin::new(stream).poll_shutdown(cx),
            TargetStream::Relay(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// Handles a single accepted TCP-family connection, logging any error.
pub async fn handle_tcp_connection(
    stream: tokio::net::TcpStream,
    peer: SocketAddr,
    cfg: Arc<PreparedForwarding>,
) {
    if let Err(e) = handle_tcp_inner(stream, peer, &cfg).await {
        tracing::warn!(tag = %cfg.forwarding.tag, error = %e, "connection failed");
    }
}

async fn handle_tcp_inner(
    stream: tokio::net::TcpStream,
    peer: SocketAddr,
    cfg: &PreparedForwarding,
) -> Result<(), BoxError> {
    let _open = cfg.stats.open();
    let (mut client, client_addr, confirm) = entry::ingest_tcp(
        stream,
        peer,
        &cfg.ingest,
        cfg.forwarding.receive_proxy_protocol,
    )
    .await?;
    let out = connect_answering(cfg, &mut client, client_addr, confirm).await?;
    splice(Counted::new(client, cfg.stats.clone()), out).await;
    Ok(())
}

/// Connects the route; when the dialer asked for a confirmation, within the time
/// it said it would wait, answering it either way before the payload flows.
async fn connect_answering<S: AsyncWrite + Unpin>(
    cfg: &PreparedForwarding,
    client: &mut S,
    client_addr: SocketAddr,
    confirm: Option<Duration>,
) -> Result<TargetStream, BoxError> {
    let Some(patience) = confirm else {
        return route::connect(&cfg.route, client_addr).await;
    };
    let now = Instant::now();
    let budget = patience.saturating_sub(CONFIRM_MARGIN).max(CONFIRM_FLOOR);
    let deadline = now.checked_add(budget).unwrap_or(now);
    match route::connect_until(&cfg.route, client_addr, deadline).await {
        Ok(out) => {
            client.write_all(&[CONFIRM_OK]).await?;
            client.flush().await?;
            Ok(out)
        }
        Err(error) => {
            let _ = client.write_all(&[CONFIRM_FAILED]).await;
            let _ = client.flush().await;
            let _ = client.shutdown().await;
            Err(error)
        }
    }
}

/// Handles a single QUIC relay bi-stream, logging any error.
pub async fn handle_relay_quic_stream_logged(
    joined: impl AsyncRw,
    remote: SocketAddr,
    cfg: Arc<PreparedForwarding>,
) {
    if let Err(e) = handle_relay_quic_stream(joined, remote, &cfg).await {
        tracing::warn!(tag = %cfg.forwarding.tag, error = %e, "quic relay stream failed");
    }
}

async fn handle_relay_quic_stream(
    mut joined: impl AsyncRw,
    _remote: SocketAddr,
    cfg: &PreparedForwarding,
) -> Result<(), BoxError> {
    let _open = cfg.stats.open();
    let header = read_header(&mut joined).await?;
    let out = connect_answering(cfg, &mut joined, header.source, header.confirm).await?;
    let client = Counted::new(Prefixed::new(header.leftover, joined), cfg.stats.clone());
    splice(client, out).await;
    Ok(())
}
