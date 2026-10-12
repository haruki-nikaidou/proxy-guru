use crate::BoxError;
use crate::pipe::{CONFIRM_FAILED, CONFIRM_OK, write_proxy_header, write_relay_header};
use guru_worker_config::{KeepAlive, QuicTuning, RelayProtocol, TcpProxyProtocol};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::time::Instant;

pub enum RelayStream {
    Tcp(tokio::net::TcpStream),
    Tls(tokio_openssl::SslStream<tokio::net::TcpStream>),
    Quic(crate::quic::pool::Stream),
}

impl tokio::io::AsyncRead for RelayStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        match this {
            RelayStream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            RelayStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
            RelayStream::Quic(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for RelayStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match this {
            RelayStream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            RelayStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
            RelayStream::Quic(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        match this {
            RelayStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            RelayStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
            RelayStream::Quic(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        match this {
            RelayStream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            RelayStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
            RelayStream::Quic(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// How a relay hop is reached, whichever of its addresses is dialed: owned, so a
/// dial of each address can run on its own while they race.
#[derive(Clone)]
pub struct Link {
    pub protocol: RelayProtocol,
    pub sni: Option<String>,
    /// CA the relay peer is verified against; `None` means the system roots.
    pub relay_ca: Option<PathBuf>,
    pub keepalive: KeepAlive,
    /// This side of the link when `protocol` is QUIC; ignored otherwise.
    pub quic: QuicTuning,
}

/// Connects the link to one of the relay's addresses over the configured transport:
/// TCP, TLS over TCP, or a stream on the pooled QUIC connection, sent and received per
/// `quic`. TLS and QUIC peers are verified against `relay_ca` when given, else the
/// system roots; the hop probes for liveness per `keepalive`. Nothing is written yet.
pub async fn connect_link(link: Link, addr: SocketAddr) -> Result<RelayStream, BoxError> {
    let connect_tcp = || async {
        let tcp = tokio::net::TcpStream::connect(addr).await?;
        if let Err(error) = crate::keepalive::apply_tcp(&tcp, &link.keepalive) {
            tracing::warn!(%addr, %error, "keepalive not set on relay socket");
        }
        Ok::<_, BoxError>(tcp)
    };
    Ok(match link.protocol {
        RelayProtocol::Tcp => RelayStream::Tcp(connect_tcp().await?),
        RelayProtocol::TlsOverTcp => {
            let sni = link.sni.as_deref().ok_or("relay tls dial requires sni")?;
            let tcp = connect_tcp().await?;
            RelayStream::Tls(crate::tls::connect_tls(sni, tcp, link.relay_ca.as_deref()).await?)
        }
        RelayProtocol::Quic => {
            let sni = link.sni.as_deref().ok_or("relay quic dial requires sni")?;
            let client = crate::tls::quic_client(link.relay_ca.as_deref())?;
            RelayStream::Quic(
                client
                    .stream(addr, sni, &link.keepalive, &link.quic)
                    .await?,
            )
        }
    })
}

/// Writes the PROXY v2 framing header carrying the true client address on a link
/// connected to `addr`, by `connect_by`.
///
/// With `confirm_by`, the header also asks the relay to confirm, and tells it how long
/// this side will wait: this only succeeds once the relay answers that its own next
/// hop connected, which it must do by `confirm_by`.
pub async fn introduce(
    out: &mut RelayStream,
    client_addr: SocketAddr,
    addr: SocketAddr,
    connect_by: Instant,
    confirm_by: Option<Instant>,
) -> Result<(), BoxError> {
    let Some(confirm_by) = confirm_by else {
        let header = write_proxy_header(out, TcpProxyProtocol::V2, client_addr, addr);
        return match tokio::time::timeout_at(connect_by, header).await {
            Ok(written) => written,
            Err(_) => Err("timed out writing the PROXY header".into()),
        };
    };
    let budget = confirm_by.saturating_duration_since(Instant::now());
    let confirmed = async {
        write_relay_header(out, client_addr, addr, Some(budget)).await?;
        out.flush().await?;
        let mut status = [0u8; 1];
        out.read_exact(&mut status)
            .await
            .map_err(|error| BoxError::from(format!("relay closed before confirming: {error}")))?;
        match status {
            [CONFIRM_OK] => Ok(()),
            [CONFIRM_FAILED] => Err(BoxError::from("the relay's own next hops did not answer")),
            [other] => Err(format!("relay answered {other:#04x} instead of a confirmation").into()),
        }
    };
    match tokio::time::timeout_at(confirm_by, confirmed).await {
        Ok(result) => result,
        Err(_) => Err("relay did not confirm in time".into()),
    }
}
