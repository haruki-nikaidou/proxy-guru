use crate::BoxError;
use crate::pipe::{CONFIRM_FAILED, CONFIRM_OK, write_proxy_header, write_relay_header};
use guru_worker_config::{
    Ipv6Resolve, KeepAlive, QuicTuning, RelayProtocol, Remote, TcpProxyProtocol,
};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};

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

/// Everything a relay dial needs besides its time limits.
pub struct Dialing<'a> {
    pub protocol: RelayProtocol,
    pub destination: &'a Remote,
    pub ipv6_resolve: Ipv6Resolve,
    pub sni: Option<&'a str>,
    pub relay_ca: Option<&'a Path>,
    pub keepalive: &'a KeepAlive,
    pub quic: &'a QuicTuning,
    pub client_addr: SocketAddr,
}

/// Dials the next relay hop over the configured transport and writes the PROXY v2
/// framing header carrying the true client address. TLS and QUIC peers are verified
/// against `relay_ca` when given, else the system roots; the hop probes for liveness
/// per `keepalive`. A QUIC hop is a stream on the link's pooled connection, sent
/// and received per `quic`.
///
/// Connecting (resolution, TCP, the TLS or QUIC handshake, the header) must be done
/// by `connect_by`. With `confirm_by`, the header also asks the relay to confirm,
/// and tells it how long this side will wait: the dial only succeeds once the relay
/// answers that its own next hop connected, which it must do by `confirm_by`.
pub async fn dial_relay(
    dialing: &Dialing<'_>,
    connect_by: Instant,
    confirm_by: Option<Instant>,
) -> Result<RelayStream, BoxError> {
    let (mut out, addr) = match tokio::time::timeout_at(connect_by.into(), connect(dialing)).await {
        Ok(connected) => connected?,
        Err(_) => return Err("timed out connecting".into()),
    };
    let Some(confirm_by) = confirm_by else {
        let header = write_proxy_header(&mut out, TcpProxyProtocol::V2, dialing.client_addr, addr);
        match tokio::time::timeout_at(connect_by.into(), header).await {
            Ok(written) => written?,
            Err(_) => return Err("timed out writing the PROXY header".into()),
        }
        return Ok(out);
    };
    let budget = confirm_by.saturating_duration_since(Instant::now());
    let confirmed = async {
        write_relay_header(&mut out, dialing.client_addr, addr, Some(budget)).await?;
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
    match tokio::time::timeout_at(confirm_by.into(), confirmed).await {
        Ok(result) => result?,
        Err(_) => return Err("relay did not confirm in time".into()),
    }
    Ok(out)
}

async fn connect(dialing: &Dialing<'_>) -> Result<(RelayStream, SocketAddr), BoxError> {
    let addr = crate::resolver::resolve(dialing.destination, dialing.ipv6_resolve).await?;
    let connect_tcp = || async {
        let tcp = tokio::net::TcpStream::connect(addr).await?;
        if let Err(error) = crate::keepalive::apply_tcp(&tcp, dialing.keepalive) {
            tracing::warn!(%addr, %error, "keepalive not set on relay socket");
        }
        Ok::<_, BoxError>(tcp)
    };
    let out = match dialing.protocol {
        RelayProtocol::Tcp => RelayStream::Tcp(connect_tcp().await?),
        RelayProtocol::TlsOverTcp => {
            let sni = dialing.sni.ok_or("relay tls dial requires sni")?;
            let tcp = connect_tcp().await?;
            RelayStream::Tls(crate::tls::connect_tls(sni, tcp, dialing.relay_ca).await?)
        }
        RelayProtocol::Quic => {
            let sni = dialing.sni.ok_or("relay quic dial requires sni")?;
            let client = crate::tls::quic_client(dialing.relay_ca)?;
            RelayStream::Quic(
                client
                    .stream(addr, sni, dialing.keepalive, dialing.quic)
                    .await?,
            )
        }
    };
    Ok((out, addr))
}
