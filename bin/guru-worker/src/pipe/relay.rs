use crate::BoxError;
use crate::pipe::write_proxy_header;
use guru_worker_config::{Ipv6Resolve, RelayProtocol, Remote, TcpProxyProtocol};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::ReadBuf;

pub enum RelayStream {
    Tcp(tokio::net::TcpStream),
    Tls(tokio_openssl::SslStream<tokio::net::TcpStream>),
    Quic(tokio::io::Join<quinn::RecvStream, quinn::SendStream>),
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

/// Dials the next relay hop over the configured transport and writes the PROXY v2
/// framing header carrying the true client address. TLS and QUIC peers are verified
/// against `relay_ca` when given, else the system roots.
pub async fn dial_relay(
    protocol: RelayProtocol,
    destination: &Remote,
    ipv6_resolve: Ipv6Resolve,
    sni: Option<&str>,
    relay_ca: Option<&Path>,
    client_addr: SocketAddr,
) -> Result<RelayStream, BoxError> {
    let addr = crate::resolver::resolve(destination, ipv6_resolve).await?;
    let mut out = match protocol {
        RelayProtocol::Tcp => RelayStream::Tcp(tokio::net::TcpStream::connect(addr).await?),
        RelayProtocol::TlsOverTcp => {
            let sni = sni.ok_or("relay tls dial requires sni")?;
            let tcp = tokio::net::TcpStream::connect(addr).await?;
            RelayStream::Tls(crate::tls::connect_tls(sni, tcp, relay_ca).await?)
        }
        RelayProtocol::Quic => {
            let sni = sni.ok_or("relay quic dial requires sni")?;
            let endpoint = crate::tls::quic_client_endpoint(relay_ca)?;
            let conn = endpoint.connect(addr, sni)?.await?;
            let (send, recv) = conn.open_bi().await?;
            RelayStream::Quic(tokio::io::join(recv, send))
        }
    };
    write_proxy_header(&mut out, TcpProxyProtocol::V2, client_addr, addr).await?;
    Ok(out)
}
