use crate::BoxError;
use guru_worker_config::KeepAlive;
use std::net::SocketAddr;
use tokio::net::TcpStream;

/// Connects to one address of a real backend. The PROXY header, when the exit wants
/// one, is written by the caller once an address answered.
pub async fn connect_exit(addr: SocketAddr, keepalive: KeepAlive) -> Result<TcpStream, BoxError> {
    let s = TcpStream::connect(addr).await?;
    if let Err(error) = crate::keepalive::apply_tcp(&s, &keepalive) {
        tracing::warn!(%addr, %error, "keepalive not set on exit socket");
    }
    Ok(s)
}
