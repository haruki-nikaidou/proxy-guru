//! Liveness probing on the data plane, from the `[keepalive]` section of the config.
//!
//! TCP has no notion of a peer that silently went away, so every accepted and dialed
//! TCP socket gets `SO_KEEPALIVE` with the configured cadence and the kernel fails the
//! connection after the configured number of unanswered probes; `splice` then ends
//! and the other side of the pipe is closed with it. A QUIC relay connection gets the
//! opposite treatment: quinn's default idle timeout (thirty seconds, no pings) would
//! cut a long connection that is merely quiet, so it pings and is allowed a longer
//! idle period.

use guru_worker_config::KeepAlive;
use std::sync::Arc;
use std::time::Duration;

/// The kernel parameters for one TCP socket.
fn tcp(ka: &KeepAlive) -> socket2::TcpKeepalive {
    socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(u64::from(ka.tcp_idle_secs)))
        .with_interval(Duration::from_secs(u64::from(ka.tcp_interval_secs)))
        .with_retries(ka.tcp_retries)
}

/// Enables keep-alive probing on `stream`. A failure is the caller's to log: the
/// connection is still usable, it just will not notice a vanished peer.
pub fn apply_tcp(stream: &tokio::net::TcpStream, ka: &KeepAlive) -> std::io::Result<()> {
    socket2::SockRef::from(stream).set_tcp_keepalive(&tcp(ka))
}

/// The transport parameters for one QUIC connection, either side.
pub fn quic_transport(ka: &KeepAlive) -> Arc<quinn::TransportConfig> {
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(u64::from(ka.quic_ping_secs))));
    // `quic_idle_secs` is validated against `u32` seconds, so the millisecond value
    // fits a `VarInt` (a 62-bit integer) with room to spare.
    transport.max_idle_timeout(Some(
        quinn::VarInt::from_u64(u64::from(ka.quic_idle_secs) * 1000)
            .unwrap_or(quinn::VarInt::MAX)
            .into(),
    ));
    Arc::new(transport)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_socket_gets_the_configured_probe_cadence() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let dialed = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        let ka = KeepAlive {
            tcp_idle_secs: 7,
            tcp_interval_secs: 3,
            tcp_retries: 2,
            ..KeepAlive::default()
        };
        for stream in [&dialed, &accepted] {
            let sock = socket2::SockRef::from(stream);
            assert!(!sock.keepalive().unwrap(), "off until asked");
            apply_tcp(stream, &ka).unwrap();
            assert!(sock.keepalive().unwrap());
            assert_eq!(sock.tcp_keepalive_time().unwrap(), Duration::from_secs(7));
            assert_eq!(
                sock.tcp_keepalive_interval().unwrap(),
                Duration::from_secs(3)
            );
            assert_eq!(sock.tcp_keepalive_retries().unwrap(), 2);
        }
    }
}
