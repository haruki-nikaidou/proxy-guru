//! One QUIC connection per relay link on the dialing side.
//!
//! Every proxied connection used to dial its own QUIC connection: a TLS handshake
//! and a fresh slow start each time, and with a fixed-rate sender each of them
//! would have sent at the full rate. The pool keeps one connection per
//! [`Key`] (the peer, its name, and the transport parameters in force) and opens
//! a stream on it per proxied connection, the way hysteria does. Concurrent dials
//! to the same key share one handshake; a connection that died is replaced on the
//! next dial; a connection nothing has used for the configured idle period is
//! closed, so the keep-alive pings do not keep links of a superseded config alive
//! for good.

use crate::BoxError;
use guru_worker_config::{KeepAlive, QuicTuning};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::ReadBuf;

/// How long a dial waits for stream credit on a connection at the peer's stream
/// limit before failing rather than queueing.
const STREAM_WAIT: Duration = Duration::from_secs(5);

/// How long a handshake may take. Without it a peer that has gone away holds the
/// link's slot until quinn's idle timeout, and every dial behind it waits too.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(4);

/// How long after a failed handshake further dials to the same link fail at
/// once instead of handshaking again, so a dead peer costs one wait, not one per
/// connection queued behind it.
const FAILURE_HOLD: Duration = Duration::from_secs(1);

/// What makes two dials share a connection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    pub addr: SocketAddr,
    pub sni: String,
    pub keepalive: KeepAlive,
    pub tuning: QuicTuning,
}

/// A connection in use, and how busy it is.
struct Live {
    conn: quinn::Connection,
    /// Streams currently open on it.
    streams: Arc<AtomicUsize>,
    /// Milliseconds since the pool was created at which a stream was last
    /// opened or closed.
    last_used: Arc<AtomicU64>,
}

#[derive(Default)]
struct Slot {
    live: Option<Live>,
    /// The last handshake that failed, and why.
    failed: Option<(Instant, String)>,
}

pub struct Pool {
    endpoint: quinn::Endpoint,
    config: quinn::ClientConfig,
    epoch: Instant,
    /// One slot per key; the slot's own lock is held across a handshake, so
    /// concurrent dials to the same key wait for it instead of racing it.
    slots: parking_lot::Mutex<HashMap<Key, Arc<tokio::sync::Mutex<Slot>>>>,
}

/// A stream opened by the pool: a joined `recv`/`send` pair that counts itself
/// against its connection for as long as it lives.
pub struct Stream {
    inner: tokio::io::Join<quinn::RecvStream, quinn::SendStream>,
    _guard: StreamGuard,
}

struct StreamGuard {
    streams: Arc<AtomicUsize>,
    last_used: Arc<AtomicU64>,
    epoch: Instant,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.streams.fetch_sub(1, Ordering::AcqRel);
        self.last_used
            .store(millis_since(self.epoch), Ordering::Release);
    }
}

fn millis_since(epoch: Instant) -> u64 {
    u64::try_from(epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
}

impl Pool {
    pub fn new(endpoint: quinn::Endpoint, config: quinn::ClientConfig) -> Self {
        Self {
            endpoint,
            config,
            epoch: Instant::now(),
            slots: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Live connections the pool holds right now.
    pub fn connections(&self) -> usize {
        self.slots
            .lock()
            .values()
            .filter(|slot| {
                slot.try_lock()
                    .ok()
                    .and_then(|s| s.live.as_ref().map(|l| l.conn.close_reason().is_none()))
                    .unwrap_or(false)
            })
            .count()
    }

    /// Opens a stream on the link `key` names, connecting it first when there is
    /// no usable connection.
    pub async fn stream(&self, key: Key) -> Result<Stream, BoxError> {
        self.sweep(&key);
        let slot = self.slots.lock().entry(key.clone()).or_default().clone();
        let mut slot = slot.lock().await;
        // A connection that died since the last dial fails its `open_bi`; the
        // one retry goes through a fresh handshake.
        for attempt in 0..2u8 {
            let live = match &slot.live {
                Some(live) if live.conn.close_reason().is_none() => live,
                _ => {
                    if let Some((at, error)) = &slot.failed
                        && at.elapsed() < FAILURE_HOLD
                    {
                        return Err(
                            format!("relay quic handshake failed moments ago: {error}").into()
                        );
                    }
                    let conn = match self.connect(&key).await {
                        Ok(conn) => {
                            slot.failed = None;
                            conn
                        }
                        Err(error) => {
                            slot.failed = Some((Instant::now(), error.to_string()));
                            return Err(error);
                        }
                    };
                    slot.live.insert(Live {
                        conn,
                        streams: Arc::new(AtomicUsize::new(0)),
                        last_used: Arc::new(AtomicU64::new(millis_since(self.epoch))),
                    })
                }
            };
            match tokio::time::timeout(STREAM_WAIT, live.conn.open_bi()).await {
                Ok(Ok((send, recv))) => {
                    live.streams.fetch_add(1, Ordering::AcqRel);
                    live.last_used
                        .store(millis_since(self.epoch), Ordering::Release);
                    return Ok(Stream {
                        inner: tokio::io::join(recv, send),
                        _guard: StreamGuard {
                            streams: live.streams.clone(),
                            last_used: live.last_used.clone(),
                            epoch: self.epoch,
                        },
                    });
                }
                Ok(Err(error)) => {
                    tracing::debug!(addr = %key.addr, %error, "relay quic connection lost; redialing");
                    slot.live = None;
                    if attempt == 1 {
                        return Err(error.into());
                    }
                }
                Err(_) => {
                    return Err(format!(
                        "relay quic stream limit reached on the link to {} (nothing freed a \
                         stream within {STREAM_WAIT:?})",
                        key.addr
                    )
                    .into());
                }
            }
        }
        Err("relay quic dial gave up".into())
    }

    async fn connect(&self, key: &Key) -> Result<quinn::Connection, BoxError> {
        let mut config = self.config.clone();
        config.transport_config(crate::quic::transport::build(
            &key.keepalive,
            Some(&key.tuning),
        ));
        let connecting = self.endpoint.connect_with(config, key.addr, &key.sni)?;
        let conn = match tokio::time::timeout(HANDSHAKE_TIMEOUT, connecting).await {
            Ok(conn) => conn?,
            Err(_) => {
                return Err(
                    format!("relay quic handshake timed out after {HANDSHAKE_TIMEOUT:?}").into(),
                );
            }
        };
        tracing::info!(addr = %key.addr, sni = %key.sni, "relay quic connection opened");
        Ok(conn)
    }

    /// Drops dead connections and closes idle ones: a link a config no longer
    /// names, or one merely quiet for `quic_idle_secs`, is not worth pinging.
    fn sweep(&self, key: &Key) {
        let idle = Duration::from_secs(u64::from(key.keepalive.quic_idle_secs));
        let now = millis_since(self.epoch);
        let mut slots = self.slots.lock();
        slots.retain(|_, slot| {
            // A slot mid-handshake is busy by definition and stays.
            let Ok(mut slot) = slot.try_lock() else {
                return true;
            };
            let Some(live) = &slot.live else {
                // Keep a recent failure, so the hold applies to the next dial.
                return slot
                    .failed
                    .as_ref()
                    .is_some_and(|(at, _)| at.elapsed() < FAILURE_HOLD);
            };
            if live.conn.close_reason().is_some() {
                slot.live = None;
                return false;
            }
            let quiet = now.saturating_sub(live.last_used.load(Ordering::Acquire));
            if live.streams.load(Ordering::Acquire) == 0 && Duration::from_millis(quiet) >= idle {
                live.conn.close(quinn::VarInt::from_u32(0), b"idle");
                slot.live = None;
                return false;
            }
            true
        });
    }
}

impl tokio::io::AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for Stream {
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
