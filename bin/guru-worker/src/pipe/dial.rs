//! Dialing one hop: every address it has, raced in the manner of Happy Eyeballs
//! (RFC 8305), then what the hop itself needs once connected — an exit's PROXY
//! header, a relay's header and confirmation.
//!
//! A hop's addresses are its destination's (a domain may resolve to several) and,
//! for a relay, the alternates the control plane named — the same server over the
//! other address family. The address the hop last answered on goes first; the next
//! one starts [`ADDRESS_STAGGER`] later, or at once when one fails, and the first
//! to connect wins while the others are dropped. All of them failing is one failed
//! attempt on the hop.

use crate::BoxError;
use crate::liveness;
use crate::pipe::{TargetStream, exit, relay, write_proxy_header};
use crate::prepared::{Hop, HopTarget};
use guru_worker_config::{Ipv6Resolve, Remote};
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::Instant;

/// How long one address of a hop has to connect before the next one starts.
pub const ADDRESS_STAGGER: Duration = Duration::from_millis(250);

/// The most addresses of one hop an attempt dials.
pub const MAX_ADDRESSES: usize = 4;

/// The time limits of one attempt on a hop.
#[derive(Debug, Clone, Copy)]
pub struct Bounds {
    /// Connecting — resolution, TCP, the TLS or QUIC handshake, an unconfirmed
    /// header — must be done by then.
    pub connect_by: Instant,
    /// How long a confirming relay may take to answer once connected.
    pub confirm_wait: Duration,
    /// The connection's own deadline: nothing waits past it.
    pub deadline: Instant,
}

/// Dials `hop` for `client` within `bounds`.
pub async fn dial_hop(
    hop: &Hop,
    client: SocketAddr,
    bounds: Bounds,
) -> Result<TargetStream, BoxError> {
    match &hop.target {
        HopTarget::Exit {
            destination,
            ipv6_resolve,
            send_pp,
            keepalive,
        } => {
            let keepalive = *keepalive;
            let (mut stream, addr) = connect_within(bounds.connect_by, async {
                let addrs = addresses(&hop.record, destination, &[], *ipv6_resolve).await?;
                let raced = race_addresses(&addrs, ADDRESS_STAGGER, move |addr| {
                    exit::connect_exit(addr, keepalive)
                })
                .await;
                settle(&hop.record, destination, *ipv6_resolve, &addrs, raced).await
            })
            .await?;
            if let Some(version) = send_pp {
                let header = write_proxy_header(&mut stream, *version, client, addr);
                match tokio::time::timeout_at(bounds.connect_by, header).await {
                    Ok(written) => written?,
                    Err(_) => return Err("timed out writing the PROXY header".into()),
                }
            }
            Ok(TargetStream::Exit(stream))
        }
        HopTarget::Relay {
            protocol,
            destination,
            ipv6_resolve,
            sni,
            relay_ca,
            keepalive,
            quic,
            confirm,
        } => {
            let link = relay::Link {
                protocol: *protocol,
                sni: sni.clone(),
                relay_ca: relay_ca.clone(),
                keepalive: *keepalive,
                quic: *quic,
            };
            let (mut out, addr) = connect_within(bounds.connect_by, async {
                let addrs = addresses(&hop.record, destination, &[], *ipv6_resolve).await?;
                let raced = race_addresses(&addrs, ADDRESS_STAGGER, move |addr| {
                    relay::connect_link(link.clone(), addr)
                })
                .await;
                settle(&hop.record, destination, *ipv6_resolve, &addrs, raced).await
            })
            .await?;
            let confirm_by = confirm.then(|| {
                Instant::now()
                    .checked_add(bounds.confirm_wait)
                    .map_or(bounds.deadline, |by| by.min(bounds.deadline))
            });
            relay::introduce(&mut out, client, addr, bounds.connect_by, confirm_by).await?;
            Ok(TargetStream::Relay(out))
        }
    }
}

async fn connect_within<T>(
    connect_by: Instant,
    connecting: impl Future<Output = Result<T, BoxError>>,
) -> Result<T, BoxError> {
    match tokio::time::timeout_at(connect_by, connecting).await {
        Ok(connected) => connected,
        Err(_) => Err("timed out connecting".into()),
    }
}

/// Every address to dial for a hop, best first: the destination's, then the
/// alternates `ipv6_resolve` admits, each once, the one the hop last answered on
/// moved to the front, at most [`MAX_ADDRESSES`]. An alternate that does not
/// resolve is left out; the destination not resolving fails the attempt.
async fn addresses(
    record: &liveness::Hop,
    destination: &Remote,
    alternates: &[Remote],
    ipv6_resolve: Ipv6Resolve,
) -> Result<Vec<SocketAddr>, BoxError> {
    let mut addrs: Vec<SocketAddr> = crate::resolver::resolve_all(destination, ipv6_resolve)
        .await?
        .to_vec();
    for alternate in alternates {
        match crate::resolver::resolve_all(alternate, ipv6_resolve).await {
            Ok(resolved) => addrs.extend(
                resolved
                    .iter()
                    .filter(|addr| crate::resolver::admits(ipv6_resolve, **addr)),
            ),
            Err(error) => tracing::debug!(%error, "alternate address of a hop left out"),
        }
    }
    let mut seen = Vec::with_capacity(addrs.len());
    addrs.retain(|addr| {
        let first = !seen.contains(addr);
        seen.push(*addr);
        first
    });
    if let Some(preferred) = record.preferred()
        && let Some(at) = addrs.iter().position(|addr| *addr == preferred)
    {
        addrs[..=at].rotate_right(1);
    }
    addrs.truncate(MAX_ADDRESSES);
    Ok(addrs)
}

/// Remembers which address answered when there was a choice, and lets a domain be
/// resolved afresh when none of its addresses did.
async fn settle<T>(
    record: &liveness::Hop,
    destination: &Remote,
    ipv6_resolve: Ipv6Resolve,
    addrs: &[SocketAddr],
    raced: Result<(T, SocketAddr), BoxError>,
) -> Result<(T, SocketAddr), BoxError> {
    match raced {
        Ok((connected, addr)) => {
            if addrs.len() > 1 {
                record.prefer(addr);
            }
            Ok((connected, addr))
        }
        Err(error) => {
            crate::resolver::forget(destination, ipv6_resolve).await;
            Err(error)
        }
    }
}

/// Dials `addrs` in order, each [`ADDRESS_STAGGER`]-like `stagger` after the one
/// before or at once when one fails, and returns the first to connect with its
/// address. The others are aborted. All failing returns the last error.
pub(crate) async fn race_addresses<T, F, Fut>(
    addrs: &[SocketAddr],
    stagger: Duration,
    dial: F,
) -> Result<(T, SocketAddr), BoxError>
where
    T: Send + 'static,
    F: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = Result<T, BoxError>> + Send + 'static,
{
    let Some((&first, rest)) = addrs.split_first() else {
        return Err("no address to dial".into());
    };
    if rest.is_empty() {
        return dial(first)
            .await
            .map(|connected| (connected, first))
            .map_err(|error| format!("{first}: {error}").into());
    }
    let mut racing: JoinSet<(SocketAddr, Result<T, BoxError>)> = JoinSet::new();
    let start = |racing: &mut JoinSet<_>, addr: SocketAddr| {
        let dialing = dial(addr);
        racing.spawn(async move { (addr, dialing.await) });
    };
    let mut waiting = rest.iter().copied();
    start(&mut racing, first);
    let mut next = waiting.next();
    let mut next_at = Instant::now().checked_add(stagger);
    let mut last_error: Option<String> = None;
    loop {
        let due = next.and(next_at);
        let stagger_due = async move {
            match due {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            joined = racing.join_next() => {
                match joined {
                    Some(Ok((addr, Ok(connected)))) => return Ok((connected, addr)),
                    Some(Ok((addr, Err(error)))) => last_error = Some(format!("{addr}: {error}")),
                    Some(Err(error)) => last_error = Some(format!("dial task failed: {error}")),
                    None => {}
                }
                // A failure starts the next address at once.
                match next.take() {
                    Some(addr) => {
                        start(&mut racing, addr);
                        next = waiting.next();
                        next_at = Instant::now().checked_add(stagger);
                    }
                    None if racing.is_empty() => break,
                    None => {}
                }
            }
            () = stagger_due => {
                if let Some(addr) = next.take() {
                    start(&mut racing, addr);
                    next = waiting.next();
                    next_at = Instant::now().checked_add(stagger);
                }
            }
        }
    }
    Err(last_error
        .unwrap_or_else(|| "every address failed".to_string())
        .into())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::arithmetic_side_effects)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    /// A dial whose outcome and delay are scripted per address, recording when each
    /// address was started (in milliseconds of paused time).
    fn scripted(
        script: Vec<(SocketAddr, u64, bool)>,
        started: Arc<Mutex<Vec<(SocketAddr, u128)>>>,
        epoch: Instant,
    ) -> impl Fn(SocketAddr) -> std::pin::Pin<Box<dyn Future<Output = Result<u8, BoxError>> + Send>>
    {
        move |a| {
            let (_, delay, ok) = *script.iter().find(|(x, _, _)| *x == a).unwrap();
            started
                .lock()
                .unwrap()
                .push((a, epoch.elapsed().as_millis()));
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                if ok { Ok(1) } else { Err("refused".into()) }
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_next_address_starts_after_the_stagger_and_the_first_to_connect_wins() {
        let (a, b) = (addr("192.0.2.1:1"), addr("[2001:db8::1]:1"));
        let started = Arc::new(Mutex::new(Vec::new()));
        let epoch = Instant::now();
        // `a` is a blackhole: it would only answer after a minute.
        let dial = scripted(
            vec![(a, 60_000, true), (b, 50, true)],
            started.clone(),
            epoch,
        );
        let (_, won) = race_addresses(&[a, b], ADDRESS_STAGGER, dial)
            .await
            .unwrap();
        assert_eq!(won, b);
        assert_eq!(epoch.elapsed(), Duration::from_millis(300));
        assert_eq!(*started.lock().unwrap(), vec![(a, 0), (b, 250)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_starts_the_next_address_at_once() {
        let (a, b) = (addr("192.0.2.1:1"), addr("[2001:db8::1]:1"));
        let started = Arc::new(Mutex::new(Vec::new()));
        let epoch = Instant::now();
        let dial = scripted(vec![(a, 10, false), (b, 10, true)], started.clone(), epoch);
        let (_, won) = race_addresses(&[a, b], ADDRESS_STAGGER, dial)
            .await
            .unwrap();
        assert_eq!(won, b);
        assert_eq!(*started.lock().unwrap(), vec![(a, 0), (b, 10)]);
    }

    #[tokio::test(start_paused = true)]
    async fn all_failing_returns_the_last_error() {
        let (a, b) = (addr("192.0.2.1:1"), addr("[2001:db8::1]:1"));
        let started = Arc::new(Mutex::new(Vec::new()));
        let dial = scripted(
            vec![(a, 10, false), (b, 20, false)],
            started,
            Instant::now(),
        );
        let error = race_addresses(&[a, b], ADDRESS_STAGGER, dial)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.starts_with("[2001:db8::1]:1"), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn one_address_is_dialed_directly() {
        let a = addr("192.0.2.1:1");
        let started = Arc::new(Mutex::new(Vec::new()));
        let dial = scripted(vec![(a, 10, true)], started.clone(), Instant::now());
        let (_, won) = race_addresses(&[a], ADDRESS_STAGGER, dial).await.unwrap();
        assert_eq!(won, a);
        assert!(
            race_addresses(&[], ADDRESS_STAGGER, |_| async { Ok(1u8) })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn the_preferred_address_goes_first() {
        let record = liveness::Hop::default();
        let (a, b) = (addr("192.0.2.1:1"), addr("[2001:db8::1]:1"));
        let alternates = [Remote::Address(b)];
        let dest = Remote::Address(a);
        assert_eq!(
            addresses(&record, &dest, &alternates, Ipv6Resolve::Tolerated)
                .await
                .unwrap(),
            vec![a, b]
        );
        record.prefer(b);
        assert_eq!(
            addresses(&record, &dest, &alternates, Ipv6Resolve::Tolerated)
                .await
                .unwrap(),
            vec![b, a]
        );
        assert_eq!(
            addresses(&record, &dest, &alternates, Ipv6Resolve::Forbidden)
                .await
                .unwrap(),
            vec![a],
            "an alternate of a family the server must not dial is left out"
        );
    }
}
