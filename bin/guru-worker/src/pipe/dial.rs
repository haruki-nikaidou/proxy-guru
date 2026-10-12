//! Dialing one hop: every address it has, raced in the manner of Happy Eyeballs
//! (RFC 8305), then what the hop itself needs once connected — an exit's PROXY
//! header, a relay's header and confirmation.
//!
//! A hop's addresses are its destination's (a domain may resolve to several) and,
//! for a relay, the alternates the control plane named — the same server over the
//! other address family. They are dialed in the same order every time, the
//! destination's first: the address the control plane chose to dial (IPv4 when the
//! server has one) always goes first, and the others are only a fallback. The next
//! one starts [`ADDRESS_STAGGER`] later, or at once when one fails, and the first to
//! connect wins while the others are dropped. All of them failing is one failed
//! attempt on the hop.

use crate::BoxError;
use crate::pipe::{TargetStream, exit, relay, write_proxy_header};
use crate::prepared::{Hop, HopTarget};
use guru_worker_config::{Ipv6Resolve, Remote};
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::Instant;

/// How long one address of a hop has to connect before the next one, the
/// fallback, starts beside it.
pub const ADDRESS_STAGGER: Duration = Duration::from_millis(700);

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

/// Where a background probe of a hop says the connection comes from.
pub const PROBE_CLIENT: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0);

/// Dials `hop` for `client` within `bounds`.
pub async fn dial_hop(
    hop: &Hop,
    client: SocketAddr,
    bounds: Bounds,
) -> Result<TargetStream, BoxError> {
    match connect_hop(hop, bounds.connect_by).await? {
        (Connected::Exit(mut stream), addr) => {
            if let HopTarget::Exit {
                send_pp: Some(version),
                ..
            } = &hop.target
            {
                let header = write_proxy_header(&mut stream, *version, client, addr);
                match tokio::time::timeout_at(bounds.connect_by, header).await {
                    Ok(written) => written?,
                    Err(_) => return Err("timed out writing the PROXY header".into()),
                }
            }
            Ok(TargetStream::Exit(stream))
        }
        (Connected::Relay(mut out), addr) => {
            let confirm_by = confirms(hop).then(|| confirm_by(bounds));
            relay::introduce(&mut out, client, addr, bounds.connect_by, confirm_by).await?;
            Ok(TargetStream::Relay(out))
        }
    }
}

/// Dials `hop` without a client, to learn whether it answers, and lets go of it:
/// an exit or a relay that does not confirm once its transport is up (nothing is
/// sent, so the relay dials nothing on), a confirming relay once it confirmed.
pub async fn probe_hop(hop: &Hop, bounds: Bounds) -> Result<(), BoxError> {
    match connect_hop(hop, bounds.connect_by).await? {
        (Connected::Exit(_), _) => Ok(()),
        (Connected::Relay(mut out), addr) if confirms(hop) => {
            relay::introduce(
                &mut out,
                PROBE_CLIENT,
                addr,
                bounds.connect_by,
                Some(confirm_by(bounds)),
            )
            .await
        }
        (Connected::Relay(_), _) => Ok(()),
    }
}

fn confirms(hop: &Hop) -> bool {
    matches!(hop.target, HopTarget::Relay { confirm: true, .. })
}

/// When a relay connected now must have confirmed.
fn confirm_by(bounds: Bounds) -> Instant {
    Instant::now()
        .checked_add(bounds.confirm_wait)
        .map_or(bounds.deadline, |by| by.min(bounds.deadline))
}

/// A hop's transport, up and nothing written yet.
enum Connected {
    Exit(tokio::net::TcpStream),
    Relay(relay::RelayStream),
}

/// Connects `hop` on the first of its addresses to answer, by `connect_by`.
async fn connect_hop(hop: &Hop, connect_by: Instant) -> Result<(Connected, SocketAddr), BoxError> {
    match &hop.target {
        HopTarget::Exit {
            destination,
            ipv6_resolve,
            keepalive,
            ..
        } => {
            let keepalive = *keepalive;
            connect_within(connect_by, async {
                let addrs = addresses(destination, &[], *ipv6_resolve).await?;
                let raced = race_addresses(&addrs, ADDRESS_STAGGER, move |addr| async move {
                    exit::connect_exit(addr, keepalive)
                        .await
                        .map(Connected::Exit)
                })
                .await;
                settle(destination, *ipv6_resolve, raced).await
            })
            .await
        }
        HopTarget::Relay {
            protocol,
            destination,
            alternates,
            ipv6_resolve,
            sni,
            relay_ca,
            keepalive,
            quic,
            ..
        } => {
            let link = relay::Link {
                protocol: *protocol,
                sni: sni.clone(),
                relay_ca: relay_ca.clone(),
                keepalive: *keepalive,
                quic: *quic,
            };
            connect_within(connect_by, async {
                let addrs = addresses(destination, alternates, *ipv6_resolve).await?;
                let raced = race_addresses(&addrs, ADDRESS_STAGGER, move |addr| {
                    let link = link.clone();
                    async move { relay::connect_link(link, addr).await.map(Connected::Relay) }
                })
                .await;
                settle(destination, *ipv6_resolve, raced).await
            })
            .await
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

/// Every address to dial for a hop, in order: the destination's, then the
/// alternates `ipv6_resolve` admits, each once, at most [`MAX_ADDRESSES`]. An
/// alternate that does not resolve is left out; the destination not resolving
/// fails the attempt.
async fn addresses(
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
    addrs.truncate(MAX_ADDRESSES);
    Ok(addrs)
}

/// Lets a domain be resolved afresh when none of its addresses answered.
async fn settle<T>(
    destination: &Remote,
    ipv6_resolve: Ipv6Resolve,
    raced: Result<(T, SocketAddr), BoxError>,
) -> Result<(T, SocketAddr), BoxError> {
    if raced.is_err() {
        crate::resolver::forget(destination, ipv6_resolve).await;
    }
    raced
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
        let stagger = ADDRESS_STAGGER.as_millis();
        assert_eq!(epoch.elapsed().as_millis(), stagger + 50);
        assert_eq!(*started.lock().unwrap(), vec![(a, 0), (b, stagger)]);
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
    async fn the_destination_always_goes_first_and_alternates_only_after_it() {
        let (a, b) = (addr("192.0.2.1:1"), addr("[2001:db8::1]:1"));
        let alternates = [Remote::Address(b), Remote::Address(a)];
        let dest = Remote::Address(a);
        assert_eq!(
            addresses(&dest, &alternates, Ipv6Resolve::Tolerated)
                .await
                .unwrap(),
            vec![a, b],
            "an alternate repeating the destination is dialed once"
        );
        assert_eq!(
            addresses(&dest, &alternates, Ipv6Resolve::Preferred)
                .await
                .unwrap(),
            vec![a, b],
            "the order is the control plane's, whatever the resolve policy"
        );
        assert_eq!(
            addresses(&dest, &alternates, Ipv6Resolve::Forbidden)
                .await
                .unwrap(),
            vec![a],
            "an alternate of a family the server must not dial is left out"
        );
    }
}
