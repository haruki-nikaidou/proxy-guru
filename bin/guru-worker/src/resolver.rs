use crate::BoxError;
use compact_str::CompactString;
use guru_worker_config::{Ipv6Resolve, Remote};
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

/// TTL for cached DNS resolutions.
const DNS_TTL: Duration = Duration::from_secs(300);

/// The [`Ipv6Resolve`] policy is part of the cache key, so two config generations with
/// different policies (e.g. across a `SIGHUP` reload) never share a resolution.
type DnsCache = moka::future::Cache<(CompactString, u16, Ipv6Resolve), Arc<[SocketAddr]>>;

static DNS_CACHE: LazyLock<DnsCache> =
    LazyLock::new(|| moka::future::Cache::builder().time_to_live(DNS_TTL).build());

/// Every address a forwarding target stands for, in the order to try them. A literal
/// address is itself; a domain is resolved via the system resolver, filtered and ordered
/// by the given [`Ipv6Resolve`] policy (see [`order`]), and cached (single-flight per
/// key via `try_get_with`) for `DNS_TTL`, or until [`forget`].
pub async fn resolve_all(
    remote: &Remote,
    ipv6_resolve: Ipv6Resolve,
) -> Result<Arc<[SocketAddr]>, BoxError> {
    match remote {
        Remote::Address(addr) => Ok(Arc::from([*addr])),
        Remote::Domain(host, port) => {
            let key = (host.clone(), *port, ipv6_resolve);
            DNS_CACHE
                .try_get_with(key.clone(), lookup(key.0, key.1, ipv6_resolve))
                .await
                .map_err(|e: Arc<BoxError>| format!("dns resolve failed: {e}").into())
        }
    }
}

/// Drops a domain's cached addresses, so the next dial resolves it afresh: none of
/// the ones it had answered.
pub async fn forget(remote: &Remote, ipv6_resolve: Ipv6Resolve) {
    if let Remote::Domain(host, port) = remote {
        DNS_CACHE
            .invalidate(&(host.clone(), *port, ipv6_resolve))
            .await;
    }
}

async fn lookup(
    host: CompactString,
    port: u16,
    mode: Ipv6Resolve,
) -> Result<Arc<[SocketAddr]>, BoxError> {
    let addrs = order(tokio::net::lookup_host((host.as_str(), port)).await?, mode);
    if addrs.is_empty() {
        let family = match mode {
            Ipv6Resolve::Required => "IPv6 ",
            Ipv6Resolve::Forbidden => "IPv4 ",
            Ipv6Resolve::Preferred | Ipv6Resolve::Tolerated => "",
        };
        return Err(format!("no {family}DNS records for {host}:{port}").into());
    }
    Ok(addrs.into())
}

/// Whether `mode` lets an address of this family be dialed at all.
pub fn admits(mode: Ipv6Resolve, addr: SocketAddr) -> bool {
    match mode {
        Ipv6Resolve::Required => addr.is_ipv6(),
        Ipv6Resolve::Forbidden => addr.is_ipv4(),
        Ipv6Resolve::Preferred | Ipv6Resolve::Tolerated => true,
    }
}

/// The addresses of a resolver result set that `mode` admits, each once, with the
/// families interleaved (RFC 8305 §4) starting with the one `mode` prefers: IPv6 for
/// `Required` and `Preferred`, IPv4 for `Tolerated` and `Forbidden`. Within a family
/// the resolver's order is kept.
pub fn order(addrs: impl IntoIterator<Item = SocketAddr>, mode: Ipv6Resolve) -> Vec<SocketAddr> {
    let mut v4: Vec<SocketAddr> = Vec::new();
    let mut v6: Vec<SocketAddr> = Vec::new();
    for addr in addrs {
        if !admits(mode, addr) {
            continue;
        }
        let family = if addr.is_ipv4() { &mut v4 } else { &mut v6 };
        if !family.contains(&addr) {
            family.push(addr);
        }
    }
    let (first, second) = match mode {
        Ipv6Resolve::Required | Ipv6Resolve::Preferred => (v6, v4),
        Ipv6Resolve::Tolerated | Ipv6Resolve::Forbidden => (v4, v6),
    };
    let mut out = Vec::with_capacity(first.len().saturating_add(second.len()));
    let (mut first, mut second) = (first.into_iter(), second.into_iter());
    loop {
        match (first.next(), second.next()) {
            (None, None) => break,
            (a, b) => out.extend(a.into_iter().chain(b)),
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn v4() -> SocketAddr {
        addr("127.0.0.1:9000")
    }

    fn v6() -> SocketAddr {
        addr("[::1]:9000")
    }

    #[test]
    fn required_keeps_only_v6() {
        assert_eq!(order([v4(), v6()], Ipv6Resolve::Required), vec![v6()]);
        assert!(order([v4()], Ipv6Resolve::Required).is_empty());
    }

    #[test]
    fn preferred_starts_with_v6_and_falls_back_to_v4() {
        assert_eq!(
            order([v4(), v6()], Ipv6Resolve::Preferred),
            vec![v6(), v4()]
        );
        assert_eq!(order([v4()], Ipv6Resolve::Preferred), vec![v4()]);
    }

    #[test]
    fn tolerated_starts_with_v4_and_falls_back_to_v6() {
        assert_eq!(
            order([v6(), v4()], Ipv6Resolve::Tolerated),
            vec![v4(), v6()]
        );
        assert_eq!(order([v6()], Ipv6Resolve::Tolerated), vec![v6()]);
    }

    #[test]
    fn forbidden_keeps_only_v4() {
        assert_eq!(order([v6(), v4()], Ipv6Resolve::Forbidden), vec![v4()]);
        assert!(order([v6()], Ipv6Resolve::Forbidden).is_empty());
    }

    #[test]
    fn families_interleave_and_repeats_go() {
        let (a4, b4, c4) = (addr("10.0.0.1:1"), addr("10.0.0.2:1"), addr("10.0.0.3:1"));
        let (a6, b6) = (addr("[2001:db8::1]:1"), addr("[2001:db8::2]:1"));
        assert_eq!(
            order([a4, b4, a6, c4, b6, a4], Ipv6Resolve::Tolerated),
            vec![a4, a6, b4, b6, c4]
        );
        assert_eq!(
            order([a4, b4, a6, c4, b6], Ipv6Resolve::Preferred),
            vec![a6, a4, b6, b4, c4]
        );
    }

    #[test]
    fn empty_result_is_empty() {
        assert!(order(std::iter::empty(), Ipv6Resolve::Tolerated).is_empty());
    }
}
