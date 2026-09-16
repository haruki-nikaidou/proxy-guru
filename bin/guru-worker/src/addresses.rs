//! What this host knows about its own addresses, for the master.
//!
//! The master needs an address other servers can dial. Nothing on the worker
//! can know that for certain (NAT, overlays), so it reports everything it can
//! learn cheaply — the public address seen from the internet for each family,
//! and the interface addresses — and the master (or the operator) picks.
//!
//! Discovery is best effort by construction: every lookup is bounded by
//! [`LOOKUP_TIMEOUT`], a failure is a `None` plus a warning, and nothing here can
//! delay registration by more than the timeout.

use rpguru_sdk::orchestration_agent::ReportedAddresses;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Per-lookup budget: a provider that does not answer within this is skipped.
pub const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
/// How often a running session re-checks its addresses. Public IPs do change
/// (dynamic allocations, failover), and a stale one strands every relay that
/// dials this server, so the check is frequent; the report is only sent when
/// something changed.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(60);
/// Providers that answer `GET /` with the caller's address as plain text, one
/// list per family so a dual-stack provider cannot answer the wrong question.
/// Each lookup starts at a rotating offset and walks the list until a provider
/// answers, so no single one sees the full cadence.
pub const DEFAULT_PUBLIC_IPV4_URLS: &str =
    "https://checkip.amazonaws.com,https://api.ipify.org,https://ipv4.icanhazip.com";
pub const DEFAULT_PUBLIC_IPV6_URLS: &str =
    "https://ipv6.icanhazip.com,https://api6.ipify.org,https://v6.ipinfo.io/ip";

/// Where discovery looks; every field may be empty to disable that lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sources {
    pub ipv4_urls: String,
    pub ipv6_urls: String,
}

impl Sources {
    /// Nothing is looked up; only interface addresses are reported.
    pub fn none() -> Self {
        Self {
            ipv4_urls: String::new(),
            ipv6_urls: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Discovered {
    pub public_v4: Option<Ipv4Addr>,
    pub public_v6: Option<Ipv6Addr>,
    /// Non-loopback, non-link-local interface addresses, sorted, deduplicated.
    pub interfaces: Vec<IpAddr>,
}

impl Discovered {
    pub fn to_proto(&self) -> ReportedAddresses {
        ReportedAddresses {
            public_v4: self.public_v4.map(|a| a.to_string()).unwrap_or_default(),
            public_v6: self.public_v6.map(|a| a.to_string()).unwrap_or_default(),
            interfaces: self.interfaces.iter().map(ToString::to_string).collect(),
        }
    }
}

/// The round-robin cursor over the provider list, shared by every lookup of
/// the process so consecutive refreshes rotate providers.
static CURSOR: AtomicUsize = AtomicUsize::new(0);

fn split_urls(urls: &str) -> Vec<&str> {
    urls.split(',')
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .collect()
}

/// Learns this host's addresses from `sources`; an empty list or URL disables
/// that lookup (interfaces are always reported).
pub async fn discover(sources: &Sources) -> Discovered {
    let v4 = split_urls(&sources.ipv4_urls);
    let v6 = split_urls(&sources.ipv6_urls);
    let start = CURSOR.fetch_add(1, Ordering::Relaxed);
    let (public_v4, public_v6) = tokio::join!(
        first_answer(&v4, start, IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        first_answer(&v6, start, IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
    );
    let public_v4 = match public_v4 {
        Some(IpAddr::V4(a)) => Some(a),
        _ => None,
    };
    let public_v6 = match public_v6 {
        Some(IpAddr::V6(a)) => Some(a),
        _ => None,
    };
    Discovered {
        public_v4,
        public_v6,
        interfaces: interfaces(),
    }
}

/// Walks the providers from a rotating start until one answers for `family`.
async fn first_answer(providers: &[&str], start: usize, family: IpAddr) -> Option<IpAddr> {
    for offset in 0..providers.len() {
        let index = start
            .wrapping_add(offset)
            .checked_rem(providers.len())
            .unwrap_or(0);
        let Some(url) = providers.get(index) else {
            break;
        };
        if let Some(found) = public_ip(url, family).await {
            return Some(found);
        }
    }
    None
}

/// One provider, one address family: the socket is bound to the family's
/// unspecified address so a dual-stack host cannot answer the IPv4 question
/// with its IPv6 address.
async fn public_ip(url: &str, family: IpAddr) -> Option<IpAddr> {
    let client = reqwest::Client::builder()
        .local_address(Some(family))
        .timeout(LOOKUP_TIMEOUT)
        .connect_timeout(LOOKUP_TIMEOUT)
        .user_agent("guru-worker")
        .build()
        .ok()?;
    let family_name = if family.is_ipv4() { "v4" } else { "v6" };
    let body = match client.get(url).send().await {
        Ok(response) => match response.error_for_status() {
            Ok(response) => response.text().await.ok()?,
            Err(error) => {
                tracing::warn!(url, family = family_name, error = %error, "public ip lookup rejected");
                return None;
            }
        },
        Err(error) => {
            // No route for the family (a v4-only host asking over v6) is the
            // common case and not worth more than debug.
            tracing::debug!(url, family = family_name, error = %error, "public ip lookup failed");
            return None;
        }
    };
    let parsed = body.trim().parse::<IpAddr>().ok()?;
    let parsed = parsed.to_canonical();
    // The provider answered over the family we asked for, so a mismatch means
    // a proxy or a captive portal answered instead: not this host's address.
    (parsed.is_ipv4() == family.is_ipv4()).then_some(parsed)
}

/// The host's interface addresses that a peer could plausibly dial: loopback,
/// link-local and unspecified addresses are dropped.
pub fn interfaces() -> Vec<IpAddr> {
    let mut out: Vec<IpAddr> = match if_addrs::get_if_addrs() {
        Ok(list) => list
            .into_iter()
            .map(|iface| iface.ip().to_canonical())
            .filter(|ip| !ip.is_loopback() && !ip.is_unspecified() && !is_link_local(ip))
            .collect(),
        Err(error) => {
            tracing::warn!(error = %error, "listing interface addresses failed");
            Vec::new()
        }
    };
    out.sort();
    out.dedup();
    out
}

fn is_link_local(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfe80,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interfaces_exclude_loopback_and_link_local() {
        for ip in interfaces() {
            assert!(!ip.is_loopback(), "{ip}");
            assert!(!ip.is_unspecified(), "{ip}");
            assert!(!is_link_local(&ip), "{ip}");
        }
    }

    #[tokio::test]
    async fn empty_sources_skip_every_lookup() {
        let started = std::time::Instant::now();
        let found = discover(&Sources::none()).await;
        assert!(found.public_v4.is_none() && found.public_v6.is_none());
        assert!(started.elapsed() < LOOKUP_TIMEOUT);
    }

    #[test]
    fn proto_uses_empty_strings_for_unknown() {
        let proto = Discovered::default().to_proto();
        assert_eq!(proto.public_v4, "");
        assert_eq!(proto.public_v6, "");
        assert!(proto.interfaces.is_empty());
    }
}
