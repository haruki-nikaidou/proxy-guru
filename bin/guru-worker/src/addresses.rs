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
/// Providers that answer `GET /` with the caller's address as plain text.
/// Queried round-robin, so no single one sees the full cadence.
pub const DEFAULT_PUBLIC_IP_URLS: &str =
    "https://ipinfo.io/ip,https://api64.ipify.org,https://icanhazip.com";

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

/// Learns this host's addresses. `urls` is the comma-separated provider list;
/// empty disables the public lookups (interfaces are still reported).
pub async fn discover(urls: &str) -> Discovered {
    let providers: Vec<&str> = urls
        .split(',')
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .collect();
    let (public_v4, public_v6) = if providers.is_empty() {
        (None, None)
    } else {
        let index = CURSOR.fetch_add(1, Ordering::Relaxed);
        let url = providers
            .get(index.checked_rem(providers.len()).unwrap_or(0))
            .copied()
            .unwrap_or(providers[0]);
        tokio::join!(
            public_ip(url, IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
            public_ip(url, IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
        )
    };
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
    async fn empty_provider_list_skips_public_lookups() {
        let started = std::time::Instant::now();
        let found = discover("").await;
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
