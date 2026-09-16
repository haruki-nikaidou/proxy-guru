//! The tree form every worker reads, for workers that do not read route tables.

use crate::compile::{Entry, Hop, HopKind};
use crate::model::{EdgeId, Route};
use guru_worker_config::{
    ConfigError, Forwarding, ForwardingTo, LoadBalanceGroup, LoadBalanceStrategy, To,
};
use smallvec::SmallVec;
use std::collections::BTreeMap;

/// The most members a weighted balance expands to.
const MAX_EXPANDED_MEMBERS: u64 = 256;

pub(crate) fn render(entry: &Entry<'_>) -> Result<Forwarding, ConfigError> {
    let to = tree(entry.route, &entry.hops).ok_or_else(|| {
        ConfigError::EmptyLoadBalance(format!("{} (a route edge has no next hop)", entry.pod.id))
    })?;
    let forwarding = Forwarding {
        tag: entry.pod.id.to_string(),
        listen: entry.listen,
        receive_proxy_protocol: entry.receive_proxy_protocol,
        listen_as: entry.listen_as.clone(),
        quic: entry.quic,
        to: To::Tree(to),
        groups: Vec::new(),
        upstreams: Vec::new(),
    };
    forwarding.validate()?;
    Ok(forwarding)
}

fn tree(route: &Route, hops: &BTreeMap<&EdgeId, Hop>) -> Option<ForwardingTo> {
    match route {
        Route::Edge(edge) => hops.get(edge).map(leaf),
        Route::Balance { members, sticky } => {
            let counts = expanded_counts(members.iter().map(|m| m.weight).collect());
            let mut expanded: SmallVec<[ForwardingTo; 4]> = SmallVec::new();
            for (member, count) in members.iter().zip(counts) {
                let subtree = tree(&member.to, hops)?;
                for _ in 0..count {
                    expanded.push(subtree.clone());
                }
            }
            Some(ForwardingTo::LoadBalance(Box::new(LoadBalanceGroup {
                strategy: if sticky.is_some() {
                    LoadBalanceStrategy::IpHash
                } else {
                    LoadBalanceStrategy::RoundRobin
                },
                members: expanded,
            })))
        }
        Route::Failover(members) => {
            let members = members
                .iter()
                .map(|member| tree(member, hops))
                .collect::<Option<SmallVec<[ForwardingTo; 4]>>>()?;
            Some(ForwardingTo::LoadBalance(Box::new(LoadBalanceGroup {
                strategy: LoadBalanceStrategy::Fallback,
                members,
            })))
        }
    }
}

fn leaf(hop: &Hop) -> ForwardingTo {
    match &hop.kind {
        HopKind::Exit {
            send_proxy_protocol,
        } => ForwardingTo::Exit {
            destination: hop.destination.clone(),
            send_proxy_protocol: *send_proxy_protocol,
        },
        HopKind::Relay {
            protocol,
            sni,
            quic,
            ..
        } => ForwardingTo::Relay {
            protocol: *protocol,
            destination: hop.destination.clone(),
            sni: sni.clone(),
            quic: *quic,
        },
    }
}

/// How many times each member of a weighted balance is repeated in a round
/// robin: the weights over their greatest common divisor, scaled down to at
/// most 256 members in all, with every member kept at least once.
pub(crate) fn expanded_counts(weights: Vec<u32>) -> Vec<u64> {
    let weights: Vec<u64> = weights.into_iter().map(|w| u64::from(w.max(1))).collect();
    let divisor = weights.iter().copied().fold(0, gcd).max(1);
    let reduced: Vec<u64> = weights
        .iter()
        .map(|w| w.checked_div(divisor).unwrap_or(1))
        .collect();
    let total = reduced.iter().copied().fold(0u64, u64::saturating_add);
    if total <= MAX_EXPANDED_MEMBERS {
        return reduced;
    }
    reduced
        .iter()
        .map(|w| {
            w.saturating_mul(MAX_EXPANDED_MEMBERS)
                .checked_div(total)
                .unwrap_or(1)
                .max(1)
        })
        .collect()
}

fn gcd(a: u64, b: u64) -> u64 {
    let (mut a, mut b) = (a, b);
    while b != 0 {
        let r = a.checked_rem(b).unwrap_or(0);
        a = b;
        b = r;
    }
    a
}

#[cfg(test)]
mod tests {
    use super::expanded_counts;

    #[test]
    fn weights_reduce_by_their_common_divisor() {
        assert_eq!(expanded_counts(vec![2, 1]), vec![2, 1]);
        assert_eq!(expanded_counts(vec![4, 2, 6]), vec![2, 1, 3]);
        assert_eq!(expanded_counts(vec![5]), vec![1]);
    }

    #[test]
    fn large_weights_scale_down_and_keep_every_member() {
        let counts = expanded_counts(vec![999, 1]);
        assert_eq!(counts, vec![255, 1]);
        let many = expanded_counts(vec![1000, 999, 3]);
        assert!(many.iter().sum::<u64>() <= 256 + 1, "{many:?}");
        assert!(many.iter().all(|c| *c >= 1), "{many:?}");
    }
}
