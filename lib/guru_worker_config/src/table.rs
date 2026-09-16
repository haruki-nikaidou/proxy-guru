//! The route-table form of a forwarding.
//!
//! A [`crate::Forwarding`] carries its destination as one inline tree. A
//! [`TableForwarding`] names its pieces instead: every next hop is an
//! [`Upstream`], every choice between next hops is a [`Group`], and `to` is the
//! id traffic starts at. A group lists members by id, so failover over load
//! balancers, or a load balancer over failovers, is just a group whose members
//! are groups.
//!
//! ```toml
//! [[forwarding]]
//! tag = "p"
//! listen = "[::]:443"
//! listen_as = "raw"
//! to = "g"
//!
//! [[forwarding.group]]
//! id = "g"
//! failover = ["g.0", "u:x"]
//!
//! [[forwarding.group]]
//! id = "g.0"
//! balance = [{ to = "u:a", weight = 2 }, { to = "u:b" }]
//! sticky = "client_ip"
//!
//! [[forwarding.upstream]]
//! id = "u:a"
//! relay = { protocol = "quic", destination = "203.0.113.1:40000", sni = "a.relay.guru.internal", confirm = true }
//! ```
//!
//! No worker reads this form yet; the master produces it through
//! `guru_topology` and still sends [`crate::Forwarding`] until workers do.

use crate::{
    ConfigError, ListenAs, QuicTuning, RelayHost, RelayProtocol, Remote, TcpProxyProtocol,
    Transport,
};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableForwarding {
    pub tag: String,
    pub listen: SocketAddr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receive_proxy_protocol: Option<TcpProxyProtocol>,
    pub listen_as: ListenAs,
    /// Transport tuning for a `quic` relay listener, as on [`crate::Forwarding`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quic: Option<QuicTuning>,
    /// The group or upstream every accepted connection starts at.
    pub to: String,
    #[serde(rename = "group", default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<Group>,
    #[serde(rename = "upstream", default, skip_serializing_if = "Vec::is_empty")]
    pub upstreams: Vec<Upstream>,
}

/// A choice between members, each the id of a group or an upstream.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "RawGroup", into = "RawGroup")]
pub struct Group {
    pub id: String,
    pub policy: Policy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Policy {
    /// Spread connections over the members that are alive, in proportion to
    /// their weights.
    Balance {
        members: Vec<Weighted>,
        sticky: Option<Sticky>,
    },
    /// The first member that is alive, in order.
    Failover { members: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Weighted {
    pub to: String,
    #[serde(default = "one", skip_serializing_if = "is_one")]
    pub weight: u32,
}

fn one() -> u32 {
    1
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_one(weight: &u32) -> bool {
    *weight == 1
}

/// What keeps a client on the same member while that member is alive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sticky {
    ClientIp,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGroup {
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    balance: Option<Vec<Weighted>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sticky: Option<Sticky>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failover: Option<Vec<String>>,
}

impl TryFrom<RawGroup> for Group {
    type Error = String;

    fn try_from(raw: RawGroup) -> Result<Self, Self::Error> {
        let policy = match (raw.balance, raw.failover) {
            (Some(members), None) => Policy::Balance {
                members,
                sticky: raw.sticky,
            },
            (None, Some(members)) if raw.sticky.is_none() => Policy::Failover { members },
            (None, Some(_)) => {
                return Err(format!(
                    "group {}: `sticky` only applies to `balance`",
                    raw.id
                ));
            }
            _ => {
                return Err(format!(
                    "group {}: exactly one of `balance` and `failover`",
                    raw.id
                ));
            }
        };
        Ok(Self { id: raw.id, policy })
    }
}

impl From<Group> for RawGroup {
    fn from(group: Group) -> Self {
        match group.policy {
            Policy::Balance { members, sticky } => RawGroup {
                id: group.id,
                balance: Some(members),
                sticky,
                failover: None,
            },
            Policy::Failover { members } => RawGroup {
                id: group.id,
                balance: None,
                sticky: None,
                failover: Some(members),
            },
        }
    }
}

/// One next hop.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "RawUpstream", into = "RawUpstream")]
pub struct Upstream {
    pub id: String,
    pub target: Target,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Target {
    /// Traffic leaves the fabric here.
    Exit(ExitTarget),
    /// Another worker's relay listener.
    Relay(RelayTarget),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExitTarget {
    pub destination: Remote,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_proxy_protocol: Option<TcpProxyProtocol>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayTarget {
    pub protocol: RelayProtocol,
    pub destination: Remote,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sni: Option<String>,
    /// The dialing side of a `quic` hop, as on [`crate::ForwardingTo::Relay`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quic: Option<QuicTuning>,
    /// Ask the listener to confirm that its own next hop answered before the
    /// hop counts as connected. Only set toward a worker that understands it.
    #[serde(default, skip_serializing_if = "is_false")]
    pub confirm: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUpstream {
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exit: Option<ExitTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    relay: Option<RelayTarget>,
}

impl TryFrom<RawUpstream> for Upstream {
    type Error = String;

    fn try_from(raw: RawUpstream) -> Result<Self, Self::Error> {
        let target = match (raw.exit, raw.relay) {
            (Some(exit), None) => Target::Exit(exit),
            (None, Some(relay)) => Target::Relay(relay),
            _ => {
                return Err(format!(
                    "upstream {}: exactly one of `exit` and `relay`",
                    raw.id
                ));
            }
        };
        Ok(Self { id: raw.id, target })
    }
}

impl From<Upstream> for RawUpstream {
    fn from(upstream: Upstream) -> Self {
        let (exit, relay) = match upstream.target {
            Target::Exit(exit) => (Some(exit), None),
            Target::Relay(relay) => (None, Some(relay)),
        };
        RawUpstream {
            id: upstream.id,
            exit,
            relay,
        }
    }
}

impl TableForwarding {
    pub fn transport(&self) -> Transport {
        match &self.listen_as {
            ListenAs::Relay(RelayHost::Quic(_)) => Transport::Quic,
            _ => Transport::Tcp,
        }
    }

    pub fn listen_key(&self) -> (SocketAddr, Transport) {
        (self.listen, self.transport())
    }

    /// Everything that concerns this entry alone: ids are unique and resolve,
    /// groups are non-empty with positive weights and never reach themselves,
    /// and each upstream is well formed.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let tag = self.tag.as_str();
        if let Some(quic) = &self.quic {
            if self.transport() != Transport::Quic {
                return Err(ConfigError::QuicTuningWithoutQuic(self.tag.clone()));
            }
            quic.validate(&format!("forwarding {tag}"))?;
        }

        let mut ids = HashSet::new();
        let ids_of_groups = self.groups.iter().map(|g| g.id.as_str());
        let ids_of_upstreams = self.upstreams.iter().map(|u| u.id.as_str());
        for id in ids_of_groups.chain(ids_of_upstreams) {
            if !ids.insert(id) {
                return Err(route_error(ConfigError::DuplicateRouteId, tag, id));
            }
        }
        if !ids.contains(self.to.as_str()) {
            return Err(route_error(ConfigError::UnknownRouteId, tag, &self.to));
        }

        let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
        for group in &self.groups {
            let members: Vec<&str> = match &group.policy {
                Policy::Balance { members, .. } => {
                    if let Some(zero) = members.iter().find(|m| m.weight == 0) {
                        return Err(route_error(ConfigError::ZeroRouteWeight, tag, &zero.to));
                    }
                    members.iter().map(|m| m.to.as_str()).collect()
                }
                Policy::Failover { members } => members.iter().map(String::as_str).collect(),
            };
            if members.is_empty() {
                return Err(route_error(ConfigError::EmptyRouteGroup, tag, &group.id));
            }
            if let Some(unknown) = members.iter().find(|m| !ids.contains(**m)) {
                return Err(route_error(ConfigError::UnknownRouteId, tag, unknown));
            }
            children.insert(group.id.as_str(), members);
        }
        if let Some(id) = first_cycle(&children) {
            return Err(route_error(ConfigError::RouteCycle, tag, id));
        }

        for upstream in &self.upstreams {
            if let Target::Relay(relay) = &upstream.target {
                let needs_sni = matches!(
                    relay.protocol,
                    RelayProtocol::TlsOverTcp | RelayProtocol::Quic
                );
                if needs_sni && relay.sni.is_none() {
                    return Err(ConfigError::MissingSni(self.tag.clone()));
                }
                if let Some(quic) = &relay.quic {
                    if relay.protocol != RelayProtocol::Quic {
                        return Err(ConfigError::QuicTuningWithoutQuic(self.tag.clone()));
                    }
                    quic.validate(&format!("forwarding {tag} upstream {}", upstream.id))?;
                }
            }
        }
        Ok(())
    }

    /// Groups and upstreams nothing reaches from `to`. Harmless, so a warning
    /// rather than an error.
    pub fn lint(&self) -> Vec<String> {
        let mut reached: HashSet<&str> = HashSet::new();
        let mut stack = vec![self.to.as_str()];
        while let Some(id) = stack.pop() {
            if !reached.insert(id) {
                continue;
            }
            if let Some(group) = self.groups.iter().find(|g| g.id == id) {
                match &group.policy {
                    Policy::Balance { members, .. } => {
                        stack.extend(members.iter().map(|m| m.to.as_str()));
                    }
                    Policy::Failover { members } => {
                        stack.extend(members.iter().map(String::as_str))
                    }
                }
            }
        }
        let groups = self.groups.iter().map(|g| g.id.as_str());
        let upstreams = self.upstreams.iter().map(|u| u.id.as_str());
        groups
            .chain(upstreams)
            .filter(|id| !reached.contains(id))
            .map(|id| format!("forwarding {} never reaches {id}", self.tag))
            .collect()
    }
}

fn route_error(variant: fn(String, String) -> ConfigError, tag: &str, id: &str) -> ConfigError {
    variant(tag.to_string(), id.to_string())
}

/// A group that reaches itself through its members, if any. Iterative, so a
/// long chain of groups cannot exhaust the stack.
fn first_cycle<'a>(children: &HashMap<&'a str, Vec<&'a str>>) -> Option<&'a str> {
    #[derive(Clone, Copy, PartialEq)]
    enum Mark {
        Open,
        Done,
    }
    let mut marks: HashMap<&str, Mark> = HashMap::new();
    let mut roots: Vec<&str> = children.keys().copied().collect();
    roots.sort_unstable();
    for root in roots {
        if marks.contains_key(root) {
            continue;
        }
        // (group, index of the next member to visit)
        let mut stack: Vec<(&str, usize)> = vec![(root, 0)];
        marks.insert(root, Mark::Open);
        while let Some(top) = stack.last_mut() {
            let group = top.0;
            let members = children.get(group).map(Vec::as_slice).unwrap_or_default();
            let Some(member) = members.get(top.1).copied() else {
                marks.insert(group, Mark::Done);
                stack.pop();
                continue;
            };
            top.1 = top.1.saturating_add(1);
            if !children.contains_key(member) {
                continue;
            }
            match marks.get(member) {
                Some(Mark::Open) => return Some(member),
                Some(Mark::Done) => {}
                None => {
                    marks.insert(member, Mark::Open);
                    stack.push((member, 0));
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TlsHostConfig;
    use std::path::PathBuf;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Doc {
        forwarding: Vec<TableForwarding>,
    }

    fn relay(id: &str, protocol: RelayProtocol) -> Upstream {
        let sni = (protocol != RelayProtocol::Tcp).then(|| format!("{id}.relay.guru.internal"));
        Upstream {
            id: id.to_string(),
            target: Target::Relay(RelayTarget {
                protocol,
                destination: Remote::parse("203.0.113.1:40000").unwrap(),
                sni,
                quic: None,
                confirm: true,
            }),
        }
    }

    fn exit(id: &str) -> Upstream {
        Upstream {
            id: id.to_string(),
            target: Target::Exit(ExitTarget {
                destination: Remote::parse("backend.internal:8080").unwrap(),
                send_proxy_protocol: Some(TcpProxyProtocol::V2),
            }),
        }
    }

    fn balance(id: &str, members: &[(&str, u32)]) -> Group {
        Group {
            id: id.to_string(),
            policy: Policy::Balance {
                members: members
                    .iter()
                    .map(|(to, weight)| Weighted {
                        to: to.to_string(),
                        weight: *weight,
                    })
                    .collect(),
                sticky: None,
            },
        }
    }

    fn failover(id: &str, members: &[&str]) -> Group {
        Group {
            id: id.to_string(),
            policy: Policy::Failover {
                members: members.iter().map(|m| m.to_string()).collect(),
            },
        }
    }

    fn entry(to: &str, groups: Vec<Group>, upstreams: Vec<Upstream>) -> TableForwarding {
        TableForwarding {
            tag: "p".to_string(),
            listen: "[::]:443".parse().unwrap(),
            receive_proxy_protocol: None,
            listen_as: ListenAs::Raw,
            quic: None,
            to: to.to_string(),
            groups,
            upstreams,
        }
    }

    #[test]
    fn a_table_round_trips_through_toml() {
        let mut sticky = balance("g.0", &[("u:a", 2), ("u:b", 1)]);
        if let Policy::Balance { sticky: s, .. } = &mut sticky.policy {
            *s = Some(Sticky::ClientIp);
        }
        let doc = Doc {
            forwarding: vec![TableForwarding {
                listen_as: ListenAs::Relay(RelayHost::Quic(TlsHostConfig {
                    key: PathBuf::from("certs/relay/p/key.pem"),
                    full_chain: PathBuf::from("certs/relay/p/full_chain.pem"),
                })),
                quic: Some(QuicTuning {
                    send_mbps: 100,
                    receive_mbps: 50,
                    ..QuicTuning::default()
                }),
                ..entry(
                    "g",
                    vec![failover("g", &["g.0", "u:x"]), sticky],
                    vec![
                        relay("u:a", RelayProtocol::Quic),
                        relay("u:b", RelayProtocol::Tcp),
                        exit("u:x"),
                    ],
                )
            }],
        };
        let text = toml::to_string(&doc).unwrap();
        let parsed: Doc = toml::from_str(&text).unwrap();
        assert_eq!(parsed, doc, "emitted:\n{text}");
        for f in &parsed.forwarding {
            f.validate().unwrap();
        }
        assert!(text.contains("weight = 2"), "{text}");
        assert!(
            !text.contains("weight = 1"),
            "a weight of one is implied: {text}"
        );
    }

    #[test]
    fn a_group_is_exactly_one_policy() {
        let both = r#"
            [[forwarding]]
            tag = "p"
            listen = "[::]:443"
            listen_as = "raw"
            to = "g"
            [[forwarding.group]]
            id = "g"
            balance = [{ to = "u" }]
            failover = ["u"]
        "#;
        let err = toml::from_str::<Doc>(both).unwrap_err().to_string();
        assert!(err.contains("exactly one of"), "{err}");

        let sticky_failover = both.replace("balance = [{ to = \"u\" }]", "sticky = \"client_ip\"");
        let err = toml::from_str::<Doc>(&sticky_failover)
            .unwrap_err()
            .to_string();
        assert!(err.contains("only applies to `balance`"), "{err}");
    }

    #[test]
    fn references_must_resolve_and_ids_must_be_unique() {
        let unknown = entry(
            "g",
            vec![balance("g", &[("u:missing", 1)])],
            vec![exit("u:x")],
        );
        assert!(matches!(
            unknown.validate(),
            Err(ConfigError::UnknownRouteId(_, id)) if id == "u:missing"
        ));

        let bad_to = entry("nowhere", vec![], vec![exit("u:x")]);
        assert!(matches!(
            bad_to.validate(),
            Err(ConfigError::UnknownRouteId(..))
        ));

        let duplicate = entry(
            "u:x",
            vec![balance("u:x", &[("u:x", 1)])],
            vec![exit("u:x")],
        );
        assert!(matches!(
            duplicate.validate(),
            Err(ConfigError::DuplicateRouteId(_, id)) if id == "u:x"
        ));
    }

    #[test]
    fn groups_need_members_and_positive_weights() {
        let empty = entry("g", vec![failover("g", &[])], vec![exit("u:x")]);
        assert!(matches!(
            empty.validate(),
            Err(ConfigError::EmptyRouteGroup(..))
        ));

        let zero = entry("g", vec![balance("g", &[("u:x", 0)])], vec![exit("u:x")]);
        assert!(matches!(
            zero.validate(),
            Err(ConfigError::ZeroRouteWeight(..))
        ));
    }

    #[test]
    fn a_group_cannot_reach_itself() {
        let cycle = entry(
            "g",
            vec![
                failover("g", &["g.0", "u:x"]),
                balance("g.0", &[("g.1", 1)]),
                failover("g.1", &["g"]),
            ],
            vec![exit("u:x")],
        );
        assert!(matches!(cycle.validate(), Err(ConfigError::RouteCycle(..))));

        let shared = entry(
            "g",
            vec![
                failover("g", &["g.0", "g.1"]),
                balance("g.0", &[("u:x", 1)]),
                balance("g.1", &[("g.0", 1)]),
            ],
            vec![exit("u:x")],
        );
        shared.validate().unwrap();
    }

    #[test]
    fn relays_follow_the_tree_form_rules() {
        let mut no_sni = relay("u:a", RelayProtocol::Quic);
        if let Target::Relay(r) = &mut no_sni.target {
            r.sni = None;
        }
        let missing = entry("u:a", vec![], vec![no_sni]);
        assert!(matches!(
            missing.validate(),
            Err(ConfigError::MissingSni(_))
        ));

        let mut tcp_tuned = relay("u:a", RelayProtocol::Tcp);
        if let Target::Relay(r) = &mut tcp_tuned.target {
            r.quic = Some(QuicTuning {
                send_mbps: 10,
                ..QuicTuning::default()
            });
        }
        let tuned = entry("u:a", vec![], vec![tcp_tuned]);
        assert!(matches!(
            tuned.validate(),
            Err(ConfigError::QuicTuningWithoutQuic(_))
        ));
    }

    #[test]
    fn unreached_entries_are_only_linted() {
        let spare = entry("u:x", vec![balance("g", &[("u:x", 1)])], vec![exit("u:x")]);
        spare.validate().unwrap();
        assert_eq!(
            spare.lint(),
            vec!["forwarding p never reaches g".to_string()]
        );
    }
}
