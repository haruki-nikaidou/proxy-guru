use crate::ForwardingTo;
use smallvec::SmallVec;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadBalanceGroup {
    pub strategy: LoadBalanceStrategy,
    pub members: SmallVec<[ForwardingTo; 4]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LoadBalanceStrategy {
    RoundRobin,
    Random,
    IpHash,

    /// Try to connect to the first item, if it fails, try the next ones until all items are tried or success
    Fallback,
}

impl LoadBalanceGroup {
    pub fn ip_hash_somewhere(&self) -> bool {
        matches!(self.strategy, LoadBalanceStrategy::IpHash)
            || self.members.iter().any(|m| match m {
                ForwardingTo::LoadBalance(c) => c.ip_hash_somewhere(),
                _ => false,
            })
    }
    pub fn unnecessary_load_balance(&self) -> bool {
        self.members.len() == 1
    }
    pub fn empty_members(&self) -> bool {
        self.members.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::{
        Config, ConfigError, Forwarding, ForwardingTo, Ipv6Resolve, KeepAlive, ListenAs, LogConfig,
        QuicTuning, RelayHost, RelayProtocol, Remote, TcpProxyProtocol, TlsHostConfig,
    };
    use smallvec::SmallVec;
    use std::path::PathBuf;

    fn forwarding(to: ForwardingTo) -> Forwarding {
        Forwarding {
            tag: "nested".to_string(),
            listen: "203.0.113.10:443".parse().unwrap(),
            receive_proxy_protocol: None,
            listen_as: ListenAs::Raw,
            quic: None,
            to,
        }
    }

    fn relay(protocol: RelayProtocol, sni: Option<&str>) -> ForwardingTo {
        ForwardingTo::Relay {
            protocol,
            destination: Remote::parse("relay.internal:9000").unwrap(),
            sni: sni.map(str::to_string),
            quic: None,
        }
    }

    fn tls_host() -> TlsHostConfig {
        TlsHostConfig {
            key: PathBuf::from("/etc/guru/key.pem"),
            full_chain: PathBuf::from("/etc/guru/chain.pem"),
        }
    }

    #[test]
    fn config_round_trips_through_toml() {
        let cfg = Config {
            ipv6_resolve: Ipv6Resolve::Preferred,
            log: LogConfig {
                level: "debug".to_string(),
            },
            relay_ca: None,
            keepalive: KeepAlive::default(),
            quic: QuicTuning::default(),
            forwardings: vec![
                Forwarding {
                    tag: "raw-exit".to_string(),
                    listen: "203.0.113.10:443".parse().unwrap(),
                    receive_proxy_protocol: Some(TcpProxyProtocol::V2),
                    listen_as: ListenAs::Raw,
                    quic: None,
                    to: ForwardingTo::Exit {
                        destination: Remote::parse("10.0.0.5:8080").unwrap(),
                        send_proxy_protocol: Some(TcpProxyProtocol::V1),
                    },
                },
                Forwarding {
                    tag: "tls-relay".to_string(),
                    listen: "203.0.113.10:8443".parse().unwrap(),
                    receive_proxy_protocol: None,
                    listen_as: ListenAs::Tls(tls_host()),
                    quic: None,
                    to: ForwardingTo::Relay {
                        protocol: RelayProtocol::TlsOverTcp,
                        destination: Remote::parse("relay.internal:9000").unwrap(),
                        sni: Some("relay.example.com".to_string()),
                        quic: None,
                    },
                },
                Forwarding {
                    tag: "quic-lb".to_string(),
                    listen: "203.0.113.10:9443".parse().unwrap(),
                    receive_proxy_protocol: None,
                    listen_as: ListenAs::Relay(RelayHost::Quic(tls_host())),
                    quic: None,
                    to: ForwardingTo::LoadBalance(Box::new(LoadBalanceGroup {
                        strategy: LoadBalanceStrategy::Fallback,
                        members: smallvec::smallvec![
                            ForwardingTo::Exit {
                                destination: Remote::parse("10.0.0.6:8080").unwrap(),
                                send_proxy_protocol: None,
                            },
                            ForwardingTo::LoadBalance(Box::new(LoadBalanceGroup {
                                strategy: LoadBalanceStrategy::RoundRobin,
                                members: smallvec::smallvec![
                                    ForwardingTo::Exit {
                                        destination: Remote::parse("[2001:db8::1]:8080").unwrap(),
                                        send_proxy_protocol: None,
                                    },
                                    ForwardingTo::Exit {
                                        destination: Remote::parse("backend.internal:8080")
                                            .unwrap(),
                                        send_proxy_protocol: Some(TcpProxyProtocol::V2),
                                    },
                                ],
                            })),
                        ],
                    })),
                },
            ],
        };

        let text = cfg.to_toml_string().unwrap();
        let parsed = Config::from_toml_str(&text).unwrap();
        assert_eq!(parsed, cfg, "round-trip mismatch; emitted:\n{text}");
    }

    fn lb(strategy: LoadBalanceStrategy, members: SmallVec<[ForwardingTo; 4]>) -> ForwardingTo {
        ForwardingTo::LoadBalance(Box::new(LoadBalanceGroup { strategy, members }))
    }

    #[test]
    fn nested_tls_relay_without_sni_is_rejected() {
        let one_level = forwarding(lb(
            LoadBalanceStrategy::RoundRobin,
            smallvec::smallvec![relay(RelayProtocol::TlsOverTcp, None)],
        ));
        assert!(matches!(
            one_level.validate(),
            Err(ConfigError::MissingSni(tag)) if tag == "nested"
        ));

        let two_levels = forwarding(lb(
            LoadBalanceStrategy::Fallback,
            smallvec::smallvec![
                relay(RelayProtocol::Tcp, None),
                lb(
                    LoadBalanceStrategy::RoundRobin,
                    smallvec::smallvec![
                        relay(RelayProtocol::TlsOverTcp, Some("ok.example.com")),
                        relay(RelayProtocol::Quic, None),
                    ],
                ),
            ],
        ));
        assert!(matches!(
            two_levels.validate(),
            Err(ConfigError::MissingSni(tag)) if tag == "nested"
        ));
    }

    #[test]
    fn nested_relays_with_sni_are_accepted() {
        let ok = forwarding(lb(
            LoadBalanceStrategy::Fallback,
            smallvec::smallvec![lb(
                LoadBalanceStrategy::RoundRobin,
                smallvec::smallvec![
                    relay(RelayProtocol::Quic, Some("a.example.com")),
                    relay(RelayProtocol::Tcp, None),
                ],
            )],
        ));
        assert!(ok.validate().is_ok());
    }
}
