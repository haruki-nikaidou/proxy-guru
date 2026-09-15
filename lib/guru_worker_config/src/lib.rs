#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::arithmetic_side_effects)]

//! The `guru-worker` data-plane configuration model.
//!
//! Both sides of the control plane share this crate: `guru-worker` deserializes the
//! TOML it receives (or loads from disk) into [`Config`], and `guru-master` derives a
//! [`Config`] from a canvas and serializes it back to TOML.

pub mod error;
pub mod load_balance;

use compact_str::CompactString;
pub use error::ConfigError;
pub use load_balance::{LoadBalanceGroup, LoadBalanceStrategy};
use serde::Deserializer;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Forwarding {
    pub tag: String,
    pub listen: SocketAddr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receive_proxy_protocol: Option<TcpProxyProtocol>,
    pub listen_as: ListenAs,
    pub to: ForwardingTo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Remote {
    Domain(CompactString, u16),
    Address(SocketAddr),
}

impl<'de> serde::Deserialize<'de> for Remote {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = CompactString::deserialize(deserializer)?;
        Remote::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for Remote {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Remote::Domain(host, port) => s.collect_str(&format_args!("{host}:{port}")),
            Remote::Address(addr) => s.collect_str(addr),
        }
    }
}

impl Remote {
    /// Parses a `host:port` string. If the whole string parses as a `SocketAddr`
    /// (IPv4, or bracketed IPv6 like `[::1]:443`) it becomes `Address`; otherwise the
    /// text after the final `:` is the port and the rest is a domain name.
    pub fn parse(s: &str) -> Result<Remote, ConfigError> {
        if let Ok(addr) = s.parse::<SocketAddr>() {
            return Ok(Remote::Address(addr));
        }
        let (host, port) = s
            .rsplit_once(':')
            .ok_or_else(|| ConfigError::RemoteFormat(s.to_string()))?;
        if host.is_empty() {
            return Err(ConfigError::RemoteFormat(s.to_string()));
        }
        let port: u16 = port
            .parse()
            .map_err(|_| ConfigError::RemotePort(s.to_string()))?;
        Ok(Remote::Domain(CompactString::new(host), port))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TcpProxyProtocol {
    #[serde(rename = "v1")]
    V1,
    #[serde(rename = "v2")]
    V2,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsHostConfig {
    pub key: PathBuf,
    pub full_chain: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ListenAs {
    Raw,
    Tls(TlsHostConfig),
    Relay(RelayHost),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "relay_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RelayHost {
    #[serde(rename = "tcp")]
    Tcp,
    #[serde(rename = "tls")]
    TlsOverTcp(TlsHostConfig),
    #[serde(rename = "quic")]
    Quic(TlsHostConfig),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ForwardingTo {
    Exit {
        destination: Remote,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        send_proxy_protocol: Option<TcpProxyProtocol>,
    },
    Relay {
        protocol: RelayProtocol,
        destination: Remote,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sni: Option<String>,
    },
    LoadBalance(Box<LoadBalanceGroup>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RelayProtocol {
    #[serde(rename = "tcp")]
    Tcp,
    #[serde(rename = "tls")]
    TlsOverTcp,
    #[serde(rename = "quic")]
    Quic,
}

impl Forwarding {
    fn suspicious_ip_hash(&self) -> Option<String> {
        if self.receive_proxy_protocol.is_some() {
            return None;
        }
        let sus = match &self.to {
            ForwardingTo::Exit { .. } => false,
            ForwardingTo::Relay { .. } => false,
            ForwardingTo::LoadBalance(c) => c.ip_hash_somewhere(),
        };
        sus.then(|| {
            format!(
                "forward role {} doesn't enable proxy protocol but used ip_hash for load balancing",
                self.tag
            )
        })
    }
    fn unnecessary_load_balance(&self) -> Option<String> {
        match &self.to {
            ForwardingTo::LoadBalance(c) if c.unnecessary_load_balance() => Some(format!(
                "forward role {} has only one member in load balance group, unnecessary load balance",
                self.tag
            )),
            _ => None,
        }
    }
    fn empty_load_balance(&self) -> Option<String> {
        match &self.to {
            ForwardingTo::LoadBalance(c) if c.empty_members() => Some(format!(
                "forward role {} has no member in load balance group",
                self.tag
            )),
            _ => None,
        }
    }
    /// Warnings worth showing an operator. Never fatal: [`Config::validate`] owns
    /// the hard errors, and callers decide whether and how to report these.
    pub fn lint(&self) -> Vec<String> {
        if let Some(empty) = self.empty_load_balance() {
            return vec![empty];
        }
        self.suspicious_ip_hash()
            .into_iter()
            .chain(self.unnecessary_load_balance())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Transport {
    Tcp,
    Quic,
}

impl Forwarding {
    pub fn transport(&self) -> Transport {
        match &self.listen_as {
            ListenAs::Relay(RelayHost::Quic(_)) => Transport::Quic,
            _ => Transport::Tcp,
        }
    }
    pub fn listen_key(&self) -> (SocketAddr, Transport) {
        (self.listen, self.transport())
    }
}

/// Global policy for choosing between IPv6 and IPv4 addresses when resolving a domain
/// destination, captured into each compiled target. `Tolerated` is the default (prefer
/// IPv4, fall back to IPv6).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Ipv6Resolve {
    /// Only accept IPv6 results; if none exist, treat as a resolution failure.
    Required,
    /// When both families resolve, choose IPv6; otherwise fall back to IPv4.
    Preferred,
    /// When both families resolve, choose IPv4; otherwise fall back to IPv6.
    #[default]
    Tolerated,
    /// Only accept IPv4 results; if none exist, treat as a resolution failure.
    Forbidden,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub ipv6_resolve: Ipv6Resolve,
    #[serde(default)]
    pub log: LogConfig,
    /// The CA certificate (PEM) relay TLS/QUIC dialers verify peers against.
    /// Unset means the system roots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_ca: Option<PathBuf>,
    /// Liveness probing on every data-plane connection. Written out only when it
    /// differs from the defaults: every struct here rejects unknown keys, so a
    /// master must not send the section to a worker built before it existed.
    #[serde(default, skip_serializing_if = "KeepAlive::is_default")]
    pub keepalive: KeepAlive,
    #[serde(rename = "forwarding", default)]
    pub forwardings: Vec<Forwarding>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogConfig {
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}

/// How a worker tells a quiet peer from a vanished one.
///
/// TCP has no liveness of its own: a peer that disappears without a FIN or RST
/// (a phone off the network, a NAT entry expired, a host powered off) leaves the
/// connection open forever on this side, and with it the pipe it was spliced to.
/// `SO_KEEPALIVE` on every accepted and dialed TCP socket makes the kernel probe an
/// idle connection and fail it after `tcp_retries` unanswered probes. A QUIC relay
/// hop has the opposite problem: quinn times an idle connection out after thirty
/// seconds unless it is kept alive, cutting long connections that are merely quiet.
///
/// None of this is an idle limit: a connection whose peer answers the probes stays
/// open for as long as the peer likes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KeepAlive {
    /// Seconds a TCP connection is idle before the first probe (`TCP_KEEPIDLE`).
    pub tcp_idle_secs: u32,
    /// Seconds between probes once probing has started (`TCP_KEEPINTVL`).
    pub tcp_interval_secs: u32,
    /// Unanswered probes after which the connection is failed (`TCP_KEEPCNT`).
    pub tcp_retries: u32,
    /// Seconds between QUIC keep-alive pings on an idle relay connection.
    pub quic_ping_secs: u32,
    /// Seconds without any packet after which a QUIC relay connection is
    /// considered lost; must exceed `quic_ping_secs`.
    pub quic_idle_secs: u32,
}

impl Default for KeepAlive {
    fn default() -> Self {
        Self {
            tcp_idle_secs: 60,
            tcp_interval_secs: 10,
            tcp_retries: 3,
            quic_ping_secs: 15,
            quic_idle_secs: 60,
        }
    }
}

impl KeepAlive {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }

    /// Every value must be a whole positive second — the kernel and quinn both take
    /// zero as "disabled", which would silently bring the leak back — and a QUIC
    /// connection has to get a ping in before its idle timeout fires.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let fields = [
            ("tcp_idle_secs", self.tcp_idle_secs),
            ("tcp_interval_secs", self.tcp_interval_secs),
            ("tcp_retries", self.tcp_retries),
            ("quic_ping_secs", self.quic_ping_secs),
            ("quic_idle_secs", self.quic_idle_secs),
        ];
        if let Some((name, _)) = fields.iter().find(|(_, value)| *value == 0) {
            return Err(ConfigError::KeepAlive(format!(
                "keepalive.{name} must be at least 1"
            )));
        }
        if self.quic_idle_secs <= self.quic_ping_secs {
            return Err(ConfigError::KeepAlive(format!(
                "keepalive.quic_idle_secs ({}) must exceed quic_ping_secs ({})",
                self.quic_idle_secs, self.quic_ping_secs
            )));
        }
        Ok(())
    }
}

impl Config {
    /// Parses TOML text and validates it. Linting is separate: call [`Config::lint`]
    /// when the caller is in a position to report warnings to an operator.
    pub fn from_toml_str(text: &str) -> Result<Config, ConfigError> {
        let cfg: Config = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Non-fatal warnings about this config, in forwarding order.
    pub fn lint(&self) -> Vec<String> {
        self.forwardings.iter().flat_map(Forwarding::lint).collect()
    }

    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Config::from_toml_str(&text)
    }

    pub fn to_toml_string(&self) -> Result<String, ConfigError> {
        Ok(toml::to_string(self)?)
    }

    /// Anchors every relative certificate path (`relay_ca`, and each listener's
    /// `key` / `full_chain`) at `base`. A master-delivered config names its files
    /// relative to the worker's state directory; a hand-written one may use
    /// absolute paths, which are left alone.
    pub fn resolve_paths(&mut self, base: &Path) {
        fn anchor(path: &mut PathBuf, base: &Path) {
            if path.is_relative() {
                *path = base.join(&*path);
            }
        }
        fn anchor_host(host: &mut TlsHostConfig, base: &Path) {
            anchor(&mut host.key, base);
            anchor(&mut host.full_chain, base);
        }
        if let Some(ca) = &mut self.relay_ca {
            anchor(ca, base);
        }
        for f in &mut self.forwardings {
            match &mut f.listen_as {
                ListenAs::Raw | ListenAs::Relay(RelayHost::Tcp) => {}
                ListenAs::Tls(host)
                | ListenAs::Relay(RelayHost::TlsOverTcp(host) | RelayHost::Quic(host)) => {
                    anchor_host(host, base);
                }
            }
        }
    }

    /// Both the per-entry rules and the two cross-entry rules: no two forwardings
    /// may claim the same socket, and no two may share a tag — the tag is how a
    /// worker tells its listeners apart and how it reports on each of them.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.keepalive.validate()?;
        let mut seen = HashSet::new();
        let mut tags = HashSet::new();
        for f in &self.forwardings {
            if !seen.insert(f.listen_key()) {
                return Err(ConfigError::DuplicateListener {
                    addr: f.listen,
                    tag: f.tag.clone(),
                });
            }
            if !tags.insert(f.tag.as_str()) {
                return Err(ConfigError::DuplicateTag(f.tag.clone()));
            }
            f.validate()?;
        }
        Ok(())
    }
}

impl Forwarding {
    /// Everything that concerns this entry alone.
    ///
    /// Separate from [`Config::validate`] so a caller deriving a config entry by
    /// entry can attribute a failure to the entry that caused it instead of
    /// rejecting the whole file.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_to(&self.to, &self.tag)
    }
}

/// The per-destination rules, applied to every node of the [`ForwardingTo`] tree:
/// a relay over tls/quic needs an `sni`, and a load-balance group needs members.
/// Both live in this one traversal so a rule cannot be enforced at the top level
/// and silently skipped for nested members.
fn validate_to(to: &ForwardingTo, tag: &str) -> Result<(), ConfigError> {
    match to {
        ForwardingTo::Exit { .. } => Ok(()),
        ForwardingTo::Relay { protocol, sni, .. } => {
            let needs = matches!(protocol, RelayProtocol::TlsOverTcp | RelayProtocol::Quic);
            if needs && sni.is_none() {
                return Err(ConfigError::MissingSni(tag.to_string()));
            }
            Ok(())
        }
        ForwardingTo::LoadBalance(g) => {
            if g.empty_members() {
                return Err(ConfigError::EmptyLoadBalance(tag.to_string()));
            }
            for m in &g.members {
                validate_to(m, tag)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn remote_parses_ipv4_socket_addr() {
        let r = Remote::parse("127.0.0.1:9000").unwrap();
        match r {
            Remote::Address(addr) => {
                assert_eq!(addr, "127.0.0.1:9000".parse::<SocketAddr>().unwrap());
            }
            other => panic!("expected Address, got {other:?}"),
        }
    }

    #[test]
    fn remote_parses_ipv6_socket_addr() {
        let r = Remote::parse("[::1]:443").unwrap();
        match r {
            Remote::Address(addr) => {
                assert_eq!(addr, "[::1]:443".parse::<SocketAddr>().unwrap());
            }
            other => panic!("expected Address, got {other:?}"),
        }
    }

    #[test]
    fn remote_parses_domain() {
        let r = Remote::parse("backend.internal:9000").unwrap();
        match r {
            Remote::Domain(host, port) => {
                assert_eq!(host, "backend.internal");
                assert_eq!(port, 9000);
            }
            other => panic!("expected Domain, got {other:?}"),
        }
    }

    #[test]
    fn remote_rejects_missing_port() {
        assert!(Remote::parse("no-port").is_err());
    }

    #[test]
    fn two_forwardings_may_not_share_a_tag() {
        let text = r#"
ipv6_resolve = "tolerated"

[log]
level = "info"

[[forwarding]]
tag = "alpha"
listen = "[::]:443"
listen_as = "raw"

[forwarding.to]
type = "exit"
destination = "10.0.0.5:8080"

[[forwarding]]
tag = "alpha"
listen = "[::]:8443"
listen_as = "raw"

[forwarding.to]
type = "exit"
destination = "10.0.0.5:8080"
"#;
        match Config::from_toml_str(text) {
            Err(ConfigError::DuplicateTag(tag)) => assert_eq!(tag, "alpha"),
            other => panic!("expected a duplicate tag, got {other:?}"),
        }
    }

    #[test]
    fn keepalive_defaults_apply_and_are_not_written_out() {
        let cfg = Config::from_toml_str("").unwrap();
        assert_eq!(cfg.keepalive, KeepAlive::default());
        let text = cfg.to_toml_string().unwrap();
        assert!(
            !text.contains("keepalive"),
            "a default section would be rejected by workers that predate it: {text}"
        );

        let mut tuned = cfg.clone();
        tuned.keepalive.quic_ping_secs = 5;
        let text = tuned.to_toml_string().unwrap();
        assert!(
            text.contains("[keepalive]"),
            "a tuned section is written: {text}"
        );
        assert_eq!(Config::from_toml_str(&text).unwrap(), tuned);
    }

    #[test]
    fn keepalive_rejects_zero_and_an_idle_timeout_the_ping_cannot_beat() {
        let zero = "[keepalive]\ntcp_retries = 0\n";
        match Config::from_toml_str(zero) {
            Err(ConfigError::KeepAlive(msg)) => assert!(msg.contains("tcp_retries"), "{msg}"),
            other => panic!("expected a keepalive error, got {other:?}"),
        }
        let late = "[keepalive]\nquic_ping_secs = 30\nquic_idle_secs = 30\n";
        match Config::from_toml_str(late) {
            Err(ConfigError::KeepAlive(msg)) => assert!(msg.contains("quic_idle_secs"), "{msg}"),
            other => panic!("expected a keepalive error, got {other:?}"),
        }
        assert!(
            Config::from_toml_str("[keepalive]\nquic_ping_secs = 1\nquic_idle_secs = 2\n").is_ok()
        );
    }

    #[test]
    fn remote_rejects_bad_port() {
        assert!(Remote::parse("host:notaport").is_err());
    }
}
