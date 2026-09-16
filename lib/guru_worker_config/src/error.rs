use super::SocketAddr;
use std::path::PathBuf;

/// Everything that can go wrong while reading, validating or writing a config.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid remote '{0}': expected host:port")]
    RemoteFormat(String),
    #[error("invalid port in remote '{0}'")]
    RemotePort(String),
    #[error("duplicate listener {addr} ({tag})")]
    DuplicateListener { addr: SocketAddr, tag: String },
    #[error("duplicate forwarding tag {0}")]
    DuplicateTag(String),
    #[error("forwarding {0} relay to tls/quic requires `sni`")]
    MissingSni(String),
    #[error("forwarding {0} has an empty load-balance group")]
    EmptyLoadBalance(String),
    #[error("forwarding {0} has `quic` tuning on something other than a quic relay")]
    QuicTuningWithoutQuic(String),
    #[error("{0}")]
    QuicTuning(String),
    #[error("{0}")]
    KeepAlive(String),
    #[error("forwarding {0} defines route id {1} more than once")]
    DuplicateRouteId(String, String),
    #[error("forwarding {0} refers to route id {1}, which it does not define")]
    UnknownRouteId(String, String),
    #[error("forwarding {0} has an empty group {1}")]
    EmptyRouteGroup(String, String),
    #[error("forwarding {0} gives {1} a weight of zero")]
    ZeroRouteWeight(String, String),
    #[error("forwarding {0} has a group cycle through {1}")]
    RouteCycle(String, String),
    #[error("read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse toml: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("serialize toml: {0}")]
    Serialize(#[from] toml::ser::Error),
}
