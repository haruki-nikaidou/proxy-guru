//! The remote-shell relay: the Redis pub/sub contract between the replica that
//! serves a dashboard call and the replica that holds the worker's
//! `ShellChannel`.
//!
//! Like [`crate::events::live`], these are *not* AMQP messages. A shell request
//! is only worth delivering while the dashboard call that sent it still waits
//! for the answer, and a transcript event only while somebody watches; the
//! worker owns every session and its output buffer, so a lost message costs a
//! retry (`UNAVAILABLE`) or a re-attach, never state. That is pub/sub's bargain,
//! not a queue's.
//!
//! The payloads carry the worker protocol's own messages, prost-encoded, inside
//! an rkyv envelope: the relay hands them through without reading more of them
//! than it routes by.

use kanau::{RkyvMessageDe, RkyvMessageSer};

/// The one channel every `workers_grpc` replica subscribes to. A replica that
/// does not hold the named server's stream drops the message.
pub const SHELL_DOWN_CHANNEL: &str = "guru:orchestration:shell:down";

/// Every `dashboard_grpc` replica subscribes to its own channel under this
/// prefix, named by its random replica id ([`shell_up_channel`]).
pub const SHELL_UP_CHANNEL_PREFIX: &str = "guru:orchestration:shell:up:";

/// The up channel of one dashboard replica.
pub fn shell_up_channel(replica: &str) -> String {
    format!("{SHELL_UP_CHANNEL_PREFIX}{replica}")
}

/// The replica a request or watch id belongs to: every id a dashboard replica
/// hands down is `<replica>:<random>`, so the answer finds its way back without
/// the worker-facing side keeping any state. `None` for an id no replica made.
pub fn replica_of(id: &str) -> Option<&str> {
    id.split_once(':')
        .map(|(replica, _)| replica)
        .filter(|replica| !replica.is_empty())
}

/// **Shell down**
///
/// Published by: [`crate::services::shell::ShellService`] on the replica serving
/// the dashboard call (requests, attaches, renewals, detaches).
/// Consumed by: [`crate::hooks::shell::run_shell_down_subscriber`] on every
/// `workers_grpc` replica; the one holding `server_id`'s `ShellChannel` forwards
/// it ([`crate::services::shell_channel::ForwardShellDown`]).
/// Route: Redis channel [`SHELL_DOWN_CHANNEL`].
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct ShellDownMessage {
    /// The server whose worker the message is for, as text.
    pub server_id: String,
    /// A prost-encoded `guru.orchestration.agent.ShellDown`.
    pub down: Vec<u8>,
}

/// **Shell up**
///
/// Published by: the `ShellChannel` handler, through
/// [`crate::services::shell_channel::ShellUpPublisher`], on the replica holding
/// the worker's stream, for every reply and watch event the worker sends.
/// Consumed by: [`crate::hooks::shell::run_shell_up_subscriber`] on the
/// dashboard replica the id names, which routes it to the waiting call or watch
/// through [`crate::hooks::shell::ShellRouter`].
/// Route: Redis channel [`shell_up_channel`] of the replica in the id's prefix
/// ([`replica_of`]).
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct ShellUpMessage {
    /// The server whose authenticated stream carried the message, as text. The
    /// router only hands an answer to a call or watch made for this server, so
    /// one worker cannot answer for another by guessing an id.
    pub server_id: String,
    /// A prost-encoded `guru.orchestration.agent.ShellUp` (a reply or an event).
    pub up: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_names_its_replica_by_prefix() {
        assert_eq!(replica_of("abc123:42"), Some("abc123"));
        assert_eq!(replica_of("abc123:42:7"), Some("abc123"));
        assert_eq!(replica_of(":42"), None);
        assert_eq!(replica_of("plain"), None);
        assert_eq!(
            shell_up_channel("abc123"),
            "guru:orchestration:shell:up:abc123"
        );
    }
}
