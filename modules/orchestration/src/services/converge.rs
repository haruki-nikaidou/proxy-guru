//! Seamless switching: what a server may actually serve *right now*.
//!
//! Derivation ([`crate::services::derive`]) answers "what should this server serve
//! once the whole canvas has caught up". This module answers the harder question:
//! given what every other server is running at this instant, which forwardings can
//! be handed to this server without breaking a path that is still in use.
//!
//! Three rules, and they are enough:
//!
//! 1. **Never point at a listener nobody serves yet.** A forwarding whose targets
//!    are not in some server's `applied` snapshot keeps its previous shape (or is
//!    withheld if it never had one) and the server is recorded as waiting.
//! 2. **Never drop a listener somebody still points at.** A listener referenced by
//!    any other server's `desired`, `in_flight` or `applied` snapshot is kept alive
//!    from this server's own previous snapshot, even after the canvas stopped
//!    asking for it.
//! 3. **Never drop a listener because its pod stopped deriving.** A pod reported
//!    invalid by derivation keeps the shape it already had, so a half-finished edit
//!    costs an error message rather than the traffic that pod is carrying.
//!
//! Applying them on every derivation pass makes a multi-hop change converge in as
//! many passes as there are hops, with no coordinator and no ordering: each pass is
//! a pure function of the fabric's current state, so a lost message or a crashed
//! master costs a retry, never correctness.
//!
//! Rule 2 means a pod whose listener moved (a new port, or a new relay protocol
//! re-rolling a landing port) runs *two* listeners until its dependants have
//! switched. The worker tells listeners apart by tag, so the held one is renamed
//! after its socket — `osaka-hop (9443/relay_tcp)` — and drops out, name and all,
//! once nothing points at it.

use crate::config::OrchestrationConfig;
use crate::entities::db::node::NodeId;
use crate::entities::db::server::ServerId;
use crate::entities::db::topology::CanvasTopology;
use crate::entities::db::view::{
    CertificateRef, ConfigSnapshot, ForwardingDeps, InvalidPod, ListenProtocol, ListenerCap,
    ServerConfigViewEntity,
};
use crate::services::OrchestrationError;
use crate::services::derive::{
    DerivationCertificates, DerivedConfig, certificate_union, derive_server_config,
};
use crate::utils::ids::record_key;
use guru_worker_config::{Config, Forwarding};
use std::collections::HashSet;

/// One server's config as it may be rolled out right now.
#[derive(Debug, Clone)]
pub struct Converged {
    pub config: Config,
    /// Index-aligned with `config.forwardings`.
    pub forwardings: Vec<ForwardingDeps>,
    /// Servers this one is waiting on before it can adopt its ideal config.
    pub waiting_for: Vec<ServerId>,
    /// Pods derivation could not build, carried through to the config view.
    pub invalid: Vec<InvalidPod>,
    /// Every certificate `config` references: the union over `forwardings`, so
    /// a held-back previous forwarding keeps pinning the versions it was
    /// derived against.
    pub certificates: Vec<CertificateRef>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConvergeError {
    #[error(
        "listener {server}:{port} is still referenced as {old:?} and cannot become {new:?}; change the port"
    )]
    ListenerConflict {
        server: String,
        port: i64,
        old: ListenProtocol,
        new: ListenProtocol,
    },
    #[error("stored snapshot is not valid TOML: {0}")]
    Snapshot(#[from] guru_worker_config::ConfigError),
    #[error(
        "stored snapshot revision {revision} has more forwarding dependencies than the rendered TOML has entries"
    )]
    SnapshotShape { revision: i64 },
}

/// Merges a server's ideal config with what the fabric can support today. The
/// server is identified by `own.server`.
pub fn converge(
    ideal: DerivedConfig,
    own: &ServerConfigViewEntity,
    views: &[ServerConfigViewEntity],
) -> Result<Converged, ConvergeError> {
    let mut served_now: HashSet<ListenerCap> = HashSet::new();
    for view in views {
        if let Some(applied) = &view.applied {
            for deps in &applied.forwardings {
                served_now.insert(deps.serves.clone());
            }
        }
    }

    let own_key = record_key(&own.server.0);
    #[allow(clippy::mutable_key_type)]
    let mut referenced: HashSet<ListenerCap> = HashSet::new();
    for view in views {
        let is_own = record_key(&view.server.0) == own_key;
        let snapshots = [
            Some(&view.in_flight),
            Some(&view.applied),
            // A server's own previous `desired` must not pin its own listeners, or
            // a self-reference would never drain.
            (!is_own).then_some(&view.desired),
        ];
        for snapshot in snapshots.into_iter().flatten().flatten() {
            for deps in &snapshot.forwardings {
                for cap in &deps.points_at {
                    referenced.insert(cap.clone());
                }
            }
        }
    }

    let mut merged: Vec<(Forwarding, ForwardingDeps)> = Vec::new();
    let mut waiting: Vec<ServerId> = Vec::new();
    let mut waiting_keys: HashSet<String> = HashSet::new();

    for (forwarding, deps) in ideal.config.forwardings.into_iter().zip(ideal.forwardings) {
        let unready: Vec<&ListenerCap> = deps
            .points_at
            .iter()
            .filter(|cap| !served_now.contains(*cap))
            .collect();
        if unready.is_empty() {
            merged.push((forwarding, deps));
            continue;
        }
        for cap in unready {
            if waiting_keys.insert(cap.server_key()) {
                waiting.push(cap.server.clone());
            }
        }
        // Hold the previous shape of this listener until the target catches up.
        if let Some(previous) = old_forwarding_of_pod(own, &deps.pod)? {
            merged.push(previous);
        }
    }

    // Rule 3: a pod that stopped deriving keeps what it is already serving. Keyed
    // by pod, never by socket: the edit that broke the pod may also have moved it.
    for pod in &ideal.invalid {
        if let Some(previous) = old_forwarding_of_pod(own, &pod.node)? {
            merged.push(previous);
        }
    }

    // Listeners the canvas no longer asks for, but somebody still points at.
    let mut stale: Vec<&ListenerCap> = referenced
        .iter()
        .filter(|cap| {
            cap.server_key() == own_key && !merged.iter().any(|(_, deps)| &deps.serves == *cap)
        })
        .collect();
    stale.sort_by_key(|cap| (cap.port, cap.protocol as u8));
    for cap in stale {
        if let Some(previous) = old_forwarding(own, cap)? {
            merged.push(previous);
        }
    }

    // A pod may run two listeners while its dependants switch between them: the
    // one derivation asks for and the one rule 2 holds. The worker keys its
    // listeners by tag and an ack names every tag of a revision exactly once, so
    // the held entry cannot keep the pod's tag; it is renamed after its socket.
    // The next pass reads it back from the snapshot under that name (`entry_at`),
    // so the name is stable and holding it costs no revision.
    #[allow(clippy::mutable_key_type)]
    let mut sockets: HashSet<ListenerCap> = HashSet::new();
    merged.retain(|(_, deps)| sockets.insert(deps.serves.clone()));
    let mut tags: HashSet<String> = HashSet::new();
    for (forwarding, deps) in &mut merged {
        while !tags.insert(forwarding.tag.clone()) {
            forwarding.tag = held_tag(&forwarding.tag, &deps.serves);
        }
    }

    // `Transport` is not `Ord`, and a stable total order only needs the socket plus
    // a discriminant to separate two listeners that share one.
    merged.sort_by_key(|(f, _)| {
        (
            f.listen,
            f.transport() == guru_worker_config::Transport::Quic,
        )
    });
    for pair in merged.windows(2) {
        let (a, b) = (&pair[0].0, &pair[1].0);
        if a.listen_key() == b.listen_key() {
            return Err(ConvergeError::ListenerConflict {
                server: pair[0].1.serves.server_key(),
                port: pair[0].1.serves.port,
                old: pair[0].1.serves.protocol,
                new: pair[1].1.serves.protocol,
            });
        }
    }
    waiting.sort_by_key(|s| record_key(&s.0));

    let (forwardings, deps): (Vec<_>, Vec<_>) = merged.into_iter().unzip();
    let certificates = certificate_union(&deps);
    Ok(Converged {
        config: Config {
            ipv6_resolve: ideal.config.ipv6_resolve,
            log: ideal.config.log,
            relay_ca: ideal.config.relay_ca,
            keepalive: ideal.config.keepalive,
            forwardings,
        },
        forwardings: deps,
        waiting_for: waiting,
        invalid: ideal.invalid,
        certificates,
    })
}

/// The tag of a held listener that would otherwise share its pod's tag:
/// `osaka-hop (9443/relay_tcp)`. Applied again only if a pod happens to be named
/// like one, which is what keeps every tag of a config unique.
fn held_tag(tag: &str, serves: &ListenerCap) -> String {
    format!("{tag} ({}/{})", serves.port, serves.protocol.name())
}

/// This server's newest stored shape of one pod's forwarding, preferring what it
/// is actually running over what it was merely offered.
///
/// Pod identity, not socket identity: an edit that breaks a pod may also move its
/// listener, and a socket can be reused by a different pod entirely.
fn old_forwarding_of_pod(
    own: &ServerConfigViewEntity,
    pod: &NodeId,
) -> Result<Option<(Forwarding, ForwardingDeps)>, ConvergeError> {
    snapshots_newest_first(own)
        .find_map(|snapshot| {
            let index = snapshot
                .forwardings
                .iter()
                .position(|deps| deps.pod.0 == pod.0)?;
            Some(entry_at(snapshot, index))
        })
        .transpose()
}

/// This server's newest stored shape of one listener, for a listener whose pod is
/// unknown — it is only referenced by another server's snapshot.
fn old_forwarding(
    own: &ServerConfigViewEntity,
    cap: &ListenerCap,
) -> Result<Option<(Forwarding, ForwardingDeps)>, ConvergeError> {
    snapshots_newest_first(own)
        .find_map(|snapshot| {
            let index = snapshot
                .forwardings
                .iter()
                .position(|deps| &deps.serves == cap)?;
            Some(entry_at(snapshot, index))
        })
        .transpose()
}

fn snapshots_newest_first(
    own: &ServerConfigViewEntity,
) -> impl Iterator<Item = &ConfigSnapshot> + '_ {
    [&own.applied, &own.in_flight, &own.desired]
        .into_iter()
        .flatten()
}

/// Re-reads one `[[forwarding]]` entry out of a stored snapshot. `forwardings` is
/// index-aligned with the rendered TOML, which is what makes this sound.
fn entry_at(
    snapshot: &ConfigSnapshot,
    index: usize,
) -> Result<(Forwarding, ForwardingDeps), ConvergeError> {
    let config = Config::from_toml_str(&snapshot.toml)?;
    let forwarding =
        config
            .forwardings
            .into_iter()
            .nth(index)
            .ok_or(ConvergeError::SnapshotShape {
                revision: snapshot.revision,
            })?;
    Ok((forwarding, snapshot.forwardings[index].clone()))
}

/// Rejects an edit that would put a different protocol on a socket some server
/// still points at.
///
/// Such a switch has no seamless path: the two listeners cannot coexist on one
/// worker, so whichever way it is sequenced the dependants break. Rejecting it at
/// edit time turns a runtime outage into an error message that names the fix.
pub fn ensure_switch_safe(
    projected: &CanvasTopology,
    views: &[ServerConfigViewEntity],
    config: &OrchestrationConfig,
) -> Result<(), OrchestrationError> {
    let mut referenced: Vec<ListenerCap> = Vec::new();
    for view in views {
        for snapshot in [&view.desired, &view.in_flight, &view.applied]
            .into_iter()
            .flatten()
        {
            for deps in &snapshot.forwardings {
                referenced.extend(deps.points_at.iter().cloned());
            }
        }
    }
    if referenced.is_empty() {
        return Ok(());
    }
    // Only listener shapes matter here, so certificate availability must not
    // hide a pod: a switch is just as unsafe once its certificate arrives.
    let certificates = DerivationCertificates::assumed();
    for server in &projected.servers {
        // A server whose config does not derive at all is reported by the
        // derivation pass, not here.
        let Ok(derived) = derive_server_config(projected, &server.id, &certificates, config) else {
            continue;
        };
        for deps in &derived.forwardings {
            if let Some(old) = referenced.iter().find(|cap| cap.conflicts(&deps.serves)) {
                return Err(OrchestrationError::Conflict(format!(
                    "listener {}:{} is still in use as {:?}; use a new port instead of changing its protocol",
                    server.name, deps.serves.port, old.protocol
                )));
            }
        }
    }
    Ok(())
}
