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
//! Rule 2 means a pod whose listener moved to a new port runs *two* listeners
//! until its dependants have switched. The worker tells listeners apart by tag
//! (the pod's id), so the held one is renamed after its socket —
//! `<pod> (9443/relay_tcp)` — and drops out, name and all, once nothing points at
//! it.

use crate::entities::db::pod::PodId;
use crate::entities::db::server::ServerId;
use crate::entities::db::view::{
    CertificateRef, ConfigSnapshot, ForwardingDeps, InvalidPod, ListenProtocol, ListenerCap,
    ServerConfigViewEntity,
};
use crate::services::derive::{DerivedConfig, certificate_union};
use guru_worker_config::{Config, Forwarding};
use std::collections::{BTreeMap, HashSet};

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

    let own_key = own.server.to_string();
    #[allow(clippy::mutable_key_type)]
    let mut referenced: HashSet<ListenerCap> = HashSet::new();
    for view in views {
        let is_own = view.server.to_string() == own_key;
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
    let mut waiting_keys: HashSet<ServerId> = HashSet::new();

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
            if waiting_keys.insert(cap.server.clone()) {
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
        if let Some(previous) = old_forwarding_of_pod(own, &pod.pod)? {
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
    waiting.sort_by_key(|s| s.to_string());

    let (forwardings, deps): (Vec<_>, Vec<_>) = merged.into_iter().unzip();
    let certificates = certificate_union(&deps);
    Ok(Converged {
        config: Config {
            ipv6_resolve: ideal.config.ipv6_resolve,
            log: ideal.config.log,
            relay_ca: ideal.config.relay_ca,
            keepalive: ideal.config.keepalive,
            quic: ideal.config.quic,
            forwardings,
        },
        forwardings: deps,
        waiting_for: waiting,
        invalid: ideal.invalid,
        certificates,
    })
}

/// The tag of a held listener that would otherwise share its pod's tag:
/// `<pod> (9443/relay_tcp)`. Applied again only if a tag happens to read like
/// one, which is what keeps every tag of a config unique.
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
    pod: &PodId,
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

/// A listener an edit would give a different protocol while some server still
/// points at it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchConflict {
    pub server: ServerId,
    pub pod: PodId,
    pub port: i64,
    /// The protocol the fabric still depends on.
    pub in_use: ListenProtocol,
    pub wanted: ListenProtocol,
}

/// Every listener of the projected configs that would put a different protocol
/// on a socket some server still points at.
///
/// Such a switch has no seamless path: the two listeners cannot coexist on one
/// worker, so whichever way it is sequenced the dependants break. Refusing it
/// at edit time turns a runtime outage into an error message that names the
/// fix. `projected` is derived with assumed certificates: a switch is just as
/// unsafe once its certificate arrives.
pub fn switch_conflicts(
    projected: &BTreeMap<ServerId, DerivedConfig>,
    views: &[ServerConfigViewEntity],
) -> Vec<SwitchConflict> {
    let mut referenced: Vec<&ListenerCap> = Vec::new();
    for view in views {
        for snapshot in [&view.desired, &view.in_flight, &view.applied]
            .into_iter()
            .flatten()
        {
            for deps in &snapshot.forwardings {
                referenced.extend(deps.points_at.iter());
            }
        }
    }
    let mut out = Vec::new();
    for (server, derived) in projected {
        for deps in &derived.forwardings {
            if let Some(old) = referenced.iter().find(|cap| cap.conflicts(&deps.serves))
                && !out.iter().any(|c: &SwitchConflict| c.pod == deps.pod)
            {
                out.push(SwitchConflict {
                    server: server.clone(),
                    pod: deps.pod.clone(),
                    port: deps.serves.port,
                    in_use: old.protocol,
                    wanted: deps.serves.protocol,
                });
            }
        }
    }
    out
}
