#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::arithmetic_side_effects)]

//! The forwarding topology, as a directed acyclic graph of pods.
//!
//! A **pod** is one listener on one server, and the unit everything else is
//! built from. A **server** is only the set of pods one machine runs. An
//! **edge** lets a pod send its traffic on, either to another pod (a relay hop,
//! spoken in the protocol the *target* pod listens with) or to an **exit**,
//! where traffic leaves the fabric. Each pod's **route** is a tree over its own
//! out-edges: `balance` spreads connections by weight, `failover` takes the
//! first member that is alive, and either may nest inside the other.
//!
//! Universal pods, bundles, splitters and aggregators are how the dashboard
//! draws and edits this graph. None of them exist here: a pod feeding a
//! four-way splitter that feeds a four-way aggregator in front of another pod
//! is two pods and four edges.
//!
//! The crate is pure (no database, no network, no async) and answers two
//! questions about a [`Graph`]:
//!
//! - [`check`]: may this graph be stored? Errors reject an edit; warnings are
//!   reported and never block one.
//! - [`compile`]: what does every server run? One forwarding per pod, what each
//!   depends on, and the QUIC pairing of every link, rendered as a route table
//!   for workers that read one and as the older tree for those that do not.
//!
//! The same input always gives the same output, and no input panics.

mod check;
mod compile;
pub mod diagnostic;
mod index;
mod legacy;
pub mod model;

pub use check::check;
pub use compile::{Compiled, Deps, ListenProtocol, Listener, ServerConfig, compile};
pub use diagnostic::{Diagnostic, Invalid, InvalidPod, Problem, Report, Severity, Subject};
pub use model::{
    Capabilities, CertificateKind, CertificateRef, Certificates, Edge, EdgeId, EdgeTarget, Exit,
    ExitId, Graph, Ingress, MAX_ROUTE_DEPTH, Pod, PodId, Route, Server, ServerId, ServerQuic,
    Sticky, Weighted, relay_sni,
};
