//! The QUIC relay transport: how a connection sends (brutal or Cubic), what it
//! advertises, and the pool that keeps one connection per link on the dialing side.
//! Brutal itself, and the patched quinn it needs, come from shinku-quic.

pub mod pool;
pub mod transport;
