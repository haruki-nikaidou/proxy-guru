//! The QUIC relay transport: how a connection sends (brutal or Cubic), what it
//! advertises, and the pool that keeps one connection per link on the dialing side.

pub mod brutal;
pub mod pool;
pub mod transport;
