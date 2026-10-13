//! PostgreSQL rows and the queries that operate on them.
//!
//! One submodule per table or aggregate; [`tree`] and [`fence`] are the two
//! pieces of logic every transaction on a canvas tree shares, as helper
//! queries a flow runs with `tx.run`. [`tables`] names every table this crate
//! owns, for the queries' effects.

pub mod agent_release;
pub mod ca;
pub mod canvas;
pub mod certificate;
pub mod dns;
pub mod edge;
pub mod exit;
pub mod fence;
pub mod graph;
pub mod group;
pub mod health;
pub mod job_run;
pub mod pod;
pub mod server;
pub mod tables;
pub mod tree;
pub mod view;
