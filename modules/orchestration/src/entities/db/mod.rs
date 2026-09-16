//! PostgreSQL rows and the queries that operate on them.
//!
//! One submodule per table or aggregate; [`tree`] and [`fence`] are the two
//! pieces of logic every transaction on a canvas tree shares.

pub mod agent_release;
pub mod batch;
pub mod ca;
pub mod canvas;
pub mod certificate;
pub mod connection;
pub mod dns;
pub mod fence;
pub mod health;
pub mod job_run;
pub mod node;
pub mod port;
pub mod server;
pub mod topology;
pub mod tree;
pub mod view;
