//! PostgreSQL rows and the queries that operate on them.
//!
//! One submodule per table or aggregate. Each query or command is a small input
//! struct implementing `Query` (what it touches, from [`tables`]) and
//! `Execute<PgSource>` (how it runs), so [`crate::db::Db`] can run it.

pub mod app_config;
pub mod tables;
