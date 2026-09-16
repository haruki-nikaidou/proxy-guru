//! PostgreSQL rows and the queries that operate on them.
//!
//! One submodule per table or aggregate. Each query or command is a small input
//! struct with a `Processor` implementation on [`crate::db::Db`], so
//! persistence logic stays testable and composable.

pub mod app_config;
