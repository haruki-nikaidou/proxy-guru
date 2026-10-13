//! One uninhabited marker per SQL table this crate owns, named after the table.
//!
//! A query names the tables it touches in its `Query::Effect` (`Read<_>` /
//! `Write<_>`), and the [`Db`](crate::db::Db) capability must cover them.

use wakuwaku::entity::Entity;

/// `app_config`
pub enum AppConfig {}
impl Entity for AppConfig {}
