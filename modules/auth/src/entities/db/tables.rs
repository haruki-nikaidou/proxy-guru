//! One uninhabited marker per SQL table this crate owns, named after the table.
//!
//! A query names the tables it touches in its `Query::Effect` (`Read<_>` /
//! `Write<_>`), and the [`Db`](base::db::Db) capability must cover them.

use wakuwaku::entity::Entity;

/// `auth_account`
pub enum AuthAccount {}
impl Entity for AuthAccount {}

/// `auth_session`
pub enum AuthSession {}
impl Entity for AuthSession {}

/// `api_key`
pub enum ApiKey {}
impl Entity for ApiKey {}
