//! # `notify` — notifications about the fleet's health
//!
//! Turns the health facts `orchestration` publishes
//! ([`orchestration::events::HealthChanged`]) into messages an operator
//! receives by email or Telegram, in the language their settings name.
//!
//! Two scopes, both keyed by canvas ("workspace"):
//!
//! - the **canvas** row carries the destinations a whole workspace shares
//!   (a list of addresses and a list of chat ids),
//! - an **account** row carries one operator's own channels, with one default
//!   row serving every canvas the account has no row for.
//!
//! Both name the set of notice kinds they want, and both default to the empty
//! set: installing this module notifies nobody until an operator opts in.
//!
//! ## The path one notice takes
//!
//! 1. [`hooks::fanout::HealthFanout`] consumes `HealthChanged`, and
//!    [`services::fanout::FanoutService`] drops every fact that does not change
//!    what was last announced about its subject ([`entities::db::state`]).
//! 2. What survives becomes one [`events::HealthNotifyGroupEvent`] per canvas
//!    with matching destinations, and one [`events::HealthNotifyPersonalEvent`]
//!    per matching account.
//! 3. [`hooks::delivery::NoticeDelivery`] — the single-instance `notifier` mode
//!    of `guru-master` — renders each notice once ([`utils::message`]) and
//!    sends it to every destination it carries.
//!
//! The channels' secrets (the SMTP password, the Telegram bot token) come from
//! the environment ([`utils::secret::NotifySecrets`]), never from
//! [`config::NotifyConfig`]: the config document is readable by an Admin in the
//! dashboard, and a bot token is not a setting.
//!
//! See `AGENTS.md` at the workspace root for the layout every module follows.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::arithmetic_side_effects)]

pub mod config;
pub mod entities;
pub mod events;
pub mod hooks;
pub mod rpc;
pub mod services;
pub mod utils;
