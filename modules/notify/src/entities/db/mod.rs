//! PostgreSQL entities and queries.
//!
//! Put your database row structs and query/command processors here — one
//! submodule per table or aggregate (e.g. `pub mod user_account;`).
//!
//! The convention: define a row struct, then model each read or write as an
//! input struct with a `Processor` implementation on the shared
//! `DatabaseProcessor` from `wakuwaku::sqlx`. Every statement goes through the
//! `sqlx::query!`/`query_as!`/`query_scalar!` macros, so it is checked against
//! the schema at compile time; SQL longer than five lines lives in this crate's
//! `sql/` directory behind `query_file_as!`.
//!
//! ```ignore
//! use kanau::processor::Processor;
//! use wakuwaku::sqlx::DatabaseProcessor;
//!
//! #[derive(Debug, Clone)]
//! pub struct Example {
//!     pub id: uuid::Uuid,
//!     pub name: String,
//! }
//!
//! pub struct FindExampleById {
//!     pub id: uuid::Uuid,
//! }
//!
//! impl Processor<FindExampleById> for DatabaseProcessor {
//!     type Output = Option<Example>;
//!     type Error = sqlx::Error;
//!     async fn process(&self, input: FindExampleById) -> Result<Self::Output, Self::Error> {
//!         sqlx::query_as!(
//!             Example,
//!             r#"SELECT id, name FROM "base"."example" WHERE id = $1"#,
//!             input.id
//!         )
//!         .fetch_optional(self.db())
//!         .await
//!     }
//! }
//! ```
