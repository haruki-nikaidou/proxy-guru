//! Business logic layer.
//!
//! A service is a stateful `Processor`: a
//! `Clone`-able struct that owns its dependencies (database, Redis, event
//! publisher, other services, loaded config) and implements one `Processor` per
//! operation it supports.
//!
//! Services orchestrate [`entities`](crate::entities), publish
//! [`events`](crate::events), and are the layer the [`rpc`](crate::rpc) edge
//! calls into. Keep transport concerns (protobuf, HTTP) out of here — a service
//! should be usable regardless of how it is invoked.
//!
//! [`config`] is this module's own service: the configuration store every
//! binary loads its typed settings from.
//!
//! ```ignore
//! use kanau::processor::Processor;
//! use crate::db::Db;
//!
//! #[derive(Clone)]
//! pub struct ExampleService {
//!     pub db: Db,
//! }
//!
//! pub struct DoSomething {
//!     pub id: uuid::Uuid,
//! }
//!
//! impl Processor<DoSomething> for ExampleService {
//!     type Output = ();
//!     type Error = ExampleError; // a `thiserror` enum with `Database(#[from] crate::db::Error)`
//!     #[tracing::instrument(name = "Service:DoSomething", skip_all, err)]
//!     async fn process(&self, input: DoSomething) -> Result<(), Self::Error> {
//!         // load entities, apply business rules, persist, emit events...
//!         Ok(())
//!     }
//! }
//! ```

pub mod config;
