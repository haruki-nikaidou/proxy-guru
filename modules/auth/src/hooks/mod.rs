//! Background reactors that run outside the request path.
//!
//! Hooks are where a module reacts to things instead of being called directly:
//!
//! - **Event handlers** — implement `wakuwaku::iggy::EventHandler<E>` to handle
//!   an [`event`](crate::events) consumed from its Iggy topic.
//! - **Cron jobs** — implement `wakuwaku::CronJob` (a `Processor<CronTick>`
//!   with a fixed `INTERVAL`) for periodic tasks: cleanup, aggregation,
//!   reconciliation.
//! - **Event loggers** — persist or forward events for audit/observability.
//!
//! Like services, a hook is a struct that owns its dependencies, and it
//! implements `wakuwaku::ServiceCreation` so `guru-master` can build it from
//! the `ServiceBuilder` its consumer/cron workers start.
//!
//! ```ignore
//! use std::num::NonZeroUsize;
//! use wakuwaku::ServiceCreation;
//! use wakuwaku::iggy::EventHandler;
//! use wakuwaku::iggy::error::HandleError;
//!
//! use crate::events::ExampleHappened;
//! use crate::services::ExampleService;
//!
//! pub struct ExampleHook {
//!     pub service: ExampleService,
//! }
//!
//! impl ServiceCreation for ExampleHook {
//!     type Dep = ExampleService;
//!     fn create_service(service: ExampleService) -> Self {
//!         Self { service }
//!     }
//! }
//!
//! impl EventHandler<ExampleHappened> for ExampleHook {
//!     const MAX_BATCH: NonZeroUsize = NonZeroUsize::MIN;
//!     async fn handle(&self, run: &[ExampleHappened]) -> Result<(), HandleError> {
//!         // react to each event; classify failures as transient/retryable/unrecoverable
//!         Ok(())
//!     }
//! }
//! ```
