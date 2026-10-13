//! Background reactors that run outside the request path.
//!
//! Hooks are where a module reacts to things instead of being called directly:
//!
//! - **Event handlers** — implement `wakuwaku::iggy::EventHandler<E>` to handle
//!   an [`event`](crate::events) consumed from its Iggy topic. The handler maps
//!   its module error into a `HandleError` whose class decides what the
//!   consumer does next: `Transient` (infrastructure down, retry in place),
//!   `Retryable` (park the key on the retry topic) or `Unrecoverable` (drop and
//!   log).
//! - **Cron jobs** — implement `wakuwaku::CronJob` (a `Processor<CronTick>`
//!   with a fixed `INTERVAL`) for periodic tasks: cleanup, aggregation,
//!   reconciliation.
//! - **Event loggers** — persist or forward events for audit/observability.
//!
//! Like services, a hook is a struct that owns its dependencies, and it
//! implements `wakuwaku::ServiceCreation` so `guru-master` can build it from
//! the `ServiceBuilder` of the consumer or cron mode that runs it.
//!
//! ```ignore
//! use std::num::NonZeroUsize;
//! use std::time::Duration;
//! use kanau::processor::Processor;
//! use wakuwaku::iggy::EventHandler;
//! use wakuwaku::iggy::error::HandleError;
//! use wakuwaku::{CronJob, CronJobInterval, CronTick, ServiceCreation};
//!
//! use crate::events::ExampleHappened;
//! use crate::services::{ExampleError, ExampleService, React, Sweep};
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
//!     #[tracing::instrument(name = "Hook:ExampleHappened", skip_all, err)]
//!     async fn handle(&self, run: &[ExampleHappened]) -> Result<(), HandleError> {
//!         for event in run {
//!             self.service
//!                 .process(React { id: event.id })
//!                 .await
//!                 .map_err(ExampleError::into_handle_error)?;
//!         }
//!         Ok(())
//!     }
//! }
//!
//! pub struct ExampleSweepJob {
//!     pub service: ExampleService,
//! }
//!
//! impl ServiceCreation for ExampleSweepJob {
//!     type Dep = ExampleService;
//!     fn create_service(service: ExampleService) -> Self {
//!         Self { service }
//!     }
//! }
//!
//! impl Processor<CronTick> for ExampleSweepJob {
//!     type Output = ();
//!     type Error = ExampleError;
//!     #[tracing::instrument(name = "Hook:ExampleSweepJob", skip_all, err)]
//!     async fn process(&self, tick: CronTick) -> Result<(), ExampleError> {
//!         self.service.process(Sweep { at: tick.fired_at }).await
//!     }
//! }
//!
//! impl CronJob for ExampleSweepJob {
//!     const INTERVAL: CronJobInterval = CronJobInterval::AtLeast(Duration::from_secs(60));
//! }
//! ```
