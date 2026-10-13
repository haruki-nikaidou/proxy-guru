//! The fan-out hook: health facts in, notices out.
//!
//! Bound in `--mode consumer`, so it scales and fails over with the
//! orchestration handlers. A database failure is retried (in place when the
//! database did not answer, through the retry topic otherwise); everything else
//! is dropped and logged, because a fact nobody can be found for will not
//! become findable on a retry.

use crate::services::NotifyError;
use crate::services::fanout::{FanOutHealthFacts, FanoutService};
use kanau::processor::Processor;
use orchestration::events::HealthChanged;
use std::num::NonZeroUsize;
use wakuwaku::ServiceCreation;
use wakuwaku::iggy::EventHandler;
use wakuwaku::iggy::error::HandleError;

/// Consumes [`HealthChanged`].
pub struct HealthFanout {
    pub fanout: FanoutService,
}

impl ServiceCreation for HealthFanout {
    type Dep = FanoutService;
    fn create_service(fanout: FanoutService) -> Self {
        Self { fanout }
    }
}

impl EventHandler<HealthChanged> for HealthFanout {
    const MAX_BATCH: NonZeroUsize = NonZeroUsize::MIN;

    #[tracing::instrument(name = "Hook:HealthChanged", skip_all, err)]
    async fn handle(&self, run: &[HealthChanged]) -> Result<(), HandleError> {
        for event in run {
            self.fanout
                .process(FanOutHealthFacts {
                    facts: event.facts.clone(),
                })
                .await
                .map_err(NotifyError::into_handle_error)?;
        }
        Ok(())
    }
}
