//! The fan-out hook: health facts in, notices out.
//!
//! Bound in `--mode consumer`, so it scales and fails over with the
//! orchestration hooks. A database failure is returned and the delivery is
//! requeued; everything else is acked, because a fact nobody can be found for
//! will not become findable on a retry.

use crate::services::fanout::{FanOutHealthFacts, FanoutService};
use kanau::processor::Processor;
use orchestration::events::HealthChanged;
use wakuwaku::integration::amqp::AmqpMessageProcessor;

/// Consumes [`HealthChanged`].
#[derive(Clone)]
pub struct HealthFanout {
    pub fanout: FanoutService,
}

impl AmqpMessageProcessor<HealthChanged> for HealthFanout {
    const QUEUE: &'static str = "guru_notify_health_changed";
}

impl Processor<HealthChanged> for HealthFanout {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Hook:HealthChanged", skip_all, err)]
    async fn process(&self, input: HealthChanged) -> Result<Self::Output, Self::Error> {
        self.fanout
            .process(FanOutHealthFacts { facts: input.facts })
            .await?;
        Ok(())
    }
}
