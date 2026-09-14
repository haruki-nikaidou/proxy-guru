//! The ACME renewal cron.
//!
//! One pass: make sure a `certificate` row exists for every `(sni,
//! acme_directory)` an Entry asks for, then issue or renew whatever is due
//! ([`ListCertificatesDue`]). Rows are processed one at a time — a DNS-01 order
//! takes minutes and the providers rate-limit — and a failure is recorded on its
//! row without stopping the pass.
//!
//! The pass is not run by the process that schedules it: `--mode cron` publishes
//! [`RenewCertificatesSignal`] and this consumer does the work, so renewal fails
//! over and scales like any other consumer. That takes two claims, not one:
//!
//! * [`schedule::claim_run`] keeps one pass per configured `acme_interval`
//!   fleet-wide, which is what AMQP's at-least-once delivery and horizontally
//!   scaled consumers would otherwise break.
//! * [`ClaimCertificateAttempt`] keeps one *order* per certificate per
//!   `acme_retry_after`. The job claim alone cannot do it: an order runs for
//!   minutes, so two consecutive intervals can legitimately overlap, and a
//!   second order for the same name burns the CA's rate limit for nothing. The
//!   same claim is what stops a pass that crashed mid-order from retrying
//!   immediately.

use crate::entities::surreal::certificate::{ClaimCertificateAttempt, ListCertificatesDue};
use crate::events::RenewCertificatesSignal;
use crate::hooks::schedule;
use crate::services::acme::{
    AcmeService, EnsureRequestedCertificates, IssueCertificate, IssueOutcome,
};
use crate::utils::ids::record_key;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use wakuwaku::amqp::AmqpMessageProcessor;

/// One pass: ensure rows, then work through the due ones.
pub async fn renew_due(acme: &AcmeService) {
    if let Err(e) = acme.process(EnsureRequestedCertificates).await {
        tracing::error!(error = %e, "ensuring certificate rows failed");
    }
    let now = Utc::now();
    // The same bound lists a row and claims its attempt: a row may be listed on
    // every pass, but only one attempt per `acme_retry_after` gets to run.
    let retry_before = now
        .checked_sub_signed(
            chrono::Duration::from_std(acme.config.acme_retry_after())
                .unwrap_or(chrono::TimeDelta::MAX),
        )
        .unwrap_or(DateTime::<Utc>::MIN_UTC);
    let due = match acme
        .db
        .process(ListCertificatesDue {
            renew_before: now
                .checked_add_signed(
                    chrono::Duration::from_std(acme.config.acme_renew_before())
                        .unwrap_or(chrono::TimeDelta::MAX),
                )
                .unwrap_or(DateTime::<Utc>::MAX_UTC),
            retry_before,
        })
        .await
    {
        Ok(due) => due,
        Err(e) => {
            tracing::error!(error = %e, "listing due certificates failed");
            return;
        }
    };
    for row in due {
        let id = record_key(&row.id.0);
        match acme
            .db
            .process(ClaimCertificateAttempt {
                id: row.id.clone(),
                now: Utc::now(),
                claim_before: retry_before,
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::debug!(
                    certificate = %id,
                    "another consumer is already working on this certificate"
                );
                continue;
            }
            Err(e) => {
                tracing::error!(certificate = %id, error = %e, "claiming a certificate attempt failed");
                continue;
            }
        }
        match acme.process(IssueCertificate { id: row.id }).await {
            Ok(IssueOutcome::Issued(cert)) => {
                tracing::info!(certificate = %id, sni = %cert.sni, version = cert.version, "certificate issued")
            }
            Ok(IssueOutcome::Failed { sni, error }) => {
                tracing::warn!(certificate = %id, %sni, %error, "certificate issuance failed")
            }
            Err(e) => {
                tracing::error!(certificate = %id, error = %e, "certificate issuance errored")
            }
        }
    }
}

/// Consumes the ACME renewal execution signal.
#[derive(Clone)]
pub struct AcmeCronHook {
    pub acme: AcmeService,
}

impl AmqpMessageProcessor<RenewCertificatesSignal> for AcmeCronHook {
    const QUEUE: &'static str = "guru_orchestration_renew_certificates";
}

impl Processor<RenewCertificatesSignal> for AcmeCronHook {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Hook:RenewCertificatesSignal", skip_all, err)]
    async fn process(&self, input: RenewCertificatesSignal) -> Result<Self::Output, Self::Error> {
        if !schedule::claim_run(
            &self.acme.db,
            "renew_certificates",
            self.acme.config.acme_interval(),
            input.tick_time(),
        )
        .await?
        {
            return Ok(());
        }
        renew_due(&self.acme).await;
        Ok(())
    }
}
