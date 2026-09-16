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
//! * [`ClaimJobRun`] keeps one pass per configured `acme_interval` fleet-wide,
//!   which is what AMQP's at-least-once delivery and horizontally scaled
//!   consumers would otherwise break.
//! * [`ClaimCertificateAttempt`] keeps one *order* per certificate. It is an
//!   exact compare-and-set on the `last_attempt_at` the pass observed while
//!   listing, not a time window: rows are ordered one at a time and an order
//!   takes minutes, so a listing entry can be minutes stale by the time the pass
//!   reaches it. A stale entry and a duplicate pass are therefore both no-ops —
//!   whoever renewed the row in between moved `last_attempt_at`, and only the
//!   first `UPDATE` matching the observed value wins. A pass that crashed
//!   mid-order is retried by the next listing instead, because the claim stamped
//!   `last_attempt_at` before ordering and [`ListCertificatesDue`] holds the row
//!   back for `acme_retry_after`.

use crate::entities::db::certificate::{ClaimCertificateAttempt, ListCertificatesDue};
use crate::entities::db::job_run::ClaimJobRun;
use crate::events::RenewCertificatesSignal;
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
    // The window that lists a row: an attempt is stamped at claim time, so a row
    // whose order is still running (or crashed) waits out `acme_retry_after`.
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
                seen_attempt_at: row.last_attempt_at,
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::debug!(
                    certificate = %id,
                    "the row moved since it was listed; another pass has it"
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

/// The `job_run` key this cron claims its fleet-wide pass under.
const JOB: &str = "renew_certificates";

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
        if !self
            .acme
            .db
            .process(ClaimJobRun::for_tick(
                JOB,
                self.acme.config.acme_interval(),
                input.tick_time(),
            ))
            .await?
        {
            return Ok(());
        }
        renew_due(&self.acme).await;
        Ok(())
    }
}
