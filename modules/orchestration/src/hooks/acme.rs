//! The ACME renewal cron.
//!
//! Every tick: make sure a `certificate` row exists for every `(sni,
//! acme_directory)` an Entry asks for, then issue or renew whatever is due
//! ([`ListCertificatesDue`]). Rows are processed one at a time — a DNS-01 order
//! takes minutes and the providers rate-limit — and a failure is recorded on its
//! row without stopping the pass.

use crate::entities::surreal::certificate::ListCertificatesDue;
use crate::services::acme::{
    AcmeService, EnsureRequestedCertificates, IssueCertificate, IssueOutcome,
};
use crate::utils::ids::record_key;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// One pass: ensure rows, then work through the due ones.
pub async fn renew_due(acme: &AcmeService) {
    if let Err(e) = acme.process(EnsureRequestedCertificates).await {
        tracing::error!(error = %e, "ensuring certificate rows failed");
    }
    let now = Utc::now();
    let due = match acme
        .db
        .process(ListCertificatesDue {
            renew_before: now
                .checked_add_signed(
                    chrono::Duration::from_std(acme.config.acme_renew_before())
                        .unwrap_or(chrono::TimeDelta::MAX),
                )
                .unwrap_or(DateTime::<Utc>::MAX_UTC),
            retry_before: now
                .checked_sub_signed(
                    chrono::Duration::from_std(acme.config.acme_retry_after())
                        .unwrap_or(chrono::TimeDelta::MAX),
                )
                .unwrap_or(DateTime::<Utc>::MIN_UTC),
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

/// Runs [`renew_due`] every `interval` until `shutdown`.
pub async fn run_acme_renewal(acme: AcmeService, interval: Duration, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        renew_due(&acme).await;
    }
}
