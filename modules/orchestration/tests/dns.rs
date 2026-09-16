//! DNS providers and ACME certificate rows: the service rules and the row
//! queries behind them, with the ACME client faked.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use chrono::{DateTime, TimeDelta, Utc};
use common::*;
use kanau::processor::Processor;
use orchestration::entities::surreal::canvas::{CanvasEntity, FindCanvasById};
use orchestration::entities::surreal::certificate::{
    CertificateEntity, CertificateStatus, ClaimCertificateAttempt, EnsureCertificate,
    FindCertificateById, ListCertificatesDue, MarkCertificateAttemptFailed, StoreIssuedCertificate,
};
use orchestration::entities::surreal::dns::{DnsProvider, FindDnsProviderById};
use orchestration::entities::surreal::node::{DeleteNodeRow, EntryConfig, NodeSpec, TlsConfig};
use orchestration::hooks::acme::renew_due;
use orchestration::services::OrchestrationError;
use orchestration::services::acme::{
    AcmeError, AcmeIssuer, AcmeService, BoxFuture, DeleteCertificate, EnsureRequestedCertificates,
    IssueCertificate, IssueOutcome, IssueRequest, IssuedMaterial, ListCertificates,
    RetryCertificate,
};
use orchestration::services::dns::{
    CreateDnsProvider, DeleteDnsProvider, DnsProviderSummary, ListDnsProviders, UpdateDnsProvider,
};
use orchestration::utils::ids::record_key;
use std::sync::Arc;
use tokio::sync::Mutex;

const DIRECTORY: &str = "https://acme-staging-v02.api.letsencrypt.org/directory";

async fn create_provider(w: &World, name: &str, secret: &str) -> DnsProviderSummary {
    w.dns
        .process(CreateDnsProvider {
            actor: operator(),
            name: name.to_string(),
            provider: DnsProvider::Cloudflare,
            account_id: String::new(),
            api_secret: secret.to_string(),
        })
        .await
        .unwrap()
}

fn entry_spec(provider: &DnsProviderSummary, sni: &str, directory: &str) -> NodeSpec {
    NodeSpec::Entry(EntryConfig {
        receive_proxy_protocol: None,
        tls: Some(TlsConfig {
            sni: sni.to_string(),
            dns_provider: provider.id.clone(),
            domain_id: "zone1".to_string(),
            acme_directory: directory.to_string(),
        }),
    })
}

async fn ensure(
    w: &World,
    provider: &DnsProviderSummary,
    sni: &str,
    directory: &str,
) -> CertificateEntity {
    w.db.process(EnsureCertificate {
        sni: sni.to_string(),
        dns_provider: provider.id.clone(),
        domain_id: "zone1".to_string(),
        acme_directory: directory.to_string(),
        now: Utc::now(),
    })
    .await
    .unwrap()
}

async fn find(w: &World, cert: &CertificateEntity) -> CertificateEntity {
    w.db.process(FindCertificateById {
        id: cert.id.clone(),
    })
    .await
    .unwrap()
    .unwrap()
}

async fn store_issued(
    w: &World,
    cert: &CertificateEntity,
    not_after: DateTime<Utc>,
) -> CertificateEntity {
    w.db.process(StoreIssuedCertificate {
        id: cert.id.clone(),
        acme_account_key: "enc1:acct".to_string(),
        private_key_pem: "enc1:key".to_string(),
        full_chain_pem: "chain".to_string(),
        not_before: Utc::now(),
        not_after,
        now: Utc::now(),
    })
    .await
    .unwrap()
}

async fn mark_failed(w: &World, cert: &CertificateEntity, error: &str) {
    w.db.process(MarkCertificateAttemptFailed {
        id: cert.id.clone(),
        error: error.to_string(),
        now: Utc::now(),
    })
    .await
    .unwrap();
}

fn keys(rows: &[CertificateEntity]) -> Vec<String> {
    let mut out: Vec<String> = rows.iter().map(|r| record_key(&r.id.0)).collect();
    out.sort();
    out
}

async fn generation(w: &World, canvas: &CanvasEntity) -> i64 {
    w.db.process(FindCanvasById {
        id: canvas.id.clone(),
    })
    .await
    .unwrap()
    .unwrap()
    .generation
}

#[tokio::test]
async fn dns_provider_crud_encrypts_and_hides_the_secret() -> TestResult {
    let w = world().await?;

    let denied = w
        .dns
        .process(CreateDnsProvider {
            actor: maintainer(),
            name: "cf".into(),
            provider: DnsProvider::Cloudflare,
            account_id: String::new(),
            api_secret: "tok".into(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(denied, OrchestrationError::PermissionDenied),
        "{denied:?}"
    );

    let invalid = w
        .dns
        .process(CreateDnsProvider {
            actor: operator(),
            name: "   ".into(),
            provider: DnsProvider::Cloudflare,
            account_id: String::new(),
            api_secret: "tok".into(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(invalid, OrchestrationError::Invalid(_)),
        "{invalid:?}"
    );

    let created = create_provider(&w, "cf", "cf-token-1").await;
    let row =
        w.db.process(FindDnsProviderById {
            id: created.id.clone(),
        })
        .await?
        .unwrap();
    assert!(row.api_secret.starts_with("enc1:"), "{}", row.api_secret);
    assert_eq!(w.secrets.decrypt_str(&row.api_secret)?, "cf-token-1");

    let listed = w
        .dns
        .process(ListDnsProviders { actor: operator() })
        .await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "cf");
    let denied = w
        .dns
        .process(ListDnsProviders {
            actor: maintainer(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(denied, OrchestrationError::PermissionDenied),
        "{denied:?}"
    );

    // An update without a secret keeps the stored ciphertext byte for byte.
    let updated = w
        .dns
        .process(UpdateDnsProvider {
            actor: operator(),
            id: created.id.clone(),
            name: "cf-renamed".into(),
            account_id: "team".into(),
            api_secret: None,
        })
        .await?;
    assert_eq!(updated.name, "cf-renamed");
    assert_eq!(updated.account_id, "team");
    let after =
        w.db.process(FindDnsProviderById {
            id: created.id.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(after.api_secret, row.api_secret);

    w.dns
        .process(UpdateDnsProvider {
            actor: operator(),
            id: created.id.clone(),
            name: "cf-renamed".into(),
            account_id: "team".into(),
            api_secret: Some("cf-token-2".into()),
        })
        .await?;
    let rotated =
        w.db.process(FindDnsProviderById {
            id: created.id.clone(),
        })
        .await?
        .unwrap();
    assert_ne!(rotated.api_secret, row.api_secret);
    assert_eq!(w.secrets.decrypt_str(&rotated.api_secret)?, "cf-token-2");

    let missing = w
        .dns
        .process(UpdateDnsProvider {
            actor: operator(),
            id: orchestration::utils::ids::dns_provider_id("nope"),
            name: "x".into(),
            account_id: String::new(),
            api_secret: None,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(missing, OrchestrationError::NotFound),
        "{missing:?}"
    );
    Ok(())
}

#[tokio::test]
async fn dns_provider_delete_is_refused_while_an_entry_uses_it() -> TestResult {
    let w = world().await?;
    let provider = create_provider(&w, "cf", "tok").await;
    let c = canvas(&w.db, "prod").await?;
    let entry = node(
        &w.db,
        &c,
        "edge",
        entry_spec(&provider, "a.example.com", DIRECTORY),
        entry_ports(),
    )
    .await?;

    let denied = w
        .dns
        .process(DeleteDnsProvider {
            actor: maintainer(),
            id: provider.id.clone(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(denied, OrchestrationError::PermissionDenied),
        "{denied:?}"
    );

    let refused = w
        .dns
        .process(DeleteDnsProvider {
            actor: operator(),
            id: provider.id.clone(),
        })
        .await
        .unwrap_err();
    match refused {
        OrchestrationError::Conflict(message) => {
            assert!(message.contains("1 entry node"), "{message}")
        }
        other => panic!("{other:?}"),
    }
    assert!(
        w.db.process(FindDnsProviderById {
            id: provider.id.clone()
        })
        .await?
        .is_some()
    );

    w.db.process(DeleteNodeRow {
        id: entry.node.id.clone(),
        canvas: c.id.clone(),
        import_sync: None,
        frees_canvas: None,
        fence: None,
    })
    .await?;
    w.dns
        .process(DeleteDnsProvider {
            actor: operator(),
            id: provider.id.clone(),
        })
        .await?;
    assert!(
        w.db.process(FindDnsProviderById {
            id: provider.id.clone()
        })
        .await?
        .is_none()
    );
    let missing = w
        .dns
        .process(DeleteDnsProvider {
            actor: operator(),
            id: provider.id,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(missing, OrchestrationError::NotFound),
        "{missing:?}"
    );
    Ok(())
}

#[tokio::test]
async fn ensure_certificate_is_idempotent_per_sni_and_directory() -> TestResult {
    let w = world().await?;
    let provider = create_provider(&w, "cf", "tok").await;
    let other = create_provider(&w, "cf2", "tok2").await;

    let first = ensure(&w, &provider, "a.example.com", DIRECTORY).await;
    assert_eq!(first.status, CertificateStatus::Pending);
    assert_eq!(first.version, 0);
    let again =
        w.db.process(EnsureCertificate {
            sni: "a.example.com".into(),
            dns_provider: other.id.clone(),
            domain_id: "other-zone".into(),
            acme_directory: DIRECTORY.into(),
            now: Utc::now(),
        })
        .await?;
    assert_eq!(
        again.id.0, first.id.0,
        "the same pair returns the existing row"
    );
    assert_eq!(again.domain_id, "zone1", "the first Entry to ask wins");
    assert_eq!(again.dns_provider.0, provider.id.0);

    let staging = ensure(&w, &provider, "a.example.com", "https://other/directory").await;
    assert_ne!(staging.id.0, first.id.0, "another directory is another row");

    let listed = w
        .certificates
        .process(ListCertificates {
            actor: maintainer(),
        })
        .await?;
    assert_eq!(listed.len(), 2);
    Ok(())
}

#[tokio::test]
async fn list_certificates_due_picks_the_right_rows() -> TestResult {
    let w = world().await?;
    let provider = create_provider(&w, "cf", "tok").await;
    let now = Utc::now();
    let renew_before = now + TimeDelta::days(30);

    let pending = ensure(&w, &provider, "pending.example.com", DIRECTORY).await;
    let failed = ensure(&w, &provider, "failed.example.com", DIRECTORY).await;
    mark_failed(&w, &failed, "boom").await;
    let fresh = ensure(&w, &provider, "fresh.example.com", DIRECTORY).await;
    store_issued(&w, &fresh, now + TimeDelta::days(80)).await;
    let expiring = ensure(&w, &provider, "expiring.example.com", DIRECTORY).await;
    store_issued(&w, &expiring, now + TimeDelta::days(10)).await;
    let forced = ensure(&w, &provider, "forced.example.com", DIRECTORY).await;
    store_issued(&w, &forced, now + TimeDelta::days(80)).await;
    w.certificates
        .process(RetryCertificate {
            actor: operator(),
            id: forced.id.clone(),
        })
        .await?;

    // Attempts younger than the retry window are throttled: the failed row and
    // the expiring row (its last attempt is the issuance a moment ago) wait.
    let throttled =
        w.db.process(ListCertificatesDue {
            renew_before,
            retry_before: now - TimeDelta::hours(1),
        })
        .await?;
    assert_eq!(keys(&throttled), keys(&[pending.clone(), forced.clone()]));

    // Once the window has passed they are due; the fresh certificate never is.
    let due =
        w.db.process(ListCertificatesDue {
            renew_before,
            retry_before: now + TimeDelta::hours(1),
        })
        .await?;
    assert_eq!(
        keys(&due),
        keys(&[
            pending.clone(),
            failed.clone(),
            expiring.clone(),
            forced.clone()
        ])
    );

    // The throttle lives in this predicate, not in the claim: the claim stamps
    // `last_attempt_at` before the order starts, which is what holds a pending
    // row back while its attempt is still running — or after it crashed
    // mid-order.
    assert!(
        w.db.process(ClaimCertificateAttempt {
            id: pending.id.clone(),
            now: Utc::now(),
            seen_attempt_at: None,
        })
        .await?
    );
    let claimed =
        w.db.process(ListCertificatesDue {
            renew_before,
            retry_before: now - TimeDelta::hours(1),
        })
        .await?;
    assert_eq!(
        keys(&claimed),
        keys(std::slice::from_ref(&forced)),
        "a pending row whose attempt was just stamped is not listed again"
    );
    let window_passed =
        w.db.process(ListCertificatesDue {
            renew_before,
            retry_before: Utc::now() + TimeDelta::hours(1),
        })
        .await?;
    assert_eq!(
        keys(&window_passed),
        keys(&[pending, failed, expiring, forced]),
        "once the retry window passes it is due again"
    );
    Ok(())
}

#[tokio::test]
async fn store_issued_bumps_version_and_clears_the_failure() -> TestResult {
    let w = world().await?;
    let provider = create_provider(&w, "cf", "tok").await;
    let cert = ensure(&w, &provider, "a.example.com", DIRECTORY).await;

    mark_failed(&w, &cert, "dns timeout").await;
    let failed = find(&w, &cert).await;
    assert_eq!(failed.status, CertificateStatus::Failed);
    assert_eq!(failed.last_error.as_deref(), Some("dns timeout"));
    assert!(failed.last_attempt_at.is_some());

    let issued = store_issued(&w, &cert, Utc::now() + TimeDelta::days(90)).await;
    assert_eq!(issued.status, CertificateStatus::Issued);
    assert_eq!(issued.version, 1);
    assert!(issued.last_error.is_none());
    assert!(issued.is_issued());

    // A failed renewal keeps the issued material and status.
    mark_failed(&w, &cert, "rate limited").await;
    let still_issued = find(&w, &cert).await;
    assert_eq!(still_issued.status, CertificateStatus::Issued);
    assert_eq!(still_issued.last_error.as_deref(), Some("rate limited"));
    assert!(still_issued.is_issued());

    let renewed = store_issued(&w, &cert, Utc::now() + TimeDelta::days(90)).await;
    assert_eq!(renewed.version, 2);
    assert!(renewed.last_error.is_none());
    Ok(())
}

#[tokio::test]
async fn retry_and_delete_are_admin_only_and_delete_respects_entries() -> TestResult {
    let w = world().await?;
    let provider = create_provider(&w, "cf", "tok").await;
    let c = canvas(&w.db, "prod").await?;
    // The Entry leaves the directory empty: it resolves to the default, which is
    // the directory the cron keys the row by.
    let entry = node(
        &w.db,
        &c,
        "edge",
        entry_spec(&provider, "a.example.com", ""),
        entry_ports(),
    )
    .await?;
    let default_directory = w.config.acme_directory("").to_string();
    let cert = ensure(&w, &provider, "a.example.com", &default_directory).await;
    mark_failed(&w, &cert, "boom").await;

    let denied = w
        .certificates
        .process(RetryCertificate {
            actor: maintainer(),
            id: cert.id.clone(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(denied, OrchestrationError::PermissionDenied),
        "{denied:?}"
    );
    let retried = w
        .certificates
        .process(RetryCertificate {
            actor: operator(),
            id: cert.id.clone(),
        })
        .await?;
    assert_eq!(retried.status, CertificateStatus::Pending);
    assert!(retried.last_error.is_none());
    assert!(retried.last_attempt_at.is_none());

    let denied = w
        .certificates
        .process(DeleteCertificate {
            actor: maintainer(),
            id: cert.id.clone(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(denied, OrchestrationError::PermissionDenied),
        "{denied:?}"
    );
    let refused = w
        .certificates
        .process(DeleteCertificate {
            actor: operator(),
            id: cert.id.clone(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(refused, OrchestrationError::Conflict(_)),
        "{refused:?}"
    );

    w.db.process(DeleteNodeRow {
        id: entry.node.id.clone(),
        canvas: c.id.clone(),
        import_sync: None,
        frees_canvas: None,
        fence: None,
    })
    .await?;
    w.certificates
        .process(DeleteCertificate {
            actor: operator(),
            id: cert.id.clone(),
        })
        .await?;
    assert!(
        w.db.process(FindCertificateById {
            id: cert.id.clone()
        })
        .await?
        .is_none()
    );
    let missing = w
        .certificates
        .process(RetryCertificate {
            actor: operator(),
            id: cert.id,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(missing, OrchestrationError::NotFound),
        "{missing:?}"
    );
    Ok(())
}

// --- the issuance pipeline with a fake CA --------------------------------------

/// What the pipeline handed the issuer, per call.
#[derive(Debug, Clone)]
struct SeenRequest {
    directory: String,
    sni: String,
    account_credentials: Option<String>,
}

/// Answers every order with a self-signed certificate for the SNI, or with the
/// configured failure.
struct FakeIssuer {
    seen: Mutex<Vec<SeenRequest>>,
    fail_with: Option<String>,
}

impl AcmeIssuer for FakeIssuer {
    fn issue<'a>(
        &'a self,
        request: IssueRequest<'a>,
    ) -> BoxFuture<'a, Result<IssuedMaterial, AcmeError>> {
        Box::pin(async move {
            self.seen.lock().await.push(SeenRequest {
                directory: request.directory.to_string(),
                sni: request.sni.to_string(),
                account_credentials: request.account_credentials.map(str::to_string),
            });
            if let Some(error) = &self.fail_with {
                return Err(AcmeError::Order(error.clone()));
            }
            let key = rcgen::KeyPair::generate().unwrap();
            let mut params = rcgen::CertificateParams::new(vec![request.sni.to_string()]).unwrap();
            params.not_before = rcgen::date_time_ymd(2026, 1, 1);
            params.not_after = rcgen::date_time_ymd(2026, 4, 1);
            let cert = params.self_signed(&key).unwrap();
            Ok(IssuedMaterial {
                account_credentials: format!("creds-for-{}", request.sni),
                private_key_pem: key.serialize_pem(),
                full_chain_pem: cert.pem(),
            })
        })
    }
}

fn with_issuer(w: &World, issuer: Arc<dyn AcmeIssuer>) -> AcmeService {
    AcmeService {
        issuer,
        ..w.certificates.clone()
    }
}

#[tokio::test]
async fn issue_certificate_stores_encrypted_material_and_touches_canvases() -> TestResult {
    let w = world().await?;
    let provider = create_provider(&w, "cf", "cf-token").await;
    let c = canvas(&w.db, "prod").await?;
    node(
        &w.db,
        &c,
        "edge",
        entry_spec(&provider, "A.Example.com", ""),
        entry_ports(),
    )
    .await?;
    let quiet = canvas(&w.db, "quiet").await?;
    let prod_before = generation(&w, &c).await;
    let quiet_before = generation(&w, &quiet).await;

    let issuer = Arc::new(FakeIssuer {
        seen: Mutex::new(Vec::new()),
        fail_with: None,
    });
    let acme = with_issuer(&w, issuer.clone());

    // The cron's first step keys the row by the canonical SNI and the resolved
    // directory.
    let ensured = acme.process(EnsureRequestedCertificates).await?;
    assert_eq!(ensured.len(), 1);
    let row =
        w.db.process(FindCertificateById {
            id: ensured[0].clone(),
        })
        .await?
        .unwrap();
    assert_eq!(row.sni, "a.example.com");
    assert_eq!(row.acme_directory, w.config.acme_directory(""));
    assert_eq!(
        acme.process(EnsureRequestedCertificates).await?.len(),
        1,
        "a second pass finds the row instead of creating one"
    );

    let outcome = acme
        .process(IssueCertificate { id: row.id.clone() })
        .await?;
    let issued = match outcome {
        IssueOutcome::Issued(issued) => issued,
        other => panic!("{other:?}"),
    };
    assert_eq!(issued.status, CertificateStatus::Issued);
    assert_eq!(issued.version, 1);
    assert_eq!(
        issued.not_before.unwrap().to_rfc3339(),
        "2026-01-01T00:00:00+00:00"
    );
    assert_eq!(
        issued.not_after.unwrap().to_rfc3339(),
        "2026-04-01T00:00:00+00:00"
    );
    assert!(
        issued
            .full_chain_pem
            .as_deref()
            .unwrap()
            .starts_with("-----BEGIN CERTIFICATE-----")
    );
    let key = issued.private_key_pem.as_deref().unwrap();
    assert!(key.starts_with("enc1:"));
    assert!(
        w.secrets
            .decrypt_str(key)?
            .starts_with("-----BEGIN PRIVATE KEY-----")
    );
    let account = issued.acme_account_key.as_deref().unwrap();
    assert!(account.starts_with("enc1:"));
    assert_eq!(w.secrets.decrypt_str(account)?, "creds-for-a.example.com");

    assert_eq!(
        generation(&w, &c).await,
        prod_before + 1,
        "the canvas serving the SNI is touched"
    );
    assert_eq!(
        generation(&w, &quiet).await,
        quiet_before,
        "other canvases are not"
    );

    // A renewal hands the stored account back to the issuer, decrypted.
    let outcome = acme
        .process(IssueCertificate { id: row.id.clone() })
        .await?;
    assert!(
        matches!(&outcome, IssueOutcome::Issued(r) if r.version == 2),
        "{outcome:?}"
    );
    let seen = issuer.seen.lock().await.clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].sni, "a.example.com");
    assert_eq!(seen[0].directory, w.config.acme_directory(""));
    assert_eq!(seen[0].account_credentials, None);
    assert_eq!(
        seen[1].account_credentials.as_deref(),
        Some("creds-for-a.example.com")
    );
    Ok(())
}

#[tokio::test]
async fn issue_certificate_records_a_failure_without_touching_anything() -> TestResult {
    let w = world().await?;
    let provider = create_provider(&w, "cf", "cf-token").await;
    let c = canvas(&w.db, "prod").await?;
    node(
        &w.db,
        &c,
        "edge",
        entry_spec(&provider, "a.example.com", ""),
        entry_ports(),
    )
    .await?;
    let before =
        w.db.process(FindCanvasById { id: c.id.clone() })
            .await?
            .unwrap()
            .generation;
    let acme = with_issuer(
        &w,
        Arc::new(FakeIssuer {
            seen: Mutex::new(Vec::new()),
            fail_with: Some("CAA record forbids issuance".into()),
        }),
    );
    let ensured = acme.process(EnsureRequestedCertificates).await?;
    let outcome = acme
        .process(IssueCertificate {
            id: ensured[0].clone(),
        })
        .await?;
    match outcome {
        IssueOutcome::Failed { sni, error } => {
            assert_eq!(sni, "a.example.com");
            assert!(error.contains("CAA record forbids issuance"), "{error}");
        }
        other => panic!("{other:?}"),
    }
    let row =
        w.db.process(FindCertificateById {
            id: ensured[0].clone(),
        })
        .await?
        .unwrap();
    assert_eq!(row.status, CertificateStatus::Failed);
    assert!(
        row.last_error
            .unwrap()
            .contains("CAA record forbids issuance")
    );
    assert!(row.last_attempt_at.is_some());
    assert!(row.private_key_pem.is_none());
    assert_eq!(
        w.db.process(FindCanvasById { id: c.id.clone() })
            .await?
            .unwrap()
            .generation,
        before,
        "a failure publishes nothing"
    );

    // A provider that vanished underneath the row is a recorded failure too.
    let missing = w
        .certificates
        .process(IssueCertificate {
            id: orchestration::utils::ids::certificate_id("nope"),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(missing, OrchestrationError::NotFound),
        "{missing:?}"
    );
    Ok(())
}

/// An issuer that parks inside `issue` until the test releases it, so a second
/// pass provably reaches the row claim while the first one is still ordering.
struct GatedIssuer {
    ordered: Mutex<Vec<String>>,
    /// A permit appears once `issue` has been entered.
    entered: tokio::sync::Semaphore,
    /// Lets the parked `issue` finish.
    release: tokio::sync::Notify,
}

impl AcmeIssuer for GatedIssuer {
    fn issue<'a>(
        &'a self,
        request: IssueRequest<'a>,
    ) -> BoxFuture<'a, Result<IssuedMaterial, AcmeError>> {
        Box::pin(async move {
            self.ordered.lock().await.push(request.sni.to_string());
            self.entered.add_permits(1);
            self.release.notified().await;
            let key = rcgen::KeyPair::generate().unwrap();
            let mut params = rcgen::CertificateParams::new(vec![request.sni.to_string()]).unwrap();
            params.not_before = rcgen::date_time_ymd(2026, 1, 1);
            params.not_after = rcgen::date_time_ymd(2026, 4, 1);
            let cert = params.self_signed(&key).unwrap();
            Ok(IssuedMaterial {
                account_credentials: format!("creds-for-{}", request.sni),
                private_key_pem: key.serialize_pem(),
                full_chain_pem: cert.pem(),
            })
        })
    }
}

/// The job claim cannot protect the ACME pass on its own: an order takes minutes
/// and the interval is a minute, so a second signal legitimately claims a run
/// while the first pass is still working.
///
/// The gated issuer is what makes the overlap real rather than hoped for: pass A
/// parks inside the order and only then does pass B run, to completion, so B
/// reaches the row while A holds it. Drop the row claim (and the attempt stamp it
/// writes) and B lists the row as due and hands the CA a second order for the
/// same name — the assertion below counts orders, so the test fails.
#[tokio::test]
async fn two_overlapping_renewal_passes_order_one_certificate_per_row() -> TestResult {
    let w = world().await?;
    let provider = create_provider(&w, "cf", "cf-token").await;
    let c = canvas(&w.db, "prod").await?;
    node(
        &w.db,
        &c,
        "edge",
        entry_spec(&provider, "a.example.com", DIRECTORY),
        entry_ports(),
    )
    .await?;
    let issuer = Arc::new(GatedIssuer {
        ordered: Mutex::new(Vec::new()),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Notify::new(),
    });
    let acme = with_issuer(&w, issuer.clone());

    let pass_a = tokio::spawn({
        let acme = acme.clone();
        async move { renew_due(&acme).await }
    });
    // A has claimed the row and is inside the order.
    issuer.entered.acquire().await?.forget();

    renew_due(&acme).await;
    assert_eq!(
        issuer.ordered.lock().await.len(),
        1,
        "the second pass must skip the row the first one is ordering"
    );

    issuer.release.notify_one();
    pass_a.await?;

    let ordered = issuer.ordered.lock().await.clone();
    assert_eq!(
        ordered,
        vec!["a.example.com".to_string()],
        "one order for the one certificate that was due"
    );
    let rows = acme.process(ListCertificates { actor: operator() }).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, CertificateStatus::Issued);
    Ok(())
}

/// The claim itself, without task scheduling in the way: it is a compare-and-set
/// on the `last_attempt_at` the pass observed, so two passes holding the same
/// observation produce exactly one claim, and an observation that the row has
/// moved past is worthless however old it is.
#[tokio::test]
async fn claiming_an_attempt_compares_and_sets_the_observed_value() -> TestResult {
    let w = world().await?;
    let provider = create_provider(&w, "cf", "tok").await;
    let cert = ensure(&w, &provider, "a.example.com", DIRECTORY).await;
    assert!(cert.last_attempt_at.is_none());

    let claim = |seen| ClaimCertificateAttempt {
        id: cert.id.clone(),
        now: Utc::now(),
        seen_attempt_at: seen,
    };
    let (first, second) = tokio::join!(w.db.process(claim(None)), w.db.process(claim(None)));
    assert_eq!(
        [first?, second?].into_iter().filter(|won| *won).count(),
        1,
        "both passes read `last_attempt_at` as unset; only one UPDATE can match"
    );

    // What a pass holding a minutes-old listing carries: another pass renewed the
    // row in between, so the observation is stale and the claim is refused — even
    // though the retry window has long passed.
    let stale = find(&w, &cert).await.last_attempt_at;
    assert!(stale.is_some());
    store_issued(&w, &cert, Utc::now() + TimeDelta::days(90)).await;
    assert!(
        !w.db
            .process(ClaimCertificateAttempt {
                id: cert.id.clone(),
                now: Utc::now() + TimeDelta::days(1),
                seen_attempt_at: stale,
            })
            .await?,
        "a stale observation cannot claim a row that has been renewed since"
    );

    // The next listing reads the renewal's stamp, and that claims.
    let current = find(&w, &cert).await.last_attempt_at;
    assert_ne!(current, stale);
    assert!(w.db.process(claim(current)).await?);
    Ok(())
}
