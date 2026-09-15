//! The server's own agent key: issued together with the install command, and
//! checked at registration in place of an operator API key.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use kanau::processor::Processor;
use orchestration::entities::surreal::agent_release::PublishAgentRelease;
use orchestration::entities::surreal::server::ServerId;
use orchestration::services::OrchestrationError;
use orchestration::services::agent::{RegisterCredential, RegisterWorker};
use orchestration::services::server::{GetAgentRelease, IssueServerAgentInstall};
use orchestration::utils::ids::record_key;

/// A world whose config knows the public origin, with one published release.
async fn published_world() -> Result<World, Box<dyn std::error::Error>> {
    let mut w = world().await?;
    w.servers.config.agent_public_base_url = "https://guru.test".to_string();
    w.db.process(PublishAgentRelease {
        version: "0.2.0-beta".to_string(),
        sha256: "c".repeat(64),
        arch: "x86_64".to_string(),
        now: chrono::Utc::now(),
    })
    .await?;
    Ok(w)
}

fn register(credential: RegisterCredential, server: &ServerId) -> RegisterWorker {
    RegisterWorker {
        credential,
        server_id: server.clone(),
        running_revision: 0,
        observed: None,
        reported: None,
        agent_version: None,
        agent_arch: None,
    }
}

/// The key the rendered command carries.
fn key_in(command: &str) -> String {
    command
        .split_whitespace()
        .find_map(|word| word.strip_prefix("GURU_API_KEY="))
        .expect("the command carries the key")
        .to_string()
}

#[tokio::test]
async fn the_install_command_carries_a_key_that_registers_only_its_server() -> TestResult {
    let w = published_world().await?;
    let c = canvas(&w.db, "prod").await?;
    let a = server(&w.db, &c, "HK Edge 1").await?;
    let b = server(&w.db, &c, "tokyo").await?;

    let install = w
        .servers
        .process(IssueServerAgentInstall {
            actor: operator(),
            server: a.id.clone(),
            unit: None,
        })
        .await?;
    assert_eq!(install.unit, "hk-edge-1", "the unit is the server name as a slug");
    assert_eq!(install.version, "0.2.0-beta");
    assert!(
        install
            .command
            .starts_with("curl -fsSL https://guru.test/agent/install.sh"),
        "{}",
        install.command
    );
    for expected in [
        "GURU_MASTER=https://guru.test",
        &format!("GURU_SERVER_ID={}", record_key(&a.id.0)),
        "GURU_UNIT=hk-edge-1",
        "GURU_AGENT_VERSION=0.2.0-beta",
        &format!("GURU_AGENT_SHA256={}", "c".repeat(64)),
    ] {
        assert!(install.command.contains(expected), "missing {expected} in {}", install.command);
    }
    assert!(
        !install.command.contains("GURU_DOWNLOAD_BASE"),
        "the default download base is not spelled out"
    );
    assert_eq!(install.server.agent_unit.as_deref(), Some("hk-edge-1"));
    assert!(install.server.agent_key_issued_at.is_some());
    let secret = key_in(&install.command);
    assert!(secret.starts_with("gs_"));
    assert_ne!(
        install.server.agent_key_digest.as_deref(),
        Some(secret.as_str()),
        "only the digest is stored"
    );

    // The key registers its own server …
    w.agents
        .process(register(RegisterCredential::ServerKey(secret.clone()), &a.id))
        .await?;
    // … never another one, and an unknown key registers nothing at all.
    let err = w
        .agents
        .process(register(RegisterCredential::ServerKey(secret), &b.id))
        .await
        .expect_err("a key is bound to the server it was issued for");
    assert!(matches!(err, OrchestrationError::PermissionDenied), "{err}");
    let err = w
        .agents
        .process(register(RegisterCredential::ServerKey("gs_nope".to_string()), &a.id))
        .await
        .expect_err("an unknown key is refused");
    assert!(matches!(err, OrchestrationError::PermissionDenied), "{err}");
    Ok(())
}

#[tokio::test]
async fn reissuing_replaces_the_key_and_keeps_the_stored_unit() -> TestResult {
    let w = published_world().await?;
    let c = canvas(&w.db, "prod").await?;
    // A name with nothing a slug can keep falls back to the record key …
    let a = server(&w.db, &c, "東京").await?;
    let first = w
        .servers
        .process(IssueServerAgentInstall {
            actor: operator(),
            server: a.id.clone(),
            unit: Some("tokyo-1".to_string()),
        })
        .await?;
    // … unless the operator typed one, which a later issue keeps.
    assert_eq!(first.unit, "tokyo-1");
    let second = w
        .servers
        .process(IssueServerAgentInstall {
            actor: operator(),
            server: a.id.clone(),
            unit: None,
        })
        .await?;
    assert_eq!(second.unit, "tokyo-1");

    let old = key_in(&first.command);
    let new = key_in(&second.command);
    assert_ne!(old, new);
    let err = w
        .agents
        .process(register(RegisterCredential::ServerKey(old), &a.id))
        .await
        .expect_err("the replaced key is refused");
    assert!(matches!(err, OrchestrationError::PermissionDenied), "{err}");
    w.agents
        .process(register(RegisterCredential::ServerKey(new), &a.id))
        .await?;
    Ok(())
}

#[tokio::test]
async fn issuing_needs_an_origin_a_release_and_a_key_manager() -> TestResult {
    let w = world().await?;
    let c = canvas(&w.db, "prod").await?;
    let a = server(&w.db, &c, "x").await?;
    let issue = |unit: Option<&str>| IssueServerAgentInstall {
        actor: operator(),
        server: a.id.clone(),
        unit: unit.map(str::to_owned),
    };

    let info = w
        .servers
        .process(GetAgentRelease { actor: operator() })
        .await?;
    assert!(!info.base_url_configured);
    assert!(info.release.is_none());
    let err = w.servers.process(issue(None)).await.expect_err("no origin");
    assert!(matches!(err, OrchestrationError::Invalid(_)), "{err}");

    let mut servers = w.servers.clone();
    servers.config.agent_public_base_url = "https://guru.test/".to_string();
    let err = servers.process(issue(None)).await.expect_err("no release");
    assert!(matches!(err, OrchestrationError::Invalid(_)), "{err}");

    w.db.process(PublishAgentRelease {
        version: "0.2.0-beta".to_string(),
        sha256: "c".repeat(64),
        arch: "x86_64".to_string(),
        now: chrono::Utc::now(),
    })
    .await?;
    let info = servers
        .process(GetAgentRelease { actor: operator() })
        .await?;
    assert!(info.base_url_configured);
    assert_eq!(info.release.map(|r| r.version).as_deref(), Some("0.2.0-beta"));
    // A trailing slash on the origin does not double up in the URLs.
    let install = servers.process(issue(Some("x-1"))).await?;
    assert!(
        install
            .command
            .starts_with("curl -fsSL https://guru.test/agent/install.sh"),
        "{}",
        install.command
    );
    // A machine credential is not a key manager.
    assert!(
        servers
            .process(IssueServerAgentInstall {
                actor: machine(),
                server: a.id.clone(),
                unit: None,
            })
            .await
            .is_err()
    );
    Ok(())
}
