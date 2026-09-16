//! The server's own agent key: issued together with the install command, and
//! checked at registration in place of an operator API key.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use kanau::processor::Processor;
use orchestration::entities::db::agent_release::PublishAgentRelease;
use orchestration::entities::db::server::{FindServerById, ReleaseServerWatchSession, ServerId};
use orchestration::services::OrchestrationError;
use orchestration::services::agent::{
    AgentIdentity, AgentUpdate, PollAgentUpdate, RegisterCredential, RegisterWorker,
};
use orchestration::services::server::{
    GetAgentRelease, IssueServerAgentInstall, RequestAgentUpdate,
};
use orchestration::utils::ids::record_key;

/// A world whose config knows the public origin, with one published release.
async fn published_world(pool: sqlx::PgPool) -> Result<World, Box<dyn std::error::Error>> {
    let mut w = world(pool).await?;
    w.servers.config.agent_public_base_url = "https://guru.test".to_string();
    w.agents.config.agent_public_base_url = "https://guru.test".to_string();
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
        last_update_error: None,
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

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn the_install_command_carries_a_key_that_registers_only_its_server(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = published_world(pool).await?;
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
    assert_eq!(
        install.unit, "hk-edge-1",
        "the unit is the server name as a slug"
    );
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
        assert!(
            install.command.contains(expected),
            "missing {expected} in {}",
            install.command
        );
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
        .process(register(
            RegisterCredential::ServerKey(secret.clone()),
            &a.id,
        ))
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
        .process(register(
            RegisterCredential::ServerKey("gs_nope".to_string()),
            &a.id,
        ))
        .await
        .expect_err("an unknown key is refused");
    assert!(matches!(err, OrchestrationError::PermissionDenied), "{err}");
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn reissuing_replaces_the_key_and_keeps_the_stored_unit(pool: sqlx::PgPool) -> TestResult {
    let w = published_world(pool).await?;
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

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn issuing_needs_an_origin_a_release_and_a_key_manager(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
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
    assert_eq!(
        info.release.map(|r| r.version).as_deref(),
        Some("0.2.0-beta")
    );
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

/// Registers the worker of `server` as `version`, releasing the lease a
/// previous registration holds first.
async fn register_as(
    w: &World,
    server: &ServerId,
    version: &str,
    last_update_error: Option<&str>,
) -> Result<AgentIdentity, Box<dyn std::error::Error>> {
    if let Some(row) = w.db.process(FindServerById { id: server.clone() }).await? {
        w.db.process(ReleaseServerWatchSession {
            server: server.clone(),
            generation: row.refresh_key_generation,
            epoch: row.watch_epoch,
        })
        .await?;
    }
    w.agents
        .process(RegisterWorker {
            credential: RegisterCredential::Operator(machine()),
            server_id: server.clone(),
            running_revision: 0,
            observed: None,
            reported: None,
            agent_version: Some(version.to_string()),
            agent_arch: Some("x86_64".to_string()),
            last_update_error: last_update_error.map(str::to_owned),
        })
        .await?;
    let row =
        w.db.process(FindServerById { id: server.clone() })
            .await?
            .expect("registered");
    Ok(AgentIdentity {
        server: server.clone(),
        generation: row.refresh_key_generation,
    })
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_update_is_offered_once_requested_and_settled_by_what_the_worker_reports(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = published_world(pool).await?;
    let c = canvas(&w.db, "prod").await?;
    let a = server(&w.db, &c, "edge").await?;
    let row = |w: &World| {
        let id = a.id.clone();
        let db = w.db.clone();
        async move {
            db.process(FindServerById { id })
                .await
                .map(|r| r.expect("row"))
        }
    };

    // Nothing to offer before the operator asks, and nothing to ask for a
    // worker that never reported a version.
    let err = w
        .servers
        .process(RequestAgentUpdate {
            actor: operator(),
            server: a.id.clone(),
        })
        .await
        .expect_err("no version yet");
    assert!(matches!(err, OrchestrationError::Invalid(_)), "{err}");
    let agent = register_as(&w, &a.id, "0.1.0", None).await?;
    assert_eq!(
        w.agents
            .process(PollAgentUpdate {
                agent: agent.clone(),
                last_error: None,
            })
            .await?,
        None
    );

    // Requested: the poll hands out the published binary under the master's origin.
    let updated = w
        .servers
        .process(RequestAgentUpdate {
            actor: operator(),
            server: a.id.clone(),
        })
        .await?;
    assert_eq!(
        updated.agent_update_requested.as_deref(),
        Some("0.2.0-beta")
    );
    let offered = w
        .agents
        .process(PollAgentUpdate {
            agent: agent.clone(),
            last_error: None,
        })
        .await?;
    assert_eq!(
        offered,
        Some(AgentUpdate {
            version: "0.2.0-beta".to_string(),
            url: "https://guru.test/agent/0.2.0-beta/guru-worker".to_string(),
            sha256: "c".repeat(64),
        })
    );

    // A failure the worker reports ends the request and keeps the reason.
    assert_eq!(
        w.agents
            .process(PollAgentUpdate {
                agent: agent.clone(),
                last_error: Some("checksum mismatch".to_string()),
            })
            .await?,
        None
    );
    let r = row(&w).await?;
    assert_eq!(r.agent_update_requested, None);
    assert_eq!(r.agent_update_error.as_deref(), Some("checksum mismatch"));

    // Asked again and the worker comes back as the new version: settled cleanly.
    w.servers
        .process(RequestAgentUpdate {
            actor: operator(),
            server: a.id.clone(),
        })
        .await?;
    register_as(&w, &a.id, "0.2.0-beta", None).await?;
    let r = row(&w).await?;
    assert_eq!(r.agent_version.as_deref(), Some("0.2.0-beta"));
    assert_eq!(r.agent_update_requested, None);
    assert_eq!(r.agent_update_error, None);
    // … and there is nothing further to move to.
    let err = w
        .servers
        .process(RequestAgentUpdate {
            actor: operator(),
            server: a.id.clone(),
        })
        .await
        .expect_err("already on the published release");
    assert!(matches!(err, OrchestrationError::Invalid(_)), "{err}");

    // A rollback the start guard performed arrives with the next registration.
    w.db.process(PublishAgentRelease {
        version: "0.3.0".to_string(),
        sha256: "d".repeat(64),
        arch: "x86_64".to_string(),
        now: chrono::Utc::now(),
    })
    .await?;
    w.servers
        .process(RequestAgentUpdate {
            actor: operator(),
            server: a.id.clone(),
        })
        .await?;
    register_as(
        &w,
        &a.id,
        "0.2.0-beta",
        Some("0.3.0: did not come up in 3 starts"),
    )
    .await?;
    let r = row(&w).await?;
    assert_eq!(r.agent_update_requested, None);
    assert_eq!(
        r.agent_update_error.as_deref(),
        Some("0.3.0: did not come up in 3 starts")
    );

    // A request outlived by a newer publish is dropped with a reason, not served.
    w.servers
        .process(RequestAgentUpdate {
            actor: operator(),
            server: a.id.clone(),
        })
        .await?;
    w.db.process(PublishAgentRelease {
        version: "0.4.0".to_string(),
        sha256: "e".repeat(64),
        arch: "x86_64".to_string(),
        now: chrono::Utc::now(),
    })
    .await?;
    let agent = register_as(&w, &a.id, "0.2.0-beta", None).await?;
    assert_eq!(
        w.agents
            .process(PollAgentUpdate {
                agent,
                last_error: None,
            })
            .await?,
        None
    );
    let r = row(&w).await?;
    assert_eq!(r.agent_update_requested, None);
    assert!(
        r.agent_update_error
            .as_deref()
            .is_some_and(|e| e.contains("0.4.0")),
        "{:?}",
        r.agent_update_error
    );
    Ok(())
}
