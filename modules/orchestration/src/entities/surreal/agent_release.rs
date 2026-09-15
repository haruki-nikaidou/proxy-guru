//! The published `guru-worker` release: what `manage-tool agent publish` last
//! put in the download directory, so the dashboard can offer it to servers
//! running another version and a worker can verify what it downloads.
//!
//! One row, `orchestration_agent_release:current`. It is state, not
//! configuration: a publish replaces it and takes effect at once.

use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use newtype_record_id::table_record;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(AgentReleaseId, "orchestration_agent_release");

/// The record key of the single release row.
pub const AGENT_RELEASE_KEY: &str = "current";

/// The id of the single release row.
pub fn agent_release_id() -> AgentReleaseId {
    AgentReleaseId(surrealdb::types::RecordId::new(
        "orchestration_agent_release",
        AGENT_RELEASE_KEY,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, SurrealValue)]
pub struct AgentReleaseEntity {
    pub id: AgentReleaseId,
    /// The worker crate version, as `guru-worker --version` prints it.
    pub version: String,
    /// Lowercase hex SHA-256 of the published binary.
    pub sha256: String,
    /// The CPU architecture the binary was built for (`x86_64`, `aarch64`).
    pub arch: String,
    pub published_at: DateTime<Utc>,
}

/// Replaces the published release.
#[derive(Debug)]
pub struct PublishAgentRelease {
    pub version: String,
    pub sha256: String,
    pub arch: String,
    pub now: DateTime<Utc>,
}

impl Processor<PublishAgentRelease> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:PublishAgentRelease", skip_all, err)]
    async fn process(&self, input: PublishAgentRelease) -> Result<Self::Output, Self::Error> {
        self.db()
            .query(
                "UPSERT $id CONTENT {
                     version: $version, sha256: $sha256, arch: $arch, published_at: $now
                 }",
            )
            .bind(("id", agent_release_id()))
            .bind(("version", input.version))
            .bind(("sha256", input.sha256))
            .bind(("arch", input.arch))
            .bind(("now", input.now))
            .await?
            .check()?;
        Ok(())
    }
}

pub struct FindAgentRelease;

impl Processor<FindAgentRelease> for SurrealProcessor {
    type Output = Option<AgentReleaseEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindAgentRelease", skip_all, err)]
    async fn process(&self, _: FindAgentRelease) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM ONLY $id")
            .bind(("id", agent_release_id()))
            .await?;
        resp.take::<Option<AgentReleaseEntity>>(0)
    }
}
