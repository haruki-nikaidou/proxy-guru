//! The published `guru-worker` release: what `manage-tool agent publish` last
//! put in the download directory, so the dashboard can offer it to servers
//! running another version and a worker can verify what it downloads.
//!
//! One row, keyed `current`. It is state, not configuration: a publish replaces
//! it and takes effect at once.

use base::db::{Db, Error};
use chrono::{DateTime, Utc};
use db_types::table_record;
use kanau::processor::Processor;

table_record!(AgentReleaseId, "orchestration_agent_release");

/// The key of the single release row.
pub const AGENT_RELEASE_KEY: &str = "current";

/// The id of the single release row.
pub fn agent_release_id() -> AgentReleaseId {
    AgentReleaseId::from_key(AGENT_RELEASE_KEY)
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

impl Processor<PublishAgentRelease> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:PublishAgentRelease", skip_all, err)]
    async fn process(&self, input: PublishAgentRelease) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "INSERT INTO orchestration_agent_release (id, version, sha256, arch, published_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (id) DO UPDATE
                 SET version = EXCLUDED.version, sha256 = EXCLUDED.sha256,
                     arch = EXCLUDED.arch, published_at = EXCLUDED.published_at",
            agent_release_id() as _,
            input.version,
            input.sha256,
            input.arch,
            input.now
        )
        .execute(self.db())
        .await?;
        Ok(())
    }
}

pub struct FindAgentRelease;

impl Processor<FindAgentRelease> for Db {
    type Output = Option<AgentReleaseEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindAgentRelease", skip_all, err)]
    async fn process(&self, _: FindAgentRelease) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            AgentReleaseEntity,
            r#"SELECT id AS "id: AgentReleaseId", version, sha256, arch, published_at
               FROM orchestration_agent_release WHERE id = $1"#,
            agent_release_id() as _
        )
        .fetch_optional(self.db())
        .await?)
    }
}
