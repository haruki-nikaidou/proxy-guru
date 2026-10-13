//! The published `guru-worker` release: what `manage-tool agent publish` last
//! put in the download directory, so the dashboard can offer it to servers
//! running another version and a worker can verify what it downloads.
//!
//! One row, keyed `current`. It is state, not configuration: a publish replaces
//! it and takes effect at once.

use crate::entities::db::tables::OrchestrationAgentRelease;
use base::db::Error;
use db_types::table_record;
use sqlx::PgConnection;
use time::OffsetDateTime;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::{Execute, Query, Read, Write};

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
    pub published_at: OffsetDateTime,
}

/// Replaces the published release.
#[derive(Debug)]
pub struct PublishAgentRelease {
    pub version: String,
    pub sha256: String,
    pub arch: String,
    pub now: OffsetDateTime,
}

impl Query for PublishAgentRelease {
    type Effect = Write<OrchestrationAgentRelease>;
}

impl Execute<PgSource> for PublishAgentRelease {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:PublishAgentRelease", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "INSERT INTO orchestration_agent_release (id, version, sha256, arch, published_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (id) DO UPDATE
                 SET version = EXCLUDED.version, sha256 = EXCLUDED.sha256,
                     arch = EXCLUDED.arch, published_at = EXCLUDED.published_at",
            agent_release_id() as _,
            self.version,
            self.sha256,
            self.arch,
            self.now
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

pub struct FindAgentRelease;

impl Query for FindAgentRelease {
    type Effect = Read<OrchestrationAgentRelease>;
}

impl Execute<PgSource> for FindAgentRelease {
    type Output = Option<AgentReleaseEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindAgentRelease", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            AgentReleaseEntity,
            r#"SELECT id AS "id: AgentReleaseId", version, sha256, arch, published_at
               FROM orchestration_agent_release WHERE id = $1"#,
            agent_release_id() as _
        )
        .fetch_optional(conn)
        .await?)
    }
}
