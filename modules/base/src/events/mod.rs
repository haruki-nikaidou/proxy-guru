//! Iggy plumbing shared by every module's events.
//!
//! Events are how modules communicate asynchronously without depending on
//! each other's internals: one module publishes, others react from their
//! [`hooks`](crate::hooks). The contract every module follows:
//!
//! - One stream, [`STREAM`], holds every topic.
//! - An event's topic is its key type's `PartitionKey::SUPER_PARTITION_NAME`;
//!   every main topic is created with [`PARTITIONS`] partition (id `0`). The
//!   fleet is small, so one partition carries every key; a consumer still names
//!   the partitions it reads (`guru-master --iggy-partitions`), so nothing ever
//!   reads "every partition" by accident once a topic is split.
//! - A consumer's retry topic, `retry_<topic>`, is created by wakuwaku when the
//!   consumer starts, with the main topic's partition count.
//! - Each module's `events` lists its main topics in a `TOPICS` constant, which
//!   `guru-master` passes to [`ensure_topology`] at startup.
//!
//! Document each event's contract in a doc comment — who publishes it, who
//! consumes it, and its topic and type tag. Derive the `rkyv` traits for the
//! payload with `RkyvMessageSer`/`RkyvMessageDe`, and implement `EventBody` +
//! `Event` from `wakuwaku::iggy`.

use std::sync::Arc;
use wakuwaku::deps::iggy::prelude::{
    Client, Identifier, IggyClient, IggyError, StreamClient, TopicClient, TopicCreateOptions,
};

/// The stream every topic lives in.
pub const STREAM: &str = "guru";

/// Partition count of every main topic this process creates. Raising it
/// reorders keys (a key's partition is its hash modulo this count) and needs
/// the new partitions assigned to consumer replicas explicitly.
pub const PARTITIONS: u32 = 1;

/// Connects to the Iggy server at `url` (`iggy://user:password@host:port`);
/// the credentials in the URL log the client in.
pub async fn connect(url: &str) -> Result<Arc<IggyClient>, IggyError> {
    let client = IggyClient::from_connection_string(url)?;
    client.connect().await?;
    Ok(Arc::new(client))
}

/// Why the topology could not be ensured.
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error(transparent)]
    Iggy(#[from] IggyError),
    /// The topic exists with another partition count, so a consumer naming
    /// partitions from [`PARTITIONS`] would silently miss the keys hashed to
    /// the others. Recreate the topic (or migrate it) instead of starting.
    #[error(
        "topic {topic} has {found} partitions, expected {expected}: delete and recreate it \
         (its unconsumed events are lost) or set the consumers' partitions to match"
    )]
    PartitionCount {
        topic: String,
        found: u32,
        expected: u32,
    },
}

/// Creates [`STREAM`] and each of `topics` (with [`PARTITIONS`] partitions)
/// when absent, and refuses an existing topic with another partition count.
/// Several processes may start together, so losing the race to create one
/// counts as success.
pub async fn ensure_topology(client: &IggyClient, topics: &[&str]) -> Result<(), TopologyError> {
    let stream = Identifier::named(STREAM)?;
    if client.get_stream(&stream).await?.is_none() {
        match client.create_stream(STREAM).await {
            Ok(_) | Err(IggyError::StreamNameAlreadyExists(..)) => {}
            Err(e) => return Err(e.into()),
        }
    }
    for &topic in topics {
        if let Some(existing) = client
            .get_topic(&stream, &Identifier::named(topic)?)
            .await?
        {
            check_partitions(topic, existing.partitions_count)?;
            continue;
        }
        let options = TopicCreateOptions {
            partitions_count: Some(PARTITIONS),
            ..Default::default()
        };
        match client.create_topic(&stream, topic, &options).await {
            Ok(_) | Err(IggyError::TopicNameAlreadyExists(..)) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

fn check_partitions(topic: &str, found: u32) -> Result<(), TopologyError> {
    if found == PARTITIONS {
        Ok(())
    } else {
        Err(TopologyError::PartitionCount {
            topic: topic.to_owned(),
            found,
            expected: PARTITIONS,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_topic_left_with_another_partition_count_is_refused() {
        assert!(check_partitions("t", PARTITIONS).is_ok());
        let refused = check_partitions("orchestration_canvas_dirty", 4);
        assert!(matches!(
            refused,
            Err(TopologyError::PartitionCount { ref topic, found: 4, expected: PARTITIONS })
                if topic == "orchestration_canvas_dirty"
        ));
    }
}
