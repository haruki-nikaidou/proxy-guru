//! Iggy plumbing shared by every module's events.
//!
//! Events are how modules communicate asynchronously without depending on
//! each other's internals: one module publishes, others react from their
//! [`hooks`](crate::hooks). The contract every module follows:
//!
//! - One stream, [`STREAM`], holds every topic.
//! - An event's topic is its key type's `PartitionKey::SUPER_PARTITION_NAME`;
//!   every main topic has [`PARTITIONS`] partitions, ids `0..PARTITIONS`.
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

/// Partition count of every main topic (ids `0..PARTITIONS`). Changing it
/// reorders keys: a key's partition is its hash modulo this count.
pub const PARTITIONS: u32 = 4;

/// Every partition of a main topic, for a consumer that reads them all.
pub fn all_partitions() -> Vec<u32> {
    (0..PARTITIONS).collect()
}

/// Connects to the Iggy server at `url` (`iggy://user:password@host:port`);
/// the credentials in the URL log the client in.
pub async fn connect(url: &str) -> Result<Arc<IggyClient>, IggyError> {
    let client = IggyClient::from_connection_string(url)?;
    client.connect().await?;
    Ok(Arc::new(client))
}

/// Creates [`STREAM`] and each of `topics` (with [`PARTITIONS`] partitions)
/// when absent. Several processes may start together, so losing the race to
/// create one counts as success.
pub async fn ensure_topology(client: &IggyClient, topics: &[&str]) -> Result<(), IggyError> {
    let stream = Identifier::named(STREAM)?;
    if client.get_stream(&stream).await?.is_none() {
        match client.create_stream(STREAM).await {
            Ok(_) | Err(IggyError::StreamNameAlreadyExists(..)) => {}
            Err(e) => return Err(e),
        }
    }
    for &topic in topics {
        if client
            .get_topic(&stream, &Identifier::named(topic)?)
            .await?
            .is_some()
        {
            continue;
        }
        let options = TopicCreateOptions {
            partitions_count: Some(PARTITIONS),
            ..Default::default()
        };
        match client.create_topic(&stream, topic, &options).await {
            Ok(_) | Err(IggyError::TopicNameAlreadyExists(..)) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
