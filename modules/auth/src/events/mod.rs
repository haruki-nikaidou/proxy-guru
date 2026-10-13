//! Event definitions.
//!
//! Put the event payloads this module publishes or consumes here. Events are
//! how modules communicate asynchronously without depending on each other's
//! internals: one module publishes through Iggy, others react from their
//! [`hooks`](crate::hooks). The stream and topology every module shares are in
//! `base::events`.
//!
//! Document each event's contract in a doc comment — who publishes it, who
//! consumes it, its topic and type tag. Derive the `rkyv` traits for the
//! payload and implement `EventBody` + `Event` from `wakuwaku::iggy`.
//!
//! ```ignore
//! use kanau::{RkyvMessageDe, RkyvMessageSer};
//! use wakuwaku::iggy::{
//!     Event, EventAtomicOrdering, EventBody, EventTypeTag, IggySuperPartitionName,
//!     NonAssociative, PartitionKey,
//! };
//!
//! #[derive(Hash)]
//! pub struct ExampleKey(pub uuid::Uuid);
//!
//! impl PartitionKey for ExampleKey {
//!     type PartitionMerge = std::hash::DefaultHasher;
//!     const SUPER_PARTITION_NAME: IggySuperPartitionName = IggySuperPartitionName("auth_example");
//! }
//!
//! /// **Public event**
//! ///
//! /// Published by: auth module
//! /// Consumed by: (your consumers)
//! /// Topic: `auth_example`, tag 1
//! #[derive(
//!     Debug, Clone,
//!     rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
//!     RkyvMessageSer, RkyvMessageDe,
//! )]
//! pub struct ExampleHappened {
//!     pub id: uuid::Uuid,
//! }
//!
//! impl EventBody for ExampleHappened {}
//!
//! impl Event for ExampleHappened {
//!     const TYPE_TAG: EventTypeTag = EventTypeTag::new(1);
//!     type Key = ExampleKey;
//!     type Algebra = NonAssociative;
//!     fn key(&self) -> ExampleKey { ExampleKey(self.id) }
//!     fn atomic_ordering(&self) -> EventAtomicOrdering { EventAtomicOrdering::Relaxed }
//! }
//! ```
