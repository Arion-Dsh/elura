//! Transport-independent, per-observer entity replication.
//!
//! [`ReplicationSender`] converts a current visible entity set into bounded `Spawn`, `Despawn`,
//! `Update`, and `Keyframe` batches. [`ReplicationReceiver`] buffers reordered batches and applies
//! them only after every preceding sequence is present. The application owns AOI policy, entity
//! state encoding, delta construction, and transport delivery.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod error;
mod protocol;
mod receiver;
mod sender;

pub use error::{ReplicationError, ReplicationResult};
pub use protocol::{
    ReplicationAck, ReplicationBatch, ReplicationConfig, ReplicationEvent, ReplicationPacket,
    VersionedState,
};
pub use receiver::{ReplicationReceiveReport, ReplicationReceiver};
pub use sender::ReplicationSender;
