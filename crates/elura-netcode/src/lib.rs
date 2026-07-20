//! Transport-independent realtime netcode primitives.
//!
//! This crate provides client/server tick estimation, bounded redundant input history, cumulative
//! acknowledgements, replay-safe out-of-order input reception, client prediction reconciliation,
//! adaptive remote-state interpolation, and predicted-entity matching. It deliberately does not
//! open sockets, create tasks, simulate game rules, or interpolate application state itself.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod entity_prediction;
mod error;
mod input;
mod interpolation;
mod prediction;
mod tick_sync;

pub use entity_prediction::{
    EntityMatch, PredictedEntity, PredictedEntityConfig, PredictedEntityMatcher, PredictionKey,
    PredictionKeyGenerator,
};
pub use error::{NetcodeError, NetcodeResult};
pub use input::{
    InputAck, InputFrame, InputPacket, InputReceiveReport, InputReceiver, InputReceiverConfig,
    InputSender, InputSenderConfig, SequenceDisposition, SequenceWindow,
};
pub use interpolation::{
    InterpolationBuffer, InterpolationConfig, InterpolationInsert, InterpolationSample,
    InterpolationStats,
};
pub use prediction::{PredictionBuffer, PredictionConfig, PredictionFrame, ReconciliationReport};
pub use tick_sync::{
    TickSyncConfig, TickSyncReport, TickSyncRequest, TickSyncResponse, TickSyncSample,
    TickSynchronizer,
};
