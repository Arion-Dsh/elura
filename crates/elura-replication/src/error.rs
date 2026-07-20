use std::fmt;

/// Result returned by entity replication primitives.
pub type ReplicationResult<T> = std::result::Result<T, ReplicationError>;

/// Configuration, stream, entity, or state-baseline failure.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplicationError {
    /// A configured bound cannot produce a safe replication stream.
    InvalidConfig(&'static str),
    /// A Tick or batch sequence violates stream ordering.
    InvalidOrder(&'static str),
    /// A packet or event violates protocol invariants.
    InvalidPacket(&'static str),
    /// Unacknowledged batches filled the sender's bounded history.
    HistoryFull,
    /// The sender cannot allocate another batch sequence.
    SequenceExhausted,
    /// A peer acknowledged a batch that was never issued or reported the wrong Tick.
    InvalidAcknowledgement,
    /// The configured maximum number of replicated entities was exceeded.
    EntityLimitExceeded,
    /// A new state version moved backwards without a stream reset.
    VersionRegression,
    /// A delta referenced a state version not present on the receiver.
    BaselineMismatch,
    /// The application-provided delta decoder rejected an update.
    DeltaRejected,
}

impl fmt::Display for ReplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => {
                write!(formatter, "invalid replication config: {message}")
            }
            Self::InvalidOrder(message) => {
                write!(formatter, "invalid replication order: {message}")
            }
            Self::InvalidPacket(message) => {
                write!(formatter, "invalid replication packet: {message}")
            }
            Self::HistoryFull => formatter.write_str("unacknowledged replication history is full"),
            Self::SequenceExhausted => formatter.write_str("replication sequence is exhausted"),
            Self::InvalidAcknowledgement => {
                formatter.write_str("invalid replication acknowledgement")
            }
            Self::EntityLimitExceeded => formatter.write_str("replicated entity limit exceeded"),
            Self::VersionRegression => {
                formatter.write_str("replicated entity version moved backwards")
            }
            Self::BaselineMismatch => {
                formatter.write_str("replication delta baseline does not match")
            }
            Self::DeltaRejected => formatter.write_str("replication delta was rejected"),
        }
    }
}

impl std::error::Error for ReplicationError {}
