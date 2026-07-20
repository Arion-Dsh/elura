use std::fmt;

/// Result returned by netcode primitives.
pub type NetcodeResult<T> = std::result::Result<T, NetcodeError>;

/// Configuration, timing, or input validation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NetcodeError {
    /// A configured limit cannot produce a bounded state machine.
    InvalidConfig(&'static str),
    /// A Tick synchronization observation is internally inconsistent.
    InvalidSample(&'static str),
    /// An input packet or frame violates its configured bounds.
    InvalidInput(&'static str),
    /// Unacknowledged input filled the sender's bounded history.
    InputHistoryFull,
    /// The local input sequence can no longer be incremented.
    SequenceExhausted,
    /// A peer acknowledged an input sequence that was never issued.
    InvalidAcknowledgement,
    /// Unconfirmed prediction frames filled the bounded client history.
    PredictionHistoryFull,
    /// One authoritative correction would replay more inputs than configured.
    ReplayLimitExceeded,
    /// A remote interpolation sample was requested before any state arrived.
    InterpolationBufferEmpty,
    /// Pending predicted entities filled the bounded matcher.
    PredictedEntityLimit,
}

impl fmt::Display for NetcodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid netcode config: {message}"),
            Self::InvalidSample(message) => write!(formatter, "invalid Tick sample: {message}"),
            Self::InvalidInput(message) => write!(formatter, "invalid input: {message}"),
            Self::InputHistoryFull => formatter.write_str("unacknowledged input history is full"),
            Self::SequenceExhausted => formatter.write_str("input sequence is exhausted"),
            Self::InvalidAcknowledgement => {
                formatter.write_str("peer acknowledged an input sequence that was never issued")
            }
            Self::PredictionHistoryFull => {
                formatter.write_str("unconfirmed prediction history is full")
            }
            Self::ReplayLimitExceeded => formatter.write_str("prediction replay limit exceeded"),
            Self::InterpolationBufferEmpty => {
                formatter.write_str("remote interpolation buffer is empty")
            }
            Self::PredictedEntityLimit => {
                formatter.write_str("pending predicted entity limit exceeded")
            }
        }
    }
}

impl std::error::Error for NetcodeError {}
