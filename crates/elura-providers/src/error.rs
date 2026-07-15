use std::fmt;

#[derive(Debug)]
#[non_exhaustive]
pub enum ProviderError {
    Config(String),
    InvalidCredentials,
    Unavailable,
    Rejected(String),
    InvalidSignature,
    AlreadyProcessed,
    InvalidResponse(String),
    Unsupported,
}

impl fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => {
                write!(formatter, "invalid provider configuration: {message}")
            }
            Self::InvalidCredentials => formatter.write_str("invalid credentials"),
            Self::Unavailable => formatter.write_str("upstream provider unavailable"),
            Self::Rejected(message) => write!(formatter, "provider rejected request: {message}"),
            Self::InvalidSignature => formatter.write_str("invalid callback signature"),
            Self::AlreadyProcessed => formatter.write_str("callback was already processed"),
            Self::InvalidResponse(message) => {
                write!(formatter, "invalid provider response: {message}")
            }
            Self::Unsupported => formatter.write_str("unsupported provider operation"),
        }
    }
}

impl std::error::Error for ProviderError {}

pub type ProviderResult<T> = std::result::Result<T, ProviderError>;
