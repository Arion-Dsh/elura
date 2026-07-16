use std::{fmt, time::Duration};

/// Error returned by provider configuration, validation, or upstream operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum ProviderError {
    /// The provider was configured with an invalid value.
    Config(String),
    /// The caller supplied a malformed or internally inconsistent request.
    InvalidRequest(String),
    /// The requested provider is not registered.
    UnknownProvider(String),
    /// Authentication credentials are absent, malformed, or invalid.
    InvalidCredentials,
    /// The upstream provider or backing store is temporarily unavailable.
    Unavailable,
    /// The operation was rate limited and may be retried later.
    RateLimited {
        /// Suggested delay before retrying, when supplied by the upstream.
        retry_after: Option<Duration>,
    },
    /// The upstream understood the request but rejected it.
    Rejected(String),
    /// A callback or payload signature is invalid.
    InvalidSignature,
    /// The callback event was already consumed.
    AlreadyProcessed,
    /// The upstream returned a malformed or contradictory response.
    InvalidResponse(String),
    /// The selected provider does not implement the requested operation.
    Unsupported,
}

impl ProviderError {
    /// Returns a stable, transport-independent machine-readable error code.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Config(_) => "provider_config",
            Self::InvalidRequest(_) => "invalid_request",
            Self::UnknownProvider(_) => "unknown_provider",
            Self::InvalidCredentials => "invalid_credentials",
            Self::Unavailable => "provider_unavailable",
            Self::RateLimited { .. } => "provider_rate_limited",
            Self::Rejected(_) => "provider_rejected",
            Self::InvalidSignature => "invalid_signature",
            Self::AlreadyProcessed => "already_processed",
            Self::InvalidResponse(_) => "invalid_provider_response",
            Self::Unsupported => "unsupported_operation",
        }
    }

    /// Whether retrying the same operation later may succeed.
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable | Self::RateLimited { .. })
    }

    /// Returns the upstream-suggested retry delay, if one is available.
    pub const fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::RateLimited { retry_after } => *retry_after,
            _ => None,
        }
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => {
                write!(formatter, "invalid provider configuration: {message}")
            }
            Self::InvalidRequest(message) => {
                write!(formatter, "invalid provider request: {message}")
            }
            Self::UnknownProvider(name) => write!(formatter, "unknown provider: {name}"),
            Self::InvalidCredentials => formatter.write_str("invalid credentials"),
            Self::Unavailable => formatter.write_str("upstream provider unavailable"),
            Self::RateLimited { retry_after } => match retry_after {
                Some(delay) => write!(
                    formatter,
                    "upstream provider rate limited the request; retry after {delay:?}"
                ),
                None => formatter.write_str("upstream provider rate limited the request"),
            },
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

impl From<elura_core::identity::IdentityValidationError> for ProviderError {
    fn from(error: elura_core::identity::IdentityValidationError) -> Self {
        Self::InvalidRequest(error.to_string())
    }
}

/// Result type used by provider APIs.
pub type ProviderResult<T> = std::result::Result<T, ProviderError>;
