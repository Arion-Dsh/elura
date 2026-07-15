use std::fmt;
use std::io;

use serde::{Deserialize, Serialize};

/// Stable error payload exchanged across Elura process and client boundaries.
///
/// Internal implementation details are deliberately not represented here. New
/// error codes can be added without changing the binary ELR2 frame format.
///
/// ```
/// use elura_core::ErrorEnvelope;
///
/// let encoded = ErrorEnvelope::new("DENIED", "request denied", false).to_bytes();
/// let decoded = ErrorEnvelope::from_slice(&encoded)?;
/// assert_eq!(decoded.code, "DENIED");
/// # Ok::<(), elura_core::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ErrorEnvelope {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl ErrorEnvelope {
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }

    /// Encodes the envelope as the canonical JSON ELR2 error payload.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_else(|_| {
            br#"{"code":"INTERNAL","message":"internal error","retryable":false}"#.to_vec()
        })
    }

    /// Decodes and validates an ELR2 error payload.
    pub fn from_slice(input: &[u8]) -> Result<Self> {
        let envelope: Self = serde_json::from_slice(input)
            .map_err(|_| Error::InvalidFrame("invalid error envelope".into()))?;
        if envelope.code.is_empty()
            || envelope.code.len() > 64
            || envelope.message.len() > 1024
            || !envelope
                .code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(Error::InvalidFrame("invalid error envelope fields".into()));
        }
        Ok(envelope)
    }

    /// Converts a remote envelope back into the closest local error category.
    pub fn into_error(self) -> Error {
        match self.code.as_str() {
            "UNAUTHENTICATED" => Error::Authentication,
            "DUPLICATE_SESSION" => Error::DuplicateSession,
            "SESSION_REVOKED" => Error::SessionRevoked,
            "RATE_LIMITED" => Error::RateLimited,
            "QUEUE_FULL" => Error::QueueFull,
            "UNAVAILABLE" => Error::Unavailable,
            "TIMEOUT" => Error::Timeout,
            "INTERNAL" => Error::Internal("remote service error".into()),
            _ => Error::Business {
                code: self.code,
                message: self.message,
                retryable: self.retryable,
            },
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    InvalidConfig(String),
    InvalidFrame(String),
    Authentication,
    DuplicateSession,
    SessionRevoked,
    AdmissionDenied {
        code: String,
        reason: String,
        retry_after_ms: u64,
    },
    OutboxLeaseLost,
    OutboxNotFound,
    DuplicateEvent,
    AlreadyProcessed,
    Business {
        code: String,
        message: String,
        retryable: bool,
    },
    TicketExpired,
    TicketReplayed,
    RouteNotFound(u32),
    DuplicateRoute(u32),
    RateLimited,
    QueueFull,
    Unavailable,
    Timeout,
    Io(io::Error),
    Serialization(serde_json::Error),
    Internal(String),
}

impl Error {
    /// Creates a non-retryable business error safe to return to clients.
    pub fn business(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Business {
            code: code.into(),
            message: message.into(),
            retryable: false,
        }
    }

    /// Creates a retryable business error safe to return to clients.
    pub fn retryable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Business {
            code: code.into(),
            message: message.into(),
            retryable: true,
        }
    }
}

impl From<&Error> for ErrorEnvelope {
    fn from(error: &Error) -> Self {
        match error {
            Error::InvalidConfig(_) => Self::new("INVALID_CONFIG", "invalid configuration", false),
            Error::InvalidFrame(_) | Error::Serialization(_) => {
                Self::new("INVALID_REQUEST", "invalid request", false)
            }
            Error::Authentication | Error::TicketExpired | Error::TicketReplayed => {
                Self::new("UNAUTHENTICATED", "authentication failed", false)
            }
            Error::DuplicateSession => Self::new("DUPLICATE_SESSION", "duplicate session", false),
            Error::SessionRevoked => {
                Self::new("SESSION_REVOKED", "account session was revoked", false)
            }
            Error::AdmissionDenied {
                code,
                reason,
                retry_after_ms: _,
            } => Self::new(public_code(code), reason, true),
            Error::Business {
                code,
                message,
                retryable,
            } => Self::new(public_code(code), message, *retryable),
            Error::RouteNotFound(_) => {
                Self::new("ROUTE_NOT_FOUND", "route is not registered", false)
            }
            Error::RateLimited => Self::new("RATE_LIMITED", "request rate exceeded", true),
            Error::QueueFull => Self::new("QUEUE_FULL", "service queue is full", true),
            Error::Unavailable | Error::Io(_) => {
                Self::new("UNAVAILABLE", "service is unavailable", true)
            }
            Error::Timeout => Self::new("TIMEOUT", "operation timed out", true),
            Error::OutboxLeaseLost
            | Error::OutboxNotFound
            | Error::DuplicateEvent
            | Error::AlreadyProcessed
            | Error::DuplicateRoute(_)
            | Error::Internal(_) => Self::new("INTERNAL", "internal error", false),
        }
    }
}

fn public_code(value: &str) -> String {
    let code: String = value
        .bytes()
        .take(64)
        .map(|byte| {
            if byte.is_ascii_alphanumeric() {
                byte.to_ascii_uppercase() as char
            } else {
                '_'
            }
        })
        .collect();
    if code.is_empty() {
        "BUSINESS_ERROR".into()
    } else {
        code
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid configuration: {message}"),
            Self::InvalidFrame(message) => write!(formatter, "invalid frame: {message}"),
            Self::Authentication => formatter.write_str("authentication failed"),
            Self::DuplicateSession => formatter.write_str("duplicate session"),
            Self::SessionRevoked => formatter.write_str("account version revoked"),
            Self::AdmissionDenied {
                code,
                reason,
                retry_after_ms,
            } => write!(
                formatter,
                "admission denied ({code}): {reason}; retry after {retry_after_ms}ms"
            ),
            Self::OutboxLeaseLost => formatter.write_str("outbox lease was lost"),
            Self::OutboxNotFound => formatter.write_str("outbox event was not found"),
            Self::DuplicateEvent => {
                formatter.write_str("outbox event ID conflicts with another event")
            }
            Self::AlreadyProcessed => formatter.write_str("event was already processed"),
            Self::Business { code, message, .. } => write!(formatter, "{code}: {message}"),
            Self::TicketExpired => formatter.write_str("ticket expired"),
            Self::TicketReplayed => formatter.write_str("ticket replayed"),
            Self::RouteNotFound(route) => write!(formatter, "route {route} is not registered"),
            Self::DuplicateRoute(route) => write!(formatter, "route {route} already exists"),
            Self::RateLimited => formatter.write_str("request rate exceeded"),
            Self::QueueFull => formatter.write_str("queue is full"),
            Self::Unavailable => formatter.write_str("service is unavailable"),
            Self::Timeout => formatter.write_str("operation timed out"),
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Serialization(error) => write!(formatter, "serialization error: {error}"),
            Self::Internal(message) => write!(formatter, "internal error: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Serialization(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_envelope_round_trips_business_errors() {
        let error = Error::business("NOT_ENOUGH_GOLD", "not enough gold");
        let encoded = ErrorEnvelope::from(&error).to_bytes();
        let decoded = ErrorEnvelope::from_slice(&encoded).unwrap();
        assert_eq!(decoded.code, "NOT_ENOUGH_GOLD");
        assert_eq!(decoded.message, "not enough gold");
        assert!(!decoded.retryable);
        assert!(matches!(
            decoded.into_error(),
            Error::Business { code, .. } if code == "NOT_ENOUGH_GOLD"
        ));
    }

    #[test]
    fn retryable_business_errors_are_marked_for_retry() {
        let error = Error::retryable("INVENTORY_UNAVAILABLE", "try again later");
        let envelope = ErrorEnvelope::from(&error);
        assert_eq!(envelope.code, "INVENTORY_UNAVAILABLE");
        assert!(envelope.retryable);
    }

    #[test]
    fn public_envelope_redacts_internal_details() {
        let encoded = ErrorEnvelope::from(&Error::Internal("database password".into())).to_bytes();
        let decoded = ErrorEnvelope::from_slice(&encoded).unwrap();
        assert_eq!(decoded.code, "INTERNAL");
        assert_eq!(decoded.message, "internal error");
        assert!(
            !String::from_utf8(encoded)
                .unwrap()
                .contains("database password")
        );
    }
}
