use std::fmt;
use std::sync::Arc;

use elura_core::{Error, Result};
use subtle::ConstantTimeEq;

const MIN_TOKEN_BYTES: usize = 32;
const MAX_TOKEN_BYTES: usize = 4096;

/// Shared credential attached to every internal command.
///
/// The value is redacted from `Debug` output and compared in constant time.
#[derive(Clone)]
pub struct InternalToken(Arc<str>);

impl InternalToken {
    /// Creates a validated service token.
    pub fn new(token: impl Into<String>) -> Result<Self> {
        let token = token.into();
        if !(MIN_TOKEN_BYTES..=MAX_TOKEN_BYTES).contains(&token.len()) {
            return Err(Error::InvalidConfig(format!(
                "internal token must contain {MIN_TOKEN_BYTES}..={MAX_TOKEN_BYTES} bytes"
            )));
        }
        Ok(Self(Arc::from(token)))
    }

    /// Exposes the token for attaching it to a trusted service request.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Checks a candidate token using constant-time comparison.
    pub fn authorizes(&self, candidate: &str) -> bool {
        self.0.as_bytes().ct_eq(candidate.as_bytes()).into()
    }
}

impl fmt::Debug for InternalToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InternalToken([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_and_compares_tokens() {
        assert!(InternalToken::new("short").is_err());
        let token = InternalToken::new("0123456789abcdef0123456789abcdef").unwrap();
        assert!(token.authorizes("0123456789abcdef0123456789abcdef"));
        assert!(!token.authorizes("0123456789abcdef0123456789abcdeg"));
        assert_eq!(format!("{token:?}"), "InternalToken([REDACTED])");
    }
}
