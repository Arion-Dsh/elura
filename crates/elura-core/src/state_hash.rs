//! SHA-256 state digests used by replay and replication checks.

#![deny(missing_docs)]

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// A 32-byte SHA-256 digest of serialized application state.
pub struct StateHash([u8; 32]);

impl StateHash {
    /// All-zero sentinel hash.
    ///
    /// This value is not the SHA-256 digest of an empty byte slice.
    pub const ZERO: Self = Self([0; 32]);

    /// Computes the SHA-256 digest of `value`.
    pub fn digest(value: &[u8]) -> Self {
        Self(Sha256::digest(value).into())
    }

    /// Wraps an existing 32-byte digest without recomputing it.
    pub const fn from_bytes(value: [u8; 32]) -> Self {
        Self(value)
    }

    /// Returns the digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns whether `value` hashes to this digest.
    ///
    /// Digest comparison is performed in constant time.
    pub fn matches(&self, value: &[u8]) -> bool {
        bool::from(self.0.ct_eq(StateHash::digest(value).as_bytes()))
    }
}
