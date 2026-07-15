use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StateHash([u8; 32]);
impl StateHash {
    pub const ZERO: Self = Self([0; 32]);
    pub fn digest(value: &[u8]) -> Self {
        Self(Sha256::digest(value).into())
    }
    pub const fn from_bytes(value: [u8; 32]) -> Self {
        Self(value)
    }
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    pub fn matches(&self, value: &[u8]) -> bool {
        bool::from(self.0.ct_eq(StateHash::digest(value).as_bytes()))
    }
}
