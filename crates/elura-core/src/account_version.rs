//! Account-version storage used to invalidate stale authenticated sessions.
//!
//! Versions are scoped by [`AccountVersionKey`]. A login or administrative
//! action can advance the version through [`MutableAccountVersionStore`], and
//! gateways can compare that value with an identity's generation before
//! accepting traffic.

#![deny(missing_docs)]

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::session::Identity;
use crate::{Error, Result};

mod memory;

pub use memory::MemoryAccountVersionStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Identifies the account-version counter for one player in one realm.
pub struct AccountVersionKey {
    /// Region containing the player's realm.
    pub region_id: u32,
    /// Realm containing the player.
    pub realm_id: u32,
    /// Player user ID within the realm.
    pub user_id: i64,
}

impl AccountVersionKey {
    /// Creates and validates an account-version key.
    ///
    /// Returns [`Error::InvalidConfig`] when a region or realm ID is zero, or
    /// when `user_id` is not positive.
    pub fn new(region_id: u32, realm_id: u32, user_id: i64) -> Result<Self> {
        let key = Self {
            region_id,
            realm_id,
            user_id,
        };
        key.validate()?;
        Ok(key)
    }

    /// Derives the version key represented by an authenticated identity.
    pub fn from_identity(identity: &Identity) -> Self {
        Self {
            region_id: identity.region_id,
            realm_id: identity.realm_id,
            user_id: identity.user_id,
        }
    }

    /// Validates that every component can identify a player.
    ///
    /// Returns [`Error::InvalidConfig`] when a region or realm ID is zero, or
    /// when the user ID is not positive.
    pub fn validate(&self) -> Result<()> {
        if self.region_id == 0 || self.realm_id == 0 || self.user_id <= 0 {
            return Err(Error::InvalidConfig(
                "account version key requires region, realm, and user".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
/// Read-only access to account-version counters.
pub trait AccountVersionStore: Send + Sync + 'static {
    /// Returns the current version for `key`, or [`None`] if none was stored.
    async fn current(&self, key: AccountVersionKey) -> Result<Option<u64>>;
}

#[async_trait]
/// Mutable account-version storage.
///
/// Implementations must make each call to [`increment`](Self::increment)
/// atomic so concurrent invalidations cannot lose an update.
pub trait MutableAccountVersionStore: AccountVersionStore {
    /// Replaces the version stored for `key`.
    ///
    /// Implementations reject zero because stored versions represent issued
    /// identity generations.
    async fn set(&self, key: AccountVersionKey, version: u64) -> Result<()>;

    /// Atomically increments and returns the version stored for `key`.
    ///
    /// A missing key starts at version `1`.
    async fn increment(&self, key: AccountVersionKey) -> Result<u64>;
}
