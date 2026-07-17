//! Authenticated player identities, session state, and session-control events.
//!
//! [`Session`] models the local lifecycle of a client connection. The
//! [`SessionControlTransport`] contract carries cross-process events such as
//! forced logout and account-version changes.

#![deny(missing_docs)]

use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use uuid::Uuid;

use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Stable player identifier scoped to a region and realm.
pub struct PlayerKey {
    /// Region containing the player's realm.
    pub region_id: u32,
    /// Realm containing the player.
    pub realm_id: u32,
    /// Player user ID within the realm.
    pub user_id: i64,
}

impl PlayerKey {
    /// Creates and validates a player key.
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

    /// Validates that every component can identify a player.
    pub fn validate(&self) -> Result<()> {
        if self.region_id == 0 || self.realm_id == 0 || self.user_id <= 0 {
            Err(Error::InvalidConfig("invalid player key".into()))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Authenticated account and player attributes attached to a session.
///
/// `account_id` identifies the login account, while `user_id` identifies the
/// selected player in a region and realm. `generation` is advanced to revoke
/// identities issued before an account-version change.
pub struct Identity {
    /// Account that authenticated the connection.
    pub account_id: i64,
    /// Player selected for the session.
    pub user_id: i64,
    /// Region containing the selected realm.
    pub region_id: u32,
    /// Realm containing the selected player.
    pub realm_id: u32,
    /// Account version captured when the identity was issued.
    pub generation: u64,
}

impl Identity {
    /// Validates all identity fields.
    ///
    /// Returns [`Error::Authentication`] unless both numeric IDs are positive
    /// and the region, realm, and generation are nonzero.
    pub fn validate(&self) -> Result<()> {
        if self.account_id <= 0
            || self.user_id <= 0
            || self.region_id == 0
            || self.realm_id == 0
            || self.generation == 0
        {
            return Err(Error::Authentication);
        }
        Ok(())
    }

    /// Returns the region-, realm-, and user-scoped key for this identity.
    pub const fn player_key(&self) -> PlayerKey {
        PlayerKey {
            region_id: self.region_id,
            realm_id: self.realm_id,
            user_id: self.user_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
/// Reason a session-control event was emitted.
pub enum SessionControlKind {
    /// A player logged in and competing sessions may need reconciliation.
    Login,
    /// Matching sessions must disconnect.
    ForceLogout,
    /// The account version changed and older identity generations are stale.
    VersionChanged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Cross-process instruction affecting sessions for one player.
pub struct SessionControlEvent {
    /// Operation consumers should perform.
    pub kind: SessionControlKind,
    /// Region containing the player's realm.
    pub region_id: u32,
    /// Realm containing the player.
    pub realm_id: u32,
    /// Player affected by the event.
    pub user_id: i64,
    #[serde(default)]
    /// New account version for [`SessionControlKind::VersionChanged`].
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Session that caused the event, when applicable.
    pub session_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Session exempted from eviction, usually the newly accepted login.
    pub keep_session_id: Option<Uuid>,
    #[serde(default)]
    /// Operator- or application-facing reason, limited to 256 bytes.
    pub reason: String,
}

impl SessionControlEvent {
    /// Validates the event's player scope, reason length, and generation.
    ///
    /// Version-change events require a nonzero `generation`; other event kinds
    /// may leave it at zero.
    pub fn validate(&self) -> Result<()> {
        if self.region_id == 0
            || self.realm_id == 0
            || self.user_id <= 0
            || self.reason.len() > 256
            || (self.kind == SessionControlKind::VersionChanged && self.generation == 0)
        {
            return Err(Error::InvalidConfig("invalid Session control event".into()));
        }
        Ok(())
    }
}

#[async_trait]
/// Consumer of session-control events.
pub trait SessionControlHandler: Send + Sync {
    /// Applies one event.
    ///
    /// Handlers should be idempotent because transports may redeliver events.
    async fn handle(&self, event: SessionControlEvent) -> Result<()>;
}

#[async_trait]
/// Publishes and subscribes to session-control events.
pub trait SessionControlTransport: Send + Sync {
    /// Publishes an event for delivery to interested session processes.
    async fn publish(&self, event: &SessionControlEvent) -> Result<()>;

    /// Runs the subscriber until shutdown is requested. Implementations must
    /// tolerate duplicate control events and reconnect after transient errors.
    async fn subscribe(
        &self,
        handler: Arc<dyn SessionControlHandler>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
/// Current lifecycle state of a client session.
pub enum SessionState {
    /// Connected but not yet authenticated.
    Anonymous,
    /// Authenticated with an [`Identity`].
    Authenticated,
    /// Closed to further state transitions.
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Point-in-time copy of a [`Session`]'s observable state.
pub struct SessionSnapshot {
    /// Unique session ID.
    pub id: Uuid,
    /// Remote IP address captured when the session was created.
    pub remote_ip: IpAddr,
    /// Lifecycle state at snapshot time.
    pub state: SessionState,
    /// Authenticated identity, if authentication has succeeded.
    pub identity: Option<Identity>,
    /// Time at which the session was created.
    pub connected_at: SystemTime,
    /// Most recent successful activity update.
    pub last_activity_at: SystemTime,
}

#[derive(Debug)]
struct Inner {
    state: SessionState,
    identity: Option<Identity>,
    last_activity_at: SystemTime,
}

#[derive(Debug, Clone)]
/// Thread-safe state for one client connection.
///
/// Clones share lifecycle state and timestamps. A session can authenticate only
/// once, and closing it preserves its identity for diagnostics.
pub struct Session {
    id: Uuid,
    remote_ip: IpAddr,
    connected_at: SystemTime,
    inner: Arc<RwLock<Inner>>,
}

impl Session {
    /// Creates an anonymous session for `remote_ip` with a new random ID.
    pub fn new(remote_ip: IpAddr) -> Self {
        let now = SystemTime::now();
        Self {
            id: Uuid::new_v4(),
            remote_ip,
            connected_at: now,
            inner: Arc::new(RwLock::new(Inner {
                state: SessionState::Anonymous,
                identity: None,
                last_activity_at: now,
            })),
        }
    }

    /// Returns the unique session ID.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the remote IP address recorded at connection time.
    pub fn remote_ip(&self) -> IpAddr {
        self.remote_ip
    }

    /// Transitions an anonymous session to authenticated state.
    ///
    /// Returns [`Error::Authentication`] if the identity is invalid or the
    /// session is no longer anonymous.
    pub fn authenticate(&self, identity: Identity) -> Result<()> {
        identity.validate()?;
        let mut inner = self
            .inner
            .write()
            .map_err(|_| Error::Internal("session lock poisoned".into()))?;
        if inner.state != SessionState::Anonymous {
            return Err(Error::Authentication);
        }
        inner.identity = Some(identity);
        inner.state = SessionState::Authenticated;
        inner.last_activity_at = SystemTime::now();
        Ok(())
    }

    /// Returns a copy of the authenticated identity, if available.
    ///
    /// A poisoned internal lock is treated as an unavailable identity.
    pub fn identity(&self) -> Option<Identity> {
        self.inner
            .read()
            .ok()
            .and_then(|inner| inner.identity.clone())
    }

    /// Records activity at the current system time.
    ///
    /// This operation is a no-op if the internal lock is poisoned.
    pub fn touch(&self) {
        if let Ok(mut inner) = self.inner.write() {
            inner.last_activity_at = SystemTime::now();
        }
    }

    /// Marks the session closed while retaining its identity.
    ///
    /// This operation is a no-op if the internal lock is poisoned.
    pub fn close(&self) {
        if let Ok(mut inner) = self.inner.write() {
            inner.state = SessionState::Closed;
            inner.last_activity_at = SystemTime::now();
        }
    }

    /// Captures the session's current observable state.
    ///
    /// Returns [`Error::Internal`] if the internal lock is poisoned.
    pub fn snapshot(&self) -> Result<SessionSnapshot> {
        let inner = self
            .inner
            .read()
            .map_err(|_| Error::Internal("session lock poisoned".into()))?;
        Ok(SessionSnapshot {
            id: self.id,
            remote_ip: self.remote_ip,
            state: inner.state,
            identity: inner.identity.clone(),
            connected_at: self.connected_at,
            last_activity_at: inner.last_activity_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_preserve_identity_through_close() {
        let session = Session::new("192.0.2.1".parse().unwrap());
        assert_eq!(session.snapshot().unwrap().state, SessionState::Anonymous);
        let identity = Identity {
            account_id: 1,
            user_id: 2,
            region_id: 3,
            realm_id: 4,
            generation: 5,
        };
        session.authenticate(identity.clone()).unwrap();
        assert_eq!(session.snapshot().unwrap().identity, Some(identity.clone()));
        session.close();
        let closed = session.snapshot().unwrap();
        assert_eq!(closed.state, SessionState::Closed);
        assert_eq!(closed.identity, Some(identity));
    }
}
