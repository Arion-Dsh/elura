use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use uuid::Uuid;

use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PlayerKey {
    pub region_id: u32,
    pub realm_id: u32,
    pub user_id: i64,
}

impl PlayerKey {
    pub fn new(region_id: u32, realm_id: u32, user_id: i64) -> Result<Self> {
        let key = Self {
            region_id,
            realm_id,
            user_id,
        };
        key.validate()?;
        Ok(key)
    }

    pub fn validate(&self) -> Result<()> {
        if self.region_id == 0 || self.realm_id == 0 || self.user_id <= 0 {
            Err(Error::InvalidConfig("invalid player key".into()))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub account_id: i64,
    pub user_id: i64,
    pub region_id: u32,
    pub realm_id: u32,
    pub generation: u64,
}

impl Identity {
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
pub enum SessionControlKind {
    Login,
    ForceLogout,
    VersionChanged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionControlEvent {
    pub kind: SessionControlKind,
    pub region_id: u32,
    pub realm_id: u32,
    pub user_id: i64,
    #[serde(default)]
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_session_id: Option<Uuid>,
    #[serde(default)]
    pub reason: String,
}

impl SessionControlEvent {
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
pub trait SessionControlHandler: Send + Sync {
    async fn handle(&self, event: SessionControlEvent) -> Result<()>;
}

#[async_trait]
pub trait SessionControlTransport: Send + Sync {
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
pub enum SessionState {
    Anonymous,
    Authenticated,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSnapshot {
    pub id: Uuid,
    pub remote_ip: IpAddr,
    pub state: SessionState,
    pub identity: Option<Identity>,
    pub connected_at: SystemTime,
    pub last_activity_at: SystemTime,
}

#[derive(Debug)]
struct Inner {
    state: SessionState,
    identity: Option<Identity>,
    last_activity_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct Session {
    id: Uuid,
    remote_ip: IpAddr,
    connected_at: SystemTime,
    inner: Arc<RwLock<Inner>>,
}

impl Session {
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

    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn remote_ip(&self) -> IpAddr {
        self.remote_ip
    }

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

    pub fn identity(&self) -> Option<Identity> {
        self.inner
            .read()
            .ok()
            .and_then(|inner| inner.identity.clone())
    }

    pub fn touch(&self) {
        if let Ok(mut inner) = self.inner.write() {
            inner.last_activity_at = SystemTime::now();
        }
    }

    pub fn close(&self) {
        if let Ok(mut inner) = self.inner.write() {
            inner.state = SessionState::Closed;
            inner.last_activity_at = SystemTime::now();
        }
    }

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
