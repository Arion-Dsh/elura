use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::push::{PushRequest, PushTarget, PushTargetResolver};
use crate::session::Identity;
use crate::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLease {
    pub session_id: Uuid,
    pub gateway_id: String,
    pub identity: Identity,
    pub expires_at: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateLoginMode {
    AllowMultiple,
    RejectNew,
    KickExisting,
}

#[async_trait]
/// Shared online-presence contract.
///
/// Implementations must fence unregister/release operations by Gateway and
/// Session identity, expire leases, and make `claim_single` atomic across all
/// callers. Query methods must not return expired leases.
pub trait OnlineDirectory: Send + Sync {
    async fn register(&self, lease: SessionLease) -> Result<()>;
    async fn renew(&self, lease: SessionLease) -> Result<()>;
    async fn unregister(&self, lease: &SessionLease) -> Result<()>;
    async fn session(&self, session_id: Uuid) -> Result<Option<SessionLease>>;
    async fn user_sessions(
        &self,
        region_id: u32,
        realm_id: u32,
        user_id: i64,
    ) -> Result<Vec<SessionLease>>;
    async fn group_sessions(&self, group: &str) -> Result<Vec<SessionLease>>;
    async fn track_group(&self, session_id: Uuid, group: &str, join: bool) -> Result<()>;
    /// Atomically claims the single-login slot and returns a different current
    /// owner. When `replace` is false the existing owner must remain unchanged.
    async fn claim_single(&self, lease: &SessionLease, replace: bool) -> Result<Option<Uuid>>;
    async fn release_single(&self, lease: &SessionLease) -> Result<()>;
}

/// Adapts any [`OnlineDirectory`] into provider-neutral Push target routing.
/// Topic membership updates and target lookup therefore share the same online
/// directory semantics without coupling the message transport to its backend.
pub struct OnlineDirectoryTargetResolver {
    directory: Arc<dyn OnlineDirectory>,
}

impl OnlineDirectoryTargetResolver {
    pub fn new(directory: Arc<dyn OnlineDirectory>) -> Self {
        Self { directory }
    }
}

#[async_trait]
impl PushTargetResolver for OnlineDirectoryTargetResolver {
    async fn resolve_gateways(&self, request: &PushRequest) -> Result<Vec<String>> {
        request.validate()?;
        let mut gateways = HashSet::new();
        match &request.target {
            PushTarget::Realm => {}
            PushTarget::Session(id) | PushTarget::Disconnect(id) => {
                if let Some(lease) = self.directory.session(*id).await? {
                    gateways.insert(lease.gateway_id);
                }
            }
            PushTarget::User(user_id) => {
                for lease in self
                    .directory
                    .user_sessions(request.region_id, request.realm_id, *user_id)
                    .await?
                {
                    gateways.insert(lease.gateway_id);
                }
            }
            PushTarget::Users(user_ids) => {
                for user_id in user_ids {
                    for lease in self
                        .directory
                        .user_sessions(request.region_id, request.realm_id, *user_id)
                        .await?
                    {
                        gateways.insert(lease.gateway_id);
                    }
                }
            }
            PushTarget::Topic(topic) => {
                for lease in self
                    .directory
                    .group_sessions(&format!("topic:{topic}"))
                    .await?
                {
                    gateways.insert(lease.gateway_id);
                }
            }
            PushTarget::JoinTopic { session_id, topic }
            | PushTarget::LeaveTopic { session_id, topic } => {
                let join = matches!(request.target, PushTarget::JoinTopic { .. });
                let owner = self.directory.session(*session_id).await?;
                self.directory
                    .track_group(*session_id, &format!("topic:{topic}"), join)
                    .await?;
                if let Some(lease) = owner {
                    gateways.insert(lease.gateway_id);
                }
            }
        }
        let mut gateways = gateways.into_iter().collect::<Vec<_>>();
        gateways.sort_unstable();
        Ok(gateways)
    }
}

#[derive(Default)]
pub struct MemoryOnlineDirectory {
    inner: Mutex<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    sessions: HashMap<Uuid, SessionLease>,
    groups: HashMap<String, HashSet<Uuid>>,
    single: HashMap<(u32, u32, i64), Uuid>,
}

impl MemoryOnlineDirectory {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, MemoryState>> {
        self.inner
            .lock()
            .map_err(|_| Error::Internal("online directory poisoned".into()))
    }

    fn purge(state: &mut MemoryState) {
        let now = SystemTime::now();
        state.sessions.retain(|_, lease| lease.expires_at > now);
        let live: HashSet<_> = state.sessions.keys().copied().collect();
        state.groups.retain(|_, ids| {
            ids.retain(|id| live.contains(id));
            !ids.is_empty()
        });
        state.single.retain(|_, id| live.contains(id));
    }
}

#[async_trait]
impl OnlineDirectory for MemoryOnlineDirectory {
    async fn register(&self, lease: SessionLease) -> Result<()> {
        lease.identity.validate()?;
        self.lock()?.sessions.insert(lease.session_id, lease);
        Ok(())
    }

    async fn renew(&self, lease: SessionLease) -> Result<()> {
        self.register(lease).await
    }

    async fn unregister(&self, lease: &SessionLease) -> Result<()> {
        let mut state = self.lock()?;
        if state
            .sessions
            .get(&lease.session_id)
            .is_some_and(|current| current.gateway_id == lease.gateway_id)
        {
            state.sessions.remove(&lease.session_id);
            for ids in state.groups.values_mut() {
                ids.remove(&lease.session_id);
            }
        }
        Ok(())
    }

    async fn session(&self, session_id: Uuid) -> Result<Option<SessionLease>> {
        let mut state = self.lock()?;
        Self::purge(&mut state);
        Ok(state.sessions.get(&session_id).cloned())
    }

    async fn user_sessions(&self, r: u32, m: u32, u: i64) -> Result<Vec<SessionLease>> {
        let mut state = self.lock()?;
        Self::purge(&mut state);
        Ok(state
            .sessions
            .values()
            .filter(|lease| {
                let identity = &lease.identity;
                identity.region_id == r && identity.realm_id == m && identity.user_id == u
            })
            .cloned()
            .collect())
    }

    async fn group_sessions(&self, group: &str) -> Result<Vec<SessionLease>> {
        let mut state = self.lock()?;
        Self::purge(&mut state);
        Ok(state
            .groups
            .get(group)
            .into_iter()
            .flatten()
            .filter_map(|id| state.sessions.get(id).cloned())
            .collect())
    }

    async fn track_group(&self, session_id: Uuid, group: &str, join: bool) -> Result<()> {
        if group.is_empty() {
            return Err(Error::InvalidConfig("empty online group".into()));
        }
        let mut state = self.lock()?;
        if join {
            state
                .groups
                .entry(group.into())
                .or_default()
                .insert(session_id);
        } else if let Some(ids) = state.groups.get_mut(group) {
            ids.remove(&session_id);
        }
        Ok(())
    }

    async fn claim_single(&self, lease: &SessionLease, replace: bool) -> Result<Option<Uuid>> {
        let mut state = self.lock()?;
        Self::purge(&mut state);
        let identity = &lease.identity;
        let key = (identity.region_id, identity.realm_id, identity.user_id);
        let previous = state.single.get(&key).copied();
        if previous.is_none() || replace {
            state.single.insert(key, lease.session_id);
        }
        Ok(previous.filter(|id| *id != lease.session_id))
    }

    async fn release_single(&self, lease: &SessionLease) -> Result<()> {
        let mut state = self.lock()?;
        let identity = &lease.identity;
        let key = (identity.region_id, identity.realm_id, identity.user_id);
        if state.single.get(&key) == Some(&lease.session_id) {
            state.single.remove(&key);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;

    fn lease(id: Uuid) -> SessionLease {
        SessionLease {
            session_id: id,
            gateway_id: "gateway-a".into(),
            identity: Identity {
                account_id: 1,
                user_id: 2,
                region_id: 3,
                realm_id: 4,
                generation: 1,
            },
            expires_at: SystemTime::now() + Duration::from_secs(30),
        }
    }

    #[tokio::test]
    async fn claims_and_fences_single_session() {
        let directory = MemoryOnlineDirectory::default();
        let first = lease(Uuid::new_v4());
        let second = lease(Uuid::new_v4());
        directory.register(first.clone()).await.unwrap();
        assert_eq!(directory.claim_single(&first, false).await.unwrap(), None);
        assert_eq!(
            directory.claim_single(&second, false).await.unwrap(),
            Some(first.session_id)
        );
        assert_eq!(
            directory.claim_single(&second, true).await.unwrap(),
            Some(first.session_id)
        );
        directory.register(second.clone()).await.unwrap();
        directory.release_single(&first).await.unwrap();
        assert_eq!(
            directory.claim_single(&first, false).await.unwrap(),
            Some(second.session_id)
        );
    }

    #[tokio::test]
    async fn provider_neutral_resolver_routes_users_and_topics() {
        let directory = Arc::new(MemoryOnlineDirectory::default());
        let first = lease(Uuid::new_v4());
        let mut second = lease(Uuid::new_v4());
        second.gateway_id = "gateway-b".into();
        directory.register(first.clone()).await.unwrap();
        directory.register(second.clone()).await.unwrap();
        let resolver = OnlineDirectoryTargetResolver::new(directory);

        let request = PushRequest {
            region_id: 3,
            realm_id: 4,
            target: PushTarget::User(2),
            route: 100,
            sequence: 1,
            trace_id: "resolver-test".into(),
            payload: Bytes::new(),
        };
        assert_eq!(
            resolver.resolve_gateways(&request).await.unwrap(),
            vec!["gateway-a", "gateway-b"]
        );

        let join = PushRequest {
            target: PushTarget::JoinTopic {
                session_id: first.session_id,
                topic: "room-1".into(),
            },
            route: 0,
            ..request.clone()
        };
        assert_eq!(
            resolver.resolve_gateways(&join).await.unwrap(),
            vec!["gateway-a"]
        );
        let topic = PushRequest {
            target: PushTarget::Topic("room-1".into()),
            ..request
        };
        assert_eq!(
            resolver.resolve_gateways(&topic).await.unwrap(),
            vec!["gateway-a"]
        );
    }
}
