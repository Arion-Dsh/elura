use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use elura_core::push::{PushRequest, PushTarget, PushTargetResolver};
use elura_core::session::Identity;
use elura_core::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLease {
    pub session_id: Uuid,
    pub gateway_id: String,
    pub identity: Identity,
    pub expires_at: SystemTime,
}

/// Point-in-time online totals for one region and realm.
///
/// `session_count` counts authenticated client sessions, while `user_count`
/// deduplicates those sessions by player user ID.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnlineStats {
    /// Number of live authenticated sessions.
    pub session_count: u64,
    /// Number of distinct player user IDs across the live sessions.
    pub user_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateLoginMode {
    AllowMultiple,
    RejectNew,
    KickExisting,
}

/// Atomic policy applied while admitting an authenticated Session.
///
/// Capacity is scoped to the Session's region and realm. A missing maximum
/// leaves the realm unbounded, while a configured maximum must be positive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OnlineAdmissionPolicy {
    pub duplicate_login: DuplicateLoginMode,
    pub max_sessions: Option<u64>,
}

impl OnlineAdmissionPolicy {
    pub fn new(duplicate_login: DuplicateLoginMode, max_sessions: Option<u64>) -> Result<Self> {
        if max_sessions == Some(0) {
            return Err(Error::InvalidConfig(
                "online realm capacity must be positive".into(),
            ));
        }
        Ok(Self {
            duplicate_login,
            max_sessions,
        })
    }
}

/// Result of atomically acquiring an online Session slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnlineAdmission {
    Accepted { previous_session: Option<Uuid> },
    Duplicate,
    RealmFull,
}

#[async_trait]
/// Shared online-presence contract.
///
/// Implementations must make [`OnlineDirectory::acquire`] atomic across all
/// callers, fence renew/unregister operations by Gateway and Session identity,
/// and expire leases. Query methods must not return expired leases.
pub trait OnlineDirectory: Send + Sync {
    /// Atomically applies duplicate-login and realm-capacity policy, then
    /// registers the Session when accepted.
    async fn acquire(
        &self,
        lease: SessionLease,
        policy: OnlineAdmissionPolicy,
    ) -> Result<OnlineAdmission>;
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
}

#[async_trait]
/// Optional online-presence statistics contract.
///
/// Keeping aggregate queries separate from [`OnlineDirectory`] lets custom
/// directory implementations opt into statistics without making session
/// lifecycle and routing depend on a particular aggregation strategy.
pub trait OnlineStatsReader: Send + Sync {
    /// Returns live session and distinct-user totals for a region and realm.
    async fn stats(&self, region_id: u32, realm_id: u32) -> Result<OnlineStats>;
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
            _ => {
                return Err(Error::InvalidConfig("unsupported push target".into()));
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
    capacity: HashMap<(u32, u32), HashSet<Uuid>>,
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
        state.capacity.retain(|_, ids| {
            ids.retain(|id| live.contains(id));
            !ids.is_empty()
        });
    }
}

#[async_trait]
impl OnlineDirectory for MemoryOnlineDirectory {
    async fn acquire(
        &self,
        lease: SessionLease,
        policy: OnlineAdmissionPolicy,
    ) -> Result<OnlineAdmission> {
        lease.identity.validate()?;
        let policy = OnlineAdmissionPolicy::new(policy.duplicate_login, policy.max_sessions)?;
        let mut state = self.lock()?;
        Self::purge(&mut state);
        let identity = &lease.identity;
        let user_key = (identity.region_id, identity.realm_id, identity.user_id);
        let realm_key = (identity.region_id, identity.realm_id);
        let previous = state
            .single
            .get(&user_key)
            .copied()
            .filter(|id| *id != lease.session_id);

        if policy.duplicate_login == DuplicateLoginMode::RejectNew && previous.is_some() {
            return Ok(OnlineAdmission::Duplicate);
        }

        let slots = state.capacity.entry(realm_key).or_default();
        let transfers_slot = policy.duplicate_login == DuplicateLoginMode::KickExisting
            && previous.is_some_and(|id| slots.contains(&id));
        let used = slots.len().saturating_sub(usize::from(transfers_slot)) as u64;
        if !slots.contains(&lease.session_id)
            && policy.max_sessions.is_some_and(|maximum| used >= maximum)
        {
            return Ok(OnlineAdmission::RealmFull);
        }

        match policy.duplicate_login {
            DuplicateLoginMode::AllowMultiple => {}
            DuplicateLoginMode::RejectNew | DuplicateLoginMode::KickExisting => {
                state.single.insert(user_key, lease.session_id);
            }
        }
        let slots = state.capacity.entry(realm_key).or_default();
        if transfers_slot && let Some(previous) = previous {
            slots.remove(&previous);
        }
        slots.insert(lease.session_id);
        state.sessions.insert(lease.session_id, lease);
        Ok(OnlineAdmission::Accepted {
            previous_session: (policy.duplicate_login == DuplicateLoginMode::KickExisting)
                .then_some(previous)
                .flatten(),
        })
    }

    async fn renew(&self, lease: SessionLease) -> Result<()> {
        lease.identity.validate()?;
        let mut state = self.lock()?;
        Self::purge(&mut state);
        match state.sessions.get(&lease.session_id) {
            Some(current) if current.gateway_id == lease.gateway_id => {
                state.sessions.insert(lease.session_id, lease);
                Ok(())
            }
            _ => Err(Error::SessionRevoked),
        }
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
            let identity = &lease.identity;
            let realm_key = (identity.region_id, identity.realm_id);
            if let Some(ids) = state.capacity.get_mut(&realm_key) {
                ids.remove(&lease.session_id);
            }
            let user_key = (identity.region_id, identity.realm_id, identity.user_id);
            if state.single.get(&user_key) == Some(&lease.session_id) {
                state.single.remove(&user_key);
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
}

#[async_trait]
impl OnlineStatsReader for MemoryOnlineDirectory {
    async fn stats(&self, region_id: u32, realm_id: u32) -> Result<OnlineStats> {
        let mut state = self.lock()?;
        Self::purge(&mut state);
        let sessions = state.sessions.values().filter(|lease| {
            lease.identity.region_id == region_id && lease.identity.realm_id == realm_id
        });
        let mut user_ids = HashSet::new();
        let mut session_count = 0;
        for lease in sessions {
            session_count += 1;
            user_ids.insert(lease.identity.user_id);
        }
        Ok(OnlineStats {
            session_count,
            user_count: user_ids.len() as u64,
        })
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

    fn policy(duplicate_login: DuplicateLoginMode) -> OnlineAdmissionPolicy {
        OnlineAdmissionPolicy::new(duplicate_login, None).unwrap()
    }

    #[tokio::test]
    async fn atomically_applies_duplicate_login_policy() {
        let directory = MemoryOnlineDirectory::default();
        let first = lease(Uuid::new_v4());
        let second = lease(Uuid::new_v4());
        assert_eq!(
            directory
                .acquire(first.clone(), policy(DuplicateLoginMode::RejectNew))
                .await
                .unwrap(),
            OnlineAdmission::Accepted {
                previous_session: None
            }
        );
        assert_eq!(
            directory
                .acquire(second.clone(), policy(DuplicateLoginMode::RejectNew))
                .await
                .unwrap(),
            OnlineAdmission::Duplicate
        );
        assert_eq!(
            directory
                .acquire(second, policy(DuplicateLoginMode::KickExisting))
                .await
                .unwrap(),
            OnlineAdmission::Accepted {
                previous_session: Some(first.session_id)
            }
        );
    }

    #[tokio::test]
    async fn atomically_enforces_realm_capacity_and_releases_slots() {
        let directory = MemoryOnlineDirectory::default();
        let first = lease(Uuid::new_v4());
        let mut second = lease(Uuid::new_v4());
        second.identity.user_id = 3;
        let bounded =
            OnlineAdmissionPolicy::new(DuplicateLoginMode::AllowMultiple, Some(1)).unwrap();

        assert!(matches!(
            directory.acquire(first.clone(), bounded).await.unwrap(),
            OnlineAdmission::Accepted { .. }
        ));
        assert_eq!(
            directory.acquire(second.clone(), bounded).await.unwrap(),
            OnlineAdmission::RealmFull
        );
        directory.unregister(&first).await.unwrap();
        assert!(matches!(
            directory.acquire(second, bounded).await.unwrap(),
            OnlineAdmission::Accepted { .. }
        ));
    }

    #[tokio::test]
    async fn provider_neutral_resolver_routes_users_and_topics() {
        let directory = Arc::new(MemoryOnlineDirectory::default());
        let first = lease(Uuid::new_v4());
        let mut second = lease(Uuid::new_v4());
        second.gateway_id = "gateway-b".into();
        directory
            .acquire(first.clone(), policy(DuplicateLoginMode::AllowMultiple))
            .await
            .unwrap();
        directory
            .acquire(second.clone(), policy(DuplicateLoginMode::AllowMultiple))
            .await
            .unwrap();
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

    #[tokio::test]
    async fn reports_session_and_distinct_user_counts_by_realm() {
        let directory = MemoryOnlineDirectory::default();
        let first = lease(Uuid::new_v4());
        let mut second = lease(Uuid::new_v4());
        second.gateway_id = "gateway-b".into();
        let mut third = lease(Uuid::new_v4());
        third.identity.user_id = 3;
        let mut other_realm = lease(Uuid::new_v4());
        other_realm.identity.realm_id = 5;
        let mut expired = lease(Uuid::new_v4());
        expired.identity.user_id = 4;
        expired.expires_at = SystemTime::now() - Duration::from_secs(1);

        for lease in [first, second, third, other_realm, expired] {
            directory
                .acquire(lease, policy(DuplicateLoginMode::AllowMultiple))
                .await
                .unwrap();
        }
        let stats: &dyn OnlineStatsReader = &directory;

        assert_eq!(
            stats.stats(3, 4).await.unwrap(),
            OnlineStats {
                session_count: 3,
                user_count: 2,
            }
        );
        assert_eq!(
            stats.stats(3, 5).await.unwrap(),
            OnlineStats {
                session_count: 1,
                user_count: 1,
            }
        );
        assert_eq!(stats.stats(9, 9).await.unwrap(), OnlineStats::default());
    }
}
