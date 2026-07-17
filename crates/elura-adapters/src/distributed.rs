use async_trait::async_trait;
use elura_core::online::{
    DuplicateLoginMode, OnlineAdmission, OnlineAdmissionPolicy, OnlineDirectory, OnlineStats,
    OnlineStatsReader, SessionLease,
};
use elura_core::{Error, Result};
use elura_runtime::observability::ReadinessProbe;
use std::collections::HashSet;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

use crate::redis::{
    RedisConnection, cluster_connection, standalone_connection, validate_key_prefix,
};

#[derive(Clone)]
pub struct RedisOnlineDirectory {
    connection: RedisConnection,
    prefix: String,
    ttl: Duration,
}

#[async_trait]
impl OnlineDirectory for RedisOnlineDirectory {
    async fn acquire(
        &self,
        lease: SessionLease,
        policy: OnlineAdmissionPolicy,
    ) -> Result<OnlineAdmission> {
        RedisOnlineDirectory::acquire(self, lease, policy).await
    }

    async fn renew(&self, lease: SessionLease) -> Result<()> {
        if self
            .session(lease.session_id)
            .await?
            .is_some_and(|current| current.gateway_id == lease.gateway_id)
        {
            RedisOnlineDirectory::renew(self, lease).await
        } else {
            Err(Error::SessionRevoked)
        }
    }

    async fn unregister(&self, lease: &SessionLease) -> Result<()> {
        if self
            .session(lease.session_id)
            .await?
            .is_some_and(|current| current.gateway_id == lease.gateway_id)
        {
            self.remove(lease.session_id).await?;
        }
        Ok(())
    }

    async fn session(&self, session_id: Uuid) -> Result<Option<SessionLease>> {
        RedisOnlineDirectory::session(self, session_id).await
    }

    async fn user_sessions(&self, r: u32, m: u32, u: i64) -> Result<Vec<SessionLease>> {
        RedisOnlineDirectory::user_sessions(self, r, m, u).await
    }

    async fn group_sessions(&self, group: &str) -> Result<Vec<SessionLease>> {
        RedisOnlineDirectory::group_sessions(self, group).await
    }

    async fn track_group(&self, session_id: Uuid, group: &str, join: bool) -> Result<()> {
        self.track(session_id, group, join).await
    }
}

#[async_trait]
impl OnlineStatsReader for RedisOnlineDirectory {
    async fn stats(&self, region_id: u32, realm_id: u32) -> Result<OnlineStats> {
        RedisOnlineDirectory::stats(self, region_id, realm_id).await
    }
}

#[async_trait]
impl ReadinessProbe for RedisOnlineDirectory {
    async fn check(&self) -> Result<()> {
        let mut connection = self.connection.clone();
        redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .map(|_| ())
            .map_err(err)
    }
}
impl RedisOnlineDirectory {
    pub async fn connect(url: &str, prefix: impl Into<String>, ttl: Duration) -> Result<Self> {
        Self::from_connection(standalone_connection(url).await?, prefix, ttl)
    }

    pub async fn connect_cluster<I, S>(
        nodes: I,
        prefix: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::from_connection(cluster_connection(nodes).await?, prefix, ttl)
    }

    fn from_connection(
        connection: RedisConnection,
        prefix: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self> {
        let prefix = prefix.into();
        validate_key_prefix(&prefix)?;
        if ttl.is_zero() {
            return Err(Error::InvalidConfig(
                "online directory lease TTL must be positive".into(),
            ));
        }
        let prefix = connection.atomic_prefix(&prefix)?;
        Ok(Self {
            connection,
            prefix,
            ttl,
        })
    }

    pub(crate) fn key(&self, s: &str) -> String {
        format!("{}:{s}", self.prefix)
    }

    pub(crate) fn connection(&self) -> RedisConnection {
        self.connection.clone()
    }

    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    pub async fn acquire(
        &self,
        mut lease: SessionLease,
        policy: OnlineAdmissionPolicy,
    ) -> Result<OnlineAdmission> {
        lease.identity.validate()?;
        let policy = OnlineAdmissionPolicy::new(policy.duplicate_login, policy.max_sessions)?;
        lease.expires_at = SystemTime::now() + self.ttl;
        let payload = serde_json::to_vec(&lease)?;
        let user = self.key(&format!(
            "user:{}:{}:{}",
            lease.identity.region_id, lease.identity.realm_id, lease.identity.user_id
        ));
        let session = self.key(&format!("session:{}", lease.session_id));
        let single = self.key(&format!(
            "single:{}:{}:{}",
            lease.identity.region_id, lease.identity.realm_id, lease.identity.user_id
        ));
        let realm = self.key(&format!(
            "realm:{}:{}",
            lease.identity.region_id, lease.identity.realm_id
        ));
        let capacity = self.key(&format!(
            "capacity:{}:{}",
            lease.identity.region_id, lease.identity.realm_id
        ));
        let script = redis::Script::new(
            r#"
local session_id = ARGV[2]
local ttl = ARGV[3]
local mode = ARGV[4]
local maximum = tonumber(ARGV[5])
local session_prefix = ARGV[6]
local previous = redis.call('GET', KEYS[3])

if previous == session_id then previous = false end
if previous and redis.call('EXISTS', session_prefix .. previous) == 0 then
  redis.call('DEL', KEYS[3])
  redis.call('SREM', KEYS[6], previous)
  previous = false
end
if mode == 'reject_new' and previous then
  return {'duplicate', previous}
end

local used = 0
for _, id in ipairs(redis.call('SMEMBERS', KEYS[6])) do
  if redis.call('EXISTS', session_prefix .. id) == 1 then
    used = used + 1
  else
    redis.call('SREM', KEYS[6], id)
  end
end
local transfers = mode == 'kick_existing' and previous
  and redis.call('SISMEMBER', KEYS[6], previous) == 1
if transfers then used = used - 1 end
local already = redis.call('SISMEMBER', KEYS[6], session_id) == 1
if maximum > 0 and not already and used >= maximum then
  return {'full', ''}
end

if transfers then redis.call('SREM', KEYS[6], previous) end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ttl)
redis.call('SADD', KEYS[2], session_id)
redis.call('PEXPIRE', KEYS[2], ttl)
if mode ~= 'allow_multiple' then
  redis.call('SET', KEYS[3], session_id, 'PX', ttl)
end
local groups = redis.call('SMEMBERS', KEYS[4])
if #groups > 0 then
  redis.call('PEXPIRE', KEYS[4], ttl)
  for _, group in ipairs(groups) do redis.call('PEXPIRE', group, ttl) end
end
redis.call('SADD', KEYS[5], session_id)
redis.call('PEXPIRE', KEYS[5], ttl)
redis.call('SADD', KEYS[6], session_id)
redis.call('PEXPIRE', KEYS[6], ttl)
if mode == 'kick_existing' then return {'accepted', previous or ''} end
return {'accepted', ''}
"#,
        );
        let mut c = self.connection.clone();
        let result: Vec<String> = script
            .key(session)
            .key(user)
            .key(single)
            .key(self.key(&format!("session-groups:{}", lease.session_id)))
            .key(realm)
            .key(capacity)
            .arg(payload)
            .arg(lease.session_id.to_string())
            .arg(self.ttl.as_millis())
            .arg(match policy.duplicate_login {
                DuplicateLoginMode::AllowMultiple => "allow_multiple",
                DuplicateLoginMode::RejectNew => "reject_new",
                DuplicateLoginMode::KickExisting => "kick_existing",
            })
            .arg(policy.max_sessions.unwrap_or(0))
            .arg(self.key("session:"))
            .invoke_async(&mut c)
            .await
            .map_err(err)?;
        match result.as_slice() {
            [status, _] if status == "duplicate" => Ok(OnlineAdmission::Duplicate),
            [status, _] if status == "full" => Ok(OnlineAdmission::RealmFull),
            [status, previous] if status == "accepted" => Ok(OnlineAdmission::Accepted {
                previous_session: Uuid::parse_str(previous).ok(),
            }),
            _ => Err(Error::Internal(
                "invalid Redis online admission response".into(),
            )),
        }
    }

    pub async fn renew(&self, mut lease: SessionLease) -> Result<()> {
        lease.identity.validate()?;
        lease.expires_at = SystemTime::now() + self.ttl;
        let payload = serde_json::to_vec(&lease)?;
        let identity = &lease.identity;
        let script = redis::Script::new(
            r#"
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
redis.call('PEXPIRE', KEYS[2], ARGV[3])
if redis.call('GET', KEYS[3]) == ARGV[2] then redis.call('PEXPIRE', KEYS[3], ARGV[3]) end
redis.call('PEXPIRE', KEYS[4], ARGV[3])
redis.call('PEXPIRE', KEYS[5], ARGV[3])
redis.call('PEXPIRE', KEYS[6], ARGV[3])
for _, group in ipairs(redis.call('SMEMBERS', KEYS[4])) do
  redis.call('PEXPIRE', group, ARGV[3])
end
return 1
"#,
        );
        let mut connection = self.connection.clone();
        let renewed: i64 = script
            .key(self.key(&format!("session:{}", lease.session_id)))
            .key(self.key(&format!(
                "user:{}:{}:{}",
                identity.region_id, identity.realm_id, identity.user_id
            )))
            .key(self.key(&format!(
                "single:{}:{}:{}",
                identity.region_id, identity.realm_id, identity.user_id
            )))
            .key(self.key(&format!("session-groups:{}", lease.session_id)))
            .key(self.key(&format!(
                "realm:{}:{}",
                identity.region_id, identity.realm_id
            )))
            .key(self.key(&format!(
                "capacity:{}:{}",
                identity.region_id, identity.realm_id
            )))
            .arg(payload)
            .arg(lease.session_id.to_string())
            .arg(self.ttl.as_millis())
            .invoke_async(&mut connection)
            .await
            .map_err(err)?;
        if renewed == 1 {
            Ok(())
        } else {
            Err(Error::SessionRevoked)
        }
    }
    pub async fn session(&self, id: Uuid) -> Result<Option<SessionLease>> {
        let mut c = self.connection.clone();
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.key(&format!("session:{id}")))
            .query_async(&mut c)
            .await
            .map_err(err)?;
        value
            .map(|v| serde_json::from_slice(&v).map_err(Error::from))
            .transpose()
    }
    pub async fn remove(&self, id: Uuid) -> Result<()> {
        let Some(lease) = self.session(id).await? else {
            return Ok(());
        };
        let mut c = self.connection.clone();
        let memberships = self.key(&format!("session-groups:{id}"));
        let groups: Vec<String> = redis::cmd("SMEMBERS")
            .arg(&memberships)
            .query_async(&mut c)
            .await
            .map_err(err)?;
        let identity = lease.identity;
        let script = redis::Script::new(
            r#"
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
redis.call('SREM', KEYS[3], ARGV[1])
redis.call('SREM', KEYS[4], ARGV[1])
redis.call('SREM', KEYS[5], ARGV[1])
if redis.call('GET', KEYS[6]) == ARGV[1] then redis.call('DEL', KEYS[6]) end
for index = 2, #ARGV do redis.call('SREM', ARGV[index], ARGV[1]) end
return 1
"#,
        );
        let mut invocation = script.key(self.key(&format!("session:{id}")));
        invocation
            .key(memberships)
            .key(self.key(&format!(
                "user:{}:{}:{}",
                identity.region_id, identity.realm_id, identity.user_id
            )))
            .key(self.key(&format!(
                "realm:{}:{}",
                identity.region_id, identity.realm_id
            )))
            .key(self.key(&format!(
                "capacity:{}:{}",
                identity.region_id, identity.realm_id
            )))
            .key(self.key(&format!(
                "single:{}:{}:{}",
                identity.region_id, identity.realm_id, identity.user_id
            )))
            .arg(id.to_string());
        for group in groups {
            invocation.arg(group);
        }
        invocation
            .invoke_async::<i64>(&mut c)
            .await
            .map(|_| ())
            .map_err(err)
    }
    async fn sessions_in(&self, key: String) -> Result<Vec<SessionLease>> {
        let mut c = self.connection.clone();
        let raw: Vec<String> = redis::cmd("SMEMBERS")
            .arg(&key)
            .query_async(&mut c)
            .await
            .map_err(err)?;
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        let session_keys: Vec<String> = raw
            .iter()
            .map(|id| self.key(&format!("session:{id}")))
            .collect();
        let values: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(session_keys)
            .query_async(&mut c)
            .await
            .map_err(err)?;
        let mut sessions = Vec::with_capacity(values.len());
        let mut stale = Vec::new();
        for (id, value) in raw.into_iter().zip(values) {
            match value {
                Some(value) => sessions.push(serde_json::from_slice(&value)?),
                None => stale.push(id),
            }
        }
        if !stale.is_empty() {
            let mut cleanup = redis::pipe();
            cleanup.atomic().cmd("SREM").arg(&key).arg(&stale).ignore();
            for id in stale {
                cleanup
                    .cmd("DEL")
                    .arg(self.key(&format!("session-groups:{id}")))
                    .ignore();
            }
            cleanup.query_async::<()>(&mut c).await.map_err(err)?;
        }
        Ok(sessions)
    }
    pub async fn user_sessions(&self, r: u32, m: u32, u: i64) -> Result<Vec<SessionLease>> {
        self.sessions_in(self.key(&format!("user:{r}:{m}:{u}")))
            .await
    }
    pub async fn stats(&self, region_id: u32, realm_id: u32) -> Result<OnlineStats> {
        let sessions = self
            .sessions_in(self.key(&format!("realm:{region_id}:{realm_id}")))
            .await?;
        let mut user_ids = HashSet::new();
        let mut session_count = 0;
        for lease in sessions {
            if lease.identity.region_id == region_id && lease.identity.realm_id == realm_id {
                session_count += 1;
                user_ids.insert(lease.identity.user_id);
            }
        }
        Ok(OnlineStats {
            session_count,
            user_count: user_ids.len() as u64,
        })
    }
    pub async fn track(&self, id: Uuid, group: &str, join: bool) -> Result<()> {
        if group.is_empty() || group.len() > 256 {
            return Err(Error::InvalidConfig("group".into()));
        }
        if join && self.session(id).await?.is_none() {
            return Err(Error::Unavailable);
        }
        let mut c = self.connection.clone();
        let cmd = if join { "SADD" } else { "SREM" };
        let group_key = self.key(&format!("group:{group}"));
        let mut pipeline = redis::pipe();
        pipeline
            .atomic()
            .cmd(cmd)
            .arg(&group_key)
            .arg(id.to_string())
            .ignore()
            .cmd(cmd)
            .arg(self.key(&format!("session-groups:{id}")))
            .arg(&group_key)
            .ignore();
        if join {
            pipeline
                .cmd("PEXPIRE")
                .arg(&group_key)
                .arg(self.ttl.as_millis())
                .ignore()
                .cmd("PEXPIRE")
                .arg(self.key(&format!("session-groups:{id}")))
                .arg(self.ttl.as_millis())
                .ignore();
        }
        pipeline.query_async::<()>(&mut c).await.map_err(err)
    }
    pub async fn group_sessions(&self, group: &str) -> Result<Vec<SessionLease>> {
        if group.is_empty() || group.len() > 256 {
            return Err(Error::InvalidConfig("group".into()));
        }
        self.sessions_in(self.key(&format!("group:{group}"))).await
    }
}
fn err(e: redis::RedisError) -> Error {
    crate::redis::map_redis_error("redis online directory", e)
}

#[cfg(test)]
mod tests {
    use redis::cluster_routing::Slot;

    #[test]
    fn cluster_transport_keys_share_one_slot() {
        let prefix = "elura:{transport}";
        let keys = [
            format!("{prefix}:session:one"),
            format!("{prefix}:user:1:1:1"),
            format!("{prefix}:gateway:push:gateway-1"),
            format!("{prefix}:session:control"),
        ];
        let expected = Slot::for_key(&keys[0]);
        assert!(keys.iter().all(|key| Slot::for_key(key) == expected));
    }
}
