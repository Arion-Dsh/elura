use async_trait::async_trait;
use elura_core::online::{OnlineDirectory, SessionLease};
use elura_core::{Error, Result};
use elura_runtime::observability::ReadinessProbe;
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
    async fn register(&self, lease: SessionLease) -> Result<()> {
        RedisOnlineDirectory::register(self, lease).await
    }

    async fn renew(&self, lease: SessionLease) -> Result<()> {
        RedisOnlineDirectory::register(self, lease).await
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

    async fn claim_single(&self, lease: &SessionLease, replace: bool) -> Result<Option<Uuid>> {
        RedisOnlineDirectory::claim_single(self, lease, replace).await
    }

    async fn release_single(&self, lease: &SessionLease) -> Result<()> {
        let key = self.key(&format!(
            "single:{}:{}:{}",
            lease.identity.region_id, lease.identity.realm_id, lease.identity.user_id
        ));
        let script = redis::Script::new(
            "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0",
        );
        let mut connection = self.connection.clone();
        script
            .key(key)
            .arg(lease.session_id.to_string())
            .invoke_async::<i64>(&mut connection)
            .await
            .map_err(err)?;
        Ok(())
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

    pub async fn register(&self, mut lease: SessionLease) -> Result<()> {
        lease.identity.validate()?;
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
        let script = redis::Script::new(
            "redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[3]);redis.call('SADD',KEYS[2],ARGV[2]);redis.call('PEXPIRE',KEYS[2],ARGV[3]);if redis.call('GET',KEYS[3])==ARGV[2] then redis.call('PEXPIRE',KEYS[3],ARGV[3]) end;local groups=redis.call('SMEMBERS',KEYS[4]);if #groups>0 then redis.call('PEXPIRE',KEYS[4],ARGV[3]);for _,group in ipairs(groups) do redis.call('PEXPIRE',group,ARGV[3]) end end;return 1",
        );
        let mut c = self.connection.clone();
        script
            .key(session)
            .key(user)
            .key(single)
            .key(self.key(&format!("session-groups:{}", lease.session_id)))
            .arg(payload)
            .arg(lease.session_id.to_string())
            .arg(self.ttl.as_millis())
            .invoke_async::<i64>(&mut c)
            .await
            .map(|_| ())
            .map_err(err)
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
        let lease = self.session(id).await?;
        let mut c = self.connection.clone();
        let memberships = self.key(&format!("session-groups:{id}"));
        let groups: Vec<String> = redis::cmd("SMEMBERS")
            .arg(&memberships)
            .query_async(&mut c)
            .await
            .map_err(err)?;
        let mut p = redis::pipe();
        p.atomic()
            .cmd("DEL")
            .arg(self.key(&format!("session:{id}")))
            .ignore()
            .cmd("DEL")
            .arg(memberships)
            .ignore();
        for group in groups {
            p.cmd("SREM").arg(group).arg(id.to_string()).ignore();
        }
        if let Some(x) = lease {
            p.cmd("SREM")
                .arg(self.key(&format!(
                    "user:{}:{}:{}",
                    x.identity.region_id, x.identity.realm_id, x.identity.user_id
                )))
                .arg(id.to_string())
                .ignore();
        }
        p.query_async::<()>(&mut c).await.map_err(err)
    }
    pub async fn claim_single(&self, lease: &SessionLease, replace: bool) -> Result<Option<Uuid>> {
        let key = self.key(&format!(
            "single:{}:{}:{}",
            lease.identity.region_id, lease.identity.realm_id, lease.identity.user_id
        ));
        let script = redis::Script::new(
            "local o=redis.call('GET',KEYS[1]);if o and ARGV[2]=='0' then return o end;redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[3]);return o or ''",
        );
        let mut c = self.connection.clone();
        let old: String = script
            .key(key)
            .arg(lease.session_id.to_string())
            .arg(if replace { "1" } else { "0" })
            .arg(self.ttl.as_millis())
            .invoke_async(&mut c)
            .await
            .map_err(err)?;
        Ok(Uuid::parse_str(&old)
            .ok()
            .filter(|v| *v != lease.session_id))
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
