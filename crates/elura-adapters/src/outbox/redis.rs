use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use elura_core::outbox::{
    DeadLetter, OutboxDelivery, OutboxEvent, OutboxStore, validate_failure_reason,
};
use elura_core::{Error, Result};
use redis::Script;
use uuid::Uuid;

use crate::redis::{RedisConnection, standalone_connection, validate_key_prefix};

const ACQUIRE: &str = r#"
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
for _, id in ipairs(expired) do
  if redis.call('HEXISTS', KEYS[1], id) == 1 then
    redis.call('ZADD', KEYS[2], ARGV[1], id)
  end
  redis.call('ZREM', KEYS[3], id)
  redis.call('HDEL', KEYS[4], id)
end
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
local result = {}
for _, id in ipairs(ids) do
  local event = redis.call('HGET', KEYS[1], id)
  if event then
    redis.call('ZREM', KEYS[2], id)
    redis.call('ZADD', KEYS[3], ARGV[2], id)
    redis.call('HSET', KEYS[4], id, ARGV[4])
    local attempt = redis.call('HINCRBY', KEYS[5], id, 1)
    table.insert(result, id)
    table.insert(result, event)
    table.insert(result, tostring(attempt))
  else
    redis.call('ZREM', KEYS[2], id)
  end
end
return result
"#;

#[derive(Clone)]
pub struct RedisOutbox {
    connection: RedisConnection,
    prefix: String,
}

impl RedisOutbox {
    pub async fn connect(url: &str, prefix: impl Into<String>) -> Result<Self> {
        Self::from_connection(standalone_connection(url).await?, prefix)
    }

    fn from_connection(connection: RedisConnection, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        validate_key_prefix(&prefix)?;
        Ok(Self { connection, prefix })
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:outbox:{suffix}", self.prefix)
    }

    fn keys(&self, suffixes: &[&str]) -> Vec<String> {
        suffixes.iter().map(|suffix| self.key(suffix)).collect()
    }
}

#[async_trait]
impl OutboxStore for RedisOutbox {
    async fn append(&self, event: OutboxEvent) -> Result<()> {
        event.validate()?;
        let id = event.id.to_string();
        let encoded = serde_json::to_vec(&event)?;
        let available = millis(event.available_at)?;
        let keys = self.keys(&["dedup", "events", "available"]);
        let script = Script::new(
            r#"
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then return existing end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
return ARGV[2]
"#,
        );
        let mut connection = self.connection.clone();
        let stored: Vec<u8> = script
            .key(&keys[0])
            .key(&keys[1])
            .key(&keys[2])
            .arg(id)
            .arg(encoded)
            .arg(available)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        let stored: OutboxEvent = serde_json::from_slice(&stored)?;
        if stored.same_identity(&event) {
            Ok(())
        } else {
            Err(Error::DuplicateEvent)
        }
    }

    async fn acquire(
        &self,
        worker: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<OutboxDelivery>> {
        validate_lease(worker, limit, lease)?;
        let now = now_millis()?;
        let lease_until = SystemTime::now() + lease;
        let lease_until_ms = millis(lease_until)?;
        let token = Uuid::new_v4();
        let owner = format!("{worker}|{token}");
        let keys = self.keys(&["events", "available", "leases", "lease-info", "attempts"]);
        let mut connection = self.connection.clone();
        let values: Vec<Vec<u8>> = Script::new(ACQUIRE)
            .key(&keys[0])
            .key(&keys[1])
            .key(&keys[2])
            .key(&keys[3])
            .key(&keys[4])
            .arg(now)
            .arg(lease_until_ms)
            .arg(limit)
            .arg(owner)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        if !values.len().is_multiple_of(3) {
            return Err(Error::Internal("invalid redis outbox response".into()));
        }
        values
            .chunks_exact(3)
            .map(|chunk| {
                let event: OutboxEvent = serde_json::from_slice(&chunk[1])?;
                let attempt = std::str::from_utf8(&chunk[2])
                    .map_err(|error| Error::Internal(error.to_string()))?
                    .parse::<u32>()
                    .map_err(|error| Error::Internal(error.to_string()))?;
                Ok(OutboxDelivery {
                    event,
                    attempt,
                    worker: worker.to_owned(),
                    token,
                    lease_until,
                })
            })
            .collect()
    }

    async fn ack(&self, delivery: &OutboxDelivery) -> Result<()> {
        let keys = self.keys(&[
            "events",
            "available",
            "leases",
            "lease-info",
            "attempts",
            "errors",
        ]);
        let mut connection = self.connection.clone();
        let affected: i32 = Script::new(
            r#"
local owner = redis.call('HGET', KEYS[4], ARGV[1])
local until_at = redis.call('ZSCORE', KEYS[3], ARGV[1])
if owner ~= ARGV[2] or not until_at or tonumber(until_at) <= tonumber(ARGV[3]) then return 0 end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
return 1
"#,
        )
        .key(&keys[0])
        .key(&keys[1])
        .key(&keys[2])
        .key(&keys[3])
        .key(&keys[4])
        .key(&keys[5])
        .arg(delivery.event.id.to_string())
        .arg(owner(delivery))
        .arg(now_millis()?)
        .invoke_async(&mut connection)
        .await
        .map_err(redis_error)?;
        fenced(affected)
    }

    async fn renew(&self, delivery: &OutboxDelivery, lease: Duration) -> Result<()> {
        validate_lease(&delivery.worker, 1, lease)?;
        let keys = self.keys(&["leases", "lease-info"]);
        let mut connection = self.connection.clone();
        let affected: i32 = Script::new(
            r#"
local owner = redis.call('HGET', KEYS[2], ARGV[1])
local until_at = redis.call('ZSCORE', KEYS[1], ARGV[1])
if owner ~= ARGV[2] or not until_at or tonumber(until_at) <= tonumber(ARGV[3]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1])
return 1
"#,
        )
        .key(&keys[0])
        .key(&keys[1])
        .arg(delivery.event.id.to_string())
        .arg(owner(delivery))
        .arg(now_millis()?)
        .arg(millis(SystemTime::now() + lease)?)
        .invoke_async(&mut connection)
        .await
        .map_err(redis_error)?;
        fenced(affected)
    }

    async fn retry(
        &self,
        delivery: &OutboxDelivery,
        available_at: SystemTime,
        reason: &str,
    ) -> Result<()> {
        validate_failure_reason(reason)?;
        let mut event = delivery.event.clone();
        event.available_at = available_at.max(SystemTime::now());
        let keys = self.keys(&["events", "available", "leases", "lease-info", "errors"]);
        let mut connection = self.connection.clone();
        let affected: i32 = Script::new(
            r#"
local owner = redis.call('HGET', KEYS[4], ARGV[1])
local until_at = redis.call('ZSCORE', KEYS[3], ARGV[1])
if owner ~= ARGV[2] or not until_at or tonumber(until_at) <= tonumber(ARGV[3]) then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
redis.call('ZADD', KEYS[2], ARGV[5], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HSET', KEYS[5], ARGV[1], ARGV[6])
return 1
"#,
        )
        .key(&keys[0])
        .key(&keys[1])
        .key(&keys[2])
        .key(&keys[3])
        .key(&keys[4])
        .arg(delivery.event.id.to_string())
        .arg(owner(delivery))
        .arg(now_millis()?)
        .arg(serde_json::to_vec(&event)?)
        .arg(millis(event.available_at)?)
        .arg(reason)
        .invoke_async(&mut connection)
        .await
        .map_err(redis_error)?;
        fenced(affected)
    }

    async fn dead_letter(&self, delivery: &OutboxDelivery, reason: &str) -> Result<()> {
        validate_failure_reason(reason)?;
        let letter = DeadLetter {
            event: delivery.event.clone(),
            attempt: delivery.attempt,
            reason: reason.to_owned(),
            failed_at: SystemTime::now(),
        };
        let keys = self.keys(&[
            "dead",
            "events",
            "available",
            "leases",
            "lease-info",
            "attempts",
            "errors",
        ]);
        let mut connection = self.connection.clone();
        let affected: i32 = Script::new(
            r#"
local owner = redis.call('HGET', KEYS[5], ARGV[1])
local until_at = redis.call('ZSCORE', KEYS[4], ARGV[1])
if owner ~= ARGV[2] or not until_at or tonumber(until_at) <= tonumber(ARGV[3]) then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
redis.call('HDEL', KEYS[7], ARGV[1])
return 1
"#,
        )
        .key(&keys[0])
        .key(&keys[1])
        .key(&keys[2])
        .key(&keys[3])
        .key(&keys[4])
        .key(&keys[5])
        .key(&keys[6])
        .arg(delivery.event.id.to_string())
        .arg(owner(delivery))
        .arg(now_millis()?)
        .arg(serde_json::to_vec(&letter)?)
        .invoke_async(&mut connection)
        .await
        .map_err(redis_error)?;
        fenced(affected)
    }

    async fn list_dead_letters(&self, limit: usize) -> Result<Vec<DeadLetter>> {
        if limit == 0 {
            return Err(Error::InvalidConfig("dead-letter limit is zero".into()));
        }
        let mut connection = self.connection.clone();
        let values: Vec<Vec<u8>> = redis::cmd("HVALS")
            .arg(self.key("dead"))
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        let mut letters: Vec<DeadLetter> = values
            .iter()
            .map(|value| serde_json::from_slice(value).map_err(Error::from))
            .collect::<Result<_>>()?;
        letters.sort_by_key(|letter| std::cmp::Reverse(letter.failed_at));
        letters.truncate(limit);
        Ok(letters)
    }

    async fn replay_dead_letter(&self, id: Uuid, available_at: SystemTime) -> Result<()> {
        let keys = self.keys(&[
            "dead",
            "events",
            "available",
            "leases",
            "lease-info",
            "attempts",
            "errors",
        ]);
        let mut connection = self.connection.clone();
        let encoded: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(&keys[0])
            .arg(id.to_string())
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        let Some(encoded) = encoded else {
            return Err(Error::OutboxNotFound);
        };
        let mut letter: DeadLetter = serde_json::from_slice(&encoded)?;
        letter.event.available_at = available_at.max(SystemTime::now());
        let affected: i32 = Script::new(
            r#"
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then return 0 end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
redis.call('HDEL', KEYS[7], ARGV[1])
return 1
"#,
        )
        .key(&keys[0])
        .key(&keys[1])
        .key(&keys[2])
        .key(&keys[3])
        .key(&keys[4])
        .key(&keys[5])
        .key(&keys[6])
        .arg(id.to_string())
        .arg(serde_json::to_vec(&letter.event)?)
        .arg(millis(letter.event.available_at)?)
        .invoke_async(&mut connection)
        .await
        .map_err(redis_error)?;
        if affected == 1 {
            Ok(())
        } else {
            Err(Error::OutboxNotFound)
        }
    }
}

fn validate_lease(worker: &str, limit: usize, lease: Duration) -> Result<()> {
    if worker.trim().is_empty()
        || worker.len() > 128
        || limit == 0
        || limit > 4096
        || lease.is_zero()
    {
        Err(Error::InvalidConfig("invalid outbox lease".into()))
    } else {
        Ok(())
    }
}

fn owner(delivery: &OutboxDelivery) -> String {
    format!("{}|{}", delivery.worker, delivery.token)
}

fn fenced(affected: i32) -> Result<()> {
    if affected == 1 {
        Ok(())
    } else {
        Err(Error::OutboxLeaseLost)
    }
}

fn now_millis() -> Result<u64> {
    millis(SystemTime::now())
}

fn millis(time: SystemTime) -> Result<u64> {
    let value = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::InvalidConfig("outbox time precedes unix epoch".into()))?
        .as_millis();
    u64::try_from(value).map_err(|_| Error::InvalidConfig("outbox time overflow".into()))
}

fn redis_error(error: redis::RedisError) -> Error {
    crate::redis::map_redis_error("Redis Outbox", error)
}
