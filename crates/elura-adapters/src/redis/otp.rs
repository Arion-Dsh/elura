use std::time::Duration;

use async_trait::async_trait;
use elura_core::otp::{OtpCreateResult, OtpRecord, OtpStore, OtpVerifyResult};
use elura_core::{Error, Result};
use redis::aio::ConnectionManager;

const CREATE: &str = r#"
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'digest', ARGV[1], 'attempts', 0)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('SET', KEYS[2], '1', 'PX', ARGV[3])
return 1
"#;

const VERIFY: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if redis.call('HGET', KEYS[1], 'digest') == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
local attempts = redis.call('HINCRBY', KEYS[1], 'attempts', 1)
if attempts >= tonumber(ARGV[2]) then redis.call('DEL', KEYS[1]) end
return -1
"#;

#[derive(Clone)]
pub struct RedisOtpStore {
    connection: ConnectionManager,
    prefix: String,
}

impl RedisOtpStore {
    pub async fn connect(url: &str, prefix: impl Into<String>) -> Result<Self> {
        let client = redis::Client::open(url).map_err(redis_error)?;
        let connection = client.get_connection_manager().await.map_err(redis_error)?;
        Self::new(connection, prefix)
    }

    pub fn new(connection: ConnectionManager, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        if prefix.trim().is_empty() {
            return Err(Error::InvalidConfig("Redis OTP prefix is empty".into()));
        }
        Ok(Self { connection, prefix })
    }

    fn challenge_key(&self, subject: &str, purpose: &str) -> String {
        format!("{}:challenge:{subject}:{purpose}", self.prefix)
    }

    fn cooldown_key(&self, subject: &str, purpose: &str) -> String {
        format!("{}:cooldown:{subject}:{purpose}", self.prefix)
    }
}

#[async_trait]
impl OtpStore for RedisOtpStore {
    async fn create(
        &self,
        record: OtpRecord,
        ttl: Duration,
        cooldown: Duration,
    ) -> Result<OtpCreateResult> {
        record.validate()?;
        if ttl.is_zero() || cooldown.is_zero() || cooldown > ttl {
            return Err(Error::InvalidConfig("invalid Redis OTP expiry".into()));
        }
        let mut connection = self.connection.clone();
        let stored = redis::Script::new(CREATE)
            .key(self.challenge_key(&record.subject_key, &record.purpose))
            .key(self.cooldown_key(&record.subject_key, &record.purpose))
            .arg(hex::encode(record.code_digest))
            .arg(ttl.as_millis())
            .arg(cooldown.as_millis())
            .invoke_async::<i64>(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(if stored == 1 {
            OtpCreateResult::Stored
        } else {
            OtpCreateResult::Cooldown
        })
    }

    async fn verify_and_consume(
        &self,
        record: OtpRecord,
        max_attempts: u32,
    ) -> Result<OtpVerifyResult> {
        record.validate()?;
        if max_attempts == 0 {
            return Err(Error::InvalidConfig(
                "Redis OTP max attempts must be positive".into(),
            ));
        }
        let mut connection = self.connection.clone();
        let result = redis::Script::new(VERIFY)
            .key(self.challenge_key(&record.subject_key, &record.purpose))
            .arg(hex::encode(record.code_digest))
            .arg(max_attempts)
            .invoke_async::<i64>(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(match result {
            1 => OtpVerifyResult::Valid,
            -1 => OtpVerifyResult::Invalid,
            _ => OtpVerifyResult::Missing,
        })
    }

    async fn delete(&self, subject_key: &str, purpose: &str) -> Result<()> {
        let mut connection = self.connection.clone();
        redis::cmd("DEL")
            .arg(self.challenge_key(subject_key, purpose))
            .arg(self.cooldown_key(subject_key, purpose))
            .query_async::<u64>(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(())
    }
}

fn redis_error(error: redis::RedisError) -> Error {
    super::map_redis_error("Redis OTP", error)
}
