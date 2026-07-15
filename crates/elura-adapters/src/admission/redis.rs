use std::net::IpAddr;
use std::time::Duration;

use async_trait::async_trait;
use elura_core::session::Identity;
use elura_core::{Error, Result};
use elura_gateway::observability::AdmissionAdmin;
use elura_gateway::transport::{
    AdmissionController, AdmissionDecision, AdmissionRejection, AdmissionRequest, AdmissionStage,
};
use redis::Script;
use redis::aio::ConnectionManager;

const ADMIT: &str = r#"
local function remaining(key)
  local ttl = redis.call('PTTL', key)
  if ttl < 1 then return '0' end
  return tostring(ttl)
end

local maintenance = redis.call('GET', KEYS[1])
if maintenance then return {'1', maintenance, remaining(KEYS[1])} end

local ip_ban = redis.call('GET', KEYS[2])
if ip_ban then return {'2', ip_ban, remaining(KEYS[2])} end

if ARGV[1] == 'authenticated' then
  local user_ban = redis.call('GET', KEYS[3])
  if user_ban then return {'3', user_ban, remaining(KEYS[3])} end
end

if ARGV[1] == 'connected' and tonumber(ARGV[2]) > 0 then
  local count = redis.call('INCR', KEYS[4])
  if count == 1 then redis.call('PEXPIRE', KEYS[4], ARGV[3]) end
  if count > tonumber(ARGV[2]) then
    return {'4', 'connection rate exceeded', remaining(KEYS[4])}
  end
end

if ARGV[1] == 'authenticated' and tonumber(ARGV[4]) > 0 then
  local count = redis.call('INCR', KEYS[5])
  if count == 1 then redis.call('PEXPIRE', KEYS[5], ARGV[5]) end
  if count > tonumber(ARGV[4]) then
    return {'5', 'authentication rate exceeded', remaining(KEYS[5])}
  end
end

return {'0', '', '0'}
"#;

#[derive(Debug, Clone)]
pub struct AdmissionLimit {
    pub maximum: u32,
    pub window: Duration,
}

impl AdmissionLimit {
    fn validate(&self) -> Result<()> {
        if self.maximum == 0 || self.window.is_zero() {
            return Err(Error::InvalidConfig(
                "admission limit requires a positive maximum and window".into(),
            ));
        }
        milliseconds(self.window)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RedisAdmissionConfig {
    pub prefix: String,
    pub connection_limit: Option<AdmissionLimit>,
    pub authentication_limit: Option<AdmissionLimit>,
}

impl RedisAdmissionConfig {
    fn validate(&self) -> Result<()> {
        if self.prefix.trim().is_empty() || self.prefix.len() > 128 {
            return Err(Error::InvalidConfig(
                "Redis admission prefix is invalid".into(),
            ));
        }
        if let Some(limit) = &self.connection_limit {
            limit.validate()?;
        }
        if let Some(limit) = &self.authentication_limit {
            limit.validate()?;
        }
        Ok(())
    }
}

impl Default for RedisAdmissionConfig {
    fn default() -> Self {
        Self {
            prefix: "elura".into(),
            connection_limit: None,
            authentication_limit: None,
        }
    }
}

#[derive(Clone)]
pub struct RedisAdmissionController {
    connection: ConnectionManager,
    config: RedisAdmissionConfig,
}

impl RedisAdmissionController {
    pub async fn connect(url: &str, config: RedisAdmissionConfig) -> Result<Self> {
        config.validate()?;
        let client = redis::Client::open(url).map_err(redis_error)?;
        let connection = client.get_connection_manager().await.map_err(redis_error)?;
        Ok(Self { connection, config })
    }

    pub fn new(connection: ConnectionManager, config: RedisAdmissionConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { connection, config })
    }

    pub async fn set_maintenance(&self, ttl: Duration, reason: &str) -> Result<()> {
        self.set_temporary(self.key("maintenance"), ttl, reason)
            .await
    }

    pub async fn clear_maintenance(&self) -> Result<()> {
        self.delete(self.key("maintenance")).await
    }

    pub async fn ban_ip(&self, ip: IpAddr, ttl: Duration, reason: &str) -> Result<()> {
        self.set_temporary(self.ip_ban_key(ip), ttl, reason).await
    }

    pub async fn unban_ip(&self, ip: IpAddr) -> Result<()> {
        self.delete(self.ip_ban_key(ip)).await
    }

    pub async fn ban_user(&self, identity: &Identity, ttl: Duration, reason: &str) -> Result<()> {
        identity.validate()?;
        self.set_temporary(self.user_ban_key(identity), ttl, reason)
            .await
    }

    pub async fn unban_user(&self, identity: &Identity) -> Result<()> {
        identity.validate()?;
        self.delete(self.user_ban_key(identity)).await
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:admission:{suffix}", self.config.prefix)
    }

    fn ip_ban_key(&self, ip: IpAddr) -> String {
        self.key(&format!("ban:ip:{ip}"))
    }

    fn user_ban_key(&self, identity: &Identity) -> String {
        self.key(&format!(
            "ban:user:{}:{}:{}",
            identity.region_id, identity.realm_id, identity.user_id
        ))
    }

    fn user_rate_key(&self, identity: Option<&Identity>) -> String {
        match identity {
            Some(identity) => self.key(&format!(
                "rate:user:{}:{}:{}",
                identity.region_id, identity.realm_id, identity.user_id
            )),
            None => self.key("rate:user:anonymous"),
        }
    }

    async fn set_temporary(&self, key: String, ttl: Duration, reason: &str) -> Result<()> {
        validate_reason(reason)?;
        let mut connection = self.connection.clone();
        redis::cmd("SET")
            .arg(key)
            .arg(reason)
            .arg("PX")
            .arg(milliseconds(ttl)?)
            .query_async::<()>(&mut connection)
            .await
            .map_err(redis_error)
    }

    async fn delete(&self, key: String) -> Result<()> {
        let mut connection = self.connection.clone();
        redis::cmd("DEL")
            .arg(key)
            .query_async::<usize>(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(())
    }
}

#[async_trait]
impl AdmissionAdmin for RedisAdmissionController {
    async fn ban_ip(&self, ip: IpAddr, ttl: Duration, reason: &str) -> Result<()> {
        RedisAdmissionController::ban_ip(self, ip, ttl, reason).await
    }

    async fn unban_ip(&self, ip: IpAddr) -> Result<()> {
        RedisAdmissionController::unban_ip(self, ip).await
    }

    async fn ban_user(&self, identity: &Identity, ttl: Duration, reason: &str) -> Result<()> {
        RedisAdmissionController::ban_user(self, identity, ttl, reason).await
    }

    async fn unban_user(&self, identity: &Identity) -> Result<()> {
        RedisAdmissionController::unban_user(self, identity).await
    }

    async fn set_maintenance(&self, ttl: Duration, reason: &str) -> Result<()> {
        RedisAdmissionController::set_maintenance(self, ttl, reason).await
    }

    async fn clear_maintenance(&self) -> Result<()> {
        RedisAdmissionController::clear_maintenance(self).await
    }
}

#[async_trait]
impl AdmissionController for RedisAdmissionController {
    async fn admit(&self, request: &AdmissionRequest) -> Result<AdmissionDecision> {
        request.validate()?;
        let identity = request.identity.as_ref();
        let (connection_maximum, connection_window) = limit_args(&self.config.connection_limit)?;
        let (authentication_maximum, authentication_window) =
            limit_args(&self.config.authentication_limit)?;
        let user_ban = identity
            .map(|identity| self.user_ban_key(identity))
            .unwrap_or_else(|| self.key("ban:user:anonymous"));
        let mut connection = self.connection.clone();
        let values: Vec<Vec<u8>> = Script::new(ADMIT)
            .key(self.key("maintenance"))
            .key(self.ip_ban_key(request.remote_ip))
            .key(user_ban)
            .key(self.key(&format!("rate:ip:{}", request.remote_ip)))
            .key(self.user_rate_key(identity))
            .arg(match request.stage {
                AdmissionStage::Connected => "connected",
                AdmissionStage::Authenticated => "authenticated",
            })
            .arg(connection_maximum)
            .arg(connection_window)
            .arg(authentication_maximum)
            .arg(authentication_window)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        decision(values)
    }
}

fn decision(values: Vec<Vec<u8>>) -> Result<AdmissionDecision> {
    if values.len() != 3 {
        return Err(Error::Internal("invalid Redis admission response".into()));
    }
    let status = text(&values[0])?;
    if status == "0" {
        return Ok(AdmissionDecision::Allow);
    }
    let code = match status {
        "1" => "maintenance",
        "2" => "ip_banned",
        "3" => "user_banned",
        "4" => "connection_rate_limited",
        "5" => "authentication_rate_limited",
        _ => return Err(Error::Internal("unknown Redis admission status".into())),
    };
    let reason = text(&values[1])?;
    let retry_millis = text(&values[2])?
        .parse::<u64>()
        .map_err(|error| Error::Internal(format!("invalid admission TTL: {error}")))?;
    let retry_after = (retry_millis > 0).then(|| Duration::from_millis(retry_millis));
    Ok(AdmissionDecision::Deny(AdmissionRejection::new(
        code,
        reason,
        retry_after,
    )?))
}

fn limit_args(limit: &Option<AdmissionLimit>) -> Result<(u32, u64)> {
    match limit {
        Some(limit) => Ok((limit.maximum, milliseconds(limit.window)?)),
        None => Ok((0, 1)),
    }
}

fn validate_reason(reason: &str) -> Result<()> {
    if reason.trim().is_empty() || reason.len() > 256 {
        Err(Error::InvalidConfig(
            "admission reason must contain 1..=256 bytes".into(),
        ))
    } else {
        Ok(())
    }
}

fn milliseconds(duration: Duration) -> Result<u64> {
    if duration.is_zero() {
        return Err(Error::InvalidConfig(
            "admission duration must be positive".into(),
        ));
    }
    u64::try_from(duration.as_millis())
        .map_err(|_| Error::InvalidConfig("admission duration overflow".into()))
}

fn text(value: &[u8]) -> Result<&str> {
    std::str::from_utf8(value).map_err(|error| Error::Internal(error.to_string()))
}

fn redis_error(error: redis::RedisError) -> Error {
    crate::redis::map_redis_error("Redis admission", error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_denials_with_retry_hint() {
        let result = decision(vec![
            b"4".to_vec(),
            b"connection rate exceeded".to_vec(),
            b"250".to_vec(),
        ])
        .unwrap();
        let AdmissionDecision::Deny(rejection) = result else {
            panic!("expected denial");
        };
        assert_eq!(rejection.code(), "connection_rate_limited");
        assert_eq!(rejection.retry_after(), Some(Duration::from_millis(250)));
    }

    #[test]
    fn validates_configuration() {
        assert!(
            RedisAdmissionConfig {
                prefix: String::new(),
                ..RedisAdmissionConfig::default()
            }
            .validate()
            .is_err()
        );
        AdmissionLimit {
            maximum: 10,
            window: Duration::from_secs(1),
        }
        .validate()
        .unwrap();
    }
}
