use crate::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PushTarget {
    Session(Uuid),
    User(i64),
    Users(Vec<i64>),
    Realm,
    Topic(String),
    JoinTopic { session_id: Uuid, topic: String },
    LeaveTopic { session_id: Uuid, topic: String },
    Disconnect(Uuid),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushRequest {
    pub region_id: u32,
    pub realm_id: u32,
    pub target: PushTarget,
    pub route: u32,
    #[serde(default)]
    pub sequence: u32,
    #[serde(default)]
    pub trace_id: String,
    pub payload: Bytes,
}
impl PushRequest {
    pub fn validate(&self) -> Result<()> {
        let control = matches!(
            self.target,
            PushTarget::JoinTopic { .. }
                | PushTarget::LeaveTopic { .. }
                | PushTarget::Disconnect(_)
        );
        if self.region_id == 0
            || self.realm_id == 0
            || (!control && self.route == 0)
            || self.trace_id.len() > 128
            || self.payload.len() > 1024 * 1024
            || !valid_target(&self.target)
        {
            return Err(Error::InvalidConfig("invalid push".into()));
        }
        Ok(())
    }
}

fn valid_target(target: &PushTarget) -> bool {
    match target {
        PushTarget::Session(id) | PushTarget::Disconnect(id) => !id.is_nil(),
        PushTarget::User(_) | PushTarget::Realm => true,
        PushTarget::Users(users) => !users.is_empty() && users.len() <= 1024,
        PushTarget::Topic(topic) => valid_topic(topic),
        PushTarget::JoinTopic { session_id, topic }
        | PushTarget::LeaveTopic { session_id, topic } => {
            !session_id.is_nil() && valid_topic(topic)
        }
    }
}

fn valid_topic(topic: &str) -> bool {
    !topic.is_empty()
        && topic.len() <= 128
        && topic
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushReceipt {
    pub accepted: bool,
    pub delivered: u32,
    pub sequence: u32,
    pub trace_id: String,
}

impl PushReceipt {
    pub fn accepted(request: &PushRequest, delivered: u32) -> Self {
        Self {
            accepted: true,
            delivered,
            sequence: request.sequence,
            trace_id: request.trace_id.clone(),
        }
    }
}
#[async_trait]
pub trait PushHandler: Send + Sync {
    async fn deliver(&self, request: PushRequest) -> Result<PushReceipt>;
}

/// Resolves the Gateway instances that currently own a Push target.
///
/// Keeping target resolution separate from the message transport allows an
/// application to combine, for example, a SQL-backed online directory with a
/// broker-backed Push transport.
#[async_trait]
pub trait PushTargetResolver: Send + Sync {
    async fn resolve_gateways(&self, request: &PushRequest) -> Result<Vec<String>>;
}

#[async_trait]
pub trait PushTransport: Send + Sync {
    /// Accepts a Push for transport. Implementations that provide at-least-once
    /// delivery may deliver the same request more than once; consumers must use
    /// the request sequence/trace metadata when application-level deduplication
    /// is required.
    async fn publish(&self, request: &PushRequest) -> Result<PushReceipt>;

    /// Runs the subscriber until shutdown is requested.
    ///
    /// Returning `Ok(())` before shutdown is an unexpected transport stop. A
    /// transport should handle its own transient reconnects and return `Err`
    /// only when it cannot continue serving.
    async fn subscribe(
        &self,
        handler: Arc<dyn PushHandler>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()>;
}
