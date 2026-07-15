use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use elura_core::push::{PushReceipt, PushRequest, PushTarget, PushTransport};
use elura_core::session::Identity;
use elura_core::{Error, Result};
use uuid::Uuid;

use super::middleware::WorldTransaction;

#[derive(Clone)]
pub struct TransactionHandle {
    pub(crate) inner: Arc<Mutex<Option<Box<dyn WorldTransaction>>>>,
}

impl TransactionHandle {
    pub async fn with<T, R>(&self, operation: impl FnOnce(&mut T) -> Result<R>) -> Result<R>
    where
        T: Any + Send + 'static,
    {
        let mut transaction = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let transaction = transaction.as_mut().ok_or(Error::Unavailable)?;
        let typed = transaction
            .as_any_mut()
            .downcast_mut::<T>()
            .ok_or_else(|| Error::InvalidConfig("unexpected transaction type".into()))?;
        operation(typed)
    }
}

#[derive(Clone)]
pub struct WorldContext {
    pub identity: Identity,
    pub session_id: Uuid,
    pub trace_id: String,
    pub route: u32,
    pub request_id: u64,
    pub shard_id: Option<u32>,
    pub owner_id: Option<String>,
    pub owner_epoch: Option<u64>,
    pub(crate) pusher: Option<Arc<dyn PushTransport>>,
    pub(crate) transaction: Option<TransactionHandle>,
    pub(crate) state: Arc<HashMap<u64, Arc<dyn Any + Send + Sync>>>,
}

static NEXT_CONTEXT_KEY: AtomicU64 = AtomicU64::new(1);

pub struct ContextKey<T> {
    id: u64,
    name: Arc<str>,
    marker: PhantomData<fn() -> T>,
}

impl<T> Clone for ContextKey<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            marker: PhantomData,
        }
    }
}

impl<T> ContextKey<T> {
    pub fn new(name: impl Into<Arc<str>>) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "World context key name is empty".into(),
            ));
        }
        Ok(Self {
            id: NEXT_CONTEXT_KEY.fetch_add(1, Ordering::Relaxed),
            name,
            marker: PhantomData,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl WorldContext {
    pub(crate) fn with_pusher(mut self, pusher: Option<Arc<dyn PushTransport>>) -> Self {
        self.pusher = pusher;
        self
    }

    pub(crate) fn with_transaction(mut self, transaction: TransactionHandle) -> Self {
        self.transaction = Some(transaction);
        self
    }

    pub fn transaction(&self) -> Result<TransactionHandle> {
        self.transaction.clone().ok_or(Error::Unavailable)
    }

    pub fn with_value<T>(mut self, key: &ContextKey<T>, value: T) -> Self
    where
        T: Any + Send + Sync + 'static,
    {
        Arc::make_mut(&mut self.state).insert(key.id, Arc::new(value));
        self
    }

    pub fn value<T>(&self, key: &ContextKey<T>) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        self.state
            .get(&key.id)
            .cloned()
            .and_then(|value| value.downcast::<T>().ok())
    }

    pub async fn push_session(&self, route: u32, payload: Bytes) -> Result<PushReceipt> {
        self.publish(PushTarget::Session(self.session_id), route, 0, payload)
            .await
    }

    pub async fn push_user(&self, route: u32, payload: Bytes) -> Result<PushReceipt> {
        self.publish(PushTarget::User(self.identity.user_id), route, 0, payload)
            .await
    }

    pub async fn push_users(
        &self,
        users: Vec<i64>,
        route: u32,
        payload: Bytes,
    ) -> Result<PushReceipt> {
        self.publish(PushTarget::Users(users), route, 0, payload)
            .await
    }

    pub async fn push_realm(&self, route: u32, payload: Bytes) -> Result<PushReceipt> {
        self.publish(PushTarget::Realm, route, 0, payload).await
    }

    pub async fn push_topic(
        &self,
        topic: impl Into<String>,
        route: u32,
        payload: Bytes,
    ) -> Result<PushReceipt> {
        self.publish(PushTarget::Topic(topic.into()), route, 0, payload)
            .await
    }

    pub async fn join_topic(&self, topic: impl Into<String>) -> Result<PushReceipt> {
        self.publish(
            PushTarget::JoinTopic {
                session_id: self.session_id,
                topic: topic.into(),
            },
            0,
            0,
            Bytes::new(),
        )
        .await
    }

    pub async fn leave_topic(&self, topic: impl Into<String>) -> Result<PushReceipt> {
        self.publish(
            PushTarget::LeaveTopic {
                session_id: self.session_id,
                topic: topic.into(),
            },
            0,
            0,
            Bytes::new(),
        )
        .await
    }

    pub async fn push_sequenced(
        &self,
        target: PushTarget,
        route: u32,
        sequence: u32,
        payload: Bytes,
    ) -> Result<PushReceipt> {
        self.publish(target, route, sequence, payload).await
    }

    async fn publish(
        &self,
        target: PushTarget,
        route: u32,
        sequence: u32,
        payload: Bytes,
    ) -> Result<PushReceipt> {
        let pusher = self.pusher.as_ref().ok_or(Error::Unavailable)?;
        pusher
            .publish(&PushRequest {
                region_id: self.identity.region_id,
                realm_id: self.identity.realm_id,
                target,
                route,
                sequence,
                trace_id: self.trace_id.clone(),
                payload,
            })
            .await
    }
}
