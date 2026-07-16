use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use elura_core::protocol::FIRST_APPLICATION_ROUTE;
use elura_core::push::{PushReceipt, PushRequest, PushTarget, PushTransport};
use elura_core::session::Identity;
use elura_core::{Error, Result};
use prost::Message;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

use super::Event;
use super::middleware::WorldTransaction;

#[derive(Clone)]
pub(crate) struct TransactionHandle {
    pub(crate) inner: Arc<Mutex<Option<Box<dyn WorldTransaction>>>>,
}

/// Typed access to the current unit-of-work transaction.
pub struct TransactionGuard<'a, T> {
    inner: MutexGuard<'a, Option<Box<dyn WorldTransaction>>>,
    marker: PhantomData<T>,
}

impl TransactionHandle {
    async fn lock<T>(&self) -> Result<TransactionGuard<'_, T>>
    where
        T: Any + Send + 'static,
    {
        let mut inner = self.inner.lock().await;
        inner
            .as_mut()
            .ok_or(Error::Unavailable)?
            .as_any_mut()
            .downcast_mut::<T>()
            .ok_or_else(|| Error::InvalidConfig("unexpected transaction type".into()))?;
        Ok(TransactionGuard {
            inner,
            marker: PhantomData,
        })
    }
}

impl<T> TransactionGuard<'_, T>
where
    T: Any + Send + 'static,
{
    /// Returns the application transaction while retaining the unit-of-work lock.
    ///
    /// The guard may be held across `.await`, allowing asynchronous database
    /// operations to participate in the surrounding unit of work.
    pub fn get_mut(&mut self) -> &mut T {
        self.inner
            .as_mut()
            .expect("transaction checked when guard was created")
            .as_any_mut()
            .downcast_mut::<T>()
            .expect("transaction type checked when guard was created")
    }
}

impl<T> AsMut<T> for TransactionGuard<'_, T>
where
    T: Any + Send + 'static,
{
    fn as_mut(&mut self) -> &mut T {
        self.get_mut()
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

    pub async fn transaction<T>(&self) -> Result<TransactionGuard<'_, T>>
    where
        T: Any + Send + 'static,
    {
        self.transaction
            .as_ref()
            .ok_or(Error::Unavailable)?
            .lock::<T>()
            .await
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

    pub async fn push_session<E: Event>(
        &self,
        _event: E,
        message: &E::Message,
    ) -> Result<PushReceipt> {
        self.publish_event::<E>(PushTarget::Session(self.session_id), 0, message)
            .await
    }

    pub async fn push_user<E: Event>(
        &self,
        _event: E,
        message: &E::Message,
    ) -> Result<PushReceipt> {
        self.publish_event::<E>(PushTarget::User(self.identity.user_id), 0, message)
            .await
    }

    pub async fn push_users<E: Event>(
        &self,
        users: Vec<i64>,
        _event: E,
        message: &E::Message,
    ) -> Result<PushReceipt> {
        self.publish_event::<E>(PushTarget::Users(users), 0, message)
            .await
    }

    pub async fn push_realm<E: Event>(
        &self,
        _event: E,
        message: &E::Message,
    ) -> Result<PushReceipt> {
        self.publish_event::<E>(PushTarget::Realm, 0, message).await
    }

    pub async fn push_topic<E: Event>(
        &self,
        topic: impl Into<String>,
        _event: E,
        message: &E::Message,
    ) -> Result<PushReceipt> {
        self.publish_event::<E>(PushTarget::Topic(topic.into()), 0, message)
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

    pub async fn push_sequenced<E: Event>(
        &self,
        target: PushTarget,
        _event: E,
        sequence: u32,
        message: &E::Message,
    ) -> Result<PushReceipt> {
        self.publish_event::<E>(target, sequence, message).await
    }

    async fn publish_event<E: Event>(
        &self,
        target: PushTarget,
        sequence: u32,
        message: &E::Message,
    ) -> Result<PushReceipt> {
        if E::ID < FIRST_APPLICATION_ROUTE {
            return Err(Error::InvalidConfig(format!(
                "event route {} is reserved",
                E::ID
            )));
        }
        self.publish(
            target,
            E::ID,
            sequence,
            Bytes::from(message.encode_to_vec()),
        )
        .await
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use elura_core::push::{PushHandler, PushTarget};

    use super::*;

    #[derive(Clone, PartialEq, Message)]
    struct PlayerUpdated {
        #[prost(uint64, tag = "1")]
        version: u64,
    }

    struct PlayerUpdatedEvent;

    impl Event for PlayerUpdatedEvent {
        const ID: u32 = 120;
        type Message = PlayerUpdated;
    }

    struct ReservedEvent;

    impl Event for ReservedEvent {
        const ID: u32 = 4;
        type Message = PlayerUpdated;
    }

    #[derive(Default)]
    struct CapturePush {
        request: StdMutex<Option<PushRequest>>,
    }

    #[async_trait::async_trait]
    impl PushTransport for CapturePush {
        async fn publish(&self, request: &PushRequest) -> Result<PushReceipt> {
            *self.request.lock().unwrap() = Some(request.clone());
            Ok(PushReceipt::accepted(request, 1))
        }

        async fn subscribe(
            &self,
            _handler: Arc<dyn PushHandler>,
            mut shutdown: tokio::sync::watch::Receiver<bool>,
        ) -> Result<()> {
            let _ = shutdown.changed().await;
            Ok(())
        }
    }

    fn context(pusher: Arc<dyn PushTransport>) -> WorldContext {
        WorldContext {
            identity: Identity {
                account_id: 1,
                user_id: 2,
                region_id: 3,
                realm_id: 4,
                generation: 1,
            },
            session_id: Uuid::new_v4(),
            trace_id: "typed-push-test".into(),
            route: 100,
            request_id: 1,
            shard_id: None,
            owner_id: None,
            owner_epoch: None,
            pusher: Some(pusher),
            transaction: None,
            state: Arc::new(HashMap::new()),
        }
    }

    #[tokio::test]
    async fn typed_push_uses_the_event_route_and_protobuf_message() {
        let pusher = Arc::new(CapturePush::default());
        let context = context(pusher.clone());
        context
            .push_session(PlayerUpdatedEvent, &PlayerUpdated { version: 7 })
            .await
            .unwrap();

        let request = pusher.request.lock().unwrap().clone().unwrap();
        assert_eq!(request.route, PlayerUpdatedEvent::ID);
        assert!(matches!(request.target, PushTarget::Session(id) if id == context.session_id));
        assert_eq!(PlayerUpdated::decode(request.payload).unwrap().version, 7);
    }

    #[tokio::test]
    async fn typed_push_rejects_reserved_framework_routes() {
        let context = context(Arc::new(CapturePush::default()));
        assert!(matches!(
            context
                .push_user(ReservedEvent, &PlayerUpdated { version: 7 })
                .await,
            Err(Error::InvalidConfig(_))
        ));
    }
}
