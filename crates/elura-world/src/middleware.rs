use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use elura_core::{Error, Result};
use tokio::sync::Mutex;

use super::context::TransactionHandle;
use super::{WorldContext, WorldHandler};

pub struct Next<'a> {
    pub(crate) middleware: &'a [Arc<dyn WorldMiddleware>],
    pub(crate) handler: Arc<dyn WorldHandler>,
}

impl<'a> Next<'a> {
    pub fn run(
        self,
        context: WorldContext,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + 'a>> {
        Box::pin(async move {
            match self.middleware.split_first() {
                Some((middleware, remaining)) => {
                    middleware
                        .handle(
                            context,
                            payload,
                            Next {
                                middleware: remaining,
                                handler: self.handler,
                            },
                        )
                        .await
                }
                None => self.handler.handle(context, payload).await,
            }
        })
    }
}

#[async_trait]
pub trait WorldMiddleware: Send + Sync + 'static {
    async fn handle(&self, context: WorldContext, payload: Bytes, next: Next<'_>) -> Result<Bytes>;
}

#[derive(Default)]
pub struct LoggingMiddleware;

#[async_trait]
impl WorldMiddleware for LoggingMiddleware {
    async fn handle(&self, context: WorldContext, payload: Bytes, next: Next<'_>) -> Result<Bytes> {
        let started = std::time::Instant::now();
        let region_id = context.identity.region_id;
        let realm_id = context.identity.realm_id;
        let user_id = context.identity.user_id;
        let session_id = context.session_id;
        let route = context.route;
        let request_id = context.request_id;
        let trace_id = context.trace_id.clone();
        let result = next.run(context, payload).await;
        tracing::debug!(
            region_id,
            realm_id,
            user_id,
            %session_id,
            route,
            request_id,
            %trace_id,
            duration_micros = started.elapsed().as_micros() as u64,
            outcome = if result.is_ok() { "ok" } else { "error" },
            "World command handled"
        );
        result
    }
}

#[async_trait]
pub trait WorldTransaction: Any + Send {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    async fn commit(&mut self) -> Result<()>;
    async fn rollback(&mut self) -> Result<()>;
}

#[async_trait]
pub trait TransactionFactory: Send + Sync + 'static {
    async fn begin(&self, context: &WorldContext) -> Result<Box<dyn WorldTransaction>>;
}

pub struct UnitOfWorkMiddleware {
    factory: Arc<dyn TransactionFactory>,
}

struct SharedRollbackOnDrop {
    transaction: Arc<Mutex<Option<Box<dyn WorldTransaction>>>>,
    armed: bool,
}

impl Drop for SharedRollbackOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let transaction = self.transaction.clone();
        tokio::spawn(async move {
            let transaction = transaction.lock().await.take();
            rollback(transaction).await;
        });
    }
}

struct RollbackOnDrop {
    transaction: Option<Box<dyn WorldTransaction>>,
}

impl Drop for RollbackOnDrop {
    fn drop(&mut self) {
        spawn_rollback(self.transaction.take());
    }
}

fn spawn_rollback(transaction: Option<Box<dyn WorldTransaction>>) {
    tokio::spawn(rollback(transaction));
}

async fn rollback(transaction: Option<Box<dyn WorldTransaction>>) {
    if let Some(mut transaction) = transaction
        && let Err(error) = transaction.rollback().await
    {
        tracing::error!(%error, "rollback cancelled World transaction");
    }
}

impl UnitOfWorkMiddleware {
    pub fn new(factory: Arc<dyn TransactionFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl WorldMiddleware for UnitOfWorkMiddleware {
    async fn handle(&self, context: WorldContext, payload: Bytes, next: Next<'_>) -> Result<Bytes> {
        let transaction = self.factory.begin(&context).await?;
        let handle = TransactionHandle {
            inner: Arc::new(Mutex::new(Some(transaction))),
        };
        let mut shared_rollback = SharedRollbackOnDrop {
            transaction: handle.inner.clone(),
            armed: true,
        };
        let result = next
            .run(context.with_transaction(handle.clone()), payload)
            .await;
        let transaction =
            handle.inner.lock().await.take().ok_or_else(|| {
                Error::Internal("transaction was removed before completion".into())
            })?;
        shared_rollback.armed = false;
        let mut transaction = RollbackOnDrop {
            transaction: Some(transaction),
        };
        match result {
            Ok(payload) => {
                transaction
                    .transaction
                    .as_mut()
                    .ok_or_else(|| Error::Internal("transaction missing before commit".into()))?
                    .commit()
                    .await?;
                transaction.transaction.take();
                Ok(payload)
            }
            Err(error) => {
                transaction
                    .transaction
                    .as_mut()
                    .ok_or_else(|| Error::Internal("transaction missing before rollback".into()))?
                    .rollback()
                    .await?;
                transaction.transaction.take();
                Err(error)
            }
        }
    }
}
