use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use elura_core::outbox::MemoryOutbox;
use elura_core::outbox::{OutboxEvent, OutboxStore};
use elura_core::{Error, Result};
use elura_runtime::outbox::{
    Dispatcher, DispatcherConfig, IdempotencyStore, MemoryIdempotencyStore,
};

fn dispatcher_config(configure: impl FnOnce(&mut DispatcherConfig)) -> DispatcherConfig {
    let mut config = DispatcherConfig::default();
    configure(&mut config);
    config
}

#[tokio::test]
async fn retries_then_dead_letters_without_losing_payload() {
    let store = Arc::new(MemoryOutbox::new());
    let event = OutboxEvent::new("mail", b"body".to_vec()).unwrap();
    store.append(event).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = Dispatcher::new(
        store.clone(),
        Arc::new({
            let calls = calls.clone();
            move |event: OutboxEvent| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    assert_eq!(event.payload, b"body");
                    Err(Error::Unavailable)
                }
            }
        }),
        dispatcher_config(|config| {
            config.max_attempts = 2;
            config.initial_backoff = Duration::from_millis(1);
            config.max_backoff = Duration::from_millis(1);
        }),
    )
    .unwrap();
    assert_eq!(dispatcher.run_once().await.unwrap(), 1);
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert_eq!(dispatcher.run_once().await.unwrap(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(store.list_dead_letters(10).await.unwrap().len(), 1);
    assert_eq!(dispatcher.stats().dead_lettered, 1);
}

#[tokio::test]
async fn idempotency_skips_duplicate_business_effect() {
    let store = Arc::new(MemoryOutbox::new());
    let event = OutboxEvent::new("reward", vec![1]).unwrap();
    store.append(event.clone()).await.unwrap();
    let idempotency = Arc::new(MemoryIdempotencyStore::default());
    idempotency
        .mark(event.id, SystemTime::now() + Duration::from_secs(60))
        .await
        .unwrap();
    let dispatcher = Dispatcher::new(
        store,
        Arc::new(|_: OutboxEvent| async { panic!("handler must be skipped") }),
        dispatcher_config(|config| {
            config.idempotency = Some(idempotency);
        }),
    )
    .unwrap();
    dispatcher.run_once().await.unwrap();
    assert_eq!(dispatcher.stats().duplicates, 1);
}

#[tokio::test]
async fn renews_lease_while_a_slow_handler_runs() {
    let store = Arc::new(MemoryOutbox::new());
    store
        .append(OutboxEvent::new("slow", vec![1]).unwrap())
        .await
        .unwrap();
    let dispatcher = Arc::new(
        Dispatcher::new(
            store.clone(),
            Arc::new(|_: OutboxEvent| async {
                tokio::time::sleep(Duration::from_millis(80)).await;
                Ok(())
            }),
            dispatcher_config(|config| {
                config.lease = Duration::from_millis(30);
            }),
        )
        .unwrap(),
    );
    let task = tokio::spawn({
        let dispatcher = dispatcher.clone();
        async move { dispatcher.run_once().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        store
            .acquire("other", 1, Duration::from_secs(1))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(task.await.unwrap().unwrap(), 1);
}

#[tokio::test]
async fn shutdown_cancels_an_in_flight_batch() {
    let store = Arc::new(MemoryOutbox::new());
    store
        .append(OutboxEvent::new("stuck", vec![1]).unwrap())
        .await
        .unwrap();
    let dispatcher = Arc::new(
        Dispatcher::new(
            store,
            Arc::new(|_: OutboxEvent| async { std::future::pending::<Result<()>>().await }),
            dispatcher_config(|config| {
                config.processing_timeout = Duration::from_secs(60);
            }),
        )
        .unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn({
        let dispatcher = dispatcher.clone();
        async move { dispatcher.run(shutdown_rx).await }
    });
    tokio::task::yield_now().await;
    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
