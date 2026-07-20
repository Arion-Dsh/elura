use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use elura_core::{Error, Result};
use elura_world::scene::{Scene, SceneCommand, SceneError, SceneRuntime, SceneRuntimeConfig};
use tokio::sync::Notify;

struct CounterScene {
    id: u64,
    value: u64,
    fail_start: bool,
    started: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
    ticks: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl CounterScene {
    fn new(id: u64) -> Self {
        Self {
            id,
            value: 0,
            fail_start: false,
            started: Arc::new(AtomicUsize::new(0)),
            stopped: Arc::new(AtomicUsize::new(0)),
            ticks: Arc::new(AtomicUsize::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl Scene for CounterScene {
    type Id = u64;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    async fn start(&mut self) -> Result<()> {
        if self.fail_start {
            return Err(Error::business("START_FAILED", "scene start failed"));
        }
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn tick(&mut self, elapsed: Duration) -> Result<()> {
        assert!(!elapsed.is_zero());
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        self.ticks.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct Add {
    amount: u64,
    delay: Duration,
    entered: Option<Arc<Notify>>,
}

#[async_trait]
impl SceneCommand<CounterScene> for Add {
    type Output = u64;

    async fn execute(self, scene: &mut CounterScene) -> Result<Self::Output> {
        let active = scene.active.fetch_add(1, Ordering::SeqCst) + 1;
        scene.max_active.fetch_max(active, Ordering::SeqCst);
        if let Some(entered) = self.entered {
            entered.notify_one();
        }
        tokio::time::sleep(self.delay).await;
        scene.value += self.amount;
        scene.active.fetch_sub(1, Ordering::SeqCst);
        Ok(scene.value)
    }
}

struct Read;

#[async_trait]
impl SceneCommand<CounterScene> for Read {
    type Output = u64;

    async fn execute(self, scene: &mut CounterScene) -> Result<Self::Output> {
        Ok(scene.value)
    }
}

struct Panic;

#[async_trait]
impl SceneCommand<CounterScene> for Panic {
    type Output = ();

    async fn execute(self, _scene: &mut CounterScene) -> Result<Self::Output> {
        panic!("broken scene invariant");
    }
}

struct Block {
    entered: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

#[async_trait]
impl SceneCommand<CounterScene> for Block {
    type Output = ();

    async fn execute(self, _scene: &mut CounterScene) -> Result<Self::Output> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        Ok(())
    }
}

fn config() -> SceneRuntimeConfig {
    let mut config = SceneRuntimeConfig::default();
    config.mailbox_capacity = 8;
    config.handler_timeout = Duration::from_millis(250);
    config.shutdown_timeout = Duration::from_millis(250);
    config
}

#[test]
fn rejects_invalid_runtime_limits() {
    let mut config = SceneRuntimeConfig::default();
    config.mailbox_capacity = 0;
    let result = SceneRuntime::<CounterScene>::new(config);
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
}

#[test]
fn maps_scene_failures_into_world_handler_errors() {
    let not_found: Error = SceneError::NotFound.into();
    assert!(matches!(
        not_found,
        Error::Business { code, .. } if code == "SCENE_NOT_FOUND"
    ));
    assert!(matches!(
        Error::from(SceneError::QueueFull),
        Error::QueueFull
    ));
}

#[tokio::test]
async fn starts_serializes_and_stops_one_scene() {
    let runtime = SceneRuntime::new(config()).unwrap();
    let scene = CounterScene::new(7);
    let started = scene.started.clone();
    let stopped = scene.stopped.clone();
    let max_active = scene.max_active.clone();
    runtime.spawn(scene).await.unwrap();
    assert_eq!(started.load(Ordering::SeqCst), 1);

    let first_entered = Arc::new(Notify::new());
    let first = {
        let runtime = runtime.clone();
        let entered = first_entered.clone();
        tokio::spawn(async move {
            runtime
                .call(
                    &7,
                    Add {
                        amount: 1,
                        delay: Duration::from_millis(40),
                        entered: Some(entered),
                    },
                )
                .await
        })
    };
    first_entered.notified().await;
    let second = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .call(
                    &7,
                    Add {
                        amount: 1,
                        delay: Duration::ZERO,
                        entered: None,
                    },
                )
                .await
        })
    };

    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(second.await.unwrap().unwrap(), 2);
    assert_eq!(max_active.load(Ordering::SeqCst), 1);
    assert!(runtime.contains(&7));

    runtime.stop(&7).await.unwrap();
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
    assert!(!runtime.contains(&7));
}

#[tokio::test]
async fn different_scenes_execute_in_parallel() {
    let runtime = SceneRuntime::new(config()).unwrap();
    runtime.spawn(CounterScene::new(1)).await.unwrap();
    runtime.spawn(CounterScene::new(2)).await.unwrap();
    let entered = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());

    let first = {
        let runtime = runtime.clone();
        let entered = entered.clone();
        let release = release.clone();
        tokio::spawn(async move { runtime.call(&1, Block { entered, release }).await })
    };
    let second = {
        let runtime = runtime.clone();
        let entered = entered.clone();
        let release = release.clone();
        tokio::spawn(async move { runtime.call(&2, Block { entered, release }).await })
    };

    tokio::time::timeout(Duration::from_millis(200), async {
        while entered.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    release.notify_waiters();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_commands_when_a_scene_mailbox_is_full() {
    let mut runtime_config = config();
    runtime_config.mailbox_capacity = 1;
    let runtime = SceneRuntime::new(runtime_config).unwrap();
    runtime.spawn(CounterScene::new(1)).await.unwrap();
    let entered = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());

    let running = {
        let runtime = runtime.clone();
        let entered = entered.clone();
        let release = release.clone();
        tokio::spawn(async move { runtime.call(&1, Block { entered, release }).await })
    };
    while entered.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }

    let queued = runtime.call(&1, Read);
    tokio::pin!(queued);
    tokio::select! {
        biased;
        result = &mut queued => panic!("queued command completed unexpectedly: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    assert!(matches!(
        runtime.call(&1, Read).await,
        Err(SceneError::QueueFull)
    ));

    release.notify_waiters();
    running.await.unwrap().unwrap();
    assert_eq!(queued.await.unwrap(), 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn drives_ticks_without_overlapping_commands() {
    let mut runtime_config = config();
    runtime_config.tick_interval = Some(Duration::from_millis(10));
    let runtime = SceneRuntime::new(runtime_config).unwrap();
    let scene = CounterScene::new(1);
    let ticks = scene.ticks.clone();
    let max_active = scene.max_active.clone();
    runtime.spawn(scene).await.unwrap();

    runtime
        .call(
            &1,
            Add {
                amount: 1,
                delay: Duration::from_millis(30),
                entered: None,
            },
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_millis(250), async {
        while ticks.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(max_active.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancels_timed_out_command_and_keeps_scene_available() {
    let mut runtime_config = config();
    runtime_config.handler_timeout = Duration::from_millis(15);
    let runtime = SceneRuntime::new(runtime_config).unwrap();
    runtime.spawn(CounterScene::new(1)).await.unwrap();

    let result = runtime
        .call(
            &1,
            Add {
                amount: 10,
                delay: Duration::from_millis(100),
                entered: None,
            },
        )
        .await;
    assert!(matches!(result, Err(SceneError::Timeout)));
    assert_eq!(runtime.call(&1, Read).await.unwrap(), 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_or_panicked_scenes_can_be_replaced() {
    let runtime = SceneRuntime::new(config()).unwrap();
    let mut failed = CounterScene::new(1);
    failed.fail_start = true;
    assert!(matches!(
        runtime.spawn(failed).await,
        Err(SceneError::Handler(Error::Business { .. }))
    ));
    runtime.spawn(CounterScene::new(1)).await.unwrap();

    assert!(matches!(
        runtime.call(&1, Panic).await,
        Err(SceneError::Panicked)
    ));
    tokio::time::timeout(Duration::from_millis(200), async {
        while runtime.contains(&1) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    runtime.spawn(CounterScene::new(1)).await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_stops_scenes_and_rejects_new_work() {
    let runtime = SceneRuntime::new(config()).unwrap();
    let first = CounterScene::new(1);
    let first_stopped = first.stopped.clone();
    let second = CounterScene::new(2);
    let second_stopped = second.stopped.clone();
    runtime.spawn(first).await.unwrap();
    runtime.spawn(second).await.unwrap();

    runtime.shutdown().await.unwrap();
    assert_eq!(first_stopped.load(Ordering::SeqCst), 1);
    assert_eq!(second_stopped.load(Ordering::SeqCst), 1);
    assert!(matches!(
        runtime.spawn(CounterScene::new(3)).await,
        Err(SceneError::RuntimeClosed)
    ));
    assert!(matches!(
        runtime.call(&1, Read).await,
        Err(SceneError::RuntimeClosed)
    ));
}
