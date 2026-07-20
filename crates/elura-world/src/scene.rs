//! Optional actor-style execution for stateful game scenes.
//!
//! A [`SceneRuntime`] owns one Tokio task and one bounded mailbox per scene. Commands and ticks for
//! the same scene never overlap, while different scenes remain independently schedulable. The
//! runtime deliberately leaves scene placement, persistence, recovery and game rules to the
//! application.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use elura_core::{Error, Result};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{MissedTickBehavior, interval_at, timeout};
use tracing::{error, warn};

/// Configuration shared by every scene managed by one [`SceneRuntime`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct SceneRuntimeConfig {
    /// Maximum number of commands waiting in one scene mailbox.
    pub mailbox_capacity: usize,
    /// Maximum duration of scene startup, one command, or one tick.
    pub handler_timeout: Duration,
    /// Optional interval for scene ticks. `None` disables automatic ticks.
    pub tick_interval: Option<Duration>,
    /// Maximum duration of a scene's shutdown hook.
    pub shutdown_timeout: Duration,
}

impl Default for SceneRuntimeConfig {
    fn default() -> Self {
        Self {
            mailbox_capacity: 64,
            handler_timeout: Duration::from_secs(5),
            tick_interval: None,
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

impl SceneRuntimeConfig {
    /// Validates runtime limits before any scene tasks are started.
    pub fn validate(&self) -> Result<()> {
        if self.mailbox_capacity == 0
            || self.handler_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
            || self
                .tick_interval
                .is_some_and(|interval| interval.is_zero())
        {
            return Err(Error::InvalidConfig(
                "scene runtime limits must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// Stateful scene lifecycle hosted by [`SceneRuntime`].
///
/// The runtime calls every method for one scene serially. A method may use asynchronous APIs, but
/// it must complete before that scene can process another command or tick.
#[async_trait]
pub trait Scene: Send + 'static {
    /// Stable identifier used to address the scene within one runtime.
    type Id: Clone + Eq + Hash + Send + Sync + 'static;

    /// Returns this scene's identifier.
    fn id(&self) -> &Self::Id;

    /// Initializes application-owned state before commands are accepted.
    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    /// Advances time-based scene behavior.
    ///
    /// This is called only when [`SceneRuntimeConfig::tick_interval`] is configured. `elapsed`
    /// measures wall-clock time since the previous tick was dispatched rather than assuming a
    /// perfectly fixed scheduler cadence.
    async fn tick(&mut self, _elapsed: Duration) -> Result<()> {
        Ok(())
    }

    /// Flushes or releases application-owned state during a graceful stop.
    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A strongly typed operation executed against one mutable scene.
///
/// Different command types may return different output types. The runtime erases them only while
/// they are in the mailbox and restores the concrete output type before returning from
/// [`SceneRuntime::call`].
#[async_trait]
pub trait SceneCommand<S>: Send + 'static
where
    S: Scene,
{
    /// Value returned to the caller after successful execution.
    type Output: Send + 'static;

    /// Applies this command to the scene.
    async fn execute(self, scene: &mut S) -> Result<Self::Output>;
}

/// Result returned by scene runtime operations.
pub type SceneResult<T> = std::result::Result<T, SceneError>;

/// Failures produced by scene lookup, scheduling, lifecycle or command execution.
#[derive(Debug)]
#[non_exhaustive]
pub enum SceneError {
    /// A live scene already uses the requested identifier.
    AlreadyExists,
    /// No live scene uses the requested identifier.
    NotFound,
    /// The runtime has begun shutting down and accepts no new work.
    RuntimeClosed,
    /// The bounded mailbox has no remaining capacity.
    QueueFull,
    /// The scene task terminated before completing the operation.
    Unavailable,
    /// A lifecycle hook, command, or tick exceeded its configured deadline.
    Timeout,
    /// Application scene code returned an Elura error.
    Handler(Error),
    /// Application scene code panicked. The affected scene is terminated.
    Panicked,
    /// A runtime invariant was violated.
    Internal(&'static str),
}

impl fmt::Display for SceneError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExists => formatter.write_str("scene already exists"),
            Self::NotFound => formatter.write_str("scene was not found"),
            Self::RuntimeClosed => formatter.write_str("scene runtime is closed"),
            Self::QueueFull => formatter.write_str("scene command queue is full"),
            Self::Unavailable => formatter.write_str("scene is unavailable"),
            Self::Timeout => formatter.write_str("scene operation timed out"),
            Self::Handler(error) => write!(formatter, "scene handler failed: {error}"),
            Self::Panicked => formatter.write_str("scene handler panicked"),
            Self::Internal(message) => {
                write!(formatter, "scene runtime invariant failed: {message}")
            }
        }
    }
}

impl std::error::Error for SceneError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Handler(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SceneError> for Error {
    fn from(error: SceneError) -> Self {
        match error {
            SceneError::AlreadyExists => {
                Self::business("SCENE_ALREADY_EXISTS", "scene already exists")
            }
            SceneError::NotFound => Self::business("SCENE_NOT_FOUND", "scene was not found"),
            SceneError::RuntimeClosed | SceneError::Unavailable => Self::Unavailable,
            SceneError::QueueFull => Self::QueueFull,
            SceneError::Timeout => Self::Timeout,
            SceneError::Handler(error) => error,
            SceneError::Panicked => Self::Internal("scene handler panicked".into()),
            SceneError::Internal(message) => Self::Internal(message.into()),
        }
    }
}

type ErasedOutput = Box<dyn Any + Send>;

#[async_trait]
trait ErasedSceneCommand<S>: Send
where
    S: Scene,
{
    async fn execute(self: Box<Self>, scene: &mut S) -> Result<ErasedOutput>;
}

#[async_trait]
impl<S, C> ErasedSceneCommand<S> for C
where
    S: Scene,
    C: SceneCommand<S>,
{
    async fn execute(self: Box<Self>, scene: &mut S) -> Result<ErasedOutput> {
        let output = SceneCommand::execute(*self, scene).await?;
        Ok(Box::new(output))
    }
}

struct CommandEnvelope<S>
where
    S: Scene,
{
    command: Box<dyn ErasedSceneCommand<S>>,
    response: oneshot::Sender<SceneResult<ErasedOutput>>,
}

enum Control {
    Stop {
        response: oneshot::Sender<SceneResult<()>>,
    },
}

struct SceneActorHandle<S>
where
    S: Scene,
{
    commands: mpsc::Sender<CommandEnvelope<S>>,
    control: mpsc::UnboundedSender<Control>,
    done: Arc<AtomicBool>,
}

impl<S> Clone for SceneActorHandle<S>
where
    S: Scene,
{
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            control: self.control.clone(),
            done: self.done.clone(),
        }
    }
}

struct SceneRuntimeInner<S>
where
    S: Scene,
{
    config: SceneRuntimeConfig,
    scenes: Mutex<HashMap<S::Id, SceneActorHandle<S>>>,
    closing: AtomicBool,
}

/// Actor-style scene manager with one bounded mailbox per live scene.
///
/// Clones share the same scene registry. Dropping the last clone closes all mailboxes; call
/// [`Self::shutdown`] when lifecycle hooks must be awaited.
pub struct SceneRuntime<S>
where
    S: Scene,
{
    inner: Arc<SceneRuntimeInner<S>>,
}

impl<S> Clone for SceneRuntime<S>
where
    S: Scene,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> SceneRuntime<S>
where
    S: Scene,
{
    /// Creates an empty runtime. Scene tasks are created lazily by [`Self::spawn`].
    pub fn new(config: SceneRuntimeConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(SceneRuntimeInner {
                config,
                scenes: Mutex::new(HashMap::new()),
                closing: AtomicBool::new(false),
            }),
        })
    }

    /// Starts and registers one scene.
    ///
    /// The scene becomes addressable before its asynchronous `start` hook runs, but queued commands
    /// are not executed until startup succeeds. Failed scene tasks can be replaced by spawning the
    /// same identifier again.
    pub async fn spawn(&self, scene: S) -> SceneResult<()> {
        if self.inner.closing.load(Ordering::Acquire) {
            return Err(SceneError::RuntimeClosed);
        }

        let id = scene.id().clone();
        let (command_tx, command_rx) = mpsc::channel(self.inner.config.mailbox_capacity);
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let done = Arc::new(AtomicBool::new(false));
        let handle = SceneActorHandle {
            commands: command_tx,
            control: control_tx,
            done: done.clone(),
        };

        {
            let mut scenes = lock_scenes(&self.inner.scenes);
            if self.inner.closing.load(Ordering::Acquire) {
                return Err(SceneError::RuntimeClosed);
            }
            if let Some(current) = scenes.get(&id)
                && !current.done.load(Ordering::Acquire)
            {
                return Err(SceneError::AlreadyExists);
            }
            scenes.insert(id.clone(), handle.clone());
        }

        let (started_tx, started_rx) = oneshot::channel();
        let config = self.inner.config.clone();
        tokio::spawn(run_scene(
            scene, config, command_rx, control_rx, started_tx, done,
        ));

        match started_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.remove_if_current(&id, &handle);
                Err(error)
            }
            Err(_) => {
                self.remove_if_current(&id, &handle);
                Err(SceneError::Unavailable)
            }
        }
    }

    /// Executes a typed command in the target scene's serial mailbox.
    pub async fn call<C>(&self, id: &S::Id, command: C) -> SceneResult<C::Output>
    where
        C: SceneCommand<S>,
    {
        let handle = self.handle(id)?;
        let (response_tx, response_rx) = oneshot::channel();
        let envelope = CommandEnvelope {
            command: Box::new(command),
            response: response_tx,
        };
        match handle.commands.try_send(envelope) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => return Err(SceneError::QueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(SceneError::Unavailable),
        }

        let output = response_rx.await.map_err(|_| SceneError::Unavailable)??;
        output
            .downcast::<C::Output>()
            .map(|output| *output)
            .map_err(|_| SceneError::Internal("command output type changed in transit"))
    }

    /// Gracefully stops and unregisters one scene.
    pub async fn stop(&self, id: &S::Id) -> SceneResult<()> {
        let handle = {
            let mut scenes = lock_scenes(&self.inner.scenes);
            scenes.remove(id).ok_or(SceneError::NotFound)?
        };
        if handle.done.load(Ordering::Acquire) {
            return Err(SceneError::NotFound);
        }
        stop_handle(&self.inner.config, handle).await
    }

    /// Gracefully stops every registered scene and permanently closes this runtime.
    ///
    /// All scenes are signalled before the method waits for their shutdown hooks, so an unrelated
    /// slow scene does not delay delivery of the stop signal to the others.
    pub async fn shutdown(&self) -> SceneResult<()> {
        if self.inner.closing.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let handles = {
            let mut scenes = lock_scenes(&self.inner.scenes);
            std::mem::take(&mut *scenes)
                .into_values()
                .filter(|handle| !handle.done.load(Ordering::Acquire))
                .collect::<Vec<_>>()
        };

        let mut responses = Vec::with_capacity(handles.len());
        for handle in handles {
            let (response_tx, response_rx) = oneshot::channel();
            if handle
                .control
                .send(Control::Stop {
                    response: response_tx,
                })
                .is_ok()
            {
                responses.push(response_rx);
            }
        }

        let wait = async {
            let mut first_error = None;
            for response in responses {
                let result = response.await.unwrap_or(Err(SceneError::Unavailable));
                if first_error.is_none()
                    && let Err(error) = result
                {
                    first_error = Some(error);
                }
            }
            first_error.map_or(Ok(()), Err)
        };
        timeout(
            self.inner
                .config
                .handler_timeout
                .saturating_add(self.inner.config.shutdown_timeout),
            wait,
        )
        .await
        .map_err(|_| SceneError::Timeout)?
    }

    /// Returns true when a live scene is currently registered.
    pub fn contains(&self, id: &S::Id) -> bool {
        lock_scenes(&self.inner.scenes)
            .get(id)
            .is_some_and(|handle| !handle.done.load(Ordering::Acquire))
    }

    /// Returns the number of live registered scenes.
    pub fn len(&self) -> usize {
        lock_scenes(&self.inner.scenes)
            .values()
            .filter(|handle| !handle.done.load(Ordering::Acquire))
            .count()
    }

    /// Returns true when no live scene is registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn handle(&self, id: &S::Id) -> SceneResult<SceneActorHandle<S>> {
        if self.inner.closing.load(Ordering::Acquire) {
            return Err(SceneError::RuntimeClosed);
        }
        let mut scenes = lock_scenes(&self.inner.scenes);
        let handle = scenes.get(id).cloned().ok_or(SceneError::NotFound)?;
        if handle.done.load(Ordering::Acquire) {
            scenes.remove(id);
            return Err(SceneError::Unavailable);
        }
        Ok(handle)
    }

    fn remove_if_current(&self, id: &S::Id, expected: &SceneActorHandle<S>) {
        let mut scenes = lock_scenes(&self.inner.scenes);
        let matches = scenes
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(&current.done, &expected.done));
        if matches {
            scenes.remove(id);
        }
    }
}

fn lock_scenes<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

async fn stop_handle<S>(config: &SceneRuntimeConfig, handle: SceneActorHandle<S>) -> SceneResult<()>
where
    S: Scene,
{
    let (response_tx, response_rx) = oneshot::channel();
    handle
        .control
        .send(Control::Stop {
            response: response_tx,
        })
        .map_err(|_| SceneError::Unavailable)?;
    timeout(
        config
            .handler_timeout
            .saturating_add(config.shutdown_timeout),
        response_rx,
    )
    .await
    .map_err(|_| SceneError::Timeout)?
    .map_err(|_| SceneError::Unavailable)?
}

enum Guarded<T> {
    Completed(Result<T>),
    TimedOut,
    Panicked,
}

async fn guarded<F, T>(duration: Duration, future: F) -> Guarded<T>
where
    F: std::future::Future<Output = Result<T>> + Send,
{
    match timeout(duration, AssertUnwindSafe(future).catch_unwind()).await {
        Ok(Ok(result)) => Guarded::Completed(result),
        Ok(Err(_)) => Guarded::Panicked,
        Err(_) => Guarded::TimedOut,
    }
}

async fn run_scene<S>(
    mut scene: S,
    config: SceneRuntimeConfig,
    mut commands: mpsc::Receiver<CommandEnvelope<S>>,
    mut control: mpsc::UnboundedReceiver<Control>,
    started: oneshot::Sender<SceneResult<()>>,
    done: Arc<AtomicBool>,
) where
    S: Scene,
{
    let start_result = match guarded(config.handler_timeout, scene.start()).await {
        Guarded::Completed(Ok(())) => Ok(()),
        Guarded::Completed(Err(error)) => Err(SceneError::Handler(error)),
        Guarded::TimedOut => Err(SceneError::Timeout),
        Guarded::Panicked => Err(SceneError::Panicked),
    };
    if start_result.is_err() {
        done.store(true, Ordering::Release);
        let _ = started.send(start_result);
        return;
    }
    let _ = started.send(Ok(()));

    let mut ticker = config.tick_interval.map(|period| {
        let mut ticker = interval_at(tokio::time::Instant::now() + period, period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker
    });
    let mut last_tick = Instant::now();
    let mut commands_open = true;
    let mut control_open = true;
    let mut stop_response = None;
    let mut terminate = false;

    while !terminate {
        if !commands_open && !control_open {
            break;
        }
        tokio::select! {
            message = control.recv(), if control_open => {
                match message {
                    Some(Control::Stop { response }) => {
                        stop_response = Some(response);
                        terminate = true;
                    }
                    None => control_open = false,
                }
            }
            message = commands.recv(), if commands_open => {
                let Some(envelope) = message else {
                    commands_open = false;
                    continue;
                };
                match guarded(config.handler_timeout, envelope.command.execute(&mut scene)).await {
                    Guarded::Completed(Ok(output)) => {
                        let _ = envelope.response.send(Ok(output));
                    }
                    Guarded::Completed(Err(handler_error)) => {
                        let _ = envelope.response.send(Err(SceneError::Handler(handler_error)));
                    }
                    Guarded::TimedOut => {
                        let _ = envelope.response.send(Err(SceneError::Timeout));
                    }
                    Guarded::Panicked => {
                        error!("scene command panicked; terminating scene");
                        let _ = envelope.response.send(Err(SceneError::Panicked));
                        terminate = true;
                    }
                }
            }
            _ = next_tick(&mut ticker) => {
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(last_tick);
                last_tick = now;
                match guarded(config.handler_timeout, scene.tick(elapsed)).await {
                    Guarded::Completed(Ok(())) => {}
                    Guarded::Completed(Err(handler_error)) => {
                        warn!(error = %handler_error, "scene tick failed; terminating scene");
                        terminate = true;
                    }
                    Guarded::TimedOut => {
                        warn!("scene tick timed out; terminating scene");
                        terminate = true;
                    }
                    Guarded::Panicked => {
                        error!("scene tick panicked; terminating scene");
                        terminate = true;
                    }
                }
            }
        }
    }

    let stop_result = match guarded(config.shutdown_timeout, scene.stop()).await {
        Guarded::Completed(Ok(())) => Ok(()),
        Guarded::Completed(Err(error)) => Err(SceneError::Handler(error)),
        Guarded::TimedOut => Err(SceneError::Timeout),
        Guarded::Panicked => Err(SceneError::Panicked),
    };
    done.store(true, Ordering::Release);
    if let Some(response) = stop_response {
        let _ = response.send(stop_result);
    } else if let Err(error) = stop_result {
        warn!(error = %error, "scene stopped with an error");
    }
}

async fn next_tick(ticker: &mut Option<tokio::time::Interval>) {
    match ticker {
        Some(ticker) => {
            ticker.tick().await;
        }
        None => std::future::pending().await,
    }
}
