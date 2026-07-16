use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior};

use crate::session::PlayerKey;
use crate::state_hash::StateHash;
use crate::{Error, Result};

mod replay_player;
pub use replay_player::{ReplayDriver, ReplayStats, play_replay};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RoomConfig {
    pub tick_rate: u32,
    pub input_delay_ticks: u64,
    pub max_future_input_ticks: u64,
    pub command_capacity: usize,
    /// Maximum commands retained in the scheduler between ticks.
    pub max_pending_commands: usize,
    pub max_commands_per_tick: usize,
    pub snapshot_interval: u64,
    pub rollback_window_ticks: u64,
    pub max_snapshot_bytes: usize,
    pub publish_timeout: Duration,
}

impl Default for RoomConfig {
    fn default() -> Self {
        Self {
            tick_rate: 30,
            input_delay_ticks: 2,
            max_future_input_ticks: 600,
            command_capacity: 1024,
            max_pending_commands: 4096,
            max_commands_per_tick: 256,
            snapshot_interval: 2,
            rollback_window_ticks: 0,
            max_snapshot_bytes: 16 << 20,
            publish_timeout: Duration::from_millis(50),
        }
    }
}

impl RoomConfig {
    fn validate(&self) -> Result<()> {
        if !(1..=240).contains(&self.tick_rate)
            || self.max_future_input_ticks == 0
            || self.command_capacity == 0
            || self.max_pending_commands == 0
            || self.max_commands_per_tick == 0
            || self.snapshot_interval == 0
            || self.rollback_window_ticks > 10_000
            || self.max_snapshot_bytes == 0
            || self.publish_timeout.is_zero()
        {
            return Err(Error::InvalidConfig("invalid room limits".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command<T> {
    pub player: PlayerKey,
    pub sequence: u64,
    pub value: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tick {
    pub number: u64,
    pub delta: Duration,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finalization {
    pub room_id: Arc<str>,
    pub through_tick: u64,
    pub elapsed: Duration,
}

#[derive(Debug, Clone)]
pub struct Snapshot<S> {
    pub room_id: Arc<str>,
    pub tick: u64,
    pub elapsed: Duration,
    pub state: S,
    pub state_hash: StateHash,
}

#[async_trait]
pub trait SnapshotPublisher<S>: Send + Sync + 'static {
    async fn publish(&self, snapshot: Snapshot<S>) -> Result<()>;
    fn force_keyframe(&self) {}
}

#[async_trait]
pub trait Simulation: Send + 'static {
    type Command: Send + Clone + 'static;
    type Snapshot: Send + Sync + Clone + 'static;
    async fn apply(&mut self, command: Command<Self::Command>) -> Result<()>;
    async fn step(&mut self, tick: Tick) -> Result<()>;
    async fn snapshot(&self, tick: Tick) -> Result<Self::Snapshot>;
    /// Encodes a snapshot into stable binary bytes for hashing and replay.
    ///
    /// The same logical state must produce exactly the same bytes across
    /// processes. Prefer fixed-width fields and sorted map iteration.
    fn encode_snapshot(&self, snapshot: &Self::Snapshot) -> Result<Vec<u8>>;
    async fn restore(&mut self, _tick: Tick, _snapshot: Self::Snapshot) -> Result<()> {
        Err(Error::Unavailable)
    }
}

#[async_trait]
pub trait RoomObserver<C, S>: Send + Sync + 'static {
    async fn command(&self, _tick: u64, _command: &Command<C>) -> Result<()> {
        Ok(())
    }
    async fn tick(&self, _tick: Tick) -> Result<()> {
        Ok(())
    }
    async fn snapshot(&self, _tick: Tick, _snapshot: &S, _hash: StateHash) -> Result<()> {
        Ok(())
    }
    async fn rollback(&self, _from: u64, _through: u64) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoomStats {
    pub current_tick: u64,
    pub finalized_tick: u64,
    pub ticks_completed: u64,
    pub tick_failures: u64,
    pub tick_duration_nanos: u64,
    pub max_tick_duration_nanos: u64,
    pub commands_accepted: u64,
    pub commands_queue_full: u64,
    pub command_queue_depth: u64,
    pub commands_received: u64,
    pub commands_applied: u64,
    pub commands_rejected: u64,
    pub duplicates: u64,
    pub late_commands: u64,
    pub snapshots_created: u64,
    pub snapshot_errors: u64,
    pub publications_completed: u64,
    pub publication_errors: u64,
    pub observer_errors: u64,
    pub rollbacks_completed: u64,
    pub rollback_failures: u64,
    pub ticks_resimulated: u64,
    pub finalization_updates: u64,
}

#[derive(Default)]
struct Counters {
    ticks: AtomicU64,
    tick_failures: AtomicU64,
    tick_nanos: AtomicU64,
    max_tick_nanos: AtomicU64,
    accepted: AtomicU64,
    queue_full: AtomicU64,
    received: AtomicU64,
    applied: AtomicU64,
    rejected: AtomicU64,
    duplicates: AtomicU64,
    late: AtomicU64,
    snapshots: AtomicU64,
    snapshot_errors: AtomicU64,
    published: AtomicU64,
    publication_errors: AtomicU64,
    observer_errors: AtomicU64,
    rollbacks: AtomicU64,
    rollback_failures: AtomicU64,
    resimulated: AtomicU64,
    finalizations: AtomicU64,
}

struct Submitted<T> {
    target: u64,
    command: Command<T>,
}

#[derive(Clone)]
struct Scheduled<T> {
    target: u64,
    order: u64,
    command: Command<T>,
}
impl<T> PartialEq for Scheduled<T> {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target && self.order == other.order
    }
}
impl<T> Eq for Scheduled<T> {}
impl<T> PartialOrd for Scheduled<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for Scheduled<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .target
            .cmp(&self.target)
            .then_with(|| other.order.cmp(&self.order))
    }
}

struct TickRecord<C, S> {
    commands: Vec<Command<C>>,
    snapshot: S,
    hash: StateHash,
}
struct Runtime<C, S> {
    receiver: mpsc::Receiver<Submitted<C>>,
    pending: BinaryHeap<Scheduled<C>>,
    history: BTreeMap<u64, TickRecord<C, S>>,
    sequences: HashMap<PlayerKey, u64>,
    order: u64,
    initialized: bool,
}

pub struct Room<S: Simulation> {
    id: Arc<str>,
    config: RoomConfig,
    delta: Duration,
    simulation: Mutex<S>,
    runtime: Mutex<Runtime<S::Command, S::Snapshot>>,
    sender: mpsc::Sender<Submitted<S::Command>>,
    snapshots: watch::Sender<Option<S::Snapshot>>,
    finalizations: watch::Sender<Option<Finalization>>,
    current: AtomicU64,
    finalized: AtomicU64,
    running: AtomicBool,
    counters: Arc<Counters>,
    observer: Option<Arc<dyn RoomObserver<S::Command, S::Snapshot>>>,
    publisher: Option<Arc<dyn SnapshotPublisher<S::Snapshot>>>,
}

impl<S: Simulation> Room<S> {
    pub fn new(id: impl Into<Arc<str>>, config: RoomConfig, simulation: S) -> Result<Self> {
        Self::with_observer(id, config, simulation, None)
    }
    pub fn with_observer(
        id: impl Into<Arc<str>>,
        config: RoomConfig,
        simulation: S,
        observer: Option<Arc<dyn RoomObserver<S::Command, S::Snapshot>>>,
    ) -> Result<Self> {
        config.validate()?;
        let id = id.into();
        if id.is_empty() || id.len() > 128 {
            return Err(Error::InvalidConfig("invalid room id".into()));
        }
        let (sender, receiver) = mpsc::channel(config.command_capacity);
        let (snapshots, _) = watch::channel(None);
        let (finalizations, _) = watch::channel(None);
        let delta = Duration::from_secs_f64(1.0 / config.tick_rate as f64);
        Ok(Self {
            id,
            config,
            delta,
            simulation: Mutex::new(simulation),
            runtime: Mutex::new(Runtime {
                receiver,
                pending: BinaryHeap::new(),
                history: BTreeMap::new(),
                sequences: HashMap::new(),
                order: 0,
                initialized: false,
            }),
            sender,
            snapshots,
            finalizations,
            current: AtomicU64::new(0),
            finalized: AtomicU64::new(0),
            running: AtomicBool::new(false),
            counters: Arc::new(Counters::default()),
            observer,
            publisher: None,
        })
    }

    pub fn with_publisher(mut self, publisher: Arc<dyn SnapshotPublisher<S::Snapshot>>) -> Self {
        self.publisher = Some(publisher);
        self
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn current_tick(&self) -> u64 {
        self.current.load(AtomicOrdering::Acquire)
    }
    pub fn finalized_tick(&self) -> u64 {
        self.finalized.load(AtomicOrdering::Acquire)
    }
    pub const fn tick_rate(&self) -> u32 {
        self.config.tick_rate
    }
    pub const fn input_delay_ticks(&self) -> u64 {
        self.config.input_delay_ticks
    }
    pub const fn rollback_window_ticks(&self) -> u64 {
        self.config.rollback_window_ticks
    }

    pub async fn canonical_state(&self) -> Result<Vec<u8>> {
        let simulation = self.simulation.lock().await;
        let snapshot = simulation.snapshot(self.tick(self.current_tick())).await?;
        self.encode(&*simulation, &snapshot)
    }
    pub fn subscribe_snapshots(&self) -> watch::Receiver<Option<S::Snapshot>> {
        self.snapshots.subscribe()
    }
    pub fn subscribe_finalizations(&self) -> watch::Receiver<Option<Finalization>> {
        self.finalizations.subscribe()
    }
    pub fn try_submit(&self, command: Command<S::Command>) -> Result<()> {
        self.try_submit_at(
            self.current_tick() + self.config.input_delay_ticks + 1,
            command,
        )
    }
    pub async fn submit(&self, command: Command<S::Command>) -> Result<()> {
        self.submit_at(
            self.current_tick() + self.config.input_delay_ticks + 1,
            command,
        )
        .await
    }
    pub fn try_submit_at(&self, target: u64, command: Command<S::Command>) -> Result<()> {
        self.validate(target, &command)?;
        match self.sender.try_send(Submitted { target, command }) {
            Ok(()) => {
                self.counters.accepted.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(())
            }
            Err(_) => {
                self.counters
                    .queue_full
                    .fetch_add(1, AtomicOrdering::Relaxed);
                Err(Error::QueueFull)
            }
        }
    }
    pub async fn submit_at(&self, target: u64, command: Command<S::Command>) -> Result<()> {
        self.validate(target, &command)?;
        self.sender
            .send(Submitted { target, command })
            .await
            .map_err(|_| Error::Unavailable)?;
        self.counters.accepted.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }
    fn validate(&self, target: u64, command: &Command<S::Command>) -> Result<()> {
        let current = self.current_tick();
        if target == 0
            || command.player.validate().is_err()
            || command.sequence == 0
            || target > current.saturating_add(self.config.max_future_input_ticks)
        {
            return Err(Error::InvalidConfig("invalid room command".into()));
        }
        if target <= current
            && (self.config.rollback_window_ticks == 0 || target <= self.finalized_tick())
        {
            return Err(Error::InvalidConfig("target tick cannot be changed".into()));
        }
        Ok(())
    }
    pub async fn advance(&self) -> Result<()> {
        if self.running.load(AtomicOrdering::Acquire) {
            return Err(Error::InvalidConfig("room is running".into()));
        }
        self.advance_one().await
    }
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        if self.running.swap(true, AtomicOrdering::AcqRel) {
            return Err(Error::InvalidConfig("room already running".into()));
        }
        let mut timer = tokio::time::interval_at(Instant::now() + self.delta, self.delta);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let result = loop {
            tokio::select! {changed=shutdown.changed()=>if changed.is_err()||*shutdown.borrow(){break Ok(())},_=timer.tick()=>self.advance_one().await?,}
        };
        self.running.store(false, AtomicOrdering::Release);
        result
    }
    async fn advance_one(&self) -> Result<()> {
        let started = Instant::now();
        let result = self.advance_inner().await;
        let nanos = started.elapsed().as_nanos() as u64;
        self.counters
            .tick_nanos
            .fetch_add(nanos, AtomicOrdering::Relaxed);
        self.counters
            .max_tick_nanos
            .fetch_max(nanos, AtomicOrdering::Relaxed);
        if result.is_err() {
            self.counters
                .tick_failures
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        result
    }

    async fn advance_inner(&self) -> Result<()> {
        let mut runtime = self.runtime.lock().await;
        let mut simulation = self.simulation.lock().await;
        self.initialize(&mut runtime, &simulation).await?;
        let late = self.drain(&mut runtime);
        let current = self.current_tick();
        if let Some(from) = late {
            self.resimulate(&mut runtime, &mut simulation, from, current)
                .await?
        }
        let number = current + 1;
        let tick = self.tick(number);
        let mut commands = Vec::new();
        for _ in 0..self.config.max_commands_per_tick {
            let Some(next) = runtime.pending.peek() else {
                break;
            };
            if next.target > number {
                break;
            }
            let Some(item) = runtime.pending.pop() else {
                break;
            };
            if let Some(o) = &self.observer
                && let Err(error) = o.command(number, &item.command).await
            {
                self.counters
                    .observer_errors
                    .fetch_add(1, AtomicOrdering::Relaxed);
                tracing::warn!(room_id = %self.id, tick = number, %error, "Realtime command observer failed");
            }
            match simulation.apply(item.command.clone()).await {
                Ok(()) => {
                    self.counters.applied.fetch_add(1, AtomicOrdering::Relaxed);
                    commands.push(item.command)
                }
                Err(_) => {
                    self.counters.rejected.fetch_add(1, AtomicOrdering::Relaxed);
                }
            }
        }
        simulation.step(tick).await?;
        if let Some(o) = &self.observer
            && let Err(error) = o.tick(tick).await
        {
            self.counters
                .observer_errors
                .fetch_add(1, AtomicOrdering::Relaxed);
            tracing::warn!(room_id = %self.id, tick = number, %error, "Realtime tick observer failed");
        }
        let (snapshot, hash) = self.capture(&simulation, tick).await?;
        runtime.history.insert(
            number,
            TickRecord {
                commands,
                snapshot: snapshot.clone(),
                hash,
            },
        );
        self.current.store(number, AtomicOrdering::Release);
        self.counters.ticks.fetch_add(1, AtomicOrdering::Relaxed);
        if number.is_multiple_of(self.config.snapshot_interval) {
            self.snapshots.send_replace(Some(snapshot.clone()));
            if let Some(o) = &self.observer
                && let Err(error) = o.snapshot(tick, &snapshot, hash).await
            {
                self.counters
                    .observer_errors
                    .fetch_add(1, AtomicOrdering::Relaxed);
                tracing::warn!(room_id = %self.id, tick = number, %error, "Realtime snapshot observer failed");
            }
            if let Some(publisher) = &self.publisher {
                let publication = publisher.publish(Snapshot {
                    room_id: self.id.clone(),
                    tick: number,
                    elapsed: tick.elapsed,
                    state: snapshot.clone(),
                    state_hash: hash,
                });
                match tokio::time::timeout(self.config.publish_timeout, publication).await {
                    Ok(Ok(())) => {
                        self.counters
                            .published
                            .fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    Ok(Err(error)) => {
                        self.counters
                            .publication_errors
                            .fetch_add(1, AtomicOrdering::Relaxed);
                        tracing::warn!(room_id = %self.id, tick = number, %error, "Realtime snapshot publication failed");
                    }
                    Err(_) => {
                        self.counters
                            .publication_errors
                            .fetch_add(1, AtomicOrdering::Relaxed);
                    }
                }
            }
        }
        let through = number.saturating_sub(self.config.rollback_window_ticks);
        if through > self.finalized_tick() {
            self.finalized.store(through, AtomicOrdering::Release);
            self.finalizations.send_replace(Some(Finalization {
                room_id: self.id.clone(),
                through_tick: through,
                elapsed: self.tick(through).elapsed,
            }));
            self.counters
                .finalizations
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        runtime.history.retain(|tick, _| *tick >= through);
        Ok(())
    }
    async fn initialize(&self, r: &mut Runtime<S::Command, S::Snapshot>, s: &S) -> Result<()> {
        if !r.initialized {
            let (snapshot, hash) = self.capture(s, self.tick(0)).await?;
            r.history.insert(
                0,
                TickRecord {
                    commands: Vec::new(),
                    snapshot,
                    hash,
                },
            );
            r.initialized = true
        }
        Ok(())
    }
    fn drain(&self, r: &mut Runtime<S::Command, S::Snapshot>) -> Option<u64> {
        let current = self.current_tick();
        let mut late = None;
        while let Ok(input) = r.receiver.try_recv() {
            self.counters.received.fetch_add(1, AtomicOrdering::Relaxed);
            let last = r.sequences.get(&input.command.player).copied().unwrap_or(0);
            if input.command.sequence <= last {
                self.counters
                    .duplicates
                    .fetch_add(1, AtomicOrdering::Relaxed);
                continue;
            }
            if input.target <= current {
                self.counters.late.fetch_add(1, AtomicOrdering::Relaxed);
                if let Some(record) = r.history.get_mut(&input.target)
                    && record.commands.len() < self.config.max_commands_per_tick
                {
                    r.sequences
                        .insert(input.command.player, input.command.sequence);
                    record.commands.push(input.command);
                    late = Some(late.map_or(input.target, |v: u64| v.min(input.target)))
                } else {
                    self.counters.rejected.fetch_add(1, AtomicOrdering::Relaxed);
                }
            } else if r.pending.len() >= self.config.max_pending_commands {
                self.counters.rejected.fetch_add(1, AtomicOrdering::Relaxed);
            } else {
                r.sequences
                    .insert(input.command.player, input.command.sequence);
                r.order += 1;
                r.pending.push(Scheduled {
                    target: input.target,
                    order: r.order,
                    command: input.command,
                })
            }
        }
        late
    }
    async fn resimulate(
        &self,
        r: &mut Runtime<S::Command, S::Snapshot>,
        s: &mut S,
        from: u64,
        through: u64,
    ) -> Result<()> {
        let base = from
            .checked_sub(1)
            .ok_or_else(|| Error::InvalidConfig("rollback target".into()))?;
        let checkpoint = r
            .history
            .get(&base)
            .ok_or_else(|| Error::InvalidConfig("rollback window".into()))?;
        if !checkpoint
            .hash
            .matches(&self.encode(s, &checkpoint.snapshot)?)
        {
            return Err(Error::InvalidFrame("checkpoint corrupted".into()));
        }
        if let Err(error) = s
            .restore(self.tick(base), checkpoint.snapshot.clone())
            .await
        {
            self.counters
                .rollback_failures
                .fetch_add(1, AtomicOrdering::Relaxed);
            return Err(error);
        }
        let verified = s.snapshot(self.tick(base)).await?;
        if !checkpoint.hash.matches(&self.encode(s, &verified)?) {
            return Err(Error::InvalidFrame("restore mismatch".into()));
        }
        for number in from..=through {
            let commands = r
                .history
                .get(&number)
                .ok_or_else(|| Error::InvalidConfig("rollback history".into()))?
                .commands
                .clone();
            for command in commands {
                s.apply(command).await?
            }
            let tick = self.tick(number);
            s.step(tick).await?;
            let (snapshot, hash) = self.capture(s, tick).await?;
            let record = r
                .history
                .get_mut(&number)
                .ok_or_else(|| Error::InvalidConfig("rollback history".into()))?;
            record.snapshot = snapshot;
            record.hash = hash;
        }
        self.counters
            .rollbacks
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.counters
            .resimulated
            .fetch_add(through - from + 1, AtomicOrdering::Relaxed);
        if let Some(publisher) = &self.publisher {
            publisher.force_keyframe();
        }
        if let Some(o) = &self.observer
            && let Err(error) = o.rollback(from, through).await
        {
            self.counters
                .observer_errors
                .fetch_add(1, AtomicOrdering::Relaxed);
            tracing::warn!(room_id = %self.id, %error, "Realtime rollback observer failed");
        }
        Ok(())
    }
    async fn capture(&self, s: &S, tick: Tick) -> Result<(S::Snapshot, StateHash)> {
        let result = async {
            let snapshot = s.snapshot(tick).await?;
            let bytes = self.encode(s, &snapshot)?;
            Ok((snapshot, StateHash::digest(&bytes)))
        }
        .await;
        match result {
            Ok(value) => {
                self.counters
                    .snapshots
                    .fetch_add(1, AtomicOrdering::Relaxed);
                Ok(value)
            }
            Err(error) => {
                self.counters
                    .snapshot_errors
                    .fetch_add(1, AtomicOrdering::Relaxed);
                Err(error)
            }
        }
    }
    fn encode(&self, simulation: &S, snapshot: &S::Snapshot) -> Result<Vec<u8>> {
        let bytes = simulation.encode_snapshot(snapshot)?;
        if bytes.len() > self.config.max_snapshot_bytes {
            return Err(Error::InvalidFrame("snapshot too large".into()));
        }
        Ok(bytes)
    }
    fn tick(&self, number: u64) -> Tick {
        Tick {
            number,
            delta: self.delta,
            elapsed: self
                .delta
                .saturating_mul(u32::try_from(number).unwrap_or(u32::MAX)),
        }
    }
    pub fn stats(&self) -> RoomStats {
        RoomStats {
            current_tick: self.current_tick(),
            finalized_tick: self.finalized_tick(),
            ticks_completed: self.counters.ticks.load(AtomicOrdering::Relaxed),
            tick_failures: self.counters.tick_failures.load(AtomicOrdering::Relaxed),
            tick_duration_nanos: self.counters.tick_nanos.load(AtomicOrdering::Relaxed),
            max_tick_duration_nanos: self.counters.max_tick_nanos.load(AtomicOrdering::Relaxed),
            commands_accepted: self.counters.accepted.load(AtomicOrdering::Relaxed),
            commands_queue_full: self.counters.queue_full.load(AtomicOrdering::Relaxed),
            command_queue_depth: self
                .sender
                .max_capacity()
                .saturating_sub(self.sender.capacity()) as u64,
            commands_received: self.counters.received.load(AtomicOrdering::Relaxed),
            commands_applied: self.counters.applied.load(AtomicOrdering::Relaxed),
            commands_rejected: self.counters.rejected.load(AtomicOrdering::Relaxed),
            duplicates: self.counters.duplicates.load(AtomicOrdering::Relaxed),
            late_commands: self.counters.late.load(AtomicOrdering::Relaxed),
            snapshots_created: self.counters.snapshots.load(AtomicOrdering::Relaxed),
            snapshot_errors: self.counters.snapshot_errors.load(AtomicOrdering::Relaxed),
            publications_completed: self.counters.published.load(AtomicOrdering::Relaxed),
            publication_errors: self
                .counters
                .publication_errors
                .load(AtomicOrdering::Relaxed),
            observer_errors: self.counters.observer_errors.load(AtomicOrdering::Relaxed),
            rollbacks_completed: self.counters.rollbacks.load(AtomicOrdering::Relaxed),
            rollback_failures: self
                .counters
                .rollback_failures
                .load(AtomicOrdering::Relaxed),
            ticks_resimulated: self.counters.resimulated.load(AtomicOrdering::Relaxed),
            finalization_updates: self.counters.finalizations.load(AtomicOrdering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{ReplayHeader, ReplayReader, ReplayRecord, ReplayWriter};
    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct Add(i64);
    #[derive(Clone)]
    struct State {
        i: i64,
    }
    struct Sim(State);
    fn player_in(realm_id: u32, user_id: i64) -> PlayerKey {
        PlayerKey::new(1, realm_id, user_id).unwrap()
    }
    fn player(user_id: i64) -> PlayerKey {
        player_in(1, user_id)
    }
    #[async_trait]
    impl Simulation for Sim {
        type Command = Add;
        type Snapshot = State;
        async fn apply(&mut self, c: Command<Add>) -> Result<()> {
            self.0.i += c.value.0;
            Ok(())
        }
        async fn step(&mut self, _: Tick) -> Result<()> {
            self.0.i += 1;
            Ok(())
        }
        async fn snapshot(&self, _: Tick) -> Result<State> {
            Ok(self.0.clone())
        }
        fn encode_snapshot(&self, snapshot: &State) -> Result<Vec<u8>> {
            Ok(snapshot.i.to_le_bytes().to_vec())
        }
        async fn restore(&mut self, _: Tick, s: State) -> Result<()> {
            self.0 = s;
            Ok(())
        }
    }
    #[tokio::test]
    async fn rolls_back_late_input() {
        let room = Room::new(
            "r",
            RoomConfig {
                input_delay_ticks: 0,
                rollback_window_ticks: 3,
                snapshot_interval: 1,
                ..Default::default()
            },
            Sim(State { i: 0 }),
        )
        .unwrap();
        room.try_submit_at(
            1,
            Command {
                player: player(1),
                sequence: 1,
                value: Add(2),
            },
        )
        .unwrap();
        room.advance().await.unwrap();
        room.advance().await.unwrap();
        room.try_submit_at(
            1,
            Command {
                player: player(2),
                sequence: 1,
                value: Add(5),
            },
        )
        .unwrap();
        room.advance().await.unwrap();
        assert_eq!(room.stats().rollbacks_completed, 1);
        assert_eq!(room.snapshots.borrow().as_ref().unwrap().i, 10);
    }
    #[tokio::test]
    async fn finalization_rejects_old_input() {
        let room = Room::new(
            "r",
            RoomConfig {
                rollback_window_ticks: 1,
                ..Default::default()
            },
            Sim(State { i: 0 }),
        )
        .unwrap();
        room.advance().await.unwrap();
        room.advance().await.unwrap();
        assert_eq!(room.finalized_tick(), 1);
        assert!(
            room.try_submit_at(
                1,
                Command {
                    player: player(1),
                    sequence: 1,
                    value: Add(1)
                }
            )
            .is_err()
        );
    }

    struct FailingPublisher;

    #[async_trait]
    impl SnapshotPublisher<State> for FailingPublisher {
        async fn publish(&self, _snapshot: Snapshot<State>) -> Result<()> {
            Err(Error::Unavailable)
        }
    }

    #[tokio::test]
    async fn publisher_failure_is_isolated_and_counted() {
        let room = Room::new(
            "r",
            RoomConfig {
                snapshot_interval: 1,
                ..Default::default()
            },
            Sim(State { i: 0 }),
        )
        .unwrap()
        .with_publisher(Arc::new(FailingPublisher));
        room.advance().await.unwrap();
        assert_eq!(room.current_tick(), 1);
        assert_eq!(room.stats().publication_errors, 1);
    }

    #[tokio::test]
    async fn equal_user_ids_in_different_realms_have_independent_sequences() {
        let room = Room::new(
            "cross-realm",
            RoomConfig {
                snapshot_interval: 1,
                ..RoomConfig::default()
            },
            Sim(State { i: 0 }),
        )
        .unwrap();
        room.try_submit_at(
            1,
            Command {
                player: player_in(1, 42),
                sequence: 1,
                value: Add(2),
            },
        )
        .unwrap();
        room.try_submit_at(
            1,
            Command {
                player: player_in(2, 42),
                sequence: 1,
                value: Add(3),
            },
        )
        .unwrap();
        room.advance().await.unwrap();
        assert_eq!(room.snapshots.borrow().as_ref().unwrap().i, 6);
        assert_eq!(room.stats().duplicates, 0);
    }

    #[tokio::test]
    async fn rejects_commands_beyond_the_scheduler_capacity() {
        let room = Room::new(
            "bounded",
            RoomConfig {
                input_delay_ticks: 0,
                max_pending_commands: 2,
                ..RoomConfig::default()
            },
            Sim(State { i: 0 }),
        )
        .unwrap();
        for user_id in 1..=3 {
            room.try_submit_at(
                2,
                Command {
                    player: player(user_id),
                    sequence: 1,
                    value: Add(1),
                },
            )
            .unwrap();
        }
        room.advance().await.unwrap();
        assert_eq!(room.stats().commands_rejected, 1);
    }

    #[tokio::test]
    async fn replay_player_drives_and_verifies_fresh_room() {
        let header = ReplayHeader {
            room_id: "replay-room".into(),
            tick_rate: 30,
            input_delay_ticks: 0,
            rollback_window_ticks: 0,
            created_unix_ms: 1,
            metadata: serde_json::Value::Null,
        };
        let canonical = 3_i64.to_le_bytes().to_vec();
        let hash = *StateHash::digest(&canonical).as_bytes();
        let mut writer = ReplayWriter::new(Vec::new(), &header).unwrap();
        writer
            .record(&ReplayRecord::Command {
                tick: 1,
                player: player(1),
                sequence: 1,
                command: Add(2),
            })
            .unwrap();
        writer
            .record::<Add>(&ReplayRecord::Tick { tick: 1 })
            .unwrap();
        writer
            .record::<Add>(&ReplayRecord::Checkpoint {
                tick: 1,
                state: canonical,
                state_hash: hash,
            })
            .unwrap();
        let bytes = writer.finish().unwrap();
        let mut reader = ReplayReader::new(bytes.as_slice()).unwrap();
        let room = Room::new(
            "replay-room",
            RoomConfig {
                input_delay_ticks: 0,
                rollback_window_ticks: 0,
                ..Default::default()
            },
            Sim(State { i: 0 }),
        )
        .unwrap();
        let stats = play_replay::<_, Add, _>(&mut reader, &room).await.unwrap();
        assert_eq!(stats.commands_submitted, 1);
        assert_eq!(stats.checkpoints_verified, 1);
    }
}
