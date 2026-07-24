//! Whole-room byte snapshot replication.
//!
//! Entity-aware gameplay replication lives in the separate `elura-replication`
//! crate. This module is the lower-level encoded snapshot stream used by the
//! authoritative realtime runtime.

use crate::state_hash::StateHash;
use crate::{Error, Result};
use async_trait::async_trait;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::realtime::{Snapshot, SnapshotPublisher};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PacketKind {
    Keyframe,
    Delta,
}
#[derive(Debug, Clone)]
pub struct Packet {
    pub room_id: Arc<str>,
    pub tick: u64,
    pub elapsed: Duration,
    pub base_tick: u64,
    pub kind: PacketKind,
    pub payload: Vec<u8>,
    pub state_hash: StateHash,
}
#[derive(Clone)]
#[non_exhaustive]
pub struct ReplicationConfig {
    pub keyframe_interval: u64,
    pub max_payload: usize,
}
impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            keyframe_interval: 60,
            max_payload: 4 << 20,
        }
    }
}
pub struct ReplicationStream {
    room: Arc<str>,
    config: ReplicationConfig,
    previous: Option<(u64, Vec<u8>)>,
    force: bool,
}
impl ReplicationStream {
    pub fn new(room: impl Into<Arc<str>>, config: ReplicationConfig) -> Result<Self> {
        let room = room.into();
        if room.is_empty() || config.keyframe_interval == 0 || config.max_payload == 0 {
            return Err(Error::InvalidConfig("replication".into()));
        }
        Ok(Self {
            room,
            config,
            previous: None,
            force: true,
        })
    }
    pub fn force_keyframe(&mut self) {
        self.force = true
    }
    pub fn packet(&mut self, tick: u64, state: &[u8]) -> Result<Packet> {
        if tick == 0
            || state.len() > self.config.max_payload
            || self.previous.as_ref().is_some_and(|(t, _)| tick <= *t)
        {
            return Err(Error::InvalidFrame("replication tick or size".into()));
        }
        let periodic = tick.is_multiple_of(self.config.keyframe_interval);
        let (base_tick, kind, payload) = if let Some((base_tick, base)) = &self.previous {
            if !self.force && !periodic {
                let delta = xor_delta(base, state)?;
                if delta.len() < state.len() {
                    (*base_tick, PacketKind::Delta, delta)
                } else {
                    (0, PacketKind::Keyframe, state.to_vec())
                }
            } else {
                (0, PacketKind::Keyframe, state.to_vec())
            }
        } else {
            (0, PacketKind::Keyframe, state.to_vec())
        };
        self.previous = Some((tick, state.to_vec()));
        self.force = false;
        Ok(Packet {
            room_id: self.room.clone(),
            tick,
            elapsed: Duration::ZERO,
            base_tick,
            kind,
            payload,
            state_hash: StateHash::digest(state),
        })
    }
}

#[async_trait]
pub trait PacketPublisher: Send + Sync + 'static {
    async fn publish_packet(&self, packet: Packet) -> Result<()>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplicationStats {
    pub keyframes_published: u64,
    pub deltas_published: u64,
    pub payload_bytes: u64,
    pub publish_errors: u64,
}

#[derive(Default)]
struct ReplicationCounters {
    keyframes: AtomicU64,
    deltas: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
}

type SnapshotEncoder<S> = dyn Fn(&S) -> Result<Vec<u8>> + Send + Sync;

pub struct ReplicationPublisher<S> {
    stream: Mutex<ReplicationStream>,
    sink: Arc<dyn PacketPublisher>,
    encoder: Arc<SnapshotEncoder<S>>,
    counters: ReplicationCounters,
    marker: PhantomData<fn(S)>,
}

impl<S> ReplicationPublisher<S> {
    pub fn new(
        stream: ReplicationStream,
        sink: Arc<dyn PacketPublisher>,
        encoder: impl Fn(&S) -> Result<Vec<u8>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            stream: Mutex::new(stream),
            sink,
            encoder: Arc::new(encoder),
            counters: ReplicationCounters::default(),
            marker: PhantomData,
        }
    }

    pub fn stats(&self) -> ReplicationStats {
        ReplicationStats {
            keyframes_published: self.counters.keyframes.load(Ordering::Relaxed),
            deltas_published: self.counters.deltas.load(Ordering::Relaxed),
            payload_bytes: self.counters.bytes.load(Ordering::Relaxed),
            publish_errors: self.counters.errors.load(Ordering::Relaxed),
        }
    }
}

#[async_trait]
impl<S> SnapshotPublisher<S> for ReplicationPublisher<S>
where
    S: Send + Sync + 'static,
{
    async fn publish(&self, snapshot: Snapshot<S>) -> Result<()> {
        let encoded = (self.encoder)(&snapshot.state)?;
        let mut packet = self
            .stream
            .lock()
            .map_err(|_| Error::Internal("replication stream poisoned".into()))?
            .packet(snapshot.tick, &encoded)?;
        packet.elapsed = snapshot.elapsed;
        let kind = packet.kind;
        let bytes = packet.payload.len() as u64;
        if let Err(error) = self.sink.publish_packet(packet).await {
            self.counters.errors.fetch_add(1, Ordering::Relaxed);
            self.force_keyframe();
            return Err(error);
        }
        match kind {
            PacketKind::Keyframe => self.counters.keyframes.fetch_add(1, Ordering::Relaxed),
            PacketKind::Delta => self.counters.deltas.fetch_add(1, Ordering::Relaxed),
        };
        self.counters.bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    fn force_keyframe(&self) {
        if let Ok(mut stream) = self.stream.lock() {
            stream.force_keyframe();
        }
    }
}
pub struct ReplicationReceiver {
    room: Arc<str>,
    max: usize,
    current: Option<(u64, Vec<u8>)>,
}
impl ReplicationReceiver {
    pub fn new(room: impl Into<Arc<str>>, max: usize) -> Result<Self> {
        if max == 0 {
            return Err(Error::InvalidConfig("replication".into()));
        }
        Ok(Self {
            room: room.into(),
            max,
            current: None,
        })
    }
    pub fn apply(&mut self, p: &Packet) -> Result<&[u8]> {
        if p.room_id != self.room
            || p.tick == 0
            || p.payload.len() > self.max
            || self.current.as_ref().is_some_and(|(t, _)| p.tick <= *t)
        {
            return Err(Error::InvalidFrame("replication packet".into()));
        }
        let state = match p.kind {
            PacketKind::Keyframe if p.base_tick == 0 => p.payload.clone(),
            PacketKind::Delta => {
                let Some((tick, base)) = &self.current else {
                    return Err(Error::InvalidFrame("missing base".into()));
                };
                if *tick != p.base_tick {
                    return Err(Error::InvalidFrame("base mismatch".into()));
                }
                xor_apply(base, &p.payload)?
            }
            _ => return Err(Error::InvalidFrame("invalid keyframe".into())),
        };
        if state.len() > self.max || !p.state_hash.matches(&state) {
            return Err(Error::InvalidFrame("state hash".into()));
        }
        Ok(&self.current.insert((p.tick, state)).1)
    }
}
fn xor_delta(base: &[u8], now: &[u8]) -> Result<Vec<u8>> {
    let len =
        u32::try_from(now.len()).map_err(|_| Error::InvalidFrame("state too large".into()))?;
    let mut out = len.to_be_bytes().to_vec();
    out.extend(
        now.iter()
            .enumerate()
            .map(|(i, b)| *b ^ base.get(i).copied().unwrap_or(0)),
    );
    while out.len() > 4 && out.last() == Some(&0) {
        out.pop();
    }
    Ok(out)
}
fn xor_apply(base: &[u8], delta: &[u8]) -> Result<Vec<u8>> {
    let raw: [u8; 4] = delta
        .get(..4)
        .ok_or_else(|| Error::InvalidFrame("delta".into()))?
        .try_into()
        .unwrap();
    let mut out = vec![0; u32::from_be_bytes(raw) as usize];
    for (i, v) in out.iter_mut().enumerate() {
        *v = base.get(i).copied().unwrap_or(0) ^ delta.get(i + 4).copied().unwrap_or(0)
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Sink(Mutex<Vec<Packet>>);

    #[async_trait]
    impl PacketPublisher for Sink {
        async fn publish_packet(&self, packet: Packet) -> Result<()> {
            self.0.lock().unwrap().push(packet);
            Ok(())
        }
    }

    #[tokio::test]
    async fn publisher_emits_packets_and_tracks_statistics() {
        let sink = Arc::new(Sink::default());
        let publisher = ReplicationPublisher::new(
            ReplicationStream::new("room", ReplicationConfig::default()).unwrap(),
            sink.clone(),
            |state: &Vec<u8>| Ok(state.clone()),
        );
        publisher
            .publish(Snapshot {
                room_id: Arc::from("room"),
                tick: 1,
                elapsed: Duration::from_millis(10),
                state: vec![0; 16],
                state_hash: StateHash::ZERO,
            })
            .await
            .unwrap();
        let mut next = vec![0; 16];
        next[0] = 1;
        publisher
            .publish(Snapshot {
                room_id: Arc::from("room"),
                tick: 2,
                elapsed: Duration::from_millis(20),
                state: next,
                state_hash: StateHash::ZERO,
            })
            .await
            .unwrap();
        let stats = publisher.stats();
        assert_eq!(stats.keyframes_published, 1);
        assert_eq!(stats.deltas_published, 1);
        assert_eq!(sink.0.lock().unwrap()[1].elapsed, Duration::from_millis(20));
    }
}
