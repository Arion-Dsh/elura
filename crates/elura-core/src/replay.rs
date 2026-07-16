use crate::session::PlayerKey;
use crate::state_hash::StateHash;
use crate::{Error, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::{ErrorKind, Read, Write};
const MAGIC: &[u8; 8] = b"HZNREP04";
const MAX: usize = 16 << 20;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayHeader {
    pub room_id: String,
    pub tick_rate: u32,
    #[serde(default)]
    pub input_delay_ticks: u64,
    #[serde(default)]
    pub rollback_window_ticks: u64,
    pub created_unix_ms: u64,
    pub metadata: serde_json::Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ReplayRecord<C> {
    Command {
        tick: u64,
        player: PlayerKey,
        sequence: u64,
        command: C,
    },
    Tick {
        tick: u64,
    },
    Checkpoint {
        tick: u64,
        state: Vec<u8>,
        state_hash: [u8; 32],
    },
}
impl<C> ReplayRecord<C> {
    pub const fn tick(&self) -> u64 {
        match self {
            Self::Command { tick, .. } | Self::Tick { tick } | Self::Checkpoint { tick, .. } => {
                *tick
            }
        }
    }
}
pub struct ReplayWriter<W> {
    out: W,
    last: u64,
}
impl<W: Write> ReplayWriter<W> {
    pub fn new(mut out: W, header: &ReplayHeader) -> Result<Self> {
        if header.room_id.is_empty() || !(1..=240).contains(&header.tick_rate) {
            return Err(Error::InvalidConfig("replay header".into()));
        }
        out.write_all(MAGIC)?;
        block_write(&mut out, &serde_json::to_vec(header)?)?;
        Ok(Self { out, last: 0 })
    }
    pub fn record<C: Serialize>(&mut self, r: &ReplayRecord<C>) -> Result<()> {
        if r.tick() == 0 || r.tick() < self.last {
            return Err(Error::InvalidFrame("replay order".into()));
        }
        if let ReplayRecord::Checkpoint {
            state, state_hash, ..
        } = r
            && !StateHash::from_bytes(*state_hash).matches(state)
        {
            return Err(Error::InvalidFrame("checkpoint hash".into()));
        }
        block_write(&mut self.out, &serde_json::to_vec(r)?)?;
        self.last = r.tick();
        Ok(())
    }
    pub fn finish(mut self) -> Result<W> {
        self.out.flush()?;
        Ok(self.out)
    }
}
pub struct ReplayReader<R> {
    input: R,
    header: ReplayHeader,
    last: u64,
}
impl<R: Read> ReplayReader<R> {
    pub fn new(mut input: R) -> Result<Self> {
        let mut magic = [0; 8];
        input.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(Error::InvalidFrame("replay format".into()));
        }
        let header = serde_json::from_slice(
            &block_read(&mut input, false)?
                .ok_or_else(|| Error::InvalidFrame("missing replay header".into()))?,
        )?;
        Ok(Self {
            input,
            header,
            last: 0,
        })
    }
    pub const fn header(&self) -> &ReplayHeader {
        &self.header
    }
    pub fn read_next<C: DeserializeOwned>(&mut self) -> Result<Option<ReplayRecord<C>>> {
        let Some(bytes) = block_read(&mut self.input, true)? else {
            return Ok(None);
        };
        let r: ReplayRecord<C> = serde_json::from_slice(&bytes)?;
        if r.tick() == 0 || r.tick() < self.last {
            return Err(Error::InvalidFrame("replay order".into()));
        }
        if let ReplayRecord::Checkpoint {
            state, state_hash, ..
        } = &r
            && !StateHash::from_bytes(*state_hash).matches(state)
        {
            return Err(Error::InvalidFrame("checkpoint hash".into()));
        }
        self.last = r.tick();
        Ok(Some(r))
    }
}
fn crc(b: &[u8]) -> u32 {
    let mut c = u32::MAX;
    for x in b {
        c ^= u32::from(*x);
        for _ in 0..8 {
            c = (c >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(c & 1));
        }
    }
    !c
}
fn block_write(w: &mut impl Write, b: &[u8]) -> Result<()> {
    if b.is_empty() || b.len() > MAX {
        return Err(Error::InvalidFrame("replay block".into()));
    }
    w.write_all(&(b.len() as u32).to_be_bytes())?;
    w.write_all(b)?;
    w.write_all(&crc(b).to_be_bytes())?;
    Ok(())
}
fn block_read(r: &mut impl Read, eof: bool) -> Result<Option<Vec<u8>>> {
    let mut n = [0; 4];
    if let Err(e) = r.read_exact(&mut n) {
        if eof && e.kind() == ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(e.into());
    }
    let n = u32::from_be_bytes(n) as usize;
    if n == 0 || n > MAX {
        return Err(Error::InvalidFrame("replay block".into()));
    }
    let mut b = vec![0; n];
    r.read_exact(&mut b)?;
    let mut c = [0; 4];
    r.read_exact(&mut c)?;
    if crc(&b) != u32::from_be_bytes(c) {
        return Err(Error::InvalidFrame("replay crc".into()));
    }
    Ok(Some(b))
}
