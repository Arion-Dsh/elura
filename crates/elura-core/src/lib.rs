//! Protocol-independent primitives shared by Elura processes.

#![deny(rustdoc::broken_intra_doc_links)]

pub mod account_version;
pub mod error;
pub mod gateway_world;
pub mod identity;
pub mod online;
pub mod otp;
pub mod outbox;
pub mod ownership;
pub mod protocol;
pub mod push;
pub mod rate_limit;
pub mod realm_gateway;
pub mod realtime;
pub mod replay;
pub mod replication;
pub mod session;
pub mod state_hash;
pub mod ticket;

pub use error::{Error, ErrorEnvelope, Result};
