//! Provider-neutral contracts shared by Gateway and World processes.

use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use uuid::Uuid;

use crate::ownership::Assignment;
use crate::session::Identity;
use crate::{Error, Result};

/// Request submitted by a Gateway to a World implementation.
pub struct WorldRequest {
    pub identity: Identity,
    pub session_id: Uuid,
    pub trace_id: String,
    pub route: u32,
    pub request_id: u64,
    pub payload: Bytes,
    pub ownership: Option<Assignment>,
    pub timeout: Duration,
}

/// Business command after the Gateway-to-World Protobuf message is validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldCommand {
    pub authorization: Option<String>,
    pub identity: Identity,
    pub session_id: String,
    pub trace_id: String,
    pub request_id: u64,
    pub payload: Bytes,
    pub shard_id: Option<u32>,
    pub owner_id: Option<String>,
    pub owner_epoch: Option<u64>,
    pub timeout: Duration,
}

/// `elura.internal.v2.GatewayWorldCommand` Protobuf message.
#[derive(Clone, PartialEq, Message)]
pub struct GatewayWorldCommand {
    #[prost(message, optional, tag = "1")]
    pub identity: Option<GatewayWorldIdentity>,
    #[prost(string, tag = "2")]
    pub session_id: String,
    #[prost(string, tag = "3")]
    pub trace_id: String,
    #[prost(uint64, tag = "4")]
    pub request_id: u64,
    #[prost(bytes = "bytes", tag = "5")]
    pub payload: Bytes,
    #[prost(string, optional, tag = "6")]
    pub authorization: Option<String>,
    #[prost(uint32, optional, tag = "7")]
    pub shard_id: Option<u32>,
    #[prost(string, optional, tag = "8")]
    pub owner_id: Option<String>,
    #[prost(uint64, optional, tag = "9")]
    pub owner_epoch: Option<u64>,
    #[prost(uint64, tag = "10")]
    pub timeout_millis: u64,
}

/// `elura.internal.v2.GatewayWorldIdentity` Protobuf message.
#[derive(Clone, PartialEq, Message)]
pub struct GatewayWorldIdentity {
    #[prost(int64, tag = "1")]
    pub account_id: i64,
    #[prost(int64, tag = "2")]
    pub user_id: i64,
    #[prost(uint32, tag = "3")]
    pub region_id: u32,
    #[prost(uint32, tag = "4")]
    pub realm_id: u32,
    #[prost(uint64, tag = "5")]
    pub generation: u64,
}

impl GatewayWorldCommand {
    pub fn encode_frame_payload(&self) -> Bytes {
        Bytes::from(self.encode_to_vec())
    }

    pub fn decode_frame_payload(input: Bytes) -> Result<Self> {
        Self::decode(input)
            .map_err(|_| Error::InvalidFrame("invalid Gateway-to-World Protobuf command".into()))
    }
}

impl From<WorldCommand> for GatewayWorldCommand {
    fn from(command: WorldCommand) -> Self {
        Self {
            identity: Some((&command.identity).into()),
            session_id: command.session_id,
            trace_id: command.trace_id,
            request_id: command.request_id,
            payload: command.payload,
            authorization: command.authorization,
            shard_id: command.shard_id,
            owner_id: command.owner_id,
            owner_epoch: command.owner_epoch,
            timeout_millis: command.timeout.as_millis().try_into().unwrap_or(u64::MAX),
        }
    }
}

impl TryFrom<GatewayWorldCommand> for WorldCommand {
    type Error = Error;

    fn try_from(command: GatewayWorldCommand) -> Result<Self> {
        if command.timeout_millis == 0 {
            return Err(Error::InvalidFrame(
                "Gateway-to-World timeout is zero".into(),
            ));
        }
        let identity = command
            .identity
            .ok_or_else(|| Error::InvalidFrame("missing Gateway-to-World identity".into()))?
            .into();
        Ok(Self {
            authorization: command.authorization,
            identity,
            session_id: command.session_id,
            trace_id: command.trace_id,
            request_id: command.request_id,
            payload: command.payload,
            shard_id: command.shard_id,
            owner_id: command.owner_id,
            owner_epoch: command.owner_epoch,
            timeout: Duration::from_millis(command.timeout_millis),
        })
    }
}

impl From<&Identity> for GatewayWorldIdentity {
    fn from(identity: &Identity) -> Self {
        Self {
            account_id: identity.account_id,
            user_id: identity.user_id,
            region_id: identity.region_id,
            realm_id: identity.realm_id,
            generation: identity.generation,
        }
    }
}

impl From<GatewayWorldIdentity> for Identity {
    fn from(identity: GatewayWorldIdentity) -> Self {
        Self {
            account_id: identity.account_id,
            user_id: identity.user_id,
            region_id: identity.region_id,
            realm_id: identity.realm_id,
            generation: identity.generation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command() -> WorldCommand {
        WorldCommand {
            authorization: Some("internal-token".into()),
            identity: Identity {
                account_id: 7,
                user_id: 9,
                region_id: 1,
                realm_id: 2,
                generation: 3,
            },
            session_id: Uuid::new_v4().to_string(),
            trace_id: "trace-7".into(),
            request_id: 17,
            payload: Bytes::from(vec![7; 1024]),
            shard_id: Some(11),
            owner_id: Some("world-a".into()),
            owner_epoch: Some(13),
            timeout: Duration::from_secs(5),
        }
    }

    #[test]
    fn protobuf_command_round_trips() {
        let command = command();
        let wire = GatewayWorldCommand::from(command.clone()).encode_frame_payload();
        let decoded = GatewayWorldCommand::decode_frame_payload(wire.clone()).unwrap();

        assert_eq!(WorldCommand::try_from(decoded).unwrap(), command);
        assert!(wire.len() < 1200);
    }

    #[test]
    fn protobuf_decode_keeps_payload_in_input_allocation() {
        let wire = GatewayWorldCommand::from(command()).encode_frame_payload();
        let allocation = wire.clone();
        let decoded = GatewayWorldCommand::decode_frame_payload(wire).unwrap();
        let allocation_start = allocation.as_ptr() as usize;
        let allocation_end = allocation_start + allocation.len();
        let payload_start = decoded.payload.as_ptr() as usize;
        let payload_end = payload_start + decoded.payload.len();

        assert!(payload_start >= allocation_start);
        assert!(payload_end <= allocation_end);
    }
}
