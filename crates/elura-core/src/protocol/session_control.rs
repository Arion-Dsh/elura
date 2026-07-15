use bytes::Bytes;
use prost::{Enumeration, Message};

use crate::{Error, Result};

/// Framework action carried by a [`super::ROUTE_SESSION_CONTROL`] Push.
///
/// Numeric values are part of the public wire contract and must remain stable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Enumeration)]
#[repr(i32)]
pub enum SessionControlAction {
    Unspecified = 0,
    Kick = 1,
    AccountVersionChanged = 2,
    DuplicateLogin = 3,
    ForceLogout = 4,
    ServerDraining = 5,
}

/// Typed Protobuf payload for a [`super::ROUTE_SESSION_CONTROL`] Push.
#[derive(Clone, PartialEq, Message)]
pub struct SessionControl {
    #[prost(enumeration = "SessionControlAction", tag = "1")]
    pub action: i32,
    #[prost(string, tag = "2")]
    pub reason: String,
}

impl SessionControl {
    pub fn new(action: SessionControlAction, reason: impl Into<String>) -> Result<Self> {
        let control = Self {
            action: action as i32,
            reason: reason.into(),
        };
        control.validate()?;
        Ok(control)
    }

    pub fn action_kind(&self) -> Result<SessionControlAction> {
        let action = SessionControlAction::try_from(self.action)
            .map_err(|_| Error::InvalidConfig("unknown Session control action".into()))?;
        if action == SessionControlAction::Unspecified {
            return Err(Error::InvalidConfig(
                "Session control action is unspecified".into(),
            ));
        }
        Ok(action)
    }

    pub fn validate(&self) -> Result<()> {
        self.action_kind()?;
        if self.reason.len() > 256 {
            return Err(Error::InvalidConfig(
                "Session control reason exceeds 256 bytes".into(),
            ));
        }
        Ok(())
    }

    /// Serializes this message as the payload of a Session Control frame.
    pub fn encode_frame_payload(&self) -> Result<Bytes> {
        self.validate()?;
        Ok(Bytes::from(self.encode_to_vec()))
    }

    /// Decodes and validates a Session Control frame payload.
    pub fn decode_frame_payload(payload: Bytes) -> Result<Self> {
        let control = Self::decode(payload)
            .map_err(|_| Error::InvalidFrame("invalid Session control Protobuf payload".into()))?;
        control
            .validate()
            .map_err(|_| Error::InvalidFrame("invalid Session control Protobuf payload".into()))?;
        Ok(control)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_control_round_trips() {
        let control = SessionControl::new(
            SessionControlAction::AccountVersionChanged,
            "credentials rotated",
        )
        .unwrap();
        let decoded =
            SessionControl::decode_frame_payload(control.encode_frame_payload().unwrap()).unwrap();

        assert_eq!(
            decoded.action_kind().unwrap(),
            SessionControlAction::AccountVersionChanged
        );
        assert_eq!(decoded.reason, "credentials rotated");
    }

    #[test]
    fn session_control_golden_vector_is_stable() {
        let control = SessionControl::new(
            SessionControlAction::AccountVersionChanged,
            "credentials rotated",
        )
        .unwrap();
        let expected = b"\x08\x02\x12\x13credentials rotated";
        assert_eq!(control.encode_frame_payload().unwrap().as_ref(), expected);
        assert_eq!(
            SessionControl::decode_frame_payload(Bytes::from_static(expected)).unwrap(),
            control
        );
    }

    #[test]
    fn session_control_rejects_unspecified_and_unknown_actions() {
        for action in [SessionControlAction::Unspecified as i32, 99] {
            let payload = Bytes::from(
                SessionControl {
                    action,
                    reason: String::new(),
                }
                .encode_to_vec(),
            );
            assert!(SessionControl::decode_frame_payload(payload).is_err());
        }
    }
}
