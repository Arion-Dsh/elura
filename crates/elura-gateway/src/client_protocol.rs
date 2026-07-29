use elura_core::protocol::{
    FIRST_APPLICATION_ROUTE, Frame, FrameKind, ROUTE_AUTHENTICATE, ROUTE_HEARTBEAT, ROUTE_RECONNECT,
};
use elura_core::session::Identity;
use elura_core::{Error, ErrorEnvelope, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthenticateRequest {
    pub(crate) ticket: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct AuthenticateResponse {
    pub(crate) session_id: String,
    pub(crate) identity: Identity,
    pub(crate) reconnect: ReconnectTicketResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconnectTicketRequest {
    pub ticket: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectTicketResponse {
    pub ticket: String,
    pub expires_in_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientFrameAction {
    Request,
    HeartbeatResponse,
}

pub(crate) fn validate_client_frame(
    frame: &Frame,
    authenticated: bool,
    pending_heartbeat: Option<u64>,
) -> Result<ClientFrameAction> {
    if frame.kind == FrameKind::Response && frame.route == ROUTE_HEARTBEAT {
        if frame.request_id != pending_heartbeat.unwrap_or(0)
            || frame.sequence != 0
            || !frame.payload.is_empty()
        {
            return Err(Error::InvalidFrame(
                "heartbeat response does not match an outstanding request".into(),
            ));
        }
        return Ok(ClientFrameAction::HeartbeatResponse);
    }
    if frame.kind != FrameKind::Request {
        return Err(Error::InvalidFrame(
            "gateway accepts request frames and correlated heartbeat responses only".into(),
        ));
    }
    if frame.route < FIRST_APPLICATION_ROUTE && frame.sequence != 0 {
        return Err(Error::InvalidFrame(
            "framework requests must have sequence zero".into(),
        ));
    }
    let allowed = if authenticated {
        matches!(frame.route, ROUTE_HEARTBEAT | ROUTE_RECONNECT)
            || frame.route >= FIRST_APPLICATION_ROUTE
    } else {
        frame.route == ROUTE_AUTHENTICATE
    };
    if !allowed {
        return Err(Error::InvalidFrame(
            "route is not allowed in the current session state".into(),
        ));
    }
    Ok(ClientFrameAction::Request)
}

pub(crate) fn error_response(request: &Frame, error: &Error) -> Frame {
    Frame::error(request, ErrorEnvelope::from(error).to_bytes())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use elura_core::protocol::{
        FIRST_APPLICATION_ROUTE, ROUTE_AUTHENTICATE, ROUTE_HEARTBEAT, ROUTE_RECONNECT,
        ROUTE_SESSION_CONTROL,
    };

    use super::*;

    #[test]
    fn rejects_reserved_and_out_of_state_routes() {
        let authentication = Frame::request(ROUTE_AUTHENTICATE, 1, Bytes::new()).unwrap();
        assert_eq!(
            validate_client_frame(&authentication, false, None).unwrap(),
            ClientFrameAction::Request
        );
        assert!(validate_client_frame(&authentication, true, None).is_err());

        let application = Frame::request(FIRST_APPLICATION_ROUTE, 2, Bytes::new()).unwrap();
        assert!(validate_client_frame(&application, false, None).is_err());
        assert_eq!(
            validate_client_frame(&application, true, None).unwrap(),
            ClientFrameAction::Request
        );

        for route in [ROUTE_SESSION_CONTROL, ROUTE_SESSION_CONTROL + 1] {
            let reserved = Frame::request(route, 3, Bytes::new()).unwrap();
            assert!(validate_client_frame(&reserved, true, None).is_err());
        }
    }

    #[test]
    fn framework_sequence_and_heartbeat_response_are_correlated() {
        let mut reconnect = Frame::request(ROUTE_RECONNECT, 1, Bytes::new()).unwrap();
        reconnect.sequence = 1;
        assert!(validate_client_frame(&reconnect, true, None).is_err());

        let heartbeat = Frame::request(ROUTE_HEARTBEAT, 77, Bytes::new()).unwrap();
        let response = Frame::response(&heartbeat, Bytes::new());
        assert_eq!(
            validate_client_frame(&response, true, Some(77)).unwrap(),
            ClientFrameAction::HeartbeatResponse
        );
        assert!(validate_client_frame(&response, true, Some(78)).is_err());
        assert!(validate_client_frame(&response, true, None).is_err());

        let response_with_payload = Frame::response(&heartbeat, Bytes::from_static(b"fake"));
        assert!(validate_client_frame(&response_with_payload, true, Some(77)).is_err());
    }
}
