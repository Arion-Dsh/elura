//! Standalone Rust implementation of the public Elura ELR2 protocol.

use std::fmt;
#[cfg(feature = "tokio-codec")]
use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use serde::{Deserialize, Serialize};
#[cfg(feature = "tokio-codec")]
use tokio_util::codec::{Decoder, Encoder};

pub const ELR2_MAGIC: u32 = 0x454c5232;
pub const ELR2_VERSION: u16 = 2;
pub const ELR2_HEADER_LENGTH: usize = 28;
pub const DEFAULT_MAX_PAYLOAD: usize = 1 << 20;
pub const ABSOLUTE_MAX_PAYLOAD: usize = 64 << 20;
pub const PROTOCOL_IDENTIFIER: &str = "elura.v2";

pub struct EluraRoutes;

impl EluraRoutes {
    pub const AUTHENTICATE: u32 = 1;
    pub const HEARTBEAT: u32 = 2;
    pub const RENEW_RECONNECT_TICKET: u32 = 3;
    pub const SESSION_CONTROL: u32 = 4;
    pub const FIRST_APPLICATION: u32 = 100;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Request = 1,
    Response = 2,
    Push = 3,
    Error = 4,
}

impl TryFrom<u8> for FrameKind {
    type Error = Elr2ProtocolError;

    fn try_from(value: u8) -> Result<Self, Elr2ProtocolError> {
        match value {
            1 => Ok(Self::Request),
            2 => Ok(Self::Response),
            3 => Ok(Self::Push),
            4 => Ok(Self::Error),
            _ => Err(Elr2ProtocolError::new("unknown frame kind")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Elr2Frame {
    pub kind: FrameKind,
    pub flags: u8,
    pub route: u32,
    pub request_id: u64,
    pub sequence: u32,
    pub payload: Bytes,
}

impl Elr2Frame {
    pub fn request(route: u32, request_id: u64, payload: impl Into<Bytes>) -> ProtocolResult<Self> {
        Self::request_with_sequence(route, request_id, 0, payload)
    }

    pub fn request_with_sequence(
        route: u32,
        request_id: u64,
        sequence: u32,
        payload: impl Into<Bytes>,
    ) -> ProtocolResult<Self> {
        Self::create(
            FrameKind::Request,
            route,
            request_id,
            sequence,
            payload.into(),
        )
    }

    pub fn response(request: &Self, payload: impl Into<Bytes>) -> ProtocolResult<Self> {
        Self::from_request(FrameKind::Response, request, payload.into())
    }

    pub fn error(request: &Self, payload: impl Into<Bytes>) -> ProtocolResult<Self> {
        Self::from_request(FrameKind::Error, request, payload.into())
    }

    pub fn push(route: u32, payload: impl Into<Bytes>) -> ProtocolResult<Self> {
        Self::push_with_sequence(route, 0, payload)
    }

    pub fn push_with_sequence(
        route: u32,
        sequence: u32,
        payload: impl Into<Bytes>,
    ) -> ProtocolResult<Self> {
        Self::create(FrameKind::Push, route, 0, sequence, payload.into())
    }

    fn from_request(kind: FrameKind, request: &Self, payload: Bytes) -> ProtocolResult<Self> {
        if request.kind != FrameKind::Request {
            return Err(Elr2ProtocolError::new(
                "response source must be a request frame",
            ));
        }
        Self::create(
            kind,
            request.route,
            request.request_id,
            request.sequence,
            payload,
        )
    }

    fn create(
        kind: FrameKind,
        route: u32,
        request_id: u64,
        sequence: u32,
        payload: Bytes,
    ) -> ProtocolResult<Self> {
        let frame = Self {
            kind,
            flags: 0,
            route,
            request_id,
            sequence,
            payload,
        };
        Elr2Codec::validate(&frame, ABSOLUTE_MAX_PAYLOAD)?;
        Ok(frame)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Elr2ProtocolError {
    message: String,
}

impl Elr2ProtocolError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for Elr2ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Elr2ProtocolError {}

pub type ProtocolResult<T> = Result<T, Elr2ProtocolError>;

#[derive(Debug, Clone)]
pub struct Elr2Codec {
    #[cfg(feature = "tokio-codec")]
    max_payload: usize,
}

impl Elr2Codec {
    #[cfg(feature = "tokio-codec")]
    pub fn new(max_payload: usize) -> ProtocolResult<Self> {
        validate_limit(max_payload)?;
        Ok(Self { max_payload })
    }

    pub fn validate(frame: &Elr2Frame, max_payload: usize) -> ProtocolResult<()> {
        validate_limit(max_payload)?;
        if frame.payload.len() > max_payload {
            return Err(Elr2ProtocolError::new("payload is too large"));
        }
        if frame.flags != 0 {
            return Err(Elr2ProtocolError::new("unsupported frame flags"));
        }
        if frame.route == 0 {
            return Err(Elr2ProtocolError::new("route must be non-zero"));
        }
        if frame.kind == FrameKind::Push {
            if frame.request_id != 0 {
                return Err(Elr2ProtocolError::new("push request id must be zero"));
            }
        } else if frame.request_id == 0 {
            return Err(Elr2ProtocolError::new("request id must be non-zero"));
        }
        Ok(())
    }

    pub fn encode(frame: &Elr2Frame) -> ProtocolResult<Bytes> {
        Self::encode_with_limit(frame, DEFAULT_MAX_PAYLOAD)
    }

    pub fn encode_with_limit(frame: &Elr2Frame, max_payload: usize) -> ProtocolResult<Bytes> {
        Self::validate(frame, max_payload)?;
        let mut output = BytesMut::with_capacity(ELR2_HEADER_LENGTH + frame.payload.len());
        write_frame(frame, &mut output);
        Ok(output.freeze())
    }

    pub fn decode(message: &[u8]) -> ProtocolResult<Elr2Frame> {
        Self::decode_with_limit(message, DEFAULT_MAX_PAYLOAD)
    }

    pub fn decode_with_limit(message: &[u8], max_payload: usize) -> ProtocolResult<Elr2Frame> {
        decode_bytes(Bytes::copy_from_slice(message), max_payload)
    }

    #[cfg(feature = "tokio-codec")]
    fn decode_message(&self, message: Bytes) -> io::Result<Elr2Frame> {
        decode_bytes(message, self.max_payload).map_err(protocol_io_error)
    }
}

impl Default for Elr2Codec {
    fn default() -> Self {
        Self {
            #[cfg(feature = "tokio-codec")]
            max_payload: DEFAULT_MAX_PAYLOAD,
        }
    }
}

#[cfg(feature = "tokio-codec")]
impl Decoder for Elr2Codec {
    type Item = Elr2Frame;
    type Error = io::Error;

    fn decode(&mut self, source: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        if source.len() < ELR2_HEADER_LENGTH {
            return Ok(None);
        }
        validate_header(&source[..ELR2_HEADER_LENGTH], self.max_payload)
            .map_err(protocol_io_error)?;
        let payload_length =
            u32::from_be_bytes(source[24..28].try_into().expect("header length checked")) as usize;
        let total = ELR2_HEADER_LENGTH + payload_length;
        if source.len() < total {
            source.reserve(total - source.len());
            return Ok(None);
        }
        let message = source.split_to(total).freeze();
        self.decode_message(message).map(Some)
    }
}

#[cfg(feature = "tokio-codec")]
impl Encoder<Elr2Frame> for Elr2Codec {
    type Error = io::Error;

    fn encode(&mut self, frame: Elr2Frame, destination: &mut BytesMut) -> io::Result<()> {
        Self::validate(&frame, self.max_payload).map_err(protocol_io_error)?;
        destination.reserve(ELR2_HEADER_LENGTH + frame.payload.len());
        write_frame(&frame, destination);
        Ok(())
    }
}

fn validate_limit(max_payload: usize) -> ProtocolResult<()> {
    if max_payload == 0 || max_payload > ABSOLUTE_MAX_PAYLOAD {
        return Err(Elr2ProtocolError::new("max payload must be in 1..=64MiB"));
    }
    Ok(())
}

fn validate_header(header: &[u8], max_payload: usize) -> ProtocolResult<()> {
    validate_limit(max_payload)?;
    if header.len() < ELR2_HEADER_LENGTH {
        return Err(Elr2ProtocolError::new("incomplete Elura frame"));
    }
    if u32::from_be_bytes(header[0..4].try_into().expect("header length checked")) != ELR2_MAGIC {
        return Err(Elr2ProtocolError::new("invalid Elura magic"));
    }
    if u16::from_be_bytes(header[4..6].try_into().expect("header length checked")) != ELR2_VERSION {
        return Err(Elr2ProtocolError::new("unsupported Elura version"));
    }
    FrameKind::try_from(header[6])?;
    let payload_length =
        u32::from_be_bytes(header[24..28].try_into().expect("header length checked")) as usize;
    if payload_length > max_payload {
        return Err(Elr2ProtocolError::new("Elura payload is too large"));
    }
    Ok(())
}

fn decode_bytes(message: Bytes, max_payload: usize) -> ProtocolResult<Elr2Frame> {
    validate_header(&message, max_payload)?;
    let payload_length =
        u32::from_be_bytes(message[24..28].try_into().expect("header length checked")) as usize;
    if message.len() != ELR2_HEADER_LENGTH + payload_length {
        return Err(Elr2ProtocolError::new(
            "Elura message must contain exactly one frame",
        ));
    }
    let frame = Elr2Frame {
        kind: FrameKind::try_from(message[6])?,
        flags: message[7],
        route: u32::from_be_bytes(message[8..12].try_into().expect("header length checked")),
        request_id: u64::from_be_bytes(message[12..20].try_into().expect("header length checked")),
        sequence: u32::from_be_bytes(message[20..24].try_into().expect("header length checked")),
        payload: message.slice(ELR2_HEADER_LENGTH..),
    };
    Elr2Codec::validate(&frame, max_payload)?;
    Ok(frame)
}

fn write_frame(frame: &Elr2Frame, output: &mut BytesMut) {
    output.put_u32(ELR2_MAGIC);
    output.put_u16(ELR2_VERSION);
    output.put_u8(frame.kind as u8);
    output.put_u8(frame.flags);
    output.put_u32(frame.route);
    output.put_u64(frame.request_id);
    output.put_u32(frame.sequence);
    output.put_u32(frame.payload.len() as u32);
    output.put_slice(&frame.payload);
}

#[cfg(feature = "tokio-codec")]
fn protocol_io_error(error: Elr2ProtocolError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub account_id: i64,
    pub user_id: i64,
    pub region_id: u32,
    pub realm_id: u32,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthenticateRequest {
    pub ticket: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthenticateResponse {
    pub session_id: String,
    pub identity: Identity,
    pub reconnect: ReconnectTicketResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconnectTicketRenewalRequest {
    pub ticket: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconnectTicketResponse {
    pub ticket: String,
    pub expires_in_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

pub struct EluraProtocol;

impl EluraProtocol {
    pub fn authenticate(request_id: u64, ticket: impl Into<String>) -> ProtocolResult<Elr2Frame> {
        json_request(
            EluraRoutes::AUTHENTICATE,
            request_id,
            &AuthenticateRequest {
                ticket: ticket.into(),
            },
        )
    }

    pub fn renew_reconnect_ticket(
        request_id: u64,
        ticket: impl Into<String>,
    ) -> ProtocolResult<Elr2Frame> {
        json_request(
            EluraRoutes::RENEW_RECONNECT_TICKET,
            request_id,
            &ReconnectTicketRenewalRequest {
                ticket: ticket.into(),
            },
        )
    }

    pub fn heartbeat_response(request: &Elr2Frame) -> ProtocolResult<Elr2Frame> {
        if request.kind != FrameKind::Request
            || request.route != EluraRoutes::HEARTBEAT
            || request.sequence != 0
            || !request.payload.is_empty()
        {
            return Err(Elr2ProtocolError::new("invalid heartbeat request"));
        }
        Elr2Frame::response(request, Bytes::new())
    }

    pub fn decode_authenticate(frame: &Elr2Frame) -> ProtocolResult<AuthenticateResponse> {
        require_response(frame, EluraRoutes::AUTHENTICATE, "authentication")?;
        let response: AuthenticateResponse = decode_json(&frame.payload)?;
        validate_authentication_response(&response)?;
        Ok(response)
    }

    pub fn decode_reconnect_ticket(frame: &Elr2Frame) -> ProtocolResult<ReconnectTicketResponse> {
        require_response(
            frame,
            EluraRoutes::RENEW_RECONNECT_TICKET,
            "reconnect ticket renewal",
        )?;
        let response: ReconnectTicketResponse = decode_json(&frame.payload)?;
        validate_reconnect_ticket(&response)?;
        Ok(response)
    }

    pub fn decode_error(frame: &Elr2Frame) -> ProtocolResult<ErrorEnvelope> {
        if frame.kind != FrameKind::Error {
            return Err(Elr2ProtocolError::new("expected an error frame"));
        }
        let envelope: ErrorEnvelope = decode_json(&frame.payload)?;
        validate_error_envelope(&envelope)?;
        Ok(envelope)
    }

    pub fn validate_client_frame(
        frame: &Elr2Frame,
        authenticated: bool,
        pending_heartbeat: Option<u64>,
    ) -> ProtocolResult<()> {
        Elr2Codec::validate(frame, DEFAULT_MAX_PAYLOAD)?;
        if frame.kind == FrameKind::Response && frame.route == EluraRoutes::HEARTBEAT {
            if pending_heartbeat != Some(frame.request_id)
                || frame.sequence != 0
                || !frame.payload.is_empty()
            {
                return Err(Elr2ProtocolError::new(
                    "heartbeat response does not match an outstanding request",
                ));
            }
            return Ok(());
        }
        if frame.kind != FrameKind::Request {
            return Err(Elr2ProtocolError::new(
                "the Elura endpoint accepts request frames and heartbeat responses only",
            ));
        }
        if frame.route < EluraRoutes::FIRST_APPLICATION && frame.sequence != 0 {
            return Err(Elr2ProtocolError::new(
                "framework requests must have sequence zero",
            ));
        }
        let allowed = if authenticated {
            frame.route == EluraRoutes::HEARTBEAT
                || frame.route == EluraRoutes::RENEW_RECONNECT_TICKET
                || frame.route >= EluraRoutes::FIRST_APPLICATION
        } else {
            frame.route == EluraRoutes::AUTHENTICATE
        };
        if !allowed {
            return Err(Elr2ProtocolError::new(
                "route is not allowed in the current session state",
            ));
        }
        Ok(())
    }
}

fn json_request<T: Serialize>(route: u32, request_id: u64, value: &T) -> ProtocolResult<Elr2Frame> {
    let payload = serde_json::to_vec(value)
        .map_err(|error| Elr2ProtocolError::new(format!("invalid Elura JSON payload: {error}")))?;
    Elr2Frame::request(route, request_id, payload)
}

fn decode_json<T: for<'de> Deserialize<'de>>(payload: &[u8]) -> ProtocolResult<T> {
    serde_json::from_slice(payload)
        .map_err(|error| Elr2ProtocolError::new(format!("invalid Elura JSON payload: {error}")))
}

fn require_response(frame: &Elr2Frame, route: u32, description: &str) -> ProtocolResult<()> {
    if frame.kind != FrameKind::Response || frame.route != route {
        return Err(Elr2ProtocolError::new(format!(
            "expected a {description} response frame"
        )));
    }
    Ok(())
}

fn validate_authentication_response(response: &AuthenticateResponse) -> ProtocolResult<()> {
    if response.session_id.is_empty()
        || response.identity.account_id <= 0
        || response.identity.user_id <= 0
        || response.identity.region_id == 0
        || response.identity.realm_id == 0
        || response.identity.generation == 0
    {
        return Err(Elr2ProtocolError::new(
            "invalid authentication response fields",
        ));
    }
    validate_reconnect_ticket(&response.reconnect)
}

fn validate_reconnect_ticket(response: &ReconnectTicketResponse) -> ProtocolResult<()> {
    if response.ticket.is_empty() || response.expires_in_seconds == 0 {
        return Err(Elr2ProtocolError::new("invalid reconnect ticket fields"));
    }
    Ok(())
}

fn validate_error_envelope(envelope: &ErrorEnvelope) -> ProtocolResult<()> {
    let valid_code = !envelope.code.is_empty()
        && envelope.code.len() <= 64
        && envelope
            .code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_');
    if !valid_code || envelope.message.len() > 1024 || envelope.retry_after_ms == Some(0) {
        return Err(Elr2ProtocolError::new("invalid error envelope fields"));
    }
    Ok(())
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum SessionControlAction {
    Kick = 1,
    AccountVersionChanged = 2,
    DuplicateLogin = 3,
    ForceLogout = 4,
    ServerDraining = 5,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionControl {
    pub action: SessionControlAction,
    pub reason: String,
}

#[derive(Clone, PartialEq, Message)]
struct SessionControlWire {
    #[prost(enumeration = "SessionControlAction", tag = "1")]
    action: i32,
    #[prost(string, tag = "2")]
    reason: String,
}

pub struct SessionControlCodec;

impl SessionControlCodec {
    pub fn encode(control: &SessionControl) -> ProtocolResult<Bytes> {
        if control.reason.len() > 256 {
            return Err(Elr2ProtocolError::new(
                "Session Control reason exceeds 256 bytes",
            ));
        }
        Ok(Bytes::from(
            SessionControlWire {
                action: control.action as i32,
                reason: control.reason.clone(),
            }
            .encode_to_vec(),
        ))
    }

    pub fn decode(payload: &[u8]) -> ProtocolResult<SessionControl> {
        let wire = SessionControlWire::decode(payload).map_err(|error| {
            Elr2ProtocolError::new(format!("invalid Session Control protobuf: {error}"))
        })?;
        let action = SessionControlAction::try_from(wire.action)
            .map_err(|_| Elr2ProtocolError::new("unknown Session Control action"))?;
        if wire.reason.len() > 256 {
            return Err(Elr2ProtocolError::new(
                "Session Control reason exceeds 256 bytes",
            ));
        }
        Ok(SessionControl {
            action,
            reason: wire.reason,
        })
    }
}
