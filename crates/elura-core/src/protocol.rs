use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::{Error, Result};

mod session_control;

pub use session_control::{SessionControl, SessionControlAction};

pub const MAGIC: u32 = 0x454C_5232; // ELR2
/// Stable ELR2 wire version. Changing this breaks the public protocol.
pub const VERSION: u16 = 2;
/// Identifier negotiated as the WebSocket subprotocol or QUIC ALPN.
pub const PROTOCOL_IDENTIFIER: &str = "elura.v2";
pub const HEADER_LEN: usize = 28;
pub const DEFAULT_MAX_PAYLOAD: usize = 1 << 20;
/// Client authentication. This is the first public protocol interaction.
pub const ROUTE_AUTHENTICATE: u32 = 1;
/// Transport heartbeat. Client libraries should handle this automatically.
pub const ROUTE_HEARTBEAT: u32 = 2;
pub const ROUTE_RECONNECT: u32 = 3;
pub const ROUTE_SESSION_CONTROL: u32 = 4;
pub const FIRST_APPLICATION_ROUTE: u32 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Request = 1,
    Response = 2,
    Push = 3,
    Error = 4,
}

impl TryFrom<u8> for FrameKind {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Request),
            2 => Ok(Self::Response),
            3 => Ok(Self::Push),
            4 => Ok(Self::Error),
            _ => Err(Error::InvalidFrame("unknown frame kind".into())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameKind,
    pub flags: u8,
    pub route: u32,
    pub request_id: u64,
    pub sequence: u32,
    pub payload: Bytes,
}

impl Frame {
    pub fn request(route: u32, request_id: u64, payload: impl Into<Bytes>) -> Result<Self> {
        if route == 0 {
            return Err(Error::InvalidFrame("route must be non-zero".into()));
        }
        if request_id == 0 {
            return Err(Error::InvalidFrame("request id must be non-zero".into()));
        }
        Ok(Self {
            kind: FrameKind::Request,
            flags: 0,
            route,
            request_id,
            sequence: 0,
            payload: payload.into(),
        })
    }

    pub fn response(request: &Self, payload: impl Into<Bytes>) -> Self {
        Self {
            kind: FrameKind::Response,
            flags: 0,
            route: request.route,
            request_id: request.request_id,
            sequence: request.sequence,
            payload: payload.into(),
        }
    }

    pub fn error(request: &Self, message: impl Into<Bytes>) -> Self {
        Self {
            kind: FrameKind::Error,
            flags: 0,
            route: request.route,
            request_id: request.request_id,
            sequence: request.sequence,
            payload: message.into(),
        }
    }

    pub fn validate(&self, max_payload: usize) -> Result<()> {
        if self.payload.len() > max_payload {
            return Err(Error::InvalidFrame("payload is too large".into()));
        }
        if self.flags != 0 {
            return Err(Error::InvalidFrame("unsupported frame flags".into()));
        }
        if self.route == 0 {
            return Err(Error::InvalidFrame("route must be non-zero".into()));
        }
        match self.kind {
            FrameKind::Request | FrameKind::Response | FrameKind::Error if self.request_id == 0 => {
                Err(Error::InvalidFrame("request id must be non-zero".into()))
            }
            FrameKind::Push if self.request_id != 0 => {
                Err(Error::InvalidFrame("push request id must be zero".into()))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FrameCodec {
    max_payload: usize,
}

impl FrameCodec {
    pub fn new(max_payload: usize) -> Result<Self> {
        if max_payload == 0 || max_payload > 64 << 20 {
            return Err(Error::InvalidConfig(
                "max payload must be in 1..=64MiB".into(),
            ));
        }
        Ok(Self { max_payload })
    }

    /// Decodes exactly one frame from an already-delimited binary message.
    ///
    /// This path keeps the payload backed by the original `Bytes` allocation,
    /// which avoids the copy required when a WebSocket message is staged in a
    /// `BytesMut` stream buffer.
    pub fn decode_message(&self, mut source: Bytes) -> io::Result<Frame> {
        if source.len() < HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete Elura frame",
            ));
        }
        let mut header = &source[..HEADER_LEN];
        if header.get_u32() != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Elura magic",
            ));
        }
        if header.get_u16() != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported Elura version",
            ));
        }
        let kind = FrameKind::try_from(header.get_u8())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let flags = header.get_u8();
        let route = header.get_u32();
        let request_id = header.get_u64();
        let sequence = header.get_u32();
        let payload_len = header.get_u32() as usize;
        if payload_len > self.max_payload {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Elura payload is too large",
            ));
        }
        if source.len() != HEADER_LEN + payload_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Elura message must contain exactly one frame",
            ));
        }
        source.advance(HEADER_LEN);
        let frame = Frame {
            kind,
            flags,
            route,
            request_id,
            sequence,
            payload: source,
        };
        frame
            .validate(self.max_payload)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        Ok(frame)
    }
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self {
            max_payload: DEFAULT_MAX_PAYLOAD,
        }
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, source: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        if source.len() < HEADER_LEN {
            return Ok(None);
        }
        let mut header = &source[..HEADER_LEN];
        if header.get_u32() != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Elura magic",
            ));
        }
        if header.get_u16() != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported Elura version",
            ));
        }
        let kind = FrameKind::try_from(header.get_u8())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let flags = header.get_u8();
        let route = header.get_u32();
        let request_id = header.get_u64();
        let sequence = header.get_u32();
        let payload_len = header.get_u32() as usize;
        if payload_len > self.max_payload {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Elura payload is too large",
            ));
        }
        if source.len() < HEADER_LEN + payload_len {
            source.reserve(HEADER_LEN + payload_len - source.len());
            return Ok(None);
        }
        source.advance(HEADER_LEN);
        let frame = Frame {
            kind,
            flags,
            route,
            request_id,
            sequence,
            payload: source.split_to(payload_len).freeze(),
        };
        frame
            .validate(self.max_payload)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        Ok(Some(frame))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = io::Error;

    fn encode(&mut self, frame: Frame, destination: &mut BytesMut) -> io::Result<()> {
        frame
            .validate(self.max_payload)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        destination.reserve(HEADER_LEN + frame.payload.len());
        destination.put_u32(MAGIC);
        destination.put_u16(VERSION);
        destination.put_u8(frame.kind as u8);
        destination.put_u8(frame.flags);
        destination.put_u32(frame.route);
        destination.put_u64(frame.request_id);
        destination.put_u32(frame.sequence);
        destination.put_u32(frame.payload.len() as u32);
        destination.extend_from_slice(&frame.payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framework_route_assignments_are_stable() {
        assert_eq!(ROUTE_AUTHENTICATE, 1);
        assert_eq!(ROUTE_HEARTBEAT, 2);
        assert_eq!(ROUTE_RECONNECT, 3);
        assert_eq!(ROUTE_SESSION_CONTROL, 4);
    }

    #[test]
    fn frame_round_trip() {
        let mut frame = Frame::request(100, 7, Bytes::from_static(b"hello")).unwrap();
        frame.sequence = 11;
        let mut buffer = BytesMut::new();
        FrameCodec::default()
            .encode(frame.clone(), &mut buffer)
            .unwrap();
        assert_eq!(buffer.len(), HEADER_LEN + 5);
        assert_eq!(
            FrameCodec::default().decode(&mut buffer).unwrap(),
            Some(frame)
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn elr2_v2_request_golden_vector_is_stable() {
        let expected: &[u8] = &[
            0x45, 0x4c, 0x52, 0x32, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x05,
            0x68, 0x65, 0x6c, 0x6c, 0x6f,
        ];
        let mut frame = Frame::request(100, 7, Bytes::from_static(b"hello")).unwrap();
        frame.sequence = 11;
        let mut encoded = BytesMut::new();
        FrameCodec::default()
            .encode(frame.clone(), &mut encoded)
            .unwrap();
        assert_eq!(encoded.as_ref(), expected);

        let decoded = FrameCodec::default()
            .decode_message(Bytes::copy_from_slice(expected))
            .unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn rejects_a_mismatched_wire_version_in_both_decoders() {
        let frame = Frame::request(100, 7, Bytes::from_static(b"hello")).unwrap();
        let mut encoded = BytesMut::new();
        FrameCodec::default().encode(frame, &mut encoded).unwrap();
        encoded[4..6].copy_from_slice(&3_u16.to_be_bytes());

        let message_error = FrameCodec::default()
            .decode_message(encoded.clone().freeze())
            .unwrap_err();
        assert_eq!(message_error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(message_error.to_string(), "unsupported Elura version");

        let stream_error = FrameCodec::default().decode(&mut encoded).unwrap_err();
        assert_eq!(stream_error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(stream_error.to_string(), "unsupported Elura version");
    }

    #[test]
    fn rejects_oversized_payload_before_allocation() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(MAGIC);
        buffer.put_u16(VERSION);
        buffer.put_u8(FrameKind::Request as u8);
        buffer.put_u8(0);
        buffer.put_u32(100);
        buffer.put_u64(1);
        buffer.put_u32(0);
        buffer.put_u32(1024);
        assert!(FrameCodec::new(16).unwrap().decode(&mut buffer).is_err());
    }

    #[test]
    fn rejects_reserved_route_and_unknown_flags() {
        assert!(Frame::request(0, 1, Bytes::new()).is_err());

        let mut frame = Frame::request(FIRST_APPLICATION_ROUTE, 1, Bytes::new()).unwrap();
        frame.flags = 1;
        assert!(frame.validate(DEFAULT_MAX_PAYLOAD).is_err());
    }

    #[test]
    fn decodes_an_exact_message_without_a_staging_buffer() {
        let frame = Frame::request(100, 7, Bytes::from_static(b"hello")).unwrap();
        let mut buffer = BytesMut::new();
        FrameCodec::default()
            .encode(frame.clone(), &mut buffer)
            .unwrap();
        let message = buffer.freeze();
        let expected_payload = message.slice(HEADER_LEN..);
        let decoded = FrameCodec::default().decode_message(message).unwrap();

        assert_eq!(decoded, frame);
        assert_eq!(decoded.payload.as_ptr(), expected_payload.as_ptr());
    }
}
