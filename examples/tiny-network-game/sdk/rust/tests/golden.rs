use bytes::Bytes;
use elura_protocol::{
    ELR2_VERSION, Elr2Codec, Elr2Frame, EluraProtocol, EluraRoutes, FrameKind, Identity,
    ReconnectTicketResponse, SessionControl, SessionControlAction, SessionControlCodec,
};
use tokio_util::codec::{Decoder, Encoder};

#[test]
fn elr2_v2_golden_vector_and_stream_round_trip() {
    assert_eq!(ELR2_VERSION, 2);
    let request = Elr2Frame::request_with_sequence(100, 7, 11, "hello").unwrap();
    let expected = [
        0x45, 0x4c, 0x52, 0x32, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x05, 0x68, 0x65,
        0x6c, 0x6c, 0x6f,
    ];
    assert_eq!(Elr2Codec::encode(&request).unwrap().as_ref(), expected);
    assert_eq!(Elr2Codec::decode(&expected).unwrap(), request);

    let mut encoded = bytes::BytesMut::new();
    Elr2Codec::default()
        .encode(request.clone(), &mut encoded)
        .unwrap();
    let split = encoded.split_off(10);
    let mut decoder = Elr2Codec::default();
    assert!(decoder.decode(&mut encoded).unwrap().is_none());
    encoded.extend_from_slice(&split);
    assert_eq!(decoder.decode(&mut encoded).unwrap(), Some(request));
}

#[test]
fn elura_protocol_payloads_and_session_control_round_trip() {
    let auth = EluraProtocol::authenticate(8, "ticket-value").unwrap();
    assert_eq!(auth.route, EluraRoutes::AUTHENTICATE);
    assert_eq!(
        auth.payload,
        Bytes::from_static(br#"{"ticket":"ticket-value"}"#)
    );

    let response = Elr2Frame::response(
        &auth,
        serde_json::to_vec(&serde_json::json!({
            "session_id": "session-1",
            "identity": {
                "account_id": 1,
                "user_id": 2,
                "region_id": 3,
                "realm_id": 4,
                "generation": 5
            },
            "reconnect": {
                "ticket": "next-ticket",
                "expires_in_seconds": 60
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let authenticated = EluraProtocol::decode_authenticate(&response).unwrap();
    assert_eq!(
        authenticated.identity,
        Identity {
            account_id: 1,
            user_id: 2,
            region_id: 3,
            realm_id: 4,
            generation: 5,
        }
    );
    assert_eq!(
        authenticated.reconnect,
        ReconnectTicketResponse {
            ticket: "next-ticket".into(),
            expires_in_seconds: 60,
        }
    );

    let heartbeat = Elr2Frame::request(EluraRoutes::HEARTBEAT, 9, Bytes::new()).unwrap();
    let reply = EluraProtocol::heartbeat_response(&heartbeat).unwrap();
    assert_eq!(reply.kind, FrameKind::Response);
    assert_eq!(reply.request_id, heartbeat.request_id);

    let control = SessionControl {
        action: SessionControlAction::AccountVersionChanged,
        reason: "credentials rotated".into(),
    };
    let encoded = SessionControlCodec::encode(&control).unwrap();
    assert_eq!(
        encoded.as_ref(),
        [
            0x08, 0x02, 0x12, 0x13, b'c', b'r', b'e', b'd', b'e', b'n', b't', b'i', b'a', b'l',
            b's', b' ', b'r', b'o', b't', b'a', b't', b'e', b'd',
        ]
    );
    assert_eq!(SessionControlCodec::decode(&encoded).unwrap(), control);
}
