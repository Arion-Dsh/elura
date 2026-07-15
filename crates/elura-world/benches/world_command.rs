use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use elura_core::gateway_world::{GatewayWorldCommand, WorldCommand};
use elura_core::session::Identity;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct JsonWorldCommand {
    authorization: Option<String>,
    identity: Identity,
    session_id: String,
    trace_id: String,
    request_id: u64,
    payload: Vec<u8>,
    shard_id: Option<u32>,
    owner_id: Option<String>,
    owner_epoch: Option<u64>,
}

impl From<&WorldCommand> for JsonWorldCommand {
    fn from(command: &WorldCommand) -> Self {
        Self {
            authorization: command.authorization.clone(),
            identity: command.identity.clone(),
            session_id: command.session_id.clone(),
            trace_id: command.trace_id.clone(),
            request_id: command.request_id,
            payload: command.payload.to_vec(),
            shard_id: command.shard_id,
            owner_id: command.owner_id.clone(),
            owner_epoch: command.owner_epoch,
        }
    }
}

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
        session_id: "5c65203e-a3d0-4ec0-b727-bec61f9d47eb".into(),
        trace_id: "0123456789abcdef0123456789abcdef".into(),
        request_id: 17,
        payload: Bytes::from(vec![7; 1024]),
        shard_id: Some(11),
        owner_id: Some("world-a".into()),
        owner_epoch: Some(13),
        timeout: std::time::Duration::from_secs(5),
    }
}

fn world_command(c: &mut Criterion) {
    let command = command();
    let protobuf = GatewayWorldCommand::from(command.clone());
    let wire = protobuf.encode_frame_payload();
    let json_command = JsonWorldCommand::from(&command);
    let json = serde_json::to_vec(&json_command).unwrap();
    let mut group = c.benchmark_group("world_command_1k");
    group.throughput(Throughput::Bytes(1024));
    group.bench_function("protobuf_encode", |b| {
        b.iter(|| black_box(&protobuf).encode_frame_payload())
    });
    group.bench_function("json_encode", |b| {
        b.iter(|| serde_json::to_vec(black_box(&json_command)).unwrap())
    });
    group.bench_function("protobuf_decode", |b| {
        b.iter(|| {
            let decoded =
                GatewayWorldCommand::decode_frame_payload(black_box(wire.clone())).unwrap();
            WorldCommand::try_from(decoded).unwrap()
        })
    });
    group.bench_function("json_decode", |b| {
        b.iter(|| serde_json::from_slice::<JsonWorldCommand>(black_box(&json)).unwrap())
    });
    group.finish();
}

criterion_group!(benches, world_command);
criterion_main!(benches);
