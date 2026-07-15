use bytes::{Bytes, BytesMut};
use criterion::{Criterion, criterion_group, criterion_main};
use elura_core::protocol::{Frame, FrameCodec};
use tokio_util::codec::{Decoder, Encoder};

fn protocol(c: &mut Criterion) {
    let frame = Frame::request(100, 1, Bytes::from(vec![7; 1024])).unwrap();
    c.bench_function("elr2_encode_1k", |b| {
        b.iter(|| {
            let mut output = BytesMut::new();
            FrameCodec::default()
                .encode(frame.clone(), &mut output)
                .unwrap();
            output
        })
    });
    let mut encoded = BytesMut::new();
    FrameCodec::default().encode(frame, &mut encoded).unwrap();
    c.bench_function("elr2_decode_1k", |b| {
        b.iter(|| {
            let mut input = encoded.clone();
            FrameCodec::default().decode(&mut input).unwrap()
        })
    });
    let message = encoded.freeze();
    c.bench_function("elr2_decode_message_1k", |b| {
        b.iter(|| {
            FrameCodec::default()
                .decode_message(message.clone())
                .unwrap()
        })
    });
}

criterion_group!(benches, protocol);
criterion_main!(benches);
