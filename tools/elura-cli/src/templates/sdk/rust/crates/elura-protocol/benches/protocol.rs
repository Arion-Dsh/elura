use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use elura_protocol::{Elr2Codec, Elr2Frame};

fn protocol_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("elr2_frame");

    for payload_size in [64_usize, 1024, 64 * 1024] {
        let frame = Elr2Frame::request(100, 42, Bytes::from(vec![0x5a; payload_size])).unwrap();
        let encoded = Elr2Codec::encode(&frame).unwrap();
        group.throughput(Throughput::Bytes(payload_size as u64));

        group.bench_with_input(
            BenchmarkId::new("encode", payload_size),
            &frame,
            |benchmark, frame| {
                benchmark.iter(|| Elr2Codec::encode(frame).unwrap());
            },
        );
        group.bench_with_input(
            BenchmarkId::new("decode", payload_size),
            &encoded,
            |benchmark, encoded| {
                benchmark.iter(|| Elr2Codec::decode(encoded).unwrap());
            },
        );
    }

    group.finish();
}

criterion_group!(benches, protocol_benchmarks);
criterion_main!(benches);
