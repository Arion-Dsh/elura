use bytes::{BufMut, BytesMut};
use elura_core::protocol::{FrameCodec, HEADER_LEN};
use rand::{Rng, SeedableRng, rngs::StdRng};
use tokio_util::codec::Decoder;

#[test]
fn arbitrary_frames_never_panic_or_allocate_past_limit() {
    let mut random = StdRng::seed_from_u64(0x52535432);
    for length in 0..=(HEADER_LEN + 512) {
        let mut data = vec![0; length];
        random.fill_bytes(&mut data);
        let mut input = BytesMut::from(data.as_slice());
        let _ = FrameCodec::new(256).unwrap().decode(&mut input);
    }
    let mut oversized = BytesMut::new();
    oversized.put_slice(&[0x52, 0x53, 0x54, 0x32, 0, 2, 1, 0]);
    oversized.put_u32(100);
    oversized.put_u64(1);
    oversized.put_u32(0);
    oversized.put_u32(u32::MAX);
    assert!(
        FrameCodec::new(256)
            .unwrap()
            .decode(&mut oversized)
            .is_err()
    );
}
