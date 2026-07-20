use std::time::Duration;

use elura_net_sim::{NetSimConfig, SendOutcome, SimulatedLink};

#[test]
fn delays_packets_until_their_deadline() {
    let mut config = NetSimConfig::default();
    config.latency = Duration::from_millis(50);
    let mut link = SimulatedLink::new(config).unwrap();
    let outcome = link.send(Duration::ZERO, 10, "packet").unwrap();
    assert_eq!(
        outcome,
        SendOutcome::Queued {
            copies: 1,
            first_delivery: Duration::from_millis(50),
        }
    );
    assert!(link.receive(Duration::from_millis(49)).unwrap().is_empty());
    assert_eq!(
        link.receive(Duration::from_millis(50)).unwrap()[0].payload,
        "packet"
    );
}

#[test]
fn supports_total_loss_and_forced_duplication() {
    let mut loss = NetSimConfig::default();
    loss.loss_rate = 1.0;
    let mut link = SimulatedLink::new(loss).unwrap();
    assert_eq!(
        link.send(Duration::ZERO, 1, 7).unwrap(),
        SendOutcome::DroppedByLoss
    );

    let mut duplicate = NetSimConfig::default();
    duplicate.duplicate_rate = 1.0;
    let mut link = SimulatedLink::new(duplicate).unwrap();
    assert!(matches!(
        link.send(Duration::ZERO, 1, 7).unwrap(),
        SendOutcome::Queued { copies: 2, .. }
    ));
    assert_eq!(link.receive(Duration::from_nanos(1)).unwrap().len(), 2);
    assert_eq!(link.stats().duplicate_copies, 1);
}

#[test]
fn bandwidth_serializes_packets_in_time() {
    let mut config = NetSimConfig::default();
    config.bandwidth_bytes_per_second = 1_000;
    let mut link = SimulatedLink::new(config).unwrap();
    link.send(Duration::ZERO, 1_000, 1).unwrap();
    link.send(Duration::ZERO, 1_000, 2).unwrap();
    assert_eq!(link.receive(Duration::from_millis(999)).unwrap().len(), 0);
    assert_eq!(link.receive(Duration::from_secs(1)).unwrap()[0].payload, 1);
    assert_eq!(link.receive(Duration::from_secs(2)).unwrap()[0].payload, 2);
    assert_eq!(link.stats().bandwidth_delay_nanos, 1_000_000_000);
}

#[test]
fn queue_capacity_drops_whole_send_atomically() {
    let mut config = NetSimConfig::default();
    config.latency = Duration::from_secs(1);
    config.duplicate_rate = 1.0;
    config.max_queued_packets = 2;
    let mut link = SimulatedLink::new(config).unwrap();
    link.send(Duration::ZERO, 1, 1).unwrap();
    assert_eq!(link.queued_packets(), 2);
    assert_eq!(
        link.send(Duration::ZERO, 1, 2).unwrap(),
        SendOutcome::DroppedByQueue
    );
    assert_eq!(link.queued_packets(), 2);
}

#[test]
fn identical_seeds_produce_identical_jitter_and_reordering() {
    let mut config = NetSimConfig::default();
    config.latency = Duration::from_millis(100);
    config.jitter = Duration::from_millis(40);
    config.reorder_rate = 0.5;
    config.max_reorder_delay = Duration::from_millis(80);
    config.seed = 42;
    let mut first = SimulatedLink::new(config).unwrap();
    let mut second = SimulatedLink::new(config).unwrap();
    for packet in 0..20 {
        first.send(Duration::ZERO, 10, packet).unwrap();
        second.send(Duration::ZERO, 10, packet).unwrap();
    }
    let first = first.receive(Duration::from_secs(1)).unwrap();
    let second = second.receive(Duration::from_secs(1)).unwrap();
    assert_eq!(first, second);
    assert!(
        first
            .windows(2)
            .any(|pair| pair[0].payload > pair[1].payload)
    );
}

#[test]
fn monotonic_time_is_enforced() {
    let mut link = SimulatedLink::new(NetSimConfig::default()).unwrap();
    link.send(Duration::from_secs(2), 1, ()).unwrap();
    assert!(link.receive(Duration::from_secs(1)).is_err());
}

#[test]
fn extreme_sizes_and_delays_saturate_without_panicking() {
    let mut config = NetSimConfig::default();
    config.latency = Duration::MAX;
    config.jitter = Duration::MAX;
    config.bandwidth_bytes_per_second = 1;
    let mut link = SimulatedLink::new(config).unwrap();
    link.send(Duration::ZERO, usize::MAX, ()).unwrap();
    assert_eq!(link.next_delivery(), Some(Duration::MAX));
}
