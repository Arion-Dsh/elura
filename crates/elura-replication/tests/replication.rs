use elura_aoi::{AoiConfig, AoiGrid, Point2};
use elura_netcode::PredictionKey;
use elura_replication::{
    ReplicationAck, ReplicationBatch, ReplicationConfig, ReplicationError, ReplicationEvent,
    ReplicationPacket, ReplicationReceiver, ReplicationSender, VersionedState,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn state(version: u64, value: i32) -> VersionedState<i32> {
    VersionedState {
        version,
        prediction_key: None,
        state: value,
    }
}

fn encode_delta(
    _: &u64,
    previous: &VersionedState<i32>,
    current: &VersionedState<i32>,
) -> Option<i32> {
    Some(current.state - previous.state)
}

fn apply_delta(_: &u64, previous: &i32, delta: &i32) -> Option<i32> {
    Some(previous + delta)
}

fn acknowledge(
    sender: &mut ReplicationSender<u64, i32, i32>,
    receiver: &mut ReplicationReceiver<u64, i32, i32>,
) {
    let report = receiver.receive(sender.packet(), apply_delta).unwrap();
    sender.acknowledge(report.acknowledgement).unwrap();
}

#[test]
fn converts_visible_entities_into_spawn_update_and_despawn() {
    let config = ReplicationConfig::default();
    let mut sender = ReplicationSender::new(config).unwrap();
    let mut receiver = ReplicationReceiver::new(config).unwrap();

    assert_eq!(
        sender
            .update(1, [(1, state(1, 10)), (2, state(1, 20))], encode_delta)
            .unwrap(),
        1
    );
    acknowledge(&mut sender, &mut receiver);
    assert_eq!(receiver.entity(&1).unwrap().state, 10);
    assert_eq!(receiver.entity(&2).unwrap().state, 20);

    sender.update(2, [(2, state(2, 25))], encode_delta).unwrap();
    let packet = sender.packet();
    assert!(matches!(
        packet.batches[0].events[0],
        ReplicationEvent::Despawn { entity: 1 }
    ));
    assert!(matches!(
        packet.batches[0].events[1],
        ReplicationEvent::Update {
            entity: 2,
            base_version: 1,
            version: 2,
            delta: 5,
        }
    ));
    acknowledge(&mut sender, &mut receiver);
    assert!(receiver.entity(&1).is_none());
    assert_eq!(receiver.entity(&2).unwrap(), &state(2, 25));
}

#[test]
fn composes_aoi_queries_with_entity_state_resolution() {
    let mut aoi = AoiGrid::new(AoiConfig::default()).unwrap();
    aoi.insert(1, Point2::new(0.0, 0.0)).unwrap();
    aoi.insert(2, Point2::new(2.0, 0.0)).unwrap();
    aoi.insert(3, Point2::new(20.0, 0.0)).unwrap();
    let states = [(1, state(1, 10)), (2, state(1, 20)), (3, state(1, 30))]
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    let visible = aoi
        .visible_to(&1, 5.0)
        .unwrap()
        .into_iter()
        .map(|entity| (entity, states[&entity].clone()));

    let mut sender = ReplicationSender::<u64, i32, i32>::new(ReplicationConfig::default()).unwrap();
    sender.update(1, visible, encode_delta).unwrap();
    let spawned = sender.packet().batches[0]
        .events
        .iter()
        .map(|event| *event.entity())
        .collect::<Vec<_>>();
    assert_eq!(spawned, vec![2]);
}

#[test]
fn falls_back_to_keyframe_and_can_force_resynchronization() {
    let config = ReplicationConfig::default();
    let mut sender = ReplicationSender::new(config).unwrap();
    let mut receiver = ReplicationReceiver::new(config).unwrap();
    sender.update(1, [(1, state(1, 10))], encode_delta).unwrap();
    acknowledge(&mut sender, &mut receiver);

    sender
        .update(2, [(1, state(2, 20))], |_, _, _| None)
        .unwrap();
    assert!(matches!(
        sender.packet().batches[0].events[0],
        ReplicationEvent::Keyframe {
            entity: 1,
            version: 2,
            state: 20,
            ..
        }
    ));
    acknowledge(&mut sender, &mut receiver);

    sender.force_keyframes();
    sender.update(3, [(1, state(2, 20))], encode_delta).unwrap();
    assert!(matches!(
        sender.packet().batches[0].events[0],
        ReplicationEvent::Keyframe { entity: 1, .. }
    ));
}

#[test]
fn event_budget_splits_one_tick_into_ordered_batches() {
    let mut config = ReplicationConfig::default();
    config.max_events_per_batch = 2;
    config.redundancy = 3;
    let mut sender = ReplicationSender::<u64, i32, i32>::new(config).unwrap();
    let visible = (1..=5).map(|entity| (entity, state(1, entity as i32)));
    assert_eq!(sender.update(1, visible, encode_delta).unwrap(), 3);
    let packet = sender.packet();
    assert_eq!(
        packet
            .batches
            .iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        packet
            .batches
            .iter()
            .map(|batch| batch.events.len())
            .collect::<Vec<_>>(),
        vec![2, 2, 1]
    );
}

#[test]
fn receiver_buffers_out_of_order_batches_until_gap_closes() {
    let mut config = ReplicationConfig::default();
    config.max_events_per_batch = 1;
    let mut sender = ReplicationSender::new(config).unwrap();
    sender
        .update(1, [(1, state(1, 10)), (2, state(1, 20))], encode_delta)
        .unwrap();
    let batches = sender.packet().batches;
    let mut receiver = ReplicationReceiver::new(config).unwrap();

    let second = receiver
        .receive(
            ReplicationPacket {
                batches: vec![batches[1].clone()],
            },
            apply_delta,
        )
        .unwrap();
    assert_eq!(second.applied_batches, 0);
    assert_eq!(second.acknowledgement.acknowledged_sequence, 0);
    assert_eq!(receiver.pending_batches(), 1);

    let first = receiver
        .receive(
            ReplicationPacket {
                batches: vec![batches[0].clone()],
            },
            apply_delta,
        )
        .unwrap();
    assert_eq!(first.applied_batches, 2);
    assert_eq!(first.acknowledgement.acknowledged_sequence, 2);
    assert_eq!(receiver.len(), 2);
}

#[test]
fn packet_prioritizes_oldest_ack_gap_and_latest_batches() {
    let mut config = ReplicationConfig::default();
    config.max_events_per_batch = 1;
    config.redundancy = 3;
    let mut sender = ReplicationSender::<u64, i32, i32>::new(config).unwrap();
    sender
        .update(
            1,
            (1..=5).map(|entity| (entity, state(1, entity as i32))),
            encode_delta,
        )
        .unwrap();
    assert_eq!(
        sender
            .packet()
            .batches
            .iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>(),
        vec![1, 4, 5]
    );
}

#[test]
fn redundant_resend_recovers_a_missing_oldest_batch() {
    let mut config = ReplicationConfig::default();
    config.max_events_per_batch = 1;
    config.redundancy = 5;
    let mut sender = ReplicationSender::<u64, i32, i32>::new(config).unwrap();
    sender
        .update(
            1,
            (1..=5).map(|entity| (entity, state(1, entity as i32))),
            encode_delta,
        )
        .unwrap();
    let batches = sender.packet().batches;
    let mut receiver = ReplicationReceiver::new(config).unwrap();

    for batch in batches.iter().skip(1) {
        receiver
            .receive(
                ReplicationPacket {
                    batches: vec![batch.clone()],
                },
                apply_delta,
            )
            .unwrap();
    }
    assert_eq!(receiver.acknowledged_sequence(), 0);
    assert_eq!(receiver.pending_batches(), 4);

    let recovered = receiver.receive(sender.packet(), apply_delta).unwrap();
    assert_eq!(recovered.applied_batches, 5);
    assert_eq!(recovered.duplicate_batches, 4);
    assert_eq!(recovered.acknowledgement.acknowledged_sequence, 5);
    assert_eq!(receiver.len(), 5);
}

#[test]
fn history_capacity_failure_is_transactional() {
    let mut config = ReplicationConfig::default();
    config.history_capacity = 2;
    config.redundancy = 2;
    config.max_events_per_batch = 1;
    let mut sender = ReplicationSender::<u64, i32, i32>::new(config).unwrap();
    assert_eq!(
        sender.update(
            1,
            (1..=3).map(|entity| (entity, state(1, entity as i32))),
            encode_delta,
        ),
        Err(ReplicationError::HistoryFull)
    );
    assert_eq!(sender.pending_batches(), 0);
    assert_eq!(sender.projected_entities(), 0);
    assert_eq!(
        sender.update(1, [(1, state(1, 1))], encode_delta).unwrap(),
        1
    );
}

#[test]
fn acknowledgement_checks_sequence_and_applied_tick() {
    let mut sender = ReplicationSender::<u64, i32, i32>::new(ReplicationConfig::default()).unwrap();
    sender.update(7, [(1, state(1, 1))], encode_delta).unwrap();
    assert_eq!(
        sender.acknowledge(ReplicationAck {
            acknowledged_sequence: 1,
            applied_tick: 6,
        }),
        Err(ReplicationError::InvalidAcknowledgement)
    );
    assert_eq!(sender.pending_batches(), 1);
}

#[test]
fn invalid_delta_is_transactional() {
    let config = ReplicationConfig::default();
    let mut sender = ReplicationSender::new(config).unwrap();
    let mut receiver = ReplicationReceiver::new(config).unwrap();
    sender.update(1, [(1, state(1, 10))], encode_delta).unwrap();
    acknowledge(&mut sender, &mut receiver);

    let packet = ReplicationPacket {
        batches: vec![ReplicationBatch {
            sequence: 2,
            tick: 2,
            events: vec![ReplicationEvent::Update {
                entity: 1,
                base_version: 99,
                version: 100,
                delta: 1,
            }],
        }],
    };
    assert_eq!(
        receiver.receive(packet, apply_delta),
        Err(ReplicationError::BaselineMismatch)
    );
    assert_eq!(receiver.acknowledged_sequence(), 1);
    assert_eq!(receiver.entity(&1).unwrap(), &state(1, 10));
}

#[test]
fn version_regression_is_rejected_without_mutation() {
    let mut sender = ReplicationSender::<u64, i32, i32>::new(ReplicationConfig::default()).unwrap();
    sender.update(1, [(1, state(2, 20))], encode_delta).unwrap();
    assert_eq!(
        sender.update(2, [(1, state(1, 10))], encode_delta),
        Err(ReplicationError::VersionRegression)
    );
    assert_eq!(sender.projected_entities(), 1);
}

#[test]
fn reorder_window_must_cover_sender_history() {
    let mut config = ReplicationConfig::default();
    config.history_capacity = 300;
    config.reorder_window = 256;
    assert!(matches!(
        ReplicationSender::<u64, i32, i32>::new(config),
        Err(ReplicationError::InvalidConfig(_))
    ));
}

#[test]
fn malformed_duplicate_batch_cannot_bypass_packet_limits() {
    let config = ReplicationConfig::default();
    let mut sender = ReplicationSender::new(config).unwrap();
    let mut receiver = ReplicationReceiver::new(config).unwrap();
    sender.update(1, [(1, state(1, 10))], encode_delta).unwrap();
    acknowledge(&mut sender, &mut receiver);

    let malformed = ReplicationPacket {
        batches: vec![ReplicationBatch {
            sequence: 1,
            tick: 0,
            events: Vec::<ReplicationEvent<u64, i32, i32>>::new(),
        }],
    };
    assert!(matches!(
        receiver.receive(malformed, apply_delta),
        Err(ReplicationError::InvalidPacket(_))
    ));
    assert_eq!(receiver.acknowledged_sequence(), 1);
}

#[test]
fn authoritative_spawn_carries_the_client_prediction_key() {
    let config = ReplicationConfig::default();
    let mut sender = ReplicationSender::<u64, i32, i32>::new(config).unwrap();
    let mut receiver = ReplicationReceiver::new(config).unwrap();
    sender
        .update(
            1,
            [(
                9001,
                VersionedState {
                    version: 1,
                    prediction_key: Some(PredictionKey(42)),
                    state: 10,
                },
            )],
            encode_delta,
        )
        .unwrap();
    assert!(matches!(
        sender.packet().batches[0].events[0],
        ReplicationEvent::Spawn {
            prediction_key: Some(PredictionKey(42)),
            ..
        }
    ));
    acknowledge(&mut sender, &mut receiver);
    assert_eq!(
        receiver.entity(&9001).unwrap().prediction_key,
        Some(PredictionKey(42))
    );
}

#[test]
fn receiver_only_clones_states_changed_by_the_packet() {
    #[derive(Debug)]
    struct TrackedState {
        value: i32,
        clones: Arc<AtomicUsize>,
    }

    impl Clone for TrackedState {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::Relaxed);
            Self {
                value: self.value,
                clones: Arc::clone(&self.clones),
            }
        }
    }

    let clones = Arc::new(AtomicUsize::new(0));
    let state = |version, value| VersionedState {
        version,
        prediction_key: None,
        state: TrackedState {
            value,
            clones: Arc::clone(&clones),
        },
    };
    let config = ReplicationConfig::default();
    let mut sender = ReplicationSender::<u64, TrackedState, i32>::new(config).unwrap();
    let mut receiver = ReplicationReceiver::<u64, TrackedState, i32>::new(config).unwrap();

    sender
        .update(
            1,
            (0..512).map(|entity| (entity, state(1, 0))),
            |_, _, _| None,
        )
        .unwrap();
    let report = receiver.receive(sender.packet(), |_, _, _| None).unwrap();
    sender.acknowledge(report.acknowledgement).unwrap();

    sender
        .update(
            2,
            (0..512).map(|entity| {
                if entity == 0 {
                    (entity, state(2, 1))
                } else {
                    (entity, state(1, 0))
                }
            }),
            |_, previous, current| Some(current.state.value - previous.state.value),
        )
        .unwrap();
    let packet = sender.packet();
    clones.store(0, Ordering::Relaxed);
    receiver
        .receive(packet, |_, previous, delta| {
            Some(TrackedState {
                value: previous.value + delta,
                clones: Arc::clone(&previous.clones),
            })
        })
        .unwrap();

    assert_eq!(receiver.len(), 512);
    assert_eq!(receiver.entity(&0).unwrap().state.value, 1);
    assert_eq!(clones.load(Ordering::Relaxed), 0);
}

#[test]
fn failure_after_staged_and_buffered_batches_is_transactional() {
    let config = ReplicationConfig::default();
    let mut receiver = ReplicationReceiver::<u64, i32, i32>::new(config).unwrap();
    receiver
        .receive(
            ReplicationPacket {
                batches: vec![ReplicationBatch {
                    sequence: 2,
                    tick: 2,
                    events: vec![ReplicationEvent::Spawn {
                        entity: 2,
                        version: 1,
                        prediction_key: None,
                        state: 20,
                    }],
                }],
            },
            apply_delta,
        )
        .unwrap();

    let result = receiver.receive(
        ReplicationPacket {
            batches: vec![
                ReplicationBatch {
                    sequence: 1,
                    tick: 1,
                    events: vec![ReplicationEvent::Spawn {
                        entity: 1,
                        version: 1,
                        prediction_key: None,
                        state: 10,
                    }],
                },
                ReplicationBatch {
                    sequence: 3,
                    tick: 3,
                    events: vec![ReplicationEvent::Update {
                        entity: 1,
                        base_version: 99,
                        version: 100,
                        delta: 1,
                    }],
                },
            ],
        },
        apply_delta,
    );

    assert_eq!(result, Err(ReplicationError::BaselineMismatch));
    assert_eq!(receiver.acknowledged_sequence(), 0);
    assert_eq!(receiver.pending_batches(), 1);
    assert!(receiver.is_empty());
}
