use elura_netcode::{
    InputAck, InputFrame, InputPacket, InputReceiver, InputReceiverConfig, InputSender,
    InputSenderConfig, NetcodeError, SequenceDisposition, SequenceWindow,
};

fn packet(inputs: Vec<InputFrame<&'static str>>) -> InputPacket<&'static str> {
    InputPacket {
        client_tick: 20,
        acknowledged_server_tick: 0,
        inputs,
    }
}

#[test]
fn sender_repeats_recent_unacknowledged_inputs() {
    let mut config = InputSenderConfig::default();
    config.history_capacity = 8;
    config.redundancy = 3;
    let mut sender = InputSender::new(config).unwrap();
    for tick in 10..=13 {
        sender.record(tick, tick).unwrap();
    }

    let packet = sender.packet(9);
    assert_eq!(
        packet
            .inputs
            .iter()
            .map(|frame| frame.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert_eq!(
        packet
            .inputs
            .iter()
            .map(|frame| frame.target_tick)
            .collect::<Vec<_>>(),
        vec![11, 12, 13]
    );
}

#[test]
fn acknowledgement_releases_history_and_rejects_impossible_values() {
    let mut sender = InputSender::new(InputSenderConfig::default()).unwrap();
    sender.record(10, "a").unwrap();
    sender.record(11, "b").unwrap();

    assert_eq!(
        sender
            .acknowledge(InputAck {
                server_tick: 8,
                acknowledged_sequence: 1,
            })
            .unwrap(),
        1
    );
    assert_eq!(sender.pending_len(), 1);
    assert_eq!(sender.acknowledged_server_tick(), 8);
    assert_eq!(
        sender.acknowledge(InputAck {
            server_tick: 9,
            acknowledged_sequence: 3,
        }),
        Err(NetcodeError::InvalidAcknowledgement)
    );
}

#[test]
fn sender_never_drops_unacknowledged_input_silently() {
    let mut config = InputSenderConfig::default();
    config.history_capacity = 2;
    config.redundancy = 2;
    let mut sender = InputSender::new(config).unwrap();
    sender.record(1, ()).unwrap();
    sender.record(2, ()).unwrap();
    assert_eq!(sender.record(3, ()), Err(NetcodeError::InputHistoryFull));
}

#[test]
fn sequence_window_closes_out_of_order_gaps() {
    let mut window = SequenceWindow::new(8).unwrap();
    assert_eq!(window.observe(2).unwrap(), SequenceDisposition::Accepted);
    assert_eq!(window.acknowledged(), 0);
    assert_eq!(window.pending_len(), 1);

    assert_eq!(window.observe(1).unwrap(), SequenceDisposition::Accepted);
    assert_eq!(window.acknowledged(), 2);
    assert_eq!(window.pending_len(), 0);
    assert_eq!(window.observe(1).unwrap(), SequenceDisposition::Duplicate);
}

#[test]
fn receiver_is_order_independent_and_returns_cumulative_ack() {
    let mut receiver = InputReceiver::new(InputReceiverConfig::default()).unwrap();
    let first = receiver
        .receive(
            20,
            packet(vec![InputFrame {
                sequence: 2,
                target_tick: 22,
                input: "second",
            }]),
        )
        .unwrap();
    assert_eq!(first.acknowledgement.acknowledged_sequence, 0);
    assert_eq!(first.accepted.len(), 1);

    let second = receiver
        .receive(
            21,
            packet(vec![InputFrame {
                sequence: 1,
                target_tick: 21,
                input: "first",
            }]),
        )
        .unwrap();
    assert_eq!(second.acknowledgement.acknowledged_sequence, 2);
    assert_eq!(second.accepted[0].input, "first");
}

#[test]
fn old_redundant_frames_are_ignored_after_tick_window_moves() {
    let mut config = InputReceiverConfig::default();
    config.max_past_ticks = 1;
    let mut receiver = InputReceiver::new(config).unwrap();
    let old = InputFrame {
        sequence: 1,
        target_tick: 10,
        input: "move",
    };
    receiver.receive(10, packet(vec![old.clone()])).unwrap();

    let report = receiver.receive(100, packet(vec![old])).unwrap();
    assert!(report.accepted.is_empty());
    assert_eq!(report.duplicates, 1);
}

#[test]
fn invalid_new_frame_does_not_partially_advance_receiver() {
    let mut config = InputReceiverConfig::default();
    config.max_future_ticks = 2;
    let mut receiver = InputReceiver::new(config).unwrap();
    let result = receiver.receive(
        10,
        packet(vec![
            InputFrame {
                sequence: 1,
                target_tick: 11,
                input: "valid",
            },
            InputFrame {
                sequence: 2,
                target_tick: 20,
                input: "future",
            },
        ]),
    );
    assert!(matches!(result, Err(NetcodeError::InvalidInput(_))));
    assert_eq!(receiver.acknowledged_sequence(), 0);
    assert_eq!(receiver.pending_sequences(), 0);
}

#[test]
fn one_lost_packet_is_recovered_by_the_next_redundant_packet() {
    let mut config = InputSenderConfig::default();
    config.history_capacity = 8;
    config.redundancy = 3;
    let mut sender = InputSender::new(config).unwrap();
    let mut receiver = InputReceiver::new(InputReceiverConfig::default()).unwrap();

    sender.record(11, "left").unwrap();
    let _lost = sender.packet(8);
    sender.record(12, "right").unwrap();

    let report = receiver.receive(10, sender.packet(9)).unwrap();
    assert_eq!(report.accepted.len(), 2);
    assert_eq!(report.acknowledgement.acknowledged_sequence, 2);
    sender.acknowledge(report.acknowledgement).unwrap();
    assert!(sender.is_empty());
}
