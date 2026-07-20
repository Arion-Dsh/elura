#![cfg(all(
    feature = "room",
    feature = "aoi",
    feature = "simulation",
    feature = "netcode",
    feature = "replication",
    feature = "lag-compensation",
    feature = "net-sim"
))]

use std::time::Duration;

use elura::aoi::{AoiConfig, AoiGrid, Point2};
use elura::lag_compensation::{LagCompensationConfig, LagCompensationHistory};
use elura::net_sim::{NetSimConfig, SimulatedLink};
use elura::netcode::{InputSender, InputSenderConfig, PredictionBuffer, PredictionConfig};
use elura::replication::{ReplicationConfig, ReplicationSender, VersionedState};
use elura::room::{Room, RoomConfig};
use elura::simulation::{FixedStepClock, SimulationConfig};

#[test]
fn facade_exposes_opt_in_gameplay_primitives() {
    let mut room = Room::new(1_u64, RoomConfig::default()).unwrap();
    room.join(10_u64, ()).unwrap();

    let mut aoi = AoiGrid::new(AoiConfig::default()).unwrap();
    aoi.insert(10_u64, Point2::new(0.0, 0.0)).unwrap();

    let mut simulation = FixedStepClock::new(SimulationConfig::default()).unwrap();
    let report = simulation
        .advance(Duration::from_millis(50), |_| Ok::<_, ()>(()))
        .unwrap();

    let mut inputs = InputSender::new(InputSenderConfig::default()).unwrap();
    inputs.record(3, 7_u8).unwrap();

    let mut replication =
        ReplicationSender::<u64, u8, u8>::new(ReplicationConfig::default()).unwrap();
    replication
        .update(
            1,
            [(
                10,
                VersionedState {
                    version: 1,
                    prediction_key: None,
                    state: 7,
                },
            )],
            |_, previous, current| Some(current.state - previous.state),
        )
        .unwrap();

    let prediction = PredictionBuffer::<u8, u8>::new(PredictionConfig::default()).unwrap();
    let history = LagCompensationHistory::<u8>::new(LagCompensationConfig::default()).unwrap();
    let link = SimulatedLink::<u8>::new(NetSimConfig::default()).unwrap();

    assert_eq!(room.len(), 1);
    assert_eq!(aoi.len(), 1);
    assert_eq!(report.steps, 1);
    assert_eq!(inputs.packet(1).inputs.len(), 1);
    assert_eq!(replication.packet().batches.len(), 1);
    assert!(prediction.is_empty());
    assert!(history.is_empty());
    assert_eq!(link.queued_packets(), 0);
}
