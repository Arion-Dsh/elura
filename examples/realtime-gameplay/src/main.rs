use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;
use std::time::Duration;

use elura::gameplay::aoi::{AoiConfig, AoiGrid, Point2};
use elura::gameplay::lag_compensation::{LagCompensationConfig, LagCompensationHistory};
use elura::gameplay::net_sim::{NetSimConfig, SendOutcome, SimulatedLink};
use elura::gameplay::netcode::{
    InputReceiver, InputReceiverConfig, InputSender, InputSenderConfig, InterpolationBuffer,
    InterpolationConfig, PredictedEntityConfig, PredictedEntityMatcher, PredictionBuffer,
    PredictionConfig, PredictionKeyGenerator, TickSyncConfig, TickSyncRequest, TickSyncResponse,
    TickSynchronizer,
};
use elura::gameplay::replication::{
    ReplicationConfig, ReplicationReceiver, ReplicationSender, VersionedState,
};
use elura::gameplay::room::{Room, RoomConfig};
use elura::gameplay::simulation::{FixedStepClock, SimulationConfig};

type ExampleResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MoveInput {
    dx: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlayerState {
    x: i32,
}

fn simulate(state: &mut PlayerState, input: &MoveInput) {
    state.x += input.dx.clamp(-1, 1);
}

fn room_lifecycle() -> ExampleResult {
    let mut room = Room::<_, u64, ()>::new("arena-1", RoomConfig::default())?;
    room.join(1, ())?;
    room.join(2, ())?;
    room.set_ready(&1, true)?;
    room.set_ready(&2, true)?;
    room.start()?;

    assert_eq!(room.leader(), Some(&1));
    assert_eq!(room.len(), 2);
    Ok(())
}

fn tick_synchronization() -> ExampleResult {
    let request = TickSyncRequest {
        sequence: 1,
        client_sent_at: Duration::ZERO,
    };
    let response = TickSyncResponse {
        sequence: request.sequence,
        client_sent_at: request.client_sent_at,
        server_received_at: Duration::from_millis(100),
        server_sent_at: Duration::from_millis(101),
        server_tick: 12,
    };
    let sample = response.sample(request, Duration::from_millis(202), 10.0)?;
    let mut synchronizer = TickSynchronizer::new(TickSyncConfig::default())?;
    let report = synchronizer.observe(sample)?;

    assert!(report.estimated_server_tick > 12.0);
    assert!(report.recommended_input_tick > report.estimated_server_tick as u64);
    Ok(())
}

fn redundant_input_and_fixed_tick() -> ExampleResult {
    let mut client = InputSender::new(InputSenderConfig::default())?;
    client.record(1, MoveInput { dx: 1 })?;
    let _lost_packet = client.packet(1);

    client.record(2, MoveInput { dx: 1 })?;
    let recovery_packet = client.packet(2);
    assert_eq!(recovery_packet.inputs.len(), 2);

    let mut server = InputReceiver::new(InputReceiverConfig::default())?;
    let received = server.receive(2, recovery_packet)?;
    client.acknowledge(received.acknowledgement)?;

    let accepted = received.accepted;
    let mut state = PlayerState { x: 0 };
    let mut clock = FixedStepClock::new(SimulationConfig::default())?;
    clock.advance::<Infallible, _>(Duration::from_millis(100), |step| {
        for frame in accepted
            .iter()
            .filter(|frame| u128::from(frame.target_tick) == step.tick)
        {
            simulate(&mut state, &frame.input);
        }
        Ok(())
    })?;

    assert_eq!(state.x, 2);
    assert!(client.is_empty());
    Ok(())
}

fn visible_states(
    aoi: &AoiGrid<u64>,
    states: &BTreeMap<u64, VersionedState<PlayerState>>,
    observer: u64,
    radius: f64,
) -> ExampleResult<Vec<(u64, VersionedState<PlayerState>)>> {
    let mut visible = aoi.visible_to(&observer, radius)?;
    visible.push(observer);
    visible.sort_unstable();
    Ok(visible
        .into_iter()
        .filter_map(|entity| states.get(&entity).cloned().map(|state| (entity, state)))
        .collect())
}

fn aoi_and_replication() -> ExampleResult {
    let mut aoi = AoiGrid::new(AoiConfig::default())?;
    aoi.insert(1, Point2::new(0.0, 0.0))?;
    aoi.insert(2, Point2::new(2.0, 0.0))?;
    aoi.insert(3, Point2::new(20.0, 0.0))?;

    let versioned = |version, x| VersionedState {
        version,
        prediction_key: None,
        state: PlayerState { x },
    };
    let mut states = BTreeMap::from([
        (1, versioned(1, 0)),
        (2, versioned(1, 2)),
        (3, versioned(1, 20)),
    ]);
    let config = ReplicationConfig::default();
    let mut sender = ReplicationSender::<u64, PlayerState, i32>::new(config)?;
    let mut receiver = ReplicationReceiver::<u64, PlayerState, i32>::new(config)?;

    sender.update(1, visible_states(&aoi, &states, 1, 5.0)?, |_, old, new| {
        Some(new.state.x - old.state.x)
    })?;
    let report = receiver.receive(sender.packet(), |_, old, delta| {
        Some(PlayerState { x: old.x + delta })
    })?;
    sender.acknowledge(report.acknowledgement)?;
    assert_eq!(receiver.len(), 2);
    assert!(receiver.entity(&3).is_none());

    aoi.move_entity(&2, Point2::new(10.0, 0.0))?;
    states.insert(1, versioned(2, 1));
    sender.update(2, visible_states(&aoi, &states, 1, 5.0)?, |_, old, new| {
        Some(new.state.x - old.state.x)
    })?;
    let report = receiver.receive(sender.packet(), |_, old, delta| {
        Some(PlayerState { x: old.x + delta })
    })?;
    sender.acknowledge(report.acknowledgement)?;

    assert_eq!(receiver.entity(&1).unwrap().state.x, 1);
    assert!(receiver.entity(&2).is_none());
    Ok(())
}

fn prediction_and_interpolation() -> ExampleResult {
    let mut prediction = PredictionBuffer::new(PredictionConfig::default())?;
    prediction.record(1, MoveInput { dx: 1 }, PlayerState { x: 1 })?;
    prediction.record(2, MoveInput { dx: 1 }, PlayerState { x: 2 })?;
    let corrected = prediction.reconcile(1, PlayerState { x: 0 }, |state, _, input| {
        simulate(state, input);
    })?;
    assert_eq!(corrected.corrected_state.x, 1);
    assert_eq!(corrected.replayed_inputs, 1);

    let mut interpolation_config = InterpolationConfig::default();
    interpolation_config.base_delay_ticks = 1.0;
    interpolation_config.min_delay_ticks = 1.0;
    interpolation_config.max_delay_ticks = 1.0;
    let mut interpolation = InterpolationBuffer::new(interpolation_config)?;
    interpolation.insert(10, PlayerState { x: 10 }, Duration::ZERO)?;
    interpolation.insert(11, PlayerState { x: 20 }, Duration::from_millis(33))?;
    let sample = interpolation.sample(11.5)?;
    let rendered_x =
        sample.previous.x as f64 + f64::from(sample.next.x - sample.previous.x) * sample.alpha;

    assert_eq!(sample.previous_tick, 10);
    assert_eq!(sample.next_tick, 11);
    assert!((rendered_x - 15.0).abs() < f64::EPSILON);
    Ok(())
}

fn predicted_entity_matching() -> ExampleResult {
    let mut keys = PredictionKeyGenerator::default();
    let key = keys.generate()?;
    let mut matcher = PredictedEntityMatcher::new(PredictedEntityConfig::default())?;
    matcher.register(key, -1_i64, 10)?;

    let matched = matcher.resolve(key, 9001, 12)?.unwrap();
    assert_eq!(matched.temporary_entity, -1);
    assert_eq!(matched.authoritative_entity, 9001);
    assert_eq!(matched.age_ticks, 2);
    Ok(())
}

fn lag_compensated_query() -> ExampleResult {
    let mut history = LagCompensationHistory::new(LagCompensationConfig::default())?;
    history.record(1, PlayerState { x: 10 })?;
    history.record(2, PlayerState { x: 20 })?;
    history.record(3, PlayerState { x: 30 })?;

    let historical_hit = history.with_rewind(2, |context, target| {
        context.rewind_ticks == 1 && target.x == 20
    })?;
    assert!(historical_hit);
    assert_eq!(history.current_tick(), Some(3));
    Ok(())
}

fn adverse_network_simulation() -> ExampleResult {
    let mut config = NetSimConfig::default();
    config.latency = Duration::from_millis(50);
    let mut link = SimulatedLink::new(config)?;
    link.send(Duration::ZERO, 128, "replication packet")?;
    assert!(link.receive(Duration::from_millis(49))?.is_empty());
    assert_eq!(link.receive(Duration::from_millis(50))?.len(), 1);

    let mut loss = NetSimConfig::default();
    loss.loss_rate = 1.0;
    let mut lossy_link = SimulatedLink::new(loss)?;
    assert_eq!(
        lossy_link.send(Duration::ZERO, 64, "input packet")?,
        SendOutcome::DroppedByLoss
    );
    Ok(())
}

fn run() -> ExampleResult {
    room_lifecycle()?;
    tick_synchronization()?;
    redundant_input_and_fixed_tick()?;
    aoi_and_replication()?;
    prediction_and_interpolation()?;
    predicted_entity_matching()?;
    lag_compensated_query()?;
    adverse_network_simulation()?;
    println!("all realtime gameplay primitive examples passed");
    Ok(())
}

fn main() -> ExampleResult {
    run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walkthrough_runs_end_to_end() {
        run().unwrap();
    }
}
