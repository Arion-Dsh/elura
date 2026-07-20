use elura_netcode::{NetcodeError, PredictionBuffer, PredictionConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct NonCloneInput(i32);

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

#[test]
fn authoritative_state_replays_only_unconfirmed_inputs() {
    let mut prediction = PredictionBuffer::new(PredictionConfig::default()).unwrap();
    prediction.record(1, 1_i32, 1_i32).unwrap();
    prediction.record(2, 2, 3).unwrap();
    prediction.record(3, 3, 6).unwrap();

    let report = prediction
        .reconcile(1, 2_i32, |state, _, input| *state += input)
        .unwrap();
    assert_eq!(report.predicted_state_at_authoritative_tick, Some(1));
    assert_eq!(report.corrected_state, 7);
    assert_eq!(report.replayed_inputs, 2);
    assert_eq!(report.current_tick, 3);
    assert_eq!(prediction.confirmed_tick(), 1);
    assert_eq!(
        prediction
            .frames()
            .map(|frame| (frame.tick, frame.predicted_state))
            .collect::<Vec<_>>(),
        vec![(2, 4), (3, 7)]
    );
}

#[test]
fn replay_limit_failure_is_transactional() {
    let mut config = PredictionConfig::default();
    config.max_replay_steps = 2;
    let mut prediction = PredictionBuffer::new(config).unwrap();
    for tick in 1..=3 {
        prediction.record(tick, 1_i32, tick as i32).unwrap();
    }
    assert_eq!(
        prediction.reconcile(0, 0, |state, _, input| *state += input),
        Err(NetcodeError::ReplayLimitExceeded)
    );
    assert_eq!(prediction.pending_len(), 3);
    assert_eq!(prediction.confirmed_tick(), 0);
}

#[test]
fn prediction_history_is_bounded_and_ticks_are_ordered() {
    let mut config = PredictionConfig::default();
    config.history_capacity = 2;
    config.max_replay_steps = 2;
    let mut prediction = PredictionBuffer::new(config).unwrap();
    prediction.record(1, (), ()).unwrap();
    assert!(matches!(
        prediction.record(1, (), ()),
        Err(NetcodeError::InvalidInput(_))
    ));
    prediction.record(2, (), ()).unwrap();
    assert_eq!(
        prediction.record(3, (), ()),
        Err(NetcodeError::PredictionHistoryFull)
    );
}

#[test]
fn authoritative_tick_beyond_local_history_confirms_everything() {
    let mut prediction = PredictionBuffer::new(PredictionConfig::default()).unwrap();
    prediction.record(1, 1_i32, 1_i32).unwrap();
    let report = prediction
        .reconcile(5, 10, |state, _, input| *state += input)
        .unwrap();
    assert_eq!(report.corrected_state, 10);
    assert_eq!(report.replayed_inputs, 0);
    assert!(prediction.is_empty());
    assert_eq!(prediction.confirmed_tick(), 5);
}

#[test]
fn reconciliation_does_not_clone_inputs_or_the_entire_history() {
    let clones = Arc::new(AtomicUsize::new(0));
    let tracked = |value| TrackedState {
        value,
        clones: Arc::clone(&clones),
    };
    let mut prediction = PredictionBuffer::new(PredictionConfig::default()).unwrap();
    prediction.record(1, NonCloneInput(1), tracked(1)).unwrap();
    prediction.record(2, NonCloneInput(2), tracked(3)).unwrap();
    prediction.record(3, NonCloneInput(3), tracked(6)).unwrap();

    let report = prediction
        .reconcile(1, tracked(2), |state, _, input| state.value += input.0)
        .unwrap();

    assert_eq!(report.corrected_state.value, 7);
    assert_eq!(clones.load(Ordering::Relaxed), 3);
}
