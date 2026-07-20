use elura_lag_compensation::{LagCompensationConfig, LagCompensationError, LagCompensationHistory};

fn config() -> LagCompensationConfig {
    let mut config = LagCompensationConfig::default();
    config.history_capacity = 5;
    config.max_rewind_ticks = 4;
    config
}

#[test]
fn queries_an_exact_historical_state_without_mutating_live_state() {
    let mut history = LagCompensationHistory::new(config()).unwrap();
    for tick in 1..=5 {
        history.record(tick, tick * 10).unwrap();
    }
    let hit = history
        .with_rewind(3, |context, position| {
            assert_eq!(context.current_tick, 5);
            assert_eq!(context.rewind_ticks, 2);
            *position == 30
        })
        .unwrap();
    assert!(hit);
    assert_eq!(history.current_tick(), Some(5));
}

#[test]
fn rejects_future_expired_and_missing_ticks() {
    let mut history = LagCompensationHistory::new(config()).unwrap();
    history.record(10, 10).unwrap();
    history.record(12, 12).unwrap();
    assert!(matches!(
        history.rewind(13),
        Err(LagCompensationError::FutureTick)
    ));
    assert!(matches!(
        history.rewind(11),
        Err(LagCompensationError::StateUnavailable)
    ));
    history.record(20, 20).unwrap();
    assert!(matches!(
        history.rewind(12),
        Err(LagCompensationError::RewindLimitExceeded)
    ));
    assert_eq!(history.stats().queries_rejected, 3);
}

#[test]
fn evicts_oldest_states_at_capacity() {
    let mut history = LagCompensationHistory::new(config()).unwrap();
    for tick in 1..=7 {
        history.record(tick, tick).unwrap();
    }
    assert_eq!(history.len(), 5);
    assert_eq!(history.oldest_tick(), Some(3));
    assert_eq!(history.current_tick(), Some(7));
}

#[test]
fn recorded_ticks_must_increase() {
    let mut history = LagCompensationHistory::new(config()).unwrap();
    history.record(2, ()).unwrap();
    assert_eq!(
        history.record(2, ()),
        Err(LagCompensationError::InvalidTickOrder)
    );
    assert_eq!(
        history.record(1, ()),
        Err(LagCompensationError::InvalidTickOrder)
    );
}

#[test]
fn capacity_must_cover_the_rewind_window() {
    let mut invalid = LagCompensationConfig::default();
    invalid.history_capacity = 4;
    invalid.max_rewind_ticks = 4;
    assert!(matches!(
        LagCompensationHistory::<()>::new(invalid),
        Err(LagCompensationError::InvalidConfig(_))
    ));
}
