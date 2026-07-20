use std::time::Duration;

use elura_netcode::{InterpolationBuffer, InterpolationConfig, InterpolationInsert, NetcodeError};

fn config() -> InterpolationConfig {
    let mut config = InterpolationConfig::default();
    config.tick_rate = 10;
    config.capacity = 4;
    config.smoothing = 0.5;
    config.max_adjustment_per_sample_ticks = 0.25;
    config
}

#[test]
fn selects_states_around_the_adaptive_render_tick() {
    let mut buffer = InterpolationBuffer::new(config()).unwrap();
    buffer.insert(10, 10.0_f64, Duration::from_secs(1)).unwrap();
    buffer
        .insert(11, 11.0, Duration::from_millis(1_100))
        .unwrap();
    buffer
        .insert(12, 12.0, Duration::from_millis(1_300))
        .unwrap();

    let stats = buffer.stats();
    assert!((stats.jitter_ticks - 0.5).abs() < 1e-9);
    assert!((stats.delay_ticks - 2.25).abs() < 1e-9);
    let sample = buffer.sample(13.75).unwrap();
    assert_eq!(sample.previous_tick, 11);
    assert_eq!(sample.next_tick, 12);
    assert!((sample.alpha - 0.5).abs() < 1e-9);
    assert!(!sample.holding_newest);
}

#[test]
fn counts_late_and_duplicate_states_and_adapts_delay() {
    let mut buffer = InterpolationBuffer::new(config()).unwrap();
    buffer.insert(10, 10, Duration::from_secs(1)).unwrap();
    buffer.insert(12, 12, Duration::from_millis(1_200)).unwrap();
    assert_eq!(
        buffer.insert(11, 11, Duration::from_millis(1_250)).unwrap(),
        InterpolationInsert::Late
    );
    assert_eq!(
        buffer
            .insert(11, 111, Duration::from_millis(1_260))
            .unwrap(),
        InterpolationInsert::Replaced
    );
    let stats = buffer.stats();
    assert_eq!(stats.late_samples, 1);
    assert_eq!(stats.replaced_samples, 1);
    assert!(stats.delay_ticks > 2.0);
}

#[test]
fn buffer_is_bounded_and_holds_the_newest_state_without_extrapolating() {
    let mut buffer = InterpolationBuffer::new(config()).unwrap();
    for tick in 1..=5 {
        buffer
            .insert(tick, tick, Duration::from_millis(tick.saturating_mul(100)))
            .unwrap();
    }
    assert_eq!(buffer.len(), 4);
    let sample = buffer.sample(100.0).unwrap();
    assert_eq!(*sample.previous, 5);
    assert_eq!(*sample.next, 5);
    assert!(sample.holding_newest);
}

#[test]
fn rejects_empty_sampling_and_backwards_arrival_time() {
    let mut buffer = InterpolationBuffer::new(config()).unwrap();
    assert!(matches!(
        buffer.sample(1.0),
        Err(NetcodeError::InterpolationBufferEmpty)
    ));
    buffer.insert(1, (), Duration::from_secs(2)).unwrap();
    assert!(matches!(
        buffer.insert(2, (), Duration::from_secs(1)),
        Err(NetcodeError::InvalidSample(_))
    ));
}
