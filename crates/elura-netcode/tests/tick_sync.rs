use std::time::Duration;

use elura_netcode::{
    NetcodeError, TickSyncConfig, TickSyncRequest, TickSyncResponse, TickSyncSample,
    TickSynchronizer,
};

fn config() -> TickSyncConfig {
    let mut config = TickSyncConfig::default();
    config.tick_rate = 20;
    config.input_delay_ticks = 2;
    config.smoothing = 0.5;
    config.max_round_trip_time = Duration::from_secs(1);
    config.max_offset_correction_ticks = 2.0;
    config
}

#[test]
fn estimates_server_tick_from_network_rtt() {
    let mut sync = TickSynchronizer::new(config()).unwrap();
    let report = sync
        .observe(TickSyncSample {
            local_tick: 100.0,
            server_tick: 104,
            client_sent_at: Duration::from_secs(10),
            client_received_at: Duration::from_millis(10_120),
            server_processing_time: Duration::from_millis(20),
        })
        .unwrap();

    assert_eq!(report.round_trip_time, Duration::from_millis(120));
    assert_eq!(report.network_round_trip_time, Duration::from_millis(100));
    assert_eq!(report.one_way_delay, Duration::from_millis(50));
    assert!((report.estimated_server_tick - 105.0).abs() < 1e-9);
    assert!((report.offset_ticks - 5.0).abs() < 1e-9);
    assert_eq!(report.recommended_input_tick, 108);
}

#[test]
fn later_samples_are_smoothed_and_correction_is_bounded() {
    let mut sync = TickSynchronizer::new(config()).unwrap();
    sync.observe(TickSyncSample {
        local_tick: 10.0,
        server_tick: 10,
        client_sent_at: Duration::ZERO,
        client_received_at: Duration::ZERO,
        server_processing_time: Duration::ZERO,
    })
    .unwrap();

    let report = sync
        .observe(TickSyncSample {
            local_tick: 10.0,
            server_tick: 100,
            client_sent_at: Duration::from_secs(1),
            client_received_at: Duration::from_secs(1),
            server_processing_time: Duration::ZERO,
        })
        .unwrap();
    assert!((report.raw_offset_ticks - 90.0).abs() < 1e-9);
    assert!((report.offset_ticks - 1.0).abs() < 1e-9);
}

#[test]
fn invalid_sample_does_not_mutate_estimator() {
    let mut sync = TickSynchronizer::new(config()).unwrap();
    let result = sync.observe(TickSyncSample {
        local_tick: 1.0,
        server_tick: 1,
        client_sent_at: Duration::from_secs(2),
        client_received_at: Duration::from_secs(1),
        server_processing_time: Duration::ZERO,
    });
    assert!(matches!(result, Err(NetcodeError::InvalidSample(_))));
    assert_eq!(sync.samples(), 0);
    assert_eq!(sync.network_round_trip_time(), None);
}

#[test]
fn response_derives_server_processing_duration() {
    let response = TickSyncResponse {
        sequence: 7,
        client_sent_at: Duration::from_secs(4),
        server_received_at: Duration::from_secs(20),
        server_sent_at: Duration::from_millis(20_010),
        server_tick: 80,
    };
    let request = TickSyncRequest {
        sequence: 7,
        client_sent_at: Duration::from_secs(4),
    };
    let sample = response
        .sample(request, Duration::from_millis(4_110), 75.0)
        .unwrap();
    assert_eq!(sample.server_processing_time, Duration::from_millis(10));
    assert_eq!(sample.server_tick, 80);
}

#[test]
fn response_must_match_the_original_probe() {
    let response = TickSyncResponse {
        sequence: 8,
        client_sent_at: Duration::from_secs(4),
        server_received_at: Duration::from_secs(20),
        server_sent_at: Duration::from_secs(20),
        server_tick: 80,
    };
    let request = TickSyncRequest {
        sequence: 7,
        client_sent_at: Duration::from_secs(4),
    };
    assert!(matches!(
        response.sample(request, Duration::from_secs(5), 75.0),
        Err(NetcodeError::InvalidSample(_))
    ));
}
