use std::time::Duration;

use elura_simulation::{
    FixedStepClock, Simulation, SimulationConfig, SimulationError, SimulationStep,
};

fn config() -> SimulationConfig {
    let mut config = SimulationConfig::default();
    config.step = Duration::from_millis(10);
    config.max_steps_per_update = 4;
    config.max_accumulated_time = Duration::from_millis(100);
    config
}

#[test]
fn converts_irregular_updates_into_fixed_steps() {
    let mut clock = FixedStepClock::new(config()).unwrap();
    let mut steps = Vec::new();
    let report = clock
        .advance(Duration::from_millis(25), |step| {
            steps.push(step);
            Ok::<_, ()>(())
        })
        .unwrap();

    assert_eq!(report.steps, 2);
    assert_eq!(report.tick, 2);
    assert_eq!(report.simulation_time, Duration::from_millis(20));
    assert_eq!(report.backlog, Duration::from_millis(5));
    assert_eq!(report.interpolation, 0.5);
    assert_eq!(steps[0].delta, Duration::from_millis(10));
    assert_eq!(steps[1].tick, 2);
}

#[test]
fn bounds_catch_up_and_retains_backlog() {
    let mut clock_config = config();
    clock_config.max_steps_per_update = 2;
    let mut clock = FixedStepClock::new(clock_config).unwrap();
    let first = clock
        .advance(Duration::from_millis(50), |_| Ok::<_, ()>(()))
        .unwrap();
    assert_eq!(first.steps, 2);
    assert_eq!(first.backlog, Duration::from_millis(30));
    assert_eq!(first.backlog_steps, 3);
    assert_eq!(first.interpolation, 1.0);

    let second = clock.advance(Duration::ZERO, |_| Ok::<_, ()>(())).unwrap();
    assert_eq!(second.steps, 2);
    assert_eq!(second.backlog, Duration::from_millis(10));
}

#[test]
fn drops_excess_wall_clock_time() {
    let mut clock_config = config();
    clock_config.max_accumulated_time = Duration::from_millis(50);
    clock_config.max_steps_per_update = 1;
    let mut clock = FixedStepClock::new(clock_config).unwrap();
    let report = clock
        .advance(Duration::from_millis(200), |_| Ok::<_, ()>(()))
        .unwrap();
    assert_eq!(report.dropped_time, Duration::from_millis(150));
    assert_eq!(report.total_dropped_time, Duration::from_millis(150));
    assert_eq!(report.backlog, Duration::from_millis(40));
}

#[test]
fn failed_step_remains_in_backlog() {
    let mut clock = FixedStepClock::new(config()).unwrap();
    let error = clock.advance(Duration::from_millis(20), |_| Err("failed"));
    assert_eq!(error, Err("failed"));
    assert_eq!(clock.tick(), 0);
    assert_eq!(clock.backlog(), Duration::from_millis(20));

    let report = clock.advance(Duration::ZERO, |_| Ok::<_, ()>(())).unwrap();
    assert_eq!(report.steps, 2);
}

struct Counter {
    steps: u32,
}

impl Simulation for Counter {
    type Error = ();

    fn step(&mut self, step: SimulationStep) -> Result<(), Self::Error> {
        assert_eq!(step.tick, u128::from(self.steps + 1));
        self.steps += 1;
        Ok(())
    }
}

#[test]
fn drives_simulation_trait_and_resets() {
    let mut clock = FixedStepClock::new(config()).unwrap();
    let mut simulation = Counter { steps: 0 };
    clock
        .advance_simulation(Duration::from_millis(35), &mut simulation)
        .unwrap();
    assert_eq!(simulation.steps, 3);
    clock.reset();
    assert_eq!(clock.tick(), 0);
    assert_eq!(clock.backlog(), Duration::ZERO);
}

#[test]
fn rejects_invalid_configuration() {
    let mut clock_config = SimulationConfig::default();
    clock_config.step = Duration::ZERO;
    assert!(matches!(
        FixedStepClock::new(clock_config),
        Err(SimulationError::InvalidConfig(_))
    ));
}
