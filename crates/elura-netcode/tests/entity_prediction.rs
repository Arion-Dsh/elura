use elura_netcode::{
    NetcodeError, PredictedEntityConfig, PredictedEntityMatcher, PredictionKey,
    PredictionKeyGenerator,
};

#[test]
fn matches_temporary_and_authoritative_entities() {
    let mut keys = PredictionKeyGenerator::default();
    let key = keys.generate().unwrap();
    let mut matcher = PredictedEntityMatcher::new(PredictedEntityConfig::default()).unwrap();
    matcher.register(key, 100_u64, 10).unwrap();

    let matched = matcher.resolve(key, 9001, 13).unwrap().unwrap();
    assert_eq!(matched.temporary_entity, 100);
    assert_eq!(matched.authoritative_entity, 9001);
    assert_eq!(matched.age_ticks, 3);
    assert!(matcher.is_empty());
}

#[test]
fn expires_unmatched_predictions() {
    let mut config = PredictedEntityConfig::default();
    config.timeout_ticks = 3;
    let mut matcher = PredictedEntityMatcher::new(config).unwrap();
    matcher.register(PredictionKey(1), 10_u64, 5).unwrap();
    assert!(matcher.expire(8).is_empty());
    let expired = matcher.expire(9);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].temporary_entity, 10);
}

#[test]
fn duplicate_keys_entities_and_capacity_are_rejected() {
    let mut config = PredictedEntityConfig::default();
    config.capacity = 1;
    let mut matcher = PredictedEntityMatcher::new(config).unwrap();
    matcher.register(PredictionKey(1), 10_u64, 1).unwrap();
    assert!(matches!(
        matcher.register(PredictionKey(1), 11, 1),
        Err(NetcodeError::InvalidInput(_))
    ));
    assert!(matches!(
        matcher.register(PredictionKey(2), 10, 1),
        Err(NetcodeError::InvalidInput(_))
    ));
    assert_eq!(
        matcher.register(PredictionKey(2), 12, 1),
        Err(NetcodeError::PredictedEntityLimit)
    );
}

#[test]
fn late_authoritative_spawn_does_not_match_expired_prediction() {
    let mut config = PredictedEntityConfig::default();
    config.timeout_ticks = 2;
    let mut matcher = PredictedEntityMatcher::new(config).unwrap();
    matcher.register(PredictionKey(1), 10_u64, 1).unwrap();
    assert_eq!(matcher.resolve(PredictionKey(1), 99, 4).unwrap(), None);
    assert!(matcher.is_empty());
}
