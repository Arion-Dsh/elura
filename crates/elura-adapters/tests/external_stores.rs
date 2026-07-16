#[cfg(any(feature = "redis", feature = "sql"))]
use std::time::Duration;
#[cfg(feature = "redis")]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "redis")]
use elura_adapters::online::RedisOnlineDirectory;
#[cfg(feature = "redis")]
use elura_adapters::outbox::RedisOutbox;
#[cfg(feature = "sql")]
use elura_adapters::outbox::SqlOutbox;
#[cfg(feature = "redis")]
use elura_adapters::replay::RedisReplayStore;
#[cfg(feature = "redis")]
use elura_adapters::session_control::{RedisSessionControlBus, RedisSessionControlConfig};
#[cfg(feature = "redis")]
use elura_core::online::{OnlineDirectory, OnlineStats, OnlineStatsReader, SessionLease};
#[cfg(any(feature = "redis", feature = "sql"))]
use elura_core::outbox::{OutboxEvent, OutboxStore};
#[cfg(feature = "redis")]
use elura_core::session::Identity;
#[cfg(feature = "redis")]
use elura_core::session::{SessionControlEvent, SessionControlKind, SessionControlTransport};
#[cfg(feature = "redis")]
use elura_core::ticket::ReplayStore;

#[cfg(any(feature = "redis", feature = "sql"))]
fn configured_url(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(url) => Some(url),
        Err(_) if std::env::var_os("ELURA_REQUIRE_EXTERNAL_STORES").is_some() => {
            panic!("{name} must be configured in CI")
        }
        Err(_) => None,
    }
}

#[cfg(feature = "redis")]
fn configured_cluster_nodes() -> Option<String> {
    match std::env::var("ELURA_TEST_REDIS_CLUSTER_NODES") {
        Ok(nodes) => Some(nodes),
        Err(_) if std::env::var_os("ELURA_REQUIRE_REDIS_CLUSTER").is_some() => {
            panic!("ELURA_TEST_REDIS_CLUSTER_NODES must be configured in Cluster CI")
        }
        Err(_) => None,
    }
}

#[cfg(any(feature = "redis", feature = "sql"))]
async fn verify_store(store: &dyn OutboxStore) {
    let event = OutboxEvent::new("integration", vec![1, 2, 3]).unwrap();
    store.append(event.clone()).await.unwrap();
    let deliveries = store
        .acquire("integration-worker", 10, Duration::from_secs(5))
        .await
        .unwrap();
    let delivery = deliveries
        .into_iter()
        .find(|item| item.event.id == event.id)
        .unwrap();
    store
        .renew(&delivery, Duration::from_secs(5))
        .await
        .unwrap();
    store.ack(&delivery).await.unwrap();
}

#[tokio::test]
#[cfg(feature = "redis")]
async fn redis_outbox_round_trip_when_configured() {
    let Some(url) = configured_url("ELURA_TEST_REDIS_URL") else {
        return;
    };
    let store = RedisOutbox::connect(&url, format!("elura-test-{}", uuid::Uuid::new_v4()))
        .await
        .unwrap();
    verify_store(&store).await;
}

#[tokio::test]
#[cfg(feature = "redis")]
async fn redis_replay_store_reserves_a_ticket_once_when_configured() {
    let Some(url) = configured_url("ELURA_TEST_REDIS_URL") else {
        return;
    };
    let store =
        RedisReplayStore::connect(&url, format!("elura-test-replay-{}", uuid::Uuid::new_v4()))
            .await
            .unwrap();
    let expires_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60;

    for ticket in ["alpha", "bravo", "charlie", "delta"] {
        assert!(store.reserve(ticket, expires_at).await.unwrap());
        assert!(!store.reserve(ticket, expires_at).await.unwrap());
    }
}

#[tokio::test]
#[cfg(feature = "redis")]
async fn redis_cluster_replay_store_reserves_a_ticket_once_when_configured() {
    let Some(nodes) = configured_cluster_nodes() else {
        return;
    };
    let store = RedisReplayStore::connect_cluster(
        nodes.split(','),
        format!("elura-test-cluster-replay-{}", uuid::Uuid::new_v4()),
    )
    .await
    .unwrap();
    let expires_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60;

    for ticket in ["alpha", "bravo", "charlie", "delta"] {
        assert!(store.reserve(ticket, expires_at).await.unwrap());
        assert!(!store.reserve(ticket, expires_at).await.unwrap());
    }
}

#[tokio::test]
#[cfg(feature = "redis")]
async fn redis_cluster_transport_keeps_multi_key_operations_in_one_slot_when_configured() {
    let Some(nodes) = configured_cluster_nodes() else {
        return;
    };
    let prefix = format!("elura-test-cluster-transport-{}", uuid::Uuid::new_v4());
    let ttl = Duration::from_secs(30);
    let directory = RedisOnlineDirectory::connect_cluster(nodes.split(','), prefix.clone(), ttl)
        .await
        .unwrap();
    let session_id = uuid::Uuid::new_v4();
    let lease = SessionLease {
        session_id,
        gateway_id: "gateway-1".into(),
        identity: Identity {
            account_id: 1,
            user_id: 2,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        },
        expires_at: SystemTime::now() + ttl,
    };

    directory.register(lease.clone()).await.unwrap();
    directory
        .track_group(session_id, "room:1", true)
        .await
        .unwrap();
    assert_eq!(directory.user_sessions(1, 1, 2).await.unwrap().len(), 1);
    assert_eq!(directory.group_sessions("room:1").await.unwrap().len(), 1);
    assert_eq!(
        directory.stats(1, 1).await.unwrap(),
        OnlineStats {
            session_count: 1,
            user_count: 1,
        }
    );

    let mut control_config = RedisSessionControlConfig::default();
    control_config.stream = format!("{prefix}:{{transport}}:session:control");
    let control =
        RedisSessionControlBus::connect_cluster(nodes.split(','), "gateway-1", control_config)
            .await
            .unwrap();
    control
        .publish(&SessionControlEvent {
            kind: SessionControlKind::ForceLogout,
            region_id: 1,
            realm_id: 1,
            user_id: 2,
            generation: 0,
            session_id: Some(session_id),
            keep_session_id: None,
            reason: "cluster-test".into(),
        })
        .await
        .unwrap();

    directory.remove(session_id).await.unwrap();
}

#[tokio::test]
#[cfg(feature = "redis")]
async fn redis_online_renewal_extends_single_session_claim() {
    let Some(url) = configured_url("ELURA_TEST_REDIS_URL") else {
        return;
    };
    let ttl = Duration::from_millis(120);
    let directory = RedisOnlineDirectory::connect(
        &url,
        format!("elura-test-online-{}", uuid::Uuid::new_v4()),
        ttl,
    )
    .await
    .unwrap();
    let identity = Identity {
        account_id: 1,
        user_id: 2,
        region_id: 1,
        realm_id: 1,
        generation: 1,
    };
    let session_id = uuid::Uuid::new_v4();
    let lease = SessionLease {
        session_id,
        gateway_id: "gateway-1".into(),
        identity: identity.clone(),
        expires_at: SystemTime::now() + ttl,
    };
    assert!(
        directory
            .claim_single(&lease, false)
            .await
            .unwrap()
            .is_none()
    );
    directory.register(lease.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    directory.renew(lease).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    let challenger = SessionLease {
        session_id: uuid::Uuid::new_v4(),
        gateway_id: "gateway-2".into(),
        identity,
        expires_at: SystemTime::now() + ttl,
    };
    assert_eq!(
        directory.claim_single(&challenger, false).await.unwrap(),
        Some(session_id)
    );
}

#[tokio::test]
#[cfg(feature = "redis")]
async fn redis_online_stats_count_sessions_and_distinct_users() {
    let Some(url) = configured_url("ELURA_TEST_REDIS_URL") else {
        return;
    };
    let ttl = Duration::from_secs(30);
    let directory = RedisOnlineDirectory::connect(
        &url,
        format!("elura-test-online-{}", uuid::Uuid::new_v4()),
        ttl,
    )
    .await
    .unwrap();
    let first_id = uuid::Uuid::new_v4();
    let leases = [
        (first_id, 2, 1),
        (uuid::Uuid::new_v4(), 2, 1),
        (uuid::Uuid::new_v4(), 3, 1),
        (uuid::Uuid::new_v4(), 4, 2),
    ]
    .into_iter()
    .map(|(session_id, user_id, realm_id)| SessionLease {
        session_id,
        gateway_id: "gateway-1".into(),
        identity: Identity {
            account_id: user_id,
            user_id,
            region_id: 1,
            realm_id,
            generation: 1,
        },
        expires_at: SystemTime::now() + ttl,
    })
    .collect::<Vec<_>>();
    for lease in leases {
        directory.register(lease).await.unwrap();
    }
    let stats: &dyn OnlineStatsReader = &directory;

    assert_eq!(
        stats.stats(1, 1).await.unwrap(),
        OnlineStats {
            session_count: 3,
            user_count: 2,
        }
    );
    assert_eq!(
        stats.stats(1, 2).await.unwrap(),
        OnlineStats {
            session_count: 1,
            user_count: 1,
        }
    );
    directory.remove(first_id).await.unwrap();
    assert_eq!(
        stats.stats(1, 1).await.unwrap(),
        OnlineStats {
            session_count: 2,
            user_count: 2,
        }
    );
}

#[tokio::test]
#[cfg(feature = "redis")]
async fn redis_online_group_indexes_expire_with_the_session() {
    let Some(url) = configured_url("ELURA_TEST_REDIS_URL") else {
        return;
    };
    let ttl = Duration::from_millis(60);
    let prefix = format!("elura-test-online-{}", uuid::Uuid::new_v4());
    let directory = RedisOnlineDirectory::connect(&url, prefix.clone(), ttl)
        .await
        .unwrap();
    let session_id = uuid::Uuid::new_v4();
    let lease = SessionLease {
        session_id,
        gateway_id: "gateway-1".into(),
        identity: Identity {
            account_id: 1,
            user_id: 2,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        },
        expires_at: SystemTime::now() + ttl,
    };
    directory.register(lease).await.unwrap();
    directory
        .track_group(session_id, "room:1", true)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(directory.stats(1, 1).await.unwrap(), OnlineStats::default());

    let client = redis::Client::open(url).unwrap();
    let mut connection = client.get_connection_manager().await.unwrap();
    let reverse_exists: bool = redis::cmd("EXISTS")
        .arg(format!("{prefix}:session-groups:{session_id}"))
        .query_async(&mut connection)
        .await
        .unwrap();
    let group_exists: bool = redis::cmd("EXISTS")
        .arg(format!("{prefix}:group:room:1"))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(!reverse_exists);
    assert!(!group_exists);
}

#[tokio::test]
#[cfg(feature = "sql")]
async fn postgres_outbox_round_trip_when_configured() {
    let Some(url) = configured_url("ELURA_TEST_POSTGRES_URL") else {
        return;
    };
    let store = SqlOutbox::connect_postgres(&url).await.unwrap();
    store.ensure_schema().await.unwrap();
    verify_store(&store).await;
}

#[tokio::test]
#[cfg(feature = "sql")]
async fn mysql_outbox_round_trip_when_configured() {
    let Some(url) = configured_url("ELURA_TEST_MYSQL_URL") else {
        return;
    };
    let store = SqlOutbox::connect_mysql(&url).await.unwrap();
    store.ensure_schema().await.unwrap();
    verify_store(&store).await;
}
