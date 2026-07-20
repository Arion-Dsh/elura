use elura_room::{Room, RoomConfig, RoomError, RoomPhase};

fn config() -> RoomConfig {
    let mut config = RoomConfig::default();
    config.capacity = 3;
    config.minimum_to_start = 2;
    config
}

#[test]
fn manages_members_readiness_and_lifecycle() {
    let mut room = Room::new("match-1", config()).unwrap();
    room.join(10, "tank").unwrap();
    room.join(20, "healer").unwrap();
    assert_eq!(room.leader(), Some(&10));
    assert_eq!(room.members_in_join_order(), vec![&10, &20]);
    assert!(matches!(room.start(), Err(RoomError::MembersNotReady)));

    room.set_ready(&10, true).unwrap();
    room.set_ready(&20, true).unwrap();
    assert!(room.can_start());
    room.start().unwrap();
    assert_eq!(room.phase(), RoomPhase::Active);
    assert!(matches!(room.join(30, "dps"), Err(RoomError::NotOpen)));
}

#[test]
fn enforces_capacity_and_duplicate_membership() {
    let mut room = Room::new(1, config()).unwrap();
    room.join(1, ()).unwrap();
    assert!(matches!(room.join(1, ()), Err(RoomError::AlreadyMember)));
    room.join(2, ()).unwrap();
    room.join(3, ()).unwrap();
    assert!(matches!(room.join(4, ()), Err(RoomError::Full)));
}

#[test]
fn transfers_leadership_by_join_order() {
    let mut room = Room::new(1, config()).unwrap();
    room.join(10, "a").unwrap();
    room.join(20, "b").unwrap();
    room.join(30, "c").unwrap();
    room.transfer_leader(&20).unwrap();
    assert_eq!(room.leader(), Some(&20));

    let departure = room.leave(&20).unwrap();
    assert_eq!(departure.data, "b");
    assert_eq!(departure.new_leader, Some(10));
    assert!(!departure.empty);
}

#[test]
fn optionally_allows_joining_an_active_room() {
    let mut room_config = config();
    room_config.require_all_ready = false;
    room_config.allow_join_in_progress = true;
    let mut room = Room::new(1, room_config).unwrap();
    room.join(1, ()).unwrap();
    room.join(2, ()).unwrap();
    room.start().unwrap();
    room.join(3, ()).unwrap();
    room.close();
    assert!(matches!(room.join(4, ()), Err(RoomError::NotOpen)));
}

#[test]
fn rejects_invalid_configuration() {
    let mut room_config = RoomConfig::default();
    room_config.capacity = 0;
    assert!(matches!(
        Room::<u64, u64, ()>::new(1, room_config),
        Err(RoomError::InvalidConfig(_))
    ));
}
