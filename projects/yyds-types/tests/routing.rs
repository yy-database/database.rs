use yyds_types::{RoutingError, ShardEpoch, ShardId, ShardMap};

#[test]
fn routing_is_deterministic_and_namespace_scoped() {
    let map = ShardMap::new(ShardEpoch(4), vec![ShardId("a".into()), ShardId("b".into()), ShardId("c".into())]).expect("map");
    let first = map.route("tenant-a", b"user-1");
    assert_eq!(first, map.route("tenant-a", b"user-1"));
    assert_ne!(first.hash, map.route("tenant-b", b"user-1").hash);
    assert_eq!(first.epoch, ShardEpoch(4));
}

#[test]
fn routing_rejects_invalid_maps_and_stale_epochs() {
    assert_eq!(ShardMap::new(ShardEpoch(0), vec![ShardId("a".into())]), Err(RoutingError::InvalidEpoch));
    assert_eq!(
        ShardMap::new(ShardEpoch(1), vec![ShardId("a".into()), ShardId("a".into())]),
        Err(RoutingError::InvalidShardIdentity)
    );
    let map = ShardMap::new(ShardEpoch(2), vec![ShardId("a".into())]).expect("map");
    assert_eq!(
        map.route_at_epoch(ShardEpoch(1), "tenant", b"key"),
        Err(RoutingError::StaleEpoch { expected: ShardEpoch(2), found: ShardEpoch(1) })
    );
}
