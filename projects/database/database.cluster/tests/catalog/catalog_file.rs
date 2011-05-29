//! Catalog persistence: `.yyds` path suffix, owner publication, and flush/reopen.

use boost_fs::temp;
use database_cluster::catalog::{Catalog, catalog_path};
use database_types::cluster::{CATALOG_SUFFIX, LeaderTerm, ReplicaNodeId, ShardId};

#[test]
fn catalog_path_uses_yyds_suffix() {
    let path = catalog_path("cluster-a");
    assert_eq!(path.extension().and_then(|ext| ext.to_str()), Some(CATALOG_SUFFIX));
}

#[test]
fn empty_catalog_has_no_owners() {
    let catalog = Catalog::open_memory();
    assert!(catalog.owners().is_empty());
}

#[test]
fn publish_owner_adds_or_updates() {
    let mut catalog = Catalog::open_memory();
    catalog.publish_owner(ShardId("shard-0".into()), ReplicaNodeId("node-a".into()), LeaderTerm(1));
    assert_eq!(catalog.owners().len(), 1);
    assert_eq!(catalog.owners()[0].leader_term, LeaderTerm(1));

    // Same shard_id, higher term — should update, not append.
    catalog.publish_owner(ShardId("shard-0".into()), ReplicaNodeId("node-a".into()), LeaderTerm(2));
    assert_eq!(catalog.owners().len(), 1);
    assert_eq!(catalog.owners()[0].leader_term, LeaderTerm(2));
}

#[test]
fn publish_owners_roundtrip_via_flush_and_reopen() {
    let dir = temp::create_folder().expect("tempdir");
    let path = catalog_path(dir.path().join("owner-test"));

    {
        let mut catalog = Catalog::open(&path).expect("open");
        catalog.publish_owner(ShardId("shard-0".into()), ReplicaNodeId("node-a".into()), LeaderTerm(1));
        catalog.publish_owner(ShardId("shard-1".into()), ReplicaNodeId("node-b".into()), LeaderTerm(3));
        catalog.flush().expect("flush");
    }

    let reopened = Catalog::open(&path).expect("reopen");
    assert_eq!(reopened.owners().len(), 2);
    let shard0 = reopened.owners().iter().find(|o| o.shard_id == ShardId("shard-0".into())).expect("shard-0");
    assert_eq!(shard0.node_id, ReplicaNodeId("node-a".into()));
    assert_eq!(shard0.leader_term, LeaderTerm(1));

    let shard1 = reopened.owners().iter().find(|o| o.shard_id == ShardId("shard-1".into())).expect("shard-1");
    assert_eq!(shard1.node_id, ReplicaNodeId("node-b".into()));
    assert_eq!(shard1.leader_term, LeaderTerm(3));
}
