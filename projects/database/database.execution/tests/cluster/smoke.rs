//! Execution smoke: routing, one write/read/delete cycle and batch ordering.
//!
//! Shard persistence, sweep budgets and transaction fences are exercised through
//! the Rust, TypeScript and Python bindings, so this file keeps only the wiring
//! between the routing table and the shard map.

use std::collections::HashMap;

use database_execution::cluster::{KeyValueExecutor, LocalExecutor};
use database_storage::cluster_kv::MemoryShard;
use database_cluster::topology::ShardMap;
use database_query::cluster_kv::{KeyValueAction, KeyValueCommand, KeyValueResult};
use database_types::cluster::{Namespace, ShardEpoch, ShardId};

fn command(action: KeyValueAction, key: &[u8]) -> KeyValueCommand {
    KeyValueCommand { namespace: Namespace("redis:0".into()), key: key.to_vec(), action }
}

fn executor(shards: &[&str]) -> LocalExecutor<MemoryShard> {
    let epoch = ShardEpoch(1);
    let ids: Vec<ShardId> = shards.iter().map(|id| ShardId((*id).into())).collect();
    let routing = ShardMap::new(epoch, ids.clone()).unwrap();
    let mut map = HashMap::new();
    for id in ids {
        map.insert(id, MemoryShard::new());
    }
    LocalExecutor::new(routing, map, Vec::new()).unwrap()
}

#[test]
fn execute_reads_and_writes_through_the_routing_table() {
    let executor = executor(&["shard-a"]);
    let key = b"smoke-key";
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, key)).unwrap(), KeyValueResult::Get(None));
    assert_eq!(executor.execute(None, command(KeyValueAction::Put(b"value".to_vec()), key)).unwrap(), KeyValueResult::Put { revision: 1 });
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, key)).unwrap(), KeyValueResult::Get(Some(b"value".to_vec())));
    assert_eq!(executor.execute(None, command(KeyValueAction::Delete, key)).unwrap(), KeyValueResult::Delete { removed: true });
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, key)).unwrap(), KeyValueResult::Get(None));
}

#[test]
fn read_batch_preserves_order_and_reports_missing_keys() {
    let executor = executor(&["shard-a"]);
    executor.execute(None, command(KeyValueAction::Put(b"two".to_vec()), b"second")).unwrap();
    let results = executor
        .execute_read_batch(
            None,
            &[command(KeyValueAction::Get, b"second"), command(KeyValueAction::Get, b"missing"), command(KeyValueAction::Get, b"second")],
        )
        .unwrap();
    assert_eq!(
        results,
        vec![KeyValueResult::Get(Some(b"two".to_vec())), KeyValueResult::Get(None), KeyValueResult::Get(Some(b"two".to_vec()))]
    );
}

#[test]
fn keys_route_across_shards_by_hash() {
    let executor = executor(&["shard-a", "shard-b"]);
    let mut seen = std::collections::HashSet::new();
    for index in 0..64 {
        let key = format!("key-{index}");
        seen.insert(executor.routing().route("redis:0", key.as_bytes()).shard.0.clone());
    }
    assert!(seen.len() > 1, "routing collapsed every key onto one shard");
}
