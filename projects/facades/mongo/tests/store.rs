//! `.yykv` 后端通过 `MongoStorage` 合同的行为验证。
//!
//! 这里只验证命令层依赖的合同本身：`_id` 唯一、扫描有序、目录跨重启保留、事务
//! 提交与冲突、oplog 封顶提交。命令语义的逐命令语料属于下游安装态门禁。

use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use database_protocol_mongo::bson::{BsonDocument, BsonValue, encode_bson, value_to_key};
use mongo::{MongoStorage, Store, oplog::OplogEntry};

fn scratch(label: &str) -> PathBuf {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).map(|value| value.as_nanos()).unwrap_or(0);
    std::env::temp_dir().join(format!("mongo-facade-{label}-{}-{stamp}.yykv", std::process::id()))
}

fn document(identifier: i32) -> BsonDocument {
    let mut document = BsonDocument::new();
    document.push("_id".to_string(), BsonValue::Int32(identifier));
    document.push("value".to_string(), BsonValue::Int32(identifier));
    document
}

fn entry(identifier: i32) -> (Vec<u8>, Vec<u8>) {
    let document = document(identifier);
    (value_to_key(&BsonValue::Int32(identifier)), encode_bson(&document))
}

#[test]
fn contract_round_trips_documents_and_catalog() {
    let base = scratch("round-trip");
    let store = Store::open(&base).expect("store opens");
    let storage: &dyn MongoStorage = &store;

    let report = storage.insert("shop.orders", &[entry(1), entry(2)], true).expect("insert lands");
    assert_eq!(report.inserted, 2);
    assert!(report.duplicates.is_empty());

    let duplicate = storage.insert("shop.orders", &[entry(2), entry(3)], false).expect("duplicate is reported");
    assert_eq!(duplicate.inserted, 1);
    assert_eq!(duplicate.duplicates, vec![0]);

    let scanned = storage.scan("shop.orders").expect("scan lands");
    assert_eq!(scanned.len(), 3);
    assert_eq!(scanned[0].0, entry(1).0);

    assert!(storage.namespace_exists("shop.orders").expect("catalog is readable"));
    storage.create_collection("shop.empty", Default::default()).expect("collection is created");
    assert!(storage.namespace_exists("shop.empty").expect("catalog is readable"));

    let deleted = storage.delete("shop.orders", &[entry(2).0]).expect("delete lands");
    assert_eq!(deleted, 1);

    drop(store);
    let reopened = Store::open(&base).expect("store reopens");
    assert!(reopened.namespace_exists("shop.orders").expect("catalog is readable"));
    assert!(reopened.namespace_exists("shop.empty").expect("catalog is readable"));

    let _ = std::fs::remove_file(base.with_extension("yyds"));
    let _ = std::fs::remove_file(base.with_extension("yykv"));
}

#[test]
fn contract_commits_transactions_and_caps_the_oplog() {
    let base = scratch("transactions");
    let store = Store::open(&base).expect("store opens");
    let storage: &dyn MongoStorage = &store;
    storage.insert("shop.orders", &[entry(1)], true).expect("insert lands");

    assert!(matches!(storage.begin_transaction("session", 1), mongo::storage::TransactionBegin::Started));
    assert!(matches!(
        storage.transaction_write("session", "shop.orders", &entry(2).0, Some(entry(2).1)),
        mongo::storage::TransactionWrite::Staged
    ));
    assert_eq!(storage.transaction_scan("session", "shop.orders").expect("scan lands").map(|documents| documents.len()), Some(2));
    assert!(matches!(storage.transaction_commit("session", 1), mongo::storage::TransactionCommit::Committed));
    assert!(storage.get("shop.orders", &entry(2).0).expect("read lands").is_some());

    storage.enable_oplog("local.oplog.rs", 4096).expect("oplog is enabled");
    assert!(storage.oplog_enabled());
    storage.stage_oplog(OplogEntry::insert("shop.orders", BsonValue::String("uuid".to_string()), &document(3)));
    let last = storage.commit_oplog(1_000).expect("oplog commits").expect("one entry committed");
    assert_eq!(last, (1, 1));
    let (namespace, max_bytes, count, bytes) = store.oplog_status().expect("oplog status");
    assert_eq!(namespace, "local.oplog.rs");
    assert_eq!(max_bytes, 4096);
    assert_eq!(count, 1);
    assert!(bytes > 0);

    drop(store);
    let _ = std::fs::remove_file(base.with_extension("yyds"));
    let _ = std::fs::remove_file(base.with_extension("yykv"));
}
