//! `.yykv` shard snapshot revision semantics.

use std::collections::BTreeMap;

use database_formats::yy::yykv::{InlineValue, Key, Record, StoredValue, decode_snapshot, encode_snapshot};

fn record(key: &str, revision: u64) -> (Key, Record) {
    let key = Key::new("ns", key.as_bytes().to_vec());
    (key.clone(), Record { key, value: StoredValue::Inline(InlineValue(b"v".to_vec())), revision, expires_at: None })
}

#[test]
fn empty_shard_starts_at_revision_one() {
    let snapshot = decode_snapshot(&encode_snapshot(&BTreeMap::new(), 1).expect("encode")).expect("decode");
    assert_eq!(snapshot.next_revision, 1);
    assert!(snapshot.records.is_empty());
}

#[test]
fn first_write_keeps_revision_below_next_revision() {
    let (key, value) = record("k", 1);
    let mut records = BTreeMap::new();
    records.insert(key.clone(), value);
    let snapshot = decode_snapshot(&encode_snapshot(&records, 2).expect("encode")).expect("decode");
    assert_eq!(snapshot.next_revision, 2);
    assert_eq!(snapshot.records[&key].revision, 1);
}

#[test]
fn encode_rejects_an_empty_shard_without_a_revision() {
    assert!(encode_snapshot(&BTreeMap::new(), 0).is_err());
}

#[test]
fn encode_rejects_a_record_sitting_on_next_revision() {
    let (key, value) = record("k", 1);
    let mut records = BTreeMap::new();
    records.insert(key, value);
    assert!(encode_snapshot(&records, 1).is_err());
}
