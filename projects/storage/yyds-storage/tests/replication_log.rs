use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use yyds_kv::DurableReplicationLog;
use yyds_types::{LeaderTerm, LogIndex, ReplicationEntry};

fn temp_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("yyds-replication-{label}-{nonce}.yyrl"))
}

fn entry(index: u64, payload: &[u8]) -> ReplicationEntry {
    ReplicationEntry { index: LogIndex(index), term: LeaderTerm(3), payload: payload.to_vec() }
}

fn cleanup(path: &PathBuf) {
    let _ = fs::remove_file(path);
}

#[test]
fn durable_log_round_trips_entries_and_commit() {
    let path = temp_path("round-trip");
    let mut log = DurableReplicationLog::open(&path).expect("open");
    log.append(entry(1, b"put-a")).expect("append");
    log.append(entry(2, b"put-b")).expect("append");
    log.commit(LogIndex(1)).expect("commit");
    drop(log);

    let log = DurableReplicationLog::open(&path).expect("reopen");
    assert_eq!(log.entries().len(), 2);
    assert_eq!(log.entries()[0].payload, b"put-a");
    assert_eq!(log.entries()[1].payload, b"put-b");
    assert_eq!(log.committed(), LogIndex(1));
    let committed = log.committed_entries().map(|entry| entry.payload.as_slice()).collect::<Vec<_>>();
    assert_eq!(committed, vec![b"put-a".as_slice()]);
    cleanup(&path);
}

#[test]
fn durable_log_ignores_a_partial_tail_but_rejects_complete_checksum_corruption() {
    let path = temp_path("tail");
    let mut log = DurableReplicationLog::open(&path).expect("open");
    log.append(entry(1, b"put")).expect("append");
    drop(log);
    let mut bytes = fs::read(&path).expect("read");
    bytes.extend_from_slice(&[1, 2, 3]);
    fs::write(&path, bytes).expect("write tail");
    let mut log = DurableReplicationLog::open(&path).expect("partial tail is recoverable");
    assert_eq!(log.entries().len(), 1);
    log.append(entry(2, b"after-recovery")).expect("append after recovery");
    drop(log);
    let log = DurableReplicationLog::open(&path).expect("reopen after append");
    assert_eq!(log.entries().len(), 2);
    drop(log);
    cleanup(&path);

    let path = temp_path("checksum");
    let mut log = DurableReplicationLog::open(&path).expect("open");
    log.append(entry(1, b"put")).expect("append");
    drop(log);
    let mut bytes = fs::read(&path).expect("read");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    fs::write(&path, bytes).expect("corrupt");
    assert!(DurableReplicationLog::open(&path).is_err());
    cleanup(&path);
}

#[test]
fn durable_log_requires_contiguous_entries_and_commits() {
    let path = temp_path("ordering");
    let mut log = DurableReplicationLog::open(&path).expect("open");
    assert!(log.append(entry(2, b"gap")).is_err());
    log.append(entry(1, b"put")).expect("append");
    assert!(log.commit(LogIndex(2)).is_err());
    log.commit(LogIndex(1)).expect("commit");
    assert!(log.commit(LogIndex(2)).is_err());
    cleanup(&path);
}
