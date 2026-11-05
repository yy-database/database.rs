use database_storage::cluster_kv::{InlineValue, Key, KvStore, MemoryShard, StoredValue, compare_and_put, now_unix_ms};
use database_types::cluster::Namespace;

#[test]
fn put_get_round_trip() {
    let mut shard = MemoryShard::new();
    let key = Key::new("users", b"alice");
    let revision = shard.put(key.clone(), StoredValue::Inline(InlineValue(b"yyds".to_vec()))).expect("put");

    let record = shard.get(&key).expect("get").expect("record");
    assert_eq!(revision, record.revision);
    assert_eq!(record.value, StoredValue::Inline(InlineValue(b"yyds".to_vec())));
}

#[test]
fn compare_and_put_rejects_stale_revision() {
    let mut shard = MemoryShard::new();
    let key = Key::new("users", b"bob");
    let first = shard.put(key.clone(), StoredValue::Inline(InlineValue(vec![1]))).expect("put");

    let err = compare_and_put(&mut shard, key.clone(), Some(first - 1), StoredValue::Inline(InlineValue(vec![2]))).expect_err("cas conflict");

    assert!(err.to_string().contains("cas conflict"));
}

#[test]
fn scan_returns_all_records_in_namespace_ordered_by_key() {
    let mut shard = MemoryShard::new();
    let ns = Namespace("users".into());
    for (k, v) in &[("carol", b"c".to_vec()), ("alice", b"a".to_vec()), ("bob", b"b".to_vec())] {
        shard.put(Key::new("users", k.as_bytes()), StoredValue::Inline(InlineValue(v.clone()))).unwrap();
    }

    let results = shard.scan(&ns, b"", None, None, 0).unwrap();
    let keys: Vec<&[u8]> = results.iter().map(|r| r.key.bytes.as_slice()).collect();
    assert_eq!(keys, vec![b"alice".as_slice(), b"bob", b"carol"]);
    assert_eq!(results.len(), 3);
}

#[test]
fn scan_prefix_filters_to_matching_keys() {
    let mut shard = MemoryShard::new();
    let ns = Namespace("table:7".into());
    for &k in &[b"10" as &[u8], b"11", b"20", b"21"] {
        shard.put(Key::new("table:7", k), StoredValue::Inline(InlineValue(k.to_vec()))).unwrap();
    }

    let results = shard.scan(&ns, b"1", None, None, 0).unwrap();
    let keys: Vec<&[u8]> = results.iter().map(|r| r.key.bytes.as_slice()).collect();
    assert_eq!(keys, vec![b"10", b"11"]);
}

#[test]
fn scan_after_is_exclusive_cursor() {
    let mut shard = MemoryShard::new();
    let ns = Namespace("n".into());
    for &k in &[b"0" as &[u8], b"1", b"2", b"3"] {
        shard.put(Key::new("n", k), StoredValue::Inline(InlineValue(k.to_vec()))).unwrap();
    }

    let results = shard.scan(&ns, b"", Some(b"1"), None, 0).unwrap();
    let keys: Vec<&[u8]> = results.iter().map(|r| r.key.bytes.as_slice()).collect();
    assert_eq!(keys, vec![b"2", b"3"]);
}

#[test]
fn scan_limit_caps_results() {
    let mut shard = MemoryShard::new();
    let ns = Namespace("n".into());
    for &k in &[b"a" as &[u8], b"b", b"c", b"d", b"e"] {
        shard.put(Key::new("n", k), StoredValue::Inline(InlineValue(k.to_vec()))).unwrap();
    }

    let results = shard.scan(&ns, b"", None, None, 2).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn scan_isolated_between_namespaces() {
    let mut shard = MemoryShard::new();
    shard.put(Key::new("a", b"1"), StoredValue::Inline(InlineValue(b"v1".to_vec()))).unwrap();
    shard.put(Key::new("b", b"1"), StoredValue::Inline(InlineValue(b"v2".to_vec()))).unwrap();

    let a = shard.scan(&Namespace("a".into()), b"", None, None, 0).unwrap();
    let b = shard.scan(&Namespace("b".into()), b"", None, None, 0).unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].key.namespace.0, "a");
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].key.namespace.0, "b");
}

#[test]
fn scan_empty_namespace_returns_nothing() {
    let mut shard = MemoryShard::new();
    shard.put(Key::new("used", b"x"), StoredValue::Inline(InlineValue(b"y".to_vec()))).unwrap();
    let results = shard.scan(&Namespace("unused".into()), b"", None, None, 0).unwrap();
    assert!(results.is_empty());
}

#[test]
fn sweep_expired_removes_only_elapsed_records() {
    let mut shard = MemoryShard::new();
    let now = now_unix_ms();
    let live = Key::new("ns", b"live");
    let fresh = Key::new("ns", b"fresh");
    let gone = Key::new("ns", b"gone");
    let stale = Key::new("ns", b"stale");
    shard.put(live.clone(), StoredValue::Inline(InlineValue(b"v".to_vec()))).unwrap();
    shard.put_expiring(fresh.clone(), StoredValue::Inline(InlineValue(b"v".to_vec())), now + 60_000).unwrap();
    shard.put_expiring(gone.clone(), StoredValue::Inline(InlineValue(b"v".to_vec())), now - 1).unwrap();
    shard.put_expiring(stale.clone(), StoredValue::Inline(InlineValue(b"v".to_vec())), now).unwrap();

    // The deadline itself counts as elapsed, matching the read-path filter.
    assert_eq!(shard.sweep_expired(now, None).unwrap(), database_types::cluster::SweepReport { examined: 4, reclaimed: 2 });
    assert!(shard.get(&live).unwrap().is_some());
    assert!(shard.get(&fresh).unwrap().is_some());
    assert!(shard.get(&gone).unwrap().is_none());
    assert!(shard.get(&stale).unwrap().is_none());
    // A second sweep is a no-op.
    assert_eq!(shard.sweep_expired(now, None).unwrap(), database_types::cluster::SweepReport { examined: 2, reclaimed: 0 });
}

#[test]
fn sweep_expired_does_not_extend_revisions() {
    let mut shard = MemoryShard::new();
    let now = now_unix_ms();
    let gone = Key::new("ns", b"gone");
    let fresh = Key::new("ns", b"fresh");
    shard.put_expiring(gone.clone(), StoredValue::Inline(InlineValue(b"v".to_vec())), now - 1).unwrap();
    let fresh_revision = shard.put_expiring(fresh.clone(), StoredValue::Inline(InlineValue(b"v".to_vec())), now + 60_000).unwrap();

    assert_eq!(shard.sweep_expired(now, None).unwrap().reclaimed, 1);
    assert_eq!(shard.get(&fresh).unwrap().unwrap().revision, fresh_revision, "reclaim must not renumber surviving records");
}

#[test]
fn sweep_budget_resumes_from_the_key_cursor_without_skipping_records() {
    let mut shard = MemoryShard::new();
    let now = now_unix_ms();
    for index in 0..10 {
        let key = Key::new("ns", format!("k{index:02}").into_bytes());
        shard.put_expiring(key, StoredValue::Inline(InlineValue(b"v".to_vec())), now - 1).unwrap();
    }

    let mut passes = 0;
    let mut reclaimed = 0;
    while reclaimed < 10 {
        let report = shard.sweep_expired(now, Some(3)).unwrap();
        assert!(report.examined <= 3, "one pass must stop at the budget");
        assert!(report.reclaimed > 0, "each working pass makes progress");
        passes += 1;
        reclaimed += report.reclaimed;
    }
    // 10 records at a budget of 3 take four passes; the fourth pass sees the last record.
    assert_eq!(passes, 4, "the cursor must resume instead of restarting every pass");
    assert_eq!(reclaimed, 10, "every expired record is reclaimed exactly once");
    assert_eq!(shard.sweep_expired(now, Some(3)).unwrap(), database_types::cluster::SweepReport::default());
}

#[test]
fn sweep_uses_the_supplied_clock_and_does_not_resurrect_records() {
    let mut shard = MemoryShard::new();
    let now = now_unix_ms();
    let gone = Key::new("ns", b"gone");
    let later = Key::new("ns", b"later");
    shard.put_expiring(gone.clone(), StoredValue::Inline(InlineValue(b"v".to_vec())), now - 1).unwrap();
    shard.put_expiring(later.clone(), StoredValue::Inline(InlineValue(b"v".to_vec())), now + 60_000).unwrap();

    assert_eq!(shard.sweep_expired(now, None).unwrap().reclaimed, 1, "the elapsed record is reclaimed");

    // A backward clock jump must not restore the physically deleted record, and must not
    // reclaim a record whose deadline is still ahead of the supplied clock.
    let rollback = shard.sweep_expired(now - 120_000, None).unwrap();
    assert_eq!(rollback, database_types::cluster::SweepReport { examined: 1, reclaimed: 0 });
    assert!(shard.get(&gone).unwrap().is_none(), "physical deletion is irreversible");
    assert!(shard.get(&later).unwrap().is_some(), "a future deadline survives a rollback");
}

#[test]
fn namespaces_len_and_flush_are_scoped_to_one_namespace() {
    let mut shard = MemoryShard::new();
    shard.put(Key::new("a", b"1"), StoredValue::Inline(InlineValue(b"v1".to_vec()))).unwrap();
    shard.put(Key::new("a", b"2"), StoredValue::Inline(InlineValue(b"v2".to_vec()))).unwrap();
    shard.put(Key::new("b", b"1"), StoredValue::Inline(InlineValue(b"v3".to_vec()))).unwrap();

    assert_eq!(shard.namespaces().unwrap(), vec![Namespace("a".into()), Namespace("b".into())]);
    assert_eq!(shard.len(&Namespace("a".into())).unwrap(), 2);
    assert_eq!(shard.len(&Namespace("b".into())).unwrap(), 1);
    assert_eq!(shard.len(&Namespace("missing".into())).unwrap(), 0);

    assert_eq!(shard.flush(&Namespace("a".into())).unwrap(), 2);
    assert_eq!(shard.len(&Namespace("a".into())).unwrap(), 0);
    assert_eq!(shard.len(&Namespace("b".into())).unwrap(), 1);
    assert_eq!(shard.namespaces().unwrap(), vec![Namespace("b".into())]);
}
