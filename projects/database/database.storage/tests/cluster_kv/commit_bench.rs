//! FileShard 提交路径地板基准。
//!
//! 默认 `#[ignore]`，用 `cargo test --release -p database-storage --test cluster_kv -- --ignored --nocapture`
//! 跑。两个场景对应下游服务端线的每语句地板：`insert-single` 是每提交一条新键的单 Put 事务，
//! `update-one` 是在大分片上反复替换单个键。计数分配器记录测量窗口内的分配次数与字节数，
//! 用于区分「整份 clone + 整份重编码」与「脏区增量重编码」两种提交形态。

use std::{
    alloc::{GlobalAlloc, Layout, System},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use database_storage::cluster_kv::{FileShard, InlineValue, Key, KvStore, ShardChange, StoredValue};

struct Counting;

static ALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn temp_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("cluster-kv-bench-{label}-{nonce}.yykv"))
}

fn report(scenario: &str, commits: usize, elapsed: std::time::Duration, calls: usize, bytes: usize) {
    let ns_per_commit = elapsed.as_nanos() as f64 / commits as f64;
    println!(
        "{scenario}: commits={commits} total_ms={:.1} ns_per_commit={ns_per_commit:.0} allocs_per_commit={:.0} bytes_per_commit={:.0}",
        elapsed.as_nanos() as f64 / 1e6,
        calls as f64 / commits as f64,
        bytes as f64 / commits as f64,
    );
}

#[test]
#[ignore = "commit floor benchmark; run with --release -- --ignored"]
fn bench_commit_insert_single() {
    const COMMITS: usize = 1000;
    let path = temp_path("insert-single");
    let mut shard = FileShard::open(&path).expect("open");

    let calls0 = ALLOC_CALLS.load(Ordering::Relaxed);
    let bytes0 = ALLOC_BYTES.load(Ordering::Relaxed);
    let start = Instant::now();
    for index in 0..COMMITS {
        let key = Key::new("bench", format!("row-{index:06}").into_bytes());
        let value = StoredValue::Inline(InlineValue(vec![(index % 251) as u8; 64]));
        shard.commit_changes(vec![ShardChange::Put { key, value, expires_at: None }]).expect("commit");
    }
    let elapsed = start.elapsed();
    report("insert-single", COMMITS, elapsed, ALLOC_CALLS.load(Ordering::Relaxed) - calls0, ALLOC_BYTES.load(Ordering::Relaxed) - bytes0);

    drop(shard);
    let _ = std::fs::remove_file(&path);
}

#[test]
#[ignore = "commit floor benchmark; run with --release -- --ignored"]
fn bench_commit_update_one() {
    const RECORDS: usize = 5000;
    const COMMITS: usize = 400;
    let path = temp_path("update-one");
    let mut shard = FileShard::open(&path).expect("open");

    let seed: Vec<ShardChange> = (0..RECORDS)
        .map(|index| ShardChange::Put {
            key: Key::new("bench", format!("row-{index:06}").into_bytes()),
            value: StoredValue::Inline(InlineValue(vec![(index % 251) as u8; 64])),
            expires_at: None,
        })
        .collect();
    shard.commit_changes(seed).expect("seed");

    let calls0 = ALLOC_CALLS.load(Ordering::Relaxed);
    let bytes0 = ALLOC_BYTES.load(Ordering::Relaxed);
    let start = Instant::now();
    for round in 0..COMMITS {
        let index = (round * 7919) % RECORDS;
        let key = Key::new("bench", format!("row-{index:06}").into_bytes());
        let value = StoredValue::Inline(InlineValue(vec![(round % 251) as u8; 64]));
        shard.commit_changes(vec![ShardChange::Put { key, value, expires_at: None }]).expect("commit");
    }
    let elapsed = start.elapsed();
    report("update-one", COMMITS, elapsed, ALLOC_CALLS.load(Ordering::Relaxed) - calls0, ALLOC_BYTES.load(Ordering::Relaxed) - bytes0);

    drop(shard);
    let _ = std::fs::remove_file(&path);
}
