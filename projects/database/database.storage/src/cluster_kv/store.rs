use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    ops::Bound,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use database_formats::yy::yykv::{Key, Record, StoredValue, decode_snapshot, encode_snapshot};
use database_types::cluster::{Error, Namespace, Result, SweepReport};

/// Current wall clock as Unix epoch milliseconds, used for record expiry deadlines.
pub fn now_unix_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|duration| duration.as_millis() as i64).unwrap_or(0)
}

/// One mutation inside a shard-level atomic commit.
#[derive(Debug, Clone)]
pub enum ShardChange {
    /// Insert or replace a key with `value` at the next shard revision.
    Put {
        /// The key to insert or replace.
        key: Key,
        /// The post-commit value.
        value: StoredValue,
        /// Optional wall-clock expiry as Unix epoch milliseconds; `None` clears any previous expiry.
        expires_at: Option<i64>,
    },
    /// Remove a key when present.
    Delete {
        /// The key to remove.
        key: Key,
    },
}

/// Shard-local KV surface. Implementations back one `.yykv` shard.
pub trait KvStore {
    /// Returns the latest record for `key`, if present and not expired.
    fn get(&self, key: &Key) -> Result<Option<Record>>;

    /// Inserts or replaces `value` and returns the new revision. Any previous expiry is cleared.
    fn put(&mut self, key: Key, value: StoredValue) -> Result<u64>;

    /// Inserts or replaces `value` with a wall-clock expiry in Unix epoch milliseconds.
    fn put_expiring(&mut self, key: Key, value: StoredValue, expires_at: i64) -> Result<u64>;

    /// Deletes `key` when present. Returns whether a record was removed.
    fn delete(&mut self, key: &Key) -> Result<bool>;

    /// Applies the whole change set as one atomic commit step.
    ///
    /// The contract is a single commit boundary: the changes are staged first (the complete
    /// post-commit state is produced and written durably), then applied atomically, and only
    /// then published to readers. Either every change in `changes` is committed and durable
    /// when this returns `Ok`, or nothing is when it returns `Err`. A sequential fold over
    /// [`Self::put`] / [`Self::delete`] is not an acceptable implementation because a crash
    /// between two persists leaves a half-committed batch on disk.
    ///
    /// Stores without a durable atomic commit boundary must return `Error::Unsupported`
    /// (the default) instead of degrading to per-change execution.
    fn commit_changes(&mut self, changes: Vec<ShardChange>) -> Result<()> {
        let _ = changes;
        Err(Error::Unsupported("store does not provide a durable atomic commit boundary"))
    }

    /// Whether [`Self::commit_changes`] survives process exit.
    ///
    /// Executors may only open transactions against stores that answer `true`; a store whose
    /// commit boundary is process-volatile (e.g. an in-memory shard) must keep the default
    /// `false` so transactions are rejected instead of being disguised by an executor-side
    /// mutex.
    fn durable_atomic_commit(&self) -> bool {
        false
    }

    /// Ordered prefix scan within one namespace.
    ///
    /// Returns up to `limit` records whose key bytes start with `prefix` and compare greater
    /// exclusive lower cursor; `before` is the exclusive upper cursor. `limit` of `0`
    /// means no cap. Results are sorted by key bytes ascending.
    fn scan(&self, namespace: &Namespace, prefix: &[u8], after: Option<&[u8]>, before: Option<&[u8]>, limit: usize) -> Result<Vec<Record>>;

    /// Physically removes records whose expiry deadline has elapsed at `now_unix_ms`.
    ///
    /// `budget` caps how many records this pass **inspects**: `Some(n)` stops after `n`
    /// records and leaves a resumable key cursor, `None` scans the whole shard. The cursor is
    /// process-local and never part of the snapshot, so a reopen starts from the shard head.
    ///
    /// Reclaim only drops records that every read path already treats as absent, so it never
    /// changes observable data. When nothing has expired the store must not touch the disk.
    /// File-backed stores persist the removal through the same durable atomic commit
    /// boundary as foreground writes: a failure leaves the previous snapshot intact and
    /// visible. Stores without a reclamation contract keep the default
    /// `Error::Unsupported`.
    fn sweep_expired(&mut self, now_unix_ms: i64, budget: Option<usize>) -> Result<SweepReport> {
        let _ = (now_unix_ms, budget);
        Err(Error::Unsupported("store does not provide expiry reclamation"))
    }

    /// Namespaces that hold at least one record, sorted ascending.
    ///
    /// A shard can host several namespaces; hosts enumerate them before
    /// reporting sizes or flushing one boundary. Stores without enumeration
    /// keep the default `Error::Unsupported`.
    fn namespaces(&self) -> Result<Vec<Namespace>> {
        Err(Error::Unsupported("store does not enumerate namespaces"))
    }

    /// Number of live records inside `namespace`.
    ///
    /// Expired records count as absent, matching [`Self::get`] and [`Self::scan`].
    fn len(&self, namespace: &Namespace) -> Result<usize> {
        let _ = namespace;
        Err(Error::Unsupported("store does not report namespace length"))
    }

    /// Removes every record inside `namespace` and returns how many were removed.
    ///
    /// Revisions continue from the shard counter so a later write never reuses
    /// a flushed revision. File-backed stores persist through the same durable
    /// boundary as [`Self::commit_changes`].
    fn flush(&mut self, namespace: &Namespace) -> Result<usize> {
        let _ = namespace;
        Err(Error::Unsupported("store does not flush namespaces"))
    }
}

/// In-memory shard used by unit tests and early integrations.
#[derive(Debug)]
pub struct MemoryShard {
    records: BTreeMap<Key, Record>,
    /// Next revision to allocate; an empty shard starts at `1`.
    next_revision: u64,
    /// Resume point for a budgeted expiry pass; process-local and not observable.
    sweep_cursor: Option<Key>,
}

impl Default for MemoryShard {
    fn default() -> Self {
        Self { records: BTreeMap::new(), next_revision: 1, sweep_cursor: None }
    }
}

impl MemoryShard {
    /// Creates an empty shard.
    pub fn new() -> Self {
        Self::default()
    }

    fn insert(&mut self, key: Key, value: StoredValue, expires_at: Option<i64>) -> Result<u64> {
        let revision = self.next_revision;
        self.next_revision = revision.checked_add(1).ok_or(Error::Corrupt("shard revision exhausted"))?;
        let record = Record { key: key.clone(), value, revision, expires_at };
        self.records.insert(key, record);
        Ok(revision)
    }
}

impl KvStore for MemoryShard {
    fn get(&self, key: &Key) -> Result<Option<Record>> {
        let now = now_unix_ms();
        Ok(self.records.get(key).cloned().filter(|record| !record.is_expired_at(now)))
    }

    fn put(&mut self, key: Key, value: StoredValue) -> Result<u64> {
        self.insert(key, value, None)
    }

    fn put_expiring(&mut self, key: Key, value: StoredValue, expires_at: i64) -> Result<u64> {
        self.insert(key, value, Some(expires_at))
    }

    fn delete(&mut self, key: &Key) -> Result<bool> {
        Ok(self.records.remove(key).is_some())
    }

    fn scan(&self, namespace: &Namespace, prefix: &[u8], after: Option<&[u8]>, before: Option<&[u8]>, limit: usize) -> Result<Vec<Record>> {
        Ok(collect_scan(&self.records, namespace, prefix, after, before, limit))
    }

    fn sweep_expired(&mut self, now_unix_ms: i64, budget: Option<usize>) -> Result<SweepReport> {
        let (expired, examined, next_cursor, capped) = collect_expired(&self.records, now_unix_ms, budget, self.sweep_cursor.as_ref());
        self.sweep_cursor = if capped { next_cursor.or_else(|| self.sweep_cursor.clone()) } else { None };
        let reclaimed = expired.len();
        for key in expired {
            self.records.remove(&key);
        }
        Ok(SweepReport { examined, reclaimed })
    }

    fn namespaces(&self) -> Result<Vec<Namespace>> {
        Ok(namespaces_of(&self.records))
    }

    fn len(&self, namespace: &Namespace) -> Result<usize> {
        let now = now_unix_ms();
        Ok(self.records.values().filter(|record| record.key.namespace == *namespace && !record.is_expired_at(now)).count())
    }

    fn flush(&mut self, namespace: &Namespace) -> Result<usize> {
        let keys: Vec<Key> = self.records.keys().filter(|key| key.namespace == *namespace).cloned().collect();
        for key in &keys {
            self.records.remove(key);
        }
        Ok(keys.len())
    }
}

/// File-backed shard storing one namespace.using the native `.yykv` snapshot format.
#[derive(Debug)]
pub struct FileShard {
    path: PathBuf,
    records: BTreeMap<Key, Record>,
    next_revision: u64,
    /// Resume point for a budgeted expiry pass; process-local and not persisted.
    sweep_cursor: Option<Key>,
}

impl FileShard {
    /// Opens an existing shard or creates an empty shard at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let bytes = fs::read(&path)?;
            decode(&path, &bytes)
        }
        else {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }
            let shard = Self { path, records: BTreeMap::new(), next_revision: 1, sweep_cursor: None };
            shard.persist()?;
            Ok(shard)
        }
    }

    /// Opens a shard that must already exist.
    ///
    /// Unlike [`Self::open`], a missing file is `Error::NotFound` instead of
    /// being created. Hosts that only read existing data use this so a typo in
    /// a shard path cannot silently install an empty shard.
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Err(Error::NotFound { key: path.display().to_string() });
        }
        let bytes = fs::read(&path)?;
        decode(&path, &bytes)
    }

    /// Returns the backing `.yykv` path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Persists the current shard snapshot.
    pub fn flush(&self) -> Result<()> {
        self.persist()
    }

    fn persist(&self) -> Result<()> {
        let bytes = encode_snapshot(&self.records, self.next_revision)?;
        write_snapshot_durable(&self.path, &bytes)
    }

    fn insert(&mut self, key: Key, value: StoredValue, expires_at: Option<i64>) -> Result<u64> {
        let revision = self.next_revision;
        let next_revision = revision.checked_add(1).ok_or(Error::Corrupt("shard revision exhausted"))?;
        let record = Record { key: key.clone(), value, revision, expires_at };
        let previous = self.records.insert(key.clone(), record);
        self.next_revision = next_revision;
        if let Err(error) = self.persist() {
            self.next_revision = revision;
            match previous {
                Some(record) => {
                    self.records.insert(key, record);
                }
                None => {
                    self.records.remove(&key);
                }
            }
            return Err(error);
        }
        Ok(revision)
    }
}

impl KvStore for FileShard {
    fn get(&self, key: &Key) -> Result<Option<Record>> {
        let now = now_unix_ms();
        Ok(self.records.get(key).cloned().filter(|record| !record.is_expired_at(now)))
    }

    fn put(&mut self, key: Key, value: StoredValue) -> Result<u64> {
        self.insert(key, value, None)
    }

    fn put_expiring(&mut self, key: Key, value: StoredValue, expires_at: i64) -> Result<u64> {
        self.insert(key, value, Some(expires_at))
    }

    fn delete(&mut self, key: &Key) -> Result<bool> {
        let Some(record) = self.records.remove(key)
        else {
            return Ok(false);
        };
        if let Err(error) = self.persist() {
            self.records.insert(key.clone(), record);
            return Err(error);
        }
        Ok(true)
    }

    fn commit_changes(&mut self, changes: Vec<ShardChange>) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut staged_records = self.records.clone();
        let mut next_revision = self.next_revision;
        for change in &changes {
            match change {
                ShardChange::Put { key, value, expires_at } => {
                    let revision = next_revision;
                    next_revision = next_revision.checked_add(1).ok_or(Error::Corrupt("shard revision exhausted"))?;
                    staged_records.insert(key.clone(), Record { key: key.clone(), value: value.clone(), revision, expires_at: *expires_at });
                }
                ShardChange::Delete { key } => {
                    staged_records.remove(key);
                }
            }
        }
        let bytes = encode_snapshot(&staged_records, next_revision)?;
        // Durable commit point: the full post-commit snapshot is fsynced to a temp file and
        // atomically replaces the shard. Memory is only published after this returns `Ok`,
        // so a crash or storage failure here leaves the previous snapshot both visible and
        // durable — never a half-applied batch.
        //
        // The one exception is a directory-sync failure after the rename already happened:
        // that error means durability is unproven, not that nothing was committed. The on-disk
        // snapshot may already be the post-commit one, so the in-memory state is deliberately
        // not published and the caller must reopen the shard instead of assuming a clean
        // rollback to the previous snapshot.
        write_snapshot_durable(&self.path, &bytes)?;
        self.records = staged_records;
        self.next_revision = next_revision;
        Ok(())
    }

    fn durable_atomic_commit(&self) -> bool {
        true
    }

    fn scan(&self, namespace: &Namespace, prefix: &[u8], after: Option<&[u8]>, before: Option<&[u8]>, limit: usize) -> Result<Vec<Record>> {
        Ok(collect_scan(&self.records, namespace, prefix, after, before, limit))
    }

    fn sweep_expired(&mut self, now_unix_ms: i64, budget: Option<usize>) -> Result<SweepReport> {
        let (expired, examined, next_cursor, capped) = collect_expired(&self.records, now_unix_ms, budget, self.sweep_cursor.as_ref());
        // The cursor only advances after a successful commit, so a storage failure retries
        // the same batch instead of skipping records that are still on disk.
        if expired.is_empty() {
            self.sweep_cursor = if capped { next_cursor.or_else(|| self.sweep_cursor.clone()) } else { None };
            // No reclamation work: the snapshot on disk stays untouched.
            return Ok(SweepReport { examined, reclaimed: 0 });
        }
        let reclaimed = expired.len();
        let changes = expired.into_iter().map(|key| ShardChange::Delete { key }).collect();
        self.commit_changes(changes)?;
        self.sweep_cursor = if capped { next_cursor.or_else(|| self.sweep_cursor.clone()) } else { None };
        Ok(SweepReport { examined, reclaimed })
    }

    fn namespaces(&self) -> Result<Vec<Namespace>> {
        Ok(namespaces_of(&self.records))
    }

    fn len(&self, namespace: &Namespace) -> Result<usize> {
        let now = now_unix_ms();
        Ok(self.records.values().filter(|record| record.key.namespace == *namespace && !record.is_expired_at(now)).count())
    }

    fn flush(&mut self, namespace: &Namespace) -> Result<usize> {
        let changes: Vec<ShardChange> =
            self.records.keys().filter(|key| key.namespace == *namespace).map(|key| ShardChange::Delete { key: key.clone() }).collect();
        let removed = changes.len();
        if removed > 0 {
            self.commit_changes(changes)?;
        }
        Ok(removed)
    }
}

/// Distinct namespaces present in a shard, sorted ascending.
fn namespaces_of(records: &BTreeMap<Key, Record>) -> Vec<Namespace> {
    let mut namespaces: Vec<Namespace> = records.keys().map(|key| key.namespace.clone()).collect();
    namespaces.sort();
    namespaces.dedup();
    namespaces
}

/// Selects expired records for one budgeted pass.
///
/// Returns the expired keys, how many records were inspected, the new resume cursor and
/// whether the budget stopped the pass before the shard end.
fn collect_expired(
    records: &BTreeMap<Key, Record>,
    now_unix_ms: i64,
    budget: Option<usize>,
    cursor: Option<&Key>,
) -> (Vec<Key>, usize, Option<Key>, bool) {
    let mut expired = Vec::new();
    let mut examined = 0;
    let mut last_examined = None;
    let mut capped = false;
    let iterator = match cursor {
        Some(cursor) => records.range((Bound::Excluded(cursor), Bound::Unbounded)),
        None => records.range(..),
    };
    for (key, record) in iterator {
        if budget.is_some_and(|limit| examined >= limit) {
            capped = true;
            break;
        }
        examined += 1;
        last_examined = Some(key.clone());
        if record.is_expired_at(now_unix_ms) {
            expired.push(key.clone());
        }
    }
    (expired, examined, last_examined, capped)
}

/// Shared scan logic: filter by namespace, prefix-match, exclusive cursors (after/before), limit.
/// Expired records are treated as absent.
///
/// 键序是 `(namespace, bytes)`，同一命名空间里带同一前缀的键是连续区间，所以这里
/// 从区间起点直接 `range` 过去，只走真正可能命中的记录。逐个 `continue` 过滤会让
/// 每次前缀探针都退化成整 shard 遍历，索引回表与唯一性判定都会因此变慢。
fn collect_scan(
    records: &BTreeMap<Key, Record>,
    namespace: &Namespace,
    prefix: &[u8],
    after: Option<&[u8]>,
    before: Option<&[u8]>,
    limit: usize,
) -> Vec<Record> {
    let now = now_unix_ms();
    let mut out = Vec::new();
    let start = Key::new(namespace.0.clone(), prefix.to_vec());
    for (key, record) in records.range(start..) {
        if key.namespace != *namespace || !key.bytes.starts_with(prefix) {
            break;
        }
        if record.is_expired_at(now) {
            continue;
        }
        if let Some(after_bytes) = after {
            if key.bytes.as_slice() <= after_bytes {
                continue;
            }
        }
        if let Some(before_bytes) = before {
            if key.bytes.as_slice() >= before_bytes {
                continue;
            }
        }
        out.push(record.clone());
        if limit > 0 && out.len() >= limit {
            break;
        }
    }
    out
}

fn write_snapshot_durable(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp =
        path.with_file_name(format!(".{}.yykv-tmp-{}", path.file_name().and_then(|name| name.to_str()).unwrap_or("shard"), std::process::id()));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new().create(true).truncate(true).write(true).open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        replace_file(&temp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn decode(path: &Path, bytes: &[u8]) -> Result<FileShard> {
    let snapshot = decode_snapshot(bytes)?;
    Ok(FileShard { path: path.to_path_buf(), records: snapshot.records, next_revision: snapshot.next_revision, sweep_cursor: None })
}

#[cfg(not(windows))]
fn replace_file(temp: &Path, destination: &Path) -> Result<()> {
    fs::rename(temp, destination).map_err(Error::Io)?;
    // `rename` 只让新的目录项在内存里可见，掉电后替换本身仍可能丢失。提交点必须再同步父目录，
    // 让目录项落盘。rename 已经发生，这里失败表示「耐久性未证实」而不是「什么都没提交」，
    // 不能假装还能完整回滚。
    let parent = destination.parent().ok_or(Error::Unsupported("shard path has no parent directory"))?;
    fs::File::open(parent)?.sync_all().map_err(Error::Io)
}

#[cfg(windows)]
fn replace_file(temp: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW, ReplaceFileW};

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    let temp_wide = wide(temp);
    let destination_wide = wide(destination);
    let result = unsafe {
        if destination.exists() {
            ReplaceFileW(destination_wide.as_ptr(), temp_wide.as_ptr(), std::ptr::null(), 0, std::ptr::null_mut(), std::ptr::null_mut())
        }
        else {
            MoveFileExW(temp_wide.as_ptr(), destination_wide.as_ptr(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)
        }
    };
    if result == 0 { Err(Error::Io(std::io::Error::last_os_error())) } else { Ok(()) }
}

/// Compare-and-set helper built on top of [`KvStore`].
pub fn compare_and_put(store: &mut dyn KvStore, key: Key, expect_revision: Option<u64>, value: StoredValue) -> Result<u64> {
    let current = store.get(&key)?;
    match (expect_revision, current) {
        (None, Some(_)) => Err(Error::CasConflict { key: key.display() }),
        (Some(expected), Some(record)) if record.revision != expected => Err(Error::CasConflict { key: key.display() }),
        _ => store.put(key, value),
    }
}
