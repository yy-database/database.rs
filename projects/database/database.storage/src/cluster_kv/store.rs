use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    ops::Bound,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use database_formats::yy::yykv::{KV_MAGIC, Key, Record, StoredValue, decode_snapshot, encode_snapshot, kv_format::checksum as yykv_checksum};
use database_types::cluster::{Error, Namespace, Result, SweepReport};

/// Offset of the snapshot body inside the file: magic + body checksum. The checksum covers
/// the whole body, including the `next_revision` and record-count header fields.
const BODY_OFFSET: usize = KV_MAGIC.len() + 8;

/// Byte length of a snapshot header: magic + body checksum + `next_revision` + record count.
///
/// Everything after this prefix is a concatenation of per-record segments in key order, and
/// each segment depends only on its own record, so a clean record's bytes can be reused
/// verbatim across commits. `file_shard` tests pin this against a full `encode_snapshot`.
const RECORD_AREA_OFFSET: usize = BODY_OFFSET + 8 + 4;

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
    /// Snapshot bytes of every record as of the last durable publish, keyed and ordered
    /// exactly like `records`. A commit re-encodes only the records its changes touch and
    /// reuses these segments verbatim for the rest, so the assembled snapshot stays
    /// byte-identical to a full `encode_snapshot` of the same state.
    encoded: BTreeMap<Key, Vec<u8>>,
    /// Total byte length of the `encoded` segments; sizes the assembled snapshot.
    encoded_len: usize,
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
            let shard = Self {
                path,
                records: BTreeMap::new(),
                encoded: BTreeMap::new(),
                encoded_len: 0,
                next_revision: 1,
                sweep_cursor: None,
            };
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

    /// Full re-encode of the shard through the durable commit boundary.
    ///
    /// This is the byte-for-byte oracle for the incremental path below: it calls the format
    /// encoder directly and is the only persist that does not consult the `encoded` cache.
    fn persist(&self) -> Result<()> {
        let bytes = encode_snapshot(&self.records, self.next_revision)?;
        write_snapshot_durable(&self.path, &bytes)
    }

    /// Encodes the snapshot segment of one record.
    ///
    /// The `.yykv` body is a fixed header followed by per-record segments in key order, and a
    /// segment depends only on its own record, so running the full encoder over a one-entry
    /// map reproduces exactly the bytes the whole-shard encoder would write for this record.
    /// Reusing the format encoder (instead of a second copy of its record writer) keeps the
    /// cache byte-identical to a full re-encode by construction.
    fn encode_record(record: &Record) -> Result<Vec<u8>> {
        let next_revision = record.revision.checked_add(1).ok_or(Error::Corrupt("shard revision exhausted"))?;
        let mut single = BTreeMap::new();
        single.insert(record.key.clone(), record.clone());
        let bytes = encode_snapshot(&single, next_revision)?;
        bytes.get(RECORD_AREA_OFFSET..).map(<[u8]>::to_vec).ok_or(Error::Corrupt("yykv single-record encoding missing body"))
    }

    /// Rebuilds the per-record byte cache from `records`; done once when a shard is opened.
    fn rebuild_encoded(&mut self) -> Result<()> {
        let mut encoded = BTreeMap::new();
        let mut encoded_len = 0;
        for record in self.records.values() {
            let bytes = Self::encode_record(record)?;
            encoded_len += bytes.len();
            encoded.insert(record.key.clone(), bytes);
        }
        self.encoded = encoded;
        self.encoded_len = encoded_len;
        Ok(())
    }

    fn put_encoded(&mut self, key: Key, bytes: Vec<u8>) -> Option<Vec<u8>> {
        self.encoded_len += bytes.len();
        let previous = self.encoded.insert(key, bytes);
        if let Some(previous) = &previous {
            self.encoded_len -= previous.len();
        }
        previous
    }

    fn remove_encoded(&mut self, key: &Key) -> Option<Vec<u8>> {
        let previous = self.encoded.remove(key);
        if let Some(previous) = &previous {
            self.encoded_len -= previous.len();
        }
        previous
    }

    /// Assembles the snapshot from the per-record byte cache: only the segments the caller
    /// just re-encoded are new; every clean record is copied verbatim. The header, the
    /// record validation and the body checksum reproduce `encode_snapshot` exactly, so the
    /// output is byte-identical to a full re-encode of the same state.
    fn assemble_snapshot(&self, next_revision: u64) -> Result<Vec<u8>> {
        if next_revision == 0 {
            return Err(Error::Unsupported("yykv next_revision must be at least 1"));
        }
        let count = u32::try_from(self.records.len()).map_err(|_| Error::Unsupported("too many shard records"))?;
        if self.records.len() != self.encoded.len() {
            return Err(Error::Corrupt("shard record encoding cache out of sync"));
        }
        let mut bytes = Vec::with_capacity(RECORD_AREA_OFFSET + self.encoded_len);
        bytes.extend_from_slice(KV_MAGIC);
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&next_revision.to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        for ((key, record), (cached_key, segment)) in self.records.iter().zip(self.encoded.iter()) {
            if key != cached_key {
                return Err(Error::Corrupt("shard record encoding cache out of sync"));
            }
            if record.revision == 0 || record.revision >= next_revision {
                return Err(Error::Unsupported("yykv revision must be below next_revision"));
            }
            bytes.extend_from_slice(segment);
        }
        let body_checksum = yykv_checksum(&bytes[BODY_OFFSET..]);
        bytes[KV_MAGIC.len()..BODY_OFFSET].copy_from_slice(&body_checksum.to_le_bytes());
        Ok(bytes)
    }

    /// Incremental counterpart of [`Self::persist`]: assemble from the byte cache and write
    /// through the same durable commit boundary.
    fn persist_incremental(&mut self, next_revision: u64) -> Result<()> {
        let bytes = self.assemble_snapshot(next_revision)?;
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
        write_snapshot_durable(&self.path, &bytes)
    }

    fn apply_put(&mut self, record: Record) -> Result<()> {
        let encoded = Self::encode_record(&record)?;
        let key = record.key.clone();
        self.records.insert(key.clone(), record);
        self.put_encoded(key, encoded);
        Ok(())
    }

    fn apply_delete(&mut self, key: &Key) {
        self.records.remove(key);
        self.remove_encoded(key);
    }

    fn insert(&mut self, key: Key, value: StoredValue, expires_at: Option<i64>) -> Result<u64> {
        let revision = self.next_revision;
        let next_revision = revision.checked_add(1).ok_or(Error::Corrupt("shard revision exhausted"))?;
        let mut journal = CommitJournal::new();
        journal.capture(self, &key);
        if let Err(error) = self.apply_put(Record { key, value, revision, expires_at }) {
            journal.restore(self);
            return Err(error);
        }
        if let Err(error) = self.persist_incremental(next_revision) {
            journal.restore(self);
            return Err(error);
        }
        self.next_revision = next_revision;
        Ok(revision)
    }
}

/// Pre-images of the keys one commit touches, so a failed durable write restores the exact
/// pre-commit memory state instead of leaving a half-applied batch visible to readers.
struct CommitJournal {
    seen: BTreeSet<Key>,
    entries: Vec<(Key, Option<Record>, Option<Vec<u8>>)>,
}

impl CommitJournal {
    fn new() -> Self {
        Self { seen: BTreeSet::new(), entries: Vec::new() }
    }

    /// Records the pre-image of `key` once; repeated touches in the same batch are no-ops.
    fn capture(&mut self, shard: &FileShard, key: &Key) {
        if self.seen.insert(key.clone()) {
            self.entries.push((key.clone(), shard.records.get(key).cloned(), shard.encoded.get(key).cloned()));
        }
    }

    fn restore(self, shard: &mut FileShard) {
        for (key, record, segment) in self.entries.into_iter().rev() {
            match record {
                Some(record) => {
                    shard.records.insert(key.clone(), record);
                }
                None => {
                    shard.records.remove(&key);
                }
            }
            match segment {
                Some(segment) => {
                    shard.put_encoded(key, segment);
                }
                None => {
                    shard.remove_encoded(&key);
                }
            }
        }
    }
}

fn change_key(change: &ShardChange) -> &Key {
    match change {
        ShardChange::Put { key, .. } => key,
        ShardChange::Delete { key } => key,
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
        if !self.records.contains_key(key) {
            return Ok(false);
        }
        let mut journal = CommitJournal::new();
        journal.capture(self, key);
        self.apply_delete(key);
        if let Err(error) = self.persist_incremental(self.next_revision) {
            journal.restore(self);
            return Err(error);
        }
        Ok(true)
    }

    fn commit_changes(&mut self, changes: Vec<ShardChange>) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        // The revision budget is checked before any memory is touched, so an exhausted
        // counter leaves the shard exactly as it was, matching the pre-staging contract.
        let puts = changes.iter().filter(|change| matches!(change, ShardChange::Put { .. })).count() as u64;
        let next_revision = self.next_revision.checked_add(puts).ok_or(Error::Corrupt("shard revision exhausted"))?;
        let mut journal = CommitJournal::new();
        for change in &changes {
            journal.capture(self, change_key(change));
        }
        // Stage in place: only the touched records are re-encoded; every clean record keeps
        // its cached snapshot bytes. A failure at any step restores the journaled pre-images,
        // so the visible state is either the whole batch or nothing.
        let mut revision = self.next_revision;
        for change in &changes {
            let applied = match change {
                ShardChange::Put { key, value, expires_at } => {
                    self.apply_put(Record { key: key.clone(), value: value.clone(), revision, expires_at: *expires_at })
                }
                ShardChange::Delete { key } => {
                    self.apply_delete(key);
                    Ok(())
                }
            };
            if let Err(error) = applied {
                journal.restore(self);
                return Err(error);
            }
            if matches!(change, ShardChange::Put { .. }) {
                revision += 1;
            }
        }
        if let Err(error) = self.persist_incremental(next_revision) {
            journal.restore(self);
            return Err(error);
        }
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
    let mut shard =
        FileShard { path: path.to_path_buf(), records: snapshot.records, encoded: BTreeMap::new(), encoded_len: 0, next_revision: snapshot.next_revision, sweep_cursor: None };
    shard.rebuild_encoded()?;
    Ok(shard)
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
