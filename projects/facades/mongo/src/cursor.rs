//! 调用方自有的游标表。
//!
//! 这里没有全局注册表：每个宿主（TypeScript、Python、Rust 测试）持有自己的
//! [`CursorStore`]，并把它传给命令原语。回收是惰性的，空闲游标只会在下一次游标
//! 操作时被移除。

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use database_protocol_mongo::bson::{encode_bson, BsonValue};

/// Idle time after which a cursor is reclaimed.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// First batch `find` returns when `batchSize` is absent, matching `mongod`.
pub const DEFAULT_FIRST_BATCH: usize = 101;

/// Documents are packed into a batch until the next one would push the reply
/// past the BSON object ceiling `mongod 8.0.9` applies to a batch.
pub const MAX_BATCH_BYTES: usize = 16_777_216;

/// Encoded size of one cursor document.
fn document_bytes(value: &BsonValue) -> usize {
    match value {
        BsonValue::Document(document) => encode_bson(document).len(),
        _ => 0,
    }
}

/// Drains up to `limit` documents while staying under [`MAX_BATCH_BYTES`].
///
/// One document is always taken, so a cursor that holds a single very large
/// document still answers instead of stalling on the byte cap.
fn take_batch(documents: &mut Vec<BsonValue>, limit: usize) -> Vec<BsonValue> {
    let mut count = 0_usize;
    let mut bytes = 0_usize;
    for document in documents.iter() {
        if count >= limit {
            break;
        }
        let size = document_bytes(document);
        if count > 0 && bytes + size > MAX_BATCH_BYTES {
            break;
        }
        bytes += size;
        count += 1;
    }
    documents.drain(..count).collect()
}

/// One open cursor.
#[derive(Debug)]
pub struct Cursor {
    /// Namespace reported back in `cursor.ns` replies.
    pub namespace: String,
    /// Documents the next `getMore` returns.
    pub remaining: Vec<BsonValue>,
    /// Set when the collection was dropped after the cursor was opened.
    killed: bool,
    last_used: Instant,
}

/// Why one `getMore` could not return documents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetMoreError {
    /// No live cursor carries that id.
    NotFound(i64),
    /// The request named a namespace the cursor does not belong to. `mongod`
    /// refuses it with `Unauthorized` instead of serving the other namespace.
    NamespaceMismatch {
        /// Namespace the request asked for, as `<database>.<collection>`.
        requested: String,
        /// Namespace the cursor was opened on.
        cursor: String,
    },
    /// The collection behind the cursor was dropped. The cursor is removed by
    /// this `getMore`, exactly like `mongod 8.0.9`: one `QueryPlanKilled` reply,
    /// then `CursorNotFound`.
    CollectionDropped {
        /// Cursor identifier the client sent.
        id: i64,
        /// Namespace the dropped collection lived in.
        namespace: String,
    },
}

/// Per-host cursor table.
#[derive(Debug)]
pub struct CursorStore {
    cursors: HashMap<i64, Cursor>,
    next_id: i64,
    idle_timeout: Duration,
}

impl Default for CursorStore {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorStore {
    /// Creates an empty table with the documented 600 second reclaim window.
    pub fn new() -> Self {
        Self::with_idle_timeout(DEFAULT_IDLE_TIMEOUT)
    }

    /// Creates an empty table with an explicit reclaim window.
    pub fn with_idle_timeout(idle_timeout: Duration) -> Self {
        Self { cursors: HashMap::new(), next_id: 1, idle_timeout }
    }

    /// Number of live cursors.
    pub fn len(&self) -> usize {
        self.cursors.len()
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.cursors.is_empty()
    }

    /// Whether one cursor id is still live.
    pub fn contains(&self, id: i64) -> bool {
        self.cursors.contains_key(&id)
    }

    /// Reclaim window currently in use.
    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// Stores the unsent documents and returns the first batch.
    ///
    /// `None` means `batchSize` was absent, which `mongod` answers with the
    /// default 101 documents. An explicit `Some(0)` opens the cursor with an
    /// empty first batch, so the caller has to `getMore` for every document.
    /// A cursor id of `0` means everything fit into the first batch.
    pub fn open(&mut self, namespace: &str, mut documents: Vec<BsonValue>, batch_size: Option<usize>) -> (i64, Vec<BsonValue>) {
        self.reclaim();
        let first_batch = take_batch(&mut documents, batch_size.unwrap_or(DEFAULT_FIRST_BATCH));
        if documents.is_empty() {
            return (0, first_batch);
        }
        let id = self.next_id;
        self.next_id += 1;
        self.cursors.insert(id, Cursor { namespace: namespace.to_string(), remaining: documents, killed: false, last_used: Instant::now() });
        (id, first_batch)
    }

    /// Drains the next batch, filling it up to [`MAX_BATCH_BYTES`].
    ///
    /// `namespace` is the namespace the request asks for. A cursor belongs to the
    /// namespace it was opened on, so a request that names another one is refused
    /// before any document is returned.
    ///
    /// `None` (absent or zero `batchSize`) drains everything that fits, as
    /// `mongod 8.0.9` does for `getMore`.
    pub fn get_more(&mut self, id: i64, namespace: &str, batch_size: Option<usize>) -> Result<(Vec<BsonValue>, String), GetMoreError> {
        self.reclaim();
        let Some(cursor) = self.cursors.get(&id)
        else {
            return Err(GetMoreError::NotFound(id));
        };
        if cursor.namespace != namespace {
            return Err(GetMoreError::NamespaceMismatch { requested: namespace.to_string(), cursor: cursor.namespace.clone() });
        }
        if cursor.killed {
            let namespace = cursor.namespace.clone();
            self.cursors.remove(&id);
            return Err(GetMoreError::CollectionDropped { id, namespace });
        }
        let cursor = self.cursors.get_mut(&id).expect("cursor was just read");
        cursor.last_used = Instant::now();
        let namespace = cursor.namespace.clone();
        let batch = take_batch(&mut cursor.remaining, batch_size.unwrap_or(usize::MAX));
        let drained = cursor.remaining.is_empty();
        if drained {
            self.cursors.remove(&id);
        }
        Ok((batch, namespace))
    }

    /// Marks every cursor on `database` as killed by the drop.
    ///
    /// The cursor stays in the table so `killCursors` still reports it as killed,
    /// and the first `getMore` afterwards answers `QueryPlanKilled` and removes
    /// it. Dropping another database never touches these entries.
    pub fn drop_database(&mut self, database: &str) -> usize {
        let prefix = format!("{database}.");
        let mut killed = 0;
        for cursor in self.cursors.values_mut() {
            if !cursor.killed && cursor.namespace.starts_with(&prefix) {
                cursor.killed = true;
                cursor.remaining.clear();
                killed += 1;
            }
        }
        killed
    }

    /// Marks every cursor on one namespace as killed by a collection drop.
    ///
    /// Like [`Self::drop_database`], the entry stays in the table so the first
    /// `getMore` answers `QueryPlanKilled` and removes it.
    pub fn drop_collection(&mut self, namespace: &str) -> usize {
        let mut killed = 0;
        for cursor in self.cursors.values_mut() {
            if !cursor.killed && cursor.namespace == namespace {
                cursor.killed = true;
                cursor.remaining.clear();
                killed += 1;
            }
        }
        killed
    }

    /// Removes the given cursors and reports which ids were alive.
    pub fn kill(&mut self, identifiers: &[i64]) -> (Vec<i64>, Vec<i64>) {
        let mut killed = Vec::new();
        let mut not_found = Vec::new();
        for id in identifiers {
            if self.cursors.remove(id).is_some() {
                killed.push(*id);
            }
            else {
                not_found.push(*id);
            }
        }
        (killed, not_found)
    }

    /// Drops cursors idle for longer than the reclaim window.
    pub fn reclaim(&mut self) {
        let idle_timeout = self.idle_timeout;
        self.cursors.retain(|_, cursor| cursor.last_used.elapsed() < idle_timeout);
    }
}
