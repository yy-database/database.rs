use super::{Key, StoredValue};

/// One versioned KV record inside a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Record key.
    pub key: Key,
    /// Stored payload.
    pub value: StoredValue,
    /// Monotonic revision inside the shard.
    pub revision: u64,
    /// Optional wall-clock expiry as Unix epoch milliseconds.
    ///
    /// `None` means the record never expires. A record whose deadline has elapsed is
    /// treated as absent by every read path (`get`, `scan`, conditional writes); the
    /// physical bytes stay in the shard until the key is next written or deleted, or until
    /// `KvStore::sweep_expired` reclaims it.
    pub expires_at: Option<i64>,
}

impl Record {
    /// Creates a record without a shard-assigned revision.
    pub fn new(key: Key, value: StoredValue) -> Self {
        Self { key, value, revision: 0, expires_at: None }
    }

    /// Whether this record carries an expiry deadline that has elapsed at `now_unix_ms`.
    pub fn is_expired_at(&self, now_unix_ms: i64) -> bool {
        self.expires_at.is_some_and(|deadline| deadline <= now_unix_ms)
    }
}
