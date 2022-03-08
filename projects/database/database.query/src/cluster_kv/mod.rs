//! Protocol-neutral key/value commands consumed by 集群 execution.

use database_types::cluster::Namespace;

/// One operation on a namespace-scoped opaque key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValueCommand {
    /// Logical tenant or key-space boundary.
    pub namespace: Namespace,
    /// Opaque key bytes used for stable shard routing.
    pub key: Vec<u8>,
    /// Requested operation.
    pub action: KeyValueAction,
}

/// Read, replace, or delete one logical key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyValueAction {
    /// Return the current value, if any.
    Get,
    /// Check whether the key currently has a value.
    Exists,
    /// Return the current value and remove the key atomically.
    GetDelete,
    /// Return the current value and replace it atomically.
    GetSet(Vec<u8>),
    /// Replace the current value with these bytes.
    Put(Vec<u8>),
    /// Store these bytes only when the key is absent.
    PutIfAbsent(Vec<u8>),
    /// Store these bytes only when the key already exists.
    PutIfPresent(Vec<u8>),
    /// Remove the current value if it exists.
    Delete,
    /// Atomically add a signed delta to a decimal integer value, treating an absent key as zero.
    IncrementBy(i64),
    /// Replace the current value with these bytes and set a wall-clock expiry (Unix epoch milliseconds).
    PutExpiring {
        /// Bytes to store.
        value: Vec<u8>,
        /// Expiry deadline in Unix epoch milliseconds.
        expires_at: i64,
    },
    /// Store these bytes with an expiry only when the key is absent.
    PutIfAbsentExpiring {
        /// Bytes to store.
        value: Vec<u8>,
        /// Expiry deadline in Unix epoch milliseconds.
        expires_at: i64,
    },
    /// Store these bytes with an expiry only when the key already exists.
    PutIfPresentExpiring {
        /// Bytes to store.
        value: Vec<u8>,
        /// Expiry deadline in Unix epoch milliseconds.
        expires_at: i64,
    },
    /// Replace the current value with these bytes, keeping the deadline of the previous record.
    PutKeepingExpiry(Vec<u8>),
    /// Replace an existing key with these bytes, keeping its deadline; absent keys are left untouched.
    PutIfPresentKeepingExpiry(Vec<u8>),
    /// Set a wall-clock expiry (Unix epoch milliseconds) on an existing key without touching its value.
    SetExpiry {
        /// Expiry deadline in Unix epoch milliseconds.
        expires_at: i64,
    },
    /// Remove the expiry from an existing key without touching its value.
    ClearExpiry,
    /// Read whether the key is present and its current expiry deadline.
    GetExpiry,
    /// Read the current record's revision without materialising its value.
    ///
    /// Every successful write path assigns a fresh shard-monotonic revision, so this is the
    /// authoritative mutation version: a byte-identical rewrite still advances it.
    GetRevision,
    /// Move the record to another key in the same shard, keeping its value and deadline.
    Rename {
        /// Destination key bytes.
        destination: Vec<u8>,
        /// Move only when the destination is absent.
        if_absent: bool,
    },
}

/// Result of applying one [`KeyValueCommand`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyValueResult {
    /// Value returned by a read.
    Get(Option<Vec<u8>>),
    /// Whether the key currently has a value.
    Exists(bool),
    /// Value returned before an atomic delete.
    GetDelete(Option<Vec<u8>>),
    /// Value returned before an atomic replacement.
    GetSet(Option<Vec<u8>>),
    /// 成功写入后分配的修订号。
    Put {
        /// 新记录修订号。
        revision: u64,
    },
    /// 条件写入在键不存在时插入；若已存在则为 `None`。
    PutIfAbsent {
        /// 插入成功时的修订号；键已存在则为 `None`。
        revision: Option<u64>,
    },
    /// 条件写入在键存在时替换；若缺失则为 `None`。
    PutIfPresent {
        /// 替换成功时的修订号；键缺失则为 `None`。
        revision: Option<u64>,
    },
    /// 删除是否找到并移除了值。
    Delete {
        /// `true` 表示原先存在并已删除。
        removed: bool,
    },
    /// 原子有符号整数自增结果。
    Increment {
        /// 自增后的值。
        value: i64,
    },
    /// [`KeyValueAction::GetExpiry`] 观察到的存活与截止时间。
    Expiry {
        /// 键当前是否仍有存活值。
        present: bool,
        /// 存活记录上的截止 Unix 毫秒；无过期则为 `None`。
        expires_at: Option<i64>,
    },
    /// [`KeyValueAction::SetExpiry`] / [`KeyValueAction::ClearExpiry`] 是否作用到存活键。
    ExpiryChanged {
        /// `true` 表示过期元数据已变更。
        applied: bool,
    },
    /// [`KeyValueAction::GetRevision`] 观察到的记录修订号。
    Revision(
        /// 存活记录的修订号；键缺失（含已过期）为 `None`。
        Option<u64>,
    ),
    /// [`KeyValueAction::Rename`] 是否找到源键并完成搬迁。
    Rename {
        /// 搬迁时源键是否有存活记录。
        source_present: bool,
        /// 是否已搬到目标键。
        applied: bool,
    },
}

/// Outcome of one physical expiry reclamation pass.
pub use database_types::cluster::SweepReport;
