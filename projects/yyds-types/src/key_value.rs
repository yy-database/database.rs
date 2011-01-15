//! Protocol-neutral key/value commands consumed by YYDS execution.

use crate::Namespace;

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
    /// Revision assigned to a successful write.
    Put { revision: u64 },
    /// Revision assigned when a conditional write inserts the key, or `None` when it exists.
    PutIfAbsent { revision: Option<u64> },
    /// Revision assigned when a conditional write replaces the key, or `None` when absent.
    PutIfPresent { revision: Option<u64> },
    /// Whether a delete found and removed a value.
    Delete { removed: bool },
    /// Result of an atomic signed integer increment.
    Increment { value: i64 },
    /// Presence and expiry deadline observed by [`KeyValueAction::GetExpiry`].
    Expiry {
        /// Whether the key currently has a live value.
        present: bool,
        /// Deadline in Unix epoch milliseconds, if the live record carries one.
        expires_at: Option<i64>,
    },
    /// Whether a [`KeyValueAction::SetExpiry`] or [`KeyValueAction::ClearExpiry`] applied to a live key.
    ExpiryChanged { applied: bool },
    /// Whether a [`KeyValueAction::Rename`] found the source and moved it.
    Rename {
        /// Whether the source key had a live record when the move ran.
        source_present: bool,
        /// Whether the record was moved to the destination.
        applied: bool,
    },
}

/// Outcome of one physical expiry reclamation pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Records inspected in this pass.
    pub examined: usize,
    /// Records physically removed in this pass.
    pub reclaimed: usize,
}
