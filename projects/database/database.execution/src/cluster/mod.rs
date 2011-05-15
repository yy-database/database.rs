#![deny(missing_debug_implementations)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
//! 分布式执行面：分片路由、所有权 fencing 与键值命令。

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use database_cluster::topology::ShardMap;
use database_query::cluster_kv::{KeyValueAction, KeyValueCommand, KeyValueResult};
use database_storage::cluster_kv::{FileShard, InlineValue, Key, KvStore, ShardChange, StoredValue, now_unix_ms};
use database_types::cluster::{Error, FenceToken, Namespace, OwnerEntry, ShardId, SweepReport};

mod sweep;

pub use sweep::{DEFAULT_SWEEP_BUDGET_PER_SHARD, ExpirySweeper};

/// Execution surface consumed by protocol adapters.
pub trait KeyValueExecutor: Debug + Send + Sync {
    /// Routes and applies one namespace-scoped command.
    ///
    /// `fence` is an optional ownership token that a coordinator passes down to prove it
    /// still holds the lease for the target shard. When `Some(token)` is supplied, the
    /// executor must reject the request if its catalog-published owner record no longer
    /// matches the token's leader and term. `None` skips fencing (single-node gateways).
    fn execute(&self, fence: Option<&FenceToken>, command: KeyValueCommand) -> database_types::cluster::Result<KeyValueResult>;

    /// Reads an ordered batch atomically, preserving duplicate keys.
    /// Executors without an atomic read contract must reject the request.
    fn execute_read_batch(
        &self,
        fence: Option<&FenceToken>,
        _commands: &[KeyValueCommand],
    ) -> database_types::cluster::Result<Vec<KeyValueResult>> {
        let _ = fence;
        Err(Error::Unsupported("atomic batch reads are not implemented by this executor"))
    }

    /// Ordered prefix scan across all mounted shards for one namespace.
    ///
    /// Returns up to `limit` (key_bytes, inline_value_bytes) pairs whose key bytes start with
    /// `prefix`, compare greater than `after` (exclusive), less than `before` (exclusive), sorted
    /// by key bytes ascending.
    fn execute_scan(
        &self,
        fence: Option<&FenceToken>,
        _namespace: &Namespace,
        _prefix: &[u8],
        _after: Option<&[u8]>,
        _before: Option<&[u8]>,
        _limit: usize,
    ) -> database_types::cluster::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let _ = fence;
        Err(Error::Unsupported("scan is not implemented by this executor"))
    }

    /// Commits a buffered transaction with OCC validation under the caller's ownership context.
    ///
    /// Each element of `batch` carries the write command together with the original conditional
    /// (if any). Implementations must hold an exclusive executor-wide lock while re-checking
    /// every `BufferedCommit::condition` against the base store and then applying the writes
    /// through a single durable atomic commit boundary, so that a concurrent transaction cannot
    /// slip between validate and apply and a rejected condition — or a mid-commit storage
    /// failure — never leaves a half-committed batch. A sequential per-command replay is not an
    /// acceptable implementation: a process exit between two applies would persist a partial
    /// batch.
    ///
    /// `fence` is validated at the boundary where the writes are applied: when `Some(token)` is
    /// supplied the implementation must reject the commit if the token no longer matches the
    /// catalog-published owner of every routed shard. `None` keeps the single-node local mode
    /// (no owner context). Executors without an atomic OCC-validated commit contract must
    /// return `Error::Unsupported` (the default) instead of replaying commands one by one, and
    /// an executor whose routed store lacks a durable atomic commit boundary must reject the
    /// commit rather than fall back to an in-process lock.
    ///
    /// The committed batch must route to exactly one shard: the single-node atomic commit scope
    /// is a single shard, and a batch whose writes spread across shards is rejected with a
    /// stable `Error::Unsupported` before anything is applied.
    fn commit_batch(&self, fence: Option<&FenceToken>, batch: Vec<BufferedCommit>) -> database_types::cluster::Result<()> {
        let _ = (fence, batch);
        Err(Error::Unsupported("atomic OCC-validated batch commits are not implemented by this executor"))
    }

    /// Opens a local transaction handle bound to this executor and the caller's ownership context.
    ///
    /// The handle lets callers issue commands through a transaction boundary so that a gateway
    /// can bind `BEGIN` / `COMMIT` / `ROLLBACK` protocol verbs to a real typed object. The
    /// `fence` (routing epoch + leader term + owner node) is captured at begin time and carried
    /// verbatim into [`KeyValueExecutor::commit_batch`] at commit time: a transaction opened by a
    /// coordinator that loses its lease later must fail at the commit boundary instead of
    /// committing under a default `None`. Single-node gateways pass `None` explicitly.
    ///
    /// The atomic scope of a committed transaction is **one shard** in this product phase:
    /// buffered writes that route to different shards are rejected with a stable
    /// `Error::Unsupported` at commit time and nothing is applied. Cross-shard (distributed)
    /// transactions are not disguised as ordinary local transactions. Executors without
    /// transaction support return `Error::Unsupported`.
    ///
    /// A transaction may only be opened when every mounted shard provides a durable atomic
    /// commit boundary (see `database_storage::cluster_kv::KvStore::durable_atomic_commit`). A store whose state
    /// does not survive process exit must have its transactions rejected at begin time — an
    /// executor-side mutex does not make a volatile commit durable.
    fn begin_transaction(self: Arc<Self>, _fence: Option<&FenceToken>) -> database_types::cluster::Result<Box<dyn TransactionHandle>> {
        Err(Error::Unsupported("this executor does not support local transactions"))
    }

    /// Reclaims records whose expiry deadline has elapsed, returning how many records were
    /// inspected and physically removed across all mounted shards.
    ///
    /// `budget_per_shard` caps the records inspected per shard in this pass; `None` scans each
    /// shard completely (the one-shot maintenance entry). Reclaim is a maintenance operation,
    /// not a transaction: each shard is swept under its own lock and persists through its own
    /// durable commit boundary. A failure on one shard leaves that shard's previous snapshot
    /// intact, does not undo shards already swept, and does not stop the remaining shards.
    /// Reclaimed records were already invisible to every read path, so sweeping never
    /// changes observable data, and the foreground request path never performs reclamation
    /// work — it only shares the per-shard lock for the duration of one ordinary write.
    ///
    /// Executors without a reclamation contract must return `Error::Unsupported`
    /// (the default).
    fn sweep_expired(&self, budget_per_shard: Option<usize>) -> database_types::cluster::Result<SweepReport> {
        let _ = budget_per_shard;
        Err(Error::Unsupported("expiry reclamation is not implemented by this executor"))
    }
}

/// One buffered write ready to be committed.
#[derive(Debug, Clone)]
pub struct BufferedCommit {
    /// The command to replay against the base store.
    pub command: KeyValueCommand,
    /// Optional original conditional that must still hold on the base store at commit time.
    pub condition: Option<CommitCondition>,
}

/// OCC pre-image condition checked at commit time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitCondition {
    /// The key must be absent from the base store (PutIfAbsent).
    PutIfAbsent,
    /// The key must be present in the base store (PutIfPresent).
    PutIfPresent,
    /// The base-store value must still equal this snapshot (`None` means the key must be absent).
    ///
    /// Used for read-modify-write SQL statements so that two transactions cannot both read the
    /// same pre-image and then overwrite each other in turn.
    ValueEquals(Option<Vec<u8>>),
}

/// A local transaction handle that exposes the same command surface as the executor but
/// wraps it in a begin/commit/rollback contract.
///
/// The reference implementation (`LocalTransactionHandle`) buffers every write until commit,
/// serves reads from that buffer before falling back to the executor, and re-checks OCC
/// pre-image conditions under the executor-wide lock at commit time.
pub trait TransactionHandle: Debug {
    /// Executes one command within the transaction.
    fn execute(&self, command: KeyValueCommand) -> database_types::cluster::Result<KeyValueResult>;

    /// Reads an ordered batch within the transaction, preserving duplicate keys.
    fn execute_read_batch(&self, _commands: &[KeyValueCommand]) -> database_types::cluster::Result<Vec<KeyValueResult>> {
        Err(Error::Unsupported("transaction batch reads are not implemented by this handle"))
    }

    /// Ordered prefix scan within the transaction, merging buffered writes over the executor.
    ///
    /// Handles without a transaction snapshot must reject the request instead of silently
    /// reading the base store, otherwise read-your-writes is violated.
    fn execute_scan(
        &self,
        _namespace: &Namespace,
        _prefix: &[u8],
        _after: Option<&[u8]>,
        _before: Option<&[u8]>,
        _limit: usize,
    ) -> database_types::cluster::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Err(Error::Unsupported("transaction scans are not implemented by this handle"))
    }

    /// Buffers a `Put` / `Delete` whose base-store pre-image must equal `expected` at commit time
    /// (`None` means the key must be absent).
    ///
    /// Callers use this for read-modify-write statements so that a concurrent commit between
    /// their read and their commit is rejected by OCC instead of being overwritten. Handles
    /// without OCC support must reject the request rather than degrade to an unconditional write.
    fn write_if_unchanged(&self, _command: KeyValueCommand, _expected: Option<Vec<u8>>) -> database_types::cluster::Result<KeyValueResult> {
        Err(Error::Unsupported("conditional transaction writes are not implemented by this handle"))
    }

    /// Finalises the transaction and persists any pending state.
    fn commit(self: Box<Self>) -> database_types::cluster::Result<()>;

    /// Aborts the transaction and discards any pending state.
    fn rollback(self: Box<Self>);

    /// Creates a named savepoint that can be restored to later.
    ///
    /// Handles without savepoint support must return `Error::Unsupported` rather than silently
    /// accepting the savepoint name, otherwise clients will believe rollback-to works when it
    /// does not.
    fn savepoint_create(&self, _name: &str) -> database_types::cluster::Result<()> {
        Err(Error::Unsupported("savepoints are not implemented by this handle"))
    }

    /// Rolls the transaction back to a previously created savepoint without ending it.
    ///
    /// Implementations must reject unknown savepoint names with `Error::Unsupported` or a
    /// dedicated error so callers can distinguish missing names from other failures.
    fn savepoint_rollback_to(&self, _name: &str) -> database_types::cluster::Result<()> {
        Err(Error::Unsupported("savepoints are not implemented by this handle"))
    }

    /// Removes a savepoint without rolling anything back. Subsequent rollback-to that name
    /// must fail.
    fn savepoint_release(&self, _name: &str) -> database_types::cluster::Result<()> {
        Err(Error::Unsupported("savepoints are not implemented by this handle"))
    }
}

mod local;
pub use local::{LocalExecutor, LocalTransactionHandle};
