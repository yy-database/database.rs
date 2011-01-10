//! 分布式身份与 leader fencing 合同。
//!
//! These identifiers are referenced by [`crate::cluster::Error`], so they live in
//! the shared type package. Cluster code re-uses them instead of redefining.

use crate::cluster::identity::ShardId;

/// Monotonic routing-map generation used to reject stale writers and readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ShardEpoch(pub u64);

/// Stable identity of a node hosting a shard replica.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReplicaNodeId(pub String);

/// Monotonic term used to fence an old leader after a new election.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LeaderTerm(pub u64);

/// A token carried by a write or lease renewal to prove current leadership.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase", deny_unknown_fields))]
pub struct FenceToken {
    /// Routing epoch for the shard placement.
    pub epoch: ShardEpoch,
    /// Leader election term.
    pub term: LeaderTerm,
    /// Node that owns the lease.
    pub leader: ReplicaNodeId,
}

/// Catalog-published owner record for one shard.
///
/// The catalog stores one `OwnerEntry` per shard; the execution layer later
/// matches an incoming `FenceToken` against this record to reject stale leaders.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase", deny_unknown_fields))]
pub struct OwnerEntry {
    /// The shard whose owner this record describes.
    pub shard_id: ShardId,
    /// Node currently owning the shard lease.
    pub node_id: ReplicaNodeId,
    /// Monotonic leader term that advanced the lease to `node_id`.
    pub leader_term: LeaderTerm,
}

impl std::fmt::Display for OwnerEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Owner(shard={} node={} term={})", self.shard_id.0, self.node_id.0, self.leader_term.0)
    }
}

/// Monotonic position of an entry in one shard replication log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LogIndex(pub u64);

/// One opaque mutation staged for replication.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReplicationEntry {
    /// Position in the shard log.
    pub index: LogIndex,
    /// Leader term that authored the entry.
    pub term: LeaderTerm,
    /// Product-owned payload. Encoding is not a stable wire format yet.
    pub payload: Vec<u8>,
}
