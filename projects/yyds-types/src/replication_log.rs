//! YYDS-owned in-memory replication and quorum-commit contracts.

use std::collections::{BTreeMap, BTreeSet};

use crate::{FenceToken, LeaderTerm, ReplicaError, ReplicaNodeId, ReplicaRole, ReplicaSet};

/// Monotonic position of an entry in one shard replication log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogIndex(pub u64);

/// One opaque mutation staged for replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationEntry {
    /// Position in the shard log.
    pub index: LogIndex,
    /// Leader term that authored the entry.
    pub term: LeaderTerm,
    /// Product-owned payload. Encoding is not a stable wire format yet.
    pub payload: Vec<u8>,
}

/// Failure while appending, acknowledging, or committing a log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationError {
    /// The caller failed current leader fencing.
    Fenced(ReplicaError),
    /// A node is not part of the replica placement.
    UnknownReplica(ReplicaNodeId),
    /// A learner cannot be used as the authoring leader.
    LearnerCannotLead,
    /// The requested log position does not exist.
    UnknownIndex(LogIndex),
    /// Commit must advance one contiguous position at a time.
    NonContiguousCommit { expected: LogIndex, found: LogIndex },
    /// A quorum has not acknowledged the requested position.
    QuorumNotReached { required: usize, observed: usize },
    /// The log index counter overflowed.
    IndexExhausted,
}

/// An in-memory replication log with explicit voter quorum accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaLog {
    placement: ReplicaSet,
    entries: Vec<ReplicationEntry>,
    acknowledgements: BTreeMap<LogIndex, BTreeSet<ReplicaNodeId>>,
    committed: LogIndex,
}

impl ReplicaLog {
    /// Creates an empty log under a validated replica placement.
    pub fn new(placement: ReplicaSet) -> Self {
        Self { placement, entries: Vec::new(), acknowledgements: BTreeMap::new(), committed: LogIndex(0) }
    }

    /// Appends a mutation under the current leader fence.
    pub fn append(
        &mut self,
        caller: &ReplicaNodeId,
        token: &FenceToken,
        payload: Vec<u8>,
    ) -> Result<LogIndex, ReplicationError> {
        self.placement.validate_fence(caller, token).map_err(ReplicationError::Fenced)?;
        let index = LogIndex(
            self.entries
                .len()
                .try_into()
                .ok()
                .and_then(|value: u64| value.checked_add(1))
                .ok_or(ReplicationError::IndexExhausted)?,
        );
        self.entries.push(ReplicationEntry { index, term: self.placement.term(), payload });
        self.acknowledgements.entry(index).or_default().insert(self.placement.leader().clone());
        Ok(index)
    }

    /// Acknowledges a replicated entry from a voter or learner member.
    pub fn acknowledge(&mut self, replica: &ReplicaNodeId, index: LogIndex) -> Result<(), ReplicationError> {
        if !self.placement.members().iter().any(|member| &member.node == replica) {
            return Err(ReplicationError::UnknownReplica(replica.clone()));
        }
        if !self.entries.iter().any(|entry| entry.index == index) {
            return Err(ReplicationError::UnknownIndex(index));
        }
        self.acknowledgements.entry(index).or_default().insert(replica.clone());
        Ok(())
    }

    /// Commits the next contiguous entry when voter acknowledgements reach quorum.
    pub fn try_commit(&mut self, index: LogIndex) -> Result<(), ReplicationError> {
        let expected = LogIndex(self.committed.0.checked_add(1).ok_or(ReplicationError::IndexExhausted)?);
        if index != expected {
            return Err(ReplicationError::NonContiguousCommit { expected, found: index });
        }
        if !self.entries.iter().any(|entry| entry.index == index) {
            return Err(ReplicationError::UnknownIndex(index));
        }
        let voters = self
            .placement
            .members()
            .iter()
            .filter(|member| member.role == ReplicaRole::Voter)
            .map(|member| &member.node)
            .collect::<BTreeSet<_>>();
        let observed =
            self.acknowledgements.get(&index).into_iter().flatten().filter(|replica| voters.contains(replica)).count();
        let required = voters.len() / 2 + 1;
        if observed < required {
            return Err(ReplicationError::QuorumNotReached { required, observed });
        }
        self.committed = index;
        Ok(())
    }

    /// Returns the committed log index.
    pub fn committed(&self) -> LogIndex {
        self.committed
    }

    /// Returns the staged entries in log order.
    pub fn entries(&self) -> &[ReplicationEntry] {
        &self.entries
    }

    /// Returns the placement used by this log.
    pub fn placement(&self) -> &ReplicaSet {
        &self.placement
    }
}
