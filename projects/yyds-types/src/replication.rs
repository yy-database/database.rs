//! YYDS-owned replica placement and leader-fencing contracts.

use crate::{ShardEpoch, ShardId};

/// Stable identity of a node hosting a shard replica.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicaNodeId(pub String);

/// Placement role of one replica member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    /// Participates in quorum and may become leader.
    Voter,
    /// Receives replication but cannot become leader yet.
    Learner,
}

/// One node assignment in a shard replica set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaMember {
    /// Node hosting this replica.
    pub node: ReplicaNodeId,
    /// Current placement role.
    pub role: ReplicaRole,
}

/// Monotonic term used to fence an old leader after a new election.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    pub shard_id: crate::identity::ShardId,
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

/// Validated replica placement for one shard and routing epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSet {
    shard: ShardId,
    epoch: ShardEpoch,
    term: LeaderTerm,
    leader: ReplicaNodeId,
    members: Vec<ReplicaMember>,
}

/// Failure to construct or validate a replica placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaError {
    /// The shard or node identity is empty.
    InvalidIdentity,
    /// Epoch or term is zero.
    InvalidGeneration,
    /// The replica list is empty.
    EmptyPlacement,
    /// A node appears more than once.
    DuplicateNode,
    /// The leader is not a member.
    LeaderNotMember,
    /// A learner was selected as leader.
    LeaderMustBeVoter,
    /// The new term is not strictly newer.
    NonMonotonicTerm { current: LeaderTerm, found: LeaderTerm },
    /// The supplied epoch is older than the replica set epoch.
    StaleEpoch { expected: ShardEpoch, found: ShardEpoch },
    /// The supplied term is older than the replica set term.
    StaleTerm { expected: LeaderTerm, found: LeaderTerm },
    /// The caller is not the current leader.
    NotLeader { expected: ReplicaNodeId, found: ReplicaNodeId },
}

impl ReplicaSet {
    /// Creates a validated replica placement and current leader lease.
    pub fn new(
        shard: ShardId,
        epoch: ShardEpoch,
        term: LeaderTerm,
        leader: ReplicaNodeId,
        members: Vec<ReplicaMember>,
    ) -> Result<Self, ReplicaError> {
        let set = Self { shard, epoch, term, leader, members };
        set.validate()
    }

    /// Validates placement identities and leadership role.
    pub fn validate(self) -> Result<Self, ReplicaError> {
        if self.shard.0.is_empty() || self.leader.0.is_empty() {
            return Err(ReplicaError::InvalidIdentity);
        }
        if self.epoch.0 == 0 || self.term.0 == 0 {
            return Err(ReplicaError::InvalidGeneration);
        }
        if self.members.is_empty() {
            return Err(ReplicaError::EmptyPlacement);
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut leader_role = None;
        for member in &self.members {
            if member.node.0.is_empty() || !seen.insert(member.node.clone()) {
                return Err(ReplicaError::DuplicateNode);
            }
            if member.node == self.leader {
                leader_role = Some(member.role);
            }
        }
        match leader_role {
            None => Err(ReplicaError::LeaderNotMember),
            Some(ReplicaRole::Learner) => Err(ReplicaError::LeaderMustBeVoter),
            Some(ReplicaRole::Voter) => Ok(self),
        }
    }

    /// Returns the shard identity.
    pub fn shard(&self) -> &ShardId {
        &self.shard
    }

    /// Returns the routing epoch used by this placement.
    pub fn epoch(&self) -> ShardEpoch {
        self.epoch
    }

    /// Returns the current leader term.
    pub fn term(&self) -> LeaderTerm {
        self.term
    }

    /// Returns the current leader node.
    pub fn leader(&self) -> &ReplicaNodeId {
        &self.leader
    }

    /// Returns all replica members in placement order.
    pub fn members(&self) -> &[ReplicaMember] {
        &self.members
    }

    /// Creates the fence token that current writes must carry.
    pub fn fence_token(&self) -> FenceToken {
        FenceToken { epoch: self.epoch, term: self.term, leader: self.leader.clone() }
    }

    /// Validates a write caller against the current epoch, term, and leader.
    pub fn validate_fence(&self, caller: &ReplicaNodeId, token: &FenceToken) -> Result<(), ReplicaError> {
        if token.epoch != self.epoch {
            return Err(ReplicaError::StaleEpoch { expected: self.epoch, found: token.epoch });
        }
        if token.term != self.term {
            return Err(ReplicaError::StaleTerm { expected: self.term, found: token.term });
        }
        if caller != &self.leader || token.leader != self.leader {
            return Err(ReplicaError::NotLeader { expected: self.leader.clone(), found: caller.clone() });
        }
        Ok(())
    }

    /// Advances leadership to a voter under a strictly newer term.
    pub fn elect(&self, term: LeaderTerm, leader: ReplicaNodeId) -> Result<Self, ReplicaError> {
        if term <= self.term {
            return Err(ReplicaError::NonMonotonicTerm { current: self.term, found: term });
        }
        let candidate = Self { shard: self.shard.clone(), epoch: self.epoch, term, leader, members: self.members.clone() };
        candidate.validate()
    }
}
