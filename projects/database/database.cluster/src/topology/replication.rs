//! 集群-owned replica placement and leader-fencing contracts.

use database_types::cluster::{FenceToken, LeaderTerm, ReplicaNodeId, ShardEpoch, ShardId};

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
    /// 新任期未严格大于当前任期。
    NonMonotonicTerm {
        /// 副本集当前任期。
        current: LeaderTerm,
        /// 入站声称的任期。
        found: LeaderTerm,
    },
    /// 入站世代早于副本集世代。
    StaleEpoch {
        /// 副本集当前世代。
        expected: ShardEpoch,
        /// 入站世代。
        found: ShardEpoch,
    },
    /// 入站任期早于副本集任期。
    StaleTerm {
        /// 副本集当前任期。
        expected: LeaderTerm,
        /// 入站任期。
        found: LeaderTerm,
    },
    /// 调用方不是当前 leader。
    NotLeader {
        /// 目录发布的 leader。
        expected: ReplicaNodeId,
        /// 实际调用方。
        found: ReplicaNodeId,
    },
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
        FenceToken { epoch: self.epoch, term: self.term, leader_node_id: self.leader.clone() }
    }

    /// Validates a write caller against the current epoch, term, and leader.
    pub fn validate_fence(&self, caller: &ReplicaNodeId, token: &FenceToken) -> Result<(), ReplicaError> {
        if token.epoch != self.epoch {
            return Err(ReplicaError::StaleEpoch { expected: self.epoch, found: token.epoch });
        }
        if token.term != self.term {
            return Err(ReplicaError::StaleTerm { expected: self.term, found: token.term });
        }
        if caller != &self.leader || token.leader_node_id != self.leader {
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
