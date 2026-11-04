use yyds_types::{
    FenceToken, LeaderTerm, ReplicaError, ReplicaMember, ReplicaNodeId, ReplicaRole, ReplicaSet, ShardEpoch, ShardId,
};

fn replica_set() -> ReplicaSet {
    ReplicaSet::new(
        ShardId("shard-a".into()),
        ShardEpoch(3),
        LeaderTerm(8),
        ReplicaNodeId("node-a".into()),
        vec![
            ReplicaMember { node: ReplicaNodeId("node-a".into()), role: ReplicaRole::Voter },
            ReplicaMember { node: ReplicaNodeId("node-b".into()), role: ReplicaRole::Voter },
            ReplicaMember { node: ReplicaNodeId("node-c".into()), role: ReplicaRole::Learner },
        ],
    )
    .expect("replica set")
}

#[test]
fn leader_fence_accepts_only_current_epoch_term_and_node() {
    let set = replica_set();
    let token = set.fence_token();
    assert_eq!(set.validate_fence(set.leader(), &token), Ok(()));
    assert_eq!(
        set.validate_fence(&ReplicaNodeId("node-b".into()), &token,),
        Err(ReplicaError::NotLeader { expected: ReplicaNodeId("node-a".into()), found: ReplicaNodeId("node-b".into()) })
    );
    assert_eq!(
        set.validate_fence(set.leader(), &FenceToken { epoch: ShardEpoch(2), ..token.clone() },),
        Err(ReplicaError::StaleEpoch { expected: ShardEpoch(3), found: ShardEpoch(2) })
    );
    assert_eq!(
        set.validate_fence(set.leader(), &FenceToken { term: LeaderTerm(7), ..token },),
        Err(ReplicaError::StaleTerm { expected: LeaderTerm(8), found: LeaderTerm(7) })
    );
}

#[test]
fn replica_set_rejects_invalid_leaders_and_non_monotonic_elections() {
    let set = replica_set();
    assert_eq!(
        set.elect(LeaderTerm(8), ReplicaNodeId("node-b".into())),
        Err(ReplicaError::NonMonotonicTerm { current: LeaderTerm(8), found: LeaderTerm(8) })
    );
    assert_eq!(set.elect(LeaderTerm(9), ReplicaNodeId("node-c".into())), Err(ReplicaError::LeaderMustBeVoter));
    assert_eq!(
        ReplicaSet::new(
            ShardId("shard-a".into()),
            ShardEpoch(3),
            LeaderTerm(1),
            ReplicaNodeId("node-x".into()),
            vec![ReplicaMember { node: ReplicaNodeId("node-a".into()), role: ReplicaRole::Voter }],
        ),
        Err(ReplicaError::LeaderNotMember)
    );
}
