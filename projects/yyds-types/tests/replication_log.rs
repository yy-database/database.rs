use yyds_types::{
    LeaderTerm, LogIndex, ReplicaError, ReplicaLog, ReplicaMember, ReplicaNodeId, ReplicaRole, ReplicaSet, ReplicationError,
    ShardEpoch, ShardId,
};

fn log() -> ReplicaLog {
    let placement = ReplicaSet::new(
        ShardId("shard-a".into()),
        ShardEpoch(1),
        LeaderTerm(2),
        ReplicaNodeId("leader".into()),
        vec![
            ReplicaMember { node: ReplicaNodeId("leader".into()), role: ReplicaRole::Voter },
            ReplicaMember { node: ReplicaNodeId("follower".into()), role: ReplicaRole::Voter },
            ReplicaMember { node: ReplicaNodeId("learner".into()), role: ReplicaRole::Learner },
        ],
    )
    .expect("placement");
    ReplicaLog::new(placement)
}

#[test]
fn replication_log_commits_after_voter_quorum() {
    let mut log = log();
    let leader = ReplicaNodeId("leader".into());
    let index = log.append(&leader, &log.placement().fence_token(), b"put".to_vec()).expect("append");
    assert_eq!(log.try_commit(index), Err(ReplicationError::QuorumNotReached { required: 2, observed: 1 }));
    log.acknowledge(&ReplicaNodeId("learner".into()), index).expect("learner ack");
    assert_eq!(log.try_commit(index), Err(ReplicationError::QuorumNotReached { required: 2, observed: 1 }));
    log.acknowledge(&ReplicaNodeId("follower".into()), index).expect("voter ack");
    log.try_commit(index).expect("commit");
    assert_eq!(log.committed(), LogIndex(1));
}

#[test]
fn replication_log_rejects_stale_fences_and_unknown_replicas() {
    let mut log = log();
    let leader = ReplicaNodeId("leader".into());
    let mut token = log.placement().fence_token();
    token.term = LeaderTerm(1);
    assert!(matches!(log.append(&leader, &token, vec![]), Err(ReplicationError::Fenced(ReplicaError::StaleTerm { .. }))));
    assert_eq!(
        log.acknowledge(&ReplicaNodeId("unknown".into()), LogIndex(1)),
        Err(ReplicationError::UnknownReplica(ReplicaNodeId("unknown".into())))
    );
}
