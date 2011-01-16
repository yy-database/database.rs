//! 分布式执行、复制与路由合同。
pub mod distributed_execution;
pub mod replication;
pub mod replication_log;
pub mod routing;
pub mod vos_udf;

pub use database_types::cluster::{FenceToken, LeaderTerm, LogIndex, OwnerEntry, ReplicaNodeId, ReplicationEntry, ShardEpoch, ShardId};
pub use replication::{ReplicaError, ReplicaMember, ReplicaRole, ReplicaSet};
pub use replication_log::{ReplicaLog, ReplicationError};
pub use routing::{RouteDecision, ShardMap};
