//! 分布式分片、fencing、目录 schema 校验与运维合同。

mod errors;
mod fencing;
mod identity;
mod maintenance;
mod schema;

pub use errors::{Error, Result};
pub use fencing::{FenceToken, LeaderTerm, LogIndex, OwnerEntry, ReplicaNodeId, ReplicationEntry, ShardEpoch};
pub use identity::{CATALOG_SUFFIX, Namespace, SHARD_SUFFIX, ShardId};
pub use maintenance::SweepReport;
pub use schema::{CatalogSchema, VOS_GIT_DEV, adopt_catalog_schema, validate_document};

/// Returns the `database-types` package version.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
