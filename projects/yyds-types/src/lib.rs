#![deny(missing_debug_implementations, missing_copy_implementations)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
#![doc = include_str!("../readme.md")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/oovm/shape-rs/dev/projects/images/Trapezohedron.svg")]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/oovm/shape-rs/dev/projects/images/Trapezohedron.svg")]

mod distributed_execution;
mod errors;
mod identity;
mod key_value;
mod replication;
mod replication_log;
mod routing;
mod schema;
mod vos_udf;

pub use crate::{
    distributed_execution::{
        Approximation, Consistency, DistributedPlan, EvalError, Exchange, FileRef, Fragment, FragmentId, FragmentRole,
        LayoutField, Node, PlanValidationError, Program, RecordLayout, RecordLayoutError, RecordValue, RecordValueError,
        RetryPolicy, Type, Udf, UdfEffect, UdfPlacement, UdfValidationError, ValidatedDistributedPlan, ValidatedProgram,
        ValidatedUdf, ValidationError, Value, VectorMetric, VectorValue, VectorValueError,
    },
    errors::{Error, Result},
    identity::{Namespace, ShardId, CATALOG_SUFFIX, SHARD_SUFFIX},
    key_value::{KeyValueAction, KeyValueCommand, KeyValueResult, SweepReport},
    replication::{FenceToken, LeaderTerm, OwnerEntry, ReplicaError, ReplicaMember, ReplicaNodeId, ReplicaRole, ReplicaSet},
    replication_log::{LogIndex, ReplicaLog, ReplicationEntry, ReplicationError},
    routing::{RouteDecision, RoutingError, ShardEpoch, ShardMap},
    schema::{adopt_catalog_schema, validate_document, CatalogSchema, VOS_GIT_DEV},
    vos_udf::lower_vos_udf,
};
/// Returns the `yyds-types` crate version.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
