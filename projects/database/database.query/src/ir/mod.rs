//! 物理计划与版本化 IR 信封。
//!
//! 只含后端无关、非 SQL 的形状。外存命令文本不得出现在本模块。

#![deny(missing_docs)]

mod commit;
mod composite;
mod envelope;
mod object;
mod op;
mod plan;
mod projection;

pub use commit::{AppliedWatermark, CommitToken, DEFAULT_COMMIT_SHARD, OutboxAppend, OutboxEffect, OutboxRecord};
pub use composite::{AccessKind, COMPOSITE_PLAN_FORMAT, CompositePlan, CompositeStep, ConsistencyIntent, RouteProof};
pub use envelope::{EffectKind, IrEnvelope, IrVersion, SchemaFingerprint, hash_ops};
pub use object::{ObjectError, ObjectHash, ObjectId, ObjectLifecycleState, ObjectMeta, ObjectReference, ObjectResult, require_transition};
pub use op::{CmpOp, LiteralKind, PhysicalOp, Pred, ProjectField, SortKey, StringMatchOp, WriteField};
pub use plan::{PhysicalPlan, PlannedNode, RealizationClass};
pub use projection::{
    HydrateCompleteness, HydrateDropReason, HydrateResult, HydratedEntity, ProjectionCandidate, ProjectionDocument, ProjectionGeneration,
};

use serde::{Deserialize, Serialize};

/// Iris IR errors.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum Error {
    /// Unknown or unsupported IR major version.
    #[error("unsupported Iris IR version {0}.{1}")]
    UnsupportedVersion(u16, u16),
}

/// Result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Smoke touch of the public `vos` facade (proves the language dependency).
pub fn vos_facade_name() -> &'static str {
    let _ = std::any::type_name::<vos::SchemaProjection>();
    "vos"
}

/// Stable serialized form marker used in envelopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticHash(pub u64);
