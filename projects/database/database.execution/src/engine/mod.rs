//! 嵌入式引擎运行时：连接状态、VOS 目录发布、查询调用、CAS、租约 / TTL、
//! 备份与 UDF 会话桥。
//!
//! 产品门面只再导出本模块，不在此堆积实现。

#![deny(missing_docs)]

/// Batched write operations.
pub mod batch;
/// `bytes` placement at the inline boundary and object-reference row decoding.
pub mod bytes;
/// VOS catalog publication and contract resolution.
pub mod catalog;
/// Durable connection handle and statement surface.
pub mod connection;
/// Journal modes and `-wal` / `-shm` sidecar helpers.
pub mod journal;
/// Unified `<hash-prefix>/<chunk_hash>.blob` CAS + hot/cold tiering.
pub mod objects;
/// UDF traits (`ScalarUdf`) and registry types (`UdfRegistry`, ...).
pub mod udf;

mod kv;
mod storage;

pub use batch::Batch;
pub use bytes::{ObjectStoreResolver, YydbBytesPolicy, YydbVectorPolicy};
pub use connection::Connection;
pub use journal::{JournalMode, OpenFlags};
pub use objects::ObjectStore;
pub use udf::ScalarUdf;

pub use udf::{HostFunctionHandle, HostMicroDefinition, HostRuntimeAdapter, UdfError, UdfRegistry, UdfType, UdfValue};
/// Result alias for UDF host functions and registry operations.
pub type UdfResult<T = UdfValue> = crate::udf::Result<T>;

pub use crate::program as execution;
pub use database_query::local::QueryRow;
pub use database_types::{
    self, ChunkManifest, CommitSequence, CompactReport, DEFAULT_CHUNK_SIZE, DoctorIssue, DoctorReport, DoctorSeverity, Error, EvictBudget,
    EvictReport, EvictionPolicy, FileDescriptor, FileManifestRef, HashAlgo, INLINE_BYTES_MAX, LeaseExpectation, LeaseToken, NamespaceQuota,
    NamespaceStats, ObjectKind, ObjectRef, ReclaimReport, RecordVersion, ReleaseOutcome, Result, SchemaVersion, Tier, Value, Vector,
};

/// Storage-engine version string (Cargo package version).
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
