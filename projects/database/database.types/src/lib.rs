//! `database-types`：共享值、标识、错误与跨层合同。
//!
//! 按能力落在 crate 根（值 / 标量 / 对象 / 约束 / 诊断等）；分布式分片与
//! fencing 合同在 [`cluster`]。不按产品品牌分模块。
//!
//! UDF 正式入口已提级到 `database-udf`；本 crate 的 [`udf`] 子树仅作过渡再导出。

#![deny(missing_docs)]
use std::{collections::BTreeMap, error, fmt, io, result, sync::Arc};

/// Catalog uniqueness metadata.
pub mod constraint;

/// Exact calendar, clock, instant, and fixed-point decimal scalars.
pub mod scalar;

pub use constraint::{TableUniqueConstraint, UniqueConstraint, UniqueConstraintError};
pub use scalar::{Date, Decimal, ScalarError, Time, Timestamp};

/// Largest `bytes` payload kept inside a row cell instead of the object store.
///
/// A row must fit one leaf page payload and row storage encodes octets as hex,
/// so a payload occupies twice its size on disk plus its key and cell header.
/// Payloads above this boundary are placed in the content-addressed object
/// store and referenced from the row, which keeps every stored row storable.
pub const INLINE_BYTES_MAX: usize = 1024;

/// 宿主对声明的 `vector<N>` 列接受的最大维度。
///
/// 这是**运行时宿主约束**，不是 VOS 语言合同。VOS
/// 允许任意正 `u32` 维度；宿主必须拒绝
/// dimensions it cannot safely execute. At `16384` components the wire payload
/// is ~64 KiB of `f32`, well above the inline budget and still well below the
/// ~4 GiB that a `Vec<f32>` could theoretically hold before exhausting a
/// process address space. The inline/object placement boundary
/// (内联/对象分界见执行层 `INLINE_VECTOR_DIM_MAX` = 120) is independent and handled at
/// write time.
pub const MAX_VECTOR_DIM: u32 = 16384;

/// Default chunk size for chunked file writes (1 MiB).
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

/// 本 crate 通用结果别名。
pub type Result<T = ()> = result::Result<T, Error>;

/// Machine-readable diagnostic envelope. Every boundary surface (N-API reason,
/// WASM JSON error field, wire error body) serializes to this shape so hosts
/// can parse it without knowing which variant produced it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiagnosticEnvelope {
    /// Stable dotted code (`database.schema.invalid`).
    pub code: String,
    /// Operation category (`io`, `schema`, `udf`, …).
    pub operation: String,
    /// Whether the caller may retry with the same arguments.
    pub retryable: bool,
    /// Human-readable display text (locale-aware only at render time; do not
    /// branch on this string).
    pub message: String,
    /// Structured parameters for catalog templating.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<std::collections::BTreeMap<String, String>>,
}

impl DiagnosticEnvelope {
    /// Build from an [`Error`].
    pub fn from_error(error: &Error) -> Self {
        let params = error.params().map(|pairs| pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect());
        Self {
            code: error.code().to_owned(),
            operation: error.operation().to_owned(),
            retryable: error.retryable(),
            message: error.to_string(),
            params,
        }
    }

    /// Serialize to a compact JSON string.
    pub fn to_string(&self) -> String {
        serde_json::to_string(self).expect("DiagnosticEnvelope serialize cannot fail")
    }

    /// Parse from a JSON string. Returns `Err` on malformed input.
    pub fn from_string(json: &str) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// 打开 / 读 / 写 / UDF / 对象路径产生的错误。
#[derive(Debug)]
pub enum Error {
    /// Underlying filesystem or I/O failure.
    Io(io::Error),
    /// File contents do not match the expected layout.
    Corrupt(&'static str),
    /// Stored schema version does not match the caller’s expectation.
    SchemaConflict {
        /// Expected schema version.
        expected: u32,
        /// Stored schema version.
        found: u32,
    },
    /// No scalar UDF is registered under this name.
    UdfNotFound {
        /// Missing UDF name.
        name: String,
    },
    /// UDF called with the wrong number of arguments.
    UdfArity {
        /// UDF name.
        name: String,
        /// Expected argument count.
        expected: usize,
        /// Supplied argument count.
        got: usize,
    },
    /// UDF body failed or rejected its arguments.
    Udf {
        /// UDF name.
        name: String,
        /// Failure message from the UDF host.
        message: String,
    },
    /// Schema document failed VOS validation (shared language contract).
    Schema {
        /// Validation failure message.
        message: String,
    },
    /// CAS object missing on disk / hot cache.
    ObjectNotFound {
        /// Lowercase hex digest of the missing object.
        hash_hex: String,
    },
    /// Chunked read or manifest is inconsistent.
    ObjectCorrupt {
        /// Corruption detail message.
        message: String,
    },
    /// Feature exists as a product surface but is not implemented yet.
    Unsupported(&'static str),
    /// Wire protocol / serve client failure.
    Protocol {
        /// Protocol error message.
        message: String,
    },
    /// Parameter binding error on a prepared statement: missing / extra /
    /// wrong-type values.
    QueryBind {
        /// Parameter name (always set).
        parameter: String,
        /// Human-readable detail for diagnostics.
        message: String,
    },
    /// Independent compare-and-set failed.
    CasConflict {
        /// Key that failed the compare.
        key: String,
    },
    /// Transaction commit found the durable state ahead of the transaction
    /// base. Nothing was published and the transaction is over; retry with a
    /// fresh `begin`.
    TransactionConflict {
        /// Commit sequence the transaction started from.
        expected: u64,
        /// Commit sequence the durable state had reached at commit time.
        found: u64,
    },
    /// Lease cannot be claimed under the given expectation.
    LeaseUnavailable {
        /// Lease key.
        key: String,
    },
    /// Lease token does not match the active claim.
    FencingMismatch {
        /// Lease key.
        key: String,
    },
    /// Lease has expired.
    LeaseExpired {
        /// Lease key.
        key: String,
    },
    /// Registered UDF version does not match the invocation.
    UdfVersionMismatch {
        /// UDF name.
        name: String,
        /// Expected registered version.
        expected: u32,
        /// Supplied invocation version.
        got: u32,
    },
    /// Namespace quota cannot admit another record.
    QuotaExceeded {
        /// Namespace that exceeded quota.
        namespace: String,
    },
    /// Session handle is closed and can no longer serve operations.
    SessionClosed,
    /// Client supplied a bearer token that does not match any bootstrap
    /// principal or active session (wrong-length, wrong-value, or token
    /// format that the server cannot map to a principal).
    AuthTokenInvalid {
        /// Human-readable detail (for example "token not in bootstrap registry").
        message: String,
    },
    /// Client presented a session token that the server knows but whose
    /// `expiration` timestamp has passed.
    AuthSessionExpired {
        /// Human-readable detail (for example "session expired at …").
        message: String,
    },
    /// Bootstrap principal or named identity referenced by the connection
    /// (for example via an auth-config file) does not exist on this server.
    AuthPrincipalNotFound {
        /// Human-readable detail (for example "principal `svc-worker-07` not configured").
        message: String,
    },
    /// Connection originated from a non-loopback address and is attempting
    /// to authenticate as an anonymous principal — server refuses to open
    /// any work for an anonymous remote peer.
    AuthAnonymousRemoteDenied {
        /// Human-readable detail.
        message: String,
    },
    /// Server started with a non-loopback bind address but no bootstrap
    /// principals were configured (neither env vars nor an auth-config
    /// file). Startup must refuse to expose the process anonymously.
    AuthConfigMissing {
        /// Human-readable detail (for example expected env var names or
        /// auth-config file path hints).
        message: String,
    },
    /// Principal authenticated successfully but does not hold the permission
    /// required for the requested frame. Emitted by dispatch gates before
    /// any connection-side write / schema op runs.
    AuthInsufficientPermission {
        /// Stable label of the permission the frame required (`read`,
        /// `write`, `schema`).
        required: String,
        /// Short human-readable description of the frame that was denied
        /// (`KvPut`, `SchemaEnsure`, …).
        frame: String,
    },
    /// Prepared statement bound to a schema that has since been migrated or
    /// invalidated. Re-prepare with the current schema.
    SchemaInvalidated {
        /// FNV-1a fingerprint of the schema document captured at prepare time.
        expected_fingerprint: String,
        /// FNV-1a fingerprint of the schema document currently active.
        current_fingerprint: String,
        /// Schema revision at prepare time (IdentityHistory.revision).
        schema_revision: u64,
    },
    /// Failure reported by a remote YY wire peer (code plus peer message).
    Remote {
        /// Stable code the peer reported (for example `database.schema.invalid`).
        code: String,
        /// Display message the peer reported.
        message: String,
    },
    /// Exact scalar text or bytes rejected by a temporal/decimal parser.
    Scalar {
        /// Scalar kind (`date`, `time`, `timestamp`, `decimal`, or `scalar`).
        kind: &'static str,
        /// Human-readable rejection detail.
        message: String,
    },
    /// A write would violate a declared unique constraint.
    UniqueViolation {
        /// Storage table name.
        table: String,
        /// Constraint name from the catalog.
        constraint: String,
        /// Columns covered by the constraint.
        columns: Vec<String>,
    },
    /// A result row cannot be rendered in the requested format without
    /// inventing a representation for the value.
    ResultFormat {
        /// Result column that cannot be represented.
        column: String,
        /// Scalar kind the format cannot carry (for example `bytes`).
        kind: &'static str,
    },
    /// 声明的 `vector<N>` 列超过宿主最大维度。
    VectorDimensionExceeded {
        /// Dimension the schema declared.
        dimension: u32,
        /// 宿主接受的最大维度。
        max: u32,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Corrupt(message) => write!(f, "corrupt database file: {message}"),
            Self::SchemaConflict { expected, found } => {
                write!(f, "schema version conflict: expected {expected}, found {found}")
            }
            Self::UdfNotFound { name } => write!(f, "UDF not found: {name}"),
            Self::UdfArity { name, expected, got } => {
                write!(f, "UDF {name} arity mismatch: expected {expected} args, got {got}")
            }
            Self::Udf { name, message } => write!(f, "UDF {name}: {message}"),
            Self::Schema { message } => write!(f, "VOS schema: {message}"),
            Self::ObjectNotFound { hash_hex } => {
                write!(f, "object not found: {hash_hex}")
            }
            Self::ObjectCorrupt { message } => write!(f, "object corrupt: {message}"),
            Self::Unsupported(feature) => write!(f, "unsupported: {feature}"),
            Self::Protocol { message } => write!(f, "protocol: {message}"),
            Self::QueryBind { parameter, message } => {
                write!(f, "query bind `${parameter}: {message}")
            }
            Self::CasConflict { key } => write!(f, "CAS conflict on key {key}"),
            Self::TransactionConflict { expected, found } => {
                write!(f, "transaction conflict: durable state advanced to {found}, transaction based on {expected}")
            }
            Self::LeaseUnavailable { key } => write!(f, "lease unavailable on key {key}"),
            Self::FencingMismatch { key } => {
                write!(f, "fencing mismatch on key {key}")
            }
            Self::LeaseExpired { key } => write!(f, "lease expired on key {key}"),
            Self::UdfVersionMismatch { name, expected, got } => {
                write!(f, "UDF {name} version mismatch: expected {expected}, got {got}")
            }
            Self::QuotaExceeded { namespace } => {
                write!(f, "quota exceeded for namespace {namespace}")
            }
            Self::SessionClosed => write!(f, "session closed"),
            Self::AuthTokenInvalid { message } => write!(f, "auth: invalid token — {message}"),
            Self::AuthSessionExpired { message } => write!(f, "auth: session expired — {message}"),
            Self::AuthPrincipalNotFound { message } => write!(f, "auth: principal not found — {message}"),
            Self::AuthAnonymousRemoteDenied { message } => {
                write!(f, "auth: anonymous remote denied — {message}")
            }
            Self::AuthConfigMissing { message } => write!(f, "auth: config missing — {message}"),
            Self::AuthInsufficientPermission { required, frame } => {
                write!(f, "auth: insufficient permission — frame `{frame}` requires `{required}`")
            }
            Self::SchemaInvalidated { expected_fingerprint, current_fingerprint, schema_revision } => {
                write!(
                    f,
                    "schema invalidated at revision {schema_revision}: expected fingerprint `{expected_fingerprint}`, current `{current_fingerprint}`"
                )
            }
            Self::Remote { code, message } => write!(f, "{code}: {message}"),
            Self::Scalar { kind, message } => write!(f, "invalid {kind}: {message}"),
            Self::UniqueViolation { table, constraint, columns } => {
                write!(f, "unique constraint `{constraint}` violated on table `{table}` (columns: {})", columns.join(", "))
            }
            Self::ResultFormat { column, kind } => {
                write!(f, "result format: column `{column}` carries `{kind}` values that the requested format cannot represent")
            }
            Self::VectorDimensionExceeded { dimension, max } => {
                write!(f, "vector dimension {dimension} exceeds host maximum {max} — split into multiple vectors or use object storage")
            }
        }
    }
}

impl Error {
    /// Stable diagnostic code for wire and host surfaces.
    ///
    /// Codes use dotted lowercase segments (`database.schema.invalid`) and stay
    /// stable across releases. Hosts and tests must branch on these codes and
    /// never on [`Display`](fmt::Display) text. A [`Self::Remote`] error reports
    /// the code from the peer unchanged.
    pub fn code(&self) -> &str {
        match self {
            Self::Io(_) => "database.io",
            Self::Corrupt(_) => "database.corrupt",
            Self::SchemaConflict { .. } => "database.schema.conflict",
            Self::UdfNotFound { .. } => "database.udf.not_found",
            Self::UdfArity { .. } => "database.udf.arity",
            Self::Udf { .. } => "database.udf.failed",
            Self::Schema { .. } => "database.schema.invalid",
            Self::ObjectNotFound { .. } => "database.object.not_found",
            Self::ObjectCorrupt { .. } => "database.object.corrupt",
            Self::Unsupported(_) => "database.unsupported",
            Self::Protocol { .. } => "database.protocol",
            Self::QueryBind { .. } => "database.query.bind",
            Self::CasConflict { .. } => "database.cas.conflict",
            Self::TransactionConflict { .. } => "database.transaction.conflict",
            Self::LeaseUnavailable { .. } => "database.lease.unavailable",
            Self::FencingMismatch { .. } => "database.fencing.mismatch",
            Self::LeaseExpired { .. } => "database.lease.expired",
            Self::UdfVersionMismatch { .. } => "database.udf.version_mismatch",
            Self::QuotaExceeded { .. } => "database.quota.exceeded",
            Self::SessionClosed => "database.session.closed",
            Self::AuthTokenInvalid { .. } => "database.auth.token_invalid",
            Self::AuthSessionExpired { .. } => "database.auth.session_expired",
            Self::AuthPrincipalNotFound { .. } => "database.auth.principal_not_found",
            Self::AuthAnonymousRemoteDenied { .. } => "database.auth.anonymous_remote_denied",
            Self::AuthConfigMissing { .. } => "database.auth.config_missing",
            Self::AuthInsufficientPermission { .. } => "database.auth.insufficient_permission",
            Self::SchemaInvalidated { .. } => "database.schema.invalidated",
            Self::Remote { code, .. } => code,
            Self::Scalar { .. } => "database.scalar.invalid",
            Self::UniqueViolation { .. } => "database.unique.violation",
            Self::ResultFormat { .. } => "database.result.format",
            Self::VectorDimensionExceeded { .. } => "database.schema.vector_dimension_exceeded",
        }
    }

    /// High-level operation category for diagnostic catalog routing.
    ///
    /// One of: `"io"` / `"schema"` / `"udf"` / `"object"` / `"cas"` /
    /// `"transaction"` / `"lease"` / `"fencing"` / `"protocol"` / `"session"` / `"auth"` /
    /// `"quota"` / `"unsupported"` / `"remote"`. Hosts use this to pick the right section
    /// of a locale catalog.
    pub fn operation(&self) -> &'static str {
        match self {
            Self::Io(_) => "io",
            Self::Corrupt(_) => "object",
            Self::SchemaConflict { .. } | Self::Schema { .. } => "schema",
            Self::UdfNotFound { .. } | Self::UdfArity { .. } | Self::Udf { .. } | Self::UdfVersionMismatch { .. } => "udf",
            Self::ObjectNotFound { .. } | Self::ObjectCorrupt { .. } => "object",
            Self::Unsupported(_) => "unsupported",
            Self::Protocol { .. } => "protocol",
            Self::QueryBind { .. } => "query",
            Self::CasConflict { .. } => "cas",
            Self::TransactionConflict { .. } => "transaction",
            Self::LeaseUnavailable { .. } => "lease",
            Self::FencingMismatch { .. } => "fencing",
            Self::LeaseExpired { .. } => "lease",
            Self::QuotaExceeded { .. } => "quota",
            Self::SessionClosed => "session",
            Self::AuthTokenInvalid { .. }
            | Self::AuthSessionExpired { .. }
            | Self::AuthPrincipalNotFound { .. }
            | Self::AuthAnonymousRemoteDenied { .. }
            | Self::AuthConfigMissing { .. }
            | Self::AuthInsufficientPermission { .. } => "auth",
            Self::SchemaInvalidated { .. } => "query",
            Self::Scalar { .. } | Self::UniqueViolation { .. } => "query",
            Self::ResultFormat { .. } => "query",
            Self::VectorDimensionExceeded { .. } => "schema",
            Self::Remote { .. } => "remote",
        }
    }

    /// Structured key/value pairs for diagnostic templating, if the variant
    /// carries machine-readable parameters.
    ///
    /// Example: [`Self::UdfArity`] returns `[("name", "…"), ("expected", "3"),
    /// ("got", "5")]`. Hosts pass these to `@vmz/diagnostic` as the `args` field.
    pub fn params(&self) -> Option<Vec<(&'static str, String)>> {
        let p: Vec<(&'static str, String)> = match self {
            Self::Io(_) | Self::Corrupt(_) | Self::Unsupported(_) | Self::SessionClosed => {
                return None;
            }
            Self::AuthTokenInvalid { message }
            | Self::AuthSessionExpired { message }
            | Self::AuthPrincipalNotFound { message }
            | Self::AuthAnonymousRemoteDenied { message }
            | Self::AuthConfigMissing { message } => vec![("detail", message.clone())],
            Self::AuthInsufficientPermission { required, frame } => {
                vec![("required", required.clone()), ("frame", frame.clone())]
            }
            Self::SchemaConflict { expected, found } => vec![("expected", expected.to_string()), ("found", found.to_string())],
            Self::UdfNotFound { name } | Self::Udf { name, .. } => {
                vec![("name", name.clone())]
            }
            Self::UdfArity { name, expected, got } => {
                vec![("name", name.clone()), ("expected", expected.to_string()), ("got", got.to_string())]
            }
            Self::Schema { message } => vec![("message", message.clone())],
            Self::ObjectNotFound { hash_hex } => vec![("hash", hash_hex.clone())],
            Self::ObjectCorrupt { message } => vec![("detail", message.clone())],
            Self::Protocol { message } => vec![("detail", message.clone())],
            Self::QueryBind { parameter, message } => vec![("parameter", parameter.clone()), ("detail", message.clone())],
            Self::CasConflict { key } => vec![("key", key.clone())],
            Self::TransactionConflict { expected, found } => {
                vec![("expected", expected.to_string()), ("found", found.to_string())]
            }
            Self::LeaseUnavailable { key } | Self::FencingMismatch { key } => {
                vec![("key", key.clone())]
            }
            Self::LeaseExpired { key } => vec![("key", key.clone())],
            Self::UdfVersionMismatch { name, expected, got } => {
                vec![("name", name.clone()), ("expected", expected.to_string()), ("got", got.to_string())]
            }
            Self::QuotaExceeded { namespace } => vec![("namespace", namespace.clone())],
            Self::SchemaInvalidated { expected_fingerprint, current_fingerprint, schema_revision } => vec![
                ("expected_fingerprint", expected_fingerprint.clone()),
                ("current_fingerprint", current_fingerprint.clone()),
                ("schema_revision", schema_revision.to_string()),
            ],
            Self::Remote { code, message } => {
                vec![("code", code.clone()), ("message", message.clone())]
            }
            Self::Scalar { kind, message } => vec![("kind", (*kind).to_owned()), ("message", message.clone())],
            Self::UniqueViolation { table, constraint, columns } => {
                vec![("table", table.clone()), ("constraint", constraint.clone()), ("columns", columns.join(","))]
            }
            Self::ResultFormat { column, kind } => vec![("column", column.clone()), ("kind", (*kind).to_owned())],
            Self::VectorDimensionExceeded { dimension, max } => {
                vec![("dimension", dimension.to_string()), ("max", max.to_string())]
            }
        };
        Some(p)
    }

    /// Whether the caller may retry the same operation after backing off.
    ///
    /// `true` for transient failures (`Io`, `Protocol`, `LeaseUnavailable`,
    /// `LeaseExpired`). All other variants are non-retryable and require the
    /// caller to fix the underlying cause.
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Io(_) | Self::Protocol { .. } | Self::LeaseUnavailable { .. } | Self::LeaseExpired { .. })
    }

    /// Encode the full diagnostic contract as a JSON object.
    ///
    /// Every boundary surface (N-API, WASM, wire) serializes this object so
    /// that the host side receives a machine-identifiable error with stable
    /// keys: `code`, `operation`, `retryable`, `message`, and `params`
    /// (when present). The `message` field carries the human-readable
    /// [`Display`](fmt::Display) text so hosts can render it immediately
    /// without further resolution.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(DiagnosticEnvelope::from_error(self)).expect("DiagnosticEnvelope serialize cannot fail")
    }

    /// Serialize as a JSON string (compact, no whitespace).
    pub fn to_diagnostic_json_string(&self) -> String {
        DiagnosticEnvelope::from_error(self).to_string()
    }
}

/// Monotonic commit counter returned from fenced batch commits.
pub type CommitSequence = u64;

/// Handle proving an active lease claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseToken {
    /// Frontier or work item key under lease.
    pub key: String,
    /// Worker that holds the claim.
    pub worker_id: String,
    /// Monotonic fencing token assigned at claim time.
    pub fencing_token: u64,
    /// Wall-clock expiry in milliseconds since UNIX epoch.
    pub lease_until: u64,
}

/// `claim_lease` 的前置条件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseExpectation {
    /// Key has no active claim or the prior claim expired.
    AbsentOrExpired,
    /// Key is in the queued state.
    Queued,
}

/// Per-key version counter returned from TTL writes.
pub type RecordVersion = u64;

/// Namespace byte and record limits with an eviction policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceQuota {
    /// Maximum total payload bytes under the namespace prefix.
    pub max_bytes: u64,
    /// Maximum record count under the namespace prefix.
    pub max_records: u64,
    /// How to choose victims when limits are exceeded.
    pub eviction_policy: EvictionPolicy,
}

/// Eviction strategy for namespace quota enforcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Evict the least recently touched record first.
    Lru,
    /// Evict the soonest-expiring TTL record first.
    TtlFirst,
}

/// Observed usage for a namespace prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NamespaceStats {
    /// Total bytes stored under the prefix.
    pub bytes_used: u64,
    /// Number of records under the prefix.
    pub records_used: u64,
}

/// Upper bound for a single eviction pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictBudget {
    /// Maximum records to evict in one call.
    pub max_records: usize,
}

/// Severity for a [`DoctorIssue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DoctorSeverity {
    /// Non-fatal inconsistency or lag.
    Warn,
    /// Data or contract violation.
    Error,
}

/// One consistency finding from [`DoctorReport`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DoctorIssue {
    /// How severe the finding is.
    pub severity: DoctorSeverity,
    /// Stable diagnostic code such as `database.doctor.half_batch`.
    pub code: String,
    /// Human-readable detail.
    pub message: String,
    /// Optional record key hint.
    pub key_hint: Option<String>,
}

/// Per-namespace usage included in [`DoctorReport`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DoctorNamespaceStats {
    /// Namespace prefix.
    pub prefix: String,
    /// Records under the prefix.
    pub record_count: u64,
    /// Payload bytes under the prefix.
    pub bytes_used: u64,
    /// Referenced CAS payload bytes.
    pub object_bytes: u64,
    /// Configured byte quota, if any.
    pub quota_max_bytes: Option<u64>,
    /// TTL records past expiry but not yet evicted.
    pub ttl_expired_pending: u64,
}

/// Read-only consistency report from `Connection::doctor`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DoctorReport {
    /// Filesystem path when file-backed.
    pub file_path: Option<String>,
    /// Latest committed sequence number.
    pub last_commit_sequence: u64,
    /// Sequence folded into the main file at last checkpoint.
    pub last_checkpoint_sequence: u64,
    /// Commits not yet checkpointed.
    pub checkpoint_lag_records: u64,
    /// Uncheckpointed WAL bytes when in WAL mode.
    pub wal_bytes_uncheckpointed: u64,
    /// CAS objects without metadata references.
    pub orphan_object_count: u64,
    /// Namespace usage snapshots.
    pub namespaces: Vec<DoctorNamespaceStats>,
    /// Findings requiring attention.
    pub issues: Vec<DoctorIssue>,
}

/// Summary of CAS objects removed by orphan reclamation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct ReclaimReport {
    /// Objects deleted from the store.
    pub reclaimed_objects: u64,
}

/// 一次压缩通过的摘要（checkpoint + 安全孤儿回收）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct CompactReport {
    /// Whether WAL was folded into the main file.
    pub checkpointed: bool,
    /// Orphan blobs removed during the pass.
    pub orphans_reclaimed: u64,
}

/// Summary of keys removed by TTL or quota eviction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EvictReport {
    /// Records removed.
    pub evicted_records: u64,
    /// Payload bytes removed.
    pub evicted_bytes: u64,
}

/// How to release a lease after work finishes or fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// Return the item to the queued state.
    Requeue,
    /// Schedule a retry at a future timestamp.
    RetryAt {
        /// Milliseconds since UNIX epoch.
        when: u64,
    },
    /// Mark the item failed with a reason string.
    Failed {
        /// Human-readable failure reason.
        reason: String,
    },
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// 分布式分片、fencing、目录 schema 与运维合同。
pub mod cluster;

mod hex;
mod object;
/// 过渡：正式合同见 `database_udf`。
pub mod udf;
mod value;

pub use object::*;
pub use value::*;
