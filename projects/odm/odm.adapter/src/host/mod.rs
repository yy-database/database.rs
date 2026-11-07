//! Host session facade shared by the Node and Python bindings.
//!
//! The session state machine, profile dispatch and diagnostic classification
//! live here so host bindings cannot drift apart. Bindings only translate this
//! surface into JavaScript or Python values.
//!
//! Failure shapes are structured. Every fallible call returns an
//! [`ErrorEnvelope`] (`code` + `message`); the code is never folded back into a
//! display string, because callers assert on the code layer.

use std::{path::Path, sync::Mutex};

use odm_types::{CapabilitySet, DatasourceKind, Iris, Planner, ReferenceStore, Row, expand_endpoint, load_project, resolve_path};
use serde_json::{Value as JsonValue, json};

use crate::{
    adapter_mysql::MysqlSource,
    adapter_postgres::PostgresSource,
    adapter_sqlite::SqliteSource,
    connector_local::{Error as LocalError, LocalSource},
};

pub mod bind;
pub mod operation;
pub mod wire;

/// A unique key or primary key already exists.
pub const CODE_UNIQUE_CONFLICT: &str = "IRIS-UNIQUE-CONFLICT";
/// A unique `where` matched no row.
pub const CODE_NOT_FOUND: &str = "IRIS-NOT-FOUND";
/// A vector value does not match the schema dimension.
pub const CODE_VECTOR_DIMENSION: &str = "IRIS-VECTOR-DIMENSION";
/// Generated artifacts and the bound catalog disagree.
pub const CODE_SCHEMA_MISMATCH: &str = "IRIS-SCHEMA-MISMATCH";
/// The DSN could not be parsed, authenticated or connected.
pub const CODE_CONNECTION_FAILED: &str = "IRIS-CONNECTION-FAILED";
/// An argument, condition object or DSN shape is wrong.
pub const CODE_INVALID_ARGUMENT: &str = "IRIS-INVALID-ARGUMENT";
/// Begin, commit or rollback failed, including unsupported profiles.
pub const CODE_TRANSACTION_FAILED: &str = "IRIS-TRANSACTION-FAILED";
/// An unclassified internal failure.
pub const CODE_INTERNAL: &str = "IRIS-INTERNAL";

/// The complete author-facing code table. Nothing outside this list may
/// appear as an [`ErrorEnvelope::code`].
pub const AUTHOR_CODES: &[&str] = &[
    CODE_UNIQUE_CONFLICT,
    CODE_NOT_FOUND,
    CODE_VECTOR_DIMENSION,
    CODE_SCHEMA_MISMATCH,
    CODE_CONNECTION_FAILED,
    CODE_INVALID_ARGUMENT,
    CODE_TRANSACTION_FAILED,
    CODE_INTERNAL,
];

/// Diagnostic only: the selected backend has no usable wire client in this dev line.
pub const DIAG_BACKEND_UNSUPPORTED: &str = "IRIS-BACKEND-UNSUPPORTED";
/// Diagnostic only: the host session has been closed.
pub const DIAG_SESSION_CLOSED: &str = "IRIS-SESSION-CLOSED";
/// Diagnostic only: a parameter payload could not be bound.
pub const DIAG_PARAM_ENCODING: &str = "IRIS-PARAM-ENCODING";
/// Diagnostic only: the structured operation payload is invalid.
pub const DIAG_OPERATION_INVALID: &str = "IRIS-OPERATION-INVALID";
/// Diagnostic only: the bound backend rejected the execution.
pub const DIAG_EXEC_FAILED: &str = "IRIS-EXEC-FAILED";
/// Diagnostic only: the active profile has no transaction control.
pub const DIAG_TRANSACTION_UNSUPPORTED: &str = "IRIS-TRANSACTION-UNSUPPORTED";
/// Diagnostic only: a transaction is already open on this session.
pub const DIAG_TRANSACTION_ALREADY_OPEN: &str = "IRIS-TRANSACTION-ALREADY-OPEN";
/// Diagnostic only: no transaction is open on this session.
pub const DIAG_TRANSACTION_NOT_OPEN: &str = "IRIS-TRANSACTION-NOT-OPEN";
/// Diagnostic only: managed push is not available on this profile.
pub const DIAG_MANAGED_PUSH_UNSUPPORTED: &str = "IRIS-MANAGED-PUSH-UNSUPPORTED";
/// Diagnostic only: the selected backend cannot represent a schema capability.
pub const DIAG_SCHEMA_VECTOR_UNSUPPORTED: &str = "IRIS-SCHEMA-VECTOR-UNSUPPORTED";

/// Host facade version (the `odm-adapter` crate version).
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Structured failure shared by every host binding.
///
/// `kind` and `code` are the author-facing contract: both language bindings
/// pass them through unchanged and authors branch on them. `diagnostic_code`
/// carries the internal code (`IRIS-PLAN-REJECTED`, `23505`, `yydb.*`) for
/// logs and tests and is not part of that contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorEnvelope {
    /// Semantic class (`unique_conflict`, `not_found`, `invalid_argument`, …).
    pub kind: String,
    /// Stable author-facing code.
    pub code: String,
    /// Human-readable explanation.
    pub message: String,
    /// Internal diagnostic code, if any.
    pub diagnostic_code: Option<String>,
}

fn kind_for(code: &str) -> &'static str {
    match code {
        CODE_UNIQUE_CONFLICT => "unique_conflict",
        CODE_NOT_FOUND => "not_found",
        CODE_VECTOR_DIMENSION => "vector_dimension_mismatch",
        CODE_SCHEMA_MISMATCH => "schema_mismatch",
        CODE_CONNECTION_FAILED => "connection_failed",
        CODE_INVALID_ARGUMENT => "invalid_argument",
        CODE_TRANSACTION_FAILED => "transaction_failed",
        _ => "internal",
    }
}

impl ErrorEnvelope {
    /// Build an envelope from an author-facing code and a display message.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        let code = code.into();
        let kind = kind_for(&code).to_string();
        Self { kind, code, message: message.into(), diagnostic_code: None }
    }

    /// Attach the internal diagnostic code that produced this failure.
    pub fn with_diagnostic(mut self, diagnostic: impl Into<String>) -> Self {
        self.diagnostic_code = Some(diagnostic.into());
        self
    }

    /// Unclassified failure with the original internal code as diagnostic.
    pub fn internal(diagnostic: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(CODE_INTERNAL, message).with_diagnostic(diagnostic)
    }

    /// Author-argument failure with the original internal code as diagnostic.
    pub fn invalid_argument(diagnostic: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(CODE_INVALID_ARGUMENT, message).with_diagnostic(diagnostic)
    }

    /// JSON form used on the wire.
    pub fn to_json(&self) -> JsonValue {
        json!({
            "kind": self.kind,
            "code": self.code,
            "message": self.message,
            "diagnosticCode": self.diagnostic_code,
        })
    }
}

/// Result of one session operation on the host wire.
#[derive(Debug, Clone)]
pub struct ExecuteOutcome {
    /// Whether the operation succeeded.
    pub ok: bool,
    /// Encoded row set (empty for unit-valued calls and failures).
    pub rows: Vec<JsonValue>,
    /// Structured failure when `ok` is false.
    pub error: Option<ErrorEnvelope>,
}

impl ExecuteOutcome {
    /// Successful query with the encoded row set.
    pub fn rows(rows: Vec<Row>) -> Self {
        Self { ok: true, rows: wire::rows_to_json(rows), error: None }
    }

    /// Successful unit-valued call (`execute`, `begin`, `commit`, `close`).
    pub fn unit() -> Self {
        Self { ok: true, rows: Vec::new(), error: None }
    }

    /// Structured failure.
    pub fn failure(error: ErrorEnvelope) -> Self {
        Self { ok: false, rows: Vec::new(), error: Some(error) }
    }

    /// Full envelope JSON: `{"ok": …, "rows": […], "error": {…} | null}`.
    pub fn to_json(&self) -> String {
        json!({
            "ok": self.ok,
            "rows": self.rows,
            "error": self.error.as_ref().map(ErrorEnvelope::to_json),
        })
        .to_string()
    }

    /// Row array JSON (N-API `rowsJson`).
    pub fn rows_json(&self) -> String {
        JsonValue::Array(self.rows.clone()).to_string()
    }

    /// Display message of the failure, if any.
    pub fn error_message(&self) -> Option<String> {
        self.error.as_ref().map(|err| err.message.clone())
    }

    /// Stable code of the failure, if any.
    pub fn error_code(&self) -> Option<String> {
        self.error.as_ref().map(|err| err.code.clone())
    }

    /// Semantic class of the failure, if any.
    pub fn error_kind(&self) -> Option<String> {
        self.error.as_ref().map(|err| err.kind.clone())
    }

    /// Internal diagnostic code of the failure, if any.
    pub fn error_diagnostic_code(&self) -> Option<String> {
        self.error.as_ref().and_then(|err| err.diagnostic_code.clone())
    }
}

fn from_odm_error(err: odm_types::Error) -> ErrorEnvelope {
    match err {
        // A rejected plan means the requested shape does not fit the schema or
        // the bound catalog; the original code stays as a diagnostic.
        odm_types::Error::Diagnostic(diag) => ErrorEnvelope::invalid_argument(diag.code, diag.message),
        odm_types::Error::Ir(inner) => ErrorEnvelope::internal(DIAG_EXEC_FAILED, inner.to_string()),
        odm_types::Error::Runtime(message) => ErrorEnvelope::internal(DIAG_EXEC_FAILED, message),
    }
}

/// Source text for backends that can only consume VOS text.
fn text_source(bound: &bind::BoundParameters) -> Result<&str, ErrorEnvelope> {
    bound.text_only().map_err(|err| ErrorEnvelope::invalid_argument(DIAG_PARAM_ENCODING, err))
}

fn classify_backend_message(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("unique constraint failed") || lower.contains("primary key") {
        CODE_UNIQUE_CONFLICT
    }
    else if lower.contains("no such table") || lower.contains("no such column") {
        CODE_SCHEMA_MISMATCH
    }
    else if lower.contains("unable to open database file") || lower.contains("database is locked") {
        CODE_CONNECTION_FAILED
    }
    else {
        CODE_INTERNAL
    }
}

fn from_sqlite_error(err: crate::Error) -> ErrorEnvelope {
    let message = err.to_string();
    let code = classify_backend_message(&message);
    let diagnostic = match code {
        CODE_UNIQUE_CONFLICT => "sqlite.unique",
        CODE_SCHEMA_MISMATCH => "sqlite.schema",
        CODE_CONNECTION_FAILED => "sqlite.io",
        _ => "sqlite.error",
    };
    ErrorEnvelope::new(code, message).with_diagnostic(diagnostic)
}

fn from_postgres_error(err: crate::Error) -> ErrorEnvelope {
    match &err {
        crate::Error::Wire(inner) => ErrorEnvelope::new(CODE_CONNECTION_FAILED, inner.to_string()).with_diagnostic("postgres.wire"),
        crate::Error::Vos(message) => ErrorEnvelope::new(CODE_SCHEMA_MISMATCH, message).with_diagnostic("postgres.vos"),
        crate::Error::Policy(message) => ErrorEnvelope::new(CODE_SCHEMA_MISMATCH, message).with_diagnostic("IRIS-SCHEMA-POLICY"),
        crate::Error::Unsupported(message) => ErrorEnvelope::new(CODE_CONNECTION_FAILED, message).with_diagnostic(DIAG_BACKEND_UNSUPPORTED),
        crate::Error::Sqlite(message) => ErrorEnvelope::new(CODE_INTERNAL, message).with_diagnostic("postgres.unsupported"),
        crate::Error::Io(inner) => ErrorEnvelope::new(CODE_CONNECTION_FAILED, inner.to_string()).with_diagnostic("postgres.io"),
    }
}

fn from_mysql_error(err: crate::Error) -> ErrorEnvelope {
    match &err {
        crate::Error::Wire(inner) => ErrorEnvelope::new(CODE_CONNECTION_FAILED, inner.to_string()).with_diagnostic("mysql.wire"),
        crate::Error::Vos(message) => ErrorEnvelope::new(CODE_SCHEMA_MISMATCH, message).with_diagnostic("mysql.vos"),
        crate::Error::Policy(message) => ErrorEnvelope::new(CODE_SCHEMA_MISMATCH, message).with_diagnostic("IRIS-SCHEMA-POLICY"),
        crate::Error::Unsupported(message) => ErrorEnvelope::new(CODE_CONNECTION_FAILED, message).with_diagnostic(DIAG_BACKEND_UNSUPPORTED),
        crate::Error::Sqlite(message) => ErrorEnvelope::new(CODE_INTERNAL, message).with_diagnostic("mysql.unsupported"),
        crate::Error::Io(inner) => ErrorEnvelope::new(CODE_CONNECTION_FAILED, inner.to_string()).with_diagnostic("mysql.io"),
    }
}

fn from_local_error(err: LocalError) -> ErrorEnvelope {
    match err {
        LocalError::Yydb(inner) => {
            let code = match &inner {
                database_execution::engine::Error::UniqueViolation { .. } => CODE_UNIQUE_CONFLICT,
                database_execution::engine::Error::VectorDimensionExceeded { .. } => CODE_VECTOR_DIMENSION,
                database_execution::engine::Error::Schema { .. }
                | database_execution::engine::Error::SchemaConflict { .. }
                | database_execution::engine::Error::SchemaInvalidated { .. } => CODE_SCHEMA_MISMATCH,
                database_execution::engine::Error::Io(_) | database_execution::engine::Error::SessionClosed => CODE_CONNECTION_FAILED,
                _ => CODE_INTERNAL,
            };
            ErrorEnvelope::new(code, inner.to_string()).with_diagnostic(inner.code_with_prefix("yydb"))
        }
        LocalError::NotReady(report) => ErrorEnvelope::new(CODE_CONNECTION_FAILED, report.message).with_diagnostic(report.code),
        LocalError::Runtime(message) => ErrorEnvelope::internal(DIAG_EXEC_FAILED, message),
        LocalError::Policy(message) => ErrorEnvelope::new(CODE_SCHEMA_MISMATCH, message).with_diagnostic("yydb.policy"),
    }
}

fn closed() -> ErrorEnvelope {
    ErrorEnvelope::invalid_argument(DIAG_SESSION_CLOSED, "session closed")
}

fn transaction_unsupported(profile: &str) -> ErrorEnvelope {
    ErrorEnvelope::new(CODE_TRANSACTION_FAILED, format!("{profile} sessions have no transaction control"))
        .with_diagnostic(DIAG_TRANSACTION_UNSUPPORTED)
}

fn transaction_already_open() -> ErrorEnvelope {
    ErrorEnvelope::invalid_argument(DIAG_TRANSACTION_ALREADY_OPEN, "a transaction is already open on this session")
}

fn transaction_not_open() -> ErrorEnvelope {
    ErrorEnvelope::invalid_argument(DIAG_TRANSACTION_NOT_OPEN, "no transaction is open on this session")
}

fn lock_failed() -> ErrorEnvelope {
    ErrorEnvelope::internal(DIAG_EXEC_FAILED, "session transaction lock is poisoned")
}

/// Reject a schema the selected SQL backend cannot represent.
///
/// Vector search is a yydb capability, so building a SQLite / MySQL /
/// PostgreSQL client from a schema that declares `vector` fields fails here,
/// before any call, instead of surfacing later as an argument error.
fn reject_unsupported_schema(profile: &str, schema: Option<&str>) -> Result<(), ErrorEnvelope> {
    let Some(schema) = schema
    else {
        return Ok(());
    };
    if schema.trim().is_empty() || !schema_declares_vector(schema)? {
        return Ok(());
    }
    Err(ErrorEnvelope::new(
        CODE_SCHEMA_MISMATCH,
        format!("{profile} cannot represent the `vector` fields of this schema: vector search is only available on yydb profiles"),
    )
    .with_diagnostic(DIAG_SCHEMA_VECTOR_UNSUPPORTED))
}

fn schema_declares_vector(schema: &str) -> Result<bool, ErrorEnvelope> {
    use vos::syntax::VosDeclarationKind;
    let input = vos::parse_oak(schema)
        .map_err(|message| ErrorEnvelope::new(CODE_SCHEMA_MISMATCH, message).with_diagnostic(DIAG_SCHEMA_VECTOR_UNSUPPORTED))?;
    let root = input.into_oak();
    let declarations = root
        .declarations
        .iter()
        .filter(|declaration| matches!(declaration.kind, VosDeclarationKind::Namespace | VosDeclarationKind::Table | VosDeclarationKind::Class))
        .cloned()
        .collect();
    let projection = vos::ContractInput::from_oak(vos::syntax::VosRoot { source: root.source, syntax: root.syntax, declarations })
        .project_schema()
        .map_err(|diagnostics| {
            let reason =
                diagnostics.iter().map(|diagnostic| format!("{}: {}", diagnostic.code, diagnostic.message)).collect::<Vec<_>>().join("\n");
            ErrorEnvelope::new(CODE_SCHEMA_MISMATCH, reason).with_diagnostic(DIAG_SCHEMA_VECTOR_UNSUPPORTED)
        })?;
    Ok(projection.types.iter().flat_map(|item| item.fields.iter()).any(|field| type_declares_vector(&field.canonical_type)))
}

fn type_declares_vector(ty: &vos::CanonicalType) -> bool {
    match ty {
        vos::CanonicalType::Named(path) => path.last().is_some_and(|name| name == "vector"),
        vos::CanonicalType::Reference(inner) | vos::CanonicalType::Optional(inner) | vos::CanonicalType::List(inner) => {
            type_declares_vector(inner)
        }
        vos::CanonicalType::Generic { path, arguments } => {
            path.last().is_some_and(|name| name == "vector")
                || arguments.iter().any(|argument| match argument {
                    vos::CanonicalTypeArgument::Type(inner) => type_declares_vector(inner),
                    vos::CanonicalTypeArgument::Literal(_) => false,
                })
        }
    }
}

enum SessionStore {
    Memory(Iris),
    Sqlite(SqliteSource),
    /// Embedded YYDB (`.yydb` file or `.yydx` main file plus its object store).
    Yydb(LocalSource),
    /// A PostgreSQL session keeps its open transaction, if any, on this
    /// connection so statements inside `transaction` share one backend transaction.
    Postgres {
        db: PostgresSource,
        transaction: Mutex<Option<crate::adapter_postgres::PostgresTransaction>>,
    },
    /// MySQL counterpart of [`SessionStore::Postgres`].
    Mysql {
        db: MysqlSource,
        transaction: Mutex<Option<crate::adapter_mysql::MysqlTransaction>>,
    },
}

impl SessionStore {
    fn capabilities(&self) -> CapabilitySet {
        match self {
            Self::Memory(_) => CapabilitySet::reference_full(),
            Self::Sqlite(_) => SqliteSource::capabilities(),
            Self::Yydb(_) => LocalSource::capabilities(),
            Self::Postgres { .. } => PostgresSource::capabilities(),
            Self::Mysql { .. } => MysqlSource::capabilities(),
        }
    }

    fn query(&self, planner: &Planner, bound: &bind::BoundParameters) -> Result<Vec<Row>, ErrorEnvelope> {
        match self {
            Self::Memory(iris) => iris.session().query(text_source(bound)?).map_err(from_odm_error),
            Self::Sqlite(db) => {
                let plan = planner.plan_source(text_source(bound)?).map_err(from_odm_error)?;
                db.execute_plan(&plan).map_err(from_sqlite_error)
            }
            // YYDB binds typed `bytes` / `vector` / `file` parameters itself, so
            // the source keeps its `$name` placeholders and the payloads travel
            // beside it instead of being rendered into VOS text.
            Self::Yydb(db) => db.run_with_params(&bound.source, &bound.value_parameters).map_err(from_local_error),
            Self::Postgres { db, transaction } => {
                let plan = planner.plan_source(text_source(bound)?).map_err(from_odm_error)?;
                let mut guard = transaction.lock().map_err(|_| lock_failed())?;
                match guard.as_mut() {
                    Some(txn) => txn.execute_plan(&plan).map_err(from_postgres_error),
                    None => db.execute_plan(&plan).map_err(from_postgres_error),
                }
            }
            Self::Mysql { db, transaction } => {
                let plan = planner.plan_source(text_source(bound)?).map_err(from_odm_error)?;
                let mut guard = transaction.lock().map_err(|_| lock_failed())?;
                match guard.as_mut() {
                    Some(txn) => txn.execute_plan(&plan).map_err(from_mysql_error),
                    None => db.execute_plan(&plan).map_err(from_mysql_error),
                }
            }
        }
    }

    fn execute_unit(&self, planner: &Planner, bound: &bind::BoundParameters) -> Result<(), ErrorEnvelope> {
        match self {
            Self::Memory(iris) => iris.session().execute(text_source(bound)?).map_err(from_odm_error),
            Self::Yydb(db) => db.execute_with_params(&bound.source, &bound.value_parameters).map_err(from_local_error),
            _ => self.query(planner, bound).map(|_| ()),
        }
    }

    fn begin(&self) -> Result<(), ErrorEnvelope> {
        match self {
            Self::Memory(_) => Err(transaction_unsupported("memory")),
            Self::Sqlite(db) => {
                if db.in_transaction() {
                    return Err(transaction_already_open());
                }
                db.begin().map_err(from_sqlite_error)
            }
            Self::Yydb(db) => {
                if db.in_transaction() {
                    return Err(transaction_already_open());
                }
                db.begin().map_err(from_local_error)
            }
            Self::Postgres { db, transaction } => {
                let mut guard = transaction.lock().map_err(|_| lock_failed())?;
                if guard.is_some() {
                    return Err(transaction_already_open());
                }
                *guard = Some(db.begin_transaction().map_err(from_postgres_error)?);
                Ok(())
            }
            Self::Mysql { db, transaction } => {
                let mut guard = transaction.lock().map_err(|_| lock_failed())?;
                if guard.is_some() {
                    return Err(transaction_already_open());
                }
                *guard = Some(db.begin_transaction().map_err(from_mysql_error)?);
                Ok(())
            }
        }
    }

    fn commit(&self) -> Result<(), ErrorEnvelope> {
        match self {
            Self::Memory(_) => Err(transaction_unsupported("memory")),
            Self::Sqlite(db) => {
                if !db.in_transaction() {
                    return Err(transaction_not_open());
                }
                db.commit().map_err(from_sqlite_error)
            }
            Self::Yydb(db) => {
                if !db.in_transaction() {
                    return Err(transaction_not_open());
                }
                db.commit().map_err(from_local_error)
            }
            Self::Postgres { transaction, .. } => {
                let mut guard = transaction.lock().map_err(|_| lock_failed())?;
                match guard.take() {
                    Some(mut txn) => txn.commit().map_err(from_postgres_error),
                    None => Err(transaction_not_open()),
                }
            }
            Self::Mysql { transaction, .. } => {
                let mut guard = transaction.lock().map_err(|_| lock_failed())?;
                match guard.take() {
                    Some(mut txn) => txn.commit().map_err(from_mysql_error),
                    None => Err(transaction_not_open()),
                }
            }
        }
    }

    fn rollback(&self) -> Result<(), ErrorEnvelope> {
        match self {
            Self::Memory(_) => Err(transaction_unsupported("memory")),
            Self::Sqlite(db) => {
                if !db.in_transaction() {
                    return Err(transaction_not_open());
                }
                db.rollback().map_err(from_sqlite_error)
            }
            Self::Yydb(db) => {
                if !db.in_transaction() {
                    return Err(transaction_not_open());
                }
                db.rollback().map_err(from_local_error)
            }
            Self::Postgres { transaction, .. } => {
                let mut guard = transaction.lock().map_err(|_| lock_failed())?;
                match guard.take() {
                    Some(mut txn) => txn.rollback().map_err(from_postgres_error),
                    None => Err(transaction_not_open()),
                }
            }
            Self::Mysql { transaction, .. } => {
                let mut guard = transaction.lock().map_err(|_| lock_failed())?;
                match guard.take() {
                    Some(mut txn) => txn.rollback().map_err(from_mysql_error),
                    None => Err(transaction_not_open()),
                }
            }
        }
    }
}

/// Host session bound to a reference or foreign adapter.
pub struct HostSession {
    store: Option<SessionStore>,
    planner: Planner,
    closed: bool,
}

impl HostSession {
    fn new_store(store: SessionStore) -> Self {
        let planner = Planner::new(store.capabilities());
        Self { store: Some(store), planner, closed: false }
    }

    /// Reference profile: read-only, unseeded.
    pub fn open_memory() -> Self {
        Self::new_store(SessionStore::Memory(Iris::new(CapabilitySet::reference_full(), ReferenceStore::new())))
    }

    /// SQLite profile (`:memory:` or a file path).
    pub fn open_sqlite(path: &str) -> Result<Self, ErrorEnvelope> {
        let db = if path == ":memory:" { SqliteSource::open_in_memory() } else { SqliteSource::open(path) }.map_err(from_sqlite_error)?;
        Ok(Self::new_store(SessionStore::Sqlite(db)))
    }

    /// YYDB profile over a `.yydb` file or `.yydx` directory layout.
    ///
    /// A `.yydx` main path is resolved by the embedded engine to its sibling
    /// object store; `:memory:` opens an ephemeral database.
    pub fn open_yydb(endpoint: &str) -> Result<Self, ErrorEnvelope> {
        let path = endpoint.strip_prefix("file:").unwrap_or(endpoint);
        let db = if path == ":memory:" { LocalSource::open_in_memory() } else { LocalSource::open(path) }.map_err(from_local_error)?;
        Ok(Self::new_store(SessionStore::Yydb(db)))
    }

    /// PostgreSQL profile over a real server URL.
    pub fn open_postgres(url: &str) -> Result<Self, ErrorEnvelope> {
        let db = PostgresSource::connect(url).map_err(from_postgres_error)?;
        Ok(Self::new_store(SessionStore::Postgres { db, transaction: Mutex::new(None) }))
    }

    /// MySQL profile over a real server URL.
    pub fn open_mysql(url: &str) -> Result<Self, ErrorEnvelope> {
        let db = MysqlSource::connect(url).map_err(from_mysql_error)?;
        Ok(Self::new_store(SessionStore::Mysql { db, transaction: Mutex::new(None) }))
    }

    /// Project profile resolved from an Iris config and datasource name.
    pub fn open_project(config_path: &str, source: &str) -> Result<Self, ErrorEnvelope> {
        let (project_dir, project) =
            load_project(Path::new(config_path)).map_err(|err| ErrorEnvelope::internal(DIAG_EXEC_FAILED, err.to_string()))?;
        let ds = project.datasource(source).map_err(|err| ErrorEnvelope::internal(DIAG_EXEC_FAILED, err.to_string()))?;
        let store = match ds.kind {
            DatasourceKind::Yydb => {
                let endpoint = expand_endpoint(ds, source).map_err(|err| ErrorEnvelope::internal(DIAG_EXEC_FAILED, err))?;
                let path = resolve_path(&project_dir, &endpoint);
                SessionStore::Yydb(LocalSource::open(path).map_err(from_local_error)?)
            }
            DatasourceKind::Sqlite => {
                let endpoint = expand_endpoint(ds, source).map_err(|err| ErrorEnvelope::internal(DIAG_EXEC_FAILED, err))?;
                let path = resolve_path(&project_dir, &endpoint);
                SessionStore::Sqlite(SqliteSource::open(path).map_err(from_sqlite_error)?)
            }
            DatasourceKind::Postgres => {
                let url = expand_endpoint(ds, source).map_err(|err| ErrorEnvelope::internal(DIAG_EXEC_FAILED, err))?;
                let db = PostgresSource::connect(&url).map_err(from_postgres_error)?;
                SessionStore::Postgres { db, transaction: Mutex::new(None) }
            }
            DatasourceKind::Mysql => {
                let url = expand_endpoint(ds, source).map_err(|err| ErrorEnvelope::internal(DIAG_EXEC_FAILED, err))?;
                let db = MysqlSource::connect(&url).map_err(from_mysql_error)?;
                SessionStore::Mysql { db, transaction: Mutex::new(None) }
            }
            other => {
                return Err(ErrorEnvelope::invalid_argument(
                    DIAG_OPERATION_INVALID,
                    format!("open_project does not support {:?} yet (use sqlite/postgres/mysql)", other),
                ));
            }
        };
        Ok(Self::new_store(store))
    }

    /// Session selected by a DSN.
    ///
    /// Managed adapters (`sqlite:` and `file:`) apply `schema` when it is
    /// supplied; foreign servers get their DDL from migrations instead. SQL
    /// backends also reject a schema that declares `vector` fields while the
    /// client is built, because vector search is a yydb capability.
    pub fn open_dsn(dsn: &str, schema: Option<&str>) -> Result<Self, ErrorEnvelope> {
        let text = dsn.trim();
        if text.is_empty() {
            return Err(ErrorEnvelope::new(CODE_INVALID_ARGUMENT, "`dsn` must be a non-empty string"));
        }
        let (session, managed) = if text == "memory:" || text == "memory" {
            (Self::open_memory(), false)
        }
        else if let Some(path) = text.strip_prefix("sqlite:") {
            reject_unsupported_schema("SQLite", schema)?;
            (Self::open_sqlite(if path.is_empty() { ":memory:" } else { path })?, true)
        }
        else if let Some(path) = text.strip_prefix("file:") {
            if path.is_empty() {
                return Err(ErrorEnvelope::new(CODE_INVALID_ARGUMENT, "`file:` DSN requires a path"));
            }
            (Self::open_yydb(path)?, true)
        }
        else if text.starts_with("mysql://") {
            reject_unsupported_schema("MySQL", schema)?;
            (Self::open_mysql(text)?, false)
        }
        else if text.starts_with("postgres://") || text.starts_with("postgresql://") {
            reject_unsupported_schema("PostgreSQL", schema)?;
            (Self::open_postgres(text)?, false)
        }
        else {
            return Err(ErrorEnvelope::new(CODE_INVALID_ARGUMENT, format!("unrecognized DSN `{text}`")));
        };
        if managed {
            if let Some(schema) = schema {
                if !schema.trim().is_empty() {
                    session.managed_push(schema)?;
                }
            }
        }
        Ok(session)
    }

    /// Whether the session has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    fn bind_parameters(&self, source: &str, parameters_json: Option<&str>) -> Result<bind::BoundParameters, ErrorEnvelope> {
        match parameters_json {
            Some(json) => bind::bind_parameters_typed(source, json).map_err(|err| ErrorEnvelope::invalid_argument(DIAG_PARAM_ENCODING, err)),
            None => Ok(bind::BoundParameters::unbound(source)),
        }
    }

    /// Run a bound operation through the ODM planner.
    fn run_bound_query(&self, bound: &bind::BoundParameters) -> ExecuteOutcome {
        let Some(store) = &self.store
        else {
            return ExecuteOutcome::failure(closed());
        };
        match store.query(&self.planner, bound) {
            Ok(rows) => ExecuteOutcome::rows(rows),
            Err(err) => ExecuteOutcome::failure(err),
        }
    }
    /// Plan + execute VOS DML (returns rows).
    pub fn query(&self, source: &str, parameters_json: Option<&str>) -> ExecuteOutcome {
        if self.closed {
            return ExecuteOutcome::failure(closed());
        }
        // Bind failures are request-level rejections: they travel the same
        // structured envelope as backend diagnostics instead of an untyped
        // host exception.
        let bound = match self.bind_parameters(source, parameters_json) {
            Ok(bound) => bound,
            Err(err) => return ExecuteOutcome::failure(err),
        };
        self.run_bound_query(&bound)
    }

    /// Execute unit-valued / DDL-shaped VOS.
    pub fn execute(&self, source: &str, parameters_json: Option<&str>) -> ExecuteOutcome {
        if self.closed {
            return ExecuteOutcome::failure(closed());
        }
        let bound = match self.bind_parameters(source, parameters_json) {
            Ok(bound) => bound,
            Err(err) => return ExecuteOutcome::failure(err),
        };
        let Some(store) = &self.store
        else {
            return ExecuteOutcome::failure(closed());
        };
        match store.execute_unit(&self.planner, &bound) {
            Ok(()) => ExecuteOutcome::unit(),
            Err(err) => ExecuteOutcome::failure(err),
        }
    }

    /// Execute a structured operation JSON payload (generated client ABI).
    pub fn execute_operation(&self, request_json: &str) -> ExecuteOutcome {
        if self.closed {
            return ExecuteOutcome::failure(closed());
        }
        let bound = match operation::encode_request_json(request_json) {
            Ok(bound) => bound,
            Err(err) => return ExecuteOutcome::failure(ErrorEnvelope::invalid_argument(DIAG_OPERATION_INVALID, err)),
        };
        self.run_bound_query(&bound)
    }

    /// Apply the managed-push schema to a managed SQL session.
    pub fn managed_push(&self, schema: &str) -> Result<(), ErrorEnvelope> {
        if self.closed {
            return Err(closed());
        }
        match &self.store {
            Some(SessionStore::Sqlite(db)) => db.managed_push(schema).map_err(from_sqlite_error).map(|_| ()),
            Some(SessionStore::Yydb(db)) => db.ensure_schema(schema).map_err(from_local_error),
            Some(SessionStore::Postgres { db, .. }) => db.managed_push(schema).map_err(from_postgres_error).map(|_| ()),
            Some(SessionStore::Mysql { db, .. }) => db.managed_push(schema).map_err(from_mysql_error).map(|_| ()),
            _ => Err(ErrorEnvelope::invalid_argument(
                DIAG_MANAGED_PUSH_UNSUPPORTED,
                "managed_push is only supported on sqlite, yydb, postgres and mysql sessions",
            )),
        }
    }

    /// Open a transaction on profiles that expose transaction control.
    pub fn begin(&self) -> ExecuteOutcome {
        if self.closed {
            return ExecuteOutcome::failure(closed());
        }
        match &self.store {
            Some(store) => match store.begin() {
                Ok(()) => ExecuteOutcome::unit(),
                Err(err) => ExecuteOutcome::failure(err),
            },
            None => ExecuteOutcome::failure(closed()),
        }
    }

    /// Commit the active transaction.
    pub fn commit(&self) -> ExecuteOutcome {
        if self.closed {
            return ExecuteOutcome::failure(closed());
        }
        match &self.store {
            Some(store) => match store.commit() {
                Ok(()) => ExecuteOutcome::unit(),
                Err(err) => ExecuteOutcome::failure(err),
            },
            None => ExecuteOutcome::failure(closed()),
        }
    }

    /// Roll back the active transaction.
    pub fn rollback(&self) -> ExecuteOutcome {
        if self.closed {
            return ExecuteOutcome::failure(closed());
        }
        match &self.store {
            Some(store) => match store.rollback() {
                Ok(()) => ExecuteOutcome::unit(),
                Err(err) => ExecuteOutcome::failure(err),
            },
            None => ExecuteOutcome::failure(closed()),
        }
    }

    /// Close the session, releasing the bound datasource.
    ///
    /// Dropping the store is what frees a file-backed datasource's writer
    /// lock, so `close` must do more than flip a flag: after it returns the
    /// same database file can be opened again from this process.
    pub fn close(&mut self) {
        self.closed = true;
        if let Some(SessionStore::Yydb(db)) = self.store.as_ref() {
            db.close();
        }
        drop(self.store.take());
    }

    /// SQLite engine version for the active sqlite session.
    pub fn sqlite_engine_version(&self) -> Result<String, ErrorEnvelope> {
        if self.closed {
            return Err(closed());
        }
        match &self.store {
            Some(SessionStore::Sqlite(db)) => db.sqlite_version().map_err(from_sqlite_error),
            _ => Err(ErrorEnvelope::invalid_argument(DIAG_OPERATION_INVALID, "sqlite identity requires a sqlite session")),
        }
    }

    /// SQLite engine source id for the active sqlite session.
    pub fn sqlite_source_id(&self) -> Result<String, ErrorEnvelope> {
        if self.closed {
            return Err(closed());
        }
        match &self.store {
            Some(SessionStore::Sqlite(db)) => db.sqlite_source_id().map_err(from_sqlite_error),
            _ => Err(ErrorEnvelope::invalid_argument(DIAG_OPERATION_INVALID, "sqlite identity requires a sqlite session")),
        }
    }
}
