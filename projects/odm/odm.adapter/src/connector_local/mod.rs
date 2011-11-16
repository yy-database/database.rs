//! 本地嵌入式引擎连接器（`BACKEND_ID = "yydb"` 仅作握手标识）。
//!
//! 经 `database_execution::engine::Connection` 使用正式 VOS / 共享 IR；
//! 本连接器叠加 ODM 策略与行映射。

#![deny(missing_docs)]

mod error;
mod plan;

use std::{
    cell::RefCell,
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use database_execution::engine::SchemaVersion;
use database_query::{
    ir::{IrVersion, PhysicalPlan},
    local::PreparedStatement,
};
use odm_types::{CapabilitySet, FileDescriptor, FileManifestRef, QueryCaps, RowWrite, VectorValue, WriteCaps};
use serde::{Deserialize, Serialize};

pub use database_execution::engine::Connection;
pub use error::{Error, Result};

use database_execution::engine::Value as EngineValue;

// ---------------------------------------------------------------------------
// Readiness probe
// ---------------------------------------------------------------------------

/// Connector identifier.
pub const BACKEND_ID: &str = "yydb";

/// Stable diagnostic code when the native executor is unavailable.
pub const READINESS_CODE: &str = "IRIS-YYDB-VOS-EXECUTOR-NOT-READY";

/// Stable code when a prepared plan was built against an older DDL revision.
pub const PREPARED_STALE_CODE: &str = "VOS-PREPARED-STALE";

/// Stable code when a session was opened against an older DDL revision.
pub const SESSION_STALE_CODE: &str = "VOS-SESSION-STALE";

/// What Iris requires from YYDB before enabling native VOS execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadinessReport {
    /// Connector id.
    pub backend_id: String,
    /// True when schema install / read works on the public facade.
    pub schema_handshake_ready: bool,
    /// True when `Connection` exposes the VOS executor family (`prepare_vos`,
    /// `query_prepared`, `query_vos`, `execute_vos_write`) — live since the
    /// aligned `yydb` workspace.
    pub vos_executor_ready: bool,
    /// Stable diagnostic code.
    pub code: String,
    /// Human-readable blocker summary (no secrets).
    pub message: String,
}

impl ReadinessReport {
    /// Probe current readiness against the pinned `yydb` dependency.
    pub fn probe() -> Self {
        let schema_handshake_ready = Connection::open_in_memory().is_ok();
        // YYDB Connection now exposes the VOS executor family:
        //   prepare_vos → query_prepared / query_vos_with_params / query_vos
        //   execute_vos_write
        // plus transaction begin/commit/rollback and schema handshake.
        let vos_executor_ready = schema_handshake_ready;
        Self {
            backend_id: BACKEND_ID.into(),
            schema_handshake_ready,
            vos_executor_ready,
            code: if vos_executor_ready { "IRIS-YYDB-VOS-EXECUTOR-READY" } else { READINESS_CODE }.into(),
            message: if vos_executor_ready {
                "YYDB native VOS executor is live on the public Connection facade".into()
            }
            else {
                "YYDB native VOS executor not yet available on the Connection facade".into()
            },
        }
    }

    /// True only when every readiness bit is set.
    pub fn is_ready(&self) -> bool {
        self.schema_handshake_ready && self.vos_executor_ready
    }
}

// ---------------------------------------------------------------------------
// Schema handshake types
// ---------------------------------------------------------------------------

/// Schema handshake snapshot after open / ensure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaHandshake {
    /// Connector id (always [`BACKEND_ID`]).
    pub backend_id: &'static str,
    /// Stored schema version when present.
    pub schema_version: Option<u32>,
    /// Placeholder until YYDB exposes DDL revision on the public facade.
    pub ddl_revision: u64,
    /// Whether a VOS document is installed.
    pub has_document: bool,
}

// ---------------------------------------------------------------------------
// LocalSource — native datasource handle
// ---------------------------------------------------------------------------

/// Native YYDB datasource handle.
///
/// The connection lives behind a [`RefCell`] so the adapter surface can hand
/// out `&mut Connection` for transactions (matching the converged adapter
/// shape) while every other method keeps the shared `&self` style.
pub struct LocalSource {
    conn: RefCell<Connection>,
    path: Option<PathBuf>,
}

impl std::fmt::Debug for LocalSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalSource").field("backend", &BACKEND_ID).field("path", &self.path).finish_non_exhaustive()
    }
}

impl LocalSource {
    /// Capability set advertised by this connector.
    pub fn capabilities() -> CapabilitySet {
        CapabilitySet {
            backend_id: BACKEND_ID.into(),
            backend_version: database_execution::engine::version().into(),
            ir_version_max: IrVersion::PHASE1,
            query: QueryCaps::full(),
            write: WriteCaps::full(),
            budget: odm_types::CompensationBudget::default(),
        }
    }

    /// Current readiness probe.
    pub fn readiness() -> ReadinessReport {
        ReadinessReport::probe()
    }

    fn require_vos_executor(&self) -> Result<()> {
        if !Self::readiness().is_ready() {
            return Err(Error::NotReady(Self::readiness()));
        }
        Ok(())
    }

    /// Open an in-memory YYDB (tests / ephemeral).
    pub fn open_in_memory() -> Result<Self> {
        Ok(Self { conn: RefCell::new(Connection::open_in_memory()?), path: None })
    }

    /// Open or create a file-backed `.yydb`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        Ok(Self { conn: RefCell::new(Connection::open(&path)?), path: Some(path) })
    }

    /// On-disk path when file-backed.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Borrow the underlying YYDB connection (escape hatch for advanced ops).
    pub fn connection(&self) -> std::cell::Ref<'_, Connection> {
        self.conn.borrow()
    }

    /// Install / verify schema (Native Pull handshake input).
    pub fn ensure_schema(&self, document: &str) -> Result<()> {
        self.conn.borrow().ensure_schema(document)?;
        Ok(())
    }

    /// Read schema handshake data available on the current YYDB facade.
    pub fn schema_handshake(&self) -> Result<SchemaHandshake> {
        let schema: Option<SchemaVersion> = self.conn.borrow().schema()?;
        let has_document = schema.is_some();
        Ok(SchemaHandshake {
            backend_id: BACKEND_ID,
            schema_version: schema.as_ref().map(|s| s.version),
            ddl_revision: schema.map(|s| u64::from(s.version)).unwrap_or(0),
            has_document,
        })
    }

    /// Execute a VOS read pipeline and return typed Iris rows.
    pub fn query(&self, program: &str) -> Result<Vec<odm_types::Row>> {
        self.query_with_params(program, &BTreeMap::new())
    }

    /// Execute a VOS read pipeline with typed named parameters.
    ///
    /// Typed `bytes` parameters move their octets into the column through
    /// YYDB's own binding, so no text form of the payload is ever decoded.
    /// `vector` and `file` payloads are carried as a tagged JSON string inside
    /// `Iris::Value::Str` and stripped back to their native YYDB variants
    /// here.
    ///
    /// Returns `Err` when the result set contains a YYDB variant Iris cannot
    /// faithfully represent — currently only the CAS `Object` handle.
    pub fn query_with_params(&self, program: &str, named: &BTreeMap<String, odm_types::Value>) -> Result<Vec<odm_types::Row>> {
        self.require_vos_executor()?;
        let named = value_params_to_engine(named)?;
        let rows = self.conn.borrow().query_vos_with_params(program, &named)?;
        rows.into_iter().map(row_from_engine).collect()
    }

    /// Execute a VOS write program (schema macros / DML).
    pub fn execute(&self, program: &str) -> Result<()> {
        self.execute_with_params(program, &BTreeMap::new())
    }

    /// Execute a VOS write program with typed named parameters.
    pub fn execute_with_params(&self, program: &str, named: &BTreeMap<String, odm_types::Value>) -> Result<()> {
        self.require_vos_executor()?;
        let named = value_params_to_engine(named)?;
        self.conn.borrow().execute_vos_write_with_params(program, &named)?;
        Ok(())
    }

    /// Run a VOS program with typed named parameters, classifying it as a
    /// write or a read before execution: write programs (`Type::insert`,
    /// `Type { … }.insert()`, zero-argument schema macros) go to the write
    /// executor and read pipelines go to the query executor. Writes return
    /// no rows.
    ///
    /// Classification happens on the parsed program shape (see
    /// [`is_write_program`]): the read executor silently accepts no writes,
    /// and `.patch()` / `.delete()` attempts keep surfacing the Phase 1
    /// diagnostics of the read path instead of being misrouted.
    pub fn run_with_params(&self, program: &str, named: &BTreeMap<String, odm_types::Value>) -> Result<Vec<odm_types::Row>> {
        self.require_vos_executor()?;
        let named = value_params_to_engine(named)?;
        let conn = self.conn.borrow();
        if is_write_program(program) {
            conn.execute_vos_write_with_params(program, &named)?;
            return Ok(Vec::new());
        }
        let rows = conn.query_vos_with_params(program, &named)?;
        rows.into_iter().map(row_from_engine).collect()
    }

    /// Prepare a VOS program into a cached plan.
    ///
    /// The plan is bound to the current DDL revision — re-prepare when the
    /// schema changes.
    pub fn prepare(&self, program: &str) -> Result<PreparedVos> {
        self.require_vos_executor()?;
        let handshake = self.schema_handshake()?;
        let stmt = self.conn.borrow().prepare_vos(program)?;
        Ok(PreparedVos { program: program.to_owned(), ddl_revision: handshake.ddl_revision, stmt })
    }

    /// Begin a data transaction.
    pub fn begin(&self) -> Result<()> {
        self.require_vos_executor()?;
        self.conn.borrow().begin().map_err(Error::from)
    }

    /// Commit the open data transaction.
    pub fn commit(&self) -> Result<()> {
        self.require_vos_executor()?;
        self.conn.borrow().commit().map_err(Error::from)
    }

    /// Roll back the open data transaction.
    pub fn rollback(&self) -> Result<()> {
        self.require_vos_executor()?;
        self.conn.borrow().rollback().map_err(Error::from)
    }

    /// Whether a data transaction is open.
    pub fn in_transaction(&self) -> bool {
        self.conn.borrow().in_transaction()
    }

    /// Reject use when the database DDL revision drifted since session open.
    pub fn check_session_ddl_revision(&self, expected: u64) -> Result<()> {
        self.require_vos_executor()?;
        let handshake = self.schema_handshake()?;
        if handshake.ddl_revision != expected {
            return Err(Error::Policy(format!(
                "{SESSION_STALE_CODE}: session opened at ddl revision {} but database is at {}",
                expected, handshake.ddl_revision
            )));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Converged adapter surface (shared shape with `iris-adapter-mysql`)
    // -----------------------------------------------------------------------

    /// Execute a planned read pipeline or insert on a fresh checkout.
    pub fn execute_plan(&self, plan: &PhysicalPlan) -> Result<Vec<odm_types::Row>> {
        let mut conn = self.conn.borrow_mut();
        self.execute_plan_on(&mut conn, plan)
    }

    /// Execute a plan on an existing connection (same session / transaction).
    pub fn execute_plan_on(&self, conn: &mut Connection, plan: &PhysicalPlan) -> Result<Vec<odm_types::Row>> {
        self.require_vos_executor()?;
        if plan.is_rejected() {
            return Err(Error::Policy(plan.rejection_note().unwrap_or("plan rejected").to_string()));
        }
        let rendered = plan::render_plan(plan)?;
        let named = value_params_to_engine(&rendered.params)?;
        if rendered.is_write {
            conn.execute_vos_write_with_params(&rendered.source, &named)?;
            return Ok(Vec::new());
        }
        let rows = conn.query_vos_with_params(&rendered.source, &named)?;
        rows.into_iter().map(row_from_engine).collect()
    }

    /// Insert a full row on a fresh checkout.
    pub fn insert(&self, write: &RowWrite) -> Result<()> {
        let mut conn = self.conn.borrow_mut();
        self.insert_on(&mut conn, write)
    }

    /// Insert on an existing connection (same session / transaction).
    pub fn insert_on(&self, conn: &mut Connection, write: &RowWrite) -> Result<()> {
        self.require_vos_executor()?;
        let rendered = plan::render_row_write(write)?;
        let named = value_params_to_engine(&rendered.params)?;
        conn.execute_vos_write_with_params(&rendered.source, &named)?;
        Ok(())
    }

    /// YYDB VOS writes are insert-only — update fails closed.
    pub fn update(&self, write: &RowWrite) -> Result<u64> {
        let _ = write;
        Err(Self::insert_only_write("update"))
    }

    /// YYDB VOS writes are insert-only — update fails closed.
    pub fn update_on(&self, conn: &mut Connection, write: &RowWrite) -> Result<u64> {
        let _ = (conn, write);
        Err(Self::insert_only_write("update"))
    }

    /// YYDB VOS writes are insert-only — delete fails closed.
    pub fn delete(&self, table: &str, primary_key: &str, key: &odm_types::Value) -> Result<u64> {
        let _ = (table, primary_key, key);
        Err(Self::insert_only_write("delete"))
    }

    /// YYDB VOS writes are insert-only — delete fails closed.
    pub fn delete_on(&self, conn: &mut Connection, table: &str, primary_key: &str, key: &odm_types::Value) -> Result<u64> {
        let _ = (conn, table, primary_key, key);
        Err(Self::insert_only_write("delete"))
    }

    /// Run `f` inside a transaction, committing on success and rolling back on error.
    ///
    /// The connection is checked out for the closure — calling the `&self`
    /// facades (`query` / `execute` / `execute_plan`) inside `f` panics on the
    /// borrow; use the `*_on` helpers with the handed-in `conn`.
    pub fn transaction<R>(&self, f: impl FnOnce(&mut Connection) -> Result<R>) -> Result<R> {
        self.require_vos_executor()?;
        let mut conn = self.conn.borrow_mut();
        conn.begin()?;
        match f(&mut conn) {
            Ok(value) => match conn.commit() {
                Ok(()) => Ok(value),
                Err(error) => {
                    let _ = conn.rollback();
                    Err(Error::from(error))
                }
            },
            Err(error) => {
                let _ = conn.rollback();
                Err(error)
            }
        }
    }

    /// Always roll back — shared-database integration tests leave no residue.
    pub fn with_rollback<R>(&self, f: impl FnOnce(&mut Connection) -> Result<R>) -> Result<R> {
        self.require_vos_executor()?;
        let mut conn = self.conn.borrow_mut();
        conn.begin()?;
        let result = f(&mut conn);
        let _ = conn.rollback();
        result
    }

    fn insert_only_write(operation: &str) -> Error {
        Error::Policy(format!("YYDB VOS writes are insert-only; `{operation}` has no native form on this connector"))
    }

    /// Re-open the same file path (drop + open). In-memory sources error.
    pub fn reopen(self) -> Result<Self> {
        let Some(path) = self.path.clone()
        else {
            return Err(Error::Runtime("in-memory YYDB cannot reopen by path".into()));
        };
        drop(self);
        Self::open(path)
    }
}

// ---------------------------------------------------------------------------
// PreparedVos — cached VOS query plan
// ---------------------------------------------------------------------------

/// Prepared VOS plan pinned to a DDL revision.
#[derive(Debug)]
pub struct PreparedVos {
    #[allow(dead_code)]
    program: String,
    ddl_revision: u64,
    stmt: PreparedStatement,
}

impl PreparedVos {
    /// DDL revision at prepare time.
    pub fn ddl_revision(&self) -> u64 {
        self.ddl_revision
    }

    /// The underlying YYDB prepared statement — escape hatch for advanced use.
    pub fn statement(&self) -> &PreparedStatement {
        &self.stmt
    }

    /// Execute with no named parameters (read-only projection).
    pub fn execute(&self, source: &LocalSource) -> Result<Vec<odm_types::Row>> {
        self.execute_with_params(source, BTreeMap::new())
    }

    /// Execute with named parameter bindings.
    ///
    /// Returns `Err` when the result set contains a YYDB variant Iris cannot
    /// faithfully represent — currently only the CAS `Object` handle.
    pub fn execute_with_params(
        &self,
        source: &LocalSource,
        named: BTreeMap<String, database_execution::engine::Value>,
    ) -> Result<Vec<odm_types::Row>> {
        source.require_vos_executor()?;
        let handshake = source.schema_handshake()?;
        if handshake.ddl_revision != self.ddl_revision {
            return Err(Error::Policy(format!(
                "{PREPARED_STALE_CODE}: prepared at ddl revision {} but database is at {}",
                self.ddl_revision, handshake.ddl_revision
            )));
        }
        let rows = source.connection().query_prepared(&self.stmt, &named)?;
        rows.into_iter().map(row_from_engine).collect()
    }
}

// ---------------------------------------------------------------------------
// Row / value conversion — yydb types → iris_types
// ---------------------------------------------------------------------------

/// Whether a VOS program is a write the Phase 1 write executor accepts.
///
/// The classifier parses the program shape instead of guessing from text:
/// `$name` parameters are replaced by `null` only for parsing (typed payload
/// placeholders have no VOS literal form), then the top-level expression
/// decides. `Type::insert({ … })` (associated form), `Type { … }.insert()`
/// (method form) and zero-argument schema macro calls are writes. Everything
/// else — read pipelines, `.patch()` / `.delete()` attempts, malformed
/// sources — stays a read so the canonical Phase 1 diagnostics of the read
/// path surface for it.
fn is_write_program(source: &str) -> bool {
    let Ok(program) = vos::parse_program(&probe_source(source))
    else {
        return false;
    };
    // `let` bindings are outside the shapes the classifier owns; keep them on
    // the read path.
    if !program.statements.is_empty() {
        return false;
    }
    let Some(result) = program.result.as_ref()
    else {
        return false;
    };
    is_write_expr(result)
}

fn is_write_expr(expr: &vos::syntax::VosExpr) -> bool {
    use vos::syntax::VosExpr;
    let VosExpr::Call { callee, args, .. } = expr
    else {
        return false;
    };
    match callee.as_ref() {
        // `Type::insert({ … })` and `Type { … }.insert()` — both insert shapes
        // the Phase 1 write executor accepts. Read pipelines end in a `collect`
        // call instead, so they never reach this arm.
        VosExpr::Member { name, .. } if name == "insert" => true,
        // Zero-argument schema macro call (e.g. `seed_fixture()`); a call with
        // arguments is a declared query operation and stays a read.
        VosExpr::Name { .. } => args.is_empty(),
        _ => false,
    }
}

/// Replace each `$ident` parameter placeholder outside string literals with a
/// `null` literal so the classifier can parse the program. Only used for
/// shape detection — never executed.
fn probe_source(source: &str) -> String {
    let chars: Vec<char> = source.chars().collect();
    let mut out = String::with_capacity(source.len());
    let mut i = 0;
    let mut in_string = false;
    while i < chars.len() {
        let ch = chars[i];
        if in_string {
            out.push(ch);
            if ch == '\\' && i + 1 < chars.len() {
                out.push(chars[i + 1]);
                i += 2;
                continue;
            }
            if ch == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if ch == '"' {
            in_string = true;
            out.push('"');
            i += 1;
            continue;
        }
        if ch == '$' {
            let start = i + 1;
            let mut end = start;
            while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
                end += 1;
            }
            if end > start {
                out.push_str("null");
                i = end;
                continue;
            }
        }
        out.push(ch);
        i += 1;
    }
    out
}

/// Convert a typed Iris `FileDescriptor` into the backend file descriptor.
///
/// The manifest digest crosses as raw 32 bytes; a hex spelling that is not
/// canonical fails the bind instead of writing a corrupt identity.
fn descriptor_to_engine(f: &FileDescriptor) -> Result<database_execution::engine::FileDescriptor> {
    let hash = odm_types::hex_to_bytes(&f.manifest.hash)
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        .ok_or_else(|| Error::Runtime("file descriptor manifest hash is not 64 lowercase hex digits".into()))?;
    Ok(database_execution::engine::FileDescriptor {
        digest: f.digest.clone(),
        octets: f.octets,
        name: f.name.clone(),
        media_type: f.media_type.clone(),
        manifest: database_execution::engine::FileManifestRef { hash, size: f.manifest.size },
    })
}

/// Convert a backend file descriptor into the typed Iris `FileDescriptor`.
fn descriptor_from_yydb(file: database_execution::engine::FileDescriptor) -> Result<FileDescriptor> {
    FileDescriptor::new(
        file.digest,
        file.octets,
        file.name,
        file.media_type,
        FileManifestRef { hash: file.manifest.hash_hex(), size: file.manifest.size },
    )
    .ok_or_else(|| Error::Runtime("YYDB returned a file descriptor with a non-canonical digest or blank label".into()))
}

/// Convert Iris typed parameter values into the YYDB binding map.
///
/// `bytes` stays octets, `vector` binds the native `database_execution::engine::Vector` and `file`
/// binds the native `database_execution::engine::FileDescriptor` — every VOS payload keeps its typed
/// variant end to end, never text. Nested objects are rejected: YYDB binds a
/// scalar per `$name` slot and has no text form for one.
pub fn value_params_to_engine(named: &BTreeMap<String, odm_types::Value>) -> Result<BTreeMap<String, EngineValue>> {
    let mut out = BTreeMap::new();
    for (name, value) in named {
        let converted = match value {
            odm_types::Value::Null => EngineValue::Null,
            odm_types::Value::Bool(b) => EngineValue::Bool(*b),
            odm_types::Value::Int(i) => EngineValue::I64(*i),
            odm_types::Value::UInt(u) => EngineValue::U64(*u),
            odm_types::Value::Float(f) => {
                if !f.is_finite() {
                    return Err(Error::Runtime(format!("typed parameter `${name}` is a non-finite VOS `f64`")));
                }
                EngineValue::F64(*f)
            }
            odm_types::Value::Str(s) => EngineValue::Text(s.clone()),
            odm_types::Value::Decimal(s) | odm_types::Value::Date(s) | odm_types::Value::Time(s) | odm_types::Value::DateTime(s) => {
                EngineValue::Text(s.clone())
            }
            odm_types::Value::Bytes(octets) => EngineValue::Bytes(octets.clone()),
            odm_types::Value::Vector(v) => {
                let components: Vec<f32> = v.components.clone();
                EngineValue::Vector(
                    database_execution::engine::Vector::new(components)
                        .map_err(|e| Error::Runtime(format!("typed parameter `${name}` is not a bindable vector: {e}")))?,
                )
            }
            odm_types::Value::File(f) => EngineValue::File(descriptor_to_engine(f)?),
            odm_types::Value::Object(_) => {
                return Err(Error::Runtime(format!("typed parameter `${name}` is an object, not a scalar VOS value")));
            }
        };
        out.insert(name.clone(), converted);
    }
    Ok(out)
}

/// Convert a YYDB runtime Value to Iris Value.
///
/// `bytes` keeps its octets: YYDB `Value::Bytes` becomes Iris
/// `Value::Bytes`, never text and never `Null`. Date / time / decimal scalars
/// round-trip through their canonical ISO text form, matching the reverse
/// mapping in [`value_params_to_engine`].
///
/// `vector` and `file` payloads land in their typed Iris variants
/// (`Value::Vector` / `Value::File`), preserving dimension, components and
/// the full descriptor (digest, octets, metadata, manifest) exactly as the
/// backend produced them.
///
/// Returns `Err` for variants Iris does not model — the CAS `Object`
/// handle — so callers cannot silently read a column as `Null`. Code that
/// needs full fidelity should work directly with the YYDB `database_execution::engine::Value` via
/// the `connection()` escape hatch.
pub fn value_from_engine(v: EngineValue) -> Result<odm_types::Value> {
    match v {
        EngineValue::Null => Ok(odm_types::Value::Null),
        EngineValue::Bool(b) => Ok(odm_types::Value::Bool(b)),
        EngineValue::Text(s) => Ok(odm_types::Value::Str(s)),
        EngineValue::I64(i) => Ok(odm_types::Value::Int(i)),
        EngineValue::U64(u) => Ok(odm_types::Value::UInt(u)),
        EngineValue::F64(f) => {
            if f.is_finite() {
                Ok(odm_types::Value::Float(f))
            }
            else {
                Err(Error::Runtime("YYDB returned a non-finite VOS `f64`".into()))
            }
        }
        EngineValue::Bytes(octets) => Ok(odm_types::Value::Bytes(octets)),
        EngineValue::Uuid(u) => Ok(odm_types::Value::Str(u.to_string())),
        EngineValue::Date(d) => Ok(odm_types::Value::Date(d.to_iso())),
        EngineValue::Time(t) => Ok(odm_types::Value::Time(t.to_iso())),
        EngineValue::Timestamp(ts) => Ok(odm_types::Value::DateTime(ts.to_iso())),
        EngineValue::Decimal(d) => Ok(odm_types::Value::Decimal(d.to_string())),
        EngineValue::Row(nested) => {
            let mut out = BTreeMap::new();
            for (k, v) in nested {
                out.insert(k, value_from_engine(v)?);
            }
            Ok(odm_types::Value::Object(out))
        }
        EngineValue::Vector(vec) => VectorValue::new(vec.dim, vec.data.to_vec())
            .map(odm_types::Value::Vector)
            .ok_or_else(|| Error::Runtime(format!("YYDB returned a `vector` with dimension {} outside the VOS contract", vec.dim))),
        EngineValue::Object(_) => Err(Error::Runtime(
            "YYDB returned a CAS `object` handle which Iris does not model. \
             Use the `connection()` escape hatch for full fidelity access."
                .into(),
        )),
        EngineValue::File(file) => descriptor_from_yydb(file).map(odm_types::Value::File),
        #[allow(unreachable_patterns)]
        other => Err(Error::Runtime(format!(
            "YYDB returned an unrecognised `Value` variant that Iris cannot represent: {other:?}. \
             This usually means the pinned `yydb` dependency added a new variant; \
             upgrade `iris-connector-yydb` or use the `connection()` escape hatch."
        ))),
    }
}

/// Convert a YYDB query row to an Iris row.
///
/// Returns `Err` when any cell contains a YYDB variant Iris cannot faithfully
/// represent — currently only the CAS `Object` handle.
pub fn row_from_engine(row: BTreeMap<String, EngineValue>) -> Result<odm_types::Row> {
    let mut out = BTreeMap::new();
    for (k, v) in row {
        out.insert(k, value_from_engine(v)?);
    }
    Ok(out)
}
