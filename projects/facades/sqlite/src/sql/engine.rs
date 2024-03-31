//! In-memory execution layer: SQL subset over the pure-Rust format-3 writer.
//!
//! The engine keeps the whole database image in `SqliteWriter::pages()`. It
//! never imports an upstream SQLite engine; [`Engine::from_writer`] wraps an
//! image read by `SqliteWriter::open`, and [`Engine::save`] flushes the current
//! image back to a file.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
};

use database_udf::{Budget as UdfBudget, HostMicroDefinition, HostRuntimeAdapter, UdfContext, UdfImplementation};

use crate::{
    Affinity, Collation, ColumnSpec, DropIndexOutcome, RecordLimits, SchemaObjectKind, SqliteWriter, TableLimits, TableRow, TextEncoding,
    UpdateValue, affinity_of, decode_library_version, decode_record, decode_varint, read_index_table_with, read_table, validate_database,
};

use super::{
    datetime::{self, DateTimeFunction},
    error::SqlError,
    host::{cell_to_udf_value, udf_error_to_sql_error, udf_value_to_cell, HostFunctionTable},
    json_string,
    storage_failure,
    sql::{
        AggregateFunction, AlterAction, Assignment, BinaryOp, CastType, CheckConstraint, ColumnRef, CommonTable, CompareOp, CompoundOp,
        CompoundTerm, ConflictAction, ConflictClause, DropColumnConflict, Expr, ForeignKey, ForeignKeyAction, FrameBound, FrameEdge,
        FrameExclude, FrameUnits, IndexColumn, InsertValue, JoinKind, Limit, Literal, NamedWindow, OrderKey, ScalarFunction, SelectCore,
        SelectItems, Statement, TableConstraint, TableRef, TransactionAction, TriggerEvent, TriggerTiming, UnaryOp, WindowFunction, WindowSpec,
        core_references, parse, parse_with_host_functions, rewrite_create_index_column, rewrite_create_index_table, rewrite_create_table_column,
        rewrite_create_table_drop_column, rewrite_create_table_name, rewrite_view_column, rewrite_view_table, statement_references,
        strip_schema_qualifiers,
    },
};

/// One cell surfaced to the host.
#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    /// SQL `NULL`.
    Null,
    /// Signed integer.
    Integer(i64),
    /// Binary64 value.
    Real(f64),
    /// UTF-8 text.
    Text(String),
    /// Blob bytes.
    Blob(Vec<u8>),
}

/// Result of one executed statement.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecResult {
    /// Lowercase statement name (`create-table`, `create-index`, `insert`, `select`).
    pub statement: &'static str,
    /// Column labels in result order.
    pub columns: Vec<String>,
    /// Result rows; empty for DDL and inserts.
    pub rows: Vec<Vec<Cell>>,
    /// Rows created by an insert, or 1 for DDL.
    pub affected: usize,
}

impl ExecResult {
    /// Serializes one result as the documented JSON envelope.
    pub fn to_json(&self) -> String {
        let mut out = String::from("{\"statement\":");
        out.push_str(&json_string(self.statement));
        out.push_str(",\"affected\":");
        out.push_str(&self.affected.to_string());
        out.push_str(",\"columns\":[");
        for (index, column) in self.columns.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(&json_string(column));
        }
        out.push_str("],\"rows\":[");
        for (row_index, row) in self.rows.iter().enumerate() {
            if row_index > 0 {
                out.push(',');
            }
            out.push('[');
            for (cell_index, cell) in row.iter().enumerate() {
                if cell_index > 0 {
                    out.push(',');
                }
                out.push_str(&cell_json(cell));
            }
            out.push(']');
        }
        out.push_str("]}");
        out
    }
}

/// One table-level `PRIMARY KEY` or `UNIQUE` constraint resolved against the
/// declared columns, in written order.
#[derive(Clone)]
struct TableConstraintDef {
    /// `true` for `PRIMARY KEY`, `false` for `UNIQUE`.
    primary: bool,
    /// Key columns in key order.
    columns: Vec<usize>,
}

#[derive(Clone)]
struct TableDef {
    /// Name exactly as written in `CREATE TABLE`.
    name: String,
    columns: Vec<ColumnSpec>,
    rowid_alias: Option<usize>,
    /// A table-level `PRIMARY KEY (a, b)` clause created an automatic unique
    /// index that has to be materialized after inserts.
    primary_key_index: bool,
    /// Column positions of a table-level `PRIMARY KEY (a, b)`, in key order.
    /// Empty for a rowid alias or a table without a table-level key.
    primary_key: Vec<usize>,
    /// Table-level `PRIMARY KEY` / `UNIQUE` clauses in written order, so
    /// automatic index numbers match `sqlite_autoindex_<table>_<n>`.
    table_constraints: Vec<TableConstraintDef>,
    /// `CHECK` constraints, table- and column-level, in declaration order.
    checks: Vec<CheckDef>,
    /// `REFERENCES` clauses, table- and column-level, in declaration order.
    foreign_keys: Vec<ForeignKeyDef>,
    /// `STRICT` table option was present, so every non-NULL value has to
    /// conform to its column's declared type after affinity.
    strict: bool,
    /// The table is stored as a primary-key index b-tree and has no implicit
    /// `rowid`.
    without_rowid: bool,
    /// The table belongs to the temp schema and is not stored in the image.
    temp: bool,
}

/// One `CREATE TEMP TABLE` and its in-memory rows.
struct TempTable {
    def: TableDef,
    /// `sqlite_temp_master.sql` text: the statement without `TEMP`, exactly
    /// like SQLite stores it.
    sql: String,
    /// Root page of the temp b-tree, allocated the same way SQLite's temp
    /// database allocates pages.
    rootpage: u32,
    /// `sqlite_temp_master.rowid`, a monotonic insertion counter.
    master_rowid: i64,
    /// Declared column values keyed by rowid, in ascending rowid order.
    rows: BTreeMap<i64, Vec<Cell>>,
}

/// One resolved `REFERENCES` clause of a table.
#[derive(Clone)]
struct ForeignKeyDef {
    /// Child column positions in key order.
    columns: Vec<usize>,
    /// Lowercase parent table name.
    parent: String,
    /// Parent table name as written for diagnostics.
    parent_display: String,
    /// Parent column names as written; empty means the parent primary key.
    parent_columns: Vec<String>,
    /// `ON DELETE` action, spelled for `PRAGMA foreign_key_list`.
    on_delete: String,
    /// `ON UPDATE` action, spelled for `PRAGMA foreign_key_list`.
    on_update: String,
    /// `ON DELETE` action as parsed, applied when parent rows are removed.
    on_delete_action: ForeignKeyAction,
    /// `ON UPDATE` action as parsed, applied when parent keys are rewritten.
    on_update_action: ForeignKeyAction,
    /// `DEFERRABLE INITIALLY DEFERRED`: verification waits for `COMMIT` while
    /// an explicit transaction is open.
    deferred: bool,
}

impl TableDef {
    /// Column positions of this table's primary key, or `None` when it has no
    /// primary key at all.
    fn primary_key_positions(&self) -> Option<Vec<usize>> {
        if let Some(alias) = self.rowid_alias {
            return Some(vec![alias]);
        }
        if !self.primary_key.is_empty() {
            return Some(self.primary_key.clone());
        }
        let declared: Vec<usize> =
            self.columns.iter().enumerate().filter(|(_, column)| column.primary_key).map(|(position, _)| position).collect();
        if declared.is_empty() { None } else { Some(declared) }
    }

    /// Record layout of a WITHOUT ROWID table: stored record position to
    /// declared column position. SQLite stores the primary-key columns first,
    /// then the remaining columns in declaration order.
    fn without_rowid_layout(&self) -> Vec<usize> {
        let mut layout = self.primary_key_positions().unwrap_or_default();
        for position in 0..self.columns.len() {
            if !layout.contains(&position) {
                layout.push(position);
            }
        }
        layout
    }

    /// Decodes one WITHOUT ROWID record and maps it back to declaration order.
    fn decode_without_rowid(&self, payload: &[u8]) -> Result<Vec<Cell>, SqlError> {
        let layout = self.without_rowid_layout();
        let mut values = decode_cells(payload)?;
        // A record written before `ALTER TABLE ADD COLUMN` is short; the
        // missing trailing record slots read as their declared defaults.
        while values.len() < layout.len() {
            let position = layout[values.len()];
            let column = &self.columns[position];
            let value = column.default_value.clone().unwrap_or(crate::WriteValue::Null);
            values.push(write_value_cell(&crate::apply_write_affinity(&column.declared_type, value)));
        }
        values.truncate(layout.len());
        let mut cells = vec![Cell::Null; self.columns.len()];
        for (record_position, declared_position) in layout.iter().enumerate() {
            cells[*declared_position] = values[record_position].clone();
        }
        Ok(cells)
    }
}

#[derive(Clone)]
struct CheckDef {
    /// `CONSTRAINT name CHECK (...)` name, echoed instead of the expression.
    name: Option<String>,
    /// Expression over the table's declared columns.
    expr: ResolvedExpr,
    /// Source text of the expression for the failure message.
    text: String,
}

/// One index in the engine catalog, keyed by its lowercase name.
#[derive(Clone)]
struct IndexDef {
    /// Index name as written, used by `PRAGMA index_list`.
    name: String,
    /// Lowercase name of the indexed table.
    table: String,
    /// Indexed column positions, in key order. Empty for a managed index
    /// whose keys are expression results or filtered by a predicate.
    columns: Vec<usize>,
    /// The index enforces uniqueness.
    unique: bool,
    /// Creation counter, so `PRAGMA index_list` reports newest first.
    order: u64,
    /// The keys are computed by the engine (expression, `COLLATE` or partial
    /// index), not derived from column positions.
    managed: bool,
    /// Key terms resolved against the table, evaluated per row.
    terms: Vec<ResolvedExpr>,
    /// Partial-index predicate resolved against the table.
    predicate: Option<ResolvedExpr>,
    /// 每个键项生效的排序规则，来自索引项显式 `COLLATE` 或列声明。
    collations: Vec<Collation>,
}

/// How one base-table scan is bounded before any row is decoded.
///
/// Every variant keeps the emitted row set and row order identical to the
/// unbounded read: the full predicate still runs downstream, and a bounded
/// read only narrows the candidate rows the scan materializes.
enum ScanHint {
    /// `WHERE <rowid alias> = <integer literal>`: one candidate rowid.
    Pin(i64),
    /// `WHERE <indexed column> = <literal>` covering every key column of a
    /// plain column index: the rowids the index probe found, ascending.
    Rowids(Vec<i64>),
    /// `ORDER BY <rowid order> LIMIT n` with no other row-reducing clause:
    /// only the first `n` rows in rowid order can reach the output.
    Head(usize),
}

/// Canonical equality signature of one index or group key value.
///
/// Two cells share a signature exactly when `compare_cells_with` reports them
/// equal, so a hash bucket built from signatures never splits rows the linear
/// scan would have grouped, and never merges rows it would have kept apart.
/// Values whose equality the canonical form cannot reproduce (a binary64 NaN,
/// which the engine compares equal to every numeric, or an integer outside the
/// exact binary64 range) yield `None` and push the caller back to the linear
/// path.
#[derive(Clone, PartialEq, Eq, Hash)]
enum ProbeKey {
    /// SQL `NULL`.
    Null,
    /// A numeric value canonicalized to its binary64 bits, with every signed
    /// zero folded onto positive zero (the engine compares them equal).
    Number(u64),
    /// An integer binary64 cannot represent exactly.
    Wide(i64),
    /// Text folded for its collation, so collation equality is byte equality.
    Text(Box<[u8]>),
    /// Blob bytes.
    Blob(Box<[u8]>),
}

/// One key position's canonical signature, or `None` when the value is a NaN.
fn probe_key(cell: &Cell, collation: Collation) -> Option<ProbeKey> {
    match cell {
        Cell::Null => Some(ProbeKey::Null),
        Cell::Integer(value) => Some(integer_probe_key(*value)),
        Cell::Real(value) => real_probe_key(*value),
        Cell::Text(text) => Some(ProbeKey::Text(fold_text(text, collation).into_boxed_slice())),
        Cell::Blob(bytes) => Some(ProbeKey::Blob(bytes.clone().into_boxed_slice())),
    }
}

/// Signature of an integer: exact binary64 values fold onto the numeric bits,
/// everything else keeps the full `i64`.
fn integer_probe_key(value: i64) -> ProbeKey {
    let real = value as f64;
    if real.abs() < 9_223_372_036_854_775_808.0 && (real as i64) == value {
        ProbeKey::Number(real.to_bits())
    }
    else {
        ProbeKey::Wide(value)
    }
}

/// Signature of a real; `None` for a NaN, which the engine compares equal to
/// every numeric value and therefore has no hashable class.
fn real_probe_key(value: f64) -> Option<ProbeKey> {
    if value.is_nan() { None } else if value == 0.0 { Some(ProbeKey::Number(0.0f64.to_bits())) } else { Some(ProbeKey::Number(value.to_bits())) }
}

/// Text folded for its collation: `NOCASE` lowercases ASCII bytes and `RTRIM`
/// drops trailing spaces, which is exactly what their comparisons ignore.
fn fold_text(text: &str, collation: Collation) -> Vec<u8> {
    match collation {
        Collation::Binary => text.as_bytes().to_vec(),
        Collation::Nocase => text.as_bytes().iter().map(u8::to_ascii_lowercase).collect(),
        Collation::Rtrim => text.trim_end_matches(' ').as_bytes().to_vec(),
    }
}

/// One inclusive/exclusive range bound on an indexed column.
struct IndexBound {
    /// Literal the stored key is compared against.
    cell: Cell,
    /// Whether a key equal to `cell` is inside the range.
    inclusive: bool,
}

/// Cached live key set of one plain column index, valid while `epoch` matches
/// the engine's write epoch.
struct IndexProbe {
    /// Write epoch the key set was read under.
    epoch: u64,
    /// Rowids sharing a canonical key, ascending.
    map: HashMap<Vec<ProbeKey>, Vec<i64>>,
    /// Decoded key values in index order, used by range probes that need the
    /// engine's own comparison instead of a hash lookup.
    entries: Vec<(Vec<Cell>, i64)>,
    /// `entries` 按首列排序规则非递减,范围探针因此可以二分定位。
    sorted: bool,
    /// 已从表中删除、但还没从 `entries` 里剔除的 rowid。
    ///
    /// 删除语句按行打补丁时只把 rowid 记到这里,避免每条删除都扫一遍整份
    /// 索引项;条目累积到一半时再一次性压缩。
    deleted: HashSet<i64>,
    /// A stored key the canonical form cannot reproduce; the probe is then
    /// never used and every query falls back to the full scan.
    poisoned: bool,
}

/// Memoized result of one uncorrelated subquery, valid for a single statement.
enum SubqueryCache {
    /// Rows of a scalar subquery or `EXISTS`, in evaluation order.
    Rows(Vec<Vec<Cell>>),
    /// Value set of `IN (SELECT ...)`; `keys` is `None` when a value has no
    /// canonical signature, so membership falls back to the linear scan.
    InList { values: Vec<Cell>, keys: Option<HashSet<ProbeKey>>, saw_null: bool },
}

/// One view in the engine catalog, keyed by its lowercase name.
#[derive(Clone)]
struct ViewDef {
    /// View name as written, used for diagnostics.
    name: String,
    /// Optional `CREATE VIEW v(a, b)` list that renames the query output.
    columns: Option<Vec<String>>,
    /// Parsed query the view re-evaluates on every read.
    query: Statement,
}

/// One `CREATE TEMP VIEW` in the temp schema, kept beside the temp tables so
/// the connection owns it and `sqlite_temp_master` reports it.
struct TempView {
    def: ViewDef,
    /// `sqlite_temp_master.sql` text, the statement without `TEMP`.
    sql: String,
    /// `sqlite_temp_master.rowid`, a monotonic insertion counter.
    master_rowid: i64,
}

/// One trigger in the engine catalog.
#[derive(Clone)]
struct TriggerDef {
    /// Trigger name as written, used for diagnostics.
    name: String,
    /// Lowercase table or view the trigger is attached to.
    table: String,
    /// `BEFORE`, `AFTER` or `INSTEAD OF`.
    timing: TriggerTiming,
    /// Statement event that fires the trigger.
    event: TriggerEvent,
    /// Optional `WHEN` predicate, evaluated against `NEW` / `OLD`.
    when: Option<Expr>,
    /// Body statements run in order.
    body: Vec<Statement>,
    /// Monotonic creation counter. SQLite fires the triggers of one event in
    /// reverse creation order, so the newest trigger runs first.
    order: u64,
    /// The trigger belongs to the temp schema and only appears in
    /// `sqlite_temp_master`.
    temp: bool,
    /// `sqlite_master.sql` / `sqlite_temp_master.sql` text.
    sql: String,
    /// `sqlite_temp_master.rowid`; zero for a main trigger.
    master_rowid: i64,
}

/// The `NEW` / `OLD` row one trigger body evaluates against.
struct TriggerFrame {
    /// Column names of the trigger's table, followed by `rowid`.
    columns: Vec<String>,
    /// Values of the row after the change; `None` for `DELETE`.
    new: Option<Vec<Cell>>,
    /// Values of the row before the change; `None` for `INSERT`.
    old: Option<Vec<Cell>>,
}

/// One row-level trigger event queued by a DML statement. Queueing instead of
/// firing inline keeps the statement's own writes and its trigger programs in
/// SQLite's row order while leaving the write path unchanged.
struct PendingTriggerEvent {
    /// Lowercase table the event belongs to.
    table: String,
    timing: TriggerTiming,
    event: TriggerEvent,
    columns: Vec<String>,
    new: Option<Vec<Cell>>,
    old: Option<Vec<Cell>>,
}

/// Depth limit SQLite applies before raising `too many levels of trigger
/// recursion` when `PRAGMA recursive_triggers` is on.
const MAX_TRIGGER_DEPTH: usize = 100;

/// 写语句候选 rowid 合并成区间读的最大缝隙。
///
/// 候选按 rowid 升序,缝隙不超过这个值的相邻候选交给同一次 `table_range`,用少量
/// 多余行换掉一次 b-tree 下降。
const CANDIDATE_GAP: i64 = 8;

/// Sentinel source indexes that mark a `NEW` / `OLD` reference in a resolved
/// expression. They never collide with a `FROM` source position.
const TRIGGER_SOURCE_NEW: usize = usize::MAX;
const TRIGGER_SOURCE_OLD: usize = usize::MAX - 1;
/// Pseudo source of `excluded.<column>` inside `ON CONFLICT DO UPDATE`.
const EXCLUDED_SOURCE: usize = usize::MAX - 2;

/// One database attached with `ATTACH DATABASE`.
struct AttachedDb {
    /// Schema name as written.
    name: String,
    /// File the database lives in; `None` for an in-memory or temporary one.
    file: Option<PathBuf>,
    /// Text `PRAGMA database_list` reports in the `file` column.
    display: String,
    /// The attached database runs through the same engine as `main`, so every
    /// statement shape keeps working inside it without a second parser.
    engine: Box<Engine>,
    /// The in-memory image differs from the file on disk.
    dirty: bool,
}

/// Which schema a statement resolves its unqualified names in. The default
/// follows SQLite's search order (temp, then main, then attached databases).
#[derive(Clone, Copy, PartialEq, Eq)]
enum TableScope {
    Default,
    Main,
    Temp,
}

/// Object family an unqualified `DROP` or `ALTER` statement may target in an
/// attached database.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TargetKind {
    Table,
    Index,
    View,
    Trigger,
}

/// Reserved table names of one schema. `main.` never reaches the temp master
/// and `temp.` never reaches the main master, matching SQLite's isolation.
fn system_for_scope(scope: TableScope, name: &str) -> Option<SystemTable> {
    let system = SystemTable::from_name(name)?;
    match (scope, system) {
        (TableScope::Main, SystemTable::TempMaster) => None,
        (TableScope::Temp, SystemTable::Master | SystemTable::Sequence) => None,
        _ => Some(system),
    }
}

/// The schema qualifier that decides where a statement runs.
fn leading_schema(statement: &Statement) -> Option<&str> {
    let primary = match statement {
        Statement::CreateTable { schema, .. }
        | Statement::CreateView { schema, .. }
        | Statement::CreateIndex { schema, .. }
        | Statement::DropTable { schema, .. }
        | Statement::DropIndex { schema, .. }
        | Statement::DropView { schema, .. }
        | Statement::DropTrigger { schema, .. }
        | Statement::AlterTable { schema, .. }
        | Statement::Insert { schema, .. }
        | Statement::Update { schema, .. }
        | Statement::Delete { schema, .. }
        | Statement::Pragma { schema, .. }
        | Statement::Vacuum { schema, .. }
        | Statement::CreateTrigger { schema, .. } => schema.as_deref(),
        _ => None,
    };
    if primary.is_some() {
        return primary;
    }
    match statement {
        Statement::CreateTrigger { table_schema, .. } => table_schema.as_deref(),
        _ => None,
    }
}

/// Drops the qualifier that matches `schema` from a statement so it can run
/// inside that schema.
fn clear_statement_schema(statement: &mut Statement, schema: &str) {
    fn clear(slot: &mut Option<String>, schema: &str) {
        if slot.as_deref().is_some_and(|value| value.eq_ignore_ascii_case(schema)) {
            *slot = None;
        }
    }
    match statement {
        Statement::CreateTable { schema: slot, .. }
        | Statement::CreateView { schema: slot, .. }
        | Statement::CreateIndex { schema: slot, .. }
        | Statement::DropTable { schema: slot, .. }
        | Statement::DropIndex { schema: slot, .. }
        | Statement::DropView { schema: slot, .. }
        | Statement::DropTrigger { schema: slot, .. }
        | Statement::AlterTable { schema: slot, .. }
        | Statement::Insert { schema: slot, .. }
        | Statement::Update { schema: slot, .. }
        | Statement::Delete { schema: slot, .. }
        | Statement::Pragma { schema: slot, .. }
        | Statement::Vacuum { schema: slot, .. } => clear(slot, schema),
        Statement::CreateTrigger { schema: slot, table_schema, .. } => {
            clear(slot, schema);
            clear(table_schema, schema);
        }
        _ => {}
    }
}

/// SQLite stores an attached database's objects without their schema
/// qualifier; the same rewrite keeps the stored text reloadable.
fn strip_statement_sql(statement: &mut Statement, schema: &str) {
    match statement {
        Statement::CreateTable { sql, .. }
        | Statement::CreateView { sql, .. }
        | Statement::CreateTrigger { sql, .. }
        | Statement::CreateIndex { sql, .. } => {
            *sql = strip_schema_qualifiers(sql, schema);
        }
        _ => {}
    }
}

/// Error for a schema name that is neither `main`, `temp`, nor attached.
fn unknown_schema_error(statement: &Statement, schema: &str) -> SqlError {
    match statement {
        Statement::Insert { table, .. } | Statement::Update { table, .. } | Statement::Delete { table, .. } => {
            SqlError::new("unknown-table", format!("no such table: {schema}.{table}")).in_statement(statement.name())
        }
        _ => SqlError::new("unknown-database", format!("unknown database {schema}")).in_statement(statement.name()),
    }
}

thread_local! {
    /// Views can be nested, but a view that reaches itself would recurse
    /// forever. SQLite rejects the circular definition at prepare time; this
    /// counter turns the same case into one structured error instead.
    static VIEW_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Decrements the view depth when the nested evaluation returns, including on
/// an error path.
struct ViewDepthGuard;

impl ViewDepthGuard {
    fn enter() -> Result<Self, SqlError> {
        VIEW_DEPTH.with(|depth| {
            depth.set(depth.get() + 1);
            if depth.get() > 8 {
                depth.set(depth.get() - 1);
                return Err(SqlError::new("view-depth", "view nesting depth exceeded"));
            }
            Ok(Self)
        })
    }
}

impl Drop for ViewDepthGuard {
    fn drop(&mut self) {
        VIEW_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// Statement label a trigger event reports in diagnostics.
fn event_statement(event: &TriggerEvent) -> &'static str {
    match event {
        TriggerEvent::Insert => "insert",
        TriggerEvent::Update(_) => "update",
        TriggerEvent::Delete => "delete",
    }
}

/// Column names a trigger frame exposes: the declared columns plus `rowid`.
fn trigger_frame_columns(def: &TableDef) -> Vec<String> {
    let mut columns: Vec<String> = def.columns.iter().map(|column| column.name.clone()).collect();
    columns.push("rowid".to_string());
    columns
}

/// Appends the rowid slot a trigger frame expects to one declared row.
fn frame_cells(cells: &[Cell], width: usize, rowid: i64) -> Vec<Cell> {
    let mut out = cells.to_vec();
    out.resize(width, Cell::Null);
    out.push(Cell::Integer(rowid));
    out
}

/// Output names of one view: the declared list when present, otherwise the
/// names the query produced. SQLite rejects a declared list whose width does
/// not match the query, and suffixes repeated names with `:1`, `:2`, ...
fn view_output_names(view: &ViewDef, actual: &[String]) -> Result<Vec<String>, SqlError> {
    let names = match &view.columns {
        Some(declared) => {
            if declared.len() != actual.len() {
                return Err(SqlError::new(
                    "view-arity",
                    format!("expected {} columns for '{}' but got {}", declared.len(), view.name, actual.len()),
                )
                .in_statement("select"));
            }
            declared.clone()
        }
        None => actual.to_vec(),
    };
    Ok(unique_output_names(&names))
}

/// Makes one result set's column names unique the way SQLite does for views,
/// derived tables and CTEs: a repeated name gets `:1`, `:2`, ... appended.
fn unique_output_names(names: &[String]) -> Vec<String> {
    let mut used = BTreeSet::new();
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let key = name.to_ascii_lowercase();
        if used.insert(key) {
            out.push(name.clone());
            continue;
        }
        let mut suffix = 1usize;
        loop {
            let candidate = format!("{name}:{suffix}");
            if used.insert(candidate.to_ascii_lowercase()) {
                out.push(candidate);
                break;
            }
            suffix += 1;
        }
    }
    out
}

/// SQLite's standard type name for one declared type: the affinity class the
/// type text resolves to, or an empty string for the BLOB affinity.
fn standard_affinity_name(declared: &str) -> String {
    let upper = declared.to_ascii_uppercase();
    let name = if upper.contains("INT") {
        "INT"
    }
    else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        "TEXT"
    }
    else if declared.is_empty() || upper.contains("BLOB") {
        ""
    }
    else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        "REAL"
    }
    else {
        "NUM"
    };
    name.to_string()
}

/// Resolves a `column` reference of a CTAS output item to the unique source
/// column that defines it.
fn ctas_lookup_column<'a>(reference: &ColumnRef, sources: &'a [(String, Vec<(String, String)>)]) -> Option<&'a (String, String)> {
    let qualifier = reference.qualifier.as_deref();
    let mut found = None;
    for (label, columns) in sources {
        if let Some(qualifier) = qualifier {
            if !label.eq_ignore_ascii_case(qualifier) {
                continue;
            }
        }
        for column in columns {
            if column.0.eq_ignore_ascii_case(&reference.name) {
                if found.is_some() {
                    return None;
                }
                found = Some(column);
            }
        }
    }
    found
}

/// Output label of one CTAS item: the source column's own spelling for a bare
/// reference, otherwise the expression's label.
fn ctas_expression_label(expression: &Expr, sources: &[(String, Vec<(String, String)>)]) -> String {
    if let Expr::Column(reference) = expression {
        if let Some(column) = ctas_lookup_column(reference, sources) {
            return column.0.clone();
        }
    }
    expression.label()
}

/// Declared type of one CTAS output item: a bare column reference copies its
/// source affinity, every other expression gets no declared type.
fn ctas_expression_type(expression: &Expr, sources: &[(String, Vec<(String, String)>)]) -> String {
    match expression {
        Expr::Alias { value, .. } => ctas_expression_type(value, sources),
        Expr::Column(reference) => ctas_lookup_column(reference, sources).map(|column| standard_affinity_name(&column.1)).unwrap_or_default(),
        _ => String::new(),
    }
}

/// Quotes a schema identifier when the generated `CREATE TABLE` text needs it.
fn ctas_quote_name(name: &str) -> String {
    let plain = !name.is_empty()
        && name.chars().next().is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && name.chars().all(|character| character.is_ascii_alphanumeric() || character == '_');
    if plain { name.to_string() } else { format!("\"{}\"", name.replace('"', "\"\"")) }
}

/// `sqlite_master.sql` text of a `CREATE TABLE ... AS SELECT` result, built the
/// same way SQLite generates it. SQLite switches to a multi-line layout for
/// long definitions; the compact form is emitted here because every reader
/// (including this parser) accepts it.
fn ctas_schema_sql(name: &str, columns: &[ColumnSpec]) -> String {
    let mut sql = format!("CREATE TABLE {}(", ctas_quote_name(name));
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            sql.push(',');
        }
        sql.push_str(&ctas_quote_name(&column.name));
        if !column.declared_type.is_empty() {
            sql.push(' ');
            sql.push_str(&column.declared_type);
        }
    }
    sql.push(')');
    sql
}

/// `INSERT ... SELECT` slot mapping: SQLite reports a different arity message
/// depending on whether an explicit column list was written.
fn select_arity(
    def: &TableDef,
    table: &str,
    columns: Option<&[String]>,
    values: &[crate::WriteValue],
) -> Result<Vec<Option<crate::WriteValue>>, SqlError> {
    match columns {
        Some(names) => {
            if values.len() != names.len() {
                return Err(
                    SqlError::new("arity-mismatch", format!("{} values for {} columns", values.len(), names.len())).in_statement("insert")
                );
            }
            self_contained_arity(def, table, Some(names), values)
        }
        None => {
            if values.len() != def.columns.len() {
                return Err(SqlError::new(
                    "arity-mismatch",
                    format!("table {table} has {} columns but {} values were supplied", def.columns.len(), values.len()),
                )
                .in_statement("insert"));
            }
            self_contained_arity(def, table, None, values)
        }
    }
}

/// Converts one select result cell into an `INSERT` value when the target is a
/// view handled by `INSTEAD OF` triggers.
fn cell_insert_value(cell: &Cell) -> InsertValue {
    InsertValue::Literal(match cell {
        Cell::Null => super::Literal::Null,
        Cell::Integer(value) => super::Literal::Integer(*value),
        Cell::Real(value) => super::Literal::Real(*value),
        Cell::Text(value) => super::Literal::Text(value.clone()),
        Cell::Blob(value) => super::Literal::Blob(value.clone()),
    })
}

/// A `FROM`-shape over one view's output, so view DML can resolve its filter
/// and assignments with the regular expression path.
fn view_relation<'a>(label: &str, names: &[String], trigger: Option<&'a TriggerFrame>) -> Relation<'a> {
    let width = names.len();
    Relation {
        sources: vec![Source {
            label: label.to_string(),
            columns: SourceColumns::Materialized { names: names.to_vec(), rowid: false },
            offset: 0,
            join: None,
            star_hidden: vec![false; width],
            unqualified_hidden: vec![false; width],
            star_override: vec![None; width],
        }],
        using: vec![Vec::new()],
        using_slots: Vec::new(),
        trigger,
        excluded: false,
    }
}

/// Evaluates an optional view-DML predicate against one output row.
fn row_matches(predicate: &Option<ResolvedExpr>, row: &[Cell], engine: &Engine, shape: &ScopeShape, budget: &Budget) -> Result<bool, SqlError> {
    let Some(predicate) = predicate
    else {
        return Ok(true);
    };
    let ctx = EvalContext { engine, outer: &[], current: shape, aggregates: &[], windows: &[], window_row: 0, budget, ctes: None };
    Ok(truthiness(&predicate.eval(row, &ctx)?) == Some(true))
}

/// Whether a definition's event covers an incoming statement event.
fn trigger_event_matches(defined: &TriggerEvent, incoming: &TriggerEvent) -> bool {
    match (defined, incoming) {
        (TriggerEvent::Insert, TriggerEvent::Insert) | (TriggerEvent::Delete, TriggerEvent::Delete) => true,
        (TriggerEvent::Update(defined), TriggerEvent::Update(assigned)) => {
            defined.is_empty() || defined.iter().any(|column| assigned.iter().any(|assigned| assigned.eq_ignore_ascii_case(column)))
        }
        _ => false,
    }
}

/// One uniqueness rule an `UPDATE` or an `ON CONFLICT` target re-checks
/// against the live table.
#[derive(Clone)]
enum UniqueKey {
    /// The hidden rowid, reported under `t.rowid` or `t.<alias>`.
    Rowid(String),
    /// One or more column positions, plus the `t.a, t.b` message fragment and
    /// the collation each key position compares with.
    Columns(Vec<usize>, String, Vec<Collation>),
}

impl UniqueKey {
    /// The key values of one row; `cells` holds the declared columns only.
    fn values(&self, cells: &[Cell], rowid: i64) -> Vec<Cell> {
        match self {
            Self::Rowid(_) => vec![Cell::Integer(rowid)],
            Self::Columns(positions, _, _) => positions.iter().map(|position| cells[*position].clone()).collect(),
        }
    }

    fn label(&self) -> &str {
        match self {
            Self::Rowid(label) | Self::Columns(_, label, _) => label,
        }
    }
}

/// Everything an `UPDATE` will write, computed before storage is touched so a
/// constraint failure can never leave a half-updated table.
struct UpdatePlan {
    /// Current rowid plus one slot per column, in ascending rowid order.
    writes: Vec<(i64, Vec<UpdateValue>)>,
    /// `RETURNING` rows, evaluated against the new row values.
    returning: Vec<Vec<Cell>>,
    affected: usize,
    /// 规划期读到的行集快照。处理阶段直接复用它，不再重复整表解码。
    live: Vec<(i64, Vec<Cell>)>,
}

/// Everything a `DELETE` will remove.
struct DeletePlan {
    removed: Vec<i64>,
    /// `RETURNING` rows, evaluated against the row as it was.
    returning: Vec<Vec<Cell>>,
    /// 规划期读到的行集快照（以 `removed` 为子集），供触发器与外键动作复用。
    live: Vec<(i64, Vec<Cell>)>,
}

/// One engine-managed unique index an `ON CONFLICT` target matched.
#[derive(Clone)]
struct ManagedTarget {
    /// The partial or expression index the target selected.
    index: IndexDef,
    /// `t.a, t.b` fragment for uniqueness failures on the index.
    label: String,
}

/// The conflict target and rewrite of one `INSERT ... ON CONFLICT` clause.
struct UpsertPlan {
    /// Uniqueness rules the clause reacts to.
    keys: Vec<UniqueKey>,
    /// Managed indexes the clause reacts to.
    managed: Vec<ManagedTarget>,
    /// Other uniqueness rules that still abort the statement when the supplied
    /// row violates them.
    others: Vec<UniqueKey>,
    /// Managed indexes outside the target that still abort the statement.
    other_managed: Vec<ManagedTarget>,
    /// `DO NOTHING` or `DO UPDATE`.
    action: UpsertAction,
    /// `DO UPDATE SET` targets with their resolved right-hand sides.
    assignments: Vec<(usize, ResolvedExpr)>,
    /// Optional `DO UPDATE ... WHERE` predicate over the stored and supplied
    /// rows.
    filter: Option<ResolvedExpr>,
}

/// What an `ON CONFLICT` clause does with a conflicting row.
enum UpsertAction {
    /// `DO NOTHING`: the supplied row is discarded.
    Nothing,
    /// `DO UPDATE`: the conflicting stored row is rewritten.
    Update,
}

/// Result of applying one supplied row of an upsert.
enum UpsertOutcome {
    /// `DO NOTHING` skipped the row.
    Skipped,
    /// The row was written with this rowid.
    Inserted(i64),
    /// The conflicting row was rewritten.
    Updated {
        /// Rowid of the rewritten row.
        rowid: i64,
        /// `RETURNING` rows evaluated against the new values.
        rows: Vec<Vec<Cell>>,
    },
}

/// A resolved `RETURNING` projection.
struct ReturningPlan {
    columns: Vec<String>,
    items: Vec<ResolvedExpr>,
}

impl ReturningPlan {
    fn eval(&self, cells: &[Cell], ctx: &EvalContext) -> Result<Vec<Cell>, SqlError> {
        self.items.iter().map(|item| item.eval(cells, ctx)).collect()
    }
}

/// The single-source relation one `UPDATE` / `DELETE` resolves against.
struct TargetShape<'a> {
    relation: Relation<'a>,
    scope: ScopeShape,
}

impl<'a> TargetShape<'a> {
    fn new(def: &'a TableDef, label: &str, trigger: Option<&'a TriggerFrame>) -> Self {
        let relation = Engine::target_relation(def, label, trigger);
        let scope = relation.shape();
        Self { relation, scope }
    }

    fn resolve(&self, expr: &Expr, statement: &'static str) -> Result<ResolvedExpr, SqlError> {
        let mut windows = Vec::new();
        let mut aggregates = Vec::new();
        resolve_expr(expr, &self.relation, &[], &[], &mut windows, &mut aggregates, false, false).map_err(|error| error.in_statement(statement))
    }

    fn resolve_optional(&self, expr: Option<&Expr>, statement: &'static str) -> Result<Option<ResolvedExpr>, SqlError> {
        expr.map(|expr| self.resolve(expr, statement)).transpose()
    }

    fn plan(&self, def: &TableDef, items: &SelectItems, statement: &'static str) -> Result<ReturningPlan, SqlError> {
        match items {
            SelectItems::Star => Ok(ReturningPlan {
                columns: def.columns.iter().map(|column| column.name.clone()).collect(),
                items: (0..def.columns.len()).map(|index| ResolvedExpr::Column { source: 0, index }).collect(),
            }),
            SelectItems::Named(items) => Ok(ReturningPlan {
                columns: items.iter().map(|item| item.label()).collect(),
                items: items.iter().map(|item| self.resolve(item, statement)).collect::<Result<Vec<_>, _>>()?,
            }),
            SelectItems::CountAll => {
                Err(SqlError::new("unsupported-clause", "`RETURNING count(*)` is not supported").in_clause("RETURNING").in_statement(statement))
            }
        }
    }
}

/// Reserved tables SQLite keeps in the schema itself and that this engine
/// exposes as read-only sources.
#[derive(Clone, Copy)]
enum SystemTable {
    /// `sqlite_master`, also reachable as `sqlite_schema`.
    Master,
    /// `sqlite_temp_master`, always empty because temporary objects are not
    /// created by this surface.
    TempMaster,
    /// `sqlite_sequence`, present only after an AUTOINCREMENT table.
    Sequence,
}

impl SystemTable {
    fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "sqlite_master" | "sqlite_schema" => Some(Self::Master),
            "sqlite_temp_master" | "sqlite_temp_schema" => Some(Self::TempMaster),
            "sqlite_sequence" => Some(Self::Sequence),
            _ => None,
        }
    }
}

/// `sqlite_master` and its aliases.
fn is_schema_table(name: &str) -> bool {
    matches!(name.to_ascii_lowercase().as_str(), "sqlite_master" | "sqlite_schema" | "sqlite_temp_master" | "sqlite_temp_schema")
}

/// SQLite reserves every `sqlite_` name for internal objects.
fn check_reserved_object_name(name: &str, statement: &'static str) -> Result<(), SqlError> {
    if name.to_ascii_lowercase().starts_with("sqlite_") {
        return Err(SqlError::new("reserved-name", format!("object name reserved for internal use: {name}")).in_statement(statement));
    }
    Ok(())
}

/// Maps a storage failure onto the engine error surface.
fn storage_error(error: impl std::fmt::Display) -> SqlError {
    SqlError::new("storage", error.to_string()).in_statement("select")
}

/// A malformed on-disk structure found by the lazy scan.
fn corrupt_error(message: &'static str) -> SqlError {
    SqlError::new("corrupt", message).in_statement("select")
}

/// The schema tables are read-only for user writes.
fn check_readonly_table(name: &str, statement: &'static str) -> Result<(), SqlError> {
    if is_schema_table(name) || name.eq_ignore_ascii_case("sqlite_sequence") {
        return Err(SqlError::new("readonly-system-table", format!("table {name} may not be modified")).in_statement(statement));
    }
    Ok(())
}

/// In-memory SQLite format-3 database with a minimal SQL surface.
pub struct Engine {
    writer: SqliteWriter,
    /// Catalog in creation order, so traversal never depends on name sorting.
    /// The `sql` text and `rootpage` of every object are read back from the
    /// writer's `sqlite_master` b-tree instead of being duplicated here, which
    /// keeps one source of truth for the on-disk schema.
    tables: Vec<TableDef>,
    /// Lowercase table name to its position in [`Engine::tables`].
    table_lookup: BTreeMap<String, usize>,
    indexes: BTreeMap<String, IndexDef>,
    /// Creation counter handed to each new explicit index, so `PRAGMA
    /// index_list` lists the newest first like SQLite.
    index_sequence: u64,
    /// Views keyed by lowercase name. A view shares the object namespace with
    /// tables and indexes and re-evaluates its query on every read.
    views: BTreeMap<String, ViewDef>,
    /// `CREATE TEMP VIEW` definitions keyed by lowercase name. They shadow an
    /// equally named main view and disappear with the connection.
    temp_views: BTreeMap<String, TempView>,
    /// Triggers keyed by lowercase name, shared with the other object names.
    triggers: BTreeMap<String, TriggerDef>,
    /// Creation counter handed to each new trigger, so firing order follows
    /// SQLite's newest-first rule.
    trigger_sequence: u64,
    /// `NEW` / `OLD` frames of the trigger bodies currently executing,
    /// outermost first.
    trigger_frames: Vec<TriggerFrame>,
    /// Row-level trigger events queued by the statement being executed.
    pending_events: Vec<PendingTriggerEvent>,
    /// `CREATE TEMP TABLE` definitions and their rows. Temp tables live in a
    /// separate schema that disappears with the connection, so they never
    /// touch the persistent image or `sqlite_master`.
    temp_tables: Vec<TempTable>,
    /// Lowercase temp table name to its position in [`Engine::temp_tables`].
    /// Lookups search this map before [`Engine::table_lookup`], matching
    /// SQLite's schema search order.
    temp_lookup: BTreeMap<String, usize>,
    /// First unallocated page of the temp database, starting at 2 like every
    /// fresh SQLite database.
    temp_next_page: u32,
    /// Pages freed by `DROP TABLE` and reusable by the next temp object.
    temp_free_pages: BTreeSet<u32>,
    /// Next `sqlite_temp_master.rowid`, monotonic like SQLite's.
    temp_next_rowid: i64,
    /// The temp database was materialized once, so `PRAGMA database_list`
    /// keeps listing it even after its last object was dropped.
    temp_created: bool,
    /// Databases attached with `ATTACH DATABASE`, in attach order.
    attached: Vec<AttachedDb>,
    /// Lowercase attached schema name to its position in [`Engine::attached`].
    attached_lookup: BTreeMap<String, usize>,
    /// Schema name this database was attached under. A reference to that name
    /// inside the database itself resolves like an unqualified one.
    alias: Option<String>,
    /// `main.` / `temp.` qualifier of the statement being executed.
    scope: TableScope,
    /// Lowercase names of the triggers whose body is executing right now.
    /// With SQLite's default `recursive_triggers = OFF` a trigger program is
    /// not re-invoked by changes it caused itself, directly or indirectly,
    /// while the other triggers still fire.
    active_triggers: Vec<String>,
    /// Values bound to `?` / `:name` placeholders for the statement currently
    /// executing, in declaration order.
    parameters: Vec<Cell>,
    /// Supplied row of the `ON CONFLICT DO UPDATE` clause being evaluated.
    /// `excluded.<column>` reads this row instead of the table's stored row.
    upsert_excluded: Vec<Cell>,
    /// `PRAGMA foreign_keys`, enabled at session start like better-sqlite3.
    foreign_keys: bool,
    /// `PRAGMA recursive_triggers`, off at session start like SQLite.
    recursive_triggers: bool,
    /// Rowid of the most recent row a statement wrote successfully, like
    /// `sqlite3_last_insert_rowid`. It starts at 0 for every connection and
    /// only changes when an insert writes at least one row.
    last_insert_rowid: i64,
    /// Open explicit transaction, or `None` in autocommit. Savepoint names are
    /// stored outermost first.
    transaction: Option<TransactionState>,
    /// 单调递增的时钟,给索引内容版本取号用。
    ///
    /// 只有真正改动了某个索引内容的语句才推进它,逐语句推进会让写循环里
    /// 每次探针都重建整份索引键集。
    write_epoch: std::cell::Cell<u64>,
    /// 索引内容版本:小写索引名 → 该索引内容最后变化时的 epoch。
    ///
    /// 探针缓存的 `epoch` 与这里一致时才可复用。`INSERT` / 改动索引列的
    /// `UPDATE` / `DELETE` 之外的语句(DDL、`VACUUM` 等)一律全量失效。
    index_versions: std::cell::RefCell<HashMap<String, u64>>,
    /// Live key sets of plain column indexes, keyed by lowercase index name,
    /// valid only while the stored epoch matches the index's content version.
    index_probes: std::cell::RefCell<HashMap<String, IndexProbe>>,
    /// Memoized uncorrelated subquery results for the running statement, keyed
    /// by the resolved subquery node address. Cleared before every statement.
    subquery_cache: std::cell::RefCell<HashMap<usize, SubqueryCache>>,
    /// 单条语句内已解码的整表行,按小写表名索引,并记录该份行集覆盖的列掩码。
    /// 只缓存主库普通 rowid 表的无提示全表扫描:新请求所需列若已被缓存掩码
    /// 覆盖(全量解码覆盖任何裁剪),就复用同一份行集,让同表内外层子查询与
    /// 自连接不必各自解码一遍。语句开始与结束都清空,不跨语句保留。
    statement_scans: std::cell::RefCell<HashMap<String, (Option<Vec<bool>>, std::rc::Weak<Vec<Vec<Cell>>>)>>,
    /// Set whenever an `OuterColumn` is read, so a subquery can tell whether it
    /// depended on the enclosing row and therefore must not be memoized.
    outer_read: std::cell::Cell<bool>,
    /// 会话宿主函数表,按折叠小写名索引;解析期与求值期共用同一份。
    host_functions: HostFunctionTable,
    /// 显式事务内被推迟的索引 b-tree 重建。
    ///
    /// 索引页是整树重建出来的,单条语句重建一次会让事务内的第 n 条写入花掉
    /// O(n) 的重建成本。事务内只置位,提交或读镜像前统一重建一次。
    pending_indexes: bool,
    /// 同 [`Engine::pending_indexes`],针对 WITHOUT ROWID 表的聚簇树。
    pending_without_rowid: bool,
}

/// Explicit transaction bookkeeping, mirroring the writer's snapshot levels.
struct TransactionState {
    /// `BEGIN` opened the transaction rather than a bare `SAVEPOINT`.
    from_begin: bool,
    /// Open savepoint names, outermost first.
    savepoints: Vec<String>,
}

impl Engine {
    /// Creates a blank in-memory database image.
    pub fn new(page_size: Option<u32>) -> Result<Self, SqlError> {
        let page_size = page_size.unwrap_or(4096) as usize;
        let writer = SqliteWriter::create(page_size).map_err(|error| storage_failure(error))?;
        Self::from_writer(writer)
    }

    /// Wraps an already opened writer and rebuilds the catalog from the
    /// `sqlite_master` rows of its image, so `SELECT` works on a file opened
    /// with [`SqliteWriter::open`].
    pub fn from_writer(writer: SqliteWriter) -> Result<Self, SqlError> {
        let mut engine = Self {
            writer,
            tables: Vec::new(),
            table_lookup: BTreeMap::new(),
            indexes: BTreeMap::new(),
            index_sequence: 0,
            views: BTreeMap::new(),
            temp_views: BTreeMap::new(),
            triggers: BTreeMap::new(),
            trigger_sequence: 0,
            trigger_frames: Vec::new(),
            pending_events: Vec::new(),
            temp_tables: Vec::new(),
            temp_lookup: BTreeMap::new(),
            temp_next_page: 2,
            temp_free_pages: BTreeSet::new(),
            temp_next_rowid: 1,
            temp_created: false,
            attached: Vec::new(),
            attached_lookup: BTreeMap::new(),
            alias: None,
            scope: TableScope::Default,
            active_triggers: Vec::new(),
            parameters: Vec::new(),
            upsert_excluded: Vec::new(),
            foreign_keys: true,
            recursive_triggers: false,
            last_insert_rowid: 0,
            transaction: None,
            write_epoch: std::cell::Cell::new(0),
            index_versions: std::cell::RefCell::new(HashMap::new()),
            index_probes: std::cell::RefCell::new(HashMap::new()),
            subquery_cache: std::cell::RefCell::new(HashMap::new()),
            statement_scans: std::cell::RefCell::new(HashMap::new()),
            outer_read: std::cell::Cell::new(false),
            host_functions: HostFunctionTable::default(),
            pending_indexes: false,
            pending_without_rowid: false,
        };
        engine.load_catalog()?;
        Ok(engine)
    }

    /// Applies one explicit transaction control statement. Every scope keeps a
    /// writer snapshot level, so statements stay atomic inside a wider
    /// transaction and a rollback only undoes the scope it names.
    fn transaction(&mut self, action: TransactionAction) -> Result<ExecResult, SqlError> {
        let statement = action_name(&action);
        match action {
            TransactionAction::Begin => {
                if self.transaction.is_some() {
                    return Err(SqlError::new("transaction-active", "cannot start a transaction within a transaction").in_statement("begin"));
                }
                self.writer.begin().map_err(|error| storage_failure(error).in_statement("begin"))?;
                for attached in &mut self.attached {
                    attached.engine.writer.begin().map_err(|error| storage_failure(error).in_statement("begin"))?;
                }
                self.transaction = Some(TransactionState { from_begin: true, savepoints: Vec::new() });
            }
            TransactionAction::Commit => {
                if self.transaction.is_none() {
                    return Err(SqlError::new("transaction-missing", "cannot commit - no transaction is active").in_statement("commit"));
                }
                self.check_deferred_foreign_keys()?;
                for attached in &self.attached {
                    attached.engine.check_deferred_foreign_keys()?;
                }
                // 推迟的索引重建留到读取镜像的入口再落下:提交本身不读镜像,
                // 在这里重建会让每个写事务都付一次整树重建成本。
                self.transaction.take();
                self.writer.commit_all().map_err(|error| storage_failure(error).in_statement("commit"))?;
                for attached in &mut self.attached {
                    attached.engine.writer.commit_all().map_err(|error| storage_failure(error).in_statement("commit"))?;
                }
                self.persist_attached_databases()?;
            }
            TransactionAction::Rollback => {
                if self.transaction.take().is_none() {
                    return Err(SqlError::new("transaction-missing", "cannot rollback - no transaction is active").in_statement("rollback"));
                }
                self.writer.rollback_all().map_err(|error| storage_failure(error).in_statement("rollback"))?;
                // 回滚把内存键集退回事务前,但推迟标记可能来自事务开始之前
                // 的 autocommit 写入,这里只保留标记,不因回滚丢弃。
                self.reload_catalog()?;
                for index in 0..self.attached.len() {
                    self.attached[index]
                        .engine
                        .writer
                        .rollback_all()
                        .map_err(|error| storage_failure(error).in_statement("rollback"))?;
                    self.attached[index].dirty = false;
                    self.attached[index].engine.reload_catalog()?;
                }
            }
            TransactionAction::Savepoint(name) => {
                if self.transaction.is_none() {
                    // A savepoint outside `BEGIN` opens its own transaction.
                    self.writer.begin().map_err(|error| storage_failure(error).in_statement("savepoint"))?;
                    for attached in &mut self.attached {
                        attached
                            .engine
                            .writer
                            .begin()
                            .map_err(|error| storage_failure(error).in_statement("savepoint"))?;
                    }
                    self.transaction = Some(TransactionState { from_begin: false, savepoints: Vec::new() });
                }
                self.writer.savepoint(&name).map_err(|error| storage_failure(error).in_statement("savepoint"))?;
                for attached in &mut self.attached {
                    attached
                        .engine
                        .writer
                        .savepoint(&name)
                        .map_err(|error| storage_failure(error).in_statement("savepoint"))?;
                }
                self.transaction.as_mut().expect("savepoint opened a transaction").savepoints.push(name);
            }
            TransactionAction::Release(name) => {
                let index = self.savepoint_position(&name, "release")?;
                let closing = {
                    let state = self.transaction.as_ref().expect("an open savepoint implies a transaction");
                    index == 0 && !state.from_begin
                };
                if closing {
                    // Releasing the last savepoint of a transaction that no
                    // `BEGIN` opened commits it, so the commit gate runs
                    // first: deferred foreign keys must pass before any
                    // savepoint state is destroyed, and a failed check leaves
                    // the transaction open for a repair or a rollback.
                    self.check_deferred_foreign_keys()?;
                    for attached in &self.attached {
                        attached.engine.check_deferred_foreign_keys()?;
                    }
                }
                self.writer.release(&name).map_err(|error| storage_failure(error).in_statement("release"))?;
                for attached in &mut self.attached {
                    attached
                        .engine
                        .writer
                        .release(&name)
                        .map_err(|error| storage_failure(error).in_statement("release"))?;
                }
                let closing = {
                    let state = self.transaction.as_mut().expect("an open savepoint implies a transaction");
                    state.savepoints.truncate(index);
                    state.savepoints.is_empty() && !state.from_begin
                };
                if closing {
                    // Releasing the last savepoint of a transaction that no
                    // `BEGIN` opened commits it, as in SQLite. 索引重建同样
                    // 推迟到读取镜像的入口。
                    self.writer.commit_all().map_err(|error| storage_failure(error).in_statement("release"))?;
                    self.transaction = None;
                    self.persist_attached_databases()?;
                }
            }
            TransactionAction::RollbackTo(name) => {
                let index = self.savepoint_position(&name, "rollback-to")?;
                self.writer.rollback_to(&name).map_err(|error| storage_failure(error).in_statement("rollback-to"))?;
                self.reload_catalog()?;
                for index in 0..self.attached.len() {
                    self.attached[index]
                        .engine
                        .writer
                        .rollback_to(&name)
                        .map_err(|error| storage_failure(error).in_statement("rollback-to"))?;
                    self.attached[index].engine.reload_catalog()?;
                }
                self.transaction.as_mut().expect("an open savepoint implies a transaction").savepoints.truncate(index + 1);
            }
        }
        Ok(ExecResult { statement, columns: Vec::new(), rows: Vec::new(), affected: 0 })
    }

    /// Finds an open savepoint by name and reports SQLite's missing-savepoint
    /// error when it is absent. Names are case insensitive.
    fn savepoint_position(&self, name: &str, statement: &'static str) -> Result<usize, SqlError> {
        let missing = || SqlError::new("savepoint-missing", format!("no such savepoint: {name}")).in_statement(statement);
        let state = self.transaction.as_ref().ok_or_else(missing)?;
        state.savepoints.iter().rposition(|saved| saved.eq_ignore_ascii_case(name)).ok_or_else(missing)
    }

    /// Rebuilds the in-memory catalog after storage rolled back, so schema
    /// changes undone by the rollback also disappear from later statements.
    fn reload_catalog(&mut self) -> Result<(), SqlError> {
        self.tables.clear();
        self.table_lookup.clear();
        self.indexes.clear();
        self.views.clear();
        self.triggers.clear();
        self.load_catalog()
    }

    /// `PRAGMA integrity_check`: walks the header, the schema, every table and
    /// index b-tree, and the freelist chain over the current image. A healthy
    /// image reports one `ok` row; a structural problem is reported as a row of
    /// text, matching SQLite's non-failing result shape.
    ///
    /// The check reads pages, it does not trust the in-memory catalog or the
    /// writer's bookkeeping, so a decoder failure surfaces here instead of
    /// being answered with a hard-coded `ok`. Page-reachability accounting
    /// (detecting a page shared by two b-trees) is outside this walk because
    /// the reader API validates each tree in isolation.
    fn pragma_integrity_check(&mut self, argument: &str) -> ExecResult {
        // 检查走的是镜像页,先把推迟的索引重建落下,否则会把旧索引页判成问题。
        if let Err(error) = self.flush_pending_materialization("integrity-check") {
            return ExecResult {
                statement: "pragma",
                columns: vec!["integrity_check".to_string()],
                rows: vec![vec![Cell::Text(error.to_string())]],
                affected: 0,
            };
        }
        let mut problems = Vec::new();
        let pages = self.writer.pages();
        let limits = TableLimits::default();
        match validate_database(pages) {
            Ok(_) => {
                let objects = match self.writer.schema_objects(limits) {
                    Ok(objects) => objects,
                    Err(error) => {
                        problems.push(format!("schema: {error}"));
                        Vec::new()
                    }
                };
                for object in &objects {
                    let Some(root) = object.root_page.filter(|root| *root != 0)
                    else {
                        continue;
                    };
                    let outcome = match object.kind {
                        SchemaObjectKind::Index => {
                            let collations =
                                self.indexes.get(&object.name.to_ascii_lowercase()).map(|index| index.collations.clone()).unwrap_or_default();
                            read_index_table_with(pages, root, limits, &collations).map(|_| ())
                        }
                        SchemaObjectKind::Table if self.table_is_without_rowid(&object.name) => {
                            let collations = match self.tables.iter().find(|def| def.name.eq_ignore_ascii_case(&object.name)) {
                                Some(def) => {
                                    let layout = def.without_rowid_layout();
                                    layout[..def.primary_key_positions().map(|keys| keys.len()).unwrap_or(0)]
                                        .iter()
                                        .map(|position| def.columns[*position].collate.unwrap_or_default())
                                        .collect()
                                }
                                None => Vec::new(),
                            };
                            read_index_table_with(pages, root, limits, &collations).map(|_| ())
                        }
                        SchemaObjectKind::Table => read_table(pages, root, limits).map(|_| ()),
                        SchemaObjectKind::View | SchemaObjectKind::Trigger => Ok(()),
                    };
                    if let Err(error) = outcome {
                        let kind = match object.kind {
                            SchemaObjectKind::Table => "table",
                            SchemaObjectKind::Index => "index",
                            SchemaObjectKind::View => "view",
                            SchemaObjectKind::Trigger => "trigger",
                        };
                        problems.push(format!("{kind} `{}`: {error}", object.name));
                    }
                }
                self.collect_freelist_problems(pages, &mut problems);
            }
            Err(error) => problems.push(error.to_string()),
        }
        let limit = argument.trim().parse::<usize>().ok().filter(|limit| *limit > 0).unwrap_or(100);
        let rows = if problems.is_empty() {
            vec![vec![Cell::Text("ok".to_string())]]
        }
        else {
            problems.iter().take(limit).map(|problem| vec![Cell::Text(problem.clone())]).collect()
        };
        ExecResult { statement: "pragma", columns: vec!["integrity_check".to_string()], rows, affected: 0 }
    }

    /// Matches a schema table name against the catalog to know whether its
    /// root is a table b-tree or a primary-key index b-tree.
    fn table_is_without_rowid(&self, name: &str) -> bool {
        self.tables.iter().any(|def| def.name.eq_ignore_ascii_case(name) && def.without_rowid)
    }

    /// Replays the freelist chain and checks that the trunk pages and leaves
    /// account for exactly the count declared in the file header.
    fn collect_freelist_problems(&self, pages: &[u8], problems: &mut Vec<String>) {
        let page_size = self.writer.page_size() as usize;
        if page_size == 0 || pages.len() < page_size {
            return;
        }
        let page_count = pages.len() / page_size;
        let declared = database_formats::sqlite::read_freelist_count(pages) as usize;
        let mut trunk = database_formats::sqlite::read_freelist_trunk(pages);
        let mut seen = BTreeSet::new();
        let mut total = 0usize;
        while trunk != 0 {
            let index = trunk as usize;
            if index == 0 || index > page_count {
                problems.push(format!("freelist trunk page {trunk} is out of range"));
                return;
            }
            if !seen.insert(trunk) {
                problems.push(format!("freelist trunk page {trunk} is repeated"));
                return;
            }
            total += 1;
            let start = (index - 1) * page_size;
            let page = &pages[start..start + page_size];
            let next = u32::from_be_bytes([page[0], page[1], page[2], page[3]]);
            let leaves = u32::from_be_bytes([page[4], page[5], page[6], page[7]]) as usize;
            let capacity = page_size / 4 - 2;
            if leaves > capacity {
                problems.push(format!("freelist trunk page {trunk} declares {leaves} leaves beyond capacity {capacity}"));
                return;
            }
            for leaf_index in 0..leaves {
                let offset = 8 + leaf_index * 4;
                let leaf = u32::from_be_bytes([page[offset], page[offset + 1], page[offset + 2], page[offset + 3]]) as usize;
                if leaf == 0 || leaf > page_count {
                    problems.push(format!("freelist leaf page {leaf} on trunk {trunk} is out of range"));
                    continue;
                }
                if !seen.insert(leaf as u32) {
                    problems.push(format!("freelist leaf page {leaf} is repeated"));
                    continue;
                }
                total += 1;
            }
            trunk = next;
        }
        if total != declared {
            problems.push(format!("freelist declares {declared} pages but the chain holds {total}"));
        }
    }

    /// `EXPLAIN [QUERY PLAN]` reports this engine's own execution shape.
    ///
    /// The column headers follow SQLite, but the rows describe this tree-walking
    /// interpreter, not a SQLite VDBE program. Opcode names are this engine's
    /// steps, so the rows are not comparable with the real engine's disassembly.
    /// `EXPLAIN QUERY PLAN` reports the accesses that will really run, which is
    /// a scan whenever this engine has no index-driven lookup path.
    fn explain_statement(&mut self, query_plan: bool, statement: &Statement) -> Result<ExecResult, SqlError> {
        self.explain_check_tables(statement, &BTreeSet::new())?;
        let (columns, rows) = if query_plan {
            (vec!["id".to_string(), "parent".to_string(), "notused".to_string(), "detail".to_string()], self.explain_query_plan(statement))
        }
        else {
            (
                vec![
                    "addr".to_string(),
                    "opcode".to_string(),
                    "p1".to_string(),
                    "p2".to_string(),
                    "p3".to_string(),
                    "p4".to_string(),
                    "p5".to_string(),
                    "comment".to_string(),
                ],
                self.explain_program(statement),
            )
        };
        Ok(ExecResult { statement: "explain", columns, rows, affected: 0 })
    }

    /// `EXPLAIN` still resolves a missing source or target table, matching the
    /// prepare-time check the real engine performs. Column resolution is left
    /// to the statement itself because the program is not executed.
    fn explain_check_tables(&self, statement: &Statement, ctes: &BTreeSet<String>) -> Result<(), SqlError> {
        match statement {
            Statement::Select { with, first, compounds, .. } => {
                let mut scope = ctes.clone();
                for entry in with {
                    scope.insert(entry.name.to_ascii_lowercase());
                }
                for table in first.from.iter().chain(compounds.iter().flat_map(|term| term.core.from.iter())) {
                    self.explain_check_source(table, &scope)?;
                }
                Ok(())
            }
            Statement::Insert { table, query, .. } => {
                if !self.explain_table_known(table) {
                    return Err(SqlError::new("unknown-table", format!("no such table: {table}")));
                }
                match query {
                    Some(query) => self.explain_check_tables(query, ctes),
                    None => Ok(()),
                }
            }
            Statement::Update { table, from, .. } => {
                if !self.explain_table_known(table) {
                    return Err(SqlError::new("unknown-table", format!("no such table: {table}")));
                }
                for source in from {
                    self.explain_check_source(source, ctes)?;
                }
                Ok(())
            }
            Statement::Delete { table, .. } => {
                if self.explain_table_known(table) {
                    Ok(())
                }
                else {
                    Err(SqlError::new("unknown-table", format!("no such table: {table}")))
                }
            }
            Statement::Explain { statement, .. } => self.explain_check_tables(statement, ctes),
            _ => Ok(()),
        }
    }

    /// One `FROM` source: a derived table recurses into its own query, a named
    /// table must exist in a schema that the statement can see.
    fn explain_check_source(&self, table: &TableRef, scope: &BTreeSet<String>) -> Result<(), SqlError> {
        if let Some(query) = &table.query {
            return self.explain_check_tables(query, scope);
        }
        if let Some(schema) = &table.schema {
            let schema = schema.to_ascii_lowercase();
            if schema != "main" && schema != "temp" && self.attached_lookup.contains_key(&schema) {
                return Ok(());
            }
        }
        if scope.contains(&table.name.to_ascii_lowercase()) || self.explain_table_known(&table.name) {
            Ok(())
        }
        else {
            Err(SqlError::new("unknown-table", format!("no such table: {}", table.name)))
        }
    }

    /// Whether a name resolves to a table, view or system catalog table.
    fn explain_table_known(&self, name: &str) -> bool {
        let key = name.to_ascii_lowercase();
        self.table_lookup.contains_key(&key)
            || self.temp_lookup.contains_key(&key)
            || self.views.contains_key(&key)
            || self.temp_views.contains_key(&key)
            || is_schema_table(name)
            || SystemTable::from_name(name).is_some()
            || key == "sqlite_sequence"
    }

    /// Planner rows in SQLite's four-column shape. Ids are allocated here in
    /// visit order and `notused` stays zero because the real value is an
    /// internal bitmap, not an address this engine can produce.
    fn explain_query_plan(&self, statement: &Statement) -> Vec<Vec<Cell>> {
        let mut rows = Vec::new();
        let mut next_id = 1_i64;
        self.explain_plan_rows(statement, &mut rows, &mut next_id);
        rows
    }

    fn explain_plan_rows(&self, statement: &Statement, rows: &mut Vec<Vec<Cell>>, next_id: &mut i64) {
        match statement {
            Statement::Select { first, compounds, order, .. } => {
                self.explain_plan_core(first, rows, next_id);
                for term in compounds {
                    self.explain_plan_core(&term.core, rows, next_id);
                }
                if !first.group.is_empty() {
                    push_plan_row(rows, next_id, "USE TEMP B-TREE FOR GROUP BY");
                }
                if first.distinct {
                    push_plan_row(rows, next_id, "USE TEMP B-TREE FOR DISTINCT");
                }
                if !order.is_empty() {
                    push_plan_row(rows, next_id, "USE TEMP B-TREE FOR ORDER BY");
                }
            }
            Statement::Insert { query: Some(query), .. } => self.explain_plan_rows(query, rows, next_id),
            Statement::Update { table, from, .. } => {
                push_plan_row(rows, next_id, &format!("SCAN {table}"));
                for source in from {
                    push_plan_row(rows, next_id, &format!("SCAN {}", table_ref_label(source)));
                }
            }
            Statement::Delete { table, .. } => {
                push_plan_row(rows, next_id, &format!("SCAN {table}"));
            }
            Statement::Explain { statement, .. } => self.explain_plan_rows(statement, rows, next_id),
            _ => {}
        }
    }

    fn explain_plan_core(&self, core: &SelectCore, rows: &mut Vec<Vec<Cell>>, next_id: &mut i64) {
        if core.from.is_empty() {
            push_plan_row(rows, next_id, "SCAN CONSTANT ROW");
        }
        for table in &core.from {
            push_plan_row(rows, next_id, &format!("SCAN {}", table_ref_label(table)));
        }
    }

    /// Stepping program in SQLite's eight-column shape. This engine evaluates a
    /// statement tree, so the rows are the coarse steps it takes: open the
    /// sources, loop, produce a row, and close.
    fn explain_program(&self, statement: &Statement) -> Vec<Vec<Cell>> {
        let mut steps: Vec<ProgramStep> = Vec::new();
        match statement {
            Statement::Select { first, compounds, order, .. } => {
                let mut cursors = 0_i64;
                let mut sources: Vec<&TableRef> = first.from.iter().collect();
                for term in compounds {
                    sources.extend(term.core.from.iter());
                }
                for source in sources {
                    steps.push(ProgramStep::read(cursors, table_ref_label(source)));
                    cursors += 1;
                }
                if cursors > 0 {
                    steps.push(ProgramStep::plain("Rewind", 0));
                    steps.push(ProgramStep::plain("ResultRow", 0));
                    steps.push(ProgramStep::next(0));
                    for cursor in 0..cursors {
                        steps.push(ProgramStep::plain("Close", cursor));
                    }
                }
                else {
                    steps.push(ProgramStep::plain("ResultRow", 0));
                }
                if !order.is_empty() {
                    steps.push(ProgramStep::plain("Sort", 0));
                }
            }
            Statement::Insert { table, query, .. } => {
                if let Some(query) = query {
                    let _ = query;
                    steps.push(ProgramStep::plain("OpenRead", 0));
                    steps.push(ProgramStep::plain("Rewind", 0));
                    steps.push(ProgramStep::plain("Next", 0));
                }
                steps.push(ProgramStep::write(table));
                steps.push(ProgramStep::plain("Insert", 0));
                steps.push(ProgramStep::plain("Close", 0));
            }
            Statement::Update { table, .. } | Statement::Delete { table, .. } => {
                steps.push(ProgramStep::write(table));
                steps.push(ProgramStep::plain("Rewind", 0));
                steps.push(ProgramStep::plain("Next", 0));
                steps.push(ProgramStep::plain("Close", 0));
            }
            Statement::Transaction { .. } => steps.push(ProgramStep::plain("Transaction", 0)),
            Statement::Pragma { name, .. } => steps.push(ProgramStep::named("Pragma", name)),
            Statement::Attach { name, .. } => steps.push(ProgramStep::named("Attach", name)),
            Statement::Detach { name } => steps.push(ProgramStep::named("Detach", name)),
            Statement::Explain { statement, .. } => steps.push(ProgramStep::named("Explain", statement.name())),
            _ => steps.push(ProgramStep::named("ParseSchema", statement.name())),
        }
        let halt = steps.len() as i64 + 1;
        let mut rows = Vec::with_capacity(steps.len() + 2);
        rows.push(program_row(0, "Init", 0, halt, 0, None, 0));
        for (index, step) in steps.iter().enumerate() {
            rows.push(program_row(index as i64 + 1, step.opcode, step.p1, step.p2, 0, step.p4.as_deref(), step.p5));
        }
        rows.push(program_row(halt, "Halt", 0, 0, 0, None, 0));
        rows
    }

    /// Replays the stored `CREATE TABLE` / `CREATE INDEX` text of every
    /// schema row into the in-memory catalog without touching storage.
    fn load_catalog(&mut self) -> Result<(), SqlError> {
        let objects = self.writer.schema_objects(TableLimits::default()).map_err(storage_error)?;
        for object in objects {
            let Some(sql) = object.sql.as_deref()
            else {
                continue;
            };
            let mut statements = parse(sql)?.into_iter();
            match statements.next() {
                Some(Statement::CreateTable {
                    name,
                    columns,
                    primary_key,
                    table_constraints,
                    checks,
                    foreign_keys,
                    strict,
                    without_rowid,
                    ..
                }) => {
                    let def = Self::catalog_table(
                        name.clone(),
                        columns,
                        primary_key,
                        table_constraints,
                        checks,
                        foreign_keys,
                        strict,
                        without_rowid,
                    )?;
                    let key = name.to_ascii_lowercase();
                    if !self.table_lookup.contains_key(&key) {
                        let position = self.tables.len();
                        self.table_lookup.insert(key, position);
                        self.tables.push(def);
                    }
                }
                Some(Statement::CreateIndex { name, table, unique, columns, predicate, .. }) => {
                    let Some(def) = self.tables.iter().find(|def| def.name.eq_ignore_ascii_case(&table))
                    else {
                        continue;
                    };
                    let managed = predicate.is_some() || columns.iter().any(|column| column.expr.is_some() || column.collate.is_some());
                    // 键项显式 `COLLATE` 优先，裸列继承列声明，表达式按 `BINARY`。
                    let collations: Vec<Collation> = columns
                        .iter()
                        .map(|column| match &column.collate {
                            Some(name) => Collation::parse(name).unwrap_or_default(),
                            None => {
                                match (&column.expr, def.columns.iter().find(|existing| existing.name.eq_ignore_ascii_case(&column.name))) {
                                    (None, Some(existing)) => existing.collate.unwrap_or_default(),
                                    _ => Collation::Binary,
                                }
                            }
                        })
                        .collect();
                    let (positions, terms, predicate) = if managed {
                        let shape = TargetShape::new(def, &table, None);
                        let mut terms = Vec::with_capacity(columns.len());
                        let mut failed = false;
                        for column in &columns {
                            let resolved = match &column.expr {
                                Some(expr) => match shape.resolve(expr, "create-index") {
                                    Ok(resolved) => resolved,
                                    Err(_) => {
                                        failed = true;
                                        break;
                                    }
                                },
                                None => match def.columns.iter().position(|existing| existing.name.eq_ignore_ascii_case(&column.name)) {
                                    Some(position) => ResolvedExpr::Column { source: 0, index: position },
                                    None => {
                                        failed = true;
                                        break;
                                    }
                                },
                            };
                            terms.push(resolved);
                        }
                        if failed {
                            continue;
                        }
                        let predicate = match &predicate {
                            Some(predicate) => match shape.resolve(predicate, "create-index") {
                                Ok(resolved) => Some(resolved),
                                Err(_) => continue,
                            },
                            None => None,
                        };
                        (Vec::new(), terms, predicate)
                    }
                    else {
                        let mut positions = Vec::with_capacity(columns.len());
                        for column in &columns {
                            match def.columns.iter().position(|existing| existing.name.eq_ignore_ascii_case(&column.name)) {
                                Some(position) => positions.push(position),
                                None => break,
                            }
                        }
                        if positions.len() != columns.len() {
                            continue;
                        }
                        (positions, Vec::new(), None)
                    };
                    let order = self.index_sequence;
                    self.index_sequence += 1;
                    self.indexes.insert(
                        name.to_ascii_lowercase(),
                        IndexDef {
                            name: name.clone(),
                            table: table.to_ascii_lowercase(),
                            columns: positions,
                            unique,
                            order,
                            managed,
                            terms,
                            predicate,
                            collations,
                        },
                    );
                }
                Some(Statement::CreateView { name, columns, query, .. }) => {
                    let key = name.to_ascii_lowercase();
                    if !self.table_lookup.contains_key(&key) && !self.views.contains_key(&key) {
                        self.views.insert(key, ViewDef { name, columns, query: *query });
                    }
                }
                Some(Statement::CreateTrigger { name, timing, event, table, when, body, .. }) => {
                    let key = name.to_ascii_lowercase();
                    if !self.triggers.contains_key(&key) {
                        let order = self.trigger_sequence;
                        self.trigger_sequence += 1;
                        self.triggers.insert(
                            key,
                            TriggerDef {
                                name,
                                table: table.to_ascii_lowercase(),
                                timing,
                                event,
                                when,
                                body,
                                order,
                                temp: false,
                                sql: sql.to_string(),
                                master_rowid: 0,
                            },
                        );
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Resolves one parsed `CREATE TABLE` into a catalog entry without
    /// writing storage, shared by the DDL path and the reopen path.
    fn catalog_table(
        name: String,
        mut columns: Vec<ColumnSpec>,
        primary_key: Vec<String>,
        table_constraints: Vec<TableConstraint>,
        checks: Vec<CheckConstraint>,
        foreign_keys: Vec<ForeignKey>,
        strict: bool,
        without_rowid: bool,
    ) -> Result<TableDef, SqlError> {
        if strict {
            // SQLite limits STRICT columns to its five storage classes plus
            // `ANY`; the message quotes the declared text exactly as written.
            for column in &columns {
                let declared = column.declared_type.to_ascii_uppercase();
                if !matches!(declared.as_str(), "INT" | "INTEGER" | "REAL" | "TEXT" | "BLOB" | "ANY") {
                    return Err(SqlError::new(
                        "unknown-datatype",
                        format!("unknown datatype for {}.{}: \"{}\"", name, column.name, column.declared_type),
                    )
                    .in_statement("create-table"));
                }
            }
        }
        let mut key_positions = Vec::with_capacity(primary_key.len());
        for column in &primary_key {
            let position = columns.iter().position(|existing| existing.name.eq_ignore_ascii_case(column)).ok_or_else(|| {
                SqlError::new("unknown-column", format!("unknown column `{column}` in table `{name}`")).in_statement("create-table")
            })?;
            key_positions.push(position);
        }
        let mut resolved_constraints = Vec::with_capacity(table_constraints.len());
        for constraint in table_constraints {
            let mut positions = Vec::with_capacity(constraint.columns().len());
            for column in constraint.columns() {
                let position = columns
                    .iter()
                    .position(|existing| existing.name.eq_ignore_ascii_case(column))
                    .ok_or_else(|| SqlError::new("unknown-column", format!("no such column: {column}")).in_statement("create-table"))?;
                positions.push(position);
            }
            resolved_constraints
                .push(TableConstraintDef { primary: matches!(constraint, TableConstraint::PrimaryKey { .. }), columns: positions });
        }
        if without_rowid && resolved_constraints.iter().any(|constraint| !constraint.primary) {
            return Err(SqlError::new("unsupported-clause", "UNIQUE constraints on WITHOUT ROWID tables are not supported")
                .in_statement("create-table"));
        }
        if without_rowid {
            // A WITHOUT ROWID table needs a primary key: either the table-level
            // clause or exactly one inline `PRIMARY KEY`. The key columns are
            // implicitly NOT NULL and are stored first in every record.
            let inline: Vec<usize> =
                columns.iter().enumerate().filter(|(_, column)| column.primary_key).map(|(position, _)| position).collect();
            if !primary_key.is_empty() && !inline.is_empty() {
                return Err(SqlError::new("SQLITE_ERROR", format!("table {name} has more than one primary key")).in_statement("create-table"));
            }
            if key_positions.is_empty() {
                if inline.len() > 1 {
                    return Err(
                        SqlError::new("SQLITE_ERROR", format!("table {name} has more than one primary key")).in_statement("create-table")
                    );
                }
                key_positions = inline;
            }
            if key_positions.is_empty() {
                return Err(SqlError::new("SQLITE_ERROR", format!("PRIMARY KEY missing on table {name}")).in_statement("create-table"));
            }
            for position in &key_positions {
                columns[*position].not_null = true;
            }
        }
        // A lone INTEGER key column is the rowid alias whether the key is
        // declared inline or as a table constraint.
        let rowid_alias = if without_rowid {
            None
        }
        else {
            match key_positions.as_slice() {
                [position] if columns[*position].declared_type.eq_ignore_ascii_case("INTEGER") => Some(*position),
                _ => columns.iter().position(|column| column.primary_key && column.declared_type.eq_ignore_ascii_case("INTEGER")),
            }
        };
        let primary_key_index = !without_rowid && !primary_key.is_empty() && rowid_alias.is_none();
        // Checks are resolved before the table exists, so an unknown column
        // fails the statement instead of leaving a half-built table behind,
        // exactly like upstream.
        let resolved_checks = resolve_checks(&name, &columns, &checks, "create-table", without_rowid)?;
        // A `FOREIGN KEY (...)` list naming a missing child column is rejected
        // at DDL time, exactly like SQLite; the parent tables may still be
        // created later.
        let resolved_foreign_keys = resolve_foreign_keys(&columns, &foreign_keys)?;
        Ok(TableDef {
            name,
            columns,
            rowid_alias,
            primary_key_index,
            primary_key: key_positions,
            table_constraints: resolved_constraints,
            checks: resolved_checks,
            foreign_keys: resolved_foreign_keys,
            strict,
            without_rowid,
            temp: false,
        })
    }

    /// Executes exactly one statement.
    pub fn execute_one(&mut self, statement: &str) -> Result<ExecResult, SqlError> {
        let statements = self.parse(statement)?;
        match statements.len() {
            1 => {
                let only = statements.into_iter().next().expect("length checked");
                self.run_triggered(only)
            }
            0 => Err(SqlError::new("empty-statement", "expected exactly one statement but found 0")),
            count => Err(SqlError::new("multiple-statements", format!("expected exactly one statement but found {count}"))),
        }
    }

    /// Executes a list of statements in order and stops at the first rejection.
    pub fn execute_batch(&mut self, statements: &[String]) -> Result<Vec<ExecResult>, SqlError> {
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            results.push(self.execute_one(statement)?);
        }
        Ok(results)
    }

    /// Executes a semicolon-separated script and returns the results in order.
    pub fn execute(&mut self, sql: &str) -> Result<Vec<ExecResult>, SqlError> {
        let statements = self.parse(sql)?;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            results.push(self.run_triggered(statement)?);
        }
        Ok(results)
    }

    /// Executes a semicolon-separated script with positional parameter values
    /// bound to `?`, `?NNN` and `:name` placeholders.
    pub fn execute_with_parameters(&mut self, sql: &str, parameters: &[Cell]) -> Result<Vec<ExecResult>, SqlError> {
        self.parameters = parameters.to_vec();
        let result = self.execute(sql);
        self.parameters.clear();
        result
    }

    /// 执行一条已经解析好的语句,并绑定零基位置参数。
    ///
    /// 已解析的 AST 就是本引擎的"预编译"形态:宿主解析一次、反复执行同一
    /// 语句时不再重新词法或重新解析 SQL 文本。触发器体照常经过
    /// [`Engine::run_triggered`],与文本入口的执行路径一致。
    pub fn execute_statement(&mut self, statement: &Statement, parameters: &[Cell]) -> Result<ExecResult, SqlError> {
        self.parameters = parameters.to_vec();
        let result = self.run_triggered(statement.clone());
        self.parameters.clear();
        result
    }

    /// 按本引擎的宿主函数表解析一段分号分隔的脚本。
    ///
    /// 与 [`crate::sql::parse`] 的唯一区别:折叠名命中宿主注册表且元数相符
    /// 的函数调用成为 [`Expr::HostFunction`] 节点,同名同元数的内建派发被
    /// 替换。宿主在 prepare 之前注册即可让语句通过解析;注册发生在某条
    /// 已解析语句之后时,该语句里的宿主调用在执行期按当时的注册表求值,
    /// 未注册的名字以 `unknown-function` 如实拒绝。
    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>, SqlError> {
        parse_with_host_functions(sql, Some(&self.host_functions))
    }

    /// 注册一个宿主自定义标量函数,形态取自 `database-udf` 合同。
    ///
    /// `definition` 是合同的 [`HostMicroDefinition`] 元数据(身份名、签名、
    /// 策略、不透明句柄、指纹),`adapter` 是宿主运行时中真实函数注册表的
    /// 适配器;注册前按合同校验元数据,不合合同的定义以
    /// `udf-invalid-definition` 拒绝。派发名按 ASCII 小写折叠,与内建函数
    /// 同一口径;折叠同名再次注册替换旧实现(JS API 的注册语义),折叠名
    /// 与内建函数同名同元数时宿主实现优先,与 SQLite
    /// `sqlite3_create_function` 替换内建的真实行为一致。
    ///
    /// 注册同时传播到已附加库的引擎,并对之后 `ATTACH` 的库继续生效,
    /// 使宿主函数在跨库语句与附加库视图展开里同样可求值。
    ///
    /// 合同是 Phase-1 标量形态,没有 step / final 聚合语义:宿主聚合与
    /// 宿主窗口函数不接入,`OVER` / `FILTER` / `DISTINCT` 形态在解析期以
    /// 明确错误拒绝。
    pub fn register_host_function(&mut self, definition: HostMicroDefinition, adapter: Arc<dyn HostRuntimeAdapter>) -> Result<(), SqlError> {
        let implementation = HostFunctionTable::build(definition, adapter)?;
        self.host_functions.install(implementation.clone());
        for attached in &mut self.attached {
            attached.engine.host_functions.install(implementation.clone());
        }
        Ok(())
    }

    /// 当前会话注册的宿主函数名(折叠小写,字典序)。
    pub fn host_function_names(&self) -> Vec<String> {
        self.host_functions.names()
    }

    /// `PRAGMA synchronous` 的透传:`false` 时落盘跳过持久化刷新。
    pub fn set_synchronous(&mut self, on: bool) {
        self.writer.set_synchronous(on);
    }

    /// Current database image, suitable for writing to a `.sqlite3` file.
    ///
    /// Index b-trees are rebuilt before the image is handed out, so the exported
    /// bytes stay consistent with their `sqlite_master` entries without calling
    /// [`SqliteWriter::save`]. Inside an explicit transaction the rebuild of the
    /// writes made so far happens here, which is why the receiver is mutable and
    /// the rebuild can fail.
    pub fn snapshot(&mut self) -> Result<&[u8], SqlError> {
        self.flush_pending_materialization("snapshot")?;
        Ok(self.writer.pages())
    }

    /// 写完一条语句后标记索引 b-tree 需要重建。
    ///
    /// 索引 b-tree 只能整树重建,单条语句重建一次会让事务内第 n 条写入付掉
    /// O(n) 的重建成本,所以任何写语句都只置位。重建推迟到真正读取镜像的
    /// 入口(提交不再重建,`snapshot` / `save` / `VACUUM` /
    /// `PRAGMA integrity_check` / 惰性游标打开),这些入口都会先调用
    /// [`Engine::flush_pending_materialization`],因此读到的镜像仍然与内存
    /// 键集一致。
    fn materialize_indexes_after_write(&mut self, _statement: &'static str) -> Result<(), SqlError> {
        self.pending_indexes = true;
        Ok(())
    }

    /// 同 [`Engine::materialize_indexes_after_write`],针对 WITHOUT ROWID 表的聚簇树。
    fn materialize_without_rowid_after_write(&mut self, _statement: &'static str) -> Result<(), SqlError> {
        self.pending_without_rowid = true;
        Ok(())
    }

    /// 把推迟的索引 / 聚簇树重建落进当前镜像。
    ///
    /// 只在事务提交、写盘或读取镜像时调用;重建完成后清位,失败则保留标记让
    /// 调用方还能重试或回滚。
    fn flush_pending_materialization(&mut self, statement: &'static str) -> Result<(), SqlError> {
        if self.pending_indexes {
            self.writer.materialize_indexes().map_err(|error| storage_failure(error).in_statement(statement))?;
            self.pending_indexes = false;
        }
        if self.pending_without_rowid {
            self.writer.materialize_without_rowid().map_err(|error| storage_failure(error).in_statement(statement))?;
            self.pending_without_rowid = false;
        }
        Ok(())
    }

    /// Rowid of the last row this connection inserted successfully, mirroring
    /// `sqlite3_last_insert_rowid`.
    ///
    /// A statement that inserts several rows reports the last row it wrote. If
    /// a later row fails and the statement rolls back, the value stays at the
    /// last row that did pass, because SQLite records it row by row. Statements
    /// that insert no row leave it untouched.
    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid
    }

    /// Whether an explicit transaction or savepoint scope is open.
    pub fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    /// Flushes the current database image to `path`.
    ///
    /// Reads of an existing file go through [`Engine::from_writer`]; this is
    /// the matching write-back so a file-backed session survives reopening.
    pub fn save(&mut self, path: impl AsRef<Path>) -> Result<(), SqlError> {
        self.flush_pending_materialization("save")?;
        self.writer.save(path).map_err(|error| storage_failure(error))
    }

    /// Flushes the current database image to `path` without ever exposing a
    /// half-written file, by renaming a complete sibling temporary file over
    /// it.
    pub fn save_atomic(&mut self, path: impl AsRef<Path>) -> Result<(), SqlError> {
        self.flush_pending_materialization("save")?;
        self.writer.save_atomic(path).map_err(|error| storage_failure(error))
    }

    /// Configured page size.
    pub fn page_size(&self) -> u32 {
        self.writer.page_size() as u32
    }

    /// Format-3 library version stamped in the image header.
    pub fn sqlite_version(&self) -> Result<String, SqlError> {
        decode_library_version(self.writer.pages()).map_err(|error| storage_failure(error))
    }

    /// Materializes one reserved table with the labels and row order SQLite
    /// reports. `sqlite_master` is read straight from the writer's schema
    /// b-tree, so `type`, `sql`, `rootpage`, `rowid` and ordering are the
    /// on-disk values rather than a second copy that could drift.
    fn system_rows(&self, system: SystemTable, name: &str) -> Result<(Vec<String>, Vec<Vec<Cell>>, bool), SqlError> {
        let master_labels = || vec!["type".to_string(), "name".to_string(), "tbl_name".to_string(), "rootpage".to_string(), "sql".to_string()];
        match system {
            SystemTable::Master => {
                let objects = self.writer.schema_objects_with_rowids(TableLimits::default()).map_err(storage_error)?;
                let mut rows = Vec::with_capacity(objects.len());
                for (rowid, object) in objects {
                    let kind = match object.kind {
                        SchemaObjectKind::Table => "table",
                        SchemaObjectKind::Index => "index",
                        SchemaObjectKind::View => "view",
                        SchemaObjectKind::Trigger => "trigger",
                    };
                    rows.push(vec![
                        Cell::Text(kind.to_string()),
                        Cell::Text(object.name),
                        Cell::Text(object.table_name),
                        object.root_page.map_or(Cell::Null, |page| Cell::Integer(i64::from(page))),
                        object.sql.map_or(Cell::Null, Cell::Text),
                        Cell::Integer(rowid),
                    ]);
                }
                Ok((master_labels(), rows, true))
            }
            SystemTable::TempMaster => {
                let mut rows = Vec::with_capacity(self.temp_tables.len() + self.temp_views.len() + self.triggers.len());
                for table in &self.temp_tables {
                    rows.push(vec![
                        Cell::Text("table".to_string()),
                        Cell::Text(table.def.name.clone()),
                        Cell::Text(table.def.name.clone()),
                        Cell::Integer(i64::from(table.rootpage)),
                        Cell::Text(table.sql.clone()),
                        Cell::Integer(table.master_rowid),
                    ]);
                }
                for view in self.temp_views.values() {
                    rows.push(vec![
                        Cell::Text("view".to_string()),
                        Cell::Text(view.def.name.clone()),
                        Cell::Text(view.def.name.clone()),
                        Cell::Integer(0),
                        Cell::Text(view.sql.clone()),
                        Cell::Integer(view.master_rowid),
                    ]);
                }
                for trigger in self.triggers.values().filter(|trigger| trigger.temp) {
                    rows.push(vec![
                        Cell::Text("trigger".to_string()),
                        Cell::Text(trigger.name.clone()),
                        Cell::Text(trigger.table.clone()),
                        Cell::Integer(0),
                        Cell::Text(trigger.sql.clone()),
                        Cell::Integer(trigger.master_rowid),
                    ]);
                }
                rows.sort_by_key(|row| match row.get(5) {
                    Some(Cell::Integer(rowid)) => *rowid,
                    _ => 0,
                });
                Ok((master_labels(), rows, true))
            }
            SystemTable::Sequence => {
                let present = self
                    .writer
                    .schema_objects(TableLimits::default())
                    .map_err(storage_error)?
                    .iter()
                    .any(|object| object.kind == SchemaObjectKind::Table && object.name.eq_ignore_ascii_case(name));
                if !present {
                    return Err(SqlError::new("unknown-table", format!("no such table: {name}")).in_statement("select"));
                }
                let stored = self.writer.table_rows("sqlite_sequence", TableLimits::default()).map_err(storage_error)?;
                let mut rows = Vec::with_capacity(stored.len());
                for row in &stored {
                    let mut cells = decode_cells(&row.payload)?;
                    cells.resize(2, Cell::Null);
                    cells.push(Cell::Integer(row.rowid));
                    rows.push(cells);
                }
                Ok((vec!["name".to_string(), "seq".to_string()], rows, true))
            }
        }
    }

    /// Runs one statement and fires the row triggers attached to its target.
    ///
    /// A DML statement queues one event per affected row; the events are
    /// drained after the statement's writes are in place, in SQLite's
    /// `BEFORE` / `AFTER` order. Trigger bodies run through this same entry
    /// point, so a body statement still fires the other triggers. With
    /// `recursive_triggers` off, a trigger already on the stack is skipped.
    fn run_triggered(&mut self, statement: Statement) -> Result<ExecResult, SqlError> {
        let fired = match &statement {
            Statement::Insert { schema, table, .. } => Some((schema.as_deref(), table.clone())),
            Statement::Update { schema, table, .. } => Some((schema.as_deref(), table.clone())),
            Statement::Delete { schema, table, .. } => Some((schema.as_deref(), table.clone())),
            _ => None,
        };
        let Some((schema, table)) = fired
        else {
            return self.run_statement(statement);
        };
        if !self.target_is_local(schema, &table) {
            return self.run_statement(statement);
        }
        let saved_events = std::mem::take(&mut self.pending_events);
        // A trigger that fails has to undo the statement that fired it, like
        // SQLite's per-statement rollback, so the whole statement runs inside
        // one writer level owned here.
        self.writer.begin().map_err(|error| storage_failure(error).in_statement("trigger"))?;
        let outcome = match self.run_statement(statement) {
            Ok(result) => {
                let events = std::mem::take(&mut self.pending_events);
                let mut failure = None;
                for event in events {
                    if let Err(error) = self.fire_event(event) {
                        failure = Some(error);
                        break;
                    }
                }
                match failure {
                    Some(error) => {
                        let _ = self.writer.rollback();
                        Err(error)
                    }
                    None => {
                        self.writer.commit().map_err(|error| storage_failure(error).in_statement("trigger"))?;
                        Ok(result)
                    }
                }
            }
            Err(error) => {
                let _ = self.writer.rollback();
                Err(error)
            }
        };
        self.pending_events = saved_events;
        outcome
    }

    /// Whether an unqualified or schema-qualified target table lives in one of
    /// the schemas this engine owns; an attached target runs in its own engine
    /// and must not fire the triggers of `main`.
    fn target_is_local(&self, schema: Option<&str>, table: &str) -> bool {
        let key = table.to_ascii_lowercase();
        match schema {
            Some(qualifier) => {
                let lower = qualifier.to_ascii_lowercase();
                lower == "main" || lower == "temp" || self.alias.as_deref().is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
            }
            None => {
                self.temp_lookup.contains_key(&key)
                    || self.table_lookup.contains_key(&key)
                    || self.views.contains_key(&key)
                    || self.attached.is_empty()
            }
        }
    }

    /// Fires the triggers one queued row event matches.
    fn fire_event(&mut self, event: PendingTriggerEvent) -> Result<(), SqlError> {
        let statement = event_statement(&event.event);
        if self.recursive_triggers && self.active_triggers.len() >= MAX_TRIGGER_DEPTH {
            return Err(SqlError::new("trigger-depth", "too many levels of trigger recursion").in_statement(statement));
        }
        let frame = TriggerFrame { columns: event.columns, new: event.new, old: event.old };
        self.trigger_frames.push(frame);
        let outcome = self.fire_matching(&event.table, &event.event, event.timing);
        self.trigger_frames.pop();
        outcome
    }

    /// Runs every trigger of `table` whose event and timing match, newest
    /// first, which is the order SQLite uses for triggers of the same event.
    fn fire_matching(&mut self, table: &str, event: &TriggerEvent, timing: TriggerTiming) -> Result<(), SqlError> {
        let mut matched: Vec<(String, TriggerDef)> = self
            .triggers
            .iter()
            .filter(|(_, trigger)| trigger.table == table && trigger.timing == timing && trigger_event_matches(&trigger.event, event))
            .map(|(key, trigger)| (key.clone(), trigger.clone()))
            .collect();
        matched.sort_by(|left, right| right.1.order.cmp(&left.1.order));
        let statement = event_statement(event);
        for (key, trigger) in matched {
            if !self.recursive_triggers && self.active_triggers.contains(&key) {
                continue;
            }
            if self.recursive_triggers && self.active_triggers.len() >= MAX_TRIGGER_DEPTH {
                return Err(SqlError::new("trigger-depth", "too many levels of trigger recursion").in_statement(statement));
            }
            if let Some(when) = &trigger.when {
                match self.trigger_when_holds(when) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(error) => return Err(error),
                }
            }
            self.active_triggers.push(key);
            let mut outcome = Ok(());
            for statement in trigger.body.clone() {
                outcome = self.run_triggered(statement).map(|_| ());
                if outcome.is_err() {
                    break;
                }
            }
            self.active_triggers.pop();
            outcome?;
        }
        Ok(())
    }

    /// Evaluates one trigger `WHEN` predicate against the current frame.
    fn trigger_when_holds(&self, when: &Expr) -> Result<bool, SqlError> {
        let relation =
            Relation { sources: Vec::new(), using: Vec::new(), using_slots: Vec::new(), trigger: self.trigger_frames.last(), excluded: false };
        let shape = relation.shape();
        let mut aggregates = Vec::new();
        let mut windows = Vec::new();
        let resolved = resolve_expr(when, &relation, &[], &[], &mut windows, &mut aggregates, false, false)?;
        let budget = Budget::new();
        let ctx = EvalContext {
            engine: self,
            outer: &[],
            current: &shape,
            aggregates: &[],
            windows: &[],
            window_row: 0,
            budget: &budget,
            ctes: None,
        };
        Ok(truthiness(&resolved.eval(&[], &ctx)?) == Some(true))
    }

    /// Evaluates one `VALUES` expression of a trigger body against the active
    /// `NEW` / `OLD` frame.
    fn eval_trigger_expression(&self, expression: &Expr, statement: &'static str) -> Result<Cell, SqlError> {
        let relation =
            Relation { sources: Vec::new(), using: Vec::new(), using_slots: Vec::new(), trigger: self.trigger_frames.last(), excluded: false };
        let shape = relation.shape();
        let mut aggregates = Vec::new();
        let mut windows = Vec::new();
        let resolved = resolve_expr(expression, &relation, &[], &[], &mut windows, &mut aggregates, false, false)
            .map_err(|error| error.in_statement(statement))?;
        let budget = Budget::new();
        let ctx = EvalContext {
            engine: self,
            outer: &[],
            current: &shape,
            aggregates: &[],
            windows: &[],
            window_row: 0,
            budget: &budget,
            ctes: None,
        };
        resolved.eval(&[], &ctx)
    }

    /// Whether one table or view carries at least one trigger of any kind.
    fn table_has_triggers(&self, table: &str) -> bool {
        self.triggers.values().any(|trigger| trigger.table == table)
    }

    /// Queues one `BEFORE` update event of a row.
    fn queue_update_before(
        &mut self,
        table: &str,
        def: &TableDef,
        columns: &[String],
        assigned: &[String],
        rowid: i64,
        old: &[Cell],
        new: &[Cell],
    ) {
        self.queue_update_event(table, def, columns, assigned, rowid, old, new, TriggerTiming::Before);
    }

    /// Queues one `AFTER` update event of a row.
    fn queue_update_after(
        &mut self,
        table: &str,
        def: &TableDef,
        columns: &[String],
        assigned: &[String],
        rowid: i64,
        old: &[Cell],
        new: &[Cell],
    ) {
        self.queue_update_event(table, def, columns, assigned, rowid, old, new, TriggerTiming::After);
    }

    /// Queues the `BEFORE` and `AFTER` events of one updated row.
    fn queue_update_events(
        &mut self,
        table: &str,
        def: &TableDef,
        columns: &[String],
        assigned: &[String],
        rowid: i64,
        old: &[Cell],
        new: &[Cell],
    ) {
        self.queue_update_before(table, def, columns, assigned, rowid, old, new);
        self.queue_update_after(table, def, columns, assigned, rowid, old, new);
    }

    fn queue_update_event(
        &mut self,
        table: &str,
        def: &TableDef,
        columns: &[String],
        assigned: &[String],
        rowid: i64,
        old: &[Cell],
        new: &[Cell],
        timing: TriggerTiming,
    ) {
        let new_rowid = def
            .rowid_alias
            .and_then(|alias| new.get(alias))
            .and_then(|cell| match cell {
                Cell::Integer(value) => Some(*value),
                _ => None,
            })
            .unwrap_or(rowid);
        let old = frame_cells(old, def.columns.len(), rowid);
        let new = frame_cells(new, def.columns.len(), new_rowid);
        self.pending_events.push(PendingTriggerEvent {
            table: table.to_string(),
            timing,
            event: TriggerEvent::Update(assigned.to_vec()),
            columns: columns.to_vec(),
            new: Some(new),
            old: Some(old),
        });
    }

    /// Queues one `BEFORE` delete event of a row.
    fn queue_delete_before(&mut self, table: &str, columns: &[String], rowid: i64, old: &[Cell]) {
        self.queue_delete_event(table, columns, rowid, old, TriggerTiming::Before);
    }

    /// Queues one `AFTER` delete event of a row.
    fn queue_delete_after(&mut self, table: &str, columns: &[String], rowid: i64, old: &[Cell]) {
        self.queue_delete_event(table, columns, rowid, old, TriggerTiming::After);
    }

    /// Queues the `BEFORE` and `AFTER` events of one deleted row.
    fn queue_delete_events(&mut self, table: &str, columns: &[String], rowid: i64, old: &[Cell]) {
        self.queue_delete_before(table, columns, rowid, old);
        self.queue_delete_after(table, columns, rowid, old);
    }

    fn queue_delete_event(&mut self, table: &str, columns: &[String], rowid: i64, old: &[Cell], timing: TriggerTiming) {
        let old = frame_cells(old, columns.len() - 1, rowid);
        self.pending_events.push(PendingTriggerEvent {
            table: table.to_string(),
            timing,
            event: TriggerEvent::Delete,
            columns: columns.to_vec(),
            new: None,
            old: Some(old),
        });
    }

    /// Output columns and rows of one view, used as the source of
    /// `INSTEAD OF` triggers.
    fn view_output(&self, view: &ViewDef) -> Result<(Vec<String>, Vec<Vec<Cell>>), SqlError> {
        let budget = Budget::new();
        let result = self.select_statement(&view.query, &[], &budget, None)?;
        let names = view_output_names(view, &result.columns)?;
        Ok((names, result.rows))
    }

    /// Materializes one view exactly like a `FROM` reference does: the stored
    /// query runs again, then its output names are declared or inferred.
    fn expand_view(&self, view: &ViewDef, budget: &Budget) -> Result<(Vec<String>, Vec<Vec<Cell>>), SqlError> {
        let result = self.select_statement(&view.query, &[], budget, None)?;
        let names = view_output_names(view, &result.columns)?;
        Ok((names, result.rows))
    }

    /// Runs the `INSTEAD OF` triggers of one view when it has any; a view
    /// without one keeps SQLite's non-modifiable diagnostic.
    fn view_trigger_gate(&self, table: &str, key: &str, timing: TriggerTiming, event: &TriggerEvent) -> Result<(), SqlError> {
        if self
            .triggers
            .values()
            .any(|trigger| trigger.table == key && trigger.timing == timing && trigger_event_matches(&trigger.event, event))
        {
            return Ok(());
        }
        Err(SqlError::new("readonly-view", format!("cannot modify {table} because it is a view")).in_statement(event_statement(event)))
    }

    /// `INSERT INTO <view>` dispatched to its `INSTEAD OF INSERT` triggers.
    fn run_view_insert(
        &mut self,
        table: &str,
        key: &str,
        columns: Option<&[String]>,
        rows: &[Vec<InsertValue>],
        default_values: bool,
    ) -> Result<ExecResult, SqlError> {
        let event = TriggerEvent::Insert;
        self.view_trigger_gate(table, key, TriggerTiming::InsteadOf, &event)?;
        let view = self.views.get(key).expect("view resolved").clone();
        let (names, _) = self.view_output(&view)?;
        let mut frames: Vec<Vec<Cell>> = Vec::with_capacity(rows.len().max(1));
        if default_values {
            frames.push(vec![Cell::Null; names.len()]);
        }
        else {
            for values in rows {
                let mut cells = vec![Cell::Null; names.len()];
                if let Some(list) = columns {
                    for (slot, name) in list.iter().enumerate() {
                        let position = names.iter().position(|column| column.eq_ignore_ascii_case(name)).ok_or_else(|| {
                            SqlError::new("unknown-column", format!("table {table} has no column named {name}")).in_statement("insert")
                        })?;
                        if let Some(value) = values.get(slot) {
                            cells[position] = self.insert_value_cell(value, "insert")?;
                        }
                    }
                }
                else {
                    for (slot, value) in values.iter().enumerate() {
                        if let Some(cell) = cells.get_mut(slot) {
                            *cell = self.insert_value_cell(value, "insert")?;
                        }
                    }
                }
                frames.push(cells);
            }
        }
        let saved = self.last_insert_rowid;
        for cells in frames {
            let frame = TriggerFrame { columns: names.clone(), new: Some(cells), old: None };
            self.trigger_frames.push(frame);
            let outcome = self.fire_matching(key, &event, TriggerTiming::InsteadOf);
            self.trigger_frames.pop();
            outcome?;
        }
        self.last_insert_rowid = saved;
        Ok(ExecResult { statement: "insert", columns: Vec::new(), rows: Vec::new(), affected: 0 })
    }

    /// Evaluates one `INSERT` value, including the trigger-body expressions.
    fn insert_value_cell(&self, value: &InsertValue, statement: &'static str) -> Result<Cell, SqlError> {
        match value {
            InsertValue::Literal(literal) => Ok(literal_cell(literal)),
            InsertValue::Parameter { index, .. } => Ok(self.parameters.get(*index).cloned().unwrap_or(Cell::Null)),
            InsertValue::Expression(expression) => self.eval_trigger_expression(expression, statement),
        }
    }

    /// `UPDATE <view>` dispatched to its `INSTEAD OF UPDATE` triggers.
    fn run_view_update(
        &mut self,
        table: &str,
        key: &str,
        alias: Option<&str>,
        assignments: &[Assignment],
        filter: Option<&Expr>,
        returning: Option<&SelectItems>,
    ) -> Result<ExecResult, SqlError> {
        let assigned: Vec<String> = assignments.iter().map(|assignment| assignment.column.clone()).collect();
        let event = TriggerEvent::Update(assigned);
        self.view_trigger_gate(table, key, TriggerTiming::InsteadOf, &event)?;
        if returning.is_some() {
            return Err(
                SqlError::new("unsupported-clause", "RETURNING on an INSTEAD OF view update is not supported yet").in_statement("update")
            );
        }
        let view = self.views.get(key).expect("view resolved").clone();
        let (names, rows) = self.view_output(&view)?;
        // A placeholder frame lets `old.` / `new.` references resolve; the
        // values are replaced row by row below.
        let placeholder = vec![Cell::Null; names.len()];
        self.trigger_frames.push(TriggerFrame { columns: names.clone(), new: Some(placeholder.clone()), old: Some(placeholder) });
        let resolved = (|| -> Result<(Option<ResolvedExpr>, Vec<(usize, ResolvedExpr)>, ScopeShape), SqlError> {
            let relation = view_relation(alias.unwrap_or(table), &names, self.trigger_frames.last());
            let shape = relation.shape();
            let mut aggregates = Vec::new();
            let mut windows = Vec::new();
            let predicate = match filter {
                Some(filter) => Some(resolve_expr(filter, &relation, &[], &[], &mut windows, &mut aggregates, false, false)?),
                None => None,
            };
            let mut targets = Vec::with_capacity(assignments.len());
            for assignment in assignments {
                let index = names.iter().position(|column| column.eq_ignore_ascii_case(&assignment.column)).ok_or_else(|| {
                    SqlError::new("unknown-column", format!("table {table} has no column named {}", assignment.column)).in_statement("update")
                })?;
                let expression = resolve_expr(&assignment.value, &relation, &[], &[], &mut windows, &mut aggregates, false, false)?;
                targets.push((index, expression));
            }
            Ok((predicate, targets, shape))
        })();
        let (predicate, targets, shape) = match resolved {
            Ok(resolved) => resolved,
            Err(error) => {
                self.trigger_frames.pop();
                return Err(error);
            }
        };
        let budget = Budget::new();
        let saved = self.last_insert_rowid;
        let mut outcome = Ok(());
        for row in rows {
            let row = row.get(..names.len()).map(|cells| cells.to_vec()).unwrap_or_else(|| vec![Cell::Null; names.len()]);
            match row_matches(&predicate, &row, self, &shape, &budget) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(error) => {
                    outcome = Err(error);
                    break;
                }
            }
            let ctx = EvalContext {
                engine: self,
                outer: &[],
                current: &shape,
                aggregates: &[],
                windows: &[],
                window_row: 0,
                budget: &budget,
                ctes: None,
            };
            let mut next = row.clone();
            for (index, expression) in &targets {
                match expression.eval(&row, &ctx) {
                    Ok(value) => next[*index] = value,
                    Err(error) => {
                        outcome = Err(error.in_statement("update"));
                        break;
                    }
                }
            }
            if outcome.is_err() {
                break;
            }
            if let Some(frame) = self.trigger_frames.last_mut() {
                frame.old = Some(row);
                frame.new = Some(next);
            }
            outcome = self.fire_matching(key, &event, TriggerTiming::InsteadOf);
            if outcome.is_err() {
                break;
            }
        }
        self.trigger_frames.pop();
        self.last_insert_rowid = saved;
        outcome?;
        Ok(ExecResult { statement: "update", columns: Vec::new(), rows: Vec::new(), affected: 0 })
    }

    /// `DELETE FROM <view>` dispatched to its `INSTEAD OF DELETE` triggers.
    fn run_view_delete(
        &mut self,
        table: &str,
        key: &str,
        alias: Option<&str>,
        filter: Option<&Expr>,
        returning: Option<&SelectItems>,
    ) -> Result<ExecResult, SqlError> {
        let event = TriggerEvent::Delete;
        self.view_trigger_gate(table, key, TriggerTiming::InsteadOf, &event)?;
        if returning.is_some() {
            return Err(
                SqlError::new("unsupported-clause", "RETURNING on an INSTEAD OF view delete is not supported yet").in_statement("delete")
            );
        }
        let view = self.views.get(key).expect("view resolved").clone();
        let (names, rows) = self.view_output(&view)?;
        let placeholder = vec![Cell::Null; names.len()];
        self.trigger_frames.push(TriggerFrame { columns: names.clone(), new: None, old: Some(placeholder) });
        let resolved = (|| -> Result<(Option<ResolvedExpr>, ScopeShape), SqlError> {
            let relation = view_relation(alias.unwrap_or(table), &names, self.trigger_frames.last());
            let shape = relation.shape();
            let mut aggregates = Vec::new();
            let mut windows = Vec::new();
            let predicate = match filter {
                Some(filter) => Some(resolve_expr(filter, &relation, &[], &[], &mut windows, &mut aggregates, false, false)?),
                None => None,
            };
            Ok((predicate, shape))
        })();
        let (predicate, shape) = match resolved {
            Ok(resolved) => resolved,
            Err(error) => {
                self.trigger_frames.pop();
                return Err(error);
            }
        };
        let budget = Budget::new();
        let saved = self.last_insert_rowid;
        let mut outcome = Ok(());
        for row in rows {
            let row = row.get(..names.len()).map(|cells| cells.to_vec()).unwrap_or_else(|| vec![Cell::Null; names.len()]);
            match row_matches(&predicate, &row, self, &shape, &budget) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(error) => {
                    outcome = Err(error);
                    break;
                }
            }
            if let Some(frame) = self.trigger_frames.last_mut() {
                frame.old = Some(row);
            }
            outcome = self.fire_matching(key, &event, TriggerTiming::InsteadOf);
            if outcome.is_err() {
                break;
            }
        }
        self.trigger_frames.pop();
        self.last_insert_rowid = saved;
        outcome?;
        Ok(ExecResult { statement: "delete", columns: Vec::new(), rows: Vec::new(), affected: 0 })
    }

    fn run_statement(&mut self, statement: Statement) -> Result<ExecResult, SqlError> {
        // 子查询备忘与已解码行集都只在一条语句的 AST 存活期内有意义。
        self.subquery_cache.borrow_mut().clear();
        self.statement_scans.borrow_mut().clear();
        self.outer_read.set(false);
        let result = self.run_statement_dispatch(statement);
        // 释放本条语句解码的整表行,避免大表在下一条语句前仍驻留内存。
        self.statement_scans.borrow_mut().clear();
        result
    }

    fn run_statement_dispatch(&mut self, statement: Statement) -> Result<ExecResult, SqlError> {
        // 读语句不动索引;`UPDATE` / `DELETE` 在自己的执行分支里按实际改动
        // 的索引精确失效(见 `invalidate_indexes_after_update` 与
        // `patch_index_probes_after_delete`),其余会改行或改 schema 的语句
        // 在这里一刀切,让相关索引的缓存整体作废。
        if !matches!(statement, Statement::Select { .. } | Statement::Explain { .. }) {
            match &statement {
                Statement::Update { .. } | Statement::Delete { .. } => {}
                _ => match self.statement_table_key(&statement) {
                    Some(key) => self.invalidate_table_indexes(&key),
                    None => self.invalidate_all_indexes(),
                },
            }
        }
        let Some(schema) = leading_schema(&statement).map(str::to_owned)
        else {
            if let Some(index) = self.attached_target(&statement) {
                return self.run_attached(index, statement);
            }
            return self.run_statement_inner(statement);
        };
        let lower = schema.to_ascii_lowercase();
        if self.alias.as_deref().is_some_and(|alias| alias.eq_ignore_ascii_case(&schema)) {
            let mut statement = statement;
            clear_statement_schema(&mut statement, &schema);
            return self.run_statement_inner(statement);
        }
        if lower == "main" || lower == "temp" {
            let mut statement = statement;
            clear_statement_schema(&mut statement, &schema);
            let previous = std::mem::replace(&mut self.scope, if lower == "main" { TableScope::Main } else { TableScope::Temp });
            let result = self.run_statement_inner(statement);
            self.scope = previous;
            return result;
        }
        match self.attached_lookup.get(&lower).copied() {
            Some(index) => {
                let mut statement = statement;
                clear_statement_schema(&mut statement, &schema);
                strip_statement_sql(&mut statement, &schema);
                self.run_attached(index, statement)
            }
            None => Err(unknown_schema_error(&statement, &schema)),
        }
    }

    /// The position of the attached database an unqualified statement resolves
    /// to, following SQLite's search order after temp and main miss.
    fn attached_target(&self, statement: &Statement) -> Option<usize> {
        // A trigger body resolves unqualified names in the schema of the
        // trigger, never in an attached database.
        if !self.active_triggers.is_empty() {
            return None;
        }
        let (name, kind) = match statement {
            Statement::Insert { schema: None, table, .. }
            | Statement::Update { schema: None, table, .. }
            | Statement::Delete { schema: None, table, .. }
            | Statement::DropTable { schema: None, name: table, .. }
            | Statement::AlterTable { schema: None, table, .. } => (table, TargetKind::Table),
            Statement::DropIndex { schema: None, name, .. } => (name, TargetKind::Index),
            Statement::DropView { schema: None, name, .. } => (name, TargetKind::View),
            Statement::DropTrigger { schema: None, name, .. } => (name, TargetKind::Trigger),
            _ => return None,
        };
        let key = name.to_ascii_lowercase();
        if kind == TargetKind::Table && (self.temp_lookup.contains_key(&key) || self.table_lookup.contains_key(&key)) {
            return None;
        }
        self.attached.iter().position(|attached| match kind {
            TargetKind::Table => attached.engine.table_lookup.contains_key(&key),
            TargetKind::Index => attached.engine.indexes.contains_key(&key),
            TargetKind::View => attached.engine.views.contains_key(&key),
            TargetKind::Trigger => attached.engine.triggers.contains_key(&key),
        })
    }

    /// Runs one statement inside an attached database with the parameters and
    /// the connection-wide last rowid carried over.
    fn run_attached(&mut self, index: usize, statement: Statement) -> Result<ExecResult, SqlError> {
        let parameters = self.parameters.clone();
        let attached = &mut self.attached[index];
        attached.engine.parameters = parameters;
        let result = attached.engine.run_triggered(statement);
        let rowid = attached.engine.last_insert_rowid;
        attached.engine.parameters.clear();
        let result = result?;
        self.last_insert_rowid = rowid;
        self.attached[index].dirty = true;
        if self.transaction.is_none() {
            self.persist_attached(index)?;
        }
        Ok(result)
    }

    /// Flushes one attached database image back to its file.
    fn persist_attached(&mut self, index: usize) -> Result<(), SqlError> {
        if !self.attached.get(index).is_some_and(|attached| attached.dirty) {
            return Ok(());
        }
        if let Some(path) = self.attached[index].file.clone() {
            self.attached[index].engine.save(&path)?;
        }
        self.attached[index].dirty = false;
        Ok(())
    }

    /// Flushes every attached database that changed inside a transaction.
    fn persist_attached_databases(&mut self) -> Result<(), SqlError> {
        for index in 0..self.attached.len() {
            self.persist_attached(index)?;
        }
        Ok(())
    }

    /// Table name as reported by diagnostics under the active schema scope.
    fn scoped_name(&self, table: &str) -> String {
        match self.scope {
            TableScope::Default => table.to_string(),
            TableScope::Main => format!("main.{table}"),
            TableScope::Temp => format!("temp.{table}"),
        }
    }

    /// Error for a table that the active `main.` / `temp.` qualifier cannot
    /// find; an unqualified miss keeps the existing diagnostic.
    fn table_missing(&self, table: &str, statement: &'static str) -> SqlError {
        let message = match self.scope {
            TableScope::Default => format!("unknown table `{table}`"),
            _ => format!("no such table: {}", self.scoped_name(table)),
        };
        SqlError::new("unknown-table", message).in_statement(statement)
    }

    /// `ATTACH [DATABASE] file AS name`.
    fn attach_database(&mut self, file: &str, name: &str) -> Result<ExecResult, SqlError> {
        let key = name.to_ascii_lowercase();
        if key == "main" || key == "temp" || self.attached_lookup.contains_key(&key) {
            return Err(SqlError::new("database-in-use", format!("database {name} is already in use")).in_statement("attach"));
        }
        if self.attached.len() >= 10 {
            return Err(SqlError::new("too-many-databases", "too many attached databases - max 10").in_statement("attach"));
        }
        let memory = file.is_empty() || file.eq_ignore_ascii_case(":memory:");
        let (engine, path, display) = if memory {
            (Engine::new(Some(self.page_size()))?, None, String::new())
        }
        else {
            let requested = PathBuf::from(file);
            let path = if requested.is_absolute() {
                requested.clone()
            }
            else {
                std::env::current_dir().map(|cwd| cwd.join(&requested)).unwrap_or_else(|_| requested.clone())
            };
            let writer = if requested.is_dir() {
                return Err(SqlError::new("unable-to-open", format!("unable to open database: {file}")).in_statement("attach"));
            }
            else if requested.exists() && std::fs::metadata(&requested).map(|meta| meta.len() > 0).unwrap_or(false) {
                SqliteWriter::open(&requested).map_err(|_| SqlError::new("not-a-database", "file is not a database").in_statement("attach"))?
            }
            else {
                // SQLite creates the file at ATTACH time but leaves it empty
                // until the first change.
                if let Some(parent) = requested.parent() {
                    if !parent.as_os_str().is_empty() && !parent.exists() {
                        return Err(SqlError::new("unable-to-open", format!("unable to open database: {file}")).in_statement("attach"));
                    }
                }
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&requested)
                    .map_err(|_| SqlError::new("unable-to-open", format!("unable to open database: {file}")))?;
                SqliteWriter::create(self.page_size() as usize).map_err(|error| storage_failure(error))?
            };
            let engine = Engine::from_writer(writer)?;
            (engine, Some(path.clone()), path.to_string_lossy().into_owned())
        };
        let mut engine = Box::new(engine);
        engine.alias = Some(name.to_string());
        // 宿主函数在本会话注册一次后对之后 `ATTACH` 的库同样生效;表内只存放
        // `Arc` 句柄,克隆不会复制宿主实现。
        engine.host_functions = self.host_functions.clone();
        let index = self.attached.len();
        self.attached.push(AttachedDb { name: name.to_string(), file: path, display, engine, dirty: false });
        self.attached_lookup.insert(key, index);
        Ok(ExecResult { statement: "attach", columns: Vec::new(), rows: Vec::new(), affected: 0 })
    }

    /// `DETACH [DATABASE] name`.
    fn detach_database(&mut self, name: &str) -> Result<ExecResult, SqlError> {
        let key = name.to_ascii_lowercase();
        if key == "main" || key == "temp" {
            return Err(SqlError::new("cannot-detach", format!("cannot detach database {name}")).in_statement("detach"));
        }
        let Some(index) = self.attached_lookup.get(&key).copied()
        else {
            return Err(SqlError::new("unknown-database", format!("no such database: {name}")).in_statement("detach"));
        };
        self.persist_attached(index)?;
        self.attached.remove(index);
        self.attached_lookup.clear();
        for (position, attached) in self.attached.iter().enumerate() {
            self.attached_lookup.insert(attached.name.to_ascii_lowercase(), position);
        }
        Ok(ExecResult { statement: "detach", columns: Vec::new(), rows: Vec::new(), affected: 0 })
    }

    /// `VACUUM [schema] [INTO target]`.
    ///
    /// 重建与上游 `VACUUM` 同形:按 `sqlite_master` 的顺序重放表结构,复制行,
    /// 再重建索引、视图与触发器。新镜像没有 freelist 页,并保留页大小、文本
    /// 编码、`user_version` 与 `application_id`。不带 `INTO` 时新镜像替换
    /// 当前页集合;带 `INTO` 时写到目标文件,当前库保持不变。
    ///
    /// 与上游一致的边界:`VACUUM`/`VACUUM INTO` 在事务内拒绝,`VACUUM` 也不能
    /// 出现在触发器体里(解析期按上游报 `near "vacuum": syntax error`);
    /// `temp` 库的 `VACUUM` 是空操作,`VACUUM temp INTO` 写出空库;目标文件
    /// 已存在时报 `output file already exists`,目标父目录缺失或目标是目录时
    /// 报 `unable to open database`,目标不是文本时报 `non-text filename`。
    fn vacuum_statement(&mut self, into: Option<&Expr>) -> Result<ExecResult, SqlError> {
        if self.transaction.is_some() {
            return Err(SqlError::new("SQLITE_ERROR", "cannot VACUUM from within a transaction").in_statement("vacuum"));
        }
        if self.scope == TableScope::Temp {
            if let Some(expression) = into {
                let target = self.vacuum_target(expression)?;
                let empty = SqliteWriter::create(self.writer.page_size())
                    .map_err(|error| storage_failure(error).in_statement("vacuum"))?;
                self.write_vacuum_image(&target, empty.pages())?;
            }
            return Ok(ExecResult { statement: "vacuum", columns: Vec::new(), rows: Vec::new(), affected: 0 });
        }
        let image = self.vacuum_image()?;
        if let Some(expression) = into {
            let target = self.vacuum_target(expression)?;
            self.write_vacuum_image(&target, &image)?;
        }
        else {
            let synchronous = self.writer.synchronous();
            let mut writer =
                SqliteWriter::from_pages(image).map_err(|error| storage_failure(error).in_statement("vacuum"))?;
            writer.set_synchronous(synchronous);
            self.writer = writer;
            self.reload_catalog()?;
            self.index_probes.borrow_mut().clear();
            self.subquery_cache.borrow_mut().clear();
            self.write_epoch.set(self.write_epoch.get() + 1);
        }
        Ok(ExecResult { statement: "vacuum", columns: Vec::new(), rows: Vec::new(), affected: 0 })
    }

    /// 求出 `VACUUM INTO` 的文件名;SQLite 要求表达式的结果是文本。
    fn vacuum_target(&self, expression: &Expr) -> Result<String, SqlError> {
        match self.eval_trigger_expression(expression, "vacuum")? {
            Cell::Text(target) => Ok(target),
            _ => Err(SqlError::new("SQLITE_ERROR", "non-text filename").in_statement("vacuum")),
        }
    }

    /// 把重建镜像写到 `target`,`VACUUM INTO` 的真实落盘步骤。
    ///
    /// 空文件名与 `:memory:` 与上游一样不产生文件,命令仍然成功。目标已存在
    /// 时不覆盖;父目录缺失、目标是目录或写入失败都报 `unable to open database`。
    fn write_vacuum_image(&self, target: &str, image: &[u8]) -> Result<(), SqlError> {
        if target.is_empty() || target.eq_ignore_ascii_case(":memory:") {
            return Ok(());
        }
        let unable = || SqlError::new("SQLITE_CANTOPEN", format!("unable to open database: {target}")).in_statement("vacuum");
        let path = PathBuf::from(target);
        if path.is_dir() || path.parent().is_some_and(|parent| !parent.as_os_str().is_empty() && !parent.exists()) {
            return Err(unable());
        }
        if path.exists() {
            return Err(SqlError::new("SQLITE_ERROR", "output file already exists").in_statement("vacuum"));
        }
        std::fs::write(&path, image).map_err(|_| unable())
    }

    /// 生成 `VACUUM` 的重建镜像。
    ///
    /// 顺序与上游一致:先建表,再复制行(触发器尚未建立,复制不会改写数据),
    /// 最后建索引、视图与触发器。`sqlite_sequence` 的高水位先于行复制写入,
    /// 之后的显式 rowid 只会上抬、不会覆盖。`rowid` 表按 rowid 升序复制,
    /// 没有 `INTEGER PRIMARY KEY` 的表因此像上游一样重新编号。
    fn vacuum_image(&mut self) -> Result<Vec<u8>, SqlError> {
        self.flush_pending_materialization("vacuum")?;
        let page_size = self.writer.page_size();
        let encoding = match u32::from_be_bytes(
            self.writer
                .pages()
                .get(56..60)
                .and_then(|marker| marker.try_into().ok())
                .ok_or_else(|| SqlError::new("storage", "sqlite header too short").in_statement("vacuum"))?,
        ) {
            2 => TextEncoding::Utf16Le,
            3 => TextEncoding::Utf16Be,
            _ => TextEncoding::Utf8,
        };
        let user_version = self.writer.user_version();
        let application_id = self.writer.application_id();
        let limits = TableLimits::default();
        let objects = self.writer.schema_objects(limits).map_err(|error| storage_failure(error).in_statement("vacuum"))?;
        let writer = SqliteWriter::create_with_encoding(page_size, encoding)
            .map_err(|error| storage_failure(error).in_statement("vacuum"))?;
        let mut fresh = Engine::from_writer(writer)?;
        for object in &objects {
            if object.kind != SchemaObjectKind::Table || object.name.eq_ignore_ascii_case("sqlite_sequence") {
                continue;
            }
            if let Some(sql) = &object.sql {
                fresh.execute_one(sql)?;
            }
        }
        // `sqlite_sequence` 是保留表,不在主目录里,因此直接读存储层。它在
        // 源库里存在就照样出现在重建镜像里,高水位在建表后先写入。
        let sequence_present =
            objects.iter().any(|object| object.kind == SchemaObjectKind::Table && object.name.eq_ignore_ascii_case("sqlite_sequence"));
        if sequence_present {
            let fresh_present = fresh
                .writer
                .schema_objects(limits)
                .map_err(|error| storage_failure(error).in_statement("vacuum"))?
                .iter()
                .any(|object| object.kind == SchemaObjectKind::Table && object.name.eq_ignore_ascii_case("sqlite_sequence"));
            if !fresh_present {
                // 源库可能只留下一个空的 `sqlite_sequence`(最后一个 AUTOINCREMENT
                // 表被删后行没了、表还在),重建镜像要照样保留它。
                let columns: Vec<ColumnSpec> = ["name", "seq"]
                    .into_iter()
                    .map(|name| ColumnSpec {
                        name: name.to_string(),
                        declared_type: String::new(),
                        not_null: false,
                        default_value: None,
                        default_text: None,
                        primary_key: false,
                        unique: false,
                        autoincrement: false,
                        collate: None,
                    })
                    .collect();
                fresh
                    .writer
                    .create_table_with_primary_key_sql("sqlite_sequence", &columns, &[], Some("CREATE TABLE sqlite_sequence(name,seq)"))
                    .map_err(|error| storage_failure(error).in_statement("vacuum"))?;
            }
            let limits = TableLimits::default();
            for row in self
                .writer
                .table_rows("sqlite_sequence", limits)
                .map_err(|error| storage_failure(error).in_statement("vacuum"))?
            {
                let values = decode_record(&row.payload, encoding, RecordLimits { max_columns: 1024, max_payload_bytes: 16 * 1024 * 1024 })
                    .map_err(|error| storage_failure(error).in_statement("vacuum"))?;
                let cells: Vec<Cell> = values
                    .iter()
                    .map(|value| match value {
                        crate::RecordValue::Null => Cell::Null,
                        crate::RecordValue::Integer(value) => Cell::Integer(*value),
                        crate::RecordValue::Real(value) => Cell::Real(*value),
                        crate::RecordValue::Text(value) => Cell::Text(value.clone()),
                        crate::RecordValue::Blob(value) => Cell::Blob(value.to_vec()),
                    })
                    .collect();
                let slots: Vec<Option<crate::WriteValue>> = cells.iter().map(|cell| Some(cell_write_value(cell))).collect();
                fresh
                    .writer
                    .insert("sqlite_sequence", &slots)
                    .map_err(|error| storage_failure(error).in_statement("vacuum"))?;
            }
        }
        for def in self.tables.clone() {
            if def.temp || def.name.to_ascii_lowercase().starts_with("sqlite_") {
                continue;
            }
            for (_, cells) in self.live_rows(&def, "vacuum")? {
                let slots: Vec<Option<crate::WriteValue>> = cells.iter().map(|cell| Some(cell_write_value(cell))).collect();
                fresh.writer.insert(&def.name, &slots).map_err(|error| storage_failure(error).in_statement("vacuum"))?;
            }
        }
        fresh.writer.materialize_without_rowid().map_err(|error| storage_failure(error).in_statement("vacuum"))?;
        for kind in [SchemaObjectKind::Index, SchemaObjectKind::View, SchemaObjectKind::Trigger] {
            for object in &objects {
                if object.kind != kind {
                    continue;
                }
                if let Some(sql) = &object.sql {
                    fresh.execute_one(sql)?;
                }
            }
        }
        fresh.writer.materialize_indexes().map_err(|error| storage_failure(error).in_statement("vacuum"))?;
        fresh.writer.materialize_without_rowid().map_err(|error| storage_failure(error).in_statement("vacuum"))?;
        fresh.writer.set_user_version(user_version);
        fresh.writer.set_application_id(application_id);
        Ok(fresh.writer.pages().to_vec())
    }

    /// Executes `CREATE TABLE name AS SELECT ...`.
    ///
    /// The declared columns are derived from the query output: a bare column
    /// reference copies its source column's affinity as SQLite's standard
    /// type name (`INT`, `TEXT`, `REAL`, `NUM`), and every other expression
    /// gets no declared type. Duplicate output labels are numbered `:1`, `:2`.
    fn create_table_as(&mut self, name: String, if_not_exists: bool, temp: bool, query: &Statement) -> Result<ExecResult, SqlError> {
        let key = name.to_ascii_lowercase();
        check_reserved_object_name(&name, "create-table")?;
        let temp_target = temp || self.scope == TableScope::Temp;
        if temp_target {
            if self.temp_lookup.contains_key(&key) || self.temp_views.contains_key(&key) {
                if if_not_exists {
                    return Ok(ExecResult { statement: "create-table", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                }
                return Err(SqlError::new("table-exists", format!("table {name} already exists")).in_statement("create-table"));
            }
        }
        else {
            if self.indexes.contains_key(&key) {
                return Err(SqlError::new("index-exists", format!("there is already an index named {name}")).in_statement("create-table"));
            }
            if self.views.contains_key(&key) {
                return Err(SqlError::new("view-exists", format!("there is already a view named {name}")).in_statement("create-table"));
            }
            if self.table_lookup.contains_key(&key) {
                if if_not_exists {
                    return Ok(ExecResult { statement: "create-table", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                }
                return Err(SqlError::new("table-exists", format!("table {name} already exists")).in_statement("create-table"));
            }
        }
        let budget = Budget::new();
        let selected = self.select_statement(query, &[], &budget, None)?;
        let labels = unique_output_names(&selected.columns);
        let mut types: Vec<String> = self.ctas_statement_columns(query, &BTreeMap::new())?.into_iter().map(|(_, declared)| declared).collect();
        types.resize(labels.len(), String::new());
        types.truncate(labels.len());
        let columns: Vec<ColumnSpec> = labels
            .iter()
            .zip(types)
            .map(|(name, declared_type)| ColumnSpec {
                name: name.clone(),
                declared_type,
                not_null: false,
                default_value: None,
                default_text: None,
                primary_key: false,
                unique: false,
                autoincrement: false,
                collate: None,
            })
            .collect();
        let sql = ctas_schema_sql(&name, &columns);
        let mut def = Self::catalog_table(name.clone(), columns.clone(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), false, false)?;
        if temp_target {
            def.temp = true;
            let rootpage = match self.temp_free_pages.pop_first() {
                Some(page) => page,
                None => {
                    let page = self.temp_next_page;
                    self.temp_next_page += 1;
                    page
                }
            };
            let master_rowid = self.temp_next_rowid;
            self.temp_next_rowid += 1;
            let mut rows = BTreeMap::new();
            for row in &selected.rows {
                let slots: Vec<Option<crate::WriteValue>> = row.iter().map(|cell| Some(cell_write_value(cell))).collect();
                temp_insert_row(&def, &mut rows, &slots, &[])?;
            }
            let position = self.temp_tables.len();
            self.temp_lookup.insert(key, position);
            self.temp_tables.push(TempTable { def, sql, rootpage, master_rowid, rows });
            self.temp_created = true;
            return Ok(ExecResult { statement: "create-table", columns: Vec::new(), rows: Vec::new(), affected: 0 });
        }
        self.writer
            .create_table_with_primary_key_sql(&name, &columns, &[], Some(&sql))
            .map_err(|error| storage_failure(error).in_statement("create-table"))?;
        if !selected.rows.is_empty() {
            self.writer.begin().map_err(|error| storage_failure(error).in_statement("create-table"))?;
            for row in &selected.rows {
                let slots: Vec<Option<crate::WriteValue>> = row.iter().map(|cell| Some(cell_write_value(cell))).collect();
                if let Err(error) = self.writer.insert(&name, &slots) {
                    let _ = self.writer.rollback();
                    return Err(storage_failure(error).in_statement("create-table"));
                }
            }
            self.writer.commit().map_err(|error| storage_failure(error).in_statement("create-table"))?;
        }
        let position = self.tables.len();
        self.table_lookup.insert(key, position);
        self.tables.push(def);
        Ok(ExecResult { statement: "create-table", columns: Vec::new(), rows: Vec::new(), affected: 0 })
    }

    /// Output column labels and copied declared types of one query, used to
    /// build the schema of `CREATE TABLE ... AS SELECT`.
    fn ctas_statement_columns(
        &self,
        statement: &Statement,
        inherited: &BTreeMap<String, Vec<(String, String)>>,
    ) -> Result<Vec<(String, String)>, SqlError> {
        let Statement::Select { with, first, .. } = statement
        else {
            return Ok(Vec::new());
        };
        let mut ctes = inherited.clone();
        for entry in with {
            let mut columns = self.ctas_statement_columns(&entry.query, &ctes)?;
            if let Some(names) = &entry.columns {
                if names.len() == columns.len() {
                    for (column, name) in columns.iter_mut().zip(names) {
                        column.0 = name.clone();
                    }
                }
            }
            ctes.insert(entry.name.to_ascii_lowercase(), columns);
        }
        let mut sources: Vec<(String, Vec<(String, String)>)> = Vec::with_capacity(first.from.len());
        for table in &first.from {
            let label = table.alias.clone().unwrap_or_else(|| table.name.clone());
            sources.push((label, self.ctas_table_columns(table, &ctes)?));
        }
        let mut output = Vec::new();
        match &first.items {
            SelectItems::Named(expressions) => {
                for expression in expressions {
                    let label = ctas_expression_label(expression, &sources);
                    let declared = ctas_expression_type(expression, &sources);
                    output.push((label, declared));
                }
            }
            SelectItems::CountAll => output.push(("count(*)".to_string(), String::new())),
            SelectItems::Star => {
                for (_, columns) in &sources {
                    output.extend(columns.iter().cloned());
                }
            }
        }
        Ok(output)
    }

    /// Columns one `FROM` item exposes to the output of a CTAS query.
    fn ctas_table_columns(&self, table: &TableRef, ctes: &BTreeMap<String, Vec<(String, String)>>) -> Result<Vec<(String, String)>, SqlError> {
        if let Some(query) = &table.query {
            return self.ctas_statement_columns(query, ctes);
        }
        let name = table.name.to_ascii_lowercase();
        if let Some(columns) = ctes.get(&name) {
            return Ok(columns.clone());
        }
        if let Some(def) = self.tables.iter().find(|def| def.name.eq_ignore_ascii_case(&table.name)) {
            return Ok(def.columns.iter().map(|column| (column.name.clone(), standard_affinity_name(&column.declared_type))).collect());
        }
        if let Some(temp) = self.temp_tables.iter().find(|temp| temp.def.name.eq_ignore_ascii_case(&table.name)) {
            return Ok(temp.def.columns.iter().map(|column| (column.name.clone(), standard_affinity_name(&column.declared_type))).collect());
        }
        if let Some(view) = self.views.get(&name) {
            let mut columns = self.ctas_statement_columns(&view.query, ctes)?;
            if let Some(names) = &view.columns {
                if names.len() == columns.len() {
                    for (column, name) in columns.iter_mut().zip(names) {
                        column.0 = name.clone();
                    }
                }
            }
            return Ok(columns);
        }
        Ok(Vec::new())
    }

    fn run_statement_inner(&mut self, statement: Statement) -> Result<ExecResult, SqlError> {
        match statement {
            Statement::Explain { query_plan, statement } => self.explain_statement(query_plan, &statement),
            Statement::Transaction { action } => self.transaction(action),
            Statement::Attach { file, name } => self.attach_database(&file, &name),
            Statement::Detach { name } => self.detach_database(&name),
            Statement::Vacuum { into, .. } => self.vacuum_statement(into.as_ref()),
            Statement::CreateTable {
                schema: _,
                name,
                if_not_exists,
                columns,
                primary_key,
                table_constraints,
                checks,
                foreign_keys,
                strict,
                without_rowid,
                temp,
                query,
                sql,
            } => {
                if let Some(query) = query {
                    return self.create_table_as(name, if_not_exists, temp, &query);
                }
                let key = name.to_ascii_lowercase();
                // A WITHOUT ROWID table's writer call needs the key columns
                // whether they were declared inline or as a table constraint.
                let without_rowid_keys: Vec<String> = if primary_key.is_empty() {
                    columns.iter().filter(|column| column.primary_key).map(|column| column.name.clone()).collect()
                }
                else {
                    primary_key.clone()
                };
                check_reserved_object_name(&name, "create-table")?;
                if temp {
                    // The temp schema owns a separate namespace: a main object
                    // with the same name can coexist and temp wins in name
                    // resolution.
                    if self.temp_lookup.contains_key(&key) {
                        if if_not_exists {
                            return Ok(ExecResult { statement: "create-table", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                        }
                        return Err(SqlError::new("table-exists", format!("table {name} already exists")).in_statement("create-table"));
                    }
                    let mut def =
                        Self::catalog_table(name, columns, primary_key, table_constraints, checks, foreign_keys, strict, without_rowid)?;
                    def.temp = true;
                    // SQLite reuses a freed temp page when one is available
                    // and otherwise extends the temp database by one page.
                    let rootpage = match self.temp_free_pages.pop_first() {
                        Some(page) => page,
                        None => {
                            let page = self.temp_next_page;
                            self.temp_next_page += 1;
                            page
                        }
                    };
                    let master_rowid = self.temp_next_rowid;
                    self.temp_next_rowid += 1;
                    let position = self.temp_tables.len();
                    self.temp_lookup.insert(key, position);
                    self.temp_tables.push(TempTable { def, sql, rootpage, master_rowid, rows: BTreeMap::new() });
                    self.temp_created = true;
                    return Ok(ExecResult { statement: "create-table", columns: Vec::new(), rows: Vec::new(), affected: 1 });
                }
                if self.indexes.contains_key(&key) {
                    return Err(SqlError::new("index-exists", format!("there is already an index named {name}")).in_statement("create-table"));
                }
                if self.views.contains_key(&key) {
                    return Err(SqlError::new("view-exists", format!("there is already a view named {name}")).in_statement("create-table"));
                }
                if self.table_lookup.contains_key(&key) {
                    if if_not_exists {
                        return Ok(ExecResult { statement: "create-table", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                    }
                    return Err(SqlError::new("table-exists", format!("table {name} already exists")).in_statement("create-table"));
                }
                let def = Self::catalog_table(
                    name.clone(),
                    columns.clone(),
                    primary_key.clone(),
                    table_constraints.clone(),
                    checks.clone(),
                    foreign_keys.clone(),
                    strict,
                    without_rowid,
                )?;
                if without_rowid {
                    self.writer
                        .create_table_without_rowid_sql(&name, &columns, &without_rowid_keys, Some(&sql))
                        .map_err(|error| storage_failure(error).in_statement("create-table"))?;
                }
                else {
                    self.writer
                        .create_table_with_primary_key_sql(&name, &columns, &primary_key, Some(&sql))
                        .map_err(|error| storage_failure(error).in_statement("create-table"))?;
                }
                // Appending keeps the catalog in creation order, which is the
                // order `sqlite_master` reports.
                let position = self.tables.len();
                self.table_lookup.insert(key, position);
                self.tables.push(def);
                Ok(ExecResult { statement: "create-table", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::CreateView { schema, name, if_not_exists, columns, query, temp, sql } => {
                let key = name.to_ascii_lowercase();
                check_reserved_object_name(&name, "create-view")?;
                // `CREATE TEMP VIEW` and `CREATE VIEW temp.v` both target the
                // connection-owned schema, whose objects shadow main ones.
                let temp_target =
                    temp || self.scope == TableScope::Temp || schema.as_deref().is_some_and(|qualifier| qualifier.eq_ignore_ascii_case("temp"));
                if temp_target {
                    if self.temp_lookup.contains_key(&key) || self.temp_views.contains_key(&key) {
                        if if_not_exists {
                            return Ok(ExecResult { statement: "create-view", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                        }
                        return Err(SqlError::new("table-exists", format!("table {name} already exists")).in_statement("create-view"));
                    }
                    // `sqlite_temp_master` stores the statement without `TEMP`
                    // and without a `temp.` qualifier.
                    let stored = schema.as_deref().map(|qualifier| strip_schema_qualifiers(&sql, qualifier)).unwrap_or(sql);
                    let master_rowid = self.temp_next_rowid;
                    self.temp_next_rowid += 1;
                    self.temp_views.insert(key, TempView { def: ViewDef { name, columns, query: *query }, sql: stored, master_rowid });
                    self.temp_created = true;
                    return Ok(ExecResult { statement: "create-view", columns: Vec::new(), rows: Vec::new(), affected: 1 });
                }
                if self.indexes.contains_key(&key) {
                    return Err(SqlError::new("index-exists", format!("there is already an index named {name}")).in_statement("create-view"));
                }
                if self.views.contains_key(&key) {
                    if if_not_exists {
                        return Ok(ExecResult { statement: "create-view", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                    }
                    return Err(SqlError::new("view-exists", format!("view {name} already exists")).in_statement("create-view"));
                }
                if self.tables.iter().any(|def| def.name.eq_ignore_ascii_case(&name)) {
                    if if_not_exists {
                        return Ok(ExecResult { statement: "create-view", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                    }
                    return Err(SqlError::new("table-exists", format!("table {name} already exists")).in_statement("create-view"));
                }
                self.writer
                    .create_view(&name, &sql)
                    .map_err(|error| storage_failure(error).in_statement("create-view"))?;
                self.views.insert(key, ViewDef { name, columns, query: *query });
                Ok(ExecResult { statement: "create-view", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::DropView { schema, name, if_exists } => {
                let key = name.to_ascii_lowercase();
                let temp_target =
                    self.scope == TableScope::Temp || schema.as_deref().is_some_and(|qualifier| qualifier.eq_ignore_ascii_case("temp"));
                // Unqualified `DROP VIEW` searches temp before main, so a temp
                // view disappears while its main namesake survives.
                if schema.is_none() || temp_target {
                    if self.temp_views.remove(&key).is_some() {
                        self.triggers.retain(|_, trigger| !(trigger.temp && trigger.table == key));
                        return Ok(ExecResult { statement: "drop-view", columns: Vec::new(), rows: Vec::new(), affected: 1 });
                    }
                    if temp_target {
                        if if_exists {
                            return Ok(ExecResult { statement: "drop-view", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                        }
                        return Err(SqlError::new("unknown-view", format!("no such view: {name}")).in_statement("drop-view"));
                    }
                }
                if self.tables.iter().any(|def| def.name.eq_ignore_ascii_case(&name)) {
                    return Err(SqlError::new("wrong-object", format!("use DROP TABLE to delete table {name}")).in_statement("drop-view"));
                }
                // An index is not a view, so SQLite reports it as missing
                // rather than as the wrong object kind.
                if self.indexes.contains_key(&key) {
                    if if_exists {
                        return Ok(ExecResult { statement: "drop-view", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                    }
                    return Err(SqlError::new("unknown-view", format!("no such view: {name}")).in_statement("drop-view"));
                }
                if self.views.remove(&key).is_none() {
                    if if_exists {
                        return Ok(ExecResult { statement: "drop-view", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                    }
                    return Err(SqlError::new("unknown-view", format!("no such view: {name}")).in_statement("drop-view"));
                }
                self.writer.drop_view(&name).map_err(|error| storage_failure(error).in_statement("drop-view"))?;
                self.triggers.retain(|_, trigger| trigger.table != key);
                Ok(ExecResult { statement: "drop-view", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::CreateTrigger { schema: _, name, if_not_exists, timing, event, table_schema: _, table, when, body, sql } => {
                let key = name.to_ascii_lowercase();
                check_reserved_object_name(&name, "create-trigger")?;
                if self.triggers.contains_key(&key) {
                    if if_not_exists {
                        return Ok(ExecResult { statement: "create-trigger", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                    }
                    return Err(SqlError::new("trigger-exists", format!("trigger {name} already exists")).in_statement("create-trigger"));
                }
                let table_key = table.to_ascii_lowercase();
                let temp_target = if self.scope == TableScope::Main { None } else { self.temp_lookup.get(&table_key).copied() };
                let main_target = if temp_target.is_some() { None } else { self.table_lookup.get(&table_key).copied() };
                let view_target = if temp_target.is_some() || main_target.is_some() { false } else { self.views.contains_key(&table_key) };
                if temp_target.is_none() && main_target.is_none() && !view_target {
                    let qualifier = match self.scope {
                        TableScope::Temp => "temp",
                        _ => "main",
                    };
                    return Err(SqlError::new("unknown-table", format!("no such table: {qualifier}.{table}")).in_statement("create-trigger"));
                }
                let timing_text = match timing {
                    TriggerTiming::Before => "BEFORE",
                    TriggerTiming::After => "AFTER",
                    TriggerTiming::InsteadOf => "INSTEAD OF",
                };
                if view_target && !matches!(timing, TriggerTiming::InsteadOf) {
                    return Err(SqlError::new("trigger-target", format!("cannot create {timing_text} trigger on view: {table}"))
                        .in_statement("create-trigger"));
                }
                if !view_target && matches!(timing, TriggerTiming::InsteadOf) {
                    return Err(SqlError::new("trigger-target", format!("cannot create INSTEAD OF trigger on table: {table}"))
                        .in_statement("create-trigger"));
                }
                let temp = self.scope == TableScope::Temp || temp_target.is_some();
                let order = self.trigger_sequence;
                self.trigger_sequence += 1;
                let master_rowid = if temp {
                    let rowid = self.temp_next_rowid;
                    self.temp_next_rowid += 1;
                    self.temp_created = true;
                    rowid
                }
                else {
                    self.writer
                        .create_trigger(&name, &table, &sql)
                        .map_err(|error| storage_failure(error).in_statement("create-trigger"))?;
                    0
                };
                self.triggers.insert(key, TriggerDef { name, table: table_key, timing, event, when, body, order, temp, sql, master_rowid });
                Ok(ExecResult { statement: "create-trigger", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::DropTrigger { schema: _, name, if_exists } => {
                let key = name.to_ascii_lowercase();
                let Some(trigger) = self.triggers.remove(&key)
                else {
                    if if_exists {
                        return Ok(ExecResult { statement: "drop-trigger", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                    }
                    return Err(SqlError::new("unknown-trigger", format!("no such trigger: {name}")).in_statement("drop-trigger"));
                };
                if !trigger.temp {
                    self.writer
                        .drop_trigger(&trigger.name)
                        .map_err(|error| storage_failure(error).in_statement("drop-trigger"))?;
                }
                Ok(ExecResult { statement: "drop-trigger", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::Insert { schema: _, table, columns, rows, default_values, query, replace, ignore, conflict, returning } => {
                let key = table.to_ascii_lowercase();
                check_readonly_table(&table, "insert")?;
                // A `SELECT` source is evaluated before any row is written, so
                // `INSERT INTO t SELECT ... FROM t` reads the table's original
                // contents, exactly like SQLite.
                let selected_rows: Option<Vec<Vec<Cell>>> = match &query {
                    Some(query) => {
                        let budget = Budget::new();
                        Some(self.select_statement(query, &[], &budget, None)?.rows)
                    }
                    None => None,
                };
                if self.views.contains_key(&key) {
                    let literal_rows: Vec<Vec<InsertValue>> = match &selected_rows {
                        Some(rows) => rows.iter().map(|row| row.iter().map(cell_insert_value).collect()).collect(),
                        None => rows.clone(),
                    };
                    return self.run_view_insert(&table, &key, columns.as_deref(), &literal_rows, default_values);
                }
                let temp_position = if self.scope == TableScope::Main { None } else { self.temp_lookup.get(&key).copied() };
                let main_position = if self.scope == TableScope::Temp {
                    None
                }
                else {
                    match temp_position {
                        Some(_) => None,
                        None => Some(*self.table_lookup.get(&key).ok_or_else(|| self.table_missing(&table, "insert"))?),
                    }
                };
                // Temp tables are mutated in memory, so their definition is
                // cloned: the loop below needs the catalog while it borrows
                // the temp row map mutably.
                let def_storage = match temp_position {
                    Some(position) => self.temp_tables[position].def.clone(),
                    None => self.tables[main_position.expect("main table resolved")].clone(),
                };
                let def = &def_storage;
                let returning_shape = TargetShape::new(def, &table, None);
                let returning = match &returning {
                    Some(items) => Some(returning_shape.plan(def, items, "insert")?),
                    None => None,
                };
                let upsert = match &conflict {
                    Some(clause) => {
                        if replace {
                            return Err(SqlError::new("syntax", "ON CONFLICT clause is not allowed with REPLACE").in_statement("insert"));
                        }
                        Some(self.prepare_upsert(def, &key, &table, clause)?)
                    }
                    None => None,
                };
                let fast_upsert = upsert.as_ref().is_some_and(|plan| self.upsert_rowid_fast_path(def, &key, plan, temp_position));
                let mut upsert_live = if upsert.is_some() && !fast_upsert { self.live_rows(def, "insert")? } else { Vec::new() };
                let mut upsert_live_loaded = upsert.is_some() && !fast_upsert;
                let mut slots_by_row = Vec::with_capacity(rows.len().max(1));
                if default_values {
                    // `DEFAULT VALUES` leaves every slot unset so the writer fills
                    // it from the column default (and reports NOT NULL violations).
                    slots_by_row.push(vec![None; def.columns.len()]);
                }
                for values in &rows {
                    let mut resolved: Vec<crate::WriteValue> = Vec::with_capacity(values.len());
                    for value in values {
                        let cell = match value {
                            InsertValue::Literal(literal) => literal_cell(literal),
                            InsertValue::Parameter { index, .. } => self.parameters.get(*index).cloned().unwrap_or(Cell::Null),
                            InsertValue::Expression(expression) => self.eval_trigger_expression(expression, "insert")?,
                        };
                        resolved.push(cell_write_value(&cell));
                    }
                    slots_by_row.push(self_contained_arity(def, &table, columns.as_deref(), &resolved)?);
                }
                if let Some(rows) = &selected_rows {
                    for row in rows {
                        let resolved: Vec<crate::WriteValue> = row.iter().map(cell_write_value).collect();
                        slots_by_row.push(select_arity(def, &table, columns.as_deref(), &resolved)?);
                    }
                }
                // SQLite applies column affinity to every supplied value before
                // the row reaches conflict detection: a `REAL` bound to an
                // `INTEGER`-affinity column with no fractional part becomes an
                // integer. Normalize the slots here so the upsert rowid probe and
                // the scanning conflict path compare the same storage class the
                // writer will store, instead of missing the conflict and surfacing
                // a spurious `UNIQUE` error.
                for slots in &mut slots_by_row {
                    for (position, slot) in slots.iter_mut().enumerate() {
                        if let Some(value) = slot.take() {
                            *slot = Some(crate::apply_write_affinity(&def.columns[position].declared_type, value));
                        }
                    }
                }
                // A `STRICT` table rejects a value whose post-affinity storage
                // class differs from the declared type, before any row is
                // written. `INSERT OR IGNORE` skips the offending row instead.
                if ignore {
                    slots_by_row.retain(|slots| {
                        slots.iter().enumerate().all(|(position, slot)| match slot {
                            Some(value) => strict_value_violation(def, position, value, "insert").is_none(),
                            None => true,
                        })
                    });
                }
                else {
                    for slots in &slots_by_row {
                        for (position, slot) in slots.iter().enumerate() {
                            if let Some(value) = slot {
                                if let Some(error) = strict_value_violation(def, position, value, "insert") {
                                    return Err(error);
                                }
                            }
                        }
                    }
                }
                // One row at a time inside a transaction: a failing `CHECK`
                // must undo every row of a multi-row statement, as SQLite does.
                // Temp tables snapshot their row map instead of opening one.
                let temp_snapshot = temp_position.map(|position| self.temp_tables[position].rows.clone());
                if temp_position.is_none() {
                    self.writer.begin().map_err(|error| storage_failure(error).in_statement("insert"))?;
                }
                let temp_keys = if temp_position.is_some() { self.unique_keys(def, &key) } else { Vec::new() };
                let check_shape = ScopeShape { columns: Vec::new() };
                let check_budget = Budget::new();
                let queue_triggers = self.table_has_triggers(&key);
                let trigger_columns = trigger_frame_columns(def);
                let recursive_triggers = self.recursive_triggers;
                let replace_old_rows = if replace { self.live_rows(def, "insert")? } else { Vec::new() };
                let mut replace_deleted: Vec<(i64, Vec<Cell>)> = Vec::new();
                let mut replace_index_needed = false;
                let mut replace_without_needed = false;
                let mut rowids = Vec::with_capacity(slots_by_row.len());
                let mut last_written: Option<i64> = None;
                let mut affected = 0usize;
                let mut returned: Vec<Vec<Cell>> = Vec::new();
                let mut failure = None;
                for slots in &slots_by_row {
                    // A `BEFORE INSERT` trigger sees the row before it is
                    // written: defaults are already applied and an unassigned
                    // rowid is `-1`, exactly like SQLite.
                    if queue_triggers && !replace {
                        let before_rowid = def
                            .rowid_alias
                            .and_then(|alias| slots.get(alias).and_then(|slot| slot.as_ref()))
                            .and_then(|value| match value {
                                crate::WriteValue::Integer(value) => Some(*value),
                                _ => None,
                            })
                            .unwrap_or(-1);
                        let cells = check_row(def, slots, before_rowid);
                        self.pending_events.push(PendingTriggerEvent {
                            table: key.clone(),
                            timing: TriggerTiming::Before,
                            event: TriggerEvent::Insert,
                            columns: trigger_columns.clone(),
                            new: Some(cells),
                            old: None,
                        });
                    }
                    let outcome = if replace {
                        match self.apply_replace(
                            def,
                            &key,
                            &table,
                            slots,
                            temp_position,
                            recursive_triggers,
                            queue_triggers,
                            &trigger_columns,
                            &mut replace_deleted,
                        ) {
                            Ok(rowid) => UpsertOutcome::Inserted(rowid),
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        }
                    }
                    else {
                        match &upsert {
                            Some(plan) => {
                                let fast = if fast_upsert {
                                    Some(self.apply_upsert_rowid(
                                        def,
                                        &key,
                                        &table,
                                        plan,
                                        slots,
                                        returning.as_ref(),
                                        &returning_shape.scope,
                                        &check_budget,
                                    ))
                                }
                                else {
                                    None
                                };
                                let outcome = match fast {
                                    Some(Ok(Some(outcome))) => Ok(outcome),
                                    Some(Err(error)) => Err(error),
                                    Some(Ok(None)) | None => {
                                        if !upsert_live_loaded {
                                            upsert_live = match self.live_rows(def, "insert") {
                                                Ok(rows) => rows,
                                                Err(error) => {
                                                    failure = Some(error);
                                                    break;
                                                }
                                            };
                                            upsert_live_loaded = true;
                                        }
                                        self.apply_upsert(
                                            def,
                                            &key,
                                            &table,
                                            plan,
                                            slots,
                                            temp_position,
                                            &mut upsert_live,
                                            returning.as_ref(),
                                            &returning_shape.scope,
                                            &check_budget,
                                        )
                                    }
                                };
                                match outcome {
                                    Ok(outcome) => outcome,
                                    Err(error) => {
                                        failure = Some(error);
                                        break;
                                    }
                                }
                            }
                            None => {
                                let inserted = if let Some(position) = temp_position {
                                    let table = &mut self.temp_tables[position];
                                    temp_insert_row(def, &mut table.rows, slots, &temp_keys)
                                }
                                else {
                                    self.writer
                                        .insert(&table, slots)
                                        .map_err(|error| storage_failure(error).in_statement("insert"))
                                };
                                match inserted {
                                    Ok(rowid) => UpsertOutcome::Inserted(rowid),
                                    Err(error) if ignore && ignoreable_insert_error(&error) => continue,
                                    Err(error) => {
                                        failure = Some(error);
                                        break;
                                    }
                                }
                            }
                        }
                    };
                    let rowid = match outcome {
                        UpsertOutcome::Skipped => continue,
                        UpsertOutcome::Inserted(rowid) => {
                            affected += 1;
                            rowid
                        }
                        UpsertOutcome::Updated { rowid, rows } => {
                            // `plan_update` already validated the rewritten row
                            // and evaluated `RETURNING` against it.
                            affected += 1;
                            last_written = Some(rowid);
                            returned.extend(rows);
                            continue;
                        }
                    };
                    let cells = check_row(def, slots, rowid);
                    let ctx = EvalContext {
                        engine: self,
                        outer: &[],
                        current: &check_shape,
                        aggregates: &[],
                        windows: &[],
                        window_row: 0,
                        budget: &check_budget,
                        ctes: None,
                    };
                    for check in &def.checks {
                        let value = match check.expr.eval(&cells, &ctx) {
                            Ok(value) => value,
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        };
                        // SQLite only fails a check whose result is false; a
                        // NULL result passes.
                        if truthiness(&value) == Some(false) {
                            let mut error = SqlError::new(
                                "check-constraint",
                                format!("CHECK constraint failed: {}", check.name.clone().unwrap_or_else(|| check.text.clone())),
                            )
                            .in_statement("insert");
                            error.clause = Some("CHECK".to_string());
                            failure = Some(error);
                            break;
                        }
                    }
                    if failure.is_some() {
                        break;
                    }
                    // The row passed the writer's NOT NULL / UNIQUE / PRIMARY
                    // KEY checks and this statement's CHECK constraints, so
                    // SQLite would have recorded its rowid already. A later
                    // failure rolls the row back but does not undo the record.
                    last_written = Some(rowid);
                    if let Some(plan) = &returning {
                        match plan.eval(&cells, &ctx) {
                            Ok(row) => returned.push(row),
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        }
                    }
                    rowids.push(rowid);
                    if queue_triggers {
                        self.pending_events.push(PendingTriggerEvent {
                            table: key.clone(),
                            timing: TriggerTiming::After,
                            event: TriggerEvent::Insert,
                            columns: trigger_columns.clone(),
                            new: Some(cells.clone()),
                            old: None,
                        });
                    }
                }
                // Referential checks run after every row is written and before
                // the transaction commits: a failing statement leaves no row
                // behind, and a self-reference may point at a later row of the
                // same statement.
                if failure.is_none() && self.foreign_keys && temp_position.is_none() && !replace_deleted.is_empty() {
                    let changes: Vec<(Vec<Cell>, Option<Vec<Cell>>)> = replace_deleted.iter().map(|(_, cells)| (cells.clone(), None)).collect();
                    match self.apply_foreign_key_actions(def, &changes, "insert", 0) {
                        Ok((index_needed, without_needed)) => {
                            replace_index_needed |= index_needed;
                            replace_without_needed |= without_needed;
                        }
                        Err(error) => failure = Some(error),
                    }
                    if failure.is_none() {
                        let removed: Vec<i64> = replace_deleted.iter().map(|(rowid, _)| *rowid).collect();
                        if let Err(error) = self.check_delete_foreign_keys(def, &replace_old_rows, &removed, "insert") {
                            failure = Some(error);
                        }
                    }
                }
                if failure.is_none() && self.foreign_keys {
                    if let Err(error) = self.check_insert_foreign_keys(def, &rowids, "insert") {
                        failure = Some(error);
                    }
                }
                if let Some(rowid) = last_written {
                    if !def.without_rowid {
                        self.last_insert_rowid = rowid;
                    }
                }
                if let Some(error) = failure {
                    match (temp_position, temp_snapshot) {
                        (Some(position), Some(snapshot)) => self.temp_tables[position].rows = snapshot,
                        _ => {
                            let _ = self.writer.rollback();
                        }
                    }
                    return Err(error);
                }
                if temp_position.is_none() {
                    if let Err(error) = self.refresh_managed_indexes(&key, def, "insert") {
                        let _ = self.writer.rollback();
                        return Err(error);
                    }
                    self.writer.commit().map_err(|error| storage_failure(error).in_statement("insert"))?;
                }
                // Explicit indexes and the automatic ones built for any
                // `PRIMARY KEY` / `UNIQUE` constraint live only as key sets
                // until they are materialized into b-tree pages, so a write to
                // an indexed table has to rebuild them before the image is read.
                if temp_position.is_none() && (self.has_indexes(def, &key) || replace_index_needed) {
                    self.materialize_indexes_after_write("insert")?;
                }
                if temp_position.is_none() && (def.without_rowid || replace_without_needed) {
                    self.materialize_without_rowid_after_write("insert")?;
                }
                if let Some(plan) = &returning {
                    return Ok(ExecResult { statement: "insert", columns: plan.columns.clone(), rows: returned, affected });
                }
                if default_values {
                    // `DEFAULT VALUES` reports neither columns nor rows, like
                    // SQLite; the stored row is written the same way.
                    return Ok(ExecResult { statement: "insert", columns: Vec::new(), rows: Vec::new(), affected });
                }
                if def.without_rowid {
                    // A WITHOUT ROWID insert has no rowid to report.
                    return Ok(ExecResult { statement: "insert", columns: Vec::new(), rows: Vec::new(), affected });
                }
                let rows = rowids.into_iter().map(|rowid| vec![Cell::Integer(rowid)]).collect();
                Ok(ExecResult { statement: "insert", columns: vec!["rowid".to_string()], rows, affected })
            }
            Statement::CreateIndex { schema: _, name, table, if_not_exists, unique, columns, predicate, sql } => {
                let table_key = table.to_ascii_lowercase();
                if is_schema_table(&table) {
                    return Err(
                        SqlError::new("readonly-system-table", format!("table {table} may not be indexed")).in_statement("create-index")
                    );
                }
                if self.views.contains_key(&table_key) {
                    return Err(SqlError::new("readonly-view", "views may not be indexed").in_statement("create-index"));
                }
                check_reserved_object_name(&name, "create-index")?;
                let position = *self
                    .table_lookup
                    .get(&table_key)
                    .ok_or_else(|| SqlError::new("unknown-table", format!("unknown table `{table}`")).in_statement("create-index"))?;
                let def_storage = self.tables[position].clone();
                let def = &def_storage;
                let key = name.to_ascii_lowercase();
                if self.table_lookup.contains_key(&key) {
                    return Err(SqlError::new("table-exists", format!("there is already a table named `{name}`")).in_statement("create-index"));
                }
                if self.indexes.contains_key(&key) {
                    if if_not_exists {
                        return Ok(ExecResult { statement: "create-index", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                    }
                    return Err(SqlError::new("index-exists", format!("index `{name}` already exists")).in_statement("create-index"));
                }
                if self.views.contains_key(&key) {
                    return Err(
                        SqlError::new("index-exists", format!("there is already an object named `{name}`")).in_statement("create-index")
                    );
                }
                // Expression keys and a `WHERE` predicate make the key values
                // row-dependent, so the engine computes them and hands the exact
                // entries to the writer.
                let managed = predicate.is_some() || columns.iter().any(|column| column.expr.is_some() || column.collate.is_some());
                if managed {
                    let shape = TargetShape::new(def, &table, None);
                    let mut terms = Vec::with_capacity(columns.len());
                    let mut collations = Vec::with_capacity(columns.len());
                    for column in &columns {
                        let resolved = match &column.expr {
                            Some(expr) => shape.resolve(expr, "create-index")?,
                            None => {
                                let position =
                                    def.columns.iter().position(|existing| existing.name.eq_ignore_ascii_case(&column.name)).ok_or_else(
                                        || {
                                            SqlError::new("unknown-column", format!("unknown column `{}` in table `{table}`", column.name))
                                                .in_statement("create-index")
                                        },
                                    )?;
                                ResolvedExpr::Column { source: 0, index: position }
                            }
                        };
                        // 键项显式 `COLLATE` 优先，裸列继承列声明，表达式按 `BINARY`。
                        collations.push(match &column.collate {
                            Some(name) => Collation::parse(name).ok_or_else(|| unknown_collation(name).in_statement("create-index"))?,
                            None => match &column.expr {
                                Some(_) => Collation::Binary,
                                None => def
                                    .columns
                                    .iter()
                                    .find(|existing| existing.name.eq_ignore_ascii_case(&column.name))
                                    .and_then(|existing| existing.collate)
                                    .unwrap_or_default(),
                            },
                        });
                        terms.push(resolved);
                    }
                    let predicate = shape.resolve_optional(predicate.as_ref(), "create-index")?;
                    let rows = self.live_rows(def, "create-index")?;
                    let entries = self.managed_index_entries(def, &terms, predicate.as_ref(), &rows)?;
                    if unique {
                        let names = columns.iter().map(|column| format!("{table}.{}", column.name)).collect::<Vec<_>>().join(", ");
                        if managed_unique_violation(&entries, &collations) {
                            return Err(
                                SqlError::new("unique-constraint", format!("UNIQUE constraint failed: {names}")).in_statement("create-index")
                            );
                        }
                    }
                    self.writer
                        .create_managed_index(&name, &table, unique, &sql, &entries)
                        .map_err(|error| storage_failure(error).in_statement("create-index"))?;
                    let order = self.index_sequence;
                    self.index_sequence += 1;
                    self.indexes.insert(
                        key,
                        IndexDef {
                            name: name.clone(),
                            table: table_key,
                            columns: Vec::new(),
                            unique,
                            order,
                            managed: true,
                            terms,
                            predicate,
                            collations,
                        },
                    );
                    return Ok(ExecResult { statement: "create-index", columns: Vec::new(), rows: Vec::new(), affected: 1 });
                }
                let mut positions = Vec::with_capacity(columns.len());
                let mut collations = Vec::with_capacity(columns.len());
                for column in &columns {
                    let position =
                        def.columns.iter().position(|existing| existing.name.eq_ignore_ascii_case(&column.name)).ok_or_else(|| {
                            SqlError::new("unknown-column", format!("unknown column `{}` in table `{table}`", column.name))
                                .in_statement("create-index")
                        })?;
                    positions.push(position);
                    collations.push(match &column.collate {
                        Some(name) => Collation::parse(name).ok_or_else(|| unknown_collation(name).in_statement("create-index"))?,
                        None => def.columns[position].collate.unwrap_or_default(),
                    });
                }
                if unique {
                    self.ensure_unique_keys(&table, def, &columns)?;
                }
                let names = columns.iter().map(|column| column.name.clone()).collect::<Vec<_>>();
                self.writer
                    .create_index_with_sql(&name, &table, &names, unique, Some(&sql))
                    .map_err(|error| storage_failure(error).in_statement("create-index"))?;
                let order = self.index_sequence;
                self.index_sequence += 1;
                self.indexes.insert(
                    key,
                    IndexDef {
                        name: name.clone(),
                        table: table_key,
                        columns: positions,
                        unique,
                        order,
                        managed: false,
                        terms: Vec::new(),
                        predicate: None,
                        collations,
                    },
                );
                Ok(ExecResult { statement: "create-index", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::Update { schema: _, table, alias, assignments, from, filter, returning } => {
                check_readonly_table(&table, "update")?;
                let key = table.to_ascii_lowercase();
                if self.views.contains_key(&key) {
                    if !from.is_empty() {
                        let mut error =
                            SqlError::new("unsupported-clause", "`UPDATE ... FROM` is not supported on a view; name a base table instead");
                        error.clause = Some("FROM".to_string());
                        return Err(error.in_statement("update"));
                    }
                    return self.run_view_update(&table, &key, alias.as_deref(), &assignments, filter.as_ref(), returning.as_ref());
                }
                let temp_position = if self.scope == TableScope::Main { None } else { self.temp_lookup.get(&key).copied() };
                let main_position = if self.scope == TableScope::Temp {
                    None
                }
                else {
                    match temp_position {
                        Some(_) => None,
                        None => Some(*self.table_lookup.get(&key).ok_or_else(|| self.table_missing(&table, "update"))?),
                    }
                };
                let def_storage = match temp_position {
                    Some(position) => self.temp_tables[position].def.clone(),
                    None => self.tables[main_position.expect("main table resolved")].clone(),
                };
                let def = &def_storage;
                let budget = Budget::new();
                // An `UPDATE ... FROM` resolves assignments and the predicate
                // against the target row plus every `FROM` source, so a bare
                // column may come from either side.
                let (shape, from_tables) = if from.is_empty() {
                    (TargetShape::new(def, alias.as_deref().unwrap_or(&table), self.trigger_frames.last()), Vec::new())
                }
                else {
                    let width = def.columns.len();
                    let mut sources = vec![Source {
                        label: alias.as_deref().unwrap_or(&table).to_string(),
                        columns: SourceColumns::Table(def),
                        offset: 0,
                        join: None,
                        star_hidden: vec![false; width],
                        unqualified_hidden: vec![false; width],
                        star_override: vec![None; width],
                    }];
                    let mut offset = width + usize::from(!def.without_rowid);
                    let mut from_tables: Vec<Rc<Vec<Vec<Cell>>>> = Vec::with_capacity(from.len());
                    for entry in &from {
                        let (columns, rows) = self.select_source(entry, &budget, None, None, None)?;
                        let visible = columns.width();
                        let width = visible + usize::from(columns.hidden_rowid());
                        sources.push(Source {
                            label: entry.label().to_string(),
                            columns,
                            offset,
                            join: None,
                            star_hidden: vec![false; visible],
                            unqualified_hidden: vec![false; visible],
                            star_override: vec![None; visible],
                        });
                        offset += width;
                        from_tables.push(rows);
                    }
                    let source_count = sources.len();
                    let relation = Relation {
                        sources,
                        using: vec![Vec::new(); source_count],
                        using_slots: Vec::new(),
                        trigger: self.trigger_frames.last(),
                        excluded: false,
                    };
                    let scope = relation.shape();
                    (TargetShape { relation, scope }, from_tables)
                };
                let mut targets: Vec<(usize, ResolvedExpr)> = Vec::with_capacity(assignments.len());
                for assignment in &assignments {
                    let index =
                        def.columns.iter().position(|column| column.name.eq_ignore_ascii_case(&assignment.column)).ok_or_else(|| {
                            SqlError::new("unknown-column", format!("unknown column `{}` in table `{table}`", assignment.column))
                                .in_statement("update")
                        })?;
                    targets.push((index, shape.resolve(&assignment.value, "update")?));
                }
                let predicate = shape.resolve_optional(filter.as_ref(), "update")?;
                // SQLite resolves `RETURNING` against the real table name; the
                // `AS` alias of the update target is not visible there.
                let returning_shape = TargetShape::new(def, &table, None);
                let returning = match &returning {
                    Some(items) => Some(returning_shape.plan(def, items, "update")?),
                    None => None,
                };
                // A target row joined to several `FROM` rows is updated once,
                // using the first matching join row, like SQLite.
                let joined = if from.is_empty() {
                    None
                }
                else {
                    let product = join_product(&from_tables);
                    let mut joined: BTreeMap<i64, Vec<Cell>> = BTreeMap::new();
                    if !product.is_empty() {
                        let live = self.live_rows(def, "update")?;
                        let ctx = EvalContext {
                            engine: self,
                            outer: &[],
                            current: &shape.scope,
                            aggregates: &[],
                            windows: &[],
                            window_row: 0,
                            budget: &budget,
                            ctes: None,
                        };
                        for (rowid, cells) in &live {
                            let mut base = cells.clone();
                            if !def.without_rowid {
                                base.push(Cell::Integer(*rowid));
                            }
                            for extra in &product {
                                let mut merged = base.clone();
                                merged.extend(extra.iter().cloned());
                                let keep = match &predicate {
                                    Some(predicate) => truthiness(&predicate.eval(&merged, &ctx)?) == Some(true),
                                    None => true,
                                };
                                if keep {
                                    joined.insert(*rowid, merged);
                                    break;
                                }
                            }
                        }
                    }
                    Some(joined)
                };
                let mut needs_index = self.has_indexes(def, &key);
                // `WHERE <rowid> = <key>` 或普通列索引谓词命中时，扫描不必整表解码。
                let candidates = if from.is_empty() {
                    self.update_candidates(def, &key, &table, alias.as_deref(), filter.as_ref(), &targets)
                }
                else {
                    None
                };
                let plan = self.plan_update(
                    def,
                    &key,
                    &targets,
                    predicate.as_ref(),
                    joined.as_ref(),
                    returning.as_ref(),
                    &shape.scope,
                    &budget,
                    None,
                    candidates.as_deref(),
                )?;
                // Old row values are needed to tell which referenced keys this
                // statement rewrites, so they come from the plan's pre-statement
                // snapshot instead of decoding the table a second time.
                let old_rows = &plan.live;
                let assigned: Vec<bool> =
                    def.columns.iter().enumerate().map(|(index, _)| targets.iter().any(|(target, _)| *target == index)).collect();
                for (_, values) in &plan.writes {
                    for (position, value) in values.iter().enumerate() {
                        if let crate::UpdateValue::Set(value) = value {
                            if let Some(error) = strict_value_violation(def, position, value, "update") {
                                return Err(error);
                            }
                        }
                    }
                }
                let queue_triggers = self.table_has_triggers(&key);
                let trigger_columns = trigger_frame_columns(def);
                let assigned_names: Vec<String> = assignments.iter().map(|assignment| assignment.column.clone()).collect();
                let old_lookup: BTreeMap<i64, Vec<Cell>> =
                    if queue_triggers || def.without_rowid || self.foreign_keys { old_rows.iter().cloned().collect() } else { BTreeMap::new() };
                // The `AFTER` events wait until referential actions and checks
                // have run, matching SQLite's `BEFORE`, cascade, `AFTER` order.
                let mut after_events: Vec<(i64, Vec<Cell>, Vec<Cell>)> = Vec::new();
                if let Some(position) = temp_position {
                    let mut events: Vec<(i64, Vec<Cell>, Vec<Cell>)> = Vec::new();
                    {
                        let table = &mut self.temp_tables[position];
                        for (rowid, values) in &plan.writes {
                            let cells = table
                                .rows
                                .get_mut(rowid)
                                .ok_or_else(|| SqlError::new("storage", "temp row disappeared").in_statement("update"))?;
                            let old_cells = cells.clone();
                            for (slot, value) in values.iter().enumerate() {
                                if let crate::UpdateValue::Set(value) = value {
                                    cells[slot] = write_value_cell(value);
                                }
                            }
                            if queue_triggers {
                                events.push((*rowid, old_cells, cells.clone()));
                            }
                        }
                    }
                    for (rowid, old_cells, new_cells) in events {
                        self.queue_update_events(&key, def, &trigger_columns, &assigned_names, rowid, &old_cells, &new_cells);
                    }
                }
                else {
                    self.writer.begin().map_err(|error| storage_failure(error).in_statement("update"))?;
                    for (rowid, values) in &plan.writes {
                        let mut new_cells = old_lookup.get(rowid).cloned().unwrap_or_default();
                        if new_cells.len() < def.columns.len() {
                            new_cells.resize(def.columns.len(), Cell::Null);
                        }
                        for (slot, value) in values.iter().enumerate() {
                            if let crate::UpdateValue::Set(value) = value {
                                new_cells[slot] = write_value_cell(value);
                            }
                        }
                        if queue_triggers {
                            let old_cells = old_lookup.get(rowid).cloned().unwrap_or_default();
                            self.queue_update_before(&key, def, &trigger_columns, &assigned_names, *rowid, &old_cells, &new_cells);
                            after_events.push((*rowid, old_cells, new_cells.clone()));
                        }
                        let written = if def.without_rowid {
                            let old_values: Vec<crate::WriteValue> =
                                old_lookup.get(rowid).cloned().unwrap_or_default().iter().map(cell_write_value).collect();
                            let new_values: Vec<crate::WriteValue> = new_cells.iter().map(cell_write_value).collect();
                            self.writer.update_without_rowid(&table, &old_values, &new_values)
                        }
                        else {
                            self.writer.update(&table, *rowid, values)
                        };
                        if let Err(error) = written {
                            let _ = self.writer.rollback();
                            return Err(write_failure(error, "update", def));
                        }
                    }
                }
                let mut needs_without_rowid = temp_position.is_none() && def.without_rowid;
                if temp_position.is_none() && self.foreign_keys {
                    let changes: Vec<(Vec<Cell>, Option<Vec<Cell>>)> = plan
                        .writes
                        .iter()
                        .map(|(rowid, values)| {
                            let old = old_lookup.get(rowid).cloned().unwrap_or_default();
                            let mut next = old.clone();
                            if next.len() < def.columns.len() {
                                next.resize(def.columns.len(), Cell::Null);
                            }
                            for (slot, value) in values.iter().enumerate() {
                                if let crate::UpdateValue::Set(value) = value {
                                    next[slot] = write_value_cell(value);
                                }
                            }
                            (old, Some(next))
                        })
                        .collect();
                    match self.apply_foreign_key_actions(def, &changes, "update", 0) {
                        Ok((index_needed, without_needed)) => {
                            needs_index |= index_needed;
                            needs_without_rowid |= without_needed;
                        }
                        Err(error) => {
                            let _ = self.writer.rollback();
                            return Err(error);
                        }
                    }
                }
                if self.foreign_keys {
                    if let Err(error) = self.check_update_foreign_keys(def, &plan, &old_rows, &assigned, "update") {
                        if temp_position.is_none() {
                            let _ = self.writer.rollback();
                        }
                        return Err(error);
                    }
                }
                if temp_position.is_none() {
                    for (rowid, old_cells, new_cells) in &after_events {
                        self.queue_update_after(&key, def, &trigger_columns, &assigned_names, *rowid, old_cells, new_cells);
                    }
                }
                if temp_position.is_none() {
                    if let Err(error) = self.refresh_managed_indexes(&key, def, "update") {
                        let _ = self.writer.rollback();
                        return Err(error);
                    }
                    self.writer.commit().map_err(|error| storage_failure(error).in_statement("update"))?;
                }
                if temp_position.is_none() && needs_index {
                    self.materialize_indexes_after_write("update")?;
                }
                if needs_without_rowid {
                    self.materialize_without_rowid_after_write("update")?;
                }
                // 外键动作会直接改写子表行,索引内容不再只由本表语句决定;
                // 其余情况按被赋值的列精确失效,只改非索引列时缓存原样保留。
                let foreign_actions =
                    self.foreign_keys && (!def.foreign_keys.is_empty() || !self.referencing_foreign_keys(&def.name).is_empty());
                if foreign_actions {
                    self.invalidate_all_indexes();
                }
                else {
                    self.invalidate_indexes_after_update(def, &key, &assigned);
                }
                Ok(ExecResult {
                    statement: "update",
                    columns: returning.as_ref().map(|plan| plan.columns.clone()).unwrap_or_default(),
                    rows: plan.returning,
                    affected: plan.affected,
                })
            }
            Statement::Delete { schema: _, table, alias, filter, returning } => {
                check_readonly_table(&table, "delete")?;
                let key = table.to_ascii_lowercase();
                if self.views.contains_key(&key) {
                    return self.run_view_delete(&table, &key, alias.as_deref(), filter.as_ref(), returning.as_ref());
                }
                let temp_position = if self.scope == TableScope::Main { None } else { self.temp_lookup.get(&key).copied() };
                let main_position = if self.scope == TableScope::Temp {
                    None
                }
                else {
                    match temp_position {
                        Some(_) => None,
                        None => Some(*self.table_lookup.get(&key).ok_or_else(|| self.table_missing(&table, "delete"))?),
                    }
                };
                // Temp rows are removed from the in-memory map, so their
                // definition is cloned to keep the catalog readable while the
                // map is borrowed mutably.
                let def_storage = match temp_position {
                    Some(position) => self.temp_tables[position].def.clone(),
                    None => self.tables[main_position.expect("main table resolved")].clone(),
                };
                let def = &def_storage;
                let shape = TargetShape::new(def, alias.as_deref().unwrap_or(&table), self.trigger_frames.last());
                let predicate = shape.resolve_optional(filter.as_ref(), "delete")?;
                let returning_shape = TargetShape::new(def, &table, None);
                let returning = match &returning {
                    Some(items) => Some(returning_shape.plan(def, items, "delete")?),
                    None => None,
                };
                let budget = Budget::new();
                let mut needs_index = self.has_indexes(def, &key);
                // `WHERE <rowid> = <key>` 或普通列索引谓词命中时，删除不必整表解码。
                let candidates = self.delete_candidates(def, &key, &table, alias.as_deref(), filter.as_ref());
                let plan = self.plan_delete(def, predicate.as_ref(), returning.as_ref(), &shape.scope, &budget, candidates.as_deref())?;
                // A temp table snapshots its row map instead of opening a
                // writer transaction, so a failing referential check undoes the
                // removal exactly like a rollback would.
                let temp_snapshot = temp_position.map(|position| self.temp_tables[position].rows.clone());
                let queue_triggers = self.table_has_triggers(&key);
                let trigger_columns = trigger_frame_columns(def);
                // The deleted rows' referenced keys are only visible before the
                // removal, so the snapshot comes from the plan; 只有触发器和外键
                // 动作需要按 rowid 查旧行，其余路径不建这张表。
                let old_lookup: BTreeMap<i64, Vec<Cell>> =
                    if queue_triggers || self.foreign_keys { plan.live.iter().cloned().collect() } else { BTreeMap::new() };
                let mut after_events: Vec<(i64, Vec<Cell>)> = Vec::new();
                if let Some(position) = temp_position {
                    {
                        let table = &mut self.temp_tables[position];
                        for rowid in &plan.removed {
                            table.rows.remove(rowid);
                        }
                    }
                    if queue_triggers {
                        for rowid in &plan.removed {
                            let old_cells = old_lookup.get(rowid).cloned().unwrap_or_default();
                            self.queue_delete_events(&key, &trigger_columns, *rowid, &old_cells);
                        }
                    }
                }
                else {
                    self.writer.begin().map_err(|error| storage_failure(error).in_statement("delete"))?;
                    for rowid in &plan.removed {
                        if queue_triggers {
                            let old_cells = old_lookup.get(rowid).cloned().unwrap_or_default();
                            self.queue_delete_before(&key, &trigger_columns, *rowid, &old_cells);
                            after_events.push((*rowid, old_cells));
                        }
                        let removed = if def.without_rowid {
                            let old_values: Vec<crate::WriteValue> =
                                old_lookup.get(rowid).cloned().unwrap_or_default().iter().map(cell_write_value).collect();
                            self.writer.delete_without_rowid(&table, &old_values)
                        }
                        else {
                            self.writer.delete(&table, *rowid)
                        };
                        if let Err(error) = removed {
                            let _ = self.writer.rollback();
                            return Err(write_failure(error, "delete", def));
                        }
                    }
                }
                let mut needs_without_rowid = temp_position.is_none() && def.without_rowid;
                if temp_position.is_none() && self.foreign_keys {
                    let changes: Vec<(Vec<Cell>, Option<Vec<Cell>>)> =
                        plan.removed.iter().map(|rowid| (old_lookup.get(rowid).cloned().unwrap_or_default(), None)).collect();
                    match self.apply_foreign_key_actions(def, &changes, "delete", 0) {
                        Ok((index_needed, without_needed)) => {
                            needs_index |= index_needed;
                            needs_without_rowid |= without_needed;
                        }
                        Err(error) => {
                            let _ = self.writer.rollback();
                            return Err(error);
                        }
                    }
                }
                if self.foreign_keys {
                    if let Err(error) = self.check_delete_foreign_keys(def, &plan.live, &plan.removed, "delete") {
                        match (temp_position, temp_snapshot) {
                            (Some(position), Some(snapshot)) => self.temp_tables[position].rows = snapshot,
                            _ => {
                                let _ = self.writer.rollback();
                            }
                        }
                        return Err(error);
                    }
                }
                if temp_position.is_none() {
                    for (rowid, old_cells) in &after_events {
                        self.queue_delete_after(&key, &trigger_columns, *rowid, old_cells);
                    }
                }
                if temp_position.is_none() {
                    if let Err(error) = self.refresh_managed_indexes(&key, def, "delete") {
                        let _ = self.writer.rollback();
                        return Err(error);
                    }
                    self.writer.commit().map_err(|error| storage_failure(error).in_statement("delete"))?;
                }
                if temp_position.is_none() && needs_index {
                    self.materialize_indexes_after_write("delete")?;
                }
                if needs_without_rowid {
                    self.materialize_without_rowid_after_write("delete")?;
                }
                // 同 `UPDATE`:外键级联删除会改动子表,只能全量失效;否则
                // 把被删行从整份索引快照里精确剔除。
                let foreign_actions =
                    self.foreign_keys && (!def.foreign_keys.is_empty() || !self.referencing_foreign_keys(&def.name).is_empty());
                if foreign_actions {
                    self.invalidate_all_indexes();
                }
                else {
                    self.patch_index_probes_after_delete(&key, &plan);
                }
                Ok(ExecResult {
                    statement: "delete",
                    columns: returning.as_ref().map(|plan| plan.columns.clone()).unwrap_or_default(),
                    rows: plan.returning,
                    affected: plan.removed.len(),
                })
            }
            Statement::Pragma { schema, name, value } => {
                if name.eq_ignore_ascii_case("database_list") {
                    let mut rows = vec![vec![Cell::Integer(0), Cell::Text("main".to_string()), Cell::Text(String::new())]];
                    if self.temp_created {
                        rows.push(vec![Cell::Integer(1), Cell::Text("temp".to_string()), Cell::Text(String::new())]);
                    }
                    for (index, attached) in self.attached.iter().enumerate() {
                        rows.push(vec![
                            Cell::Integer(2 + index as i64),
                            Cell::Text(attached.name.clone()),
                            Cell::Text(attached.display.clone()),
                        ]);
                    }
                    return Ok(ExecResult {
                        statement: "pragma",
                        columns: vec!["seq".to_string(), "name".to_string(), "file".to_string()],
                        rows,
                        affected: 0,
                    });
                }
                let argument = value.unwrap_or_default();
                match name.to_ascii_lowercase().as_str() {
                    "table_info" => return pragma_table_info(self, schema.as_deref(), &argument, false),
                    "table_xinfo" => return pragma_table_info(self, schema.as_deref(), &argument, true),
                    "index_list" => return Ok(pragma_index_list(self, &argument)),
                    "index_info" => return Ok(pragma_index_info(self, &argument)),
                    "foreign_key_list" => return Ok(pragma_foreign_key_list(self, &argument)),
                    "user_version" => {
                        return Ok(match argument.is_empty() {
                            false => {
                                self.writer.set_user_version(argument.trim().parse::<i64>().unwrap_or(0));
                                ExecResult { statement: "pragma", columns: Vec::new(), rows: Vec::new(), affected: 0 }
                            }
                            true => ExecResult {
                                statement: "pragma",
                                columns: vec!["user_version".to_string()],
                                rows: vec![vec![Cell::Integer(self.writer.user_version())]],
                                affected: 0,
                            },
                        });
                    }
                    "application_id" => {
                        return Ok(match argument.is_empty() {
                            false => {
                                self.writer.set_application_id(argument.trim().parse::<i64>().unwrap_or(0));
                                ExecResult { statement: "pragma", columns: Vec::new(), rows: Vec::new(), affected: 0 }
                            }
                            true => ExecResult {
                                statement: "pragma",
                                columns: vec!["application_id".to_string()],
                                rows: vec![vec![Cell::Integer(self.writer.application_id())]],
                                affected: 0,
                            },
                        });
                    }
                    "integrity_check" => return Ok(self.pragma_integrity_check(&argument)),
                    _ => {}
                }
                let boolean = |text: &str| -> bool {
                    match text.to_ascii_lowercase().as_str() {
                        "on" | "true" | "yes" => true,
                        "off" | "false" | "no" => false,
                        other => other.parse::<i64>().map(|value| value > 0).unwrap_or(false),
                    }
                };
                let recursive = name.eq_ignore_ascii_case("recursive_triggers");
                if !name.eq_ignore_ascii_case("foreign_keys") && !recursive {
                    return Err(SqlError::new(
                        "unsupported-pragma",
                        format!(
                            "unsupported pragma `{name}`; this engine supports `foreign_keys`, `recursive_triggers`, `table_info`, `table_xinfo`, `index_list`, `index_info`, `foreign_key_list`, `database_list`, `user_version`, `application_id` and `integrity_check`"
                        ),
                    )
                    .in_clause("PRAGMA")
                    .in_statement("pragma"));
                }
                let flag = if recursive { &mut self.recursive_triggers } else { &mut self.foreign_keys };
                if argument.is_empty() {
                    Ok(ExecResult {
                        statement: "pragma",
                        columns: vec![name.clone()],
                        rows: vec![vec![Cell::Integer(i64::from(*flag))]],
                        affected: 0,
                    })
                }
                else {
                    // SQLite reads the value as a boolean: `on` / `true`
                    // / `yes` and any positive integer enable it, `off` /
                    // `false` / `no` / zero and non-numeric text disable
                    // it.
                    *flag = boolean(&argument);
                    Ok(ExecResult { statement: "pragma", columns: Vec::new(), rows: Vec::new(), affected: 0 })
                }
            }
            Statement::DropTable { schema: _, name, if_exists } => {
                if is_schema_table(&name) {
                    return Err(SqlError::new("readonly-system-table", format!("table {name} may not be dropped")).in_statement("drop-table"));
                }
                let key = name.to_ascii_lowercase();
                // The temp schema is searched before main, so `DROP TABLE`
                // removes the temp object when both schemas share the name.
                let temp_position = if self.scope == TableScope::Main { None } else { self.temp_lookup.get(&key).copied() };
                if temp_position.is_none() && self.temp_views.contains_key(&key) {
                    return Err(SqlError::new("wrong-object", format!("use DROP VIEW to delete view {name}")).in_statement("drop-table"));
                }
                if let Some(position) = temp_position {
                    let dropped = self.temp_tables.remove(position);
                    // The freed page goes back to the temp allocator, so the
                    // next temp table reuses it like SQLite does.
                    self.temp_free_pages.insert(dropped.rootpage);
                    self.temp_lookup.remove(&key);
                    self.reindex_temp_tables();
                    self.triggers.retain(|_, trigger| !(trigger.temp && trigger.table == key));
                    return Ok(ExecResult { statement: "drop-table", columns: Vec::new(), rows: Vec::new(), affected: 1 });
                }
                let objects = self.schema_objects("drop-table")?;
                let present = objects.iter().any(|object| object.kind == SchemaObjectKind::Table && object.name.eq_ignore_ascii_case(&name));
                if !present {
                    if objects.iter().any(|object| object.kind == SchemaObjectKind::View && object.name.eq_ignore_ascii_case(&name)) {
                        return Err(SqlError::new("wrong-object", format!("use DROP VIEW to delete view {name}")).in_statement("drop-table"));
                    }
                    if if_exists {
                        return Ok(ExecResult { statement: "drop-table", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                    }
                    return Err(
                        SqlError::new("unknown-table", format!("no such table: {}", self.scoped_name(&name))).in_statement("drop-table")
                    );
                }
                if name.eq_ignore_ascii_case("sqlite_sequence") {
                    return Err(SqlError::new("readonly-system-table", format!("table {name} may not be dropped")).in_statement("drop-table"));
                }
                let key = name.to_ascii_lowercase();
                let position = *self
                    .table_lookup
                    .get(&key)
                    .ok_or_else(|| SqlError::new("unknown-table", format!("no such table: {name}")).in_statement("drop-table"))?;
                self.writer.begin().map_err(|error| storage_failure(error).in_statement("drop-table"))?;
                match self.writer.drop_table(&name) {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = self.writer.rollback();
                        return Err(SqlError::new("unknown-table", format!("no such table: {name}")).in_statement("drop-table"));
                    }
                    Err(error) => {
                        let _ = self.writer.rollback();
                        return Err(storage_failure(error).in_statement("drop-table"));
                    }
                }
                self.writer.commit().map_err(|error| storage_failure(error).in_statement("drop-table"))?;
                self.tables.remove(position);
                self.reindex_tables();
                self.indexes.retain(|_, index| index.table != key);
                self.triggers.retain(|_, trigger| trigger.table != key);
                Ok(ExecResult { statement: "drop-table", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::DropIndex { schema: _, name, if_exists } => {
                self.writer.begin().map_err(|error| storage_failure(error).in_statement("drop-index"))?;
                let outcome = match self.writer.drop_index(&name) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        let _ = self.writer.rollback();
                        return Err(storage_failure(error).in_statement("drop-index"));
                    }
                };
                match outcome {
                    DropIndexOutcome::Dropped => {}
                    DropIndexOutcome::Missing => {
                        let _ = self.writer.rollback();
                        if if_exists {
                            return Ok(ExecResult { statement: "drop-index", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                        }
                        return Err(SqlError::new("unknown-index", format!("no such index: {name}")).in_statement("drop-index"));
                    }
                    DropIndexOutcome::Automatic => {
                        let _ = self.writer.rollback();
                        return Err(SqlError::new(
                            "autoindex-immutable",
                            "index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped",
                        )
                        .in_statement("drop-index"));
                    }
                }
                self.writer.commit().map_err(|error| storage_failure(error).in_statement("drop-index"))?;
                self.indexes.remove(&name.to_ascii_lowercase());
                Ok(ExecResult { statement: "drop-index", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::AlterTable { schema: _, table, action } => self.run_alter_table(table, action),
            Statement::Select { with, recursive, first, compounds, order, limit } => {
                // One budget per statement, shared by every nested subquery, so
                // deep correlation reports a structured error instead of
                // running until the host timeout.
                let budget = Budget::new();
                self.run_select(&with, recursive, &first, &compounds, order, limit, &[], &budget, None)
            }
        }
    }

    /// Reads the live schema rows with a statement-tagged storage error.
    fn schema_objects(&self, statement: &'static str) -> Result<Vec<crate::SchemaObject>, SqlError> {
        self.writer.schema_objects(TableLimits::default()).map_err(|error| storage_failure(error).in_statement(statement))
    }

    /// Rebuilds the lowercase name to position map after a table is removed.
    fn reindex_tables(&mut self) {
        self.table_lookup.clear();
        for (position, table) in self.tables.iter().enumerate() {
            self.table_lookup.insert(table.name.to_ascii_lowercase(), position);
        }
    }

    /// Rebuilds the temp schema name map after a temp table is removed.
    fn reindex_temp_tables(&mut self) {
        self.temp_lookup.clear();
        for (position, table) in self.temp_tables.iter().enumerate() {
            self.temp_lookup.insert(table.def.name.to_ascii_lowercase(), position);
        }
    }

    /// Executes the `ALTER TABLE` forms: `RENAME TO`, `RENAME COLUMN`, `ADD
    /// COLUMN` and `DROP COLUMN`. A drop rewrites every stored record and the
    /// stored `CREATE TABLE` text, refusing columns still read by an index,
    /// a view, a trigger or a table-level constraint.
    fn run_alter_table(&mut self, table: String, action: AlterAction) -> Result<ExecResult, SqlError> {
        if is_schema_table(&table) {
            return Err(SqlError::new("readonly-system-table", format!("table {table} may not be altered")).in_statement("alter-table"));
        }
        let objects = self.schema_objects("alter-table")?;
        if table.eq_ignore_ascii_case("sqlite_sequence") {
            let present = objects.iter().any(|object| object.kind == SchemaObjectKind::Table && object.name.eq_ignore_ascii_case(&table));
            if present {
                return Err(SqlError::new("readonly-system-table", format!("table {table} may not be altered")).in_statement("alter-table"));
            }
            return Err(SqlError::new("unknown-table", format!("no such table: {table}")).in_statement("alter-table"));
        }
        let key = table.to_ascii_lowercase();
        let position = *self
            .table_lookup
            .get(&key)
            .ok_or_else(|| SqlError::new("unknown-table", format!("no such table: {table}")).in_statement("alter-table"))?;
        let stored = objects
            .iter()
            .find(|object| object.kind == SchemaObjectKind::Table && object.name.eq_ignore_ascii_case(&self.tables[position].name))
            .and_then(|object| object.sql.clone())
            .ok_or_else(|| SqlError::new("storage", "sqlite table has no stored text").in_statement("alter-table"))?;
        match action {
            AlterAction::RenameTable { new_name } => {
                check_reserved_object_name(&new_name, "alter-table")?;
                let new_key = new_name.to_ascii_lowercase();
                if self.table_lookup.contains_key(&new_key) || self.indexes.contains_key(&new_key) || self.views.contains_key(&new_key) {
                    return Err(SqlError::new("table-exists", format!("there is already another table or index with this name: {new_name}"))
                        .in_statement("alter-table"));
                }
                let old_name = self.tables[position].name.clone();
                let new_sql = rewrite_create_table_name(&stored, &new_name)
                    .ok_or_else(|| SqlError::new("storage", "sqlite stored table text unrecognized").in_statement("alter-table"))?;
                let mut pending = Vec::new();
                for object in &objects {
                    if object.kind != SchemaObjectKind::Index || !object.table_name.eq_ignore_ascii_case(&old_name) {
                        continue;
                    }
                    if let Some(text) = &object.sql {
                        let rewritten = rewrite_create_index_table(text, &new_name)
                            .ok_or_else(|| SqlError::new("storage", "sqlite stored index text unrecognized").in_statement("alter-table"))?;
                        pending.push((object.name.clone(), rewritten));
                    }
                }
                // Every view that names the old table follows the rename, and
                // SQLite rewrites its stored text so a later reopen resolves.
                let mut pending_views = Vec::new();
                for object in &objects {
                    if object.kind != SchemaObjectKind::View {
                        continue;
                    }
                    let Some(view) = self.views.get(&object.name.to_ascii_lowercase())
                    else {
                        continue;
                    };
                    let Some(text) = &object.sql
                    else {
                        continue;
                    };
                    if !statement_references(&view.query, &old_name) {
                        continue;
                    }
                    if let Some(rewritten) = rewrite_view_table(text, &old_name, &new_name) {
                        pending_views.push((view.name.clone(), rewritten));
                    }
                }
                self.writer.begin().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                if let Err(error) = self.writer.rename_table_schema(&old_name, &new_name, &new_sql, &pending) {
                    let _ = self.writer.rollback();
                    return Err(storage_failure(error).in_statement("alter-table"));
                }
                self.writer.commit().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                if !pending_views.is_empty() {
                    self.writer.begin().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                    for (name, text) in &pending_views {
                        if let Err(error) = self.writer.rewrite_view_sql(name, text) {
                            let _ = self.writer.rollback();
                            return Err(storage_failure(error).in_statement("alter-table"));
                        }
                    }
                    self.writer.commit().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                }
                self.tables[position].name = new_name;
                self.reindex_tables();
                for index in self.indexes.values_mut() {
                    if index.table == key {
                        index.table = new_key.clone();
                    }
                }
                for trigger in self.triggers.values_mut() {
                    if trigger.table == key {
                        trigger.table = new_key.clone();
                    }
                }
                for (name, text) in pending_views {
                    if let Some(Statement::CreateView { query, .. }) = parse(&text).ok().and_then(|statements| statements.into_iter().next()) {
                        if let Some(view) = self.views.get_mut(&name.to_ascii_lowercase()) {
                            view.query = *query;
                        }
                    }
                }
                Ok(ExecResult { statement: "alter-table", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            AlterAction::RenameColumn { old, new } => {
                let def = &self.tables[position];
                let column = def
                    .columns
                    .iter()
                    .position(|column| column.name.eq_ignore_ascii_case(&old))
                    .ok_or_else(|| SqlError::new("unknown-column", format!("no such column: \"{old}\"")).in_statement("alter-table"))?;
                if def.columns.iter().enumerate().any(|(index, existing)| index != column && existing.name.eq_ignore_ascii_case(&new)) {
                    return Err(SqlError::new("duplicate-column", format!("duplicate column name: {new}")).in_statement("alter-table"));
                }
                if old.eq_ignore_ascii_case(&new) {
                    return Ok(ExecResult { statement: "alter-table", columns: Vec::new(), rows: Vec::new(), affected: 1 });
                }
                let name = def.name.clone();
                let old_column = old;
                let new_sql = rewrite_create_table_column(&stored, &old_column, &new)
                    .ok_or_else(|| SqlError::new("storage", "sqlite stored table text unrecognized").in_statement("alter-table"))?;
                let mut pending = Vec::new();
                for object in &objects {
                    if object.kind != SchemaObjectKind::Index || !object.table_name.eq_ignore_ascii_case(&name) {
                        continue;
                    }
                    if let Some(text) = &object.sql {
                        let rewritten = rewrite_create_index_column(text, &old_column, &new)
                            .ok_or_else(|| SqlError::new("storage", "sqlite stored index text unrecognized").in_statement("alter-table"))?;
                        pending.push((object.name.clone(), rewritten));
                    }
                }
                // Views keep the renamed column reference spelled correctly in
                // their stored text, exactly like SQLite rewrites them.
                let mut pending_views = Vec::new();
                for object in &objects {
                    if object.kind != SchemaObjectKind::View {
                        continue;
                    }
                    let Some(view) = self.views.get(&object.name.to_ascii_lowercase())
                    else {
                        continue;
                    };
                    let Some(text) = &object.sql
                    else {
                        continue;
                    };
                    if !statement_mentions_column(&view.query, &old_column, None) {
                        continue;
                    }
                    if let Some(rewritten) = rewrite_view_column(text, &old_column, &new) {
                        pending_views.push((view.name.clone(), rewritten));
                    }
                }
                self.writer.begin().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                if let Err(error) = self.writer.rename_column_schema(&name, &new_sql, &pending) {
                    let _ = self.writer.rollback();
                    return Err(storage_failure(error).in_statement("alter-table"));
                }
                self.writer.commit().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                if !pending_views.is_empty() {
                    self.writer.begin().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                    for (view, text) in &pending_views {
                        if let Err(error) = self.writer.rewrite_view_sql(view, text) {
                            let _ = self.writer.rollback();
                            return Err(storage_failure(error).in_statement("alter-table"));
                        }
                    }
                    self.writer.commit().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                }
                for (view, text) in pending_views {
                    if let Some(Statement::CreateView { query, .. }) = parse(&text).ok().and_then(|statements| statements.into_iter().next()) {
                        if let Some(view) = self.views.get_mut(&view.to_ascii_lowercase()) {
                            view.query = *query;
                        }
                    }
                }
                self.tables[position].columns[column].name = new;
                Ok(ExecResult { statement: "alter-table", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            AlterAction::AddColumn { column, checks, foreign_keys, sql } => {
                let def = &self.tables[position];
                if def.columns.iter().any(|existing| existing.name.eq_ignore_ascii_case(&column.name)) {
                    return Err(
                        SqlError::new("duplicate-column", format!("duplicate column name: {}", column.name)).in_statement("alter-table")
                    );
                }
                if column.primary_key {
                    return Err(SqlError::new("invalid-alter", "Cannot add a PRIMARY KEY column").in_statement("alter-table"));
                }
                if column.unique {
                    return Err(SqlError::new("invalid-alter", "Cannot add a UNIQUE column").in_statement("alter-table"));
                }
                if !foreign_keys.is_empty() && matches!(&column.default_value, Some(value) if !matches!(value, crate::WriteValue::Null)) {
                    return Err(SqlError::new("invalid-alter", "Cannot add a REFERENCES column with non-NULL default value")
                        .in_statement("alter-table"));
                }
                // SQLite only refuses when an existing row would read back
                // NULL; on an empty table the same definition is accepted.
                let has_rows = !self
                    .writer
                    .table_rows(&def.name, TableLimits::default())
                    .map_err(|error| storage_failure(error).in_statement("alter-table"))?
                    .is_empty();
                if column.not_null && matches!(column.default_value, None | Some(crate::WriteValue::Null)) && has_rows {
                    return Err(
                        SqlError::new("invalid-alter", "Cannot add a NOT NULL column with default value NULL").in_statement("alter-table")
                    );
                }
                let mut columns = def.columns.clone();
                columns.push(column.clone());
                let resolved = resolve_checks(&def.name, &columns, &checks, "alter-table", def.without_rowid)?;
                let resolved_foreign_keys = resolve_foreign_keys(&columns, &foreign_keys)?;
                let open = stored
                    .rfind(')')
                    .ok_or_else(|| SqlError::new("storage", "sqlite stored table text has no column list").in_statement("alter-table"))?;
                let new_sql = format!("{}, {}{}", &stored[..open], sql, &stored[open..]);
                self.writer.begin().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                if let Err(error) = self.writer.rewrite_table_sql(&def.name, &new_sql) {
                    let _ = self.writer.rollback();
                    return Err(storage_failure(error).in_statement("alter-table"));
                }
                self.writer.commit().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                self.tables[position].columns.push(column);
                self.tables[position].checks.extend(resolved);
                self.tables[position].foreign_keys.extend(resolved_foreign_keys);
                Ok(ExecResult { statement: "alter-table", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            AlterAction::DropColumn { column } => {
                let def = &self.tables[position];
                let column_position = def
                    .columns
                    .iter()
                    .position(|existing| existing.name.eq_ignore_ascii_case(&column))
                    .ok_or_else(|| SqlError::new("unknown-column", format!("no such column: \"{column}\"")).in_statement("alter-table"))?;
                let primary = def.rowid_alias == Some(column_position)
                    || def.primary_key.contains(&column_position)
                    || def.columns[column_position].primary_key;
                if primary {
                    return Err(
                        SqlError::new("invalid-alter", format!("cannot drop PRIMARY KEY column: \"{column}\"")).in_statement("alter-table")
                    );
                }
                if def.columns[column_position].unique {
                    return Err(SqlError::new("invalid-alter", format!("cannot drop UNIQUE column: \"{column}\"")).in_statement("alter-table"));
                }
                if def.columns.len() == 1 {
                    return Err(SqlError::new("invalid-alter", format!("cannot drop column \"{column}\": no other columns exist"))
                        .in_statement("alter-table"));
                }
                if let Some((name, _)) = self.indexes.iter().find(|(_, index)| index.table == key && index.columns.contains(&column_position)) {
                    return Err(SqlError::new("invalid-alter", format!("error in index {name} after drop column: no such column: {column}"))
                        .in_statement("alter-table"));
                }
                let new_sql = match rewrite_create_table_drop_column(&stored, &def.columns[column_position].name) {
                    Ok(Some(text)) => text,
                    Ok(None) => {
                        return Err(SqlError::new("storage", "sqlite stored table text unrecognized").in_statement("alter-table"));
                    }
                    Err(DropColumnConflict::TableCheck) => {
                        return Err(SqlError::new(
                            "invalid-alter",
                            format!("error in table {} after drop column: no such column: {column}", def.name),
                        )
                        .in_statement("alter-table"));
                    }
                    Err(DropColumnConflict::TableForeignKey) => {
                        return Err(SqlError::new(
                            "invalid-alter",
                            format!("error in table {} after drop column: unknown column \"{column}\" in foreign key definition", def.name),
                        )
                        .in_statement("alter-table"));
                    }
                };
                if let Some((name, _)) = self.views.iter().find(|(_, view)| statement_mentions_column(&view.query, &column, None)) {
                    return Err(SqlError::new("invalid-alter", format!("error in view {name} after drop column: no such column: {column}"))
                        .in_statement("alter-table"));
                }
                if let Some((name, _)) = self.triggers.iter().find(|(_, trigger)| {
                    trigger.table == key
                        && trigger.body.iter().any(|statement| statement_mentions_column(statement, &column, Some(&["new", "old"])))
                }) {
                    return Err(SqlError::new("invalid-alter", format!("error in trigger {name} after drop column: no such column: {column}"))
                        .in_statement("alter-table"));
                }
                // The catalog is re-derived from the rewritten DDL so checks,
                // foreign keys and the rowid alias all follow the new column
                // positions instead of being patched by hand.
                let mut statements = parse(&new_sql)
                    .map_err(|_| SqlError::new("storage", "sqlite stored table text unrecognized").in_statement("alter-table"))?;
                let Some(Statement::CreateTable {
                    name,
                    columns,
                    primary_key,
                    table_constraints,
                    checks,
                    foreign_keys,
                    strict,
                    without_rowid,
                    ..
                }) = statements.drain(..).next()
                else {
                    return Err(SqlError::new("storage", "sqlite stored table text unrecognized").in_statement("alter-table"));
                };
                let derived = Self::catalog_table(name, columns, primary_key, table_constraints, checks, foreign_keys, strict, without_rowid)?;
                self.writer.begin().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                if let Err(error) = self.writer.drop_column(&def.name, column_position) {
                    let _ = self.writer.rollback();
                    return Err(storage_failure(error).in_statement("alter-table"));
                }
                if let Err(error) = self.writer.rewrite_table_sql(&def.name, &new_sql) {
                    let _ = self.writer.rollback();
                    return Err(storage_failure(error).in_statement("alter-table"));
                }
                self.writer.commit().map_err(|error| storage_failure(error).in_statement("alter-table"))?;
                for index in self.indexes.values_mut() {
                    if index.table != key {
                        continue;
                    }
                    for position in &mut index.columns {
                        if *position > column_position {
                            *position -= 1;
                        }
                    }
                }
                self.tables[position] = derived;
                Ok(ExecResult { statement: "alter-table", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
        }
    }

    /// Rejects a `UNIQUE` index when two existing rows carry the same key.
    ///
    /// SQLite treats a `NULL` key column as distinct, so rows holding `NULL`
    /// never collide. Key comparison follows the engine's storage-class rules,
    /// which makes `1` and `1.0` collide just like upstream.
    fn ensure_unique_keys(&self, table: &str, def: &TableDef, columns: &[IndexColumn]) -> Result<(), SqlError> {
        let mut positions = Vec::with_capacity(columns.len());
        let mut collations = Vec::with_capacity(columns.len());
        for column in columns {
            let position = def.columns.iter().position(|existing| existing.name.eq_ignore_ascii_case(&column.name)).ok_or_else(|| {
                SqlError::new("unknown-column", format!("unknown column `{}` in table `{table}`", column.name)).in_statement("create-index")
            })?;
            positions.push(position);
            // 键项显式 `COLLATE` 优先，否则继承列的声明排序规则。
            collations.push(match &column.collate {
                Some(name) => Collation::parse(name).ok_or_else(|| unknown_collation(name).in_statement("create-index"))?,
                None => def.columns[position].collate.unwrap_or_default(),
            });
        }
        let mut rows = Vec::new();
        for row in self
            .writer
            .table_rows(table, TableLimits::default())
            .map_err(|error| storage_failure(error).in_statement("create-index"))?
        {
            rows.push(decode_row(&row.payload, def, row.rowid)?);
        }
        for right in 1..rows.len() {
            for left in 0..right {
                let collides = positions.iter().enumerate().all(|(position, column)| {
                    let (first, second) = (&rows[left][*column], &rows[right][*column]);
                    !matches!(first, Cell::Null)
                        && !matches!(second, Cell::Null)
                        && compare_cells_with(first, second, collations[position]) == Ordering::Equal
                });
                if collides {
                    let names =
                        positions.iter().map(|position| format!("{table}.{}", def.columns[*position].name)).collect::<Vec<_>>().join(", ");
                    return Err(SqlError::new("unique-constraint", format!("UNIQUE constraint failed: {names}")).in_statement("create-index"));
                }
            }
        }
        Ok(())
    }

    /// Builds the single-source relation an `UPDATE` / `DELETE` evaluates
    /// against, labelled with the name column references may use.
    fn target_relation<'a>(def: &'a TableDef, label: &str, trigger: Option<&'a TriggerFrame>) -> Relation<'a> {
        let width = def.columns.len();
        Relation {
            sources: vec![Source {
                label: label.to_string(),
                columns: SourceColumns::Table(def),
                offset: 0,
                join: None,
                star_hidden: vec![false; width],
                unqualified_hidden: vec![false; width],
                star_override: vec![None; width],
            }],
            using: vec![Vec::new()],
            using_slots: Vec::new(),
            trigger,
            excluded: false,
        }
    }

    /// Relation used by `ON CONFLICT DO UPDATE`: `excluded.<column>` binds
    /// against the supplied row while bare columns keep the stored row.
    fn upsert_relation<'a>(def: &'a TableDef, label: &str) -> Relation<'a> {
        let mut relation = Self::target_relation(def, label, None);
        relation.excluded = true;
        relation
    }

    /// Reads every row of `def`, decoded and sorted by rowid.
    fn live_rows(&self, def: &TableDef, statement: &'static str) -> Result<Vec<(i64, Vec<Cell>)>, SqlError> {
        if def.temp {
            let position = *self
                .temp_lookup
                .get(&def.name.to_ascii_lowercase())
                .ok_or_else(|| SqlError::new("storage", format!("temp table `{}` disappeared", def.name)).in_statement(statement))?;
            return Ok(self.temp_tables[position].rows.iter().map(|(rowid, cells)| (*rowid, cells.clone())).collect());
        }
        if def.without_rowid {
            // WITHOUT ROWID rows carry no rowid; a scan position is used as
            // the identity the update / delete planners sort by.
            let stored = self
                .writer
                .without_rowid_rows(&def.name, TableLimits::default())
                .map_err(|error| storage_failure(error).in_statement(statement))?;
            let mut rows = Vec::with_capacity(stored.len());
            for (position, payload) in stored.iter().enumerate() {
                rows.push((position as i64, def.decode_without_rowid(payload)?));
            }
            return Ok(rows);
        }
        let stored = self
            .writer
            .table_rows(&def.name, TableLimits::default())
            .map_err(|error| storage_failure(error).in_statement(statement))?;
        let mut rows = Vec::with_capacity(stored.len());
        for row in &stored {
            rows.push((row.rowid, decode_row(&row.payload, def, row.rowid)?));
        }
        rows.sort_by_key(|(rowid, _)| *rowid);
        Ok(rows)
    }

    /// 按候选 rowid 读行,候选升序取回。
    ///
    /// 相邻间隔不超过 [`CANDIDATE_GAP`] 的候选合并成一次区间读,避免逐行下降。
    /// 区间读会带回缝隙里的少量非候选行,谓词与唯一性判定照旧处理,因此只是多读
    /// 几行,不会改变语句结果。
    fn live_rows_at(&self, def: &TableDef, statement: &'static str, rowids: &[i64]) -> Result<Vec<(i64, Vec<Cell>)>, SqlError> {
        if def.temp {
            return Ok(self.live_rows(def, statement)?.into_iter().filter(|(rowid, _)| rowids.contains(rowid)).collect());
        }
        let mut sorted = rowids.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let mut rows = Vec::with_capacity(sorted.len());
        let mut position = 0;
        while position < sorted.len() {
            let start = sorted[position];
            let mut end = start;
            let mut next = position + 1;
            while next < sorted.len() && sorted[next] - end <= CANDIDATE_GAP {
                end = sorted[next];
                next += 1;
            }
            let stored = self
                .writer
                .table_range(&def.name, start, end, TableLimits::default())
                .map_err(|error| storage_failure(error).in_statement(statement))?;
            for row in &stored {
                rows.push((row.rowid, decode_row(&row.payload, def, row.rowid)?));
            }
            position = next;
        }
        rows.sort_by_key(|(rowid, _)| *rowid);
        Ok(rows)
    }

    /// `DELETE` 的候选 rowid:裸 `<rowid> = <键>` 等值,或普通列索引的探针结果。
    ///
    /// 与 SELECT 的 `rowid_pin` / `index_pin` 同一判定:只认裸 `AND` 链上的等值与
    /// 首列范围比较,键可以是字面量或执行期已绑定的参数,索引必须是未被托管改造的
    /// 普通列索引。谓词仍会在候选行上重跑一遍,所以候选只是超集,不会改变删除集合。
    fn delete_candidates(&self, def: &TableDef, table_key: &str, table: &str, alias: Option<&str>, filter: Option<&Expr>) -> Option<Vec<i64>> {
        if def.without_rowid || def.temp {
            return None;
        }
        self.write_candidates(def, table_key, alias.unwrap_or(table), filter)
    }

    /// `UPDATE` 的候选 rowid,约束与 [`Engine::delete_candidates`] 相同。
    ///
    /// `UPDATE` 还要用整表做唯一性冲突判定,因此只在「唯一键只有 rowid,且没有任何
    /// 赋值改动 rowid」时才收窄。这两条成立时,收窄后的行集与整表在冲突判定上等价;
    /// 外键检查只看被写入的行,因此不需要整表。`WITHOUT ROWID` 表一律走整表读取。
    fn update_candidates(
        &self,
        def: &TableDef,
        table_key: &str,
        table: &str,
        alias: Option<&str>,
        filter: Option<&Expr>,
        targets: &[(usize, ResolvedExpr)],
    ) -> Option<Vec<i64>> {
        if def.without_rowid || def.temp {
            return None;
        }
        if targets.iter().any(|(index, _)| Some(*index) == def.rowid_alias) {
            return None;
        }
        if self.unique_keys(def, table_key).iter().any(|key| !matches!(key, UniqueKey::Rowid(_))) {
            return None;
        }
        self.write_candidates(def, table_key, alias.unwrap_or(table), filter)
    }

    /// 按谓词取写语句的候选 rowid:先认裸 `<rowid> = <键>`,再逐个普通列索引试
    /// 全键等值与首列范围。返回的 rowid 升序,可能含谓词不成立的行。
    fn write_candidates(&self, def: &TableDef, table_key: &str, label: &str, filter: Option<&Expr>) -> Option<Vec<i64>> {
        let filter = filter?;
        let alias_name = def.rowid_alias.map(|index| def.columns[index].name.clone());
        if let Some(rowid) = Self::rowid_eq_conjunct(filter, alias_name.as_deref(), label, &self.parameters) {
            return Some(vec![rowid]);
        }
        for index in self.indexes.values() {
            if index.managed || index.table != table_key || index.columns.is_empty() {
                continue;
            }
            if let Some(rowids) = self.index_probe_rows(index, def, filter, label) {
                return Some(rowids);
            }
            if let Some(rowids) = self.index_range_rows(index, def, filter, label) {
                return Some(rowids);
            }
        }
        None
    }

    /// Resolves one `ON CONFLICT` clause against the insert target: the
    /// uniqueness rules its target names, plus the `DO UPDATE` assignments
    /// evaluated against the stored row and the supplied (`excluded`) row.
    fn prepare_upsert(&self, def: &TableDef, table_key: &str, label: &str, clause: &ConflictClause) -> Result<UpsertPlan, SqlError> {
        let mismatch = || {
            SqlError::new("conflict-target", "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint").in_statement("insert")
        };
        // The target predicate is resolved against the insert target alone:
        // `excluded` is not in scope there, exactly like SQLite.
        let target_predicate = match (&clause.target, &clause.predicate) {
            (Some(_), Some(expression)) => {
                let relation = Self::target_relation(def, label, None);
                Some(Self::resolve_upsert_expr(&relation, expression)?)
            }
            (Some(_), None) => None,
            (None, Some(_)) => return Err(mismatch()),
            (None, None) => None,
        };
        let positions = match &clause.target {
            None => None,
            Some(columns) => {
                let mut positions = Vec::with_capacity(columns.len());
                for column in columns {
                    let position = def
                        .columns
                        .iter()
                        .position(|existing| existing.name.eq_ignore_ascii_case(column))
                        .ok_or_else(|| SqlError::new("unknown-column", format!("no such column: {column}")).in_statement("insert"))?;
                    positions.push(position);
                }
                Some(positions)
            }
        };
        let all_keys = self.unique_keys(def, table_key);
        let (keys, others) = match &positions {
            None => {
                let mut keys = all_keys;
                if def.without_rowid {
                    keys.retain(|key| !matches!(key, UniqueKey::Rowid(_)));
                }
                (keys, Vec::new())
            }
            Some(positions) if target_predicate.is_some() => {
                // A target `WHERE` can only name a partial index, so no plain
                // unique constraint or full index is the conflict resolver.
                (Vec::new(), all_keys)
            }
            Some(positions) => {
                let keys = self.upsert_target_keys(def, table_key, positions);
                let others = all_keys
                    .into_iter()
                    .filter(|key| match key {
                        UniqueKey::Rowid(_) => !(positions.len() == 1 && def.rowid_alias == Some(positions[0])),
                        UniqueKey::Columns(existing, _, _) => !same_positions(existing, positions),
                    })
                    .collect();
                (keys, others)
            }
        };
        let all_managed = self.upsert_managed_indexes(def, table_key);
        let (managed, other_managed) = match &positions {
            None => (all_managed, Vec::new()),
            Some(positions) => {
                let mut managed = Vec::new();
                let mut other_managed = Vec::new();
                for target in all_managed {
                    if managed_target_matches(&target, positions, target_predicate.as_ref()) {
                        managed.push(target);
                    }
                    else {
                        other_managed.push(target);
                    }
                }
                (managed, other_managed)
            }
        };
        if positions.is_some() && keys.is_empty() && managed.is_empty() {
            return Err(mismatch());
        }
        let (action, assignments, filter) = match &clause.action {
            ConflictAction::Nothing => (UpsertAction::Nothing, Vec::new(), None),
            ConflictAction::Update { assignments, predicate } => {
                let relation = Self::upsert_relation(def, label);
                let mut targets = Vec::with_capacity(assignments.len());
                for assignment in assignments {
                    let index =
                        def.columns.iter().position(|column| column.name.eq_ignore_ascii_case(&assignment.column)).ok_or_else(|| {
                            SqlError::new("unknown-column", format!("no such column: {}", assignment.column)).in_statement("insert")
                        })?;
                    targets.push((index, Self::resolve_upsert_expr(&relation, &assignment.value)?));
                }
                let filter = match predicate {
                    Some(expression) => Some(Self::resolve_upsert_expr(&relation, expression)?),
                    None => None,
                };
                (UpsertAction::Update, targets, filter)
            }
        };
        Ok(UpsertPlan { keys, managed, others, other_managed, action, assignments, filter })
    }

    /// Resolves one expression of a `DO UPDATE` clause against the upsert
    /// relation, which exposes both the stored row and `excluded.<column>`.
    fn resolve_upsert_expr(relation: &Relation, expression: &Expr) -> Result<ResolvedExpr, SqlError> {
        let mut windows = Vec::new();
        let mut aggregates = Vec::new();
        resolve_expr(expression, relation, &[], &[], &mut windows, &mut aggregates, false, false).map_err(|error| error.in_statement("insert"))
    }

    /// Every unique engine-managed index of `table`, which includes partial and
    /// expression indexes.
    fn upsert_managed_indexes(&self, def: &TableDef, table_key: &str) -> Vec<ManagedTarget> {
        if def.temp {
            return Vec::new();
        }
        self.indexes
            .values()
            .filter(|index| index.managed && index.unique && index.table == table_key)
            .map(|index| {
                let label = managed_index_label(def, index);
                ManagedTarget { index: index.clone(), label }
            })
            .collect()
    }

    /// The uniqueness rules one `ON CONFLICT (columns)` target matches.
    fn upsert_target_keys(&self, def: &TableDef, table_key: &str, positions: &[usize]) -> Vec<UniqueKey> {
        self.unique_keys(def, table_key)
            .into_iter()
            .filter(|key| match key {
                UniqueKey::Rowid(_) => !def.without_rowid && positions.len() == 1 && def.rowid_alias == Some(positions[0]),
                UniqueKey::Columns(existing, _, _) => same_positions(existing, positions),
            })
            .collect()
    }

    /// The key values one managed index produces for a row, or `None` when the
    /// row is outside the index's partial predicate.
    fn managed_target_key(&self, def: &TableDef, target: &ManagedTarget, cells: &[Cell]) -> Result<Option<Vec<Cell>>, SqlError> {
        let shape = TargetShape::new(def, &def.name, None);
        let budget = Budget::new();
        let aggregates: Vec<Cell> = Vec::new();
        let windows: Vec<Vec<Cell>> = Vec::new();
        let context = EvalContext {
            engine: self,
            outer: &[],
            current: &shape.scope,
            aggregates: &aggregates,
            windows: &windows,
            window_row: 0,
            budget: &budget,
            ctes: None,
        };
        if let Some(predicate) = &target.index.predicate {
            if truthiness(&predicate.eval(cells, &context)?) != Some(true) {
                return Ok(None);
            }
        }
        let mut key = Vec::with_capacity(target.index.terms.len());
        for term in &target.index.terms {
            key.push(term.eval(cells, &context)?);
        }
        Ok(Some(key))
    }

    /// The first uniqueness rule outside the conflict target the supplied row
    /// still violates; SQLite aborts the statement instead of resolving it.
    fn upsert_other_conflict(
        &self,
        def: &TableDef,
        plan: &UpsertPlan,
        candidate: &[Cell],
        candidate_rowid: i64,
        live: &[(i64, Vec<Cell>)],
        skip: i64,
    ) -> Result<Option<String>, SqlError> {
        for key in &plan.others {
            let wanted = key.values(candidate, candidate_rowid);
            for (rowid, cells) in live {
                if *rowid == skip {
                    continue;
                }
                let other = key.values(cells, *rowid);
                let collides = wanted.iter().zip(&other).all(|(first, second)| {
                    !matches!(first, Cell::Null) && !matches!(second, Cell::Null) && compare_cells(first, second) == Ordering::Equal
                });
                if collides {
                    return Ok(Some(key.label().to_string()));
                }
            }
        }
        for target in &plan.other_managed {
            let Some(wanted) = self.managed_target_key(def, target, candidate)?
            else {
                continue;
            };
            for (rowid, cells) in live {
                if *rowid == skip {
                    continue;
                }
                if let Some(other) = self.managed_target_key(def, target, cells)? {
                    if managed_keys_conflict(&wanted, &other, &target.index.collations) {
                        return Ok(Some(target.label.clone()));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Whether an `ON CONFLICT (rowid-alias)` target can be resolved by
    /// probing the primary key instead of materializing the whole table.
    ///
    /// The fast path only applies when nothing else can observe the table:
    /// no temporary shadow table, no other uniqueness rule, no managed unique
    /// index, no trigger, no foreign key, and no assignment that moves the
    /// rowid. Everything else keeps the full-scan [`Self::apply_upsert`].
    fn upsert_rowid_fast_path(&self, def: &TableDef, table_key: &str, plan: &UpsertPlan, temp_position: Option<usize>) -> bool {
        temp_position.is_none()
            && def.rowid_alias.is_some()
            && matches!(plan.keys.as_slice(), [UniqueKey::Rowid(_)])
            && plan.managed.is_empty()
            && plan.others.is_empty()
            && plan.other_managed.is_empty()
            && plan.assignments.iter().all(|(position, _)| Some(*position) != def.rowid_alias)
            && (!self.foreign_keys || (def.foreign_keys.is_empty() && self.referencing_foreign_keys(&def.name).is_empty()))
            && !self.table_has_triggers(table_key)
    }

    /// Applies one supplied row of a rowid-keyed upsert by probing only the
    /// conflicting primary-key row.
    ///
    /// `None` means the row has no explicit rowid value, so the caller must
    /// fall back to the scanning path. Rows written earlier in the same
    /// statement are already in storage, so a later supplied row still sees
    /// them exactly as a full scan would.
    #[allow(clippy::too_many_arguments)]
    fn apply_upsert_rowid(
        &mut self,
        def: &TableDef,
        table_key: &str,
        table: &str,
        plan: &UpsertPlan,
        slots: &[Option<crate::WriteValue>],
        returning: Option<&ReturningPlan>,
        shape: &ScopeShape,
        budget: &Budget,
    ) -> Result<Option<UpsertOutcome>, SqlError> {
        let width = def.columns.len();
        let Some(alias) = def.rowid_alias
        else {
            return Ok(None);
        };
        let rowid = match slots.get(alias).and_then(|slot| slot.as_ref()) {
            Some(crate::WriteValue::Integer(rowid)) => *rowid,
            _ => return Ok(None),
        };
        let payload = self
            .writer
            .row_payload(table, rowid)
            .map_err(|error| storage_failure(error).in_statement("insert"))?;
        let Some(payload) = payload
        else {
            let inserted = self
                .writer
                .insert(table, slots)
                .map_err(|error| storage_failure(error).in_statement("insert"))?;
            return Ok(Some(UpsertOutcome::Inserted(inserted)));
        };
        if matches!(plan.action, UpsertAction::Nothing) {
            return Ok(Some(UpsertOutcome::Skipped));
        }
        // Lean rewrite: every assignment is a direct `excluded.<column>` with no
        // `DO UPDATE WHERE`, no `RETURNING` and no `CHECK` constraints. The stored
        // row keeps its unassigned columns and the assigned ones take the
        // supplied (already affinity-normalized) values, so the expression-eval
        // machinery, the joined-row map and `plan_update` are skipped entirely.
        let lean = returning.is_none()
            && plan.filter.is_none()
            && def.checks.is_empty()
            && plan.assignments.iter().all(|(_, expr)| matches!(expr, ResolvedExpr::ExcludedColumn { .. }));
        if lean {
            let candidate = check_row(def, slots, rowid);
            let mut values: Vec<UpdateValue> = vec![UpdateValue::Keep; width];
            for (index, expr) in &plan.assignments {
                let ResolvedExpr::ExcludedColumn { index: supplied } = expr else {
                    unreachable!("lean upsert assignments are all excluded-column references")
                };
                let value = cell_write_value(&candidate[*supplied]);
                if let Some(error) = strict_value_violation(def, *index, &value, "insert") {
                    return Err(error);
                }
                values[*index] = UpdateValue::Set(value);
            }
            if let Err(error) = self.writer.update(table, rowid, &values) {
                return Err(write_failure(error, "insert", def));
            }
            return Ok(Some(UpsertOutcome::Updated { rowid, rows: Vec::new() }));
        }
        let stored = decode_row(&payload, def, rowid)?;
        let candidate = check_row(def, slots, rowid);
        let mut eval_cells = stored.clone();
        eval_cells.push(Cell::Integer(rowid));
        self.upsert_excluded = candidate[..width].to_vec();
        if let Some(filter) = &plan.filter {
            let ctx =
                EvalContext { engine: self, outer: &[], current: shape, aggregates: &[], windows: &[], window_row: 0, budget, ctes: None };
            let keep = truthiness(&filter.eval(&eval_cells, &ctx)?) == Some(true);
            if !keep {
                self.upsert_excluded.clear();
                return Ok(Some(UpsertOutcome::Skipped));
            }
        }
        let mut joined = BTreeMap::new();
        joined.insert(rowid, eval_cells);
        let state = vec![(rowid, stored)];
        let planned = self.plan_update(def, table_key, &plan.assignments, None, Some(&joined), returning, shape, budget, Some(&state), None);
        self.upsert_excluded.clear();
        let planned = planned?;
        if planned.writes.is_empty() {
            return Ok(Some(UpsertOutcome::Skipped));
        }
        for (_, values) in &planned.writes {
            for (position, value) in values.iter().enumerate() {
                if let crate::UpdateValue::Set(value) = value {
                    if let Some(error) = strict_value_violation(def, position, value, "insert") {
                        return Err(error);
                    }
                }
            }
        }
        for (write_rowid, values) in &planned.writes {
            if let Err(error) = self.writer.update(table, *write_rowid, values) {
                return Err(write_failure(error, "insert", def));
            }
        }
        Ok(Some(UpsertOutcome::Updated { rowid, rows: planned.returning }))
    }

    /// Applies one supplied row of an upsert: insert it, skip it, or rewrite the
    /// stored row it conflicts with. `live` is the statement's own view of the
    /// table so a later supplied row sees rows written earlier in the same
    /// statement, exactly like SQLite.
    #[allow(clippy::too_many_arguments)]
    fn apply_upsert(
        &mut self,
        def: &TableDef,
        table_key: &str,
        table: &str,
        plan: &UpsertPlan,
        slots: &[Option<crate::WriteValue>],
        temp_position: Option<usize>,
        live: &mut Vec<(i64, Vec<Cell>)>,
        returning: Option<&ReturningPlan>,
        shape: &ScopeShape,
        budget: &Budget,
    ) -> Result<UpsertOutcome, SqlError> {
        let width = def.columns.len();
        // The rowid the supplied row will occupy. An explicit `INTEGER PRIMARY
        // KEY` value is used as written, otherwise SQLite's `max(rowid) + 1`.
        let explicit_rowid = def.rowid_alias.and_then(|alias| slots.get(alias).and_then(|slot| slot.as_ref())).and_then(|value| match value {
            crate::WriteValue::Integer(value) => Some(*value),
            _ => None,
        });
        let candidate_rowid = match explicit_rowid {
            Some(rowid) => rowid,
            None => live.iter().map(|(rowid, _)| *rowid).max().unwrap_or(0) + 1,
        };
        let candidate = check_row(def, slots, candidate_rowid);
        let mut conflict_rowid = None;
        for key in &plan.keys {
            for (rowid, cells) in live.iter() {
                if upsert_key_conflicts(key, &candidate, candidate_rowid, *rowid, cells) {
                    conflict_rowid = Some(*rowid);
                    break;
                }
            }
            if conflict_rowid.is_some() {
                break;
            }
        }
        if conflict_rowid.is_none() {
            for target in &plan.managed {
                let Some(wanted) = self.managed_target_key(def, target, &candidate)?
                else {
                    continue;
                };
                for (rowid, cells) in live.iter() {
                    if let Some(other) = self.managed_target_key(def, target, cells)? {
                        if managed_keys_conflict(&wanted, &other, &target.index.collations) {
                            conflict_rowid = Some(*rowid);
                            break;
                        }
                    }
                }
                if conflict_rowid.is_some() {
                    break;
                }
            }
        }
        let unique_failed =
            |label: String| SqlError::new("unique-constraint", format!("UNIQUE constraint failed: {label}")).in_statement("insert");
        let Some(rowid) = conflict_rowid
        else {
            if let Some(label) = self.upsert_other_conflict(def, plan, &candidate, candidate_rowid, live, -1)? {
                return Err(unique_failed(label));
            }
            let inserted = if let Some(position) = temp_position {
                let keys = self.unique_keys(def, table_key);
                let table_state = &mut self.temp_tables[position];
                temp_insert_row(def, &mut table_state.rows, slots, &keys)?
            }
            else {
                self.writer.insert(table, slots).map_err(|error| storage_failure(error).in_statement("insert"))?
            };
            live.push((inserted, candidate[..width].to_vec()));
            return Ok(UpsertOutcome::Inserted(inserted));
        };
        if matches!(plan.action, UpsertAction::Nothing) {
            if let Some(label) = self.upsert_other_conflict(def, plan, &candidate, candidate_rowid, live, -1)? {
                return Err(unique_failed(label));
            }
            return Ok(UpsertOutcome::Skipped);
        }
        if let Some(label) = self.upsert_other_conflict(def, plan, &candidate, candidate_rowid, live, rowid)? {
            return Err(unique_failed(label));
        }
        // `DO UPDATE` rewrites the conflicting row the query planner would have
        // matched: the stored row plus this rowid, narrowed by the clause's own
        // `WHERE`.
        let mut predicate = ResolvedExpr::Compare {
            left: Box::new(ResolvedExpr::Column { source: 0, index: width }),
            op: CompareOp::Eq,
            right: Box::new(ResolvedExpr::Cell(Cell::Integer(rowid))),
            collation: Collation::Binary,
        };
        if let Some(filter) = &plan.filter {
            predicate = ResolvedExpr::And(Box::new(predicate), Box::new(filter.clone()));
        }
        self.upsert_excluded = candidate[..width].to_vec();
        let planned = self.plan_update(def, table_key, &plan.assignments, Some(&predicate), None, returning, shape, budget, None, None);
        self.upsert_excluded.clear();
        let planned = planned?;
        if planned.writes.is_empty() {
            // `DO UPDATE ... WHERE` was false: SQLite leaves the row alone.
            return Ok(UpsertOutcome::Skipped);
        }
        for (_, values) in &planned.writes {
            for (position, value) in values.iter().enumerate() {
                if let crate::UpdateValue::Set(value) = value {
                    if let Some(error) = strict_value_violation(def, position, value, "insert") {
                        return Err(error);
                    }
                }
            }
        }
        let find_old = |rowid: i64| live.iter().find(|(candidate, _)| *candidate == rowid).map(|(_, cells)| cells.clone()).unwrap_or_default();
        // A `DO UPDATE` is an ordinary UPDATE for trigger and foreign-key
        // purposes: SQLite fires the update triggers with the rewritten row and
        // re-checks references the statement moves.
        let queue_triggers = self.table_has_triggers(table_key);
        let trigger_columns = trigger_frame_columns(def);
        let assigned_names: Vec<String> = plan.assignments.iter().map(|(position, _)| def.columns[*position].name.clone()).collect();
        let assigned: Vec<bool> = (0..width).map(|position| plan.assignments.iter().any(|(target, _)| *target == position)).collect();
        let mut events: Vec<(i64, Vec<Cell>, Vec<Cell>)> = Vec::new();
        if let Some(position) = temp_position {
            let table_state = &mut self.temp_tables[position];
            for (write_rowid, values) in &planned.writes {
                let cells = table_state
                    .rows
                    .get_mut(write_rowid)
                    .ok_or_else(|| SqlError::new("storage", "temp row disappeared").in_statement("insert"))?;
                let old_cells = cells.clone();
                for (slot, value) in values.iter().enumerate() {
                    if let crate::UpdateValue::Set(value) = value {
                        cells[slot] = write_value_cell(value);
                    }
                }
                if queue_triggers {
                    events.push((*write_rowid, old_cells, cells.clone()));
                }
            }
        }
        else {
            for (write_rowid, values) in &planned.writes {
                let old_cells = find_old(*write_rowid);
                let mut new_cells = old_cells.clone();
                if new_cells.len() < width {
                    new_cells.resize(width, Cell::Null);
                }
                for (slot, value) in values.iter().enumerate() {
                    if let crate::UpdateValue::Set(value) = value {
                        new_cells[slot] = write_value_cell(value);
                    }
                }
                if queue_triggers {
                    self.queue_update_before(table_key, def, &trigger_columns, &assigned_names, *write_rowid, &old_cells, &new_cells);
                    events.push((*write_rowid, old_cells.clone(), new_cells.clone()));
                }
                let written = if def.without_rowid {
                    let old_values: Vec<crate::WriteValue> = old_cells.iter().map(cell_write_value).collect();
                    let new_values: Vec<crate::WriteValue> = new_cells.iter().map(cell_write_value).collect();
                    self.writer.update_without_rowid(table, &old_values, &new_values)
                }
                else {
                    self.writer.update(table, *write_rowid, values)
                };
                if let Err(error) = written {
                    return Err(write_failure(error, "insert", def));
                }
            }
        }
        if temp_position.is_some() {
            for (updated_rowid, old_cells, new_cells) in &events {
                self.queue_update_events(table_key, def, &trigger_columns, &assigned_names, *updated_rowid, old_cells, new_cells);
            }
        }
        if temp_position.is_none() && self.foreign_keys {
            let changes: Vec<(Vec<Cell>, Option<Vec<Cell>>)> = planned
                .writes
                .iter()
                .map(|(write_rowid, values)| {
                    let old = find_old(*write_rowid);
                    let mut next = old.clone();
                    if next.len() < width {
                        next.resize(width, Cell::Null);
                    }
                    for (slot, value) in values.iter().enumerate() {
                        if let crate::UpdateValue::Set(value) = value {
                            next[slot] = write_value_cell(value);
                        }
                    }
                    (old, Some(next))
                })
                .collect();
            let (index_needed, without_needed) = self.apply_foreign_key_actions(def, &changes, "insert", 0)?;
            if index_needed {
                self.materialize_indexes_after_write("insert")?;
            }
            if without_needed {
                self.materialize_without_rowid_after_write("insert")?;
            }
        }
        if self.foreign_keys {
            self.check_update_foreign_keys(def, &planned, live, &assigned, "insert")?;
        }
        if temp_position.is_none() {
            for (updated_rowid, old_cells, new_cells) in &events {
                self.queue_update_after(table_key, def, &trigger_columns, &assigned_names, *updated_rowid, old_cells, new_cells);
            }
        }
        if temp_position.is_none() && !self.table_has_triggers(table_key) {
            // Without triggers nothing else can rewrite the table, so the
            // statement's own writes are enough to keep the in-memory rows
            // current; reloading every row per upsert would make a batch of
            // single-row upserts quadratic.
            for (write_rowid, values) in &planned.writes {
                if let Some((_, cells)) = live.iter_mut().find(|(candidate, _)| *candidate == *write_rowid) {
                    for (slot, value) in values.iter().enumerate() {
                        if let (crate::UpdateValue::Set(value), Some(cell)) = (value, cells.get_mut(slot)) {
                            *cell = write_value_cell(value);
                        }
                    }
                }
            }
        }
        else {
            *live = self.live_rows(def, "insert")?;
        }
        let rowid = planned.writes.first().map(|(rowid, _)| *rowid).unwrap_or(rowid);
        Ok(UpsertOutcome::Updated { rowid, rows: planned.returning })
    }

    /// Applies one `REPLACE` row: delete every stored row that conflicts on a
    /// uniqueness rule, then insert the supplied row.
    ///
    /// The delete is an ordinary row removal for foreign-key purposes, but a
    /// `DELETE` trigger only fires when `recursive_triggers` is on, matching
    /// SQLite's documented `REPLACE` behavior.
    #[allow(clippy::too_many_arguments)]
    fn apply_replace(
        &mut self,
        def: &TableDef,
        table_key: &str,
        table: &str,
        slots: &[Option<crate::WriteValue>],
        temp_position: Option<usize>,
        recursive_triggers: bool,
        queue_triggers: bool,
        trigger_columns: &[String],
        deleted: &mut Vec<(i64, Vec<Cell>)>,
    ) -> Result<i64, SqlError> {
        let live = self.live_rows(def, "insert")?;
        let explicit_rowid = def.rowid_alias.and_then(|alias| slots.get(alias).and_then(|slot| slot.as_ref())).and_then(|value| match value {
            crate::WriteValue::Integer(value) => Some(*value),
            _ => None,
        });
        let candidate_rowid = match explicit_rowid {
            Some(rowid) => rowid,
            None => live.iter().map(|(rowid, _)| *rowid).max().unwrap_or(0) + 1,
        };
        let candidate = check_row(def, slots, candidate_rowid);
        let mut conflicts: BTreeSet<i64> = BTreeSet::new();
        for unique in self.unique_keys(def, table_key) {
            for (rowid, cells) in &live {
                if upsert_key_conflicts(&unique, &candidate, candidate_rowid, *rowid, cells) {
                    conflicts.insert(*rowid);
                }
            }
        }
        for target in self.upsert_managed_indexes(def, table_key) {
            let Some(wanted) = self.managed_target_key(def, &target, &candidate)?
            else {
                continue;
            };
            for (rowid, cells) in &live {
                if let Some(other) = self.managed_target_key(def, &target, cells)? {
                    if managed_keys_conflict(&wanted, &other, &target.index.collations) {
                        conflicts.insert(*rowid);
                    }
                }
            }
        }
        for rowid in conflicts {
            let old = live.iter().find(|(candidate, _)| *candidate == rowid).map(|(_, cells)| cells.clone()).unwrap_or_default();
            if queue_triggers && recursive_triggers {
                self.queue_delete_before(table_key, trigger_columns, rowid, &old);
            }
            if let Some(position) = temp_position {
                self.temp_tables[position].rows.remove(&rowid);
            }
            else {
                let removed = if def.without_rowid {
                    let old_values: Vec<crate::WriteValue> = old.iter().map(cell_write_value).collect();
                    self.writer.delete_without_rowid(table, &old_values)
                }
                else {
                    self.writer.delete(table, rowid)
                };
                if let Err(error) = removed {
                    return Err(write_failure(error, "insert", def));
                }
            }
            deleted.push((rowid, old.clone()));
            if queue_triggers && recursive_triggers {
                // `AFTER DELETE` is queued as soon as the row is gone, so the
                // replacement's delete trigger runs before its insert trigger.
                self.queue_delete_after(table_key, trigger_columns, rowid, &old);
            }
        }
        if queue_triggers {
            // The conflict-resolution deletes have already run, so a
            // `BEFORE INSERT` trigger observes the state after them.
            let before_rowid = def
                .rowid_alias
                .and_then(|alias| slots.get(alias).and_then(|slot| slot.as_ref()))
                .and_then(|value| match value {
                    crate::WriteValue::Integer(value) => Some(*value),
                    _ => None,
                })
                .unwrap_or(-1);
            self.pending_events.push(PendingTriggerEvent {
                table: table_key.to_string(),
                timing: TriggerTiming::Before,
                event: TriggerEvent::Insert,
                columns: trigger_columns.to_vec(),
                new: Some(check_row(def, slots, before_rowid)),
                old: None,
            });
        }
        if let Some(position) = temp_position {
            let keys = self.unique_keys(def, table_key);
            let table_state = &mut self.temp_tables[position];
            temp_insert_row(def, &mut table_state.rows, slots, &keys)
        }
        else {
            self.writer.insert(table, slots).map_err(|error| storage_failure(error).in_statement("insert"))
        }
    }

    /// Computes the `(key values, rowid)` entries of one managed index by
    /// evaluating its terms and predicate against every live row.
    fn managed_index_entries(
        &self,
        def: &TableDef,
        terms: &[ResolvedExpr],
        predicate: Option<&ResolvedExpr>,
        rows: &[(i64, Vec<Cell>)],
    ) -> Result<Vec<(Vec<crate::WriteValue>, i64)>, SqlError> {
        let shape = TargetShape::new(def, &def.name, None);
        let budget = Budget::new();
        let aggregates: Vec<Cell> = Vec::new();
        let windows: Vec<Vec<Cell>> = Vec::new();
        let mut entries = Vec::with_capacity(rows.len());
        for (rowid, cells) in rows {
            let context = EvalContext {
                engine: self,
                outer: &[],
                current: &shape.scope,
                aggregates: &aggregates,
                windows: &windows,
                window_row: 0,
                budget: &budget,
                ctes: None,
            };
            if let Some(predicate) = predicate {
                if truthiness(&predicate.eval(cells, &context)?) != Some(true) {
                    continue;
                }
            }
            let mut key = Vec::with_capacity(terms.len());
            for term in terms {
                key.push(cell_write_value(&term.eval(cells, &context)?));
            }
            entries.push((key, *rowid));
        }
        Ok(entries)
    }

    /// Rebuilds the key sets of every managed index on `table` from the live
    /// rows. Called inside the statement's transaction, before it commits.
    fn refresh_managed_indexes(&mut self, table: &str, def: &TableDef, statement: &'static str) -> Result<(), SqlError> {
        let managed: Vec<IndexDef> =
            self.indexes.values().filter(|index| index.table.eq_ignore_ascii_case(table) && index.managed).cloned().collect();
        if managed.is_empty() {
            return Ok(());
        }
        let rows = self.live_rows(def, statement)?;
        for index in managed {
            let entries = self.managed_index_entries(def, &index.terms, index.predicate.as_ref(), &rows)?;
            self.writer
                .replace_managed_index_entries(&index.name, &entries)
                .map_err(|error| storage_failure(error).in_statement(statement))?;
        }
        Ok(())
    }

    /// Resolves one foreign key against the live catalog: the parent table
    /// position and the parent key column positions.
    ///
    /// A missing parent table and a parent key that is not unique both report
    /// the codes SQLite uses (`SQLITE_ERROR`), because the same statements
    /// fail identically in the reference engine.
    fn foreign_key_parent(&self, child: &TableDef, key: &ForeignKeyDef, statement: &'static str) -> Result<(usize, Vec<usize>), SqlError> {
        let parent_position = *self
            .table_lookup
            .get(&key.parent)
            .ok_or_else(|| SqlError::new("SQLITE_ERROR", format!("no such table: main.{}", key.parent_display)).in_statement(statement))?;
        let parent = &self.tables[parent_position];
        let positions = if key.parent_columns.is_empty() {
            parent.primary_key_positions().ok_or_else(|| foreign_key_mismatch(child, key, statement))?
        }
        else {
            let mut positions = Vec::with_capacity(key.parent_columns.len());
            for name in &key.parent_columns {
                let position = parent
                    .columns
                    .iter()
                    .position(|column| column.name.eq_ignore_ascii_case(name))
                    .ok_or_else(|| foreign_key_mismatch(child, key, statement))?;
                positions.push(position);
            }
            positions
        };
        if positions.len() != key.columns.len() || !self.parent_key_is_unique(parent, &positions) {
            return Err(foreign_key_mismatch(child, key, statement));
        }
        Ok((parent_position, positions))
    }

    /// Whether `positions` identifies at most one parent row: the primary key,
    /// a column-level `UNIQUE` / `PRIMARY KEY`, or an explicit unique index.
    fn parent_key_is_unique(&self, parent: &TableDef, positions: &[usize]) -> bool {
        if let Some(alias) = parent.rowid_alias {
            if positions == [alias] {
                return true;
            }
        }
        if !parent.primary_key.is_empty() && same_positions(&parent.primary_key, positions) {
            return true;
        }
        let declared: Vec<usize> =
            parent.columns.iter().enumerate().filter(|(_, column)| column.primary_key).map(|(position, _)| position).collect();
        if !declared.is_empty() && same_positions(&declared, positions) {
            return true;
        }
        if positions.len() == 1 && parent.columns[positions[0]].unique {
            return true;
        }
        if positions.iter().all(|position| parent.columns[*position].unique) {
            return true;
        }
        let parent_key = parent.name.to_ascii_lowercase();
        self.indexes.values().any(|index| index.table == parent_key && index.unique && same_positions(&index.columns, positions))
    }

    /// Every foreign key in the catalog that references `parent`, as
    /// `(child table position, key)` pairs; the child may be `parent` itself.
    fn referencing_foreign_keys(&self, parent: &str) -> Vec<(usize, ForeignKeyDef)> {
        let mut found = Vec::new();
        for (position, table) in self.tables.iter().enumerate() {
            for key in &table.foreign_keys {
                if key.parent.eq_ignore_ascii_case(parent) {
                    found.push((position, key.clone()));
                }
            }
        }
        found
    }

    /// Verifies the rows an `INSERT` just wrote against every foreign key of
    /// their table. The check runs after the writes so a self-reference may
    /// point at a later row of the same statement.
    fn check_insert_foreign_keys(&self, def: &TableDef, rowids: &[i64], statement: &'static str) -> Result<(), SqlError> {
        if def.foreign_keys.is_empty() {
            return Ok(());
        }
        let live = self.live_rows(def, statement)?;
        for key in &def.foreign_keys {
            if key.deferred && self.in_transaction() {
                continue;
            }
            let (parent_position, parent_columns) = self.foreign_key_parent(def, key, statement)?;
            let parent = &self.tables[parent_position];
            let parent_rows = self.live_rows(parent, statement)?;
            for (rowid, cells) in &live {
                if !rowids.contains(rowid) {
                    continue;
                }
                let values = pick(cells, &key.columns);
                // A NULL in any child key column satisfies the constraint.
                if values.iter().any(|cell| matches!(cell, Cell::Null)) {
                    continue;
                }
                if !parent_rows.iter().any(|(_, parent_cells)| keys_equal(&values, parent_cells, &parent_columns)) {
                    return Err(foreign_key_violation(statement));
                }
            }
        }
        Ok(())
    }

    /// Verifies an `UPDATE` against both sides of every foreign key it touches.
    ///
    /// Child keys are re-checked only when the statement assigns one of their
    /// columns. Referenced parent keys are re-checked only when the statement
    /// assigns one of their columns, and then only child rows still holding a
    /// value the update removed are examined.
    fn check_update_foreign_keys(
        &self,
        def: &TableDef,
        plan: &UpdatePlan,
        old_rows: &[(i64, Vec<Cell>)],
        assigned: &[bool],
        statement: &'static str,
    ) -> Result<(), SqlError> {
        for key in &def.foreign_keys {
            if key.deferred && self.in_transaction() {
                continue;
            }
            if !key.columns.iter().any(|position| assigned[*position]) {
                continue;
            }
            let (parent_position, parent_columns) = self.foreign_key_parent(def, key, statement)?;
            let parent_rows = self.live_rows(&self.tables[parent_position], statement)?;
            for (rowid, values) in &plan.writes {
                let old = row_of(old_rows, *rowid);
                let next = apply_assignment(old, values);
                let child_values = pick(&next, &key.columns);
                if child_values.iter().any(|cell| matches!(cell, Cell::Null)) {
                    continue;
                }
                if !parent_rows.iter().any(|(_, parent_cells)| keys_equal(&child_values, parent_cells, &parent_columns)) {
                    return Err(foreign_key_violation(statement));
                }
            }
        }
        for (child_position, key) in self.referencing_foreign_keys(&def.name) {
            if key.deferred && self.in_transaction() && key.on_update_action != ForeignKeyAction::Restrict {
                continue;
            }
            let child = &self.tables[child_position];
            let (parent_position, parent_columns) = self.foreign_key_parent(child, &key, statement)?;
            if !parent_columns.iter().any(|position| assigned.get(*position).copied().unwrap_or(false)) {
                continue;
            }
            let mut changed: Vec<Vec<Cell>> = Vec::new();
            for (rowid, values) in &plan.writes {
                let old = row_of(old_rows, *rowid);
                let old_values = pick(old, &parent_columns);
                if old_values.iter().any(|cell| matches!(cell, Cell::Null)) {
                    continue;
                }
                let next = apply_assignment(old, values);
                let new_values = pick(&next, &parent_columns);
                if !cells_equal(&old_values, &new_values) {
                    changed.push(old_values);
                }
            }
            if changed.is_empty() {
                continue;
            }
            let parent_rows = self.live_rows(&self.tables[parent_position], statement)?;
            let child_rows = self.live_rows(child, statement)?;
            for (_, child_cells) in &child_rows {
                let child_values = pick(child_cells, &key.columns);
                if child_values.iter().any(|cell| matches!(cell, Cell::Null)) {
                    continue;
                }
                if changed.iter().any(|value| cells_equal(value, &child_values))
                    && !parent_rows.iter().any(|(_, parent_cells)| keys_equal(&child_values, parent_cells, &parent_columns))
                {
                    return Err(foreign_key_violation(statement));
                }
            }
        }
        Ok(())
    }

    /// Verifies a `DELETE`: its own foreign keys are still resolved (a malformed
    /// or missing parent fails the statement like SQLite), and every child row
    /// that referenced a removed parent key is looked for after the removal.
    fn check_delete_foreign_keys(
        &self,
        def: &TableDef,
        old_rows: &[(i64, Vec<Cell>)],
        removed: &[i64],
        statement: &'static str,
    ) -> Result<(), SqlError> {
        for key in &def.foreign_keys {
            self.foreign_key_parent(def, key, statement)?;
        }
        for (child_position, key) in self.referencing_foreign_keys(&def.name) {
            if key.deferred && self.in_transaction() && key.on_delete_action != ForeignKeyAction::Restrict {
                continue;
            }
            let child = &self.tables[child_position];
            let (_, parent_columns) = self.foreign_key_parent(child, &key, statement)?;
            let mut gone: Vec<Vec<Cell>> = Vec::new();
            for (rowid, cells) in old_rows {
                if !removed.contains(rowid) {
                    continue;
                }
                let values = pick(cells, &parent_columns);
                if values.iter().any(|cell| matches!(cell, Cell::Null)) {
                    continue;
                }
                gone.push(values);
            }
            if gone.is_empty() {
                continue;
            }
            let child_rows = self.live_rows(child, statement)?;
            for (_, child_cells) in &child_rows {
                let child_values = pick(child_cells, &key.columns);
                if child_values.iter().any(|cell| matches!(cell, Cell::Null)) {
                    continue;
                }
                if gone.iter().any(|value| cells_equal(value, &child_values)) {
                    return Err(foreign_key_violation(statement));
                }
            }
        }
        Ok(())
    }

    /// Re-checks every deferred foreign key against the state `COMMIT` would
    /// publish. Immediate keys were already enforced per statement, so only
    /// deferred keys are walked. A violation leaves the transaction open, as
    /// SQLite does, so the caller can repair the rows and commit again.
    fn check_deferred_foreign_keys(&self) -> Result<(), SqlError> {
        if !self.foreign_keys {
            return Ok(());
        }
        for child in &self.tables {
            for key in child.foreign_keys.iter().filter(|key| key.deferred) {
                let (parent_position, parent_columns) = self.foreign_key_parent(child, key, "commit")?;
                let parent = &self.tables[parent_position];
                let parent_rows = self.live_rows(parent, "commit")?;
                let child_rows = self.live_rows(child, "commit")?;
                for (_, cells) in &child_rows {
                    let values = pick(cells, &key.columns);
                    if values.iter().any(|cell| matches!(cell, Cell::Null)) {
                        continue;
                    }
                    if !parent_rows.iter().any(|(_, parent_cells)| keys_equal(&values, parent_cells, &parent_columns)) {
                        return Err(foreign_key_violation("commit"));
                    }
                }
            }
        }
        Ok(())
    }

    /// Applies the `ON DELETE` / `ON UPDATE` actions of every foreign key that
    /// references `parent`, recursing into the tables those actions touch.
    ///
    /// `changes` carries the cells every changed parent row had before the
    /// statement and, for a rewrite, the cells it has now; `None` means the row
    /// was removed. `NO ACTION` and `RESTRICT` rows are left for the
    /// end-of-statement checks. Returns whether a touched child owns
    /// materialized indexes or a stored WITHOUT ROWID b-tree.
    fn apply_foreign_key_actions(
        &mut self,
        parent: &TableDef,
        changes: &[(Vec<Cell>, Option<Vec<Cell>>)],
        statement: &'static str,
        depth: usize,
    ) -> Result<(bool, bool), SqlError> {
        if depth > 100 {
            return Err(SqlError::new("constraint-failed", "foreign key cascade exceeded the depth limit").in_statement(statement));
        }
        let deleting = changes.iter().all(|(_, next)| next.is_none());
        let mut needs_index = false;
        let mut needs_without_rowid = false;
        for (child_position, key) in self.referencing_foreign_keys(&parent.name) {
            let action = if deleting { key.on_delete_action } else { key.on_update_action };
            if matches!(action, ForeignKeyAction::NoAction | ForeignKeyAction::Restrict) {
                continue;
            }
            let child = self.tables[child_position].clone();
            if child.temp {
                continue;
            }
            let (_, parent_columns) = self.foreign_key_parent(&child, &key, statement)?;
            let mut pairs: Vec<(Vec<Cell>, Option<Vec<Cell>>)> = Vec::new();
            for (old, next) in changes {
                let old_values = pick(old, &parent_columns);
                if old_values.iter().any(|cell| matches!(cell, Cell::Null)) {
                    continue;
                }
                let new_values = next.as_ref().map(|cells| pick(cells, &parent_columns));
                if let Some(values) = &new_values {
                    if cells_equal(&old_values, values) {
                        continue;
                    }
                }
                pairs.push((old_values, new_values));
            }
            if pairs.is_empty() {
                continue;
            }
            let child_rows = self.live_rows(&child, statement)?;
            let child_key = child.name.to_ascii_lowercase();
            let frame_columns = trigger_frame_columns(&child);
            let fires = self.table_has_triggers(&child_key);
            let assigned: Vec<String> = key.columns.iter().map(|position| child.columns[*position].name.clone()).collect();
            let mut deleted: Vec<(i64, Vec<Cell>)> = Vec::new();
            let mut updated: Vec<(i64, Vec<Cell>, Vec<Cell>)> = Vec::new();
            for (rowid, cells) in &child_rows {
                let values = pick(cells, &key.columns);
                if values.iter().any(|cell| matches!(cell, Cell::Null)) {
                    continue;
                }
                let Some((_, next)) = pairs.iter().find(|(old, _)| cells_equal(old, &values))
                else {
                    continue;
                };
                match action {
                    ForeignKeyAction::Cascade => match next {
                        None => deleted.push((*rowid, cells.clone())),
                        Some(next) => {
                            let mut rewritten = cells.clone();
                            for (position, value) in key.columns.iter().zip(next.iter()) {
                                rewritten[*position] = value.clone();
                            }
                            updated.push((*rowid, cells.clone(), rewritten));
                        }
                    },
                    ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault => {
                        let mut rewritten = cells.clone();
                        for position in &key.columns {
                            let column = &child.columns[*position];
                            let value = match action {
                                ForeignKeyAction::SetNull => Cell::Null,
                                _ => {
                                    let default = column.default_value.clone().unwrap_or(crate::WriteValue::Null);
                                    write_value_cell(&crate::apply_write_affinity(&column.declared_type, default))
                                }
                            };
                            if matches!(value, Cell::Null) && column.not_null {
                                return Err(SqlError::new(
                                    "not-null-constraint",
                                    format!("NOT NULL constraint failed: {}.{}", child.name, column.name),
                                )
                                .in_statement(statement));
                            }
                            rewritten[*position] = value;
                        }
                        updated.push((*rowid, cells.clone(), rewritten));
                    }
                    _ => {}
                }
            }
            if deleted.is_empty() && updated.is_empty() {
                continue;
            }
            let table_key = child.name.clone();
            for (rowid, old) in &deleted {
                if fires {
                    self.queue_delete_before(&table_key, &frame_columns, *rowid, old);
                }
                let written = if child.without_rowid {
                    let values: Vec<crate::WriteValue> = old.iter().map(cell_write_value).collect();
                    self.writer.delete_without_rowid(&table_key, &values)
                }
                else {
                    self.writer.delete(&table_key, *rowid)
                };
                written.map_err(|error| write_failure(error, statement, &child))?;
                if fires {
                    self.queue_delete_after(&table_key, &frame_columns, *rowid, old);
                }
            }
            for (rowid, old, next) in &updated {
                if fires {
                    self.queue_update_before(&table_key, &child, &frame_columns, &assigned, *rowid, old, next);
                }
                let written = if child.without_rowid {
                    let old_values: Vec<crate::WriteValue> = old.iter().map(cell_write_value).collect();
                    let new_values: Vec<crate::WriteValue> = next.iter().map(cell_write_value).collect();
                    self.writer.update_without_rowid(&table_key, &old_values, &new_values)
                }
                else {
                    let values: Vec<UpdateValue> = next.iter().map(|cell| UpdateValue::Set(cell_write_value(cell))).collect();
                    self.writer.update(&table_key, *rowid, &values)
                };
                written.map_err(|error| write_failure(error, statement, &child))?;
                if fires {
                    self.queue_update_after(&table_key, &child, &frame_columns, &assigned, *rowid, old, next);
                }
            }
            // Rewritten child rows still have to resolve every foreign key,
            // which catches `SET DEFAULT` values that name no parent.
            let validated: Vec<i64> = updated
                .iter()
                .map(|(rowid, _, next)| {
                    child
                        .rowid_alias
                        .and_then(|alias| match next.get(alias) {
                            Some(Cell::Integer(value)) => Some(*value),
                            _ => None,
                        })
                        .unwrap_or(*rowid)
                })
                .collect();
            if !validated.is_empty() {
                self.check_insert_foreign_keys(&child, &validated, statement)?;
            }
            self.refresh_managed_indexes(&child_key, &child, statement)?;
            if self.has_indexes(&child, &child_key) {
                needs_index = true;
            }
            if child.without_rowid {
                needs_without_rowid = true;
            }
            let child_changes: Vec<(Vec<Cell>, Option<Vec<Cell>>)> = if deleting {
                deleted.iter().map(|(_, old)| (old.clone(), None)).collect()
            }
            else {
                updated.iter().map(|(_, old, next)| (old.clone(), Some(next.clone()))).collect()
            };
            let (index_needed, without_needed) = self.apply_foreign_key_actions(&child, &child_changes, statement, depth + 1)?;
            needs_index |= index_needed;
            needs_without_rowid |= without_needed;
        }
        Ok((needs_index, needs_without_rowid))
    }

    /// Every uniqueness rule an `UPDATE` has to re-check, in the order SQLite
    /// reports them: the rowid first, then the primary key, then column-level
    /// `UNIQUE`, then explicit unique indexes.
    fn unique_keys(&self, def: &TableDef, table_key: &str) -> Vec<UniqueKey> {
        let label = |positions: &[usize]| {
            positions.iter().map(|position| format!("{}.{}", def.name, def.columns[*position].name)).collect::<Vec<_>>().join(", ")
        };
        let rowid_label = match def.rowid_alias {
            Some(alias) => format!("{}.{}", def.name, def.columns[alias].name),
            None => format!("{}.rowid", def.name),
        };
        let mut keys = Vec::new();
        if !def.without_rowid {
            keys.push(UniqueKey::Rowid(rowid_label));
        }
        // A WITHOUT ROWID table has no rowid and its primary key is the
        // clustered key, so that key is the first uniqueness rule.
        if def.without_rowid && !def.primary_key.is_empty() {
            keys.push(UniqueKey::Columns(def.primary_key.clone(), label(&def.primary_key), self.key_collations(def, &def.primary_key)));
        }
        else if def.primary_key_index && !def.primary_key.is_empty() {
            keys.push(UniqueKey::Columns(def.primary_key.clone(), label(&def.primary_key), self.key_collations(def, &def.primary_key)));
        }
        for (position, column) in def.columns.iter().enumerate() {
            if Some(position) == def.rowid_alias {
                continue;
            }
            if column.primary_key || column.unique {
                keys.push(UniqueKey::Columns(vec![position], label(&[position]), self.key_collations(def, &[position])));
            }
        }
        for constraint in &def.table_constraints {
            if !constraint.primary {
                keys.push(UniqueKey::Columns(
                    constraint.columns.clone(),
                    label(&constraint.columns),
                    self.key_collations(def, &constraint.columns),
                ));
            }
        }
        for index in self.indexes.values() {
            // Managed indexes are expression or partial, so their uniqueness is
            // enforced while their keys are rebuilt from the live rows.
            if !def.temp && !index.managed && index.table == table_key && index.unique {
                let collations = if index.collations.len() == index.columns.len() {
                    index.collations.clone()
                }
                else {
                    self.key_collations(def, &index.columns)
                };
                keys.push(UniqueKey::Columns(index.columns.clone(), label(&index.columns), collations));
            }
        }
        keys
    }

    /// 各键位按列声明取得的排序规则，供唯一性与冲突检测使用。
    fn key_collations(&self, def: &TableDef, positions: &[usize]) -> Vec<Collation> {
        positions.iter().map(|position| def.columns[*position].collate.unwrap_or_default()).collect()
    }

    /// True when the table owns any index whose b-tree has to be rewritten
    /// after a write.
    fn has_indexes(&self, def: &TableDef, table_key: &str) -> bool {
        def.primary_key_index
            || def
                .columns
                .iter()
                .enumerate()
                .any(|(position, column)| (column.primary_key || column.unique) && Some(position) != def.rowid_alias)
            || def.table_constraints.iter().any(|constraint| !constraint.primary)
            || (!def.temp && self.indexes.values().any(|index| index.table == table_key))
    }

    /// Plans one `UPDATE` without touching storage: every right-hand side is
    /// evaluated against the pre-statement row, assignments apply left to right
    /// (last one wins), and each row is checked against the *current* table, so
    /// a rowid shift behaves exactly like SQLite's ascending-order pass.
    ///
    /// `joined` carries the row shape of an `UPDATE ... FROM` statement: the
    /// target columns, its rowid, and every `FROM` source appended, for the
    /// target rows whose join produced a match. A row outside the map is not
    /// updated, and the predicate has already been applied to its merged row.
    fn plan_update(
        &self,
        def: &TableDef,
        table_key: &str,
        targets: &[(usize, ResolvedExpr)],
        predicate: Option<&ResolvedExpr>,
        joined: Option<&BTreeMap<i64, Vec<Cell>>>,
        returning: Option<&ReturningPlan>,
        shape: &ScopeShape,
        budget: &Budget,
        state_override: Option<&[(i64, Vec<Cell>)]>,
        candidates: Option<&[i64]>,
    ) -> Result<UpdatePlan, SqlError> {
        let width = def.columns.len();
        let keys = self.unique_keys(def, table_key);
        let owned_live = match state_override {
            Some(_) => None,
            None => Some(match candidates {
                Some(rowids) => self.live_rows_at(def, "update", rowids)?,
                None => self.live_rows(def, "update")?,
            }),
        };
        let live: &[(i64, Vec<Cell>)] = match state_override {
            Some(rows) => rows,
            None => owned_live.as_deref().unwrap_or_default(),
        };
        let mut state = live.to_vec();
        let mut writes = Vec::new();
        let mut returned = Vec::new();
        let mut affected = 0usize;
        for (rowid, cells) in live {
            let eval_cells = match joined {
                Some(joined) => match joined.get(rowid) {
                    Some(row) => row.clone(),
                    None => continue,
                },
                None => {
                    let mut cells = cells.clone();
                    cells.push(Cell::Integer(*rowid));
                    cells
                }
            };
            let ctx =
                EvalContext { engine: self, outer: &[], current: shape, aggregates: &[], windows: &[], window_row: 0, budget, ctes: None };
            if joined.is_none() {
                if let Some(predicate) = predicate {
                    if truthiness(&predicate.eval(&eval_cells, &ctx)?) != Some(true) {
                        continue;
                    }
                }
            }
            let mut next = cells.clone();
            let mut next_rowid = *rowid;
            let mut assigned = vec![false; width];
            for (index, target) in targets {
                let value = target.eval(&eval_cells, &ctx)?;
                let column = &def.columns[*index];
                let cell = write_value_cell(&crate::apply_write_affinity(&column.declared_type, cell_write_value(&value)));
                if Some(*index) == def.rowid_alias {
                    match cell {
                        Cell::Null => {
                            return Err(if column.not_null {
                                SqlError::new("not-null-constraint", format!("NOT NULL constraint failed: {}.{}", def.name, column.name))
                            }
                            else {
                                SqlError::new("datatype-mismatch", "datatype mismatch")
                            }
                            .in_statement("update"));
                        }
                        Cell::Integer(value) => {
                            next_rowid = value;
                            next[*index] = Cell::Integer(value);
                        }
                        _ => return Err(SqlError::new("datatype-mismatch", "datatype mismatch").in_statement("update")),
                    }
                }
                else {
                    if matches!(cell, Cell::Null) && column.not_null {
                        return Err(SqlError::new("not-null-constraint", format!("NOT NULL constraint failed: {}.{}", def.name, column.name))
                            .in_statement("update"));
                    }
                    next[*index] = cell;
                }
                assigned[*index] = true;
            }
            let mut check_cells = next.clone();
            check_cells.push(Cell::Integer(next_rowid));
            for check in &def.checks {
                if truthiness(&check.expr.eval(&check_cells, &ctx)?) == Some(false) {
                    let name = check.name.clone().unwrap_or_else(|| check.text.clone());
                    return Err(SqlError::new("check-constraint", format!("CHECK constraint failed: {name}"))
                        .in_clause("CHECK")
                        .in_statement("update"));
                }
            }
            if let Some(label) = unique_conflict(&keys, &next, next_rowid, &state, *rowid) {
                return Err(SqlError::new("unique-constraint", format!("UNIQUE constraint failed: {label}")).in_statement("update"));
            }
            if let Some(entry) = state.iter_mut().find(|(id, _)| *id == *rowid) {
                entry.0 = next_rowid;
                entry.1 = next.clone();
            }
            if let Some(returning) = returning {
                let mut new_cells = next.clone();
                new_cells.push(Cell::Integer(next_rowid));
                returned.push(returning.eval(&new_cells, &ctx)?);
            }
            let values = def
                .columns
                .iter()
                .enumerate()
                .map(|(index, _)| if assigned[index] { UpdateValue::Set(cell_write_value(&next[index])) } else { UpdateValue::Keep })
                .collect();
            writes.push((*rowid, values));
            affected += 1;
        }
        // 复用的快照优先直接交出所有权，避免再复制一份整表。
        let live_snapshot = match owned_live {
            Some(rows) => rows,
            None => live.to_vec(),
        };
        Ok(UpdatePlan { writes, returning: returned, affected, live: live_snapshot })
    }

    /// Plans one `DELETE`: the matching rowids in ascending order, plus any
    /// `RETURNING` rows, which see the row as it was before removal.
    ///
    /// `candidates` narrows the scan to the rowids a `WHERE <rowid> = <key>`
    /// equality or an index probe can reach. The predicate still runs over every
    /// candidate, so a stale or mismatched probe only costs the pushdown and
    /// never changes the removed set.
    fn plan_delete(
        &self,
        def: &TableDef,
        predicate: Option<&ResolvedExpr>,
        returning: Option<&ReturningPlan>,
        shape: &ScopeShape,
        budget: &Budget,
        candidates: Option<&[i64]>,
    ) -> Result<DeletePlan, SqlError> {
        let live = match candidates {
            Some(rowids) => self.live_rows_at(def, "delete", rowids)?,
            None => self.live_rows(def, "delete")?,
        };
        let mut removed = Vec::new();
        let mut returned = Vec::new();
        for (rowid, cells) in &live {
            let mut eval_cells = cells.clone();
            eval_cells.push(Cell::Integer(*rowid));
            let ctx =
                EvalContext { engine: self, outer: &[], current: shape, aggregates: &[], windows: &[], window_row: 0, budget, ctes: None };
            if let Some(predicate) = predicate {
                if truthiness(&predicate.eval(&eval_cells, &ctx)?) != Some(true) {
                    continue;
                }
            }
            if let Some(returning) = returning {
                returned.push(returning.eval(&eval_cells, &ctx)?);
            }
            removed.push(*rowid);
        }
        Ok(DeletePlan { removed, returning: returned, live })
    }

    /// Executes one `SELECT` over the comma-separated `FROM` list.
    ///
    /// The sources are flattened into one row shape in `FROM` order, then
    /// combined as a Cartesian product. Predicates are applied as soon as every
    /// source they read is bound, so an unselective `WHERE` never materializes
    /// the full product. Nested subqueries re-enter this same function with the
    /// current row pushed onto `outer`.
    fn select(
        &self,
        first: &SelectCore,
        compounds: &[CompoundTerm],
        order: Vec<OrderKey>,
        limit: Option<Limit>,
        outer: &[Scope],
        budget: &Budget,
        ctes: Option<&CteEnv>,
    ) -> Result<ExecResult, SqlError> {
        let limit = self.eval_limit(limit.as_ref(), outer, budget, ctes)?;
        if !compounds.is_empty() {
            return self.compound_select(first, compounds, order, limit, outer, budget, ctes);
        }
        let SelectCore { distinct, items, from, filter, group, having, windows: named } = first;
        // Each `FROM` item becomes one source over already materialized rows:
        // a base table is decoded from storage, a CTE comes from the `WITH`
        // environment, and a derived table is evaluated once through this same
        // `select` (as a plain subquery, never correlated).
        // A single base rowid table with a `WHERE <rowid> = <integer literal>`
        // conjunct is bounded to its one candidate row before decoding; failing
        // that, a `WHERE` conjunction covering every key column of a plain
        // column index probes the index instead of decoding the whole table.
        // Both keep the full predicate running downstream, so the emitted row
        // set and order are unchanged.
        let scan = match self.rowid_pin(from, filter) {
            Some(rowid) => Some(ScanHint::Pin(rowid)),
            None => self.index_pin(from, filter).map(ScanHint::Rowids),
        };
        // `ORDER BY <rowid order> LIMIT n` reads only the leading rows, because
        // the scan order already satisfies that sort.
        let scan = scan.or_else(|| self.rowid_head_bound(first, &order, limit).map(ScanHint::Head));
        // Columns the statement never reads are skipped while decoding the base
        // table, so a `count(*)` join or an aggregate does not materialize every
        // text cell. The mask is derived from the raw expressions; any shape it
        // cannot attribute confidently falls back to decoding every column.
        let masks = self.read_column_masks(first, &order);
        let mut sources = Vec::with_capacity(from.len());
        let mut tables: Vec<Rc<Vec<Vec<Cell>>>> = Vec::with_capacity(from.len());
        let mut offset = 0;
        for (index, table) in from.iter().enumerate() {
            let keep = masks.as_ref().and_then(|masks| masks.get(index)).and_then(|mask| mask.as_deref());
            let (columns, rows) = self.select_source(table, budget, ctes, scan.as_ref(), keep)?;
            let width = columns.width();
            let source = Source {
                label: table.label().to_string(),
                columns,
                offset,
                join: table.join,
                star_hidden: vec![false; width],
                unqualified_hidden: vec![false; width],
                star_override: vec![None; width],
            };
            offset += source.width();
            sources.push(source);
            tables.push(rows);
        }
        let mut relation = Relation { sources, using: vec![Vec::new(); from.len()], using_slots: Vec::new(), trigger: None, excluded: false };
        relation.plan_joins(&from)?;
        let shape = relation.shape();
        let outer_shapes: Vec<ScopeShape> = outer.iter().map(Scope::shape).collect();
        // Join predicates are resolved once, against the whole `FROM`, and
        // rejected when they reach a source that is not yet in scope.
        let mut ons: Vec<Option<ResolvedExpr>> = Vec::with_capacity(relation.sources.len());
        for (index, table) in from.iter().enumerate() {
            let resolved = match &table.on {
                None => None,
                Some(on) => {
                    let mut scratch_aggregates = Vec::new();
                    let mut scratch_windows = Vec::new();
                    let resolved =
                        resolve_expr(on, &relation, &outer_shapes, &[], &mut scratch_windows, &mut scratch_aggregates, false, false)?;
                    let mut refs = Vec::new();
                    resolved.collect_sources(&mut refs);
                    if refs.iter().any(|source| *source > index) {
                        return Err(SqlError::new(
                            "unknown-column",
                            format!("the `ON` clause of source {} may not reference a later source", index + 1),
                        )
                        .in_statement("select"));
                    }
                    Some(resolved)
                }
            };
            ons.push(resolved);
        }
        // Resolve every expression before reading rows, so a bad name fails closed.
        let mut specs = Vec::new();
        let mut windows: Vec<ResolvedWindow> = Vec::new();
        let projection: Option<Vec<ResolvedExpr>> = match items {
            SelectItems::Named(exprs) => Some(
                exprs
                    .iter()
                    .map(|expr| resolve_expr(expr, &relation, &outer_shapes, named, &mut windows, &mut specs, true, true))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            SelectItems::CountAll | SelectItems::Star => None,
        };
        let labels = core_output_labels(&relation, items);
        let output_width = labels.len();
        // 输出列各自的排序规则：`DISTINCT`、`GROUP BY` 序数与 `ORDER BY` 序数
        // 都按它比较文本，与上游对结果列排序规则的继承一致。
        let output_collations = match items {
            SelectItems::Named(_) => projection
                .as_ref()
                .map(|nodes| nodes.iter().map(|node| resolved_collation(node, &relation)).collect::<Vec<_>>())
                .unwrap_or_default(),
            SelectItems::Star => relation.visible_collations(),
            SelectItems::CountAll => vec![Collation::Binary],
        };
        let output_collation = |position: usize| output_collations.get(position).copied().unwrap_or_default();
        // Aggregates are only defined over the whole result, so a predicate or
        // sort key that mixes one in is rejected instead of being evaluated per
        // row. Window functions are likewise limited to projection and `ORDER BY`.
        let predicate = match filter {
            Some(expr) => {
                let mut scratch_aggregates = Vec::new();
                let mut scratch_windows = Vec::new();
                Some(resolve_expr(expr, &relation, &outer_shapes, &[], &mut scratch_windows, &mut scratch_aggregates, false, false)?)
            }
            None => None,
        };
        // `GROUP BY` accepts a projection ordinal or any expression over the
        // current row.
        let mut group_keys = Vec::with_capacity(group.len());
        for expr in group {
            if let Expr::Literal(Literal::Integer(position)) = expr {
                let position = *position;
                if position < 1 || position as usize > output_width {
                    return Err(SqlError::new(
                        "order-term-out-of-range",
                        format!("GROUP BY term out of range - should be between 1 and {output_width}"),
                    )
                    .in_statement("select"));
                }
                group_keys.push(GroupKey::Projected(position as usize - 1, output_collation(position as usize - 1)));
                continue;
            }
            let mut scratch_aggregates = Vec::new();
            let mut scratch_windows = Vec::new();
            let resolved = resolve_expr(expr, &relation, &outer_shapes, &[], &mut scratch_windows, &mut scratch_aggregates, false, false)?;
            let collation = resolved_collation(&resolved, &relation);
            group_keys.push(GroupKey::Expression(resolved, collation));
        }
        // `HAVING` shares the projection's aggregate list, so it may add an
        // aggregate that the projection does not mention. Window functions are
        // not allowed there; SQLite reports that as a `misuse`.
        let having_expr = match having {
            Some(expr) => Some(resolve_expr(expr, &relation, &outer_shapes, named, &mut windows, &mut specs, true, false)?),
            None => None,
        };
        let keys: Vec<(SortKey, SortDirection)> = order
            .iter()
            .map(|entry| {
                // A bare integer is a 1-based output-column ordinal, as in SQLite.
                if let Expr::Literal(Literal::Integer(position)) = &entry.expr {
                    let position = *position;
                    if position < 1 || position as usize > output_width {
                        return Err(SqlError::new(
                            "order-term-out-of-range",
                            format!("ORDER BY term out of range - should be between 1 and {output_width}"),
                        )
                        .in_statement("select"));
                    }
                    return Ok((
                        SortKey::Projected(position as usize - 1),
                        SortDirection::of(entry).with_collation(output_collation(position as usize - 1)),
                    ));
                }
                // An unqualified name that matches a projection alias sorts by
                // the output column; SQLite prefers the alias over a same-named
                // base column.
                if let Expr::Column(column) = &entry.expr {
                    if column.qualifier.is_none() {
                        if let Some(position) = labels.iter().position(|label| label.eq_ignore_ascii_case(&column.name)) {
                            return Ok((SortKey::Projected(position), SortDirection::of(entry).with_collation(output_collation(position))));
                        }
                    }
                }
                let resolved = resolve_expr(&entry.expr, &relation, &outer_shapes, named, &mut windows, &mut specs, true, true)?;
                let collation = resolved_collation(&resolved, &relation);
                Ok((SortKey::Expression(resolved), SortDirection::of(entry).with_collation(collation)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let grouped = !group_keys.is_empty() || !specs.is_empty();
        if having_expr.is_some() && !grouped {
            // SQLite rejects `HAVING` without a group or aggregate rather than
            // treating it as a second `WHERE`.
            let mut error = SqlError::new("misuse-of-aggregate", "HAVING clause on a non-aggregate query").in_statement("select");
            error.clause = Some("HAVING".to_string());
            return Err(error);
        }
        let joined = relation.sources.iter().any(|source| source.join.is_some());
        let windowed = !windows.is_empty();
        // `count(*)` over a plain product never has to materialize a row.
        if predicate.is_none() && keys.is_empty() && !joined && !grouped && having_expr.is_none() && !windowed {
            if let SelectItems::CountAll = items {
                let count = tables.iter().fold(1i64, |total, rows| total.saturating_mul(rows.len() as i64));
                let visible = match limit {
                    None => true,
                    Some((offset, count)) => offset == 0 && count.map_or(true, |count| count > 0),
                };
                return Ok(ExecResult {
                    statement: "select",
                    columns: labels,
                    rows: if visible { vec![vec![Cell::Integer(count)]] } else { Vec::new() },
                    affected: 0,
                });
            }
        }

        // A `LIMIT` may stop the product early only when no sort, join, aggregate
        // or window is pending. `DISTINCT` reads every row because collapsing
        // duplicates before `LIMIT` needs the whole set.
        let take = if keys.is_empty() && !joined && !grouped && !windowed && !*distinct {
            match limit {
                Some((offset, Some(count))) => offset.saturating_add(count),
                _ => usize::MAX,
            }
        }
        else {
            usize::MAX
        };
        let empty_aggregates: Vec<Cell> = Vec::new();
        let ctx =
            EvalContext { engine: self, outer, current: &shape, aggregates: &empty_aggregates, windows: &[], window_row: 0, budget, ctes };
        let mut conjuncts = Vec::new();
        if let Some(predicate) = predicate {
            split_conjuncts(predicate, &mut conjuncts);
        }
        // 裸 `count(*)` 的内连接只需要计数,不必把每个匹配行合并成
        // `Vec<Cell>`。限定在全部为普通源或内连接的形态,其余连接走通用路径。
        let bare_count_star = grouped
            && keys.is_empty()
            && having_expr.is_none()
            && !windowed
            && !*distinct
            && matches!(projection.as_deref(), Some([ResolvedExpr::AggregateRef(0)]))
            && matches!(specs.as_slice(), [spec]
                if matches!(spec.function, AggregateFunction::CountStar)
                    && spec.argument.is_none()
                    && spec.filter.is_none()
                    && !spec.distinct
                    && spec.order.is_empty());
        if bare_count_star
            && joined
            && relation.sources.iter().all(|source| matches!(source.join, None | Some(JoinKind::Inner)))
            && relation.using.iter().all(|pairs| pairs.is_empty())
        {
            let count = count_inner_join_rows(&relation, &ons, &tables, &conjuncts, &ctx)?;
            let visible = match limit {
                None => true,
                Some((offset, limit)) => offset == 0 && limit.map_or(true, |limit| limit > 0),
            };
            let rows = if visible { vec![vec![Cell::Integer(count as i64)]] } else { Vec::new() };
            return Ok(ExecResult { statement: "select", columns: labels, rows, affected: 0 });
        }
        if bare_count_star && !joined {
            let count = count_cross_product_rows(&tables, &relation, &conjuncts, &ctx)?;
            let visible = match limit {
                None => true,
                Some((offset, limit)) => offset == 0 && limit.map_or(true, |limit| limit > 0),
            };
            let rows = if visible { vec![vec![Cell::Integer(count as i64)]] } else { Vec::new() };
            return Ok(ExecResult { statement: "select", columns: labels, rows, affected: 0 });
        }
        let rows = if joined {
            join_rows(&relation, &ons, &tables, &conjuncts, take, &ctx)?
        }
        else {
            cross_product(tables, &relation, &conjuncts, take, &ctx)?
        };
        if grouped {
            // One group per distinct key; without `GROUP BY` there is exactly
            // one group, even when it is empty, because aggregates must still
            // produce their empty-input values.
            let groups = build_groups(&group_keys, &projection, rows, &ctx, budget)?;
            let mut inputs: Vec<WindowRow> = Vec::with_capacity(groups.len());
            let mut sizes: Vec<i64> = Vec::with_capacity(groups.len());
            for group_rows in groups {
                let aggregates = specs.iter().map(|spec| compute_aggregate(spec, &group_rows, &ctx)).collect::<Result<Vec<_>, _>>()?;
                let representative = group_rows.first().cloned().unwrap_or_else(|| vec![Cell::Null; shape.columns.len()]);
                let group_ctx =
                    EvalContext { engine: self, outer, current: &shape, aggregates: &aggregates, windows: &[], window_row: 0, budget, ctes };
                if let Some(having) = &having_expr {
                    if truthiness(&having.eval(&representative, &group_ctx)?) != Some(true) {
                        continue;
                    }
                }
                sizes.push(group_rows.len() as i64);
                inputs.push(WindowRow { values: representative, aggregates });
            }
            let (window_values, window_keys, window_desc) = if windowed {
                self.evaluate_windows(&windows, &inputs, &shape, outer, budget, ctes)?
            }
            else {
                (Vec::new(), Vec::new(), Vec::new())
            };
            let mut decorated: Vec<(Vec<Cell>, Vec<Cell>, usize)> = Vec::with_capacity(inputs.len());
            for (index, row) in inputs.iter().enumerate() {
                let row_ctx = EvalContext {
                    engine: self,
                    outer,
                    current: &shape,
                    aggregates: &row.aggregates,
                    windows: &window_values,
                    window_row: index,
                    budget,
                    ctes,
                };
                let cells = match items {
                    SelectItems::Named(_) => projection
                        .as_ref()
                        .expect("named projection is resolved up front")
                        .iter()
                        .map(|node| node.eval(&row.values, &row_ctx))
                        .collect::<Result<Vec<_>, _>>()?,
                    SelectItems::CountAll => vec![Cell::Integer(sizes[index])],
                    SelectItems::Star => relation.visible_row(&row.values),
                };
                let mut sort_cells = Vec::with_capacity(keys.len());
                for (key, _) in &keys {
                    sort_cells.push(match key {
                        SortKey::Projected(index) => cells.get(*index).cloned().unwrap_or(Cell::Null),
                        SortKey::Expression(expr) => expr.eval(&row.values, &row_ctx)?,
                    });
                }
                decorated.push((sort_cells, cells, index));
            }
            if !keys.is_empty() {
                decorated.sort_by(|left, right| compare_key_rows(&left.0, &right.0, &keys));
            }
            else if windowed {
                decorated.sort_by(|left, right| compare_ordered(&window_keys[left.2], &window_keys[right.2], &window_desc));
            }
            let mut out: Vec<Vec<Cell>> = decorated.into_iter().map(|(_, cells, _)| cells).collect();
            if *distinct {
                out = retain_first_distinct(out, &output_collations);
            }
            truncate_rows(&mut out, limit);
            return Ok(ExecResult { statement: "select", columns: labels, rows: out, affected: 0 });
        }
        let (window_values, window_keys, window_desc) = if windowed {
            let inputs: Vec<WindowRow> = rows.iter().map(|values| WindowRow { values: values.clone(), aggregates: Vec::new() }).collect();
            self.evaluate_windows(&windows, &inputs, &shape, outer, budget, ctes)?
        }
        else {
            (Vec::new(), Vec::new(), Vec::new())
        };
        let mut order: Vec<usize> = (0..rows.len()).collect();
        if !keys.is_empty() {
            // Sort keys may be projection ordinals or expressions over the
            // merged source row; evaluate both through the same evaluator.
            let mut sort_cells: Vec<Vec<Cell>> = Vec::with_capacity(rows.len());
            for (index, row) in rows.iter().enumerate() {
                let row_ctx = EvalContext {
                    engine: self,
                    outer,
                    current: &shape,
                    aggregates: &empty_aggregates,
                    windows: &window_values,
                    window_row: index,
                    budget,
                    ctes,
                };
                let mut cells = Vec::with_capacity(keys.len());
                for (key, _) in &keys {
                    cells.push(match key {
                        SortKey::Projected(index) => match &projection {
                            Some(nodes) => nodes[*index].eval(row, &row_ctx)?,
                            None => Cell::Null,
                        },
                        SortKey::Expression(expr) => expr.eval(row, &row_ctx)?,
                    });
                }
                sort_cells.push(cells);
            }
            order.sort_by(|left, right| compare_key_rows(&sort_cells[*left], &sort_cells[*right], &keys));
        }
        else if windowed {
            // Without a trailing `ORDER BY`, SQLite's row order is the first
            // window's sort order, with later windows breaking its ties.
            order.sort_by(|left, right| compare_ordered(&window_keys[*left], &window_keys[*right], &window_desc));
        }
        if *distinct && !matches!(items, SelectItems::CountAll) {
            // Every row's output has to be materialized before `LIMIT`, because
            // `DISTINCT` collapses duplicates across the whole set.
            let mut outputs: Vec<Vec<Cell>> = Vec::with_capacity(rows.len());
            for (index, row) in rows.iter().enumerate() {
                let cells = match items {
                    SelectItems::Named(_) => {
                        let nodes = projection.as_ref().expect("named projection is resolved up front");
                        let row_ctx = EvalContext {
                            engine: self,
                            outer,
                            current: &shape,
                            aggregates: &empty_aggregates,
                            windows: &window_values,
                            window_row: index,
                            budget,
                            ctes,
                        };
                        nodes.iter().map(|node| node.eval(row, &row_ctx)).collect::<Result<Vec<_>, _>>()?
                    }
                    SelectItems::Star => relation.visible_row(row),
                    SelectItems::CountAll => unreachable!("count(*) keeps a single row"),
                };
                outputs.push(cells);
            }
            let ordered: Vec<Vec<Cell>> = order.iter().map(|index| outputs[*index].clone()).collect();
            let unique = retain_first_distinct(ordered, &output_collations);
            let (start, end) = limit_range(unique.len(), limit);
            let out: Vec<Vec<Cell>> = unique.into_iter().skip(start).take(end - start).collect();
            return Ok(ExecResult { statement: "select", columns: labels, rows: out, affected: 0 });
        }
        let (start, end) = limit_range(rows.len(), limit);
        let mut final_rows: Vec<Vec<Cell>> = Vec::with_capacity(end.saturating_sub(start));
        let mut final_windows: Vec<Vec<Cell>> = vec![Vec::new(); window_values.len()];
        for &index in &order[start..end] {
            final_rows.push(rows[index].clone());
            for (column, values) in final_windows.iter_mut().zip(window_values.iter()) {
                column.push(values[index].clone());
            }
        }
        match items {
            SelectItems::CountAll => {
                // The count is taken before `LIMIT`; the limit only decides how
                // many of the single-row results survive.
                let visible = match limit {
                    None => true,
                    Some((offset, count)) => offset == 0 && count.map_or(true, |count| count > 0),
                };
                let rows = if visible { vec![vec![Cell::Integer(rows.len() as i64)]] } else { Vec::new() };
                Ok(ExecResult { statement: "select", columns: labels, rows, affected: 0 })
            }
            SelectItems::Star => Ok(ExecResult {
                statement: "select",
                columns: labels,
                rows: final_rows.iter().map(|row| relation.visible_row(row)).collect(),
                affected: 0,
            }),
            SelectItems::Named(_) => {
                let nodes = projection.expect("named projection is resolved up front");
                let mut out = Vec::with_capacity(final_rows.len());
                for (index, values) in final_rows.iter().enumerate() {
                    let row_ctx = EvalContext {
                        engine: self,
                        outer,
                        current: &shape,
                        aggregates: &empty_aggregates,
                        windows: &final_windows,
                        window_row: index,
                        budget,
                        ctes,
                    };
                    out.push(nodes.iter().map(|node| node.eval(values, &row_ctx)).collect::<Result<Vec<_>, _>>()?);
                }
                Ok(ExecResult { statement: "select", columns: labels, rows: out, affected: 0 })
            }
        }
    }

    /// Evaluates the trailing `LIMIT` / `OFFSET` expressions once, before any
    /// row is read, and converts them with SQLite's integer rules.
    fn eval_limit(
        &self,
        limit: Option<&Limit>,
        outer: &[Scope],
        budget: &Budget,
        ctes: Option<&CteEnv>,
    ) -> Result<Option<(usize, Option<usize>)>, SqlError> {
        let Some(limit) = limit
        else {
            return Ok(None);
        };
        let relation = Relation { sources: Vec::new(), using: Vec::new(), using_slots: Vec::new(), trigger: None, excluded: false };
        let shape = relation.shape();
        let outer_shapes: Vec<ScopeShape> = outer.iter().map(Scope::shape).collect();
        let mut aggregates = Vec::new();
        let mut windows = Vec::new();
        let empty: Vec<Cell> = Vec::new();
        let ctx = EvalContext { engine: self, outer, current: &shape, aggregates: &empty, windows: &[], window_row: 0, budget, ctes };
        let mut offset = 0usize;
        if let Some(expr) = &limit.offset {
            let resolved = resolve_expr(expr, &relation, &outer_shapes, &[], &mut windows, &mut aggregates, false, false)?;
            let value = limit_integer(&resolved.eval(&[], &ctx)?, "OFFSET")?;
            offset = value.max(0) as usize;
        }
        let resolved = resolve_expr(&limit.count, &relation, &outer_shapes, &[], &mut windows, &mut aggregates, false, false)?;
        let count = limit_integer(&resolved.eval(&[], &ctx)?, "LIMIT")?;
        let count = if count < 0 { None } else { Some(count as usize) };
        Ok(Some((offset, count)))
    }

    /// Evaluates a compound `SELECT`: every core runs through the same single
    /// core evaluator, then the set operators combine the row sets, and only
    /// then does the trailing `ORDER BY` / `LIMIT` apply.
    fn compound_select(
        &self,
        first: &SelectCore,
        compounds: &[CompoundTerm],
        order: Vec<OrderKey>,
        limit: Option<(usize, Option<usize>)>,
        outer: &[Scope],
        budget: &Budget,
        ctes: Option<&CteEnv>,
    ) -> Result<ExecResult, SqlError> {
        let head = self.select(first, &[], Vec::new(), None, outer, budget, ctes)?;
        let columns = head.columns;
        let width = columns.len();
        // SQLite folds compound terms strictly left to right, whatever the
        // operator, so the row set is combined as each core is reached.
        let mut rows = head.rows;
        for term in compounds {
            let right = self.select(&term.core, &[], Vec::new(), None, outer, budget, ctes)?;
            if right.columns.len() != width {
                let mut error = SqlError::new(
                    "arity-mismatch",
                    format!("SELECTs to the left and right of {} do not have the same number of result columns", term.op.text()),
                )
                .in_statement("select");
                error.clause = Some(term.op.clause().to_string());
                error.position = Some(term.position);
                return Err(error);
            }
            rows = combine_rows(term.op, rows, right.rows);
        }
        if !order.is_empty() {
            let keys = compound_order_keys(&order, &columns)?;
            rows.sort_by(|left, right| {
                keys.iter()
                    .find_map(|(index, direction)| {
                        let ordering = direction.compare(&left[*index], &right[*index]);
                        (ordering != Ordering::Equal).then_some(ordering)
                    })
                    .unwrap_or(Ordering::Equal)
            });
        }
        if let Some((offset, count)) = limit {
            let (start, end) = limit_range(rows.len(), Some((offset, count)));
            rows = rows.drain(start..end).collect();
        }
        Ok(ExecResult { statement: "select", columns, rows, affected: 0 })
    }

    /// Computes every window of one `SELECT` core over its input rows.
    ///
    /// Returns one value column per window (indexed by input row), the
    /// composite sort key of every input row, and the descending flag of each
    /// key column. The composite key concatenates, for each window in
    /// declaration order, its partition keys then its order keys, so the first
    /// window is the primary sort and later windows only break its ties — the
    /// order SQLite emits when no trailing `ORDER BY` is written.
    fn evaluate_windows(
        &self,
        windows: &[ResolvedWindow],
        inputs: &[WindowRow],
        shape: &ScopeShape,
        outer: &[Scope],
        budget: &Budget,
        ctes: Option<&CteEnv>,
    ) -> Result<(Vec<Vec<Cell>>, Vec<Vec<Cell>>, Vec<SortDirection>), SqlError> {
        let mut values: Vec<Vec<Cell>> = vec![Vec::new(); windows.len()];
        let mut keys: Vec<Vec<Cell>> = vec![Vec::new(); inputs.len()];
        let mut descending: Vec<SortDirection> = Vec::new();
        // Key expressions are evaluated once per window; the sorts then run
        // from the last window to the first. Each later window is a tie-breaker
        // of the previous one, which reproduces SQLite's sorter chain: the
        // first window is the primary order of the output, and within its peer
        // groups the later windows decide which row a ranking function sees
        // first.
        let mut partitions: Vec<Vec<Vec<Cell>>> = Vec::with_capacity(windows.len());
        let mut orders: Vec<Vec<Vec<Cell>>> = Vec::with_capacity(windows.len());
        for window in windows {
            let mut window_partitions: Vec<Vec<Cell>> = Vec::with_capacity(inputs.len());
            let mut window_orders: Vec<Vec<Cell>> = Vec::with_capacity(inputs.len());
            for row in inputs {
                let ctx =
                    EvalContext { engine: self, outer, current: shape, aggregates: &row.aggregates, windows: &[], window_row: 0, budget, ctes };
                let mut partition = Vec::with_capacity(window.partition.len());
                for expr in &window.partition {
                    partition.push(expr.eval(&row.values, &ctx)?);
                }
                let mut order = Vec::with_capacity(window.order.len());
                for (expr, _) in &window.order {
                    order.push(expr.eval(&row.values, &ctx)?);
                }
                window_partitions.push(partition);
                window_orders.push(order);
            }
            partitions.push(window_partitions);
            orders.push(window_orders);
            descending.resize(descending.len() + window.partition.len(), SortDirection::default());
            descending.extend(window.order.iter().map(|(_, direction)| *direction));
        }
        for (index, key_row) in keys.iter_mut().enumerate() {
            for window in 0..windows.len() {
                key_row.extend(partitions[window][index].iter().cloned());
                key_row.extend(orders[window][index].iter().cloned());
            }
        }
        let mut order: Vec<usize> = (0..inputs.len()).collect();
        for index in (0..windows.len()).rev() {
            let window = &windows[index];
            let window_partitions = &partitions[index];
            let window_orders = &orders[index];
            order.sort_by(|left, right| {
                compare_window_rows(
                    &window_partitions[*left],
                    &window_orders[*left],
                    &window_partitions[*right],
                    &window_orders[*right],
                    &window.order,
                )
            });
            let mut column = vec![Cell::Null; inputs.len()];
            let mut start = 0;
            while start < order.len() {
                let mut end = start + 1;
                while end < order.len() && cells_equal(&window_partitions[order[end]], &window_partitions[order[start]]) {
                    end += 1;
                }
                self.evaluate_window(window, &order[start..end], window_orders, inputs, &mut column, shape, outer, budget, ctes)?;
                start = end;
            }
            values[index] = column;
        }
        Ok((values, keys, descending))
    }

    /// Computes one window over one partition.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_window(
        &self,
        window: &ResolvedWindow,
        indices: &[usize],
        orders: &[Vec<Cell>],
        inputs: &[WindowRow],
        column: &mut [Cell],
        shape: &ScopeShape,
        outer: &[Scope],
        budget: &Budget,
        ctes: Option<&CteEnv>,
    ) -> Result<(), SqlError> {
        let len = indices.len();
        // Peer groups: rows whose every ORDER BY value compares equal. With no
        // ORDER BY the whole partition is one peer group, which is what makes
        // ranking functions report ties across a whole partition.
        let mut peer_start = vec![0usize; len];
        let mut peer_end = vec![len - 1; len];
        let mut dense = vec![0i64; len];
        let mut group = 0i64;
        let mut at = 0;
        while at < len {
            let mut end = at + 1;
            while end < len && cells_equal(&orders[indices[at]], &orders[indices[end]]) {
                end += 1;
            }
            for slot in at..end {
                peer_start[slot] = at;
                peer_end[slot] = end - 1;
                dense[slot] = group;
            }
            group += 1;
            at = end;
        }
        match &window.function {
            ResolvedWindowFunction::RowNumber => {
                for slot in 0..len {
                    column[indices[slot]] = Cell::Integer(slot as i64 + 1);
                }
            }
            ResolvedWindowFunction::Rank => {
                for slot in 0..len {
                    column[indices[slot]] = Cell::Integer(peer_start[slot] as i64 + 1);
                }
            }
            ResolvedWindowFunction::DenseRank => {
                for slot in 0..len {
                    column[indices[slot]] = Cell::Integer(dense[slot] + 1);
                }
            }
            ResolvedWindowFunction::PercentRank => {
                for slot in 0..len {
                    column[indices[slot]] = if len == 1 { Cell::Real(0.0) } else { Cell::Real(peer_start[slot] as f64 / (len - 1) as f64) };
                }
            }
            ResolvedWindowFunction::CumeDist => {
                for slot in 0..len {
                    column[indices[slot]] = Cell::Real((peer_end[slot] + 1) as f64 / len as f64);
                }
            }
            ResolvedWindowFunction::Ntile(buckets) => {
                let ctx = self.window_context(inputs, indices[0], shape, outer, budget, ctes);
                let count = ntile_buckets(&buckets.eval(&inputs[indices[0]].values, &ctx)?)?;
                // Bigger buckets first: the first `len % count` buckets hold one
                // extra row.
                let base = len / count;
                let extra = len % count;
                let mut slot = 0;
                for bucket in 0..count {
                    let size = if bucket < extra { base + 1 } else { base };
                    for _ in 0..size {
                        if slot >= len {
                            break;
                        }
                        column[indices[slot]] = Cell::Integer(bucket as i64 + 1);
                        slot += 1;
                    }
                }
            }
            ResolvedWindowFunction::Lag { value, offset, default } => {
                self.evaluate_offset_function(value, offset, default, true, indices, inputs, column, shape, outer, budget, ctes)?;
            }
            ResolvedWindowFunction::Lead { value, offset, default } => {
                self.evaluate_offset_function(value, offset, default, false, indices, inputs, column, shape, outer, budget, ctes)?;
            }
            ResolvedWindowFunction::FirstValue(value) => {
                self.evaluate_frame_value(
                    ValuePick::First,
                    value,
                    None,
                    window,
                    indices,
                    orders,
                    inputs,
                    column,
                    &peer_start,
                    &peer_end,
                    &dense,
                    shape,
                    outer,
                    budget,
                    ctes,
                )?;
            }
            ResolvedWindowFunction::LastValue(value) => {
                self.evaluate_frame_value(
                    ValuePick::Last,
                    value,
                    None,
                    window,
                    indices,
                    orders,
                    inputs,
                    column,
                    &peer_start,
                    &peer_end,
                    &dense,
                    shape,
                    outer,
                    budget,
                    ctes,
                )?;
            }
            ResolvedWindowFunction::NthValue { value, position } => {
                self.evaluate_frame_value(
                    ValuePick::Nth,
                    value,
                    Some(position),
                    window,
                    indices,
                    orders,
                    inputs,
                    column,
                    &peer_start,
                    &peer_end,
                    &dense,
                    shape,
                    outer,
                    budget,
                    ctes,
                )?;
            }
            ResolvedWindowFunction::Aggregate { function, argument, separator, distinct, filter, order, collation } => {
                self.evaluate_frame_aggregate(
                    *function,
                    argument.as_ref(),
                    separator.as_ref(),
                    *distinct,
                    *collation,
                    filter.as_ref(),
                    order,
                    window,
                    indices,
                    orders,
                    inputs,
                    column,
                    &peer_start,
                    &peer_end,
                    &dense,
                    shape,
                    outer,
                    budget,
                    ctes,
                )?;
            }
        }
        Ok(())
    }

    /// Context of one input row, carrying that row's whole-result aggregates.
    fn window_context<'a>(
        &'a self,
        inputs: &'a [WindowRow],
        index: usize,
        shape: &'a ScopeShape,
        outer: &'a [Scope],
        budget: &'a Budget,
        ctes: Option<&'a CteEnv>,
    ) -> EvalContext<'a> {
        EvalContext { engine: self, outer, current: shape, aggregates: &inputs[index].aggregates, windows: &[], window_row: 0, budget, ctes }
    }

    /// `lag`/`lead`: the value of a row `offset` positions away.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_offset_function(
        &self,
        value: &ResolvedExpr,
        offset: &Option<ResolvedExpr>,
        default: &Option<ResolvedExpr>,
        backward: bool,
        indices: &[usize],
        inputs: &[WindowRow],
        column: &mut [Cell],
        shape: &ScopeShape,
        outer: &[Scope],
        budget: &Budget,
        ctes: Option<&CteEnv>,
    ) -> Result<(), SqlError> {
        let len = indices.len();
        for slot in 0..len {
            let ctx = self.window_context(inputs, indices[slot], shape, outer, budget, ctes);
            let step = match offset {
                None => 1,
                Some(expr) => match lag_offset(&expr.eval(&inputs[indices[slot]].values, &ctx)?) {
                    Some(step) => step,
                    // A non-integer offset makes SQLite produce NULL, it does
                    // not raise, so the row contributes nothing.
                    None => {
                        column[indices[slot]] = Cell::Null;
                        continue;
                    }
                },
            };
            let target = if backward { (slot as i64).checked_sub(step) } else { (slot as i64).checked_add(step) };
            let target = match target {
                Some(target) if target >= 0 && (target as usize) < len => target as usize,
                _ => {
                    column[indices[slot]] = match default {
                        Some(default) => default.eval(&inputs[indices[slot]].values, &ctx)?,
                        None => Cell::Null,
                    };
                    continue;
                }
            };
            let target_ctx = self.window_context(inputs, indices[target], shape, outer, budget, ctes);
            column[indices[slot]] = value.eval(&inputs[indices[target]].values, &target_ctx)?;
        }
        Ok(())
    }

    /// `first_value`/`last_value`/`nth_value`: a value read from the frame.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_frame_value(
        &self,
        pick: ValuePick,
        value: &ResolvedExpr,
        position: Option<&ResolvedExpr>,
        window: &ResolvedWindow,
        indices: &[usize],
        orders: &[Vec<Cell>],
        inputs: &[WindowRow],
        column: &mut [Cell],
        peer_start: &[usize],
        peer_end: &[usize],
        group_of: &[i64],
        shape: &ScopeShape,
        outer: &[Scope],
        budget: &Budget,
        ctes: Option<&CteEnv>,
    ) -> Result<(), SqlError> {
        let len = indices.len();
        for slot in 0..len {
            let ctx = self.window_context(inputs, indices[slot], shape, outer, budget, ctes);
            let (start, end) = self.frame_bounds(window, slot, len, indices, orders, peer_start, peer_end, group_of, &ctx)?;
            let exclude = window.frame.as_ref().map_or(FrameExclude::NoOthers, |frame| frame.exclude);
            // Every position still in the frame after EXCLUDE, in frame order.
            let mut positions = (start.max(0)..=end.min(len as i64 - 1))
                .filter(|position| !frame_row_excluded(exclude, *position as usize, slot, peer_start, peer_end))
                .map(|position| position as usize);
            let wanted = match pick {
                ValuePick::First => positions.next(),
                ValuePick::Last => positions.last(),
                ValuePick::Nth => {
                    let position = position.expect("nth_value carries a position");
                    match nth_position(&position.eval(&inputs[indices[slot]].values, &ctx)?)? {
                        0 => unreachable!("nth_position rejects zero"),
                        nth => positions.nth(nth as usize - 1),
                    }
                }
            };
            let Some(wanted) = wanted
            else {
                column[indices[slot]] = Cell::Null;
                continue;
            };
            let target_ctx = self.window_context(inputs, indices[wanted], shape, outer, budget, ctes);
            column[indices[slot]] = value.eval(&inputs[indices[wanted]].values, &target_ctx)?;
        }
        Ok(())
    }

    /// Aggregate over the frame of each row.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_frame_aggregate(
        &self,
        function: AggregateFunction,
        argument: Option<&ResolvedExpr>,
        separator: Option<&ResolvedExpr>,
        distinct: bool,
        collation: Collation,
        filter: Option<&ResolvedExpr>,
        order: &[(ResolvedExpr, SortDirection)],
        window: &ResolvedWindow,
        indices: &[usize],
        orders: &[Vec<Cell>],
        inputs: &[WindowRow],
        column: &mut [Cell],
        peer_start: &[usize],
        peer_end: &[usize],
        group_of: &[i64],
        shape: &ScopeShape,
        outer: &[Scope],
        budget: &Budget,
        ctes: Option<&CteEnv>,
    ) -> Result<(), SqlError> {
        let len = indices.len();
        let exclude = window.frame.as_ref().map_or(FrameExclude::NoOthers, |frame| frame.exclude);
        for slot in 0..len {
            let ctx = self.window_context(inputs, indices[slot], shape, outer, budget, ctes);
            let (start, end) = self.frame_bounds(window, slot, len, indices, orders, peer_start, peer_end, group_of, &ctx)?;
            let mut entries: Vec<(Cell, Cell, Vec<Cell>)> = Vec::new();
            for position in start.max(0)..=end.min(len as i64 - 1) {
                if frame_row_excluded(exclude, position as usize, slot, peer_start, peer_end) {
                    continue;
                }
                let row = &inputs[indices[position as usize]];
                let row_ctx = self.window_context(inputs, indices[position as usize], shape, outer, budget, ctes);
                if let Some(filter) = filter {
                    if truthiness(&filter.eval(&row.values, &row_ctx)?) != Some(true) {
                        continue;
                    }
                }
                let value = match argument {
                    Some(argument) => argument.eval(&row.values, &row_ctx)?,
                    None => Cell::Null,
                };
                let separator = match separator {
                    Some(separator) => separator.eval(&row.values, &row_ctx)?,
                    None => Cell::Null,
                };
                let keys = order.iter().map(|(expr, _)| expr.eval(&row.values, &row_ctx)).collect::<Result<Vec<_>, SqlError>>()?;
                entries.push((value, separator, keys));
            }
            let (values, separators) = shape_aggregate_inputs(entries, distinct, order, collation);
            column[indices[slot]] = aggregate_cells(function, &values, if separator.is_some() { Some(&separators) } else { None });
        }
        Ok(())
    }

    /// Start and end positions (inclusive) of a row's frame inside its partition.
    #[allow(clippy::too_many_arguments)]
    fn frame_bounds(
        &self,
        window: &ResolvedWindow,
        slot: usize,
        len: usize,
        indices: &[usize],
        orders: &[Vec<Cell>],
        peer_start: &[usize],
        peer_end: &[usize],
        group_of: &[i64],
        _ctx: &EvalContext,
    ) -> Result<(i64, i64), SqlError> {
        let Some(frame) = &window.frame
        else {
            // No ORDER BY means the whole partition; otherwise the default is
            // `RANGE UNBOUNDED PRECEDING AND CURRENT ROW`, whose CURRENT ROW
            // edge is the last peer, not the current position.
            return Ok(if window.order.is_empty() { (0, len as i64 - 1) } else { (0, peer_end[slot] as i64) });
        };
        match frame.units {
            FrameUnits::Rows => {
                let start = rows_edge(&frame.start, "starting", slot, len)?;
                let end = rows_edge(&frame.end, "ending", slot, len)?;
                Ok((start, end))
            }
            FrameUnits::Groups => Ok((
                groups_edge(&frame.start, "starting", slot, len, peer_start, peer_end, group_of)?,
                groups_edge(&frame.end, "ending", slot, len, peer_start, peer_end, group_of)?,
            )),
            FrameUnits::Range => self.range_bounds(window, frame, slot, len, indices, orders, peer_start, peer_end),
        }
    }

    /// Value-based `RANGE` frame: rows whose ORDER BY value lies in the interval
    /// the edges describe, with `NULL`/text rows only ever matching peers.
    #[allow(clippy::too_many_arguments)]
    fn range_bounds(
        &self,
        window: &ResolvedWindow,
        frame: &ResolvedFrame,
        slot: usize,
        len: usize,
        indices: &[usize],
        orders: &[Vec<Cell>],
        peer_start: &[usize],
        peer_end: &[usize],
    ) -> Result<(i64, i64), SqlError> {
        let descending = window.order.first().is_some_and(|(_, direction)| direction.descending);
        let current = if window.order.len() == 1 { orders[indices[slot]].first().cloned() } else { None };
        // RANGE offsets are numeric only: text and blob ordering keys compare by
        // collation, so SQLite narrows such a frame to the peer group instead of
        // coercing the key with `numeric()`.
        let coerced = match current.as_ref() {
            Some(Cell::Integer(value)) => Some(Number::Int(*value)),
            Some(Cell::Real(value)) => Some(Number::Real(*value)),
            _ => None,
        };
        let offset_start = range_offset(&frame.start, "starting")?;
        let offset_end = range_offset(&frame.end, "ending")?;
        if (offset_start.is_some() || offset_end.is_some()) && window.order.len() != 1 {
            let mut error =
                SqlError::new("invalid-frame", "RANGE with offset PRECEDING/FOLLOWING requires one ORDER BY expression").in_statement("select");
            error.clause = Some("RANGE".to_string());
            return Err(error);
        }
        // A non-numeric current value can only match its own peers; unbounded
        // edges still reach the edges of the partition.
        let Some(current) = coerced
        else {
            let start = match frame.start.edge {
                FrameEdge::UnboundedPreceding => 0,
                FrameEdge::UnboundedFollowing => len as i64,
                _ => peer_start[slot] as i64,
            };
            let end = match frame.end.edge {
                FrameEdge::UnboundedFollowing => len as i64 - 1,
                FrameEdge::UnboundedPreceding => -1,
                _ => peer_end[slot] as i64,
            };
            return Ok((start, end));
        };
        let current = number_f64(current);
        // Each edge names a value on the ORDER BY axis. Descending order flips
        // PRECEDING towards larger values, so the frame is the interval between
        // the two edges, and is empty when they are in the wrong order.
        let start_edge = range_edge(frame.start.edge, offset_start, current, descending);
        let end_edge = range_edge(frame.end.edge, offset_end, current, descending);
        let ordered = if descending { start_edge >= end_edge } else { start_edge <= end_edge };
        if !ordered {
            return Ok((1, 0));
        }
        let low = start_edge.min(end_edge);
        let high = start_edge.max(end_edge);
        // An unbounded edge spans that whole side of the partition, so rows
        // whose ORDER BY key has no numeric form are not dropped from it.
        let start_unbounded = matches!(frame.start.edge, FrameEdge::UnboundedPreceding);
        let end_unbounded = matches!(frame.end.edge, FrameEdge::UnboundedFollowing);
        let mut first = start_unbounded.then_some(0);
        let mut last = end_unbounded.then_some(len - 1);
        for position in 0..len {
            let Some(value) = orders[indices[position]].first().and_then(numeric)
            else {
                continue;
            };
            let value = number_f64(value);
            if value >= low && value <= high {
                first.get_or_insert(position);
                if !end_unbounded {
                    last = Some(position);
                }
            }
        }
        // NULL sorts before every numeric key in ascending order, so the
        // leading run of rows without a numeric key stays inside a frame that
        // begins at the partition start even when its other edge is bounded.
        if start_unbounded && !descending && first.is_some() {
            let nulls = (0..len).take_while(|position| orders[indices[*position]].first().and_then(numeric).is_none()).count();
            if nulls > 0 {
                last = Some(last.map_or(nulls - 1, |last| last.max(nulls - 1)));
            }
        }
        Ok(match (first, last) {
            (Some(first), Some(last)) => (first as i64, last as i64),
            _ => (1, 0),
        })
    }

    /// Evaluates a `SELECT` statement, materializing its `WITH` entries first.
    ///
    /// A CTE that reads its own name is evaluated as a fixed point: the terms
    /// before the first self-reference form the seed, and the remaining terms
    /// run against the previous iteration's rows until the working set is
    /// empty. The `RECURSIVE` keyword is optional, matching SQLite.
    #[allow(clippy::too_many_arguments)]
    fn run_select(
        &self,
        with: &[CommonTable],
        recursive: bool,
        first: &SelectCore,
        compounds: &[CompoundTerm],
        order: Vec<OrderKey>,
        limit: Option<Limit>,
        outer: &[Scope],
        budget: &Budget,
        parent: Option<&CteEnv>,
    ) -> Result<ExecResult, SqlError> {
        if with.is_empty() {
            return self.select(first, compounds, order, limit, outer, budget, parent);
        }
        // Only an actual self-reference makes a `WITH` entry recursive, so the
        // keyword itself carries no extra behavior.
        let _ = recursive;
        let pushdown = self.recursive_limit(first, compounds, &order, limit.as_ref(), outer, budget, parent)?;
        let mut env = CteEnv { parent, tables: BTreeMap::new() };
        for entry in with {
            let bound = pushdown.as_ref().filter(|(name, _)| name.eq_ignore_ascii_case(&entry.name)).map(|(_, bound)| *bound);
            let result = match self.recursive_start(entry)? {
                Some(start) => self.materialize_recursive(entry, start, &env, budget, bound)?,
                None => {
                    let result = self.select_statement(&entry.query, &[], budget, Some(&env))?;
                    if let Some(names) = &entry.columns {
                        if names.len() != result.columns.len() {
                            let mut error = SqlError::new(
                                "arity-mismatch",
                                format!("table {} has {} values for {} columns", entry.name, result.columns.len(), names.len()),
                            )
                            .in_statement("select");
                            error.clause = Some("WITH".to_string());
                            return Err(error);
                        }
                    }
                    let columns = entry.columns.clone().unwrap_or(result.columns);
                    Materialized { columns: unique_output_names(&columns), rows: result.rows }
                }
            };
            env.tables.insert(entry.name.to_ascii_lowercase(), result);
        }
        self.select(first, compounds, order, limit, outer, budget, Some(&env))
    }

    /// Where the recursive part of a CTE begins, or `None` when the entry only
    /// reads earlier tables.
    ///
    /// SQLite splits the compound body at the first term that reads the CTE
    /// name: everything before it is the seed, everything after it is a
    /// recursive term and must also read the name. Mixed operators and a
    /// seed that reads its own CTE are reported as `circular reference`.
    fn recursive_start(&self, entry: &CommonTable) -> Result<Option<usize>, SqlError> {
        let Statement::Select { first, compounds, .. } = entry.query.as_ref()
        else {
            return Ok(None);
        };
        let name = entry.name.as_str();
        if core_references(first, name) {
            return Err(circular_reference(name));
        }
        let Some(start) = compounds.iter().position(|term| core_references(&term.core, name))
        else {
            if statement_references(&entry.query, name) {
                return Err(circular_reference(name));
            }
            return Ok(None);
        };
        if compounds[start..].iter().any(|term| !core_references(&term.core, name)) {
            return Err(circular_reference(name));
        }
        let op = compounds[start].op;
        if !matches!(op, CompoundOp::Union | CompoundOp::UnionAll) || compounds.iter().any(|term| term.op != op) {
            return Err(circular_reference(name));
        }
        Ok(Some(start))
    }

    /// Evaluates one recursive CTE to its fixed point.
    ///
    /// `bound` is the largest row count the enclosing query can still consume;
    /// it is only supplied when the outer `LIMIT` is directly over this CTE's
    /// rows, so an infinite recursion can be cut off without changing the
    /// result of the statement.
    fn materialize_recursive(
        &self,
        entry: &CommonTable,
        start: usize,
        env: &CteEnv,
        budget: &Budget,
        bound: Option<usize>,
    ) -> Result<Materialized, SqlError> {
        let Statement::Select { with: inner_with, first, compounds, order, limit, .. } = entry.query.as_ref()
        else {
            return Err(SqlError::new("unsupported-statement", "a `WITH` body must be a `SELECT`").in_statement("select"));
        };
        // Terms before the first self-reference are the initial select; they
        // run once and may not read the CTE.
        let seed_statement = Statement::Select {
            with: inner_with.clone(),
            recursive: false,
            first: first.clone(),
            compounds: compounds[..start].to_vec(),
            order: Vec::new(),
            limit: None,
        };
        let seed = self.select_statement(&seed_statement, &[], budget, Some(env))?;
        if let Some(names) = &entry.columns {
            if names.len() != seed.columns.len() {
                let mut error = SqlError::new(
                    "arity-mismatch",
                    format!("table {} has {} values for {} columns", entry.name, seed.columns.len(), names.len()),
                )
                .in_statement("select");
                error.clause = Some("WITH".to_string());
                return Err(error);
            }
        }
        let columns = entry.columns.clone().unwrap_or(seed.columns);
        let width = columns.len();
        let operator = compounds[start].op;
        // The CTE's own `LIMIT` is also a stopping condition: SQLite cuts an
        // infinite recursion as soon as the body has produced that many rows.
        let own = if order.is_empty() {
            match self.eval_limit(limit.as_ref(), &[], budget, Some(env))? {
                Some((offset, Some(count))) => Some(offset.saturating_add(count)),
                _ => None,
            }
        }
        else {
            None
        };
        let bound = match (bound, own) {
            (Some(outer), Some(own)) => Some(outer.min(own)),
            (outer, own) => outer.or(own),
        };
        // `UNION` deduplicates the whole fixed point, including the seed.
        let mut rows = if matches!(operator, CompoundOp::Union) { distinct_rows(&seed.rows) } else { seed.rows };
        if let Some(bound) = bound {
            rows.truncate(bound);
        }
        let mut seen: Vec<Vec<Cell>> = if matches!(operator, CompoundOp::Union) { rows.clone() } else { Vec::new() };
        let mut working = rows.clone();
        while !working.is_empty() {
            if bound.is_some_and(|limit| rows.len() >= limit) {
                break;
            }
            let mut next: Vec<Vec<Cell>> = Vec::new();
            let mut done = false;
            for term in &compounds[start..] {
                let scope = CteEnv {
                    parent: Some(env),
                    tables: BTreeMap::from([(
                        entry.name.to_ascii_lowercase(),
                        Materialized { columns: columns.clone(), rows: working.clone() },
                    )]),
                };
                let output = self.select(&term.core, &[], Vec::new(), None, &[], budget, Some(&scope))?;
                if output.columns.len() != width {
                    let mut error = SqlError::new(
                        "arity-mismatch",
                        format!("SELECTs to the left and right of {} do not have the same number of result columns", term.op.text()),
                    )
                    .in_statement("select");
                    error.clause = Some(term.op.clause().to_string());
                    error.position = Some(term.position);
                    return Err(error);
                }
                for row in output.rows {
                    if bound.is_some_and(|limit| rows.len() >= limit) {
                        done = true;
                        break;
                    }
                    if matches!(term.op, CompoundOp::Union) {
                        if seen.iter().any(|existing| cells_equal(existing, &row)) {
                            continue;
                        }
                        seen.push(row.clone());
                    }
                    next.push(row.clone());
                    rows.push(row);
                }
                if done {
                    break;
                }
            }
            if done {
                break;
            }
            working = next;
        }
        // A trailing `ORDER BY` / `LIMIT` on the CTE body applies to its whole
        // output, like a compound select outside a `WITH`.
        if !order.is_empty() {
            let keys = compound_order_keys(order, &columns)?;
            rows.sort_by(|left, right| {
                keys.iter()
                    .find_map(|(index, direction)| {
                        let ordering = direction.compare(&left[*index], &right[*index]);
                        (ordering != Ordering::Equal).then_some(ordering)
                    })
                    .unwrap_or(Ordering::Equal)
            });
        }
        let evaluated = self.eval_limit(limit.as_ref(), &[], budget, Some(env))?;
        if evaluated.is_some() {
            let (start, end) = limit_range(rows.len(), evaluated);
            rows = rows.drain(start..end).collect();
        }
        Ok(Materialized { columns, rows })
    }

    /// Computes a safe row bound for a recursive CTE when the enclosing query
    /// is `SELECT <row-wise items> FROM <cte> LIMIT n [OFFSET m]`.
    ///
    /// Materializing a recursive CTE eagerly would never terminate for an
    /// unbounded recursion, while SQLite stops as soon as the `LIMIT` is
    /// satisfied. The bound is only used when every row of the CTE produces at
    /// most one output row and nothing else reads the CTE, so truncating the
    /// fixed point at `offset + count` rows cannot change the result.
    #[allow(clippy::too_many_arguments)]
    fn recursive_limit(
        &self,
        first: &SelectCore,
        compounds: &[CompoundTerm],
        order: &[OrderKey],
        limit: Option<&Limit>,
        outer: &[Scope],
        budget: &Budget,
        parent: Option<&CteEnv>,
    ) -> Result<Option<(String, usize)>, SqlError> {
        if !compounds.is_empty()
            || !order.is_empty()
            || limit.is_none()
            || !first.group.is_empty()
            || first.having.is_some()
            || first.filter.is_some()
            || !first.windows.is_empty()
            || !row_preserving_items(&first.items)
            || first.from.len() != 1
        {
            return Ok(None);
        }
        let source = &first.from[0];
        if source.query.is_some() || source.join.is_some() || source.using.is_some() || source.natural || source.name.is_empty() {
            return Ok(None);
        }
        let Some((offset, Some(count))) = self.eval_limit(limit, outer, budget, parent)?
        else {
            return Ok(None);
        };
        Ok(Some((source.name.to_ascii_lowercase(), offset.saturating_add(count))))
    }

    /// Detects a `WHERE <rowid> = <integer literal>` conjunct that bounds a
    /// single base rowid table to one candidate row.
    ///
    /// Only the plain main-schema rowid-table branch of [`Engine::select_source`]
    /// honors the pin; every other source shape (join, derived table, view, temp
    /// table, `WITHOUT ROWID`, attached schema, non-integer literal, or an
    /// equality sitting under `OR` / `NOT`) yields `None` so the scan stays a
    /// full read. The predicate is still evaluated in full downstream, so a pin
    /// never changes the emitted row set or order — it only narrows the
    /// candidate rows the scan decodes.
    fn rowid_pin(&self, from: &[TableRef], filter: &Option<Expr>) -> Option<i64> {
        if from.len() != 1 {
            return None;
        }
        let table = &from[0];
        if table.query.is_some() {
            return None;
        }
        // An unqualified name reaches the bounded branch only when no temp
        // object shadows it; `temp.` never does. A view re-evaluates its query
        // and is not bounded here.
        let key = table.name.to_ascii_lowercase();
        let reaches_main = match table.schema.as_deref() {
            None => !self.temp_lookup.contains_key(&key) && !self.temp_views.contains_key(&key),
            Some(qualifier) => {
                let lower = qualifier.to_ascii_lowercase();
                lower == "main" || self.alias.as_deref().is_some_and(|alias| alias.eq_ignore_ascii_case(&lower))
            }
        };
        if !reaches_main || self.views.contains_key(&key) {
            return None;
        }
        let position = *self.table_lookup.get(&key)?;
        let def = &self.tables[position];
        if def.without_rowid {
            return None;
        }
        let alias_name = def.rowid_alias.map(|index| def.columns[index].name.clone());
        Self::rowid_eq_conjunct(filter.as_ref()?, alias_name.as_deref(), table.label(), &self.parameters)
    }

    /// Walks the top-level `AND` chain of a predicate for a `rowid = <int>` (or
    /// `<int> = rowid`) comparison on the rowid-alias column or an implicit
    /// `rowid` / `_rowid_` / `oid` name. Anything outside a plain `AND` of
    /// `=` comparisons yields `None`.
    ///
    /// The right-hand side may be an integer literal or a bound parameter whose
    /// value is already known at execution time; a non-integer binding keeps the
    /// full scan, exactly like a non-integer literal.
    fn rowid_eq_conjunct(expr: &Expr, alias_name: Option<&str>, label: &str, parameters: &[Cell]) -> Option<i64> {
        match expr {
            Expr::And(left, right) => Self::rowid_eq_conjunct(left, alias_name, label, parameters)
                .or_else(|| Self::rowid_eq_conjunct(right, alias_name, label, parameters)),
            Expr::Compare { left, op, right } if *op == CompareOp::Eq => {
                let (column, value) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(column), value) => (column, known_cell(value, parameters)?),
                    (value, Expr::Column(column)) => (column, known_cell(value, parameters)?),
                    _ => return None,
                };
                // JS 绑定的数字默认是 REAL 存储类，而 rowid 列是 INTEGER 亲和。
                // SQLite 比较前会做亲和转换，`id = 3.0` 与 `id = 3` 命中同一行；
                // 这里把无小数部分的 REAL 折成 INTEGER 才不丢掉这条下推。
                let value = match value {
                    Cell::Integer(value) => value,
                    Cell::Real(real) => integral_rowid(real)?,
                    _ => return None,
                };
                if column.qualifier.as_deref().is_some_and(|qualifier| !qualifier.eq_ignore_ascii_case(label)) {
                    return None;
                }
                let rowid_named = is_rowid_alias(&column.name);
                let alias_named = alias_name.is_some_and(|name| name.eq_ignore_ascii_case(&column.name));
                if rowid_named || alias_named { Some(value) } else { None }
            }
            _ => None,
        }
    }

    /// Single base rowid table of the main schema that a bounded scan may
    /// target: one `FROM` item, no derived table, no temp object or view
    /// shadowing the name, not `WITHOUT ROWID`. This is the shape gate shared
    /// by the point-pin and index-pin paths; anything else keeps the full read.
    fn bounded_scan_target<'a>(&'a self, from: &'a [TableRef]) -> Option<(&'a TableDef, &'a TableRef)> {
        if from.len() != 1 {
            return None;
        }
        let table = &from[0];
        if table.query.is_some() {
            return None;
        }
        let key = table.name.to_ascii_lowercase();
        let reaches_main = match table.schema.as_deref() {
            None => !self.temp_lookup.contains_key(&key) && !self.temp_views.contains_key(&key),
            Some(qualifier) => {
                let lower = qualifier.to_ascii_lowercase();
                lower == "main" || self.alias.as_deref().is_some_and(|alias| alias.eq_ignore_ascii_case(&lower))
            }
        };
        if !reaches_main || self.views.contains_key(&key) {
            return None;
        }
        let position = *self.table_lookup.get(&key)?;
        let def = &self.tables[position];
        if def.without_rowid {
            return None;
        }
        Some((def, table))
    }

    /// Detects a `WHERE` conjunction that pins every key column of one plain
    /// column index to a literal with `=`, and returns the matching rowids in
    /// ascending order.
    ///
    /// The gate is deliberately narrow so the probe can never miss a row the
    /// full predicate would keep. Only non-managed indexes over declared
    /// columns qualify, each key column's literal must match the column's
    /// affinity class exactly (a `TEXT` column with a text literal, an
    /// `INTEGER` column with an integer literal, a `REAL` column with a real
    /// literal), and a stored key the canonical signature cannot reproduce
    /// poisons the whole probe. The full predicate still runs downstream, so
    /// over-fetching is harmless. Within one full-key equality the index
    /// entries are ordered by rowid, which is also the order of the full
    /// table scan, so the emitted rows keep their position.
    fn index_pin(&self, from: &[TableRef], filter: &Option<Expr>) -> Option<Vec<i64>> {
        let (def, table) = self.bounded_scan_target(from)?;
        let filter = filter.as_ref()?;
        let table_key = def.name.to_ascii_lowercase();
        for index in self.indexes.values() {
            if index.managed || index.table != table_key || index.columns.is_empty() {
                continue;
            }
            if let Some(rowids) = self.index_probe_rows(index, def, filter, table.label()) {
                return Some(rowids);
            }
            if let Some(rowids) = self.index_range_rows(index, def, filter, table.label()) {
                return Some(rowids);
            }
        }
        None
    }

    /// Builds the probe key from the equality literals and looks it up in the
    /// index's cached key set. `None` means the shape is not pushdown-safe and
    /// the caller must fall back to the full scan.
    fn index_probe_rows(&self, index: &IndexDef, def: &TableDef, filter: &Expr, label: &str) -> Option<Vec<i64>> {
        let mut equalities: Vec<(usize, Cell)> = Vec::new();
        collect_index_equalities(filter, def, label, &self.parameters, &mut equalities);
        if equalities.is_empty() {
            return None;
        }
        let mut key = Vec::with_capacity(index.columns.len());
        for (slot, position) in index.columns.iter().enumerate() {
            let (_, cell) = equalities.iter().find(|(column, _)| column == position)?;
            let affinity = affinity_of(&def.columns[*position].declared_type);
            // INTEGER 亲和列上的绑定数字是 REAL 存储类；无小数部分时折成
            // INTEGER，索引键的规范签名才会与存储键一致。
            let cell = match (affinity, cell) {
                (Affinity::Integer, Cell::Real(real)) => Cell::Integer(integral_rowid(*real)?),
                _ => cell.clone(),
            };
            match (affinity, &cell) {
                (Affinity::Text, Cell::Text(_)) | (Affinity::Real, Cell::Real(_)) | (Affinity::Integer, Cell::Integer(_)) => {}
                _ => return None,
            }
            let collation = index.collations.get(slot).copied().unwrap_or_default();
            key.push(probe_key(&cell, collation)?);
        }
        if key.iter().any(|part| matches!(part, ProbeKey::Wide(_))) {
            return None;
        }
        self.index_probe_rowids(index, &key)
    }

    /// Returns the rowids stored under one probe key, rebuilding the cached
    /// key set first when the write epoch moved on.
    fn index_probe_rowids(&self, index: &IndexDef, key: &[ProbeKey]) -> Option<Vec<i64>> {
        let version = self.index_version(&index.name);
        let name = index.name.to_ascii_lowercase();
        {
            let cache = self.index_probes.borrow();
            if let Some(probe) = cache.get(&name) {
                if probe.epoch == version {
                    return if probe.poisoned { None } else { probe.map.get(key).cloned() };
                }
            }
        }
        let probe = self.build_index_probe(index, version);
        let rowids = if probe.poisoned { None } else { probe.map.get(key).cloned() };
        self.index_probes.borrow_mut().insert(name, probe);
        rowids
    }

    /// Detects a range predicate on the index's leading column and returns the
    /// matching rowids in ascending order.
    ///
    /// The gate is as narrow as the equality probe: only non-managed indexes
    /// over declared columns qualify, every bound literal must match the
    /// column's affinity class, and a poisoned key set keeps the full scan. The
    /// full predicate still runs downstream, so a wider candidate set is safe,
    /// and ascending rowids preserve the full-scan order.
    fn index_range_rows(&self, index: &IndexDef, def: &TableDef, filter: &Expr, label: &str) -> Option<Vec<i64>> {
        let position = *index.columns.first()?;
        let mut lower: Option<IndexBound> = None;
        let mut upper: Option<IndexBound> = None;
        collect_range_bounds(filter, def, label, position, &self.parameters, &mut lower, &mut upper);
        if lower.is_none() && upper.is_none() {
            return None;
        }
        let affinity = affinity_of(&def.columns[position].declared_type);
        for bound in [lower.as_mut(), upper.as_mut()].into_iter().flatten() {
            // 绑定数字是 REAL 存储类，INTEGER 亲和列的等值 / 范围比较会先做亲和
            // 转换；无小数部分的 REAL 折成 INTEGER 才能继续用索引探针。
            if let (Affinity::Integer, Cell::Real(real)) = (&affinity, &bound.cell) {
                match integral_rowid(*real) {
                    Some(value) => bound.cell = Cell::Integer(value),
                    None => return None,
                }
            }
            match (&affinity, &bound.cell) {
                (Affinity::Text, Cell::Text(_)) | (Affinity::Real, Cell::Real(_)) | (Affinity::Integer, Cell::Integer(_)) => {}
                _ => return None,
            }
        }
        let collation = index.collations.first().copied().unwrap_or_default();
        self.index_range_rowids(index, &lower, &upper, collation)
    }

    /// Filters the cached index entries against the range bounds, rebuilding
    /// the cache first when the write epoch moved on.
    fn index_range_rowids(
        &self,
        index: &IndexDef,
        lower: &Option<IndexBound>,
        upper: &Option<IndexBound>,
        collation: Collation,
    ) -> Option<Vec<i64>> {
        let version = self.index_version(&index.name);
        let name = index.name.to_ascii_lowercase();
        let select = |probe: &IndexProbe| -> Option<Vec<i64>> {
            if probe.poisoned {
                return None;
            }
            // 索引项按首列有序时,用两次二分取出候选区间,只遍历命中项。
            if probe.sorted {
                let collation = collation;
                let lower_start = match lower {
                    None => 0,
                    Some(bound) => probe.entries.partition_point(|(key, _)| match key.first() {
                        Some(cell) => {
                            let ordering = compare_cells_with(cell, &bound.cell, collation);
                            ordering == Ordering::Less || (!bound.inclusive && ordering == Ordering::Equal)
                        }
                        None => true,
                    }),
                };
                let upper_end = match upper {
                    None => probe.entries.len(),
                    Some(bound) => probe.entries.partition_point(|(key, _)| match key.first() {
                        Some(cell) => {
                            let ordering = compare_cells_with(cell, &bound.cell, collation);
                            ordering == Ordering::Less || (bound.inclusive && ordering == Ordering::Equal)
                        }
                        None => false,
                    }),
                };
                if lower_start >= upper_end {
                    return Some(Vec::new());
                }
                let mut rowids: Vec<i64> = probe.entries[lower_start..upper_end]
                    .iter()
                    .map(|(_, rowid)| *rowid)
                    .filter(|rowid| !probe.deleted.contains(rowid))
                    .collect();
                rowids.sort_unstable();
                return Some(rowids);
            }
            let mut rowids = Vec::new();
            for (key, rowid) in &probe.entries {
                if probe.deleted.contains(rowid) {
                    continue;
                }
                let Some(cell) = key.first() else {
                    continue;
                };
                if let Some(bound) = lower {
                    let ordering = compare_cells_with(cell, &bound.cell, collation);
                    if ordering == Ordering::Less || (!bound.inclusive && ordering == Ordering::Equal) {
                        continue;
                    }
                }
                if let Some(bound) = upper {
                    let ordering = compare_cells_with(cell, &bound.cell, collation);
                    if ordering == Ordering::Greater || (!bound.inclusive && ordering == Ordering::Equal) {
                        continue;
                    }
                }
                rowids.push(*rowid);
            }
            rowids.sort_unstable();
            Some(rowids)
        };
        {
            let cache = self.index_probes.borrow();
            if let Some(probe) = cache.get(&name) {
                if probe.epoch == version {
                    return select(probe);
                }
            }
        }
        let probe = self.build_index_probe(index, version);
        let rowids = select(&probe);
        self.index_probes.borrow_mut().insert(name, probe);
        rowids
    }

    /// Decodes one index's live key set into canonical signatures. A stored
    /// key outside the canonical form (a NaN or a wide integer) poisons the
    /// probe so every later query keeps the full scan.
    fn build_index_probe(&self, index: &IndexDef, epoch: u64) -> IndexProbe {
        let mut probe =
            IndexProbe { epoch, map: HashMap::new(), entries: Vec::new(), sorted: false, deleted: HashSet::new(), poisoned: false };
        let entries = match self.writer.index_entries_decoded(&index.name) {
            Ok(entries) => entries,
            Err(_) => {
                probe.poisoned = true;
                return probe;
            }
        };
        probe.entries.reserve(entries.len());
        for (values, rowid) in entries {
            if values.len() != index.columns.len() {
                probe.poisoned = true;
                break;
            }
            let cells: Vec<Cell> = values.iter().map(write_value_cell).collect();
            let mut key = Vec::with_capacity(values.len());
            let mut canonical = true;
            for (cell, collation) in cells.iter().zip(&index.collations) {
                match probe_key(cell, *collation) {
                    Some(part) if !matches!(part, ProbeKey::Wide(_)) => key.push(part),
                    _ => {
                        canonical = false;
                        break;
                    }
                }
            }
            if !canonical {
                probe.poisoned = true;
                break;
            }
            probe.map.entry(key).or_default().push(rowid);
            probe.entries.push((cells, rowid));
        }
        if !probe.poisoned {
            for rowids in probe.map.values_mut() {
                rowids.sort_unstable();
            }
            // 索引项通常按首列有序存储,确认一次后范围探针即可二分。
            let collation = index.collations.first().copied().unwrap_or_default();
            probe.sorted = probe.entries.windows(2).all(|pair| match (pair[0].0.first(), pair[1].0.first()) {
                (Some(left), Some(right)) => compare_cells_with(left, right, collation) != Ordering::Greater,
                _ => false,
            });
        }
        probe
    }

    /// 一条索引的内容版本:从未登记过的索引视为 `0`。
    fn index_version(&self, name: &str) -> u64 {
        self.index_versions.borrow().get(&name.to_ascii_lowercase()).copied().unwrap_or(0)
    }

    /// 推进时钟并返回新 epoch。
    fn next_epoch(&self) -> u64 {
        let next = self.write_epoch.get().wrapping_add(1);
        self.write_epoch.set(next);
        next
    }

    /// 取出一条写语句的目标表名(小写)。
    ///
    /// 只有裸表名或 `main` / 本库别名限定的 DML 才能精确到单表;其余形态
    /// (其它 attached schema、DDL 等)返回 `None`,调用方按全量失效处理。
    fn statement_table_key(&self, statement: &Statement) -> Option<String> {
        let (schema, table) = match statement {
            Statement::Insert { schema, table, .. }
            | Statement::Update { schema, table, .. }
            | Statement::Delete { schema, table, .. } => (schema.as_deref(), table),
            _ => return None,
        };
        match schema {
            None => Some(table.to_ascii_lowercase()),
            Some(qualifier) => {
                let lower = qualifier.to_ascii_lowercase();
                if lower == "main" || self.alias.as_deref().is_some_and(|alias| alias.eq_ignore_ascii_case(&lower)) {
                    Some(table.to_ascii_lowercase())
                }
                else {
                    None
                }
            }
        }
    }

    /// 让一张表上的全部索引缓存失效。
    fn invalidate_table_indexes(&self, table_key: &str) {
        let epoch = self.next_epoch();
        let mut versions = self.index_versions.borrow_mut();
        for index in self.indexes.values() {
            if index.table == table_key {
                versions.insert(index.name.to_ascii_lowercase(), epoch);
            }
        }
    }

    /// 让全部索引缓存失效。
    fn invalidate_all_indexes(&self) {
        let epoch = self.next_epoch();
        let mut versions = self.index_versions.borrow_mut();
        for index in self.indexes.values() {
            versions.insert(index.name.to_ascii_lowercase(), epoch);
        }
    }

    /// `UPDATE` 执行后只让真正被改动的索引失效。
    ///
    /// 索引项只由被索引列的值和行 rowid 决定:赋值没有碰到任何索引列、也没改
    /// rowid 时,索引内容与语句执行前逐项一致,缓存可以继续复用。托管索引
    /// (表达式、`COLLATE`、部分索引)的键可能依赖任意列,一律按失效处理。
    fn invalidate_indexes_after_update(&self, def: &TableDef, table_key: &str, assigned: &[bool]) {
        let rowid_moved = def.rowid_alias.is_some_and(|position| assigned.get(position).copied().unwrap_or(false));
        let mut dirty: Vec<String> = Vec::new();
        for index in self.indexes.values() {
            if index.table != table_key {
                continue;
            }
            let affected = rowid_moved
                || index.managed
                || index.columns.is_empty()
                || index.columns.iter().any(|position| assigned.get(*position).copied().unwrap_or(true));
            if affected {
                dirty.push(index.name.to_ascii_lowercase());
            }
        }
        if dirty.is_empty() {
            return;
        }
        let epoch = self.next_epoch();
        let mut versions = self.index_versions.borrow_mut();
        for name in dirty {
            versions.insert(name, epoch);
        }
    }

    /// `DELETE` 执行后把被删行从已缓存的索引探针里摘掉。
    ///
    /// 缓存是整份索引的快照,按 rowid 精确剔除与整树重建等价,因此不必让
    /// 每次删除都重付一遍全索引解码。托管索引与已过期的缓存退化为常规失效,
    /// 由下一次探针重建。
    fn patch_index_probes_after_delete(&self, table_key: &str, plan: &DeletePlan) {
        let removed: HashSet<i64> = plan.removed.iter().copied().collect();
        let mut stale: Vec<String> = Vec::new();
        let mut patched: Vec<String> = Vec::new();
        {
            let mut probes = self.index_probes.borrow_mut();
            for index in self.indexes.values() {
                if index.table != table_key {
                    continue;
                }
                let name = index.name.to_ascii_lowercase();
                let version = self.index_version(&index.name);
                let Some(probe) = probes.get_mut(&name) else {
                    continue;
                };
                if index.managed || index.columns.is_empty() || probe.poisoned || probe.epoch != version {
                    stale.push(name);
                    continue;
                }
                for (rowid, cells) in &plan.live {
                    if !removed.contains(rowid) {
                        continue;
                    }
                    let mut key = Vec::with_capacity(index.columns.len());
                    let mut canonical = true;
                    for (slot, position) in index.columns.iter().enumerate() {
                        let collation = index.collations.get(slot).copied().unwrap_or_default();
                        match cells.get(*position).and_then(|cell| probe_key(cell, collation)) {
                            Some(part) if !matches!(part, ProbeKey::Wide(_)) => key.push(part),
                            _ => {
                                canonical = false;
                                break;
                            }
                        }
                    }
                    if !canonical {
                        probe.poisoned = true;
                        break;
                    }
                    let emptied = match probe.map.get_mut(&key) {
                        Some(rowids) => {
                            rowids.retain(|id| id != rowid);
                            rowids.is_empty()
                        }
                        None => false,
                    };
                    if emptied {
                        probe.map.remove(&key);
                    }
                }
                if probe.poisoned {
                    stale.push(name);
                    continue;
                }
                if !removed.is_empty() {
                    // 只记墓碑;条目累积到一半时再一次性压缩,单条删除不再
                    // 扫整份索引项。
                    probe.deleted.extend(removed.iter().copied());
                    if probe.deleted.len() * 2 > probe.entries.len() {
                        let deleted = std::mem::take(&mut probe.deleted);
                        probe.entries.retain(|(_, rowid)| !deleted.contains(rowid));
                    }
                }
                patched.push(name);
            }
        }
        if stale.is_empty() && patched.is_empty() {
            return;
        }
        let epoch = self.next_epoch();
        {
            let mut versions = self.index_versions.borrow_mut();
            for name in stale.iter().chain(patched.iter()) {
                versions.insert(name.clone(), epoch);
            }
        }
        // 打过补丁的探针内容仍然准确,只把版本对齐到新 epoch 继续复用。
        if !patched.is_empty() {
            let mut probes = self.index_probes.borrow_mut();
            for name in &patched {
                if let Some(probe) = probes.get_mut(name) {
                    probe.epoch = epoch;
                }
            }
        }
    }

    /// Detects `ORDER BY <rowid order> LIMIT n` over a single base rowid table
    /// with nothing else that could reorder or drop rows: no `WHERE`, no
    /// `DISTINCT`, no aggregate, group or window, no `count(*)`, no `DESC` and
    /// no `NULLS` override. Returns the number of leading rows the scan has to
    /// read (`offset + count`).
    ///
    /// A table scan already emits rowid order, which is exactly what an
    /// ascending rowid sort produces, so reading only the prefix and letting
    /// the generic sort run over it keeps the rows and their order identical.
    /// Projection items must stay bare column references (or `*`): an aliased
    /// projection could make an `ORDER BY` name resolve to a different
    /// expression than the rowid column the scan order is based on.
    fn rowid_head_bound(&self, first: &SelectCore, order: &[OrderKey], limit: Option<(usize, Option<usize>)>) -> Option<usize> {
        if order.is_empty()
            || first.filter.is_some()
            || first.distinct
            || !first.group.is_empty()
            || first.having.is_some()
            || !first.windows.is_empty()
            || matches!(first.items, SelectItems::CountAll)
        {
            return None;
        }
        if !matches!(first.items, SelectItems::Star)
            && !matches!(&first.items, SelectItems::Named(exprs) if exprs.iter().all(|expr| matches!(expr, Expr::Column(_))))
        {
            return None;
        }
        let Some((offset, Some(count))) = limit else {
            return None;
        };
        let (def, table) = self.bounded_scan_target(&first.from)?;
        let alias_name = def.rowid_alias.map(|index| def.columns[index].name.clone());
        for entry in order {
            if entry.descending || entry.nulls_first.is_some() {
                return None;
            }
            let Expr::Column(column) = &entry.expr else {
                return None;
            };
            if column.qualifier.as_deref().is_some_and(|qualifier| !qualifier.eq_ignore_ascii_case(table.label())) {
                return None;
            }
            let rowid_named = is_rowid_alias(&column.name);
            let alias_named = alias_name.as_deref().is_some_and(|name| name.eq_ignore_ascii_case(&column.name));
            if !rowid_named && !alias_named {
                return None;
            }
        }
        Some(offset.saturating_add(count))
    }

    /// 尝试为一个简单单表扫描建立惰性游标。
    ///
    /// 只有同时满足下列条件的 `SELECT` 才返回 `Some`:
    ///
    /// - 单表、无 `JOIN`、无派生表/子查询、无 CTE 与复合查询;
    /// - 无 `DISTINCT`、`GROUP BY`、`HAVING`、窗口与聚合;
    /// - 无 `LIMIT`/`OFFSET`;
    /// - 无 `ORDER BY`,或排序键就是主键/rowid 升序,与 b-tree 扫描顺序一致;
    /// - 投影是 `*` 或普通列引用/常量表达式,`WHERE` 可逐行求值。
    ///
    /// 打开时把当前整页镜像克隆成独立快照,后续写入只改引擎自己的页缓冲,
    /// 游标因此始终读到打开时刻的一致状态。返回 `None` 表示语句不满足条件,
    /// 调用方走既有的物化路径。
    pub(crate) fn open_lazy_cursor(&mut self, statement: &Statement, parameters: &[Cell]) -> Result<Option<LazyCursor>, SqlError> {
        let Some(plan) = self.plan_lazy_scan(statement)?
        else {
            return Ok(None);
        };
        // 游标快照直接克隆镜像页,推迟的索引重建必须先落下。
        self.flush_pending_materialization("cursor")?;
        let objects = match self.writer.schema_objects_with_rowids(TableLimits::default()) {
            Ok(objects) => objects,
            Err(_) => return Ok(None),
        };
        let root_page = objects
            .iter()
            .find(|(_, object)| object.kind == SchemaObjectKind::Table && object.name.eq_ignore_ascii_case(&plan.def.name))
            .and_then(|(_, object)| object.root_page)
            .filter(|root| *root != 0 && *root != 1);
        let Some(root_page) = root_page
        else {
            return Ok(None);
        };
        let snapshot = Arc::new(self.writer.pages().to_vec());
        let scan = LazyTableScan::new(snapshot, root_page)?;
        Ok(Some(LazyCursor { plan, scan, budget: Budget::new(), parameters: parameters.to_vec(), finished: false }))
    }

    /// 从惰性游标读取至多 `limit` 行,保持 b-tree 扫描顺序。
    pub(crate) fn lazy_cursor_next(&mut self, cursor: &mut LazyCursor, limit: usize) -> Result<LazyBatch, SqlError> {
        if cursor.finished || limit == 0 {
            return Ok(LazyBatch { rows: Vec::new(), done: cursor.finished });
        }
        let saved = std::mem::take(&mut self.parameters);
        self.parameters = cursor.parameters.clone();
        let result = self.lazy_cursor_next_inner(cursor, limit);
        self.parameters = saved;
        result
    }

    fn lazy_cursor_next_inner(&self, cursor: &mut LazyCursor, limit: usize) -> Result<LazyBatch, SqlError> {
        let shape = ScopeShape { columns: cursor.plan.shape_columns.clone() };
        let empty_aggregates: Vec<Cell> = Vec::new();
        let mut rows = Vec::new();
        while rows.len() < limit {
            let stored = cursor.scan.next_rows(limit - rows.len())?;
            if stored.is_empty() {
                cursor.finished = true;
                break;
            }
            let decoded = decode_stored_rows(&stored, &cursor.plan.def, cursor.plan.mask.as_deref())?;
            for values in decoded {
                let ctx = EvalContext {
                    engine: self,
                    outer: &[],
                    current: &shape,
                    aggregates: &empty_aggregates,
                    windows: &[],
                    window_row: 0,
                    budget: &cursor.budget,
                    ctes: None,
                };
                if let Some(predicate) = &cursor.plan.predicate
                    && truthiness(&predicate.eval(&values, &ctx)?) != Some(true)
                {
                    continue;
                }
                cursor.budget.charge(1)?;
                let out = match &cursor.plan.projection {
                    Some(nodes) => nodes.iter().map(|node| node.eval(&values, &ctx)).collect::<Result<Vec<_>, _>>()?,
                    None => values[..cursor.plan.visible_width].to_vec(),
                };
                rows.push(out);
                if rows.len() >= limit {
                    break;
                }
            }
        }
        Ok(LazyBatch { rows, done: cursor.finished })
    }

    /// 判定语句能否走惰性扫描,能把结果表达式解析成 owned 的执行计划。
    fn plan_lazy_scan(&self, statement: &Statement) -> Result<Option<LazyPlan>, SqlError> {
        let Statement::Select { with, first, compounds, order, limit, .. } = statement
        else {
            return Ok(None);
        };
        if !with.is_empty() || !compounds.is_empty() || limit.is_some() || first.from.len() != 1 {
            return Ok(None);
        }
        let entry = &first.from[0];
        if entry.query.is_some() || entry.join.is_some() || entry.on.is_some() || entry.using.is_some() || entry.natural {
            return Ok(None);
        }
        if first.distinct || !first.group.is_empty() || first.having.is_some() || !first.windows.is_empty() {
            return Ok(None);
        }
        if matches!(first.items, SelectItems::CountAll) {
            return Ok(None);
        }
        let Some(def) = self.prunable_def(entry)
        else {
            return Ok(None);
        };
        // 子查询与窗口会读到本行之外的上下文,不能按批惰性求值。
        let mut refs = Vec::new();
        if let SelectItems::Named(exprs) = &first.items {
            for expr in exprs {
                if !collect_unmasked_columns(expr, &mut refs) {
                    return Ok(None);
                }
            }
        }
        if let Some(filter) = &first.filter
            && !collect_unmasked_columns(filter, &mut refs)
        {
            return Ok(None);
        }
        if !lazy_order_matches_scan(def, entry, &first.items, order) {
            return Ok(None);
        }
        let source = Source {
            label: entry.label().to_string(),
            columns: SourceColumns::Table(def),
            offset: 0,
            join: None,
            star_hidden: vec![false; def.columns.len()],
            unqualified_hidden: vec![false; def.columns.len()],
            star_override: vec![None; def.columns.len()],
        };
        let mut relation =
            Relation { sources: vec![source], using: vec![Vec::new()], using_slots: Vec::new(), trigger: None, excluded: false };
        relation.plan_joins(&first.from)?;
        let shape = relation.shape();
        let outer_shapes: Vec<ScopeShape> = Vec::new();
        let mut specs = Vec::new();
        let mut windows = Vec::new();
        let projection = match &first.items {
            SelectItems::Named(exprs) => Some(
                exprs
                    .iter()
                    .map(|expr| resolve_expr(expr, &relation, &outer_shapes, &first.windows, &mut windows, &mut specs, true, true))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            SelectItems::Star => None,
            SelectItems::CountAll => unreachable!("count(*) is rejected above"),
        };
        // 聚合与窗口要么读到整组行,要么依赖排序后的位置,只能走物化路径。
        if !specs.is_empty() || !windows.is_empty() {
            return Ok(None);
        }
        let predicate = match &first.filter {
            Some(expr) => {
                let mut scratch_specs = Vec::new();
                let mut scratch_windows = Vec::new();
                Some(resolve_expr(expr, &relation, &outer_shapes, &[], &mut scratch_windows, &mut scratch_specs, false, false)?)
            }
            None => None,
        };
        let labels = core_output_labels(&relation, &first.items);
        let mask = self.read_column_masks(first, order).and_then(|masks| masks.into_iter().next().flatten());
        Ok(Some(LazyPlan {
            def: def.clone(),
            labels,
            visible_width: def.columns.len(),
            projection,
            predicate,
            shape_columns: shape.columns,
            mask,
        }))
    }

    /// Per-`FROM` column masks for the projected read, or `None` when the shape
    /// is not safe to prune.
    ///
    /// A mask marks the declared columns the statement can observe. Positions
    /// outside the mask decode as [`Cell::Null`] and are never read, which keeps
    /// text and blob cells out of memory for `count(*)`, single-column
    /// projections and aggregates. Only main-schema rowid tables qualify; CTEs,
    /// views, derived tables, WITHOUT ROWID tables and attached schemas keep
    /// their full decode, and `USING` / `NATURAL` joins read coalesced slots
    /// that the raw expressions never mention.
    fn read_column_masks(&self, first: &SelectCore, order: &[OrderKey]) -> Option<Vec<Option<Vec<bool>>>> {
        if first.from.is_empty()
            || matches!(first.items, SelectItems::Star)
            || !first.windows.is_empty()
            || first.from.iter().any(|entry| entry.using.is_some() || entry.natural)
        {
            return None;
        }
        let defs: Vec<Option<&TableDef>> = first.from.iter().map(|entry| self.prunable_def(entry)).collect();
        if defs.iter().all(Option::is_none) {
            return None;
        }
        let mut masks: Vec<Option<Vec<bool>>> = defs.iter().map(|def| def.map(|def| vec![false; def.columns.len()])).collect();
        let mut refs: Vec<&ColumnRef> = Vec::new();
        let collected = match &first.items {
            SelectItems::Named(items) => items.iter().all(|expr| collect_unmasked_columns(expr, &mut refs)),
            _ => true,
        } && first.filter.as_ref().is_none_or(|expr| collect_unmasked_columns(expr, &mut refs))
            && first.group.iter().all(|expr| collect_unmasked_columns(expr, &mut refs))
            && first.having.as_ref().is_none_or(|expr| collect_unmasked_columns(expr, &mut refs))
            && order.iter().all(|key| collect_unmasked_columns(&key.expr, &mut refs))
            && first.from.iter().all(|entry| entry.on.as_ref().is_none_or(|expr| collect_unmasked_columns(expr, &mut refs)));
        if !collected {
            return None;
        }
        for column in refs {
            let matched: Vec<usize> = match column.qualifier.as_deref() {
                Some(qualifier) => (0..first.from.len()).filter(|index| first.from[*index].label().eq_ignore_ascii_case(qualifier)).collect(),
                None => {
                    // An unqualified name over several sources cannot be
                    // attributed without the resolver, so keep every column.
                    if first.from.len() > 1 {
                        return None;
                    }
                    vec![0]
                }
            };
            for index in matched {
                let Some(mask) = masks[index].as_mut()
                else {
                    continue;
                };
                let def = defs[index].expect("a mask only exists for a prunable table");
                match def.columns.iter().position(|column_def| column_def.name.eq_ignore_ascii_case(&column.name)) {
                    Some(position) => mask[position] = true,
                    // The implicit rowid slots stay available whatever the mask.
                    None if is_rowid_alias(&column.name) => {}
                    None => masks[index] = None,
                }
            }
        }
        Some(masks)
    }

    /// The definition of a plain main-schema rowid table, or `None` when the
    /// `FROM` entry can resolve to a view, CTE, temp table or WITHOUT ROWID
    /// table whose rows are materialized by another path.
    fn prunable_def(&self, entry: &TableRef) -> Option<&TableDef> {
        if entry.query.is_some() {
            return None;
        }
        let main = match entry.schema.as_deref() {
            None => true,
            Some(qualifier) => {
                qualifier.eq_ignore_ascii_case("main") || self.alias.as_deref().is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
            }
        };
        if !main {
            return None;
        }
        let key = entry.name.to_ascii_lowercase();
        // Views and temp tables shadow the main table in the resolver, so any
        // lookup they could win keeps the full decode.
        if self.views.contains_key(&key) || self.temp_views.contains_key(&key) || self.temp_lookup.contains_key(&key) {
            return None;
        }
        let position = self.table_lookup.get(&key).copied()?;
        let def = &self.tables[position];
        if def.without_rowid { None } else { Some(def) }
    }

    /// 按候选 rowid 读取行,保持升序。
    ///
    /// 候选稠密时用一次 `[first, last]` 范围读后按集合过滤,避免对每个 rowid
    /// 都重走一次表元数据与 b-tree 定位。候选含重复或跨度过大时退回逐点读,
    /// 两条路径都按 rowid 升序返回,与全表扫描顺序一致。
    fn rows_for_rowids(&self, table: &str, rowids: &[i64]) -> Result<Vec<TableRow>, SqlError> {
        let ascending = rowids.windows(2).all(|pair| pair[0] < pair[1]);
        let dense = rowids.len() >= 2
            && ascending
            && rowids.last().and_then(|last| last.checked_sub(rowids[0])).is_some_and(|span| span <= (rowids.len() as i64).saturating_mul(4).saturating_add(16));
        if dense {
            let stored = self
                .writer
                .table_range(table, rowids[0], rowids[rowids.len() - 1], TableLimits::default())
                .map_err(|error| storage_failure(error).in_statement("select"))?;
            let wanted: HashSet<i64> = rowids.iter().copied().collect();
            return Ok(stored.into_iter().filter(|row| wanted.contains(&row.rowid)).collect());
        }
        let mut stored = Vec::with_capacity(rowids.len());
        for rowid in rowids {
            stored.extend(
                self.writer
                    .table_range(table, *rowid, *rowid, TableLimits::default())
                    .map_err(|error| storage_failure(error).in_statement("select"))?,
            );
        }
        Ok(stored)
    }

    /// Runs a nested `SELECT` statement through the same entry point as a
    /// top-level one, so its own `WITH` entries work as well.
    /// Resolves one `FROM` entry against the schema it names, if any. An
    /// unqualified name follows SQLite's order: temp, main, then attached
    /// databases; `main.` and `temp.` pin the search to one schema.
    fn select_source<'b>(
        &'b self,
        table: &TableRef,
        budget: &Budget,
        ctes: Option<&CteEnv>,
        scan: Option<&ScanHint>,
        keep: Option<&[bool]>,
    ) -> Result<(SourceColumns<'b>, Rc<Vec<Vec<Cell>>>), SqlError> {
        if let Some(query) = &table.query {
            let result = self.select_statement(query, &[], budget, ctes)?;
            let names = unique_output_names(&result.columns);
            return Ok((SourceColumns::Materialized { names, rowid: false }, Rc::new(result.rows)));
        }
        let scope = match table.schema.as_deref() {
            None => Some(TableScope::Default),
            Some(qualifier) => {
                let lower = qualifier.to_ascii_lowercase();
                if lower == "main" {
                    Some(TableScope::Main)
                }
                else if lower == "temp" {
                    Some(TableScope::Temp)
                }
                else if self.alias.as_deref().is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier)) {
                    Some(TableScope::Default)
                }
                else {
                    None
                }
            }
        };
        let Some(scope) = scope
        else {
            let qualifier = table.schema.as_deref().unwrap_or_default();
            let index = match self.attached_lookup.get(&qualifier.to_ascii_lowercase()).copied() {
                Some(index) => index,
                None => {
                    return Err(SqlError::new("unknown-table", format!("no such table: {qualifier}.{}", table.name)).in_statement("select"));
                }
            };
            return self.attached_source(index, qualifier, table, budget);
        };
        if table.schema.is_none() {
            if let Some(materialized) = ctes.and_then(|env| env.lookup(&table.name)) {
                return Ok((
                    SourceColumns::Materialized { names: materialized.columns.clone(), rowid: false },
                    Rc::new(materialized.rows.clone()),
                ));
            }
        }
        if let Some(system) = system_for_scope(scope, &table.name) {
            let (columns, rows, rowid) = self.system_rows(system, &table.name)?;
            return Ok((SourceColumns::Materialized { names: columns, rowid }, Rc::new(rows)));
        }
        let key = table.name.to_ascii_lowercase();
        if scope != TableScope::Main {
            if let Some(view) = self.temp_views.get(&key) {
                let _depth = ViewDepthGuard::enter()?;
                let (names, rows) = self.expand_view(&view.def, budget)?;
                return Ok((SourceColumns::Materialized { names, rowid: false }, Rc::new(rows)));
            }
        }
        if scope != TableScope::Temp {
            if let Some(view) = self.views.get(&key) {
                // A view re-runs its stored query against the current contents
                // of every base table, exactly like SQLite. Views are not
                // correlated, so they see no outer row or CTE from this query.
                let _depth = ViewDepthGuard::enter()?;
                let (names, rows) = self.expand_view(view, budget)?;
                return Ok((SourceColumns::Materialized { names, rowid: false }, Rc::new(rows)));
            }
        }
        if scope != TableScope::Main {
            if let Some(position) = self.temp_lookup.get(&key).copied() {
                let def = &self.temp_tables[position].def;
                let decoded = self.temp_tables[position]
                    .rows
                    .iter()
                    .map(|(rowid, cells)| {
                        let mut cells = cells.clone();
                        cells.push(Cell::Integer(*rowid));
                        cells
                    })
                    .collect::<Vec<Vec<Cell>>>();
                return Ok((SourceColumns::Table(def), Rc::new(decoded)));
            }
        }
        let position = if scope == TableScope::Temp { None } else { self.table_lookup.get(&key).copied() };
        if let Some(position) = position {
            let def = &self.tables[position];
            if def.without_rowid {
                let stored = self
                    .writer
                    .without_rowid_rows(&def.name, TableLimits::default())
                    .map_err(|error| storage_failure(error).in_statement("select"))?;
                let decoded = stored.iter().map(|payload| def.decode_without_rowid(payload)).collect::<Result<Vec<Vec<Cell>>, SqlError>>()?;
                return Ok((SourceColumns::Table(def), Rc::new(decoded)));
            }
            // 无扫描提示的普通全表扫描在本语句内只解码一次。自连接和内外层
            // 同表子查询因此共享同一份行集,结果与逐次解码完全一致。缓存按表名
            // 保存该份行集覆盖的列掩码:新请求的列全被覆盖时直接复用,不重复
            // 解码;新请求更宽时用更宽的那份替换(掩码无关列在行内是 NULL 占位)。
            if scan.is_none() {
                let key = def.name.to_ascii_lowercase();
                {
                    let scans = self.statement_scans.borrow();
                    // 缓存持弱引用:同一行集若已没有使用方存活,就重新解码;仍有
                    // 使用方(自连接、内外层同表子查询)时正常复用。
                    if let Some((cached_mask, cached)) = scans.get(&key)
                        && let Some(cached) = cached.upgrade()
                        && mask_covers(cached_mask.as_deref(), keep)
                    {
                        return Ok((SourceColumns::Table(def), cached));
                    }
                }
                let stored = self
                    .writer
                    .table_rows(&def.name, TableLimits::default())
                    .map_err(|error| storage_failure(error).in_statement("select"))?;
                let decoded = Rc::new(decode_stored_rows(&stored, def, keep)?);
                let wider = {
                    let scans = self.statement_scans.borrow();
                    match scans.get(&key) {
                        None => true,
                        Some((cached_mask, _)) => cached_mask.as_deref() != keep && mask_covers(keep, cached_mask.as_deref()),
                    }
                };
                if wider {
                    self.statement_scans
                        .borrow_mut()
                        .insert(key, (keep.map(<[bool]>::to_vec), Rc::downgrade(&decoded)));
                }
                return Ok((SourceColumns::Table(def), decoded));
            }
            let stored = match scan {
                // A `WHERE <rowid alias> = <integer literal>` on this very base
                // rowid table: bound the scan to the one candidate row instead
                // of decoding the whole table. The full predicate still runs in
                // `cross_product`, so the emitted row set and order are exactly
                // the unpushed path's; a miss yields no rows.
                Some(ScanHint::Pin(rowid)) => self
                    .writer
                    .table_range(&def.name, *rowid, *rowid, TableLimits::default())
                    .map_err(|error| storage_failure(error).in_statement("select"))?,
                // A `WHERE <indexed column> = <literal>` covering every key
                // column of a plain index: fetch just the probe's rowids. They
                // arrive ascending, which is both the index order under one
                // full-key equality and the order of the full scan, so the
                // emitted rows keep their position.
                Some(ScanHint::Rowids(rowids)) => self.rows_for_rowids(&def.name, rowids)?,
                // `ORDER BY <rowid order> LIMIT n`: read only the leading rows;
                // the scan order already satisfies the sort, and the generic
                // sort downstream just confirms it over this small prefix.
                Some(ScanHint::Head(max_rows)) => self
                    .writer
                    .table_head(&def.name, *max_rows, TableLimits::default())
                    .map_err(|error| storage_failure(error).in_statement("select"))?,
                None => unreachable!("plain table scans are cached above"),
            };
            return Ok((SourceColumns::Table(def), Rc::new(decode_stored_rows(&stored, def, keep)?)));
        }
        if table.schema.is_none() {
            // The attached databases come after temp and main in the search
            // order, and the first one that defines the name wins.
            for index in 0..self.attached.len() {
                if let Ok(source) = self.attached_source(index, &self.attached[index].name, table, budget) {
                    return Ok(source);
                }
            }
        }
        let message = match table.schema.as_deref() {
            None => format!("unknown table `{}`", table.name),
            Some(qualifier) => format!("no such table: {qualifier}.{}", table.name),
        };
        Err(SqlError::new("unknown-table", message).in_statement("select"))
    }

    /// Reads one table, view or system table of an attached database.
    fn attached_source<'b>(
        &'b self,
        index: usize,
        qualifier: &str,
        table: &TableRef,
        budget: &Budget,
    ) -> Result<(SourceColumns<'b>, Rc<Vec<Vec<Cell>>>), SqlError> {
        let engine = &self.attached[index].engine;
        if let Some(system) = SystemTable::from_name(&table.name) {
            let (columns, rows, rowid) = engine.system_rows(system, &table.name)?;
            return Ok((SourceColumns::Materialized { names: columns, rowid }, Rc::new(rows)));
        }
        let key = table.name.to_ascii_lowercase();
        if let Some(view) = engine.temp_views.get(&key) {
            let _depth = ViewDepthGuard::enter()?;
            let (names, rows) = engine.expand_view(&view.def, budget)?;
            return Ok((SourceColumns::Materialized { names, rowid: false }, Rc::new(rows)));
        }
        if let Some(view) = engine.views.get(&key) {
            let _depth = ViewDepthGuard::enter()?;
            let (names, rows) = engine.expand_view(view, budget)?;
            return Ok((SourceColumns::Materialized { names, rowid: false }, Rc::new(rows)));
        }
        if let Some(position) = engine.table_lookup.get(&key).copied() {
            let def = &engine.tables[position];
            let stored = engine
                .writer
                .table_rows(&def.name, TableLimits::default())
                .map_err(|error| storage_failure(error).in_statement("select"))?;
            return Ok((SourceColumns::Table(def), Rc::new(decode_stored_rows(&stored, def, None)?)));
        }
        Err(SqlError::new("unknown-table", format!("no such table: {qualifier}.{}", table.name)).in_statement("select"))
    }

    fn select_statement(&self, statement: &Statement, outer: &[Scope], budget: &Budget, ctes: Option<&CteEnv>) -> Result<ExecResult, SqlError> {
        match statement {
            Statement::Select { with, recursive, first, compounds, order, limit } => {
                self.run_select(with, *recursive, first, compounds, order.clone(), limit.clone(), outer, budget, ctes)
            }
            other => Err(SqlError::new("unsupported-statement", format!("a subquery must be a `SELECT` but found `{}`", other.name()))
                .in_statement("select")),
        }
    }

    /// 预编译期语句元数据:从目录推导结果列标签,绝不执行语句。
    ///
    /// `Ok(Some(标签))` 表示目录足以确定行集表头,标签与成功执行后的
    /// `ExecResult.columns` 逐字节一致(两边共用 [`core_output_labels`] 与
    /// 同一套 `Relation` 标签方法)。`Ok(None)` 表示语句没有行集结果:
    /// 无 `RETURNING` 的 DML、DDL、事务控制、`ATTACH`/`DETACH` 与写形
    /// `PRAGMA`。`Err` 表示目录解析失败(缺表、缺列、歧义列、视图递归超限
    /// 等),与执行期同一阶段的名字检查一起失败;连接层把它折叠成 `None`,
    /// 错误留待真正执行时按既有语义抛出,prepare 不提前拒绝。
    /// schema 限定名(`main.`、`temp.`、附加库、连接别名)按执行路径
    /// [`Engine::run_statement`] 的同一顺序分派,但只走目录,不写镜像。
    pub fn describe(&self, statement: &Statement) -> Result<Option<Vec<String>>, SqlError> {
        let mut statement = statement.clone();
        let scope = match leading_schema(&statement).map(str::to_owned) {
            None => {
                if let Some(index) = self.attached_target(&statement) {
                    return self.attached[index].engine.describe(&statement);
                }
                TableScope::Default
            }
            Some(schema) => {
                let lower = schema.to_ascii_lowercase();
                if self.alias.as_deref().is_some_and(|alias| alias.eq_ignore_ascii_case(&schema)) {
                    clear_statement_schema(&mut statement, &schema);
                    TableScope::Default
                }
                else if lower == "main" {
                    clear_statement_schema(&mut statement, &schema);
                    TableScope::Main
                }
                else if lower == "temp" {
                    clear_statement_schema(&mut statement, &schema);
                    TableScope::Temp
                }
                else if let Some(index) = self.attached_lookup.get(&lower).copied() {
                    clear_statement_schema(&mut statement, &schema);
                    strip_statement_sql(&mut statement, &schema);
                    return self.attached[index].engine.describe(&statement);
                }
                else {
                    return Err(unknown_schema_error(&statement, &schema));
                }
            }
        };
        self.describe_in(&statement, scope)
    }

    /// 在已解析出的 schema 作用域内推导一条语句的结果列标签。
    fn describe_in(&self, statement: &Statement, scope: TableScope) -> Result<Option<Vec<String>>, SqlError> {
        match statement {
            Statement::Select { .. } => Ok(Some(self.describe_select_statement(statement, None)?)),
            Statement::Explain { query_plan, statement } => {
                self.explain_check_tables(statement, &BTreeSet::new())?;
                Ok(Some(if *query_plan {
                    vec!["id".to_string(), "parent".to_string(), "notused".to_string(), "detail".to_string()]
                }
                else {
                    vec![
                        "addr".to_string(),
                        "opcode".to_string(),
                        "p1".to_string(),
                        "p2".to_string(),
                        "p3".to_string(),
                        "p4".to_string(),
                        "p5".to_string(),
                        "comment".to_string(),
                    ]
                }))
            }
            Statement::Pragma { schema, name, value } => self.describe_pragma(schema.as_deref(), name, value.as_deref()),
            Statement::Insert { schema: _, table, query, returning, .. } => {
                check_readonly_table(table, "insert")?;
                let key = table.to_ascii_lowercase();
                if let Some(query) = query {
                    self.describe_select_statement(query, None)?;
                }
                if self.views.contains_key(&key) {
                    return Ok(None);
                }
                let def = self.describe_target_def(table, &key, scope, "insert")?;
                match returning {
                    Some(items) => Ok(Some(TargetShape::new(&def, table, None).plan(&def, items, "insert")?.columns)),
                    None => Ok(None),
                }
            }
            Statement::Update { schema: _, table, alias, assignments, from, filter, returning } => {
                check_readonly_table(table, "update")?;
                let key = table.to_ascii_lowercase();
                if self.views.contains_key(&key) {
                    if !from.is_empty() {
                        let mut error =
                            SqlError::new("unsupported-clause", "`UPDATE ... FROM` is not supported on a view; name a base table instead");
                        error.clause = Some("FROM".to_string());
                        return Err(error.in_statement("update"));
                    }
                    if returning.is_some() {
                        return Err(SqlError::new("unsupported-clause", "RETURNING on an INSTEAD OF view update is not supported yet")
                            .in_statement("update"));
                    }
                    return Ok(None);
                }
                let def = self.describe_target_def(table, &key, scope, "update")?;
                let shape = self.describe_update_shape(&def, table, alias.as_deref(), from)?;
                for assignment in assignments {
                    if !def.columns.iter().any(|column| column.name.eq_ignore_ascii_case(&assignment.column)) {
                        return Err(SqlError::new("unknown-column", format!("unknown column `{}` in table `{table}`", assignment.column))
                            .in_statement("update"));
                    }
                    shape.resolve(&assignment.value, "update")?;
                }
                shape.resolve_optional(filter.as_ref(), "update")?;
                match returning {
                    Some(items) => Ok(Some(TargetShape::new(&def, table, None).plan(&def, items, "update")?.columns)),
                    None => Ok(None),
                }
            }
            Statement::Delete { schema: _, table, alias, filter, returning } => {
                check_readonly_table(table, "delete")?;
                let key = table.to_ascii_lowercase();
                if self.views.contains_key(&key) {
                    if returning.is_some() {
                        return Err(SqlError::new("unsupported-clause", "RETURNING on an INSTEAD OF view delete is not supported yet")
                            .in_statement("delete"));
                    }
                    return Ok(None);
                }
                let def = self.describe_target_def(table, &key, scope, "delete")?;
                let shape = TargetShape::new(&def, alias.as_deref().unwrap_or(table), None);
                shape.resolve_optional(filter.as_ref(), "delete")?;
                match returning {
                    Some(items) => Ok(Some(TargetShape::new(&def, table, None).plan(&def, items, "delete")?.columns)),
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// 定位一条 DML 的目标表定义,镜像执行路径的查找顺序:temp 先于 main,
    /// `main.`/`temp.` 作用域只查各自一侧。返回克隆的定义,让调用方在只读
    /// 借用目录的同时安全持有它。
    fn describe_target_def(&self, table: &str, key: &str, scope: TableScope, statement: &'static str) -> Result<TableDef, SqlError> {
        let temp_position = if scope == TableScope::Main { None } else { self.temp_lookup.get(key).copied() };
        let main_position = if scope == TableScope::Temp {
            None
        }
        else {
            match temp_position {
                Some(_) => None,
                None => Some(
                    *self
                        .table_lookup
                        .get(key)
                        .ok_or_else(|| SqlError::new("unknown-table", format!("unknown table `{table}`")).in_statement(statement))?,
                ),
            }
        };
        Ok(match temp_position {
            Some(position) => self.temp_tables[position].def.clone(),
            None => self.tables[main_position.expect("main table resolved")].clone(),
        })
    }

    /// 构造 `UPDATE ... FROM` 解析赋值与谓词所用的 `FROM` 形状,镜像执行期
    /// 的关系;目标表以目录列名进入,派生 `FROM` 源只推导名字。
    fn describe_update_shape<'a>(
        &self,
        def: &'a TableDef,
        table: &str,
        alias: Option<&str>,
        from: &[TableRef],
    ) -> Result<TargetShape<'a>, SqlError> {
        if from.is_empty() {
            return Ok(TargetShape::new(def, alias.unwrap_or(table), None));
        }
        let width = def.columns.len();
        let mut sources = vec![Source {
            label: alias.unwrap_or(table).to_string(),
            columns: SourceColumns::Materialized {
                names: def.columns.iter().map(|column| column.name.clone()).collect(),
                rowid: !def.without_rowid,
            },
            offset: 0,
            join: None,
            star_hidden: vec![false; width],
            unqualified_hidden: vec![false; width],
            star_override: vec![None; width],
        }];
        let mut offset = width + usize::from(!def.without_rowid);
        for entry in from {
            let (names, rowid) = self.describe_source(entry, None)?;
            let visible = names.len();
            sources.push(Source {
                label: entry.label().to_string(),
                columns: SourceColumns::Materialized { names, rowid },
                offset,
                join: entry.join,
                star_hidden: vec![false; visible],
                unqualified_hidden: vec![false; visible],
                star_override: vec![None; visible],
            });
            offset += visible + usize::from(rowid);
        }
        let source_count = sources.len();
        let relation = Relation { sources, using: vec![Vec::new(); source_count], using_slots: Vec::new(), trigger: None, excluded: false };
        let scope = relation.shape();
        Ok(TargetShape { relation, scope })
    }

    /// 推导一条读形 `PRAGMA` 的静态表头;写形 `PRAGMA` 没有行集结果,如实
    /// 返回 `None`。`table_info`/`table_xinfo` 指向视图时镜像执行期的
    /// `view_output` 校验:视图体推导失败(缺列、宽度不符)即整体失败。
    fn describe_pragma(&self, schema: Option<&str>, name: &str, value: Option<&str>) -> Result<Option<Vec<String>>, SqlError> {
        if name.eq_ignore_ascii_case("database_list") {
            return Ok(Some(vec!["seq".to_string(), "name".to_string(), "file".to_string()]));
        }
        let argument = value.unwrap_or_default();
        match name.to_ascii_lowercase().as_str() {
            "table_info" | "table_xinfo" => {
                let extended = name.eq_ignore_ascii_case("table_xinfo");
                if pragma_table_def(self, schema, argument).is_none() {
                    if let Some(view) = pragma_view_def(self, schema, argument) {
                        let _depth = ViewDepthGuard::enter()?;
                        let actual = self.describe_select_statement(&view.query, None)?;
                        view_output_names(view, &actual)?;
                    }
                }
                let mut columns =
                    vec!["cid".to_string(), "name".to_string(), "type".to_string(), "notnull".to_string(), "dflt_value".to_string(), "pk".to_string()];
                if extended {
                    columns.push("hidden".to_string());
                }
                Ok(Some(columns))
            }
            "index_list" => Ok(Some(vec!["seq".to_string(), "name".to_string(), "unique".to_string(), "origin".to_string(), "partial".to_string()])),
            "index_info" => Ok(Some(vec!["seqno".to_string(), "cid".to_string(), "name".to_string()])),
            "foreign_key_list" => Ok(Some(vec![
                "id".to_string(),
                "seq".to_string(),
                "table".to_string(),
                "from".to_string(),
                "to".to_string(),
                "on_update".to_string(),
                "on_delete".to_string(),
                "match".to_string(),
            ])),
            "integrity_check" => Ok(Some(vec!["integrity_check".to_string()])),
            "user_version" | "application_id" | "foreign_keys" | "recursive_triggers" => {
                Ok(if argument.is_empty() { Some(vec![name.to_string()]) } else { None })
            }
            _ => Err(SqlError::new(
                "unsupported-pragma",
                format!(
                    "unsupported pragma `{name}`; this engine supports `foreign_keys`, `recursive_triggers`, `table_info`, `table_xinfo`, `index_list`, `index_info`, `foreign_key_list`, `database_list`, `user_version`, `application_id` and `integrity_check`"
                ),
            )
            .in_clause("PRAGMA")
            .in_statement("pragma")),
        }
    }

    /// 推导一条已解析 `SELECT`(含其 `WITH` 前言)的结果列标签。
    /// `env` 是外层已物化的 CTE 名字表,与执行期的 `CteEnv` 平行:派生表
    /// 能看见外层 CTE,视图体看不见(镜像执行路径的传参)。
    fn describe_select_statement(&self, statement: &Statement, env: Option<&DescribeEnv>) -> Result<Vec<String>, SqlError> {
        let Statement::Select { with, first, compounds, order, limit, .. } = statement
        else {
            return Err(SqlError::new("unsupported-statement", format!("a subquery must be a `SELECT` but found `{}`", statement.name()))
                .in_statement("select"));
        };
        if with.is_empty() {
            return self.describe_select(first, compounds, order, limit.as_ref(), env);
        }
        let mut owned = DescribeEnv { parent: env, tables: BTreeMap::new() };
        for entry in with {
            let columns = match self.recursive_start(entry)? {
                // 递归项:镜像 `materialize_recursive` 的种子推导——种子列
                // 即 CTE 列,且不做去重改名(执行路径同样没有)。
                Some(start) => {
                    let Statement::Select { with: inner_with, first, compounds, .. } = entry.query.as_ref()
                    else {
                        return Err(SqlError::new("unsupported-statement", "a `WITH` body must be a `SELECT`").in_statement("select"));
                    };
                    let seed = Statement::Select {
                        with: inner_with.clone(),
                        recursive: false,
                        first: first.clone(),
                        compounds: compounds[..start].to_vec(),
                        order: Vec::new(),
                        limit: None,
                    };
                    let seed_columns = self.describe_select_statement(&seed, Some(&owned))?;
                    if let Some(names) = &entry.columns {
                        if names.len() != seed_columns.len() {
                            let mut error = SqlError::new(
                                "arity-mismatch",
                                format!("table {} has {} values for {} columns", entry.name, seed_columns.len(), names.len()),
                            )
                            .in_statement("select");
                            error.clause = Some("WITH".to_string());
                            return Err(error);
                        }
                    }
                    entry.columns.clone().unwrap_or(seed_columns)
                }
                None => {
                    let result_columns = self.describe_select_statement(&entry.query, Some(&owned))?;
                    if let Some(names) = &entry.columns {
                        if names.len() != result_columns.len() {
                            let mut error = SqlError::new(
                                "arity-mismatch",
                                format!("table {} has {} values for {} columns", entry.name, result_columns.len(), names.len()),
                            )
                            .in_statement("select");
                            error.clause = Some("WITH".to_string());
                            return Err(error);
                        }
                    }
                    unique_output_names(&entry.columns.clone().unwrap_or(result_columns))
                }
            };
            owned.tables.insert(entry.name.to_ascii_lowercase(), columns);
        }
        self.describe_select(first, compounds, order, limit.as_ref(), Some(&owned))
    }

    /// 推导一个 `SELECT` 语句体(可能复合)的结果列标签。`LIMIT` 表达式按
    /// 执行路径 [`Engine::eval_limit`] 的同一位置、同一关系求名字:只解析
    /// 不求值,字面量才做整数转换检查,避免预编译期执行任何子查询。
    fn describe_select(
        &self,
        first: &SelectCore,
        compounds: &[CompoundTerm],
        order: &[OrderKey],
        limit: Option<&Limit>,
        env: Option<&DescribeEnv>,
    ) -> Result<Vec<String>, SqlError> {
        if let Some(limit) = limit {
            let relation = Relation { sources: Vec::new(), using: Vec::new(), using_slots: Vec::new(), trigger: None, excluded: false };
            let mut aggregates = Vec::new();
            let mut windows = Vec::new();
            if let Some(offset) = &limit.offset {
                let resolved = resolve_expr(offset, &relation, &[], &[], &mut windows, &mut aggregates, false, false)?;
                if let ResolvedExpr::Cell(cell) = &resolved {
                    limit_integer(cell, "OFFSET")?;
                }
            }
            let resolved = resolve_expr(&limit.count, &relation, &[], &[], &mut windows, &mut aggregates, false, false)?;
            if let ResolvedExpr::Cell(cell) = &resolved {
                limit_integer(cell, "LIMIT")?;
            }
        }
        if compounds.is_empty() {
            return self.describe_core(first, order, env);
        }
        let columns = self.describe_core(first, &[], env)?;
        for term in compounds {
            let right = self.describe_core(&term.core, &[], env)?;
            if right.len() != columns.len() {
                let mut error = SqlError::new(
                    "arity-mismatch",
                    format!("SELECTs to the left and right of {} do not have the same number of result columns", term.op.text()),
                )
                .in_statement("select");
                error.clause = Some(term.op.clause().to_string());
                error.position = Some(term.position);
                return Err(error);
            }
        }
        if !order.is_empty() {
            compound_order_keys(order, &columns)?;
        }
        Ok(columns)
    }

    /// 推导一个 `SELECT` 核心的结果列标签。
    ///
    /// 与执行路径 [`Engine::select`] 同一顺序:先按目录构造每个 `FROM` 源
    /// 的名字表,再规划 `USING`/`NATURAL` 合并,然后解析 `ON`、投影、
    /// `WHERE`、`GROUP BY`、`HAVING`、`ORDER BY`——任何一处名字解析失败即
    /// 整体失败(执行路径同样在读行之前全部解析,坏名字 fail-closed)。
    /// 标签由 [`core_output_labels`] 产出,与执行期共用。派生表、视图与
    /// CTE 只递归推导名字,不物化任何行。
    fn describe_core(&self, core: &SelectCore, order: &[OrderKey], env: Option<&DescribeEnv>) -> Result<Vec<String>, SqlError> {
        let SelectCore { distinct: _, items, from, filter, group, having, windows: named } = core;
        let mut sources = Vec::with_capacity(from.len());
        let mut offset = 0;
        for table in from {
            let (names, rowid) = self.describe_source(table, env)?;
            let width = names.len();
            sources.push(Source {
                label: table.label().to_string(),
                columns: SourceColumns::Materialized { names, rowid },
                offset,
                join: table.join,
                star_hidden: vec![false; width],
                unqualified_hidden: vec![false; width],
                star_override: vec![None; width],
            });
            offset += width + usize::from(rowid);
        }
        let mut relation = Relation { sources, using: vec![Vec::new(); from.len()], using_slots: Vec::new(), trigger: None, excluded: false };
        relation.plan_joins(from)?;
        for (index, table) in from.iter().enumerate() {
            if let Some(on) = &table.on {
                let mut scratch_windows = Vec::new();
                let mut scratch_aggregates = Vec::new();
                let resolved = resolve_expr(on, &relation, &[], &[], &mut scratch_windows, &mut scratch_aggregates, false, false)?;
                let mut refs = Vec::new();
                resolved.collect_sources(&mut refs);
                if refs.iter().any(|source| *source > index) {
                    return Err(SqlError::new(
                        "unknown-column",
                        format!("the `ON` clause of source {} may not reference a later source", index + 1),
                    )
                    .in_statement("select"));
                }
            }
        }
        let mut specs = Vec::new();
        let mut windows: Vec<ResolvedWindow> = Vec::new();
        if let SelectItems::Named(exprs) = items {
            for expr in exprs {
                resolve_expr(expr, &relation, &[], named, &mut windows, &mut specs, true, true)?;
            }
        }
        let labels = core_output_labels(&relation, items);
        if let Some(predicate) = filter {
            let mut scratch_windows = Vec::new();
            let mut scratch_aggregates = Vec::new();
            resolve_expr(predicate, &relation, &[], &[], &mut scratch_windows, &mut scratch_aggregates, false, false)?;
        }
        for expr in group {
            if let Expr::Literal(Literal::Integer(position)) = expr {
                let position = *position;
                if position < 1 || position as usize > labels.len() {
                    return Err(SqlError::new(
                        "order-term-out-of-range",
                        format!("GROUP BY term out of range - should be between 1 and {}", labels.len()),
                    )
                    .in_statement("select"));
                }
                continue;
            }
            let mut scratch_windows = Vec::new();
            let mut scratch_aggregates = Vec::new();
            resolve_expr(expr, &relation, &[], &[], &mut scratch_windows, &mut scratch_aggregates, false, false)?;
        }
        if let Some(having_expr) = having {
            resolve_expr(having_expr, &relation, &[], named, &mut windows, &mut specs, true, false)?;
        }
        if having.is_some() && group.is_empty() && specs.is_empty() {
            let mut error = SqlError::new("misuse-of-aggregate", "HAVING clause on a non-aggregate query").in_statement("select");
            error.clause = Some("HAVING".to_string());
            return Err(error);
        }
        for entry in order {
            if let Expr::Literal(Literal::Integer(position)) = &entry.expr {
                let position = *position;
                if position < 1 || position as usize > labels.len() {
                    return Err(SqlError::new(
                        "order-term-out-of-range",
                        format!("ORDER BY term out of range - should be between 1 and {}", labels.len()),
                    )
                    .in_statement("select"));
                }
                continue;
            }
            if let Expr::Column(column) = &entry.expr {
                if column.qualifier.is_none() && labels.iter().any(|label| label.eq_ignore_ascii_case(&column.name)) {
                    continue;
                }
            }
            resolve_expr(&entry.expr, &relation, &[], named, &mut windows, &mut specs, true, true)?;
        }
        Ok(labels)
    }

    /// 推导一个 `FROM` 源暴露的列名与隐藏 rowid 标志,镜像执行路径
    /// [`Engine::select_source`] 的解析顺序:派生表 → schema 限定 → CTE →
    /// 系统表 → temp 视图 → 视图 → temp 表 → main 表 → 附加库回退。
    /// 每一步只取名字:派生表与视图递归 [`Self::describe_select_statement`],
    /// 基础表直接读目录定义,系统表用静态表头。
    fn describe_source(&self, table: &TableRef, env: Option<&DescribeEnv>) -> Result<(Vec<String>, bool), SqlError> {
        if let Some(query) = &table.query {
            let columns = self.describe_select_statement(query, env)?;
            return Ok((unique_output_names(&columns), false));
        }
        let scope = match table.schema.as_deref() {
            None => Some(TableScope::Default),
            Some(qualifier) => {
                let lower = qualifier.to_ascii_lowercase();
                if lower == "main" {
                    Some(TableScope::Main)
                }
                else if lower == "temp" {
                    Some(TableScope::Temp)
                }
                else if self.alias.as_deref().is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier)) {
                    Some(TableScope::Default)
                }
                else {
                    None
                }
            }
        };
        let Some(scope) = scope
        else {
            let qualifier = table.schema.as_deref().unwrap_or_default();
            let index = match self.attached_lookup.get(&qualifier.to_ascii_lowercase()).copied() {
                Some(index) => index,
                None => {
                    return Err(SqlError::new("unknown-table", format!("no such table: {qualifier}.{}", table.name)).in_statement("select"));
                }
            };
            return self.describe_attached_source(index, table);
        };
        if table.schema.is_none() {
            if let Some(names) = env.and_then(|env| env.lookup(&table.name)) {
                return Ok((names.clone(), false));
            }
        }
        if let Some(system) = system_for_scope(scope, &table.name) {
            return self.describe_system_rows(system, &table.name);
        }
        let key = table.name.to_ascii_lowercase();
        if scope != TableScope::Main {
            if let Some(view) = self.temp_views.get(&key) {
                let _depth = ViewDepthGuard::enter()?;
                let actual = self.describe_select_statement(&view.def.query, None)?;
                return Ok((view_output_names(&view.def, &actual)?, false));
            }
        }
        if scope != TableScope::Temp {
            if let Some(view) = self.views.get(&key) {
                let _depth = ViewDepthGuard::enter()?;
                let actual = self.describe_select_statement(&view.query, None)?;
                return Ok((view_output_names(view, &actual)?, false));
            }
        }
        if scope != TableScope::Main {
            if let Some(position) = self.temp_lookup.get(&key).copied() {
                let def = &self.temp_tables[position].def;
                return Ok((def.columns.iter().map(|column| column.name.clone()).collect(), !def.without_rowid));
            }
        }
        if scope != TableScope::Temp {
            if let Some(position) = self.table_lookup.get(&key).copied() {
                let def = &self.tables[position];
                return Ok((def.columns.iter().map(|column| column.name.clone()).collect(), !def.without_rowid));
            }
        }
        if table.schema.is_none() {
            for index in 0..self.attached.len() {
                if let Ok(source) = self.describe_attached_source(index, table) {
                    return Ok(source);
                }
            }
        }
        let message = match table.schema.as_deref() {
            None => format!("unknown table `{}`", table.name),
            Some(qualifier) => format!("no such table: {qualifier}.{}", table.name),
        };
        Err(SqlError::new("unknown-table", message).in_statement("select"))
    }

    /// 推导附加数据库中一个 `FROM` 源的名字表,镜像 [`Engine::attached_source`]。
    fn describe_attached_source(&self, index: usize, table: &TableRef) -> Result<(Vec<String>, bool), SqlError> {
        let engine = &self.attached[index].engine;
        if let Some(system) = SystemTable::from_name(&table.name) {
            return engine.describe_system_rows(system, &table.name);
        }
        let key = table.name.to_ascii_lowercase();
        if let Some(view) = engine.temp_views.get(&key) {
            let _depth = ViewDepthGuard::enter()?;
            let actual = engine.describe_select_statement(&view.def.query, None)?;
            return Ok((view_output_names(&view.def, &actual)?, false));
        }
        if let Some(view) = engine.views.get(&key) {
            let _depth = ViewDepthGuard::enter()?;
            let actual = engine.describe_select_statement(&view.query, None)?;
            return Ok((view_output_names(view, &actual)?, false));
        }
        if let Some(position) = engine.table_lookup.get(&key).copied() {
            let def = &engine.tables[position];
            return Ok((def.columns.iter().map(|column| column.name.clone()).collect(), !def.without_rowid));
        }
        let qualifier = table.schema.as_deref().unwrap_or_default();
        Err(SqlError::new("unknown-table", format!("no such table: {qualifier}.{}", table.name)).in_statement("select"))
    }

    /// 系统表的静态列名与隐藏 rowid 标志,镜像 [`Engine::system_rows`] 的
    /// 名字部分:`sqlite_sequence` 只在目录里真的存在时可用,与执行期一致。
    fn describe_system_rows(&self, system: SystemTable, name: &str) -> Result<(Vec<String>, bool), SqlError> {
        match system {
            SystemTable::Master | SystemTable::TempMaster => {
                Ok((vec!["type".to_string(), "name".to_string(), "tbl_name".to_string(), "rootpage".to_string(), "sql".to_string()], true))
            }
            SystemTable::Sequence => {
                let present = self
                    .writer
                    .schema_objects(TableLimits::default())
                    .map_err(storage_error)?
                    .iter()
                    .any(|object| object.kind == SchemaObjectKind::Table && object.name.eq_ignore_ascii_case(name));
                if !present {
                    return Err(SqlError::new("unknown-table", format!("no such table: {name}")).in_statement("select"));
                }
                Ok((vec!["name".to_string(), "seq".to_string()], true))
            }
        }
    }
}

/// 一个 `WITH` 条目在 describe 路径上的名字表,与执行期的 `CteEnv` 平行但
/// 只携带列名,不携带行。链式父作用域镜像 CTE 作用域的嵌套可见性。
struct DescribeEnv<'a> {
    parent: Option<&'a DescribeEnv<'a>>,
    tables: BTreeMap<String, Vec<String>>,
}

impl<'a> DescribeEnv<'a> {
    fn lookup(&self, name: &str) -> Option<&Vec<String>> {
        self.tables.get(&name.to_ascii_lowercase()).or_else(|| self.parent.and_then(|parent| parent.lookup(name)))
    }
}

/// 一个 `SELECT` 核心的结果列标签。
///
/// 执行路径与 [`Engine::describe`] 共用本函数,保证预编译期推导的表头与
/// 执行后 `ExecResult.columns` 逐字节一致:裸列引用按目录拼写命名(所以
/// `SELECT A FROM t` 报 `a`),别名取书写名,表达式列由 `Expr::label` 重建
/// 文本,`*` 按 `FROM` 源的可见列展开(含 `USING` 合并列的隐藏规则)。
fn core_output_labels(relation: &Relation<'_>, items: &SelectItems) -> Vec<String> {
    match items {
        SelectItems::Named(exprs) => exprs
            .iter()
            .map(|expr| match expr {
                Expr::Column(reference) => relation.resolved_column_name(reference).unwrap_or_else(|| expr.label()),
                _ => expr.label(),
            })
            .collect(),
        SelectItems::CountAll => vec!["count(*)".to_string()],
        SelectItems::Star => relation.star_columns(),
    }
}

/// Resolves the trailing `ORDER BY` of a compound result.
///
/// SQLite only lets a compound query order by an output column, either by
/// 1-based ordinal or by its result label; source columns are out of scope
/// once the cores have been combined.
fn compound_order_keys(order: &[OrderKey], columns: &[String]) -> Result<Vec<(usize, SortDirection)>, SqlError> {
    order
        .iter()
        .map(|entry| {
            if let Expr::Literal(Literal::Integer(position)) = &entry.expr {
                let position = *position;
                if position < 1 || position as usize > columns.len() {
                    return Err(SqlError::new(
                        "order-term-out-of-range",
                        format!("ORDER BY term out of range - should be between 1 and {}", columns.len()),
                    )
                    .in_statement("select"));
                }
                return Ok((position as usize - 1, SortDirection::of(entry)));
            }
            if let Expr::Column(reference) = &entry.expr {
                if reference.qualifier.is_none() {
                    let matching = columns
                        .iter()
                        .enumerate()
                        .filter(|(_, column)| column.eq_ignore_ascii_case(&reference.name))
                        .map(|(index, _)| index)
                        .collect::<Vec<_>>();
                    return match matching.as_slice() {
                        [index] => Ok((*index, SortDirection::of(entry))),
                        [] => Err(SqlError::new(
                            "unknown-column",
                            format!("ORDER BY term `{}` does not match any column in the result set", reference.name),
                        )
                        .in_statement("select")),
                        _ => {
                            Err(SqlError::new("ambiguous-column", format!("ORDER BY term `{}` is ambiguous in the result set", reference.name))
                                .in_statement("select"))
                        }
                    };
                }
            }
            let mut error = SqlError::new("unsupported-clause", "a compound `SELECT` can only order by an output column name or ordinal")
                .in_statement("select");
            error.clause = Some("ORDER BY".to_string());
            Err(error)
        })
        .collect()
}

/// Upper bound on rows emitted by one statement, shared with every nested
/// subquery. Counting emissions rather than wall-clock keeps the result
/// deterministic across machines.
const EXECUTION_BUDGET: usize = 1_000_000;

/// Row budget for one statement.
struct Budget {
    remaining: std::cell::Cell<usize>,
}

impl Budget {
    fn new() -> Self {
        Self { remaining: std::cell::Cell::new(EXECUTION_BUDGET) }
    }

    fn charge(&self, rows: usize) -> Result<(), SqlError> {
        let remaining = self.remaining.get();
        if remaining < rows {
            return Err(SqlError::new(
                "execution-limit",
                format!("statement exceeded the execution budget of {EXECUTION_BUDGET} rows, most likely from a correlated subquery over a large product"),
            )
            .in_statement("select"));
        }
        self.remaining.set(remaining - rows);
        Ok(())
    }
}

/// One CTE or derived table already reduced to rows.
struct Materialized {
    columns: Vec<String>,
    rows: Vec<Vec<Cell>>,
}

/// `WITH` entries in scope, chained so a nested query keeps seeing the outer
/// ones. A CTE shadows a base table with the same name.
struct CteEnv<'a> {
    parent: Option<&'a CteEnv<'a>>,
    tables: BTreeMap<String, Materialized>,
}

impl<'a> CteEnv<'a> {
    fn lookup(&self, name: &str) -> Option<&Materialized> {
        self.tables.get(&name.to_ascii_lowercase()).or_else(|| self.parent.and_then(|parent| parent.lookup(name)))
    }
}

/// One column of an enclosing `SELECT`, paired with the value of the row
/// currently being evaluated.
#[derive(Clone)]
struct ScopeColumn {
    label: String,
    name: String,
    value: Cell,
}

/// One enclosing `SELECT` row: every column it exposes and its current value.
#[derive(Clone)]
struct Scope {
    columns: Vec<ScopeColumn>,
}

impl Scope {
    fn shape(&self) -> ScopeShape {
        ScopeShape { columns: self.columns.iter().map(|column| (column.label.clone(), column.name.clone())).collect() }
    }
}

/// Static column catalog of one `SELECT` core.
///
/// An inner expression resolves an outer reference to `(depth, index)` once;
/// evaluation then reads the value from the matching `Scope` of the current
/// outer row.
struct ScopeShape {
    columns: Vec<(String, String)>,
}

impl ScopeShape {
    fn instantiate(&self, values: &[Cell]) -> Scope {
        Scope {
            columns: self
                .columns
                .iter()
                .enumerate()
                .map(|(index, (label, name))| ScopeColumn {
                    label: label.clone(),
                    name: name.clone(),
                    value: values.get(index).cloned().unwrap_or(Cell::Null),
                })
                .collect(),
        }
    }
}

/// Evaluation context shared by every expression of one `SELECT` core.
struct EvalContext<'a> {
    engine: &'a Engine,
    /// Enclosing scopes, innermost first.
    outer: &'a [Scope],
    /// Column catalog of the core currently being evaluated.
    current: &'a ScopeShape,
    /// Whole-result aggregate values for the current row.
    aggregates: &'a [Cell],
    /// Already computed window values, one vector per window of this core.
    windows: &'a [Vec<Cell>],
    /// Row currently being projected, indexing into `windows`.
    window_row: usize,
    budget: &'a Budget,
    /// CTEs visible to the current core, including enclosing `WITH` clauses.
    ctes: Option<&'a CteEnv<'a>>,
}

/// One whole-result aggregate to compute over the filtered rows.
struct AggregateSpec {
    function: AggregateFunction,
    argument: Option<ResolvedExpr>,
    /// `group_concat` separator, evaluated per row like SQLite does.
    separator: Option<ResolvedExpr>,
    /// `DISTINCT` was written before the argument.
    distinct: bool,
    /// `FILTER (WHERE ...)` applied before the aggregate sees a row.
    filter: Option<ResolvedExpr>,
    /// `ORDER BY` inside the argument list, used by `group_concat`.
    order: Vec<(ResolvedExpr, SortDirection)>,
    /// `DISTINCT` 去重时使用的排序规则，取自参数表达式的生效排序规则。
    collation: Collation,
}

/// Two result rows are equal when every cell compares equal under the engine's
/// value rules; `NULL` equals `NULL`, as set operations require.
fn rows_equal(left: &[Cell], right: &[Cell]) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|(first, second)| compare_cells(first, second) == Ordering::Equal)
}

fn rows_contain(rows: &[Vec<Cell>], candidate: &[Cell]) -> bool {
    rows.iter().any(|row| rows_equal(row, candidate))
}

/// Removes duplicate rows while keeping the first occurrence of each value.
fn distinct_rows(rows: &[Vec<Cell>]) -> Vec<Vec<Cell>> {
    retain_first_distinct(rows.to_vec(), &[])
}

/// Applies one set operator with SQLite's multiplicity rules.
fn combine_rows(op: CompoundOp, left: Vec<Vec<Cell>>, right: Vec<Vec<Cell>>) -> Vec<Vec<Cell>> {
    match op {
        CompoundOp::UnionAll => left.into_iter().chain(right).collect(),
        CompoundOp::Union => {
            let mut rows = left;
            rows.extend(right);
            distinct_rows(&rows)
        }
        CompoundOp::Intersect => {
            let right_rows = distinct_rows(&right);
            distinct_rows(&left).into_iter().filter(|row| rows_contain(&right_rows, row)).collect()
        }
        CompoundOp::Except => {
            let right_rows = distinct_rows(&right);
            distinct_rows(&left).into_iter().filter(|row| !rows_contain(&right_rows, row)).collect()
        }
    }
}

/// The error SQLite raises when a recursive `WITH` entry reads itself in a
/// place the fixed-point evaluator cannot handle.
fn circular_reference(name: &str) -> SqlError {
    SqlError::new("SQLITE_ERROR", format!("circular reference: {name}")).in_statement("select")
}

/// Whether a projection turns each input row into at most one output row, so a
/// recursive CTE may be truncated at the enclosing `LIMIT`.
fn row_preserving_items(items: &SelectItems) -> bool {
    match items {
        SelectItems::Star => true,
        SelectItems::CountAll => false,
        SelectItems::Named(exprs) => exprs.iter().all(row_preserving_expr),
    }
}

fn row_preserving_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(_) | Expr::Parameter { .. } => true,
        Expr::Unary { value, .. } => row_preserving_expr(value),
        Expr::Binary { left, right, .. } | Expr::Compare { left, right, .. } | Expr::And(left, right) | Expr::Or(left, right) => {
            row_preserving_expr(left) && row_preserving_expr(right)
        }
        Expr::IsDistinct { left, right, .. } => row_preserving_expr(left) && row_preserving_expr(right),
        Expr::Glob { value, pattern, .. } => row_preserving_expr(value) && row_preserving_expr(pattern),
        Expr::IsNull { value, .. } | Expr::IsTruth { value, .. } | Expr::Not(value) => row_preserving_expr(value),
        Expr::Case { operand, branches, otherwise } => {
            operand.as_ref().map_or(true, |expr| row_preserving_expr(expr))
                && branches.iter().all(|(condition, result)| row_preserving_expr(condition) && row_preserving_expr(result))
                && otherwise.as_ref().map_or(true, |expr| row_preserving_expr(expr))
        }
        Expr::In { value, list, .. } => row_preserving_expr(value) && list.iter().all(row_preserving_expr),
        Expr::Function { args, .. } | Expr::HostFunction { args, .. } => args.iter().all(row_preserving_expr),
        Expr::Alias { value, .. } | Expr::Cast { value, .. } | Expr::Collate { value, .. } => row_preserving_expr(value),
        Expr::Like { value, pattern, escape, .. } => {
            row_preserving_expr(value) && row_preserving_expr(pattern) && escape.as_ref().map_or(true, |expr| row_preserving_expr(expr))
        }
        _ => false,
    }
}

/// One table bound in a `FROM` clause.
struct Source<'a> {
    /// Label used by column references: the alias when written, else the table
    /// name. An unaliased derived table has the empty label.
    label: String,
    /// Column catalog: a base table's definition, or the materialized output
    /// names of a CTE / derived table.
    columns: SourceColumns<'a>,
    /// Index of this source's first column inside the merged row.
    offset: usize,
    /// How this source is joined to the ones before it; `None` means a plain
    /// product (comma or `CROSS JOIN`).
    join: Option<JoinKind>,
    /// Columns `SELECT *` must not expose: the right side of a `USING` /
    /// `NATURAL` join, whose value lives in the coalesced slot instead.
    star_hidden: Vec<bool>,
    /// Columns that cannot be named without a qualifier because a coalesced
    /// `USING` column owns the name.
    unqualified_hidden: Vec<bool>,
    /// For a column that owns a `USING` name, the coalesced slot `SELECT *`
    /// and unqualified references read instead.
    star_override: Vec<Option<usize>>,
}

/// Column catalog of one `FROM` source.
enum SourceColumns<'a> {
    /// Base table still read through the storage layer.
    Table(&'a TableDef),
    /// CTE, derived table or reserved system table whose rows were evaluated
    /// up front. `rowid` marks sources that carry a trailing hidden rowid slot.
    Materialized { names: Vec<String>, rowid: bool },
}

impl<'a> SourceColumns<'a> {
    fn width(&self) -> usize {
        match self {
            Self::Table(def) => def.columns.len(),
            Self::Materialized { names, .. } => names.len(),
        }
    }

    fn name_at(&self, local: usize) -> &str {
        match self {
            Self::Table(def) => &def.columns[local].name,
            Self::Materialized { names, .. } => &names[local],
        }
    }

    /// A base table is a rowid table, so each of its rows carries one extra,
    /// unnamed slot holding the rowid. A WITHOUT ROWID table has none, and
    /// derived tables and CTEs never do, while `sqlite_master` and
    /// `sqlite_sequence` do.
    fn hidden_rowid(&self) -> bool {
        match self {
            Self::Table(def) => !def.without_rowid,
            Self::Materialized { rowid, .. } => *rowid,
        }
    }
}

/// SQLite exposes the implicit integer primary key under three interchangeable
/// names, unless a declared column shadows that name.
fn is_rowid_alias(name: &str) -> bool {
    ["rowid", "_rowid_", "oid"].iter().any(|alias| alias.eq_ignore_ascii_case(name))
}

impl<'a> Source<'a> {
    /// Width of this source inside the merged row, hidden rowid included.
    fn width(&self) -> usize {
        self.columns.width() + usize::from(self.columns.hidden_rowid())
    }

    /// Width of the columns `SELECT *` exposes.
    fn visible_width(&self) -> usize {
        self.columns.width()
    }

    fn index(&self, name: &str) -> Option<usize> {
        if let Some(local) = (0..self.visible_width()).find(|local| self.columns.name_at(*local).eq_ignore_ascii_case(name)) {
            return Some(self.offset + local);
        }
        // A declared column with the same name shadows the implicit rowid.
        if self.columns.hidden_rowid() && is_rowid_alias(name) {
            return Some(self.offset + self.visible_width());
        }
        None
    }

    /// Resolves an unqualified reference, skipping columns a coalesced `USING`
    /// slot owns.
    fn unqualified_index(&self, name: &str) -> Option<usize> {
        if let Some(local) =
            (0..self.visible_width()).find(|local| !self.unqualified_hidden[*local] && self.columns.name_at(*local).eq_ignore_ascii_case(name))
        {
            return Some(self.offset + local);
        }
        if self.columns.hidden_rowid() && is_rowid_alias(name) {
            return Some(self.offset + self.visible_width());
        }
        None
    }
}

/// One `USING` / `NATURAL` column pair of a join.
#[derive(Clone)]
struct UsingJoin {
    /// Merged index of the left operand: the previous coalesced slot for this
    /// name, or a source column.
    left: usize,
    /// Merged index of the right source's column.
    right: usize,
    /// Merged index of the coalesced slot appended after all sources.
    slot: usize,
}

/// One coalesced slot, in slot order. Its merged index is the source width
/// plus the position in this vector.
struct UsingSlot {
    name: String,
    /// Label reported by ambiguity errors and outer references.
    label: String,
    source: usize,
    /// False once a later join extends the same name; the newest slot owns it.
    nameable: bool,
    /// Source column `SELECT *` prints at the owner's position.
    owner: Option<(usize, usize)>,
}

enum UsingOperand {
    Slot(usize),
    Column { source: usize, local: usize },
}

/// The sources of one `FROM` clause, flattened into a single row shape.
struct Relation<'a> {
    sources: Vec<Source<'a>>,
    /// Per source: the coalesced pairs its join introduced.
    using: Vec<Vec<UsingJoin>>,
    /// Coalesced slots, appended after every source column.
    using_slots: Vec<UsingSlot>,
    /// `NEW` / `OLD` frame of the trigger body being resolved, if any. A
    /// `new.` or `old.` reference binds here instead of to a `FROM` source.
    trigger: Option<&'a TriggerFrame>,
    /// `excluded.` references bind to the supplied row of an upsert.
    excluded: bool,
}

impl<'a> Relation<'a> {
    /// Width of the merged row: every source column, in `FROM` order, then the
    /// coalesced `USING` slots.
    fn columns(&self) -> usize {
        self.base_width() + self.using_slots.len()
    }

    /// Width of the merged row before the coalesced slots.
    fn base_width(&self) -> usize {
        self.sources.last().map_or(0, |source| source.offset + source.width())
    }

    /// Resolves one reference to `(source index, merged row index)`.
    fn resolve(&self, reference: &ColumnRef) -> Result<(usize, usize), SqlError> {
        if let Some(qualifier) = &reference.qualifier {
            if qualifier.eq_ignore_ascii_case("new") || qualifier.eq_ignore_ascii_case("old") {
                return self.resolve_trigger(reference, qualifier);
            }
            if qualifier.eq_ignore_ascii_case("excluded") && self.excluded {
                let source = self
                    .sources
                    .first()
                    .ok_or_else(|| SqlError::new("unknown-table", "no table to bind `excluded` against").in_statement("insert"))?;
                let index = source.index(&reference.name).ok_or_else(|| {
                    SqlError::new("unknown-column", format!("no such column: excluded.{}", reference.name)).in_statement("insert")
                })?;
                return Ok((EXCLUDED_SOURCE, index));
            }
            let at = match self.sources.iter().position(|source| source.label.eq_ignore_ascii_case(qualifier)) {
                Some(at) => at,
                // Outside an upsert, `excluded` is not a pseudo table: SQLite
                // reports the qualified name as a missing column.
                None if qualifier.eq_ignore_ascii_case("excluded") => {
                    return Err(SqlError::new("unknown-column", format!("no such column: excluded.{}", reference.name)).in_statement("select"));
                }
                None => {
                    return Err(
                        SqlError::new("unknown-table", format!("unknown table `{qualifier}` in the `FROM` clause")).in_statement("select")
                    );
                }
            };
            let source = &self.sources[at];
            let index = source.index(&reference.name).ok_or_else(|| {
                SqlError::new("unknown-column", format!("unknown column `{}` in table `{}`", reference.display(), source.label))
                    .in_statement("select")
            })?;
            return Ok((at, index));
        }
        // A coalesced `USING` column owns its name; the newest slot wins.
        let mut matches: Vec<(usize, usize, String)> = Vec::new();
        for (position, slot) in self.using_slots.iter().enumerate().rev() {
            if slot.nameable && slot.name.eq_ignore_ascii_case(&reference.name) {
                matches.push((slot.source, self.base_width() + position, slot.label.clone()));
                break;
            }
        }
        for (at, source) in self.sources.iter().enumerate() {
            if let Some(index) = source.unqualified_index(&reference.name) {
                matches.push((at, index, source.label.clone()));
            }
        }
        match matches.len() {
            0 => Err(self.unknown_column(reference)),
            1 => Ok((matches[0].0, matches[0].1)),
            _ => {
                let left = &matches[0].2;
                let right = &matches[1].2;
                Err(SqlError::new(
                    "ambiguous-column",
                    format!("ambiguous column `{}`: it appears in tables `{left}` and `{right}`", reference.name),
                )
                .in_statement("select"))
            }
        }
    }

    /// Name of one resolved reference as the schema spells it, used for
    /// result-column labels. `None` for rowid aliases, `USING` slots and
    /// trigger frames, where the written name is reported instead.
    fn resolved_column_name(&self, reference: &ColumnRef) -> Option<String> {
        let (source, index) = self.resolve(reference).ok()?;
        if source >= self.sources.len() {
            return None;
        }
        let offset = self.sources[source].offset;
        let local = index.checked_sub(offset)?;
        if local < self.sources[source].visible_width() { Some(self.sources[source].columns.name_at(local).to_string()) } else { None }
    }

    /// Resolves an `new.col` / `old.col` reference against the active trigger
    /// frame. A side the event does not provide reports the same
    /// `no such column` error SQLite raises.
    fn resolve_trigger(&self, reference: &ColumnRef, qualifier: &str) -> Result<(usize, usize), SqlError> {
        let old = qualifier.eq_ignore_ascii_case("old");
        if let Some(frame) = self.trigger {
            let available = if old { frame.old.is_some() } else { frame.new.is_some() };
            if available {
                if let Some(index) = frame.columns.iter().position(|column| column.eq_ignore_ascii_case(&reference.name)) {
                    return Ok((if old { TRIGGER_SOURCE_OLD } else { TRIGGER_SOURCE_NEW }, index));
                }
            }
        }
        Err(SqlError::new("unknown-column", format!("no such column: {}.{}", qualifier, reference.name)).in_statement("select"))
    }

    fn unknown_column(&self, reference: &ColumnRef) -> SqlError {
        let scope = match self.sources.len() {
            // SQLite reports `no such column`; a projection without `FROM` has
            // no scope at all, so the message must not name a `FROM` clause.
            0 => "the `SELECT` clause".to_string(),
            1 => {
                let label = &self.sources[0].label;
                if label.is_empty() { "the derived table".to_string() } else { format!("table `{label}`") }
            }
            _ => "the `FROM` clause".to_string(),
        };
        SqlError::new("unknown-column", format!("unknown column `{}` in {scope}", reference.display())).in_statement("select")
    }

    /// Builds the coalesced slots for `USING` / `NATURAL` joins. Runs before
    /// any expression is resolved, so unqualified references bind to them.
    fn plan_joins(&mut self, from: &[TableRef]) -> Result<(), SqlError> {
        for index in 0..self.sources.len() {
            let reference = &from[index];
            let mut names: Vec<String> = Vec::new();
            let raw = if reference.natural { self.natural_columns(index) } else { reference.using.clone().unwrap_or_default() };
            for name in raw {
                if !names.iter().any(|existing| existing.eq_ignore_ascii_case(&name)) {
                    names.push(name);
                }
            }
            for name in names {
                let right_local = (0..self.sources[index].visible_width())
                    .find(|local| self.sources[index].columns.name_at(*local).eq_ignore_ascii_case(&name))
                    .ok_or_else(|| {
                        SqlError::new("unknown-column", format!("cannot join using column {name} - column not present in both tables"))
                            .in_statement("select")
                    })?;
                let left = match self.using_operand(&name, index) {
                    Some(operand) => operand,
                    None => {
                        return Err(SqlError::new(
                            "unknown-column",
                            format!("cannot join using column {name} - column not present in both tables"),
                        )
                        .in_statement("select"));
                    }
                };
                let (left_index, label, owner, source, matched) = match left {
                    UsingOperand::Slot(position) => {
                        let slot = &self.using_slots[position];
                        let (label, owner, source, matched) = (slot.label.clone(), slot.owner, slot.source, slot.name.clone());
                        self.using_slots[position].nameable = false;
                        if let Some((owner_source, owner_local)) = owner {
                            self.sources[owner_source].star_override[owner_local] = Some(self.base_width() + self.using_slots.len());
                        }
                        (self.base_width() + position, label, owner, source, matched)
                    }
                    UsingOperand::Column { source, local } => {
                        self.sources[source].unqualified_hidden[local] = true;
                        self.sources[source].star_override[local] = Some(self.base_width() + self.using_slots.len());
                        let matched = self.sources[source].columns.name_at(local).to_string();
                        (self.sources[source].offset + local, self.sources[source].label.clone(), Some((source, local)), source, matched)
                    }
                };
                let slot = self.base_width() + self.using_slots.len();
                self.sources[index].star_hidden[right_local] = true;
                self.sources[index].unqualified_hidden[right_local] = true;
                self.using[index].push(UsingJoin { left: left_index, right: self.sources[index].offset + right_local, slot });
                self.using_slots.push(UsingSlot { name: matched, label, source, nameable: true, owner });
            }
        }
        Ok(())
    }

    /// Finds the current left operand for one `USING` column name among the
    /// sources joined so far.
    fn using_operand(&self, name: &str, before: usize) -> Option<UsingOperand> {
        if let Some(position) = self.using_slots.iter().rposition(|slot| slot.nameable && slot.name.eq_ignore_ascii_case(name)) {
            return Some(UsingOperand::Slot(position));
        }
        for (source, item) in self.sources[..before].iter().enumerate() {
            for local in 0..item.visible_width() {
                if !item.unqualified_hidden[local] && item.columns.name_at(local).eq_ignore_ascii_case(name) {
                    return Some(UsingOperand::Column { source, local });
                }
            }
        }
        None
    }

    /// Shared column names for `NATURAL JOIN`, in right-source order.
    fn natural_columns(&self, index: usize) -> Vec<String> {
        let right = &self.sources[index];
        let mut names = Vec::new();
        for local in 0..right.visible_width() {
            let name = right.columns.name_at(local);
            if self.using_operand(name, index).is_some() && !names.iter().any(|existing: &String| existing.eq_ignore_ascii_case(name)) {
                names.push(name.to_string());
            }
        }
        names
    }

    /// Column catalog of the merged row: `(label, declared name)` per column.
    fn shape(&self) -> ScopeShape {
        let mut columns = Vec::with_capacity(self.columns());
        for source in &self.sources {
            for local in 0..source.visible_width() {
                if source.star_hidden[local] || source.unqualified_hidden[local] {
                    columns.push((source.label.clone(), String::new()));
                }
                else {
                    columns.push((source.label.clone(), source.columns.name_at(local).to_string()));
                }
            }
            if source.columns.hidden_rowid() {
                // The hidden rowid slot has no name, so no identifier can bind
                // to it; it only has to keep merged-row indexes aligned.
                columns.push((source.label.clone(), String::new()));
            }
        }
        for slot in &self.using_slots {
            if slot.nameable {
                columns.push((slot.label.clone(), slot.name.clone()));
            }
            else {
                columns.push((String::new(), String::new()));
            }
        }
        ScopeShape { columns }
    }

    /// Column labels `SELECT *` exposes.
    fn star_columns(&self) -> Vec<String> {
        let mut names = Vec::new();
        for source in &self.sources {
            for local in 0..source.visible_width() {
                if !source.star_hidden[local] {
                    names.push(source.columns.name_at(local).to_string());
                }
            }
        }
        names
    }

    /// Drops the hidden rowid slots and right-side `USING` columns from one
    /// merged row for `SELECT *`, printing the coalesced value where a join
    /// folded a column into its left neighbour.
    fn visible_row(&self, row: &[Cell]) -> Vec<Cell> {
        let mut visible = Vec::with_capacity(self.star_columns().len());
        for source in &self.sources {
            for local in 0..source.visible_width() {
                if source.star_hidden[local] {
                    continue;
                }
                match source.star_override[local] {
                    Some(slot) => visible.push(row[slot].clone()),
                    None => visible.push(row[source.offset + local].clone()),
                }
            }
        }
        visible
    }

    /// 合并行某一列声明的排序规则。`USING` 合并槽取左侧列的规则，rowid 空槽
    /// 与派生表、CTE 一律按 `BINARY`。
    fn column_collation(&self, merged: usize) -> Collation {
        for source in &self.sources {
            let end = source.offset + source.width();
            if merged >= source.offset && merged < end {
                if merged >= source.offset + source.visible_width() {
                    return Collation::Binary;
                }
                return match &source.columns {
                    SourceColumns::Table(def) => def.columns[merged - source.offset].collate.unwrap_or_default(),
                    SourceColumns::Materialized { .. } => Collation::Binary,
                };
            }
        }
        for join in self.using.iter().flatten() {
            if join.slot == merged {
                return self.column_collation(join.left);
            }
        }
        Collation::Binary
    }

    /// `SELECT *` 输出列各自的排序规则。
    fn visible_collations(&self) -> Vec<Collation> {
        let mut collations = Vec::new();
        for source in &self.sources {
            for local in 0..source.visible_width() {
                if source.star_hidden[local] {
                    continue;
                }
                collations.push(match source.star_override[local] {
                    Some(slot) => self.column_collation(slot),
                    None => self.column_collation(source.offset + local),
                });
            }
        }
        collations
    }
}

/// A projection / `WHERE` / `ORDER BY` expression with every column resolved
/// to a merged row index.
#[derive(Clone, PartialEq)]
enum ResolvedExpr {
    /// Value of a bound parameter placeholder, or `NULL` when the host did not
    /// supply one for that slot.
    Parameter(usize),
    /// Literal value.
    Cell(Cell),
    /// Bound column: owning source plus merged row index.
    Column { source: usize, index: usize },
    /// `new.col` / `old.col` of the trigger body currently executing.
    TriggerColumn {
        /// Read the `OLD` row instead of `NEW`.
        old: bool,
        /// Column position inside the trigger frame.
        index: usize,
    },
    /// `excluded.col` of an `ON CONFLICT DO UPDATE` clause: the supplied row.
    ExcludedColumn {
        /// Column position inside the supplied row.
        index: usize,
    },
    /// Column borrowed from an enclosing scope: `depth` counts outward from the
    /// current core, `index` addresses that scope's flattened row.
    OuterColumn { depth: usize, index: usize },
    /// Whole-result aggregate value already computed for this row.
    AggregateRef(usize),
    /// Prefix `-` or `+`.
    Unary { op: UnaryOp, value: Box<ResolvedExpr> },
    /// Arithmetic `+`, `-`, `*` or `/`.
    Binary { left: Box<ResolvedExpr>, op: BinaryOp, right: Box<ResolvedExpr> },
    /// Value comparison; yields `1`, `0` or `NULL`.
    ///
    /// `collation` 是两个操作数合并后生效的排序规则，解析期确定，求值期只按
    /// 它比较文本。
    Compare { left: Box<ResolvedExpr>, op: CompareOp, right: Box<ResolvedExpr>, collation: Collation },
    /// `IS NULL` or `IS NOT NULL`.
    IsNull { value: Box<ResolvedExpr>, negated: bool },
    /// `IS TRUE`, `IS NOT TRUE`, `IS FALSE` or `IS NOT FALSE`.
    IsTruth { value: Box<ResolvedExpr>, truth: bool, negated: bool },
    /// `NOT`, with three-valued logic.
    Not(Box<ResolvedExpr>),
    /// `AND`, with three-valued logic.
    And(Box<ResolvedExpr>, Box<ResolvedExpr>),
    /// `OR`, with three-valued logic.
    Or(Box<ResolvedExpr>, Box<ResolvedExpr>),
    /// `CASE` in both the searched and the simple form.
    Case {
        /// Simple-form operand; absent in the searched form.
        operand: Option<Box<ResolvedExpr>>,
        /// 简单形式里操作数生效的排序规则；分支自带显式 `COLLATE` 时优先。
        collation: Option<Collation>,
        /// `WHEN condition THEN result` pairs.
        branches: Vec<(ResolvedExpr, ResolvedExpr)>,
        /// `ELSE result`; absent means `NULL`.
        otherwise: Option<Box<ResolvedExpr>>,
    },
    /// `x IN (...)` or `x NOT IN (...)`.
    In { value: Box<ResolvedExpr>, list: Vec<ResolvedExpr>, negated: bool, collation: Option<Collation> },
    /// Scalar function call.
    Function { name: ScalarFunction, args: Vec<ResolvedExpr> },
    /// 宿主注册的自定义标量函数调用;名字已折叠为小写。
    HostFunction {
        /// 折叠小写后的注册名。
        name: String,
        /// 实参,按书写顺序。
        args: Vec<ResolvedExpr>,
    },
    /// `CAST(expr AS type)`.
    Cast { value: Box<ResolvedExpr>, target: CastType },
    /// `expr COLLATE name`；只改变文本比较规则，求值结果与内层表达式相同。
    Collate {
        /// 被施加排序规则的表达式。
        value: Box<ResolvedExpr>,
        /// 解析后的内置排序规则。
        collation: Collation,
    },
    /// `x [NOT] LIKE y [ESCAPE z]`.
    Like { value: Box<ResolvedExpr>, pattern: Box<ResolvedExpr>, negated: bool, escape: Option<Box<ResolvedExpr>> },
    /// `x IS DISTINCT FROM y` or `x IS NOT DISTINCT FROM y`.
    IsDistinct { left: Box<ResolvedExpr>, right: Box<ResolvedExpr>, negated: bool, collation: Collation },
    /// `x GLOB y` or `x NOT GLOB y`.
    Glob { value: Box<ResolvedExpr>, pattern: Box<ResolvedExpr>, negated: bool },
    /// Scalar subquery `(SELECT ...)`.
    Subquery(Box<Statement>),
    /// `EXISTS (SELECT ...)`.
    Exists(Box<Statement>),
    /// `x [NOT] IN (SELECT ...)`.
    InSubquery { value: Box<ResolvedExpr>, query: Box<Statement>, negated: bool },
    /// Value of a window already computed for the current row.
    Window(usize),
}

/// One row of the input a window partition is computed over: either a source
/// row or the representative row of a group, plus that row's aggregate values.
#[derive(Clone)]
struct WindowRow {
    /// Row cells.
    values: Vec<Cell>,
    /// Whole-result aggregate values, empty in an ungrouped query.
    aggregates: Vec<Cell>,
}

/// One resolved window call: function, partitioning, ordering and frame.
struct ResolvedWindow {
    /// Function and its resolved arguments.
    function: ResolvedWindowFunction,
    /// `PARTITION BY` expressions.
    partition: Vec<ResolvedExpr>,
    /// `ORDER BY` keys with their direction.
    order: Vec<(ResolvedExpr, SortDirection)>,
    /// Explicit frame, already merged with a base window's frame.
    frame: Option<ResolvedFrame>,
}

/// A window function with resolved arguments.
enum ResolvedWindowFunction {
    /// `row_number()`.
    RowNumber,
    /// `rank()`.
    Rank,
    /// `dense_rank()`.
    DenseRank,
    /// `percent_rank()`.
    PercentRank,
    /// `cume_dist()`.
    CumeDist,
    /// `ntile(n)`.
    Ntile(ResolvedExpr),
    /// `lag(expr[, offset[, default]])`.
    Lag { value: ResolvedExpr, offset: Option<ResolvedExpr>, default: Option<ResolvedExpr> },
    /// `lead(expr[, offset[, default]])`.
    Lead { value: ResolvedExpr, offset: Option<ResolvedExpr>, default: Option<ResolvedExpr> },
    /// `first_value(expr)`.
    FirstValue(ResolvedExpr),
    /// `last_value(expr)`.
    LastValue(ResolvedExpr),
    /// `nth_value(expr, n)`.
    NthValue { value: ResolvedExpr, position: ResolvedExpr },
    /// Aggregate over the frame.
    Aggregate {
        function: AggregateFunction,
        argument: Option<ResolvedExpr>,
        separator: Option<ResolvedExpr>,
        /// `DISTINCT` was written before the argument.
        distinct: bool,
        /// `FILTER (WHERE ...)` applied before the aggregate sees a row.
        filter: Option<ResolvedExpr>,
        /// `ORDER BY` inside the argument list, used by `group_concat`.
        order: Vec<(ResolvedExpr, SortDirection)>,
        /// `DISTINCT` 去重时使用的排序规则，取自参数表达式的生效排序规则。
        collation: Collation,
    },
}

/// One resolved frame edge.
struct ResolvedFrameBound {
    /// Which edge this is.
    edge: FrameEdge,
    /// Constant offset; `None` for `UNBOUNDED` and `CURRENT ROW`.
    offset: Option<ResolvedExpr>,
}

/// A resolved explicit frame.
struct ResolvedFrame {
    /// `ROWS`, `RANGE` or `GROUPS`.
    units: FrameUnits,
    /// Starting edge.
    start: ResolvedFrameBound,
    /// Ending edge.
    end: ResolvedFrameBound,
    /// Rows dropped from the computed frame.
    exclude: FrameExclude,
}

impl ResolvedExpr {
    /// Evaluates this expression against one merged row.
    ///
    /// Every position (projection, `WHERE`, `ORDER BY`, nested subquery) goes
    /// through this single function, so SQLite's NULL propagation, integer
    /// arithmetic and correlation rules cannot diverge between clauses.
    fn eval(&self, values: &[Cell], ctx: &EvalContext) -> Result<Cell, SqlError> {
        Ok(match self {
            Self::Cell(cell) => cell.clone(),
            Self::Parameter(index) => ctx.engine.parameters.get(*index).cloned().unwrap_or(Cell::Null),
            Self::Column { index, .. } => values[*index].clone(),
            Self::TriggerColumn { old, index } => ctx
                .engine
                .trigger_frames
                .last()
                .and_then(|frame| if *old { frame.old.as_ref() } else { frame.new.as_ref() })
                .and_then(|cells| cells.get(*index))
                .cloned()
                .unwrap_or(Cell::Null),
            Self::ExcludedColumn { index } => ctx.engine.upsert_excluded.get(*index).cloned().unwrap_or(Cell::Null),
            Self::OuterColumn { depth, index } => {
                ctx.engine.outer_read.set(true);
                ctx.outer[*depth].columns[*index].value.clone()
            }
            Self::AggregateRef(index) => ctx.aggregates[*index].clone(),
            Self::Window(index) => ctx.windows.get(*index).and_then(|column| column.get(ctx.window_row)).cloned().unwrap_or(Cell::Null),
            Self::Unary { op, value } => {
                let value = value.eval(values, ctx)?;
                match op {
                    UnaryOp::Negate => negate_cell(&value),
                    UnaryOp::Plus => value,
                    UnaryOp::BitNot => bit_not(&value),
                }
            }
            Self::Binary { left, op: BinaryOp::Concat, right } => concat(&left.eval(values, ctx)?, &right.eval(values, ctx)?),
            Self::Binary { left, op: BinaryOp::Arrow, right } => {
                super::json::arrow(&left.eval(values, ctx)?, &right.eval(values, ctx)?, false)?
            }
            Self::Binary { left, op: BinaryOp::ArrowText, right } => {
                super::json::arrow(&left.eval(values, ctx)?, &right.eval(values, ctx)?, true)?
            }
            Self::Binary { left, op, right } => arithmetic(*op, &left.eval(values, ctx)?, &right.eval(values, ctx)?),
            Self::Compare { left, op, right, collation } => {
                match compare_values(&left.eval(values, ctx)?, &right.eval(values, ctx)?, *op, *collation) {
                    Some(true) => Cell::Integer(1),
                    Some(false) => Cell::Integer(0),
                    None => Cell::Null,
                }
            }
            Self::IsNull { value, negated } => {
                let is_null = matches!(value.eval(values, ctx)?, Cell::Null);
                Cell::Integer(i64::from(is_null != *negated))
            }
            Self::IsTruth { value, truth, negated } => {
                // `IS TRUE` is the SQLite truthiness test, not a comparison with 1,
                // and it is never NULL: a NULL operand is simply not true (nor false).
                let is_truth = truthiness(&value.eval(values, ctx)?) == Some(*truth);
                Cell::Integer(i64::from(is_truth != *negated))
            }
            Self::Not(inner) => match truthiness(&inner.eval(values, ctx)?) {
                Some(value) => Cell::Integer(i64::from(!value)),
                None => Cell::Null,
            },
            Self::And(left, right) => match (truthiness(&left.eval(values, ctx)?), truthiness(&right.eval(values, ctx)?)) {
                (Some(false), _) | (_, Some(false)) => Cell::Integer(0),
                (Some(true), Some(true)) => Cell::Integer(1),
                _ => Cell::Null,
            },
            Self::Or(left, right) => match (truthiness(&left.eval(values, ctx)?), truthiness(&right.eval(values, ctx)?)) {
                (Some(true), _) | (_, Some(true)) => Cell::Integer(1),
                (Some(false), Some(false)) => Cell::Integer(0),
                _ => Cell::Null,
            },
            Self::Case { operand, branches, otherwise, collation } => {
                match operand {
                    None => {
                        for (condition, result) in branches {
                            if truthiness(&condition.eval(values, ctx)?) == Some(true) {
                                return result.eval(values, ctx);
                            }
                        }
                    }
                    Some(operand) => {
                        let base = operand.eval(values, ctx)?;
                        for (candidate, result) in branches {
                            // 分支自带的显式 `COLLATE` 优先于 CASE 操作数的列声明。
                            let branch_collation = collation.or_else(|| match candidate {
                                Self::Collate { collation, .. } => Some(*collation),
                                _ => None,
                            });
                            if compare_values(&base, &candidate.eval(values, ctx)?, CompareOp::Eq, branch_collation.unwrap_or_default())
                                == Some(true)
                            {
                                return result.eval(values, ctx);
                            }
                        }
                    }
                }
                match otherwise {
                    Some(expr) => expr.eval(values, ctx)?,
                    None => Cell::Null,
                }
            }
            Self::In { value, list, negated, collation } => {
                let value = value.eval(values, ctx)?;
                if matches!(value, Cell::Null) {
                    return Ok(Cell::Null);
                }
                let mut saw_null = false;
                for item in list {
                    let item_value = item.eval(values, ctx)?;
                    if matches!(item_value, Cell::Null) {
                        saw_null = true;
                        continue;
                    }
                    // 左操作数的显式 `COLLATE` 优先；否则取列表项自带的显式规则。
                    let item_collation = match collation {
                        Some(collation) => *collation,
                        None => match item {
                            Self::Collate { collation, .. } => *collation,
                            _ => Collation::Binary,
                        },
                    };
                    if compare_values(&value, &item_value, CompareOp::Eq, item_collation) == Some(true) {
                        return Ok(Cell::Integer(i64::from(!*negated)));
                    }
                }
                if saw_null { Cell::Null } else { Cell::Integer(i64::from(*negated)) }
            }
            Self::Function { name, args } => {
                let values = args.iter().map(|arg| arg.eval(values, ctx)).collect::<Result<Vec<_>, _>>()?;
                if name.is_json() {
                    // SQLite 给 JSON 函数的结果打上 JSON 子类型，嵌套进
                    // `json_array` / `json_object` / `json_set` 时按 JSON 值而不是
                    // 普通文本处理。这里同样标记直接调用 JSON 函数的实参。
                    let json_arguments =
                        args.iter().map(|arg| matches!(arg, Self::Function { name, .. } if name.is_json())).collect::<Vec<_>>();
                    super::json::eval(*name, &values, &json_arguments)?
                }
                else {
                    eval_function(*name, &values)
                }
            }
            Self::HostFunction { name, args } => {
                let values = args.iter().map(|arg| arg.eval(values, ctx)).collect::<Result<Vec<_>, _>>()?;
                let implementation = ctx
                    .engine
                    .host_functions
                    .get(name)
                    .ok_or_else(|| SqlError::new("unknown-function", format!("no such function: {name}")))?;
                let arguments = values.iter().map(|cell| cell_to_udf_value(name, cell)).collect::<Result<Vec<_>, _>>()?;
                let mut context = UdfContext::new(UdfBudget::new(ctx.budget.remaining.get() as u64));
                implementation
                    .invoke(&mut context, &arguments)
                    .map_err(|error| udf_error_to_sql_error(name, error))
                    .map(udf_value_to_cell)?
            }
            Self::Cast { value, target } => cast_cell(&value.eval(values, ctx)?, *target),
            Self::Collate { value, .. } => value.eval(values, ctx)?,
            Self::IsDistinct { left, right, negated, collation } => {
                let left = left.eval(values, ctx)?;
                let right = right.eval(values, ctx)?;
                // `NULL` is a value again: two `NULL`s are not distinct, while a
                // `NULL` against anything else is. Non-NULL operands use the same
                // storage-class ordering as `=`, so `1` and `1.0` are the same.
                let distinct = match (&left, &right) {
                    (Cell::Null, Cell::Null) => false,
                    (Cell::Null, _) | (_, Cell::Null) => true,
                    _ => compare_cells_with(&left, &right, *collation) != Ordering::Equal,
                };
                Cell::Integer(i64::from(distinct != *negated))
            }
            Self::Glob { value, pattern, negated } => {
                let value = value.eval(values, ctx)?;
                let pattern = pattern.eval(values, ctx)?;
                // Both operands must be text: `NULL` propagates, and a blob never
                // matches, unlike `LIKE` which unblobs its operand.
                if matches!(value, Cell::Null) || matches!(pattern, Cell::Null) {
                    return Ok(Cell::Null);
                }
                let matched = match (&value, &pattern) {
                    (Cell::Blob(_), _) | (_, Cell::Blob(_)) => false,
                    _ => {
                        let value: Vec<char> = text_form(&value).unwrap_or_default().chars().collect();
                        let pattern: Vec<char> = text_form(&pattern).unwrap_or_default().chars().collect();
                        glob_match(&value, &pattern)
                    }
                };
                Cell::Integer(i64::from(matched != *negated))
            }
            Self::Like { value, pattern, negated, escape } => {
                let value = value.eval(values, ctx)?;
                let pattern = pattern.eval(values, ctx)?;
                let escape = match escape {
                    Some(escape) => {
                        let text = text_form(&escape.eval(values, ctx)?);
                        match text.as_deref().map(|text| text.chars().collect::<Vec<_>>()) {
                            Some(chars) if chars.len() == 1 => Some(chars[0]),
                            Some(_) => {
                                return Err(
                                    SqlError::new("invalid-escape", "ESCAPE expression must be a single character").in_statement("select")
                                );
                            }
                            None => None,
                        }
                    }
                    None => None,
                };
                // `LIKE` with a NULL value, pattern or escape yields NULL.
                let (Some(value), Some(pattern)) = (text_form(&value), text_form(&pattern))
                else {
                    return Ok(Cell::Null);
                };
                let characters: Vec<char> = value.chars().collect();
                let pattern: Vec<char> = pattern.chars().collect();
                let matched = like_match(&characters, &pattern, escape);
                Cell::Integer(i64::from(matched != *negated))
            }
            Self::Subquery(query) => {
                let key = query.as_ref() as *const Statement as usize;
                {
                    let cache = ctx.engine.subquery_cache.borrow();
                    if let Some(SubqueryCache::Rows(rows)) = cache.get(&key) {
                        return Ok(rows.first().map_or(Cell::Null, |row| row[0].clone()));
                    }
                }
                ctx.engine.outer_read.set(false);
                let result = run_subquery(query, values, ctx)?;
                check_single_column(&result)?;
                if !ctx.engine.outer_read.get() {
                    ctx.engine.subquery_cache.borrow_mut().insert(key, SubqueryCache::Rows(result.rows.clone()));
                }
                result.rows.first().map_or(Cell::Null, |row| row[0].clone())
            }
            Self::Exists(query) => {
                // Existence is two-valued: no result value is consumed, so a
                // surrounding `NOT` can never turn this into `NULL`.
                let key = query.as_ref() as *const Statement as usize;
                {
                    let cache = ctx.engine.subquery_cache.borrow();
                    if let Some(SubqueryCache::Rows(rows)) = cache.get(&key) {
                        return Ok(Cell::Integer(i64::from(!rows.is_empty())));
                    }
                }
                ctx.engine.outer_read.set(false);
                let result = run_subquery(query, values, ctx)?;
                if !ctx.engine.outer_read.get() {
                    ctx.engine.subquery_cache.borrow_mut().insert(key, SubqueryCache::Rows(result.rows.clone()));
                }
                Cell::Integer(i64::from(!result.rows.is_empty()))
            }
            Self::InSubquery { value, query, negated } => {
                let value = value.eval(values, ctx)?;
                let key = query.as_ref() as *const Statement as usize;
                {
                    let cache = ctx.engine.subquery_cache.borrow();
                    if let Some(SubqueryCache::InList { values: list, keys, saw_null }) = cache.get(&key) {
                        return Ok(in_result_cached(&value, list, keys.as_ref(), *saw_null, *negated));
                    }
                }
                ctx.engine.outer_read.set(false);
                let result = run_subquery(query, values, ctx)?;
                check_single_column(&result)?;
                let correlated = ctx.engine.outer_read.get();
                let mut list = Vec::with_capacity(result.rows.len());
                let mut saw_null = false;
                for row in &result.rows {
                    let cell = row[0].clone();
                    if matches!(cell, Cell::Null) {
                        saw_null = true;
                    }
                    list.push(cell);
                }
                let keys = in_list_keys(&list);
                let answer = in_result_cached(&value, &list, keys.as_ref(), saw_null, *negated);
                if !correlated {
                    ctx.engine.subquery_cache.borrow_mut().insert(key, SubqueryCache::InList { values: list, keys, saw_null });
                }
                answer
            }
        })
    }

    /// Appends the source indices this expression reads.
    fn collect_sources(&self, out: &mut Vec<usize>) {
        match self {
            Self::Cell(_) | Self::OuterColumn { .. } | Self::AggregateRef(_) | Self::TriggerColumn { .. } | Self::ExcludedColumn { .. } => {}
            // A subquery's own scope is resolved when it runs, so it cannot be
            // attributed to a single source here; `contains_subquery` makes
            // such a conjunct wait for the complete row instead.
            Self::Subquery(_) | Self::Exists(_) => {}
            Self::Column { source, .. } => out.push(*source),
            Self::Unary { value, .. }
            | Self::Not(value)
            | Self::IsNull { value, .. }
            | Self::IsTruth { value, .. }
            | Self::Cast { value, .. }
            | Self::Collate { value, .. } => value.collect_sources(out),
            Self::Binary { left, right, .. } | Self::Compare { left, right, .. } | Self::And(left, right) | Self::Or(left, right) => {
                left.collect_sources(out);
                right.collect_sources(out);
            }
            Self::IsDistinct { left, right, .. } => {
                left.collect_sources(out);
                right.collect_sources(out);
            }
            Self::Glob { value, pattern, .. } => {
                value.collect_sources(out);
                pattern.collect_sources(out);
            }
            Self::Case { operand, branches, otherwise, .. } => {
                if let Some(operand) = operand {
                    operand.collect_sources(out);
                }
                for (condition, result) in branches {
                    condition.collect_sources(out);
                    result.collect_sources(out);
                }
                if let Some(otherwise) = otherwise {
                    otherwise.collect_sources(out);
                }
            }
            Self::In { value, list, .. } => {
                value.collect_sources(out);
                for item in list {
                    item.collect_sources(out);
                }
            }
            Self::InSubquery { value, .. } => value.collect_sources(out),
            Self::Function { args, .. } | Self::HostFunction { args, .. } => {
                for arg in args {
                    arg.collect_sources(out);
                }
            }
            // Window values are already materialized for the current row.
            Self::Window(_) => {}
            Self::Parameter(_) => {}
            Self::Like { value, pattern, escape, .. } => {
                value.collect_sources(out);
                pattern.collect_sources(out);
                if let Some(escape) = escape {
                    escape.collect_sources(out);
                }
            }
        }
    }

    /// Whether this expression runs a subquery and therefore cannot be
    /// evaluated before the complete current row is bound.
    fn contains_subquery(&self) -> bool {
        match self {
            Self::Subquery(_) | Self::Exists(_) | Self::InSubquery { .. } => true,
            Self::Unary { value, .. }
            | Self::Not(value)
            | Self::IsNull { value, .. }
            | Self::IsTruth { value, .. }
            | Self::Cast { value, .. }
            | Self::Collate { value, .. } => value.contains_subquery(),
            Self::Binary { left, right, .. } | Self::Compare { left, right, .. } | Self::And(left, right) | Self::Or(left, right) => {
                left.contains_subquery() || right.contains_subquery()
            }
            Self::IsDistinct { left, right, .. } => left.contains_subquery() || right.contains_subquery(),
            Self::Glob { value, pattern, .. } => value.contains_subquery() || pattern.contains_subquery(),
            Self::Case { operand, branches, otherwise, .. } => {
                operand.as_ref().is_some_and(|operand| operand.contains_subquery())
                    || branches.iter().any(|(condition, result)| condition.contains_subquery() || result.contains_subquery())
                    || otherwise.as_ref().is_some_and(|otherwise| otherwise.contains_subquery())
            }
            Self::In { value, list, .. } => value.contains_subquery() || list.iter().any(ResolvedExpr::contains_subquery),
            Self::Function { args, .. } | Self::HostFunction { args, .. } => args.iter().any(ResolvedExpr::contains_subquery),
            Self::Like { value, pattern, escape, .. } => {
                value.contains_subquery() || pattern.contains_subquery() || escape.as_ref().is_some_and(|escape| escape.contains_subquery())
            }
            Self::Cell(_)
            | Self::Parameter(_)
            | Self::Column { .. }
            | Self::OuterColumn { .. }
            | Self::AggregateRef(_)
            | Self::Window(_)
            | Self::TriggerColumn { .. }
            | Self::ExcludedColumn { .. } => false,
        }
    }
}

/// One resolved `ORDER BY` key.
enum SortKey {
    /// Zero-based index into the projection output.
    Projected(usize),
    /// Expression over the merged source row.
    Expression(ResolvedExpr),
}

/// Direction of one resolved sort key plus its explicit `NULL` placement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SortDirection {
    /// `DESC` was written.
    descending: bool,
    /// `NULLS FIRST` / `NULLS LAST`; `None` keeps the SQLite default, which is
    /// first for `ASC` and last for `DESC`.
    nulls_first: Option<bool>,
    /// 键表达式的文本排序规则，默认为 `BINARY`。
    collation: Collation,
}

impl SortDirection {
    /// Direction of a parsed key.
    fn of(key: &OrderKey) -> Self {
        Self { descending: key.descending, nulls_first: key.nulls_first, collation: Collation::Binary }
    }

    /// 附加键表达式的排序规则。
    fn with_collation(mut self, collation: Collation) -> Self {
        self.collation = collation;
        self
    }

    /// Compares two cells, honouring the direction and the `NULL` placement.
    fn compare(self, left: &Cell, right: &Cell) -> Ordering {
        match (left, right) {
            (Cell::Null, Cell::Null) => Ordering::Equal,
            (Cell::Null, _) | (_, Cell::Null) => {
                let first = self.nulls_first.unwrap_or(!self.descending);
                if matches!(left, Cell::Null) == first { Ordering::Less } else { Ordering::Greater }
            }
            _ => {
                let ordering = compare_cells_with(left, right, self.collation);
                if self.descending { ordering.reverse() } else { ordering }
            }
        }
    }
}

/// One `GROUP BY` term.
enum GroupKey {
    /// A one-based projection ordinal.
    Projected(usize, Collation),
    /// Expression over the merged source row.
    Expression(ResolvedExpr, Collation),
}

/// 排序规则未知时的统一诊断文案，与上游 SQLite 一致。
fn unknown_collation(name: &str) -> SqlError {
    SqlError::new("unknown-collation", format!("no such collation sequence: {name}"))
}

/// 解析期求一个表达式生效的排序规则：`COLLATE` 用显式声明，裸列用列声明，
/// 其余表达式按 `BINARY`。
fn parsed_collation(expr: &Expr, relation: &Relation) -> Result<Collation, SqlError> {
    match expr {
        Expr::Collate { collation, .. } => Collation::parse(collation).ok_or_else(|| unknown_collation(collation)),
        Expr::Column(reference) => Ok(match relation.resolve(reference) {
            Ok((_, merged)) => relation.column_collation(merged),
            Err(_) => Collation::Binary,
        }),
        Expr::Alias { value, .. } => parsed_collation(value, relation),
        _ => Ok(Collation::Binary),
    }
}

/// 返回带显式 `COLLATE` 的那一层表达式，供二元比较按左侧优先取规则。
fn explicit_collate(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::Collate { .. } => Some(expr),
        Expr::Alias { value, .. } => match value.as_ref() {
            Expr::Collate { .. } => Some(value),
            _ => None,
        },
        _ => None,
    }
}

/// 二元比较合并后的排序规则：左显式、右显式、左列、右列，最后 `BINARY`。
fn comparison_collation(left: &Expr, right: &Expr, relation: &Relation) -> Result<Collation, SqlError> {
    if let Some(expr) = explicit_collate(left) {
        return parsed_collation(expr, relation);
    }
    if let Some(expr) = explicit_collate(right) {
        return parsed_collation(expr, relation);
    }
    match parsed_collation(left, relation)? {
        Collation::Binary => parsed_collation(right, relation),
        collation => Ok(collation),
    }
}

/// 已解析表达式生效的排序规则，供 `ORDER BY` / `GROUP BY` 使用。
fn resolved_collation(expr: &ResolvedExpr, relation: &Relation) -> Collation {
    match expr {
        ResolvedExpr::Collate { collation, .. } => *collation,
        ResolvedExpr::Column { index, .. } => relation.column_collation(*index),
        _ => Collation::Binary,
    }
}

/// Resolves one parsed expression against the current core.
///
/// An unresolved column is retried in each enclosing scope, innermost first.
/// Because the current core is always tried first, a same-named inner column
/// shadows an outer one, exactly as in SQLite.
#[allow(clippy::too_many_arguments)]
fn resolve_expr(
    expr: &Expr,
    relation: &Relation,
    outer: &[ScopeShape],
    named: &[NamedWindow],
    windows: &mut Vec<ResolvedWindow>,
    aggregates: &mut Vec<AggregateSpec>,
    allow_aggregate: bool,
    allow_window: bool,
) -> Result<ResolvedExpr, SqlError> {
    Ok(match expr {
        Expr::Column(reference) => match relation.resolve(reference) {
            Ok((TRIGGER_SOURCE_NEW, index)) => ResolvedExpr::TriggerColumn { old: false, index },
            Ok((TRIGGER_SOURCE_OLD, index)) => ResolvedExpr::TriggerColumn { old: true, index },
            Ok((EXCLUDED_SOURCE, index)) => ResolvedExpr::ExcludedColumn { index },
            Ok((source, index)) => ResolvedExpr::Column { source, index },
            Err(error) if error.code == "unknown-column" || error.code == "unknown-table" => match resolve_outer(reference, outer)? {
                Some(found) => found,
                None => return Err(error),
            },
            Err(error) => return Err(error),
        },
        Expr::Literal(literal) => ResolvedExpr::Cell(literal_cell(literal)),
        Expr::Parameter { index, .. } => ResolvedExpr::Parameter(*index),
        // A projection alias only affects the header, which is taken from the
        // unresolved expression, so evaluation delegates straight through.
        Expr::Alias { value, .. } => resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?,
        Expr::Unary { op, value } => ResolvedExpr::Unary {
            op: *op,
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
        },
        Expr::Binary { left, op, right } => ResolvedExpr::Binary {
            left: Box::new(resolve_expr(left, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            op: *op,
            right: Box::new(resolve_expr(right, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
        },
        Expr::Compare { left, op, right } => ResolvedExpr::Compare {
            left: Box::new(resolve_expr(left, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            op: *op,
            collation: comparison_collation(left, right, relation)?,
            right: Box::new(resolve_expr(right, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
        },
        Expr::IsNull { value, negated } => ResolvedExpr::IsNull {
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            negated: *negated,
        },
        Expr::IsTruth { value, truth, negated } => ResolvedExpr::IsTruth {
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            truth: *truth,
            negated: *negated,
        },
        Expr::Not(inner) => {
            ResolvedExpr::Not(Box::new(resolve_expr(inner, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?))
        }
        Expr::And(left, right) => ResolvedExpr::And(
            Box::new(resolve_expr(left, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            Box::new(resolve_expr(right, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
        ),
        Expr::Or(left, right) => ResolvedExpr::Or(
            Box::new(resolve_expr(left, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            Box::new(resolve_expr(right, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
        ),
        Expr::Case { operand, branches, otherwise } => ResolvedExpr::Case {
            collation: match operand {
                Some(operand) => match parsed_collation(operand, relation)? {
                    Collation::Binary => None,
                    collation => Some(collation),
                },
                None => None,
            },
            operand: operand
                .as_ref()
                .map(|inner| {
                    Ok::<_, SqlError>(Box::new(resolve_expr(
                        inner,
                        relation,
                        outer,
                        named,
                        windows,
                        aggregates,
                        allow_aggregate,
                        allow_window,
                    )?))
                })
                .transpose()?,
            branches: branches
                .iter()
                .map(|(condition, result)| {
                    Ok((
                        resolve_expr(condition, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?,
                        resolve_expr(result, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?,
                    ))
                })
                .collect::<Result<Vec<_>, SqlError>>()?,
            otherwise: otherwise
                .as_ref()
                .map(|inner| {
                    Ok::<_, SqlError>(Box::new(resolve_expr(
                        inner,
                        relation,
                        outer,
                        named,
                        windows,
                        aggregates,
                        allow_aggregate,
                        allow_window,
                    )?))
                })
                .transpose()?,
        },
        Expr::In { value, list, negated } => ResolvedExpr::In {
            collation: match parsed_collation(value, relation)? {
                Collation::Binary => None,
                collation => Some(collation),
            },
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            list: list
                .iter()
                .map(|item| resolve_expr(item, relation, outer, named, windows, aggregates, allow_aggregate, allow_window))
                .collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        },
        Expr::Function { name, args } => ResolvedExpr::Function {
            name: *name,
            args: args
                .iter()
                .map(|arg| resolve_expr(arg, relation, outer, named, windows, aggregates, allow_aggregate, allow_window))
                .collect::<Result<Vec<_>, _>>()?,
        },
        Expr::HostFunction { name, args } => ResolvedExpr::HostFunction {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| resolve_expr(arg, relation, outer, named, windows, aggregates, allow_aggregate, allow_window))
                .collect::<Result<Vec<_>, _>>()?,
        },
        Expr::Cast { value, target } => ResolvedExpr::Cast {
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            target: *target,
        },
        Expr::Collate { value, collation } => ResolvedExpr::Collate {
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            collation: Collation::parse(collation).ok_or_else(|| unknown_collation(collation))?,
        },
        Expr::Like { value, pattern, negated, escape } => ResolvedExpr::Like {
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            pattern: Box::new(resolve_expr(pattern, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            negated: *negated,
            escape: match escape {
                Some(escape) => {
                    Some(Box::new(resolve_expr(escape, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?))
                }
                None => None,
            },
        },
        Expr::Glob { value, pattern, negated } => ResolvedExpr::Glob {
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            pattern: Box::new(resolve_expr(pattern, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            negated: *negated,
        },
        Expr::IsDistinct { left, right, negated } => ResolvedExpr::IsDistinct {
            left: Box::new(resolve_expr(left, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            right: Box::new(resolve_expr(right, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            negated: *negated,
            collation: comparison_collation(left, right, relation)?,
        },
        Expr::Subquery(query) => ResolvedExpr::Subquery(query.clone()),
        Expr::Exists(query) => ResolvedExpr::Exists(query.clone()),
        Expr::InSubquery { value, query, negated } => ResolvedExpr::InSubquery {
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            query: query.clone(),
            negated: *negated,
        },
        Expr::Aggregate { function, argument, separator, distinct, filter, order } => {
            if !allow_aggregate {
                let mut error =
                    SqlError::new("misuse-of-aggregate", format!("misuse of aggregate function {}()", function.name())).in_statement("select");
                error.clause = Some(function.name().to_ascii_uppercase());
                return Err(error);
            }
            // An aggregate argument is evaluated per row, so it may not contain
            // another aggregate; subqueries inside it are still allowed.
            let argument =
                argument.as_ref().map(|arg| resolve_expr(arg, relation, outer, named, windows, aggregates, false, false)).transpose()?;
            let separator =
                separator.as_ref().map(|sep| resolve_expr(sep, relation, outer, named, windows, aggregates, false, false)).transpose()?;
            let filter =
                filter.as_ref().map(|expr| resolve_expr(expr, relation, outer, named, windows, aggregates, false, false)).transpose()?;
            let order = order
                .iter()
                .map(|key| {
                    let resolved = resolve_expr(&key.expr, relation, outer, named, windows, aggregates, false, false)?;
                    let collation = resolved_collation(&resolved, relation);
                    Ok((resolved, SortDirection::of(key).with_collation(collation)))
                })
                .collect::<Result<Vec<_>, SqlError>>()?;
            let index = aggregates.len();
            let collation = argument.as_ref().map(|arg| resolved_collation(arg, relation)).unwrap_or_default();
            aggregates.push(AggregateSpec { function: *function, argument, separator, distinct: *distinct, filter, order, collation });
            ResolvedExpr::AggregateRef(index)
        }
        Expr::Window { function, spec } => {
            if !allow_window {
                let mut error = SqlError::new("misuse-of-window", "misuse of window function").in_statement("select");
                error.clause = Some(window_call_name(function).to_ascii_uppercase());
                return Err(error);
            }
            let resolved = resolve_window(function, spec, relation, outer, named, windows, aggregates)?;
            let index = windows.len();
            windows.push(resolved);
            ResolvedExpr::Window(index)
        }
    })
}

/// Lowercase name of a window function, used in `misuse` diagnostics.
fn window_call_name(function: &WindowFunction) -> &'static str {
    match function {
        WindowFunction::RowNumber => "row_number",
        WindowFunction::Rank => "rank",
        WindowFunction::DenseRank => "dense_rank",
        WindowFunction::PercentRank => "percent_rank",
        WindowFunction::CumeDist => "cume_dist",
        WindowFunction::Ntile(_) => "ntile",
        WindowFunction::Lag { .. } => "lag",
        WindowFunction::Lead { .. } => "lead",
        WindowFunction::FirstValue(_) => "first_value",
        WindowFunction::LastValue(_) => "last_value",
        WindowFunction::NthValue { .. } => "nth_value",
        WindowFunction::Aggregate { function, .. } => function.name(),
    }
}

/// Resolves a window call, merging the `OVER` clause with any base window.
fn resolve_window(
    function: &WindowFunction,
    spec: &WindowSpec,
    relation: &Relation,
    outer: &[ScopeShape],
    named: &[NamedWindow],
    windows: &mut Vec<ResolvedWindow>,
    aggregates: &mut Vec<AggregateSpec>,
) -> Result<ResolvedWindow, SqlError> {
    let merged = merge_window_spec(spec, named, &mut Vec::new(), None)?;
    let partition = merged
        .partition
        .iter()
        .map(|expr| resolve_expr(expr, relation, outer, named, windows, aggregates, true, false))
        .collect::<Result<Vec<_>, _>>()?;
    let order = merged
        .order
        .iter()
        .map(|key| {
            let resolved = resolve_expr(&key.expr, relation, outer, named, windows, aggregates, true, false)?;
            let collation = resolved_collation(&resolved, relation);
            Ok((resolved, SortDirection::of(key).with_collation(collation)))
        })
        .collect::<Result<Vec<_>, SqlError>>()?;
    let frame = match merged.frame {
        Some(frame) => Some(resolve_frame(frame, relation, outer, named, windows, aggregates)?),
        None => None,
    };
    let function = resolve_window_function(function, relation, outer, named, windows, aggregates)?;
    Ok(ResolvedWindow { function, partition, order, frame })
}

/// Flattened window definition after following base-window references.
struct MergedWindow<'a> {
    partition: Vec<&'a Expr>,
    order: Vec<&'a OrderKey>,
    frame: Option<&'a super::sql::Frame>,
}

/// Follows `w1 -> w2 -> ...` references and rejects clauses that would
/// override a base window's own clause, as SQLite does.
fn merge_window_spec<'a>(
    spec: &'a WindowSpec,
    named: &'a [NamedWindow],
    seen: &mut Vec<String>,
    window_name: Option<&str>,
) -> Result<MergedWindow<'a>, SqlError> {
    let mut partition = Vec::new();
    let mut order = Vec::new();
    let mut frame = None;
    if let Some(base) = &spec.base {
        let self_reference = window_name.is_some_and(|name| name.eq_ignore_ascii_case(base));
        if !self_reference && !seen.iter().any(|name| name.eq_ignore_ascii_case(base)) {
            let definition = named.iter().rev().find(|window| window.name.eq_ignore_ascii_case(base)).ok_or_else(|| {
                let mut error = SqlError::new("unknown-window", format!("no such window: {base}")).in_statement("select");
                error.clause = Some(base.to_ascii_uppercase());
                error
            })?;
            seen.push(base.clone());
            let inherited = merge_window_spec(&definition.spec, named, seen, Some(base))?;
            partition = inherited.partition;
            order = inherited.order;
            frame = inherited.frame;
        }
    }
    if !spec.partition.is_empty() {
        if !partition.is_empty() {
            return Err(SqlError::new(
                "window-override",
                format!("cannot override PARTITION clause of window: {}", spec.base.clone().unwrap_or_default()),
            )
            .in_statement("select"));
        }
        partition = spec.partition.iter().collect();
    }
    if !spec.order.is_empty() {
        if !order.is_empty() {
            return Err(SqlError::new(
                "window-override",
                format!("cannot override ORDER BY clause of window: {}", spec.base.clone().unwrap_or_default()),
            )
            .in_statement("select"));
        }
        order = spec.order.iter().collect();
    }
    if let Some(explicit) = &spec.frame {
        if frame.is_some() {
            return Err(SqlError::new(
                "window-override",
                format!("cannot override frame clause of window: {}", spec.base.clone().unwrap_or_default()),
            )
            .in_statement("select"));
        }
        frame = Some(explicit);
    }
    Ok(MergedWindow { partition, order, frame })
}

/// Resolves an explicit frame; offsets must be constant non-negative integers.
fn resolve_frame(
    frame: &super::sql::Frame,
    _relation: &Relation,
    _outer: &[ScopeShape],
    _named: &[NamedWindow],
    _windows: &mut Vec<ResolvedWindow>,
    _aggregates: &mut Vec<AggregateSpec>,
) -> Result<ResolvedFrame, SqlError> {
    // SQLite only accepts a literal non-negative integer here; `abs(-1)` and
    // `1.5` are both rejected at prepare time. Validating now keeps evaluation
    // free of column lookups, which a frame offset can never have.
    let resolve_bound = |bound: &super::sql::FrameBound, which: &str| -> Result<ResolvedFrameBound, SqlError> {
        let offset = match &bound.offset {
            Some(offset) => match offset.as_ref() {
                Expr::Literal(Literal::Integer(value)) if *value >= 0 => Some(ResolvedExpr::Cell(Cell::Integer(*value))),
                _ => {
                    return Err(
                        SqlError::new("invalid-frame", format!("frame {which} offset must be a non-negative integer")).in_statement("select")
                    );
                }
            },
            None => None,
        };
        Ok(ResolvedFrameBound { edge: bound.edge, offset })
    };
    let start = resolve_bound(&frame.start, "starting")?;
    let end = resolve_bound(&frame.end, "ending")?;
    Ok(ResolvedFrame { units: frame.units, start, end, exclude: frame.exclude })
}

/// Resolves the arguments of a window function.
fn resolve_window_function(
    function: &WindowFunction,
    relation: &Relation,
    outer: &[ScopeShape],
    named: &[NamedWindow],
    windows: &mut Vec<ResolvedWindow>,
    aggregates: &mut Vec<AggregateSpec>,
) -> Result<ResolvedWindowFunction, SqlError> {
    // Arguments are evaluated per row of the frame, so they may contain
    // aggregates but never another window function.
    let mut argument = |expr: &Expr| resolve_expr(expr, relation, outer, named, windows, aggregates, true, false);
    Ok(match function {
        WindowFunction::RowNumber => ResolvedWindowFunction::RowNumber,
        WindowFunction::Rank => ResolvedWindowFunction::Rank,
        WindowFunction::DenseRank => ResolvedWindowFunction::DenseRank,
        WindowFunction::PercentRank => ResolvedWindowFunction::PercentRank,
        WindowFunction::CumeDist => ResolvedWindowFunction::CumeDist,
        WindowFunction::Ntile(buckets) => ResolvedWindowFunction::Ntile(argument(buckets)?),
        WindowFunction::Lag { value, offset, default } => ResolvedWindowFunction::Lag {
            value: argument(value)?,
            offset: offset.as_ref().map(|expr| argument(expr)).transpose()?,
            default: default.as_ref().map(|expr| argument(expr)).transpose()?,
        },
        WindowFunction::Lead { value, offset, default } => ResolvedWindowFunction::Lead {
            value: argument(value)?,
            offset: offset.as_ref().map(|expr| argument(expr)).transpose()?,
            default: default.as_ref().map(|expr| argument(expr)).transpose()?,
        },
        WindowFunction::FirstValue(value) => ResolvedWindowFunction::FirstValue(argument(value)?),
        WindowFunction::LastValue(value) => ResolvedWindowFunction::LastValue(argument(value)?),
        WindowFunction::NthValue { value, position } => {
            ResolvedWindowFunction::NthValue { value: argument(value)?, position: argument(position)? }
        }
        WindowFunction::Aggregate { function, argument: value, separator, distinct, filter, order } => {
            let mut resolved_order = Vec::with_capacity(order.len());
            for key in order {
                let resolved = argument(&key.expr)?;
                let collation = resolved_collation(&resolved, relation);
                resolved_order.push((resolved, SortDirection::of(key).with_collation(collation)));
            }
            let resolved_argument = value.as_ref().map(|expr| argument(expr)).transpose()?;
            let collation = resolved_argument.as_ref().map(|arg| resolved_collation(arg, relation)).unwrap_or_default();
            ResolvedWindowFunction::Aggregate {
                function: *function,
                argument: resolved_argument,
                separator: separator.as_ref().map(|expr| argument(expr)).transpose()?,
                distinct: *distinct,
                filter: filter.as_ref().map(|expr| argument(expr)).transpose()?,
                order: resolved_order,
                collation,
            }
        }
    })
}

/// Retries a column reference in the enclosing scopes, innermost first.
fn resolve_outer(reference: &ColumnRef, outer: &[ScopeShape]) -> Result<Option<ResolvedExpr>, SqlError> {
    for (depth, scope) in outer.iter().enumerate() {
        let mut matches = Vec::new();
        for (index, (label, name)) in scope.columns.iter().enumerate() {
            let qualifier_matches = reference.qualifier.as_ref().is_none_or(|qualifier| qualifier.eq_ignore_ascii_case(label));
            if qualifier_matches && name.eq_ignore_ascii_case(&reference.name) {
                matches.push(index);
            }
        }
        match matches.len() {
            0 => continue,
            1 => return Ok(Some(ResolvedExpr::OuterColumn { depth, index: matches[0] })),
            _ => {
                return Err(SqlError::new("ambiguous-column", format!("ambiguous column `{}` in an enclosing `SELECT`", reference.display()))
                    .in_statement("select"));
            }
        }
    }
    Ok(None)
}

/// Runs one nested `SELECT` with the current row as the innermost enclosing
/// scope.
///
/// The subquery reuses the same `select` evaluator, so its `FROM`, predicates,
/// ordering, compounds and further nesting all behave exactly like a top-level
/// statement.
fn run_subquery(query: &Statement, values: &[Cell], ctx: &EvalContext) -> Result<ExecResult, SqlError> {
    ctx.budget.charge(1)?;
    let mut scopes = Vec::with_capacity(ctx.outer.len() + 1);
    scopes.push(ctx.current.instantiate(values));
    scopes.extend(ctx.outer.iter().cloned());
    // The subquery keeps the CTEs that are in scope where it appears, so
    // `(select ... from c)` inside a `WITH` statement resolves too.
    ctx.engine.select_statement(query, &scopes, ctx.budget, ctx.ctes)
}

/// A scalar subquery and an `IN (SELECT ...)` must produce exactly one column,
/// as upstream SQLite enforces at prepare time.
fn check_single_column(result: &ExecResult) -> Result<(), SqlError> {
    if result.columns.len() != 1 {
        return Err(
            SqlError::new("arity-mismatch", format!("sub-select returns {} columns - expected 1", result.columns.len())).in_statement("select")
        );
    }
    Ok(())
}

/// Which position of a frame `first_value`/`last_value`/`nth_value` reads.
#[derive(Clone, Copy)]
enum ValuePick {
    /// First row of the frame.
    First,
    /// Last row of the frame.
    Last,
    /// `nth` row of the frame.
    Nth,
}

/// Compares two input rows by partition keys, then by ORDER BY keys.
fn compare_window_rows(
    left_partition: &[Cell],
    left_order: &[Cell],
    right_partition: &[Cell],
    right_order: &[Cell],
    order: &[(ResolvedExpr, SortDirection)],
) -> Ordering {
    for (left, right) in left_partition.iter().zip(right_partition) {
        let ordering = compare_cells(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    for (index, (_, direction)) in order.iter().enumerate() {
        let ordering = direction.compare(&left_order[index], &right_order[index]);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

/// Row-wise equality using SQLite's cross-type numeric rule (`1` equals `1.0`).
fn cells_equal(left: &[Cell], right: &[Cell]) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|(left, right)| compare_cells(left, right) == Ordering::Equal)
}

/// Integer offset a frame edge carries; `None` for `UNBOUNDED` and `CURRENT ROW`.
fn range_offset(bound: &ResolvedFrameBound, which: &str) -> Result<Option<i64>, SqlError> {
    match &bound.offset {
        Some(ResolvedExpr::Cell(Cell::Integer(value))) if *value >= 0 => Ok(Some(*value)),
        Some(_) => Err(SqlError::new("invalid-frame", format!("frame {which} offset must be a non-negative integer")).in_statement("select")),
        None => Ok(None),
    }
}

/// Value an `RANGE` edge names on the ORDER BY axis, given the sort direction.
fn range_edge(edge: FrameEdge, offset: Option<i64>, current: f64, descending: bool) -> f64 {
    let offset = offset.unwrap_or(0) as f64;
    match edge {
        FrameEdge::UnboundedPreceding => {
            if descending {
                f64::INFINITY
            }
            else {
                f64::NEG_INFINITY
            }
        }
        FrameEdge::UnboundedFollowing => {
            if descending {
                f64::NEG_INFINITY
            }
            else {
                f64::INFINITY
            }
        }
        FrameEdge::CurrentRow => current,
        FrameEdge::Preceding => {
            if descending {
                current + offset
            }
            else {
                current - offset
            }
        }
        FrameEdge::Following => {
            if descending {
                current - offset
            }
            else {
                current + offset
            }
        }
    }
}

/// Resolves one `ROWS` frame edge to a position in the partition.
fn rows_edge(bound: &ResolvedFrameBound, which: &str, slot: usize, len: usize) -> Result<i64, SqlError> {
    let position = slot as i64;
    Ok(match bound.edge {
        FrameEdge::UnboundedPreceding => 0,
        FrameEdge::UnboundedFollowing => len.saturating_sub(1) as i64,
        FrameEdge::CurrentRow => position,
        // Frames never extend past either end of the partition.
        FrameEdge::Preceding => position.saturating_sub(range_offset(bound, which)?.unwrap_or(0)).max(0),
        FrameEdge::Following => position.saturating_add(range_offset(bound, which)?.unwrap_or(0)).min(len.saturating_sub(1) as i64),
    })
}

/// Resolves one `GROUPS` frame edge to a position in the partition.
///
/// Peer groups are contiguous, so `group_of` (each row's dense group number)
/// both locates the target group and tells whether it lies outside the
/// partition.
fn groups_edge(
    bound: &ResolvedFrameBound,
    which: &str,
    slot: usize,
    len: usize,
    peer_start: &[usize],
    peer_end: &[usize],
    group_of: &[i64],
) -> Result<i64, SqlError> {
    let starting = which == "starting";
    // `CURRENT ROW` names the row's whole peer group: a start edge lands on its
    // first row and an end edge on its last.
    if bound.edge == FrameEdge::CurrentRow {
        return Ok(if starting { peer_start[slot] as i64 } else { peer_end[slot] as i64 });
    }
    let group = group_of[slot];
    let target = match bound.edge {
        FrameEdge::UnboundedPreceding => return Ok(if starting { 0 } else { -1 }),
        FrameEdge::UnboundedFollowing => {
            return Ok(if starting { len as i64 } else { len.saturating_sub(1) as i64 });
        }
        FrameEdge::Preceding => group.saturating_sub(range_offset(bound, which)?.unwrap_or(0)),
        FrameEdge::Following => group.saturating_add(range_offset(bound, which)?.unwrap_or(0)),
        FrameEdge::CurrentRow => unreachable!("handled above"),
    };
    if target < 0 {
        return Ok(if starting { 0 } else { -1 });
    }
    // Frames never extend past either end of the partition.
    let first = group_of.partition_point(|value| *value < target);
    if first >= len {
        return Ok(if starting { len as i64 } else { len as i64 - 1 });
    }
    Ok(if starting { peer_start[first] as i64 } else { peer_end[first] as i64 })
}

/// `EXCLUDE` decision for one frame position computed for `slot`.
fn frame_row_excluded(exclude: FrameExclude, position: usize, slot: usize, peer_start: &[usize], peer_end: &[usize]) -> bool {
    match exclude {
        FrameExclude::NoOthers => false,
        FrameExclude::CurrentRow => position == slot,
        FrameExclude::Group => position >= peer_start[slot] && position <= peer_end[slot],
        // `TIES` drops the peers but keeps the row being computed, even when it
        // is the only member of its group.
        FrameExclude::Ties => position >= peer_start[slot] && position <= peer_end[slot] && position != slot,
    }
}

/// Bucket count of `ntile(n)`; SQLite rejects zero and negative counts.
fn ntile_buckets(cell: &Cell) -> Result<usize, SqlError> {
    match numeric(cell) {
        Some(Number::Int(value)) if value > 0 => Ok(value as usize),
        Some(Number::Real(value)) if value.is_finite() && value.fract() == 0.0 && value > 0.0 && value <= usize::MAX as f64 => {
            Ok(value as usize)
        }
        _ => Err(SqlError::new("invalid-argument", "argument of ntile must be a positive integer").in_statement("select")),
    }
}

/// Position argument of `nth_value`; SQLite rejects zero and negative values.
fn nth_position(cell: &Cell) -> Result<i64, SqlError> {
    match numeric(cell) {
        Some(Number::Int(value)) if value > 0 => Ok(value),
        Some(Number::Real(value)) if value.is_finite() && value.fract() == 0.0 && value > 0.0 && value <= i64::MAX as f64 => Ok(value as i64),
        _ => Err(SqlError::new("invalid-argument", "second argument to nth_value must be a positive integer").in_statement("select")),
    }
}

/// Offset of `lag`/`lead`; a non-integer offset makes SQLite yield `NULL`.
fn lag_offset(cell: &Cell) -> Option<i64> {
    match numeric(cell)? {
        Number::Int(value) => Some(value),
        Number::Real(value) if value.is_finite() && value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 => {
            Some(value as i64)
        }
        Number::Real(_) => None,
    }
}

/// Converts a `LIMIT`/`OFFSET` expression result to an integer, or reports
/// SQLite's `datatype mismatch`.
fn limit_integer(cell: &Cell, clause: &str) -> Result<i64, SqlError> {
    let converted = match cell {
        Cell::Integer(value) => Some(*value),
        Cell::Real(value) => real_integer(*value),
        // Text first tries the exact 64-bit form, then the numeric one, so
        // `'9223372036854775807'` survives where an `f64` round-trip would not.
        Cell::Text(text) => match text.trim().parse::<i64>() {
            Ok(value) => Some(value),
            Err(_) => text.trim().parse::<f64>().ok().and_then(real_integer),
        },
        Cell::Null | Cell::Blob(_) => None,
    };
    converted.ok_or_else(|| {
        let mut error = SqlError::new("datatype-mismatch", "datatype mismatch").in_statement("select");
        error.clause = Some(clause.to_string());
        error
    })
}

fn real_integer(value: f64) -> Option<i64> {
    if value.is_finite() && value.fract() == 0.0 && value >= -9223372036854775808.0 && value < 9223372036854775808.0 {
        Some(value as i64)
    }
    else {
        None
    }
}

/// Clamps a limit pair to the rows actually available.
fn limit_range(len: usize, limit: Option<(usize, Option<usize>)>) -> (usize, usize) {
    match limit {
        None => (0, len),
        Some((offset, count)) => {
            let start = offset.min(len);
            let end = match count {
                Some(count) => start.saturating_add(count).min(len),
                None => len,
            };
            (start, end)
        }
    }
}

/// Statement name reported for one transaction action, matching
/// [`Statement::name`].
fn action_name(action: &TransactionAction) -> &'static str {
    match action {
        TransactionAction::Begin => "begin",
        TransactionAction::Commit => "commit",
        TransactionAction::Rollback => "rollback",
        TransactionAction::Savepoint(_) => "savepoint",
        TransactionAction::Release(_) => "release",
        TransactionAction::RollbackTo(_) => "rollback-to",
    }
}

/// Drops the rows a limit pair hides.
fn truncate_rows(rows: &mut Vec<Vec<Cell>>, limit: Option<(usize, Option<usize>)>) {
    if let Some(limit) = limit {
        let (start, end) = limit_range(rows.len(), Some(limit));
        rows.truncate(end);
        rows.drain(..start);
    }
}

/// Keeps the first row for every distinct output value, preserving order.
///
/// Values are compared with `Cell`'s own equality, the same semantics grouping
/// uses, so integers and reals stay distinct exactly as SQLite keeps them.
fn retain_first_distinct(rows: Vec<Vec<Cell>>, collations: &[Collation]) -> Vec<Vec<Cell>> {
    // Single-column output needs one signature, not a `Vec<ProbeKey>` per row.
    if rows.first().is_some_and(|row| row.len() == 1) {
        let collation = collations.first().copied().unwrap_or_default();
        let mut buckets: HashSet<ProbeKey> = HashSet::with_capacity(rows.len());
        // 保留下标而不是整行副本:线性回退时直接与已保留的行比较,
        // 命中哈希的常见路径不再为每行复制一份 `Vec<Cell>`。
        let mut seen: Vec<usize> = Vec::with_capacity(rows.len());
        let mut out: Vec<Vec<Cell>> = Vec::with_capacity(rows.len());
        let mut linear = false;
        for row in rows {
            let signature = if linear {
                None
            }
            else {
                match probe_key(&row[0], collation) {
                    Some(part) if !matches!(part, ProbeKey::Wide(_)) => Some(part),
                    _ => None,
                }
            };
            let duplicate = match &signature {
                Some(signature) => buckets.contains(signature),
                None => {
                    linear = true;
                    seen.iter()
                        .any(|index| compare_cells_with(&out[*index][0], &row[0], collation) == Ordering::Equal)
                }
            };
            if !duplicate {
                if let Some(signature) = signature {
                    buckets.insert(signature);
                }
                seen.push(out.len());
                out.push(row);
            }
        }
        return out;
    }
    // Canonical signatures give a hash lookup for the common case; once a row
    // has no signature, the rest of the run falls back to the engine's linear
    // comparison so semantics never change.
    let mut buckets: HashSet<Vec<ProbeKey>> = HashSet::with_capacity(rows.len());
    // 保留下标而不是整行副本,避免哈希命中路径上每行多一次行克隆。
    let mut seen: Vec<usize> = Vec::with_capacity(rows.len());
    let mut out: Vec<Vec<Cell>> = Vec::with_capacity(rows.len());
    let mut linear = false;
    for row in rows {
        let signature = if linear { None } else { row_signature(&row, collations) };
        let duplicate = match &signature {
            Some(signature) => buckets.contains(signature),
            None => {
                linear = true;
                seen.iter().any(|index| rows_equal_with(&out[*index], &row, collations))
            }
        };
        if !duplicate {
            if let Some(signature) = signature {
                buckets.insert(signature);
            }
            seen.push(out.len());
            out.push(row);
        }
    }
    out
}

/// Computes one whole-result aggregate over the filtered rows.
///
/// Empty input follows SQLite: `count` is `0`, every other aggregate is `NULL`.
fn compute_aggregate(spec: &AggregateSpec, rows: &[Vec<Cell>], ctx: &EvalContext) -> Result<Cell, SqlError> {
    // `count(*)` 只数行,不需要为每行物化一个占位值。
    if !spec.distinct && spec.order.is_empty() && matches!(spec.function, AggregateFunction::CountStar) {
        let count = match &spec.filter {
            None => rows.len(),
            Some(filter) => {
                let mut count = 0usize;
                for row in rows {
                    if truthiness(&filter.eval(row, ctx)?) == Some(true) {
                        count += 1;
                    }
                }
                count
            }
        };
        return Ok(Cell::Integer(count as i64));
    }
    // `min` / `max` 逐行折叠即可,不必先把整列参数收进一个向量。
    if !spec.distinct && spec.order.is_empty() && spec.separator.is_none() && matches!(spec.function, AggregateFunction::Min | AggregateFunction::Max) {
        let Some(argument) = &spec.argument
        else {
            return Ok(Cell::Null);
        };
        let function = spec.function;
        let mut best: Option<Cell> = None;
        for row in rows {
            if let Some(filter) = &spec.filter {
                if truthiness(&filter.eval(row, ctx)?) != Some(true) {
                    continue;
                }
            }
            let value = argument.eval(row, ctx)?;
            if matches!(value, Cell::Null) {
                continue;
            }
            best = Some(match best {
                None => value,
                Some(current) => {
                    let ordering = compare_cells(&value, &current);
                    let replace = match function {
                        AggregateFunction::Min => ordering == Ordering::Less,
                        AggregateFunction::Max => ordering == Ordering::Greater,
                        _ => false,
                    };
                    if replace { value } else { current }
                }
            });
        }
        return Ok(best.unwrap_or(Cell::Null));
    }
    // 无 `DISTINCT`、无参数内 `ORDER BY` 时不需要物化 (value, separator, key)
    // 三元组,直接按行收集参数值,省掉每组一次分配与二次装载。
    if !spec.distinct && spec.order.is_empty() {
        let mut values = Vec::with_capacity(rows.len());
        let mut separators = if spec.separator.is_some() { Vec::with_capacity(rows.len()) } else { Vec::new() };
        for row in rows {
            if let Some(filter) = &spec.filter {
                if truthiness(&filter.eval(row, ctx)?) != Some(true) {
                    continue;
                }
            }
            let value = match &spec.argument {
                Some(argument) => argument.eval(row, ctx)?,
                None => Cell::Null,
            };
            if let Some(separator) = &spec.separator {
                separators.push(separator.eval(row, ctx)?);
            }
            values.push(value);
        }
        return Ok(aggregate_cells(spec.function, &values, if spec.separator.is_some() { Some(&separators) } else { None }));
    }
    let mut entries: Vec<(Cell, Cell, Vec<Cell>)> = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(filter) = &spec.filter {
            if truthiness(&filter.eval(row, ctx)?) != Some(true) {
                continue;
            }
        }
        let value = match &spec.argument {
            Some(argument) => argument.eval(row, ctx)?,
            None => Cell::Null,
        };
        let separator = match &spec.separator {
            Some(separator) => separator.eval(row, ctx)?,
            None => Cell::Null,
        };
        let keys = spec.order.iter().map(|(expr, _)| expr.eval(row, ctx)).collect::<Result<Vec<_>, SqlError>>()?;
        entries.push((value, separator, keys));
    }
    let (values, separators) = shape_aggregate_inputs(entries, spec.distinct, &spec.order, spec.collation);
    Ok(aggregate_cells(spec.function, &values, if spec.separator.is_some() { Some(&separators) } else { None }))
}

/// Applies `DISTINCT` and the in-argument `ORDER BY` to one aggregate's input.
///
/// `DISTINCT` sorts the values first so equal ones sit together, drops the
/// duplicates, then the explicit `ORDER BY` (when present) re-sorts the
/// survivors. Without `DISTINCT` only the explicit order applies, and it is
/// stable so equal keys keep the row order SQLite would keep.
fn shape_aggregate_inputs(
    mut entries: Vec<(Cell, Cell, Vec<Cell>)>,
    distinct: bool,
    order: &[(ResolvedExpr, SortDirection)],
    collation: Collation,
) -> (Vec<Cell>, Vec<Cell>) {
    let directions: Vec<SortDirection> = order.iter().map(|(_, direction)| *direction).collect();
    if distinct {
        entries.sort_by(|left, right| compare_cells_with(&left.0, &right.0, collation));
        entries.dedup_by(|left, right| compare_cells_with(&left.0, &right.0, collation) == Ordering::Equal);
        if !order.is_empty() {
            entries.sort_by(|left, right| compare_ordered(&left.2, &right.2, &directions));
        }
    }
    else if !order.is_empty() {
        entries.sort_by(|left, right| compare_ordered(&left.2, &right.2, &directions));
    }
    let mut values = Vec::with_capacity(entries.len());
    let mut separators = Vec::with_capacity(entries.len());
    for (value, separator, _) in entries {
        values.push(value);
        separators.push(separator);
    }
    (values, separators)
}

/// Aggregates already evaluated argument values.
///
/// Shared by whole-result aggregates and window frames so both follow one rule
/// for empty input, integer overflow and text coercion.
fn aggregate_cells(function: AggregateFunction, values: &[Cell], separators: Option<&[Cell]>) -> Cell {
    match function {
        AggregateFunction::CountStar => Cell::Integer(values.len() as i64),
        AggregateFunction::Count => Cell::Integer(values.iter().filter(|value| !matches!(value, Cell::Null)).count() as i64),
        AggregateFunction::Avg => {
            let mut total = 0.0f64;
            let mut count = 0u64;
            for value in values {
                if let Some(number) = numeric(value) {
                    total += number_f64(number);
                    count += 1;
                }
            }
            if count == 0 { Cell::Null } else { Cell::Real(total / count as f64) }
        }
        AggregateFunction::Sum => {
            // Stay integral only while every input is an INTEGER *storage*
            // class; text and blobs that convert to whole numbers still make
            // the result a real, exactly like `sqlite3_value_type`.
            let mut integral: Option<i64> = Some(0);
            let mut real = 0.0f64;
            let mut seen = false;
            for cell in values {
                let Some(number) = numeric(cell)
                else {
                    continue;
                };
                seen = true;
                match (cell, number) {
                    (Cell::Integer(_), Number::Int(value)) => match integral {
                        Some(sum) => match sum.checked_add(value) {
                            Some(sum) => integral = Some(sum),
                            None => {
                                real = sum as f64 + value as f64;
                                integral = None;
                            }
                        },
                        None => real += value as f64,
                    },
                    (_, number) => match integral.take() {
                        Some(sum) => real = sum as f64 + number_f64(number),
                        None => real += number_f64(number),
                    },
                }
            }
            if !seen {
                Cell::Null
            }
            else {
                match integral {
                    Some(value) => Cell::Integer(value),
                    None => Cell::Real(real),
                }
            }
        }
        // `total()` is `sum()` in real mode, except that an empty input is 0.0.
        AggregateFunction::Total => {
            let mut real = 0.0f64;
            for value in values {
                if let Some(number) = numeric(value) {
                    real += number_f64(number);
                }
            }
            Cell::Real(real)
        }
        // `group_concat` joins the text forms in input order, skips `NULL`,
        // uses `,` when the separator argument is absent, and yields `NULL`
        // when nothing is left to join.
        AggregateFunction::GroupConcat => {
            let mut joined = String::new();
            let mut seen = false;
            for (index, value) in values.iter().enumerate() {
                let Some(value) = text_form(value)
                else {
                    continue;
                };
                if seen {
                    let separator = match separators {
                        // A present but NULL separator acts like an empty one.
                        Some(separators) => separators.get(index).and_then(text_form).unwrap_or_default(),
                        None => ",".to_string(),
                    };
                    joined.push_str(&separator);
                }
                joined.push_str(&value);
                seen = true;
            }
            if seen { Cell::Text(joined) } else { Cell::Null }
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let mut best: Option<Cell> = None;
            for value in values {
                if matches!(value, Cell::Null) {
                    continue;
                }
                best = Some(match best {
                    None => value.clone(),
                    Some(current) => {
                        let ordering = compare_cells(value, &current);
                        let replace = match function {
                            AggregateFunction::Min => ordering == Ordering::Less,
                            AggregateFunction::Max => ordering == Ordering::Greater,
                            _ => false,
                        };
                        if replace { value.clone() } else { current }
                    }
                });
            }
            best.unwrap_or(Cell::Null)
        }
    }
}

/// Partitions the filtered rows into `GROUP BY` buckets.
///
/// With no keys every row forms one bucket, and an empty input still yields one
/// empty bucket so that whole-result aggregates can report their empty-input
/// values. Group equality uses the same `NULL == NULL` rule as set operations.
fn build_groups(
    group_keys: &[GroupKey],
    projection: &Option<Vec<ResolvedExpr>>,
    rows: Vec<Vec<Cell>>,
    ctx: &EvalContext,
    budget: &Budget,
) -> Result<Vec<Vec<Vec<Cell>>>, SqlError> {
    if group_keys.is_empty() {
        return Ok(vec![rows]);
    }
    // 单键分组不需要每行一份 `Vec<ProbeKey>`:一个键签名就足以定位分组,
    // 桶里直接存首次出现的分组下标。分桶与线性回退的判定和多键路径一致。
    if group_keys.len() == 1 {
        let group_key = &group_keys[0];
        let collation = match group_key {
            GroupKey::Projected(_, collation) | GroupKey::Expression(_, collation) => *collation,
        };
        let mut groups: Vec<(Cell, Vec<Vec<Cell>>)> = Vec::new();
        let mut buckets: HashMap<ProbeKey, usize> = HashMap::new();
        let mut linear = false;
        for row in rows {
            let value = match group_key {
                GroupKey::Projected(index, _) => match projection {
                    Some(nodes) => match nodes.get(*index) {
                        Some(node) => node.eval(&row, ctx)?,
                        None => Cell::Null,
                    },
                    None => row.get(*index).cloned().unwrap_or(Cell::Null),
                },
                GroupKey::Expression(expr, _) => expr.eval(&row, ctx)?,
            };
            let signature = if linear {
                None
            }
            else {
                match probe_key(&value, collation) {
                    Some(part) if !matches!(part, ProbeKey::Wide(_)) => Some(part),
                    _ => None,
                }
            };
            let matched = match &signature {
                Some(signature) => buckets.get(signature).copied(),
                None => {
                    linear = true;
                    groups.iter().position(|(existing, _)| compare_cells_with(existing, &value, collation) == Ordering::Equal)
                }
            };
            match matched {
                Some(index) => {
                    budget.charge(1)?;
                    groups[index].1.push(row);
                }
                None => {
                    budget.charge(1)?;
                    let index = groups.len();
                    if let Some(signature) = signature {
                        buckets.insert(signature, index);
                    }
                    groups.push((value, vec![row]));
                }
            }
        }
        return Ok(groups.into_iter().map(|(_, rows)| rows).collect());
    }
    let mut groups: Vec<(Vec<Cell>, Vec<Vec<Cell>>)> = Vec::new();
    // Hash buckets over the canonical key signatures, so a row finds its
    // group in one probe instead of a linear scan over every group built so
    // far. Two keys land in one bucket exactly when they compare equal, and
    // within a bucket the first match in group-creation order is the same
    // first match the linear scan would have taken, so the groups and their
    // output order are unchanged.
    let mut buckets: HashMap<Vec<ProbeKey>, Vec<usize>> = HashMap::new();
    // A key the canonical form cannot hash (a NaN, which compares equal to
    // every numeric, or a wide integer) switches the rest of the build to the
    // linear scan, which reproduces the engine's comparison exactly.
    let mut linear = false;
    for row in rows {
        let key = group_keys
            .iter()
            .map(|key| match key {
                GroupKey::Projected(index, _) => match projection {
                    Some(nodes) => match nodes.get(*index) {
                        Some(node) => node.eval(&row, ctx),
                        None => Ok(Cell::Null),
                    },
                    None => Ok(row.get(*index).cloned().unwrap_or(Cell::Null)),
                },
                GroupKey::Expression(expr, _) => expr.eval(&row, ctx),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let signature = if linear { None } else { group_signature(&key, group_keys) };
        let matched = match &signature {
            Some(signature) => buckets
                .get(signature)
                .and_then(|candidates| candidates.iter().find(|index| group_keys_equal(&groups[**index].0, &key, group_keys)))
                .copied(),
            None => {
                linear = true;
                groups.iter().position(|(existing, _)| group_keys_equal(existing, &key, group_keys))
            }
        };
        match matched {
            Some(index) => {
                budget.charge(1)?;
                groups[index].1.push(row);
            }
            None => {
                budget.charge(1)?;
                let index = groups.len();
                if let Some(signature) = signature {
                    buckets.entry(signature).or_default().push(index);
                }
                groups.push((key, vec![row]));
            }
        }
    }
    Ok(groups.into_iter().map(|(_, rows)| rows).collect())
}

/// Canonical hash signature of one group key, or `None` when a component is
/// outside the canonical form and the row has to go through the linear scan.
fn group_signature(key: &[Cell], group_keys: &[GroupKey]) -> Option<Vec<ProbeKey>> {
    key
        .iter()
        .zip(group_keys)
        .map(|(cell, group_key)| {
            let collation = match group_key {
                GroupKey::Projected(_, collation) | GroupKey::Expression(_, collation) => *collation,
            };
            match probe_key(cell, collation) {
                Some(part) if !matches!(part, ProbeKey::Wide(_)) => Some(part),
                _ => None,
            }
        })
        .collect()
}

/// The engine's group equality: every position compares equal under its key's
/// collation, exactly as the linear scan decided.
fn group_keys_equal(left: &[Cell], right: &[Cell], group_keys: &[GroupKey]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .zip(group_keys)
            .all(|((existing, key), group_key)| {
                let collation = match group_key {
                    GroupKey::Projected(_, collation) | GroupKey::Expression(_, collation) => *collation,
                };
                compare_cells_with(existing, key, collation) == Ordering::Equal
            })
}

/// 统计普通源与内连接的匹配行数,不物化匹配行。
///
/// 与 [`join_rows`] 的通用路径逐行等价:按 `FROM` 顺序绑定,`ON` 在候选行上
/// 求值,`WHERE` 合取在完整行上求值。只有 `rowid` 等值探针借用右表的 rowid
/// 升序做二分,其余形态退回逐行扫描,因而不会改变计数结果。
fn count_inner_join_rows(
    relation: &Relation,
    ons: &[Option<ResolvedExpr>],
    tables: &[Rc<Vec<Vec<Cell>>>],
    conjuncts: &[ResolvedExpr],
    ctx: &EvalContext,
) -> Result<usize, SqlError> {
    // 探针形态与右表有序性每条语句只判定一次,避免在每行递归里重算。
    let probes: Vec<Option<RowidProbe<'_>>> = relation
        .sources
        .iter()
        .enumerate()
        .map(|(index, source)| {
            Ok(rowid_join_probe(source, &ons[index], index).map(|(right_slot, probe)| {
                let within = right_slot - source.offset;
                let ascending = tables[index].windows(2).all(|pair| match (&pair[0][within], &pair[1][within]) {
                    (Cell::Integer(left), Cell::Integer(right)) => left < right,
                    _ => false,
                });
                RowidProbe { within, probe, ascending }
            }))
        })
        .collect::<Result<Vec<_>, SqlError>>()?;
    let mut merged = vec![Cell::Null; relation.columns()];
    let mut count = 0usize;
    count_join_level(0, relation, ons, tables, &probes, conjuncts, ctx, &mut merged, &mut count)?;
    Ok(count)
}

/// 一条 `ON <右侧 rowid> = <前缀表达式>` 的等值探针,`ascending` 表示右表按该
/// rowid 列严格升序,可用二分定位。
struct RowidProbe<'a> {
    within: usize,
    probe: &'a ResolvedExpr,
    ascending: bool,
}

#[allow(clippy::too_many_arguments)]
fn count_join_level(
    index: usize,
    relation: &Relation,
    ons: &[Option<ResolvedExpr>],
    tables: &[Rc<Vec<Vec<Cell>>>],
    probes: &[Option<RowidProbe<'_>>],
    conjuncts: &[ResolvedExpr],
    ctx: &EvalContext,
    merged: &mut [Cell],
    count: &mut usize,
) -> Result<(), SqlError> {
    if index == relation.sources.len() {
        for conjunct in conjuncts {
            if truthiness(&conjunct.eval(merged, ctx)?) != Some(true) {
                return Ok(());
            }
        }
        ctx.budget.charge(1)?;
        *count += 1;
        return Ok(());
    }
    let source = &relation.sources[index];
    let start = source.offset;
    let end = start + source.width();
    // 第一项与后续无 `ON` 的内连接都是无条件乘积。
    let Some(on) = ons[index].as_ref()
    else {
        for values in tables[index].iter() {
            merged[start..end].clone_from_slice(values);
            count_join_level(index + 1, relation, ons, tables, probes, conjuncts, ctx, merged, count)?;
        }
        return Ok(());
    };
    if let Some(rowid) = &probes[index] {
        let right = tables[index].as_slice();
        if let Cell::Integer(key) = rowid.probe.eval(merged, ctx)? {
            if rowid.ascending {
                let within = rowid.within;
                let position = right.partition_point(|row| matches!(&row[within], Cell::Integer(value) if *value < key));
                if let Some(row) = right.get(position) {
                    if matches!(&row[within], Cell::Integer(value) if *value == key) {
                        // The probe is the whole `ON` equality: an integer
                        // rowid equal to the integer the probe produced makes
                        // it true without re-evaluating the comparison.
                        merged[start..end].clone_from_slice(row);
                        count_join_level(index + 1, relation, ons, tables, probes, conjuncts, ctx, merged, count)?;
                    }
                }
                return Ok(());
            }
            let within = rowid.within;
            for row in right {
                if matches!(&row[within], Cell::Integer(value) if *value == key) {
                    merged[start..end].clone_from_slice(row);
                    count_join_level(index + 1, relation, ons, tables, probes, conjuncts, ctx, merged, count)?;
                }
            }
            return Ok(());
        }
    }
    for row in tables[index].iter() {
        merged[start..end].clone_from_slice(row);
        if truthiness(&on.eval(merged, ctx)?) == Some(true) {
            count_join_level(index + 1, relation, ons, tables, probes, conjuncts, ctx, merged, count)?;
        }
    }
    Ok(())
}

/// Builds joined rows for a `FROM` clause that uses `JOIN`.
///
/// Sources are added left to right, and rows come out in left-major order, as
/// SQLite emits them. An inner join keeps only pairs whose predicate is SQL
/// `TRUE`; a left join keeps the left row with `NULL` on the right when no
/// pair matches; a right join drops unmatched left rows but appends unmatched
/// right rows padded with `NULL` on the left; a full join keeps both. `USING`
/// and `NATURAL` columns are matched by equality and written into a coalesced
/// slot, so the shared column exists once without losing either side's raw
/// value. Comma and `CROSS JOIN` items stay plain products.
fn join_rows(
    relation: &Relation,
    ons: &[Option<ResolvedExpr>],
    tables: &[Rc<Vec<Vec<Cell>>>],
    conjuncts: &[ResolvedExpr],
    take: usize,
    ctx: &EvalContext,
) -> Result<Vec<Vec<Cell>>, SqlError> {
    let mut rows: Vec<Vec<Cell>> = vec![vec![Cell::Null; relation.columns()]];
    for (index, source) in relation.sources.iter().enumerate() {
        let width = source.width();
        let pairs = &relation.using[index];
        // No predicate means every pair matches, which SQLite treats as a
        // product regardless of the join keyword.
        let unconditional = ons[index].is_none() && pairs.is_empty();
        let kind = source.join.unwrap_or(JoinKind::Inner);
        let mut next = Vec::new();
        // Fast path for the common `INNER JOIN ... ON <right>.<rowid> = <expr
        // over the left>` shape: index the right side by rowid once and probe
        // per left row instead of rescanning every right row for every left
        // row. The full `ON` still decides each candidate and any non-integer
        // probe falls back to the nested scan, so the emitted rows and their
        // left-major order are identical to the generic path below.
        let fast =
            if unconditional || !pairs.is_empty() || !matches!(kind, JoinKind::Inner) { None } else { rowid_join_probe(source, &ons[index], index) };
        if unconditional {
            for left in &rows {
                for values in tables[index].iter() {
                    let mut merged = left.clone();
                    merged[source.offset..source.offset + width].clone_from_slice(values);
                    ctx.budget.charge(1)?;
                    next.push(merged);
                }
            }
        }
        else if let Some((right_slot, probe)) = fast {
            let within = right_slot - source.offset;
            let mut by_rowid: HashMap<i64, Vec<usize>> = HashMap::new();
            for (position, row) in tables[index].iter().enumerate() {
                if let Cell::Integer(key) = &row[within] {
                    by_rowid.entry(*key).or_default().push(position);
                }
            }
            let on = ons[index].as_ref().expect("the probe only returns for a present ON");
            for left in &rows {
                match probe.eval(left, ctx)? {
                    Cell::Integer(key) => {
                        if let Some(positions) = by_rowid.get(&key) {
                            for &position in positions {
                                let mut merged = left.clone();
                                merged[source.offset..source.offset + width].clone_from_slice(&tables[index][position]);
                                // The probe is the whole `ON` equality and the
                                // matched rowid is the same integer, so the
                                // comparison is already true.
                                ctx.budget.charge(1)?;
                                next.push(merged);
                            }
                        }
                    }
                    // A non-integer probe could still equal an integer rowid
                    // under SQLite's affinity rules, so keep the nested scan for
                    // this left row to guarantee an identical result.
                    _ => {
                        for values in tables[index].iter() {
                            let mut merged = left.clone();
                            merged[source.offset..source.offset + width].clone_from_slice(values);
                            if truthiness(&on.eval(&merged, ctx)?) == Some(true) {
                                ctx.budget.charge(1)?;
                                next.push(merged);
                            }
                        }
                    }
                }
            }
        }
        else {
            let mut matched_right = vec![false; tables[index].len()];
            for left in &rows {
                let mut matched = false;
                for (position, values) in tables[index].iter().enumerate() {
                    let mut merged = left.clone();
                    merged[source.offset..source.offset + width].clone_from_slice(values);
                    coalesce_using(&mut merged, pairs);
                    let keep = match &ons[index] {
                        None => pairs.iter().all(|pair| {
                            let collation = relation.column_collation(pair.left);
                            compare_values(&merged[pair.left], &merged[pair.right], CompareOp::Eq, collation) == Some(true)
                        }),
                        Some(on) => truthiness(&on.eval(&merged, ctx)?) == Some(true),
                    };
                    if keep {
                        ctx.budget.charge(1)?;
                        next.push(merged);
                        matched = true;
                        matched_right[position] = true;
                    }
                }
                if !matched && matches!(kind, JoinKind::Left | JoinKind::Full) {
                    // The left row already carries NULLs for this source's slice.
                    let mut padded = left.clone();
                    coalesce_using(&mut padded, pairs);
                    ctx.budget.charge(1)?;
                    next.push(padded);
                }
            }
            if matches!(kind, JoinKind::Right | JoinKind::Full) {
                // Unmatched right rows follow every pair, padded on the left.
                for (position, values) in tables[index].iter().enumerate() {
                    if !matched_right[position] {
                        let mut merged = vec![Cell::Null; relation.columns()];
                        merged[source.offset..source.offset + width].clone_from_slice(values);
                        coalesce_using(&mut merged, pairs);
                        ctx.budget.charge(1)?;
                        next.push(merged);
                    }
                }
            }
        }
        rows = next;
    }
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut keep = true;
        for conjunct in conjuncts {
            if truthiness(&conjunct.eval(&row, ctx)?) != Some(true) {
                keep = false;
                break;
            }
        }
        if keep {
            ctx.budget.charge(1)?;
            out.push(row);
            if out.len() >= take {
                break;
            }
        }
    }
    Ok(out)
}

/// Fills one row's coalesced `USING` slots: only an outer join can make the
/// left and right values differ, because unmatched rows arrive `NULL`-padded
/// on the missing side.
fn coalesce_using(row: &mut [Cell], pairs: &[UsingJoin]) {
    for pair in pairs {
        let left = row[pair.left].clone();
        row[pair.slot] = if matches!(left, Cell::Null) { row[pair.right].clone() } else { left };
    }
}

/// Detects an inner-join `ON` that is a single equality between one base
/// rowid table's rowid (its rowid-alias column or the implicit `rowid`) and an
/// expression over strictly earlier sources. Returns the merged-row slot that
/// holds that rowid and the probe expression, or `None` for any shape the hash
/// fast path cannot prove equivalent to the nested scan.
fn rowid_join_probe<'a>(source: &Source, on: &'a Option<ResolvedExpr>, index: usize) -> Option<(usize, &'a ResolvedExpr)> {
    if !source.columns.hidden_rowid() {
        return None;
    }
    let def = match &source.columns {
        SourceColumns::Table(def) => *def,
        SourceColumns::Materialized { .. } => return None,
    };
    if def.without_rowid {
        return None;
    }
    let alias_slot = def.rowid_alias.map(|alias| source.offset + alias);
    let hidden_slot = source.offset + def.columns.len();
    let on = on.as_ref()?;
    let (slot, probe) = match on {
        ResolvedExpr::Compare { left, op: CompareOp::Eq, right, .. } => match (left.as_ref(), right.as_ref()) {
            (ResolvedExpr::Column { source: owner, index: slot }, other) if *owner == index && (*slot == hidden_slot || alias_slot == Some(*slot)) => {
                (*slot, other)
            }
            (other, ResolvedExpr::Column { source: owner, index: slot }) if *owner == index && (*slot == hidden_slot || alias_slot == Some(*slot)) => {
                (*slot, other)
            }
            _ => return None,
        },
        _ => return None,
    };
    if probe.contains_subquery() {
        return None;
    }
    let mut refs = Vec::new();
    probe.collect_sources(&mut refs);
    if refs.iter().any(|owner| *owner >= index) {
        return None;
    }
    Some((slot, probe))
}

/// Materializes the Cartesian product of the `UPDATE ... FROM` sources.
/// Every source row already carries its hidden rowid slot, so concatenating
/// them in `FROM` order reproduces the merged row the relation resolves
/// against.
fn join_product(tables: &[Rc<Vec<Vec<Cell>>>]) -> Vec<Vec<Cell>> {
    let mut product: Vec<Vec<Cell>> = vec![Vec::new()];
    for rows in tables {
        let mut next = Vec::with_capacity(product.len() * rows.len());
        for prefix in &product {
            for row in rows.iter() {
                let mut merged = prefix.clone();
                merged.extend(row.iter().cloned());
                next.push(merged);
            }
        }
        product = next;
    }
    product
}

/// Flattens `a AND b AND c` into single conjuncts, each applied as soon as its
/// sources are bound.
fn split_conjuncts(predicate: ResolvedExpr, out: &mut Vec<ResolvedExpr>) {
    match predicate {
        ResolvedExpr::And(left, right) => {
            split_conjuncts(*left, out);
            split_conjuncts(*right, out);
        }
        other => out.push(other),
    }
}

/// Orders the sources so predicates can prune as early as possible.
///
/// Starting from the source with the most single-source conjuncts, each step
/// binds the source that completes the most predicates with the set already
/// bound. Without this an equality chain over twenty tables would enumerate the
/// whole product before any comparison applied.
fn binding_order(relation: &Relation, conjuncts: &[ResolvedExpr]) -> Vec<usize> {
    let refs: Vec<Vec<usize>> = conjuncts
        .iter()
        .map(|conjunct| {
            let mut sources = Vec::new();
            conjunct.collect_sources(&mut sources);
            sources.sort_unstable();
            sources.dedup();
            sources
        })
        .collect();
    let count = relation.sources.len();
    let mut order = Vec::with_capacity(count);
    let mut bound = vec![false; count];
    while order.len() < count {
        let mut best = None;
        for candidate in 0..count {
            if bound[candidate] {
                continue;
            }
            let applicable = refs
                .iter()
                .filter(|sources| !sources.is_empty() && sources.iter().all(|source| *source == candidate || bound[*source]))
                .count();
            if best.map_or(true, |(score, _)| applicable > score) {
                best = Some((applicable, candidate));
            }
        }
        let (_, source) = best.expect("an unbound source always remains");
        bound[source] = true;
        order.push(source);
    }
    order
}

/// Emits the Cartesian product, pruning a partial combination as soon as one
/// conjunct over the bound sources is false.
///
/// Sources are bound in `binding_order`; the merged row keeps `FROM` column
/// order throughout. A conjunct that contains a subquery cannot be attributed
/// to one source, so it waits for the complete row instead of pruning early.
/// Collects the `AND` conjuncts of the form `<column> = <literal>` (or the
/// mirrored shape) that name a column of one base table, as `(position, cell)`
/// pairs. A bound parameter counts as a literal once its value is known.
/// Anything outside a plain `AND` of `=` comparisons contributes nothing, so a
/// missed equality only costs the pushdown, never a row.
fn collect_index_equalities(expr: &Expr, def: &TableDef, label: &str, parameters: &[Cell], out: &mut Vec<(usize, Cell)>) {
    match expr {
        Expr::And(left, right) => {
            collect_index_equalities(left, def, label, parameters, out);
            collect_index_equalities(right, def, label, parameters, out);
        }
        Expr::Compare { left, op, right } if *op == CompareOp::Eq => {
            let (column, value) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), value) => (column, known_cell(value, parameters)),
                (value, Expr::Column(column)) => (column, known_cell(value, parameters)),
                _ => return,
            };
            if column.qualifier.as_deref().is_some_and(|qualifier| !qualifier.eq_ignore_ascii_case(label)) {
                return;
            }
            if let Some(cell) = value
                && let Some(position) = def.columns.iter().position(|entry| entry.name.eq_ignore_ascii_case(&column.name))
            {
                out.push((position, cell));
            }
        }
        _ => {}
    }
}

/// Collects the lower and upper bounds of an `AND` conjunction on one column.
///
/// Only a plain `AND` of `<column> <op> <literal>` (or the mirrored shape)
/// contributes, where a bound parameter counts as a literal once its value is
/// known; any other shape is ignored, so a missed bound only costs the
/// pushdown. When several bounds name the same side, the last one wins, which
/// stays a superset of the true result because the full predicate still runs.
fn collect_range_bounds(
    expr: &Expr,
    def: &TableDef,
    label: &str,
    position: usize,
    parameters: &[Cell],
    lower: &mut Option<IndexBound>,
    upper: &mut Option<IndexBound>,
) {
    match expr {
        Expr::And(left, right) => {
            collect_range_bounds(left, def, label, position, parameters, lower, upper);
            collect_range_bounds(right, def, label, position, parameters, lower, upper);
        }
        Expr::Compare { left, op, right } => {
            let (column, value, flipped) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), value) => (column, known_cell(value, parameters), false),
                (value, Expr::Column(column)) => (column, known_cell(value, parameters), true),
                _ => return,
            };
            let Some(cell) = value
            else {
                return;
            };
            if column.qualifier.as_deref().is_some_and(|qualifier| !qualifier.eq_ignore_ascii_case(label)) {
                return;
            }
            if def.columns.get(position).is_none_or(|entry| !entry.name.eq_ignore_ascii_case(&column.name)) {
                return;
            }
            let op = if flipped { flip_range_op(*op) } else { *op };
            let bound = IndexBound { cell, inclusive: matches!(op, CompareOp::GtEq | CompareOp::LtEq) };
            match op {
                CompareOp::Gt | CompareOp::GtEq => *lower = Some(bound),
                CompareOp::Lt | CompareOp::LtEq => *upper = Some(bound),
                _ => {}
            }
        }
        _ => {}
    }
}

/// Mirrors a range comparison written with the literal on the left.
fn flip_range_op(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::GtEq => CompareOp::LtEq,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::LtEq => CompareOp::GtEq,
        other => other,
    }
}

/// 统计笛卡尔积中满足全部合取的行数,不物化结果行。
///
/// 绑定顺序与提前剪枝复用 [`cross_product`] 的同一套规则,因此计数与通用
/// 路径逐行一致,只是把最终结果行换成了计数。
fn count_cross_product_rows(
    tables: &[Rc<Vec<Vec<Cell>>>],
    relation: &Relation,
    conjuncts: &[ResolvedExpr],
    ctx: &EvalContext,
) -> Result<usize, SqlError> {
    let order = binding_order(relation, conjuncts);
    let mut position = vec![0usize; order.len()];
    for (slot, source) in order.iter().enumerate() {
        position[*source] = slot;
    }
    let mut refs = Vec::new();
    let depths: Vec<Option<usize>> = conjuncts
        .iter()
        .map(|conjunct| {
            if conjunct.contains_subquery() {
                return Some(order.len().saturating_sub(1));
            }
            refs.clear();
            conjunct.collect_sources(&mut refs);
            refs.iter().map(|source| position[*source]).max()
        })
        .collect();
    let mut merged = vec![Cell::Null; relation.columns()];
    for (conjunct, depth) in conjuncts.iter().zip(&depths) {
        if depth.is_none() && truthiness(&conjunct.eval(&merged, ctx)?) == Some(false) {
            return Ok(0);
        }
    }
    // A single source already occupies the whole merged row, so every conjunct
    // can read the stored row slice directly; skipping the per-row copy avoids
    // cloning text cells once per scanned row.
    if relation.sources.len() == 1 && tables.len() == 1 {
        let mut count = 0usize;
        for values in tables[0].iter() {
            let mut matched = true;
            for conjunct in conjuncts {
                if truthiness(&conjunct.eval(values, ctx)?) != Some(true) {
                    matched = false;
                    break;
                }
            }
            if matched {
                ctx.budget.charge(1)?;
                count += 1;
            }
        }
        return Ok(count);
    }
    let mut count = 0usize;
    count_cross_level(0, tables, relation, &order, conjuncts, &depths, ctx, &mut merged, &mut count)?;
    Ok(count)
}

#[allow(clippy::too_many_arguments)]
fn count_cross_level(
    slot: usize,
    tables: &[Rc<Vec<Vec<Cell>>>],
    relation: &Relation,
    order: &[usize],
    conjuncts: &[ResolvedExpr],
    depths: &[Option<usize>],
    ctx: &EvalContext,
    merged: &mut [Cell],
    count: &mut usize,
) -> Result<(), SqlError> {
    if slot == order.len() {
        for conjunct in conjuncts {
            if truthiness(&conjunct.eval(merged, ctx)?) != Some(true) {
                return Ok(());
            }
        }
        ctx.budget.charge(1)?;
        *count += 1;
        return Ok(());
    }
    let source = &relation.sources[order[slot]];
    for values in tables[order[slot]].iter() {
        merged[source.offset..source.offset + values.len()].clone_from_slice(values);
        let mut pruned = false;
        for (conjunct, depth) in conjuncts.iter().zip(depths) {
            if *depth == Some(slot) && truthiness(&conjunct.eval(merged, ctx)?) == Some(false) {
                pruned = true;
                break;
            }
        }
        if !pruned {
            count_cross_level(slot + 1, tables, relation, order, conjuncts, depths, ctx, merged, count)?;
        }
    }
    Ok(())
}

fn cross_product(
    tables: Vec<Rc<Vec<Vec<Cell>>>>,
    relation: &Relation,
    conjuncts: &[ResolvedExpr],
    take: usize,
    ctx: &EvalContext,
) -> Result<Vec<Vec<Cell>>, SqlError> {
    let order = binding_order(relation, conjuncts);
    let mut position = vec![0usize; order.len()];
    for (slot, source) in order.iter().enumerate() {
        position[*source] = slot;
    }
    // Depth of each conjunct in binding order; `None` means it is constant.
    let mut refs = Vec::new();
    let depths: Vec<Option<usize>> = conjuncts
        .iter()
        .map(|conjunct| {
            if conjunct.contains_subquery() {
                return Some(order.len().saturating_sub(1));
            }
            refs.clear();
            conjunct.collect_sources(&mut refs);
            refs.iter().map(|source| position[*source]).max()
        })
        .collect();
    let mut merged = vec![Cell::Null; relation.columns()];
    let mut rows = Vec::new();
    // Constant conjuncts decide the whole result once.
    for (conjunct, depth) in conjuncts.iter().zip(&depths) {
        if depth.is_none() && truthiness(&conjunct.eval(&merged, ctx)?) == Some(false) {
            return Ok(rows);
        }
    }
    // Same single-source elision as the count path: the stored row is already
    // the merged row, so predicates and the emitted row can use it directly.
    // 单源查询独占这份行集时直接按值消费,命中的行移动进结果而不是再复制一份;
    // 行集仍被其它源共享(自连接、同表子查询)时才退回复制。
    if relation.sources.len() == 1 && tables.len() == 1 {
        let stored = tables.into_iter().next().expect("length checked");
        let mut kept = Vec::with_capacity(stored.len().min(take));
        match Rc::try_unwrap(stored) {
            Ok(values) if conjuncts.is_empty() => {
                let count = values.len().min(take);
                for _ in 0..count {
                    ctx.budget.charge(1)?;
                }
                if count == values.len() {
                    kept = values;
                }
                else {
                    kept.extend(values.into_iter().take(count));
                }
            }
            Ok(values) => {
                for values in values {
                    if kept.len() >= take {
                        break;
                    }
                    let mut matched = true;
                    for conjunct in conjuncts {
                        if truthiness(&conjunct.eval(&values, ctx)?) != Some(true) {
                            matched = false;
                            break;
                        }
                    }
                    if matched {
                        ctx.budget.charge(1)?;
                        kept.push(values);
                    }
                }
            }
            Err(shared) => {
                for values in shared.iter() {
                    if kept.len() >= take {
                        break;
                    }
                    let mut matched = true;
                    for conjunct in conjuncts {
                        if truthiness(&conjunct.eval(values, ctx)?) != Some(true) {
                            matched = false;
                            break;
                        }
                    }
                    if matched {
                        ctx.budget.charge(1)?;
                        kept.push(values.clone());
                    }
                }
            }
        }
        return Ok(kept);
    }
    emit_rows(0, &tables, relation, &order, conjuncts, &depths, take, ctx, &mut merged, &mut rows)?;
    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
fn emit_rows(
    slot: usize,
    tables: &[Rc<Vec<Vec<Cell>>>],
    relation: &Relation,
    order: &[usize],
    conjuncts: &[ResolvedExpr],
    depths: &[Option<usize>],
    take: usize,
    ctx: &EvalContext,
    merged: &mut [Cell],
    rows: &mut Vec<Vec<Cell>>,
) -> Result<(), SqlError> {
    if rows.len() >= take {
        return Ok(());
    }
    if slot == order.len() {
        // Only rows whose whole conjunction is SQL `TRUE` survive, so a `NULL`
        // comparison at any level still rejects the row.
        for conjunct in conjuncts {
            if truthiness(&conjunct.eval(merged, ctx)?) != Some(true) {
                return Ok(());
            }
        }
        ctx.budget.charge(1)?;
        rows.push(merged.to_vec());
        return Ok(());
    }
    let source = &relation.sources[order[slot]];
    for values in tables[order[slot]].iter() {
        merged[source.offset..source.offset + values.len()].clone_from_slice(values);
        let mut pruned = false;
        for (conjunct, bound) in conjuncts.iter().zip(depths) {
            if *bound == Some(slot) && truthiness(&conjunct.eval(merged, ctx)?) == Some(false) {
                pruned = true;
                break;
            }
        }
        if !pruned {
            emit_rows(slot + 1, tables, relation, order, conjuncts, depths, take, ctx, merged, rows)?;
            if rows.len() >= take {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// 下推键的执行期取值：字面量，或绑定到该位置的参数。
///
/// 参数在语句执行前已经绑定，形态与字面量等价，所以把「仅字面量」放宽到「执行期
/// 已知的值」不改变结果：下推只收窄候选行，全量谓词照旧在下游执行。缺失的绑定
/// 退化为 `NULL`，既不会被当成可下推的整数键，也不会改变求值结果。
fn known_cell(expr: &Expr, parameters: &[Cell]) -> Option<Cell> {
    match expr {
        Expr::Literal(literal) => Some(literal_cell(literal)),
        Expr::Parameter { index, .. } => Some(parameters.get(*index).cloned().unwrap_or(Cell::Null)),
        _ => None,
    }
}

/// 把可精确表示整数的 REAL 折成 rowid 键，供等值下推使用。
///
/// 只接受有限、无小数部分且落在 `2^53` 内的值：这个范围内 f64 能精确表示每个
/// 整数，折算不会改变比较结果。超出范围或带小数时返回 `None`，调用方保持整表
/// 扫描，让通用比较逻辑按亲和规则处理。
fn integral_rowid(value: f64) -> Option<i64> {
    if !value.is_finite() || value.fract() != 0.0 || value.abs() > 9_007_199_254_740_992.0 {
        return None;
    }
    Some(value as i64)
}

fn literal_cell(literal: &Literal) -> Cell {
    match literal {
        Literal::Null => Cell::Null,
        Literal::Integer(value) => Cell::Integer(*value),
        Literal::Real(value) => Cell::Real(*value),
        Literal::Text(value) => Cell::Text(value.clone()),
        Literal::Blob(value) => Cell::Blob(value.clone()),
    }
}

/// SQLite storage-class order: `NULL` < numbers < text < blob.
fn storage_rank(cell: &Cell) -> u8 {
    match cell {
        Cell::Null => 0,
        Cell::Integer(_) | Cell::Real(_) => 1,
        Cell::Text(_) => 2,
        Cell::Blob(_) => 3,
    }
}

/// Compares two cells with the SQLite ordering rules.
///
/// Integers and reals compare numerically across the two storage classes, text
/// compares with `BINARY`, and blobs compare byte by byte.
fn compare_cells(left: &Cell, right: &Cell) -> Ordering {
    compare_cells_with(left, right, Collation::Binary)
}

/// 按给定排序规则比较两个单元格，文本以外的比较不受排序规则影响。
fn compare_cells_with(left: &Cell, right: &Cell, collation: Collation) -> Ordering {
    use Ordering::Equal;
    let (left_rank, right_rank) = (storage_rank(left), storage_rank(right));
    if left_rank != right_rank {
        return left_rank.cmp(&right_rank);
    }
    match (left, right) {
        (Cell::Null, Cell::Null) => Equal,
        (Cell::Integer(left), Cell::Integer(right)) => left.cmp(right),
        (Cell::Real(left), Cell::Real(right)) => left.partial_cmp(right).unwrap_or(Equal),
        (Cell::Integer(left), Cell::Real(right)) => (*left as f64).partial_cmp(right).unwrap_or(Equal),
        (Cell::Real(left), Cell::Integer(right)) => left.partial_cmp(&(*right as f64)).unwrap_or(Equal),
        (Cell::Text(left), Cell::Text(right)) => collation.compare_text(left, right),
        (Cell::Blob(left), Cell::Blob(right)) => left.cmp(right),
        _ => Equal,
    }
}

/// Applies one comparison operator. `None` means the result is SQL `NULL`.
fn compare_values(left: &Cell, right: &Cell, operator: CompareOp, collation: Collation) -> Option<bool> {
    use Ordering::{Equal, Greater, Less};
    if matches!(left, Cell::Null) || matches!(right, Cell::Null) {
        return None;
    }
    let ordering = compare_cells_with(left, right, collation);
    Some(match operator {
        CompareOp::Eq => ordering == Equal,
        CompareOp::NotEq => ordering != Equal,
        CompareOp::Lt => ordering == Less,
        CompareOp::LtEq => ordering != Greater,
        CompareOp::Gt => ordering == Greater,
        CompareOp::GtEq => ordering != Less,
    })
}

/// SQLite boolean conversion: `NULL` stays unknown, numbers are true when
/// non-zero, and text or blobs are converted numerically first (`'1'` and
/// `x'31'` are true, `'abc'` and `x'01'` are false).
fn truthiness(cell: &Cell) -> Option<bool> {
    match cell {
        Cell::Null => None,
        Cell::Integer(value) => Some(*value != 0),
        Cell::Real(value) => Some(*value != 0.0),
        Cell::Text(text) => Some(number_truthy(&text_number(text))),
        Cell::Blob(bytes) => Some(number_truthy(&text_number(&String::from_utf8_lossy(bytes)))),
    }
}

fn compare_key_rows(left: &[Cell], right: &[Cell], keys: &[(SortKey, SortDirection)]) -> Ordering {
    for (index, (_, direction)) in keys.iter().enumerate() {
        let ordering = direction.compare(&left[index], &right[index]);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

/// Compares two composite window sort keys, honouring each column's direction.
fn compare_ordered(left: &[Cell], right: &[Cell], directions: &[SortDirection]) -> Ordering {
    for (index, direction) in directions.iter().enumerate() {
        let ordering = direction.compare(&left[index], &right[index]);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

/// Numeric view of a cell used by arithmetic and the numeric functions.
#[derive(Clone, Copy)]
enum Number {
    /// 64-bit integer.
    Int(i64),
    /// Binary64 value.
    Real(f64),
}

/// SQLite numeric coercion: `NULL` has no numeric value, and text or blobs
/// contribute their longest numeric prefix (unparseable input becomes zero).
fn numeric(cell: &Cell) -> Option<Number> {
    match cell {
        Cell::Null => None,
        Cell::Integer(value) => Some(Number::Int(*value)),
        Cell::Real(value) => Some(Number::Real(*value)),
        Cell::Text(text) => Some(text_number(text)),
        // Blobs convert through their text form too, so `x'31' + 1` is 2.
        Cell::Blob(bytes) => Some(text_number(&String::from_utf8_lossy(bytes))),
    }
}

/// Numeric value of text: SQLite takes the longest numeric *prefix*
/// (`sqlite3AtoF`), so `'2abc'` is 2 and `'abc'` is 0.
fn text_number(text: &str) -> Number {
    let trimmed = text.trim_start();
    let bytes = trimmed.as_bytes();
    let mut index = 0;
    if matches!(bytes.first(), Some(b'+') | Some(b'-')) {
        index = 1;
    }
    let integer_start = index;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }
    let mut has_digits = index > integer_start;
    let mut is_real = false;
    if index < bytes.len() && bytes[index] == b'.' {
        is_real = true;
        index += 1;
        let fraction_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        has_digits |= index > fraction_start;
    }
    if !has_digits {
        return Number::Int(0);
    }
    // An exponent only counts when it is followed by at least one digit;
    // otherwise the number ends at the mantissa (`'1e'` is 1).
    let mut end = index;
    if index < bytes.len() && (bytes[index] == b'e' || bytes[index] == b'E') {
        let mut probe = index + 1;
        if probe < bytes.len() && matches!(bytes[probe], b'+' | b'-') {
            probe += 1;
        }
        let exponent_start = probe;
        while probe < bytes.len() && bytes[probe].is_ascii_digit() {
            probe += 1;
        }
        if probe > exponent_start {
            end = probe;
            is_real = true;
        }
    }
    let prefix = &trimmed[..end];
    if is_real {
        prefix.parse::<f64>().map(Number::Real).unwrap_or(Number::Int(0))
    }
    else {
        prefix.parse::<i64>().map(Number::Int).unwrap_or_else(|_| prefix.parse::<f64>().map(Number::Real).unwrap_or(Number::Int(0)))
    }
}

/// `CAST(expr AS type)`: SQLite converts between its five storage classes and
/// keeps `NULL` unchanged.
fn cast_cell(cell: &Cell, target: CastType) -> Cell {
    if matches!(cell, Cell::Null) {
        return Cell::Null;
    }
    match target {
        CastType::Integer => Cell::Integer(cast_integer(cell)),
        CastType::Real => Cell::Real(cast_real(cell)),
        CastType::Text => Cell::Text(cast_text(cell)),
        CastType::Blob => Cell::Blob(cast_text(cell).into_bytes()),
        CastType::Numeric => cast_numeric(cell),
    }
}

fn cast_integer(cell: &Cell) -> i64 {
    match cell {
        Cell::Integer(value) => *value,
        // SQLite truncates toward zero and saturates at the `i64` bounds.
        Cell::Real(value) => *value as i64,
        Cell::Text(text) => integer_prefix(text),
        // Blobs convert through their text form, so `cast(x'31' as integer)` is 1.
        Cell::Blob(bytes) => integer_prefix(&String::from_utf8_lossy(bytes)),
        Cell::Null => 0,
    }
}

/// Parses the leading integer prefix of `text`, stopping at the first
/// non-digit the way `sqlite3Atoi64` does. `'1e3'` therefore becomes `1`.
fn integer_prefix(text: &str) -> i64 {
    let trimmed = text.trim_start();
    let bytes = trimmed.as_bytes();
    let mut index = 0;
    let negative = match bytes.first() {
        Some(b'+') => {
            index = 1;
            false
        }
        Some(b'-') => {
            index = 1;
            true
        }
        _ => false,
    };
    let start = index;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }
    if index == start {
        return 0;
    }
    let magnitude = trimmed[start..index].parse::<i128>().unwrap_or(i128::MAX);
    let signed = if negative { -magnitude } else { magnitude };
    signed.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

fn cast_real(cell: &Cell) -> f64 {
    match cell {
        Cell::Integer(value) => *value as f64,
        Cell::Real(value) => *value,
        Cell::Text(text) => real_prefix(text),
        Cell::Blob(bytes) => real_prefix(&String::from_utf8_lossy(bytes)),
        Cell::Null => 0.0,
    }
}

/// Parses the leading real prefix of `text`, stopping at the first byte that
/// cannot be part of a number, the way `sqlite3AtoF` does.
fn real_prefix(text: &str) -> f64 {
    let trimmed = text.trim_start();
    let bytes = trimmed.as_bytes();
    let mut index = 0;
    if matches!(bytes.first(), Some(b'+') | Some(b'-')) {
        index = 1;
    }
    let start = index;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }
    let mut has_dot = false;
    if index < bytes.len() && bytes[index] == b'.' {
        has_dot = true;
        index += 1;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
    }
    if index == start && !has_dot {
        return 0.0;
    }
    let mut end = index;
    if index < bytes.len() && (bytes[index] == b'e' || bytes[index] == b'E') {
        let mut cursor = index + 1;
        if matches!(bytes.get(cursor), Some(b'+') | Some(b'-')) {
            cursor += 1;
        }
        let exponent_start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        if cursor > exponent_start {
            end = cursor;
        }
    }
    trimmed[..end].parse::<f64>().unwrap_or(0.0)
}

fn cast_text(cell: &Cell) -> String {
    match cell {
        Cell::Null => String::new(),
        Cell::Integer(value) => value.to_string(),
        Cell::Real(value) => sqlite_real_text(*value),
        Cell::Text(text) => text.clone(),
        Cell::Blob(bytes) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

fn cast_numeric(cell: &Cell) -> Cell {
    match cell {
        Cell::Integer(value) => Cell::Integer(*value),
        Cell::Real(value) => Cell::Real(*value),
        Cell::Text(text) => numeric_text(text),
        Cell::Blob(bytes) => numeric_text(&String::from_utf8_lossy(bytes)),
        Cell::Null => Cell::Integer(0),
    }
}

/// Numeric view of text for `CAST(... AS NUMERIC)`: the same prefix rule as
/// arithmetic, collapsed back to an integer when the value is integral.
fn numeric_text(text: &str) -> Cell {
    match text_number(text) {
        Number::Int(value) => Cell::Integer(value),
        Number::Real(value) if value.is_finite() && value.fract() == 0.0 && (i64::MIN as f64..=i64::MAX as f64).contains(&value) => {
            Cell::Integer(value as i64)
        }
        Number::Real(value) => Cell::Real(value),
    }
}

/// Text form shared by `LIKE` and `||`; `NULL` has none and short-circuits.
fn text_form(cell: &Cell) -> Option<String> {
    match cell {
        Cell::Null => None,
        Cell::Integer(value) => Some(value.to_string()),
        Cell::Real(value) => Some(sqlite_real_text(*value)),
        Cell::Text(text) => Some(text.clone()),
        Cell::Blob(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
    }
}

/// `||`: concatenates the text forms of both sides. `NULL` propagates and the
/// result is always `TEXT`, even when both operands are numbers.
fn concat(left: &Cell, right: &Cell) -> Cell {
    match (text_form(left), text_form(right)) {
        (Some(left), Some(right)) => Cell::Text(format!("{left}{right}")),
        _ => Cell::Null,
    }
}

/// Renders a real the way SQLite does with `%!.15g`: 15 significant digits,
/// always a decimal point, and an exponent only outside `[1e-4, 1e15)`.
fn sqlite_real_text(value: f64) -> String {
    if value == 0.0 {
        return "0.0".to_string();
    }
    // `{:.14e}` carries 15 significant digits and always prints an exponent.
    let scientific = format!("{value:.14e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((scientific.as_str(), "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let negative = mantissa.starts_with('-');
    let digits: String = mantissa.chars().filter(|character| character.is_ascii_digit()).collect();
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let mut text = if exponent < -4 || exponent >= 15 {
        let (head, tail) = digits.split_at(1);
        let tail = if tail.is_empty() { "0" } else { tail };
        format!("{head}.{tail}e{}{:02}", if exponent < 0 { '-' } else { '+' }, exponent.abs())
    }
    else {
        let point = exponent + 1;
        if point <= 0 {
            format!("0.{}{digits}", "0".repeat((-point) as usize))
        }
        else if point as usize >= digits.len() {
            format!("{digits}{}.0", "0".repeat(point as usize - digits.len()))
        }
        else {
            format!("{}.{}", &digits[..point as usize], &digits[point as usize..])
        }
    };
    if negative {
        text.insert(0, '-');
    }
    text
}

/// Matches one `GLOB` pattern against a value, one character at a time.
///
/// `*` matches any run including the empty one, `?` matches exactly one
/// character, and `[...]` is a class: `^` negates it, `a-c` is an inclusive
/// range, a leading `]` is a literal member, and an unclosed `[` never matches.
/// Unlike `LIKE`, every comparison is case sensitive.
fn glob_match(value: &[char], pattern: &[char]) -> bool {
    let Some((first, rest)) = pattern.split_first()
    else {
        return value.is_empty();
    };
    match *first {
        '*' => glob_match(value, rest) || value.split_first().is_some_and(|(_, tail)| glob_match(tail, pattern)),
        '?' => value.split_first().is_some_and(|(_, tail)| glob_match(tail, rest)),
        '[' => {
            let Some((candidate, tail)) = value.split_first()
            else {
                return false;
            };
            match glob_class(*candidate, rest) {
                Some(true) => glob_match(tail, &rest[glob_class_width(rest)..]),
                _ => false,
            }
        }
        literal => value.split_first().is_some_and(|(candidate, tail)| *candidate == literal && glob_match(tail, rest)),
    }
}

/// Width of the `[...]` class that starts at `pattern[0]`, including both
/// brackets. Zero means the class is never closed.
fn glob_class_width(pattern: &[char]) -> usize {
    let mut index = usize::from(pattern.first() == Some(&'^'));
    let mut first_item = true;
    while index < pattern.len() {
        if pattern[index] == ']' && !first_item {
            return index + 1;
        }
        first_item = false;
        // `a-c` is a range only when a member follows the dash.
        if index + 2 < pattern.len() && pattern[index + 1] == '-' && pattern[index + 2] != ']' {
            index += 3;
        }
        else {
            index += 1;
        }
    }
    0
}

/// Matches one character against the class body that follows `[`.
fn glob_class(candidate: char, pattern: &[char]) -> Option<bool> {
    let width = glob_class_width(pattern);
    if width == 0 {
        return None;
    }
    let invert = pattern.first() == Some(&'^');
    let body = &pattern[usize::from(invert)..width - 1];
    let mut matched = false;
    let mut index = 0;
    let mut first_item = true;
    while index < body.len() {
        // A leading `]` is a member instead of the closing bracket.
        if body[index] == ']' && !first_item {
            break;
        }
        first_item = false;
        if index + 2 < body.len() && body[index + 1] == '-' && body[index + 2] != ']' {
            if candidate >= body[index] && candidate <= body[index + 2] {
                matched = true;
            }
            index += 3;
        }
        else {
            if candidate == body[index] {
                matched = true;
            }
            index += 1;
        }
    }
    Some(if invert { !matched } else { matched })
}

/// `LIKE` folds case only for ASCII letters; every other character compares
/// exactly, which is why `Ü` never matches `ü`.
fn like_eq(left: char, right: char) -> bool {
    if left.is_ascii() && right.is_ascii() { left.eq_ignore_ascii_case(&right) } else { left == right }
}

/// Matches one `LIKE` pattern against a value, one character at a time.
///
/// `%` matches any run including the empty one, `_` matches exactly one
/// character, and the `ESCAPE` character turns the next pattern character into
/// a literal.
fn like_match(value: &[char], pattern: &[char], escape: Option<char>) -> bool {
    let Some((first, rest)) = pattern.split_first()
    else {
        return value.is_empty();
    };
    if escape != Some(*first) && *first == '%' {
        return like_match(value, rest, escape) || value.split_first().is_some_and(|(_, tail)| like_match(tail, pattern, escape));
    }
    let literal = if escape == Some(*first) {
        // A trailing escape character has nothing left to quote.
        match rest.split_first() {
            Some((quoted, rest)) => (*quoted, rest),
            None => return false,
        }
    }
    else {
        (*first, rest)
    };
    let Some((candidate, tail)) = value.split_first()
    else {
        return false;
    };
    if escape != Some(*first) && *first == '_' {
        return like_match(tail, literal.1, escape);
    }
    like_eq(literal.0, *candidate) && like_match(tail, literal.1, escape)
}

fn number_truthy(number: &Number) -> bool {
    match number {
        Number::Int(value) => *value != 0,
        Number::Real(value) => *value != 0.0,
    }
}

fn number_f64(number: Number) -> f64 {
    match number {
        Number::Int(value) => value as f64,
        Number::Real(value) => value,
    }
}

/// Unary negation: integer negation that overflows becomes a real, and `NULL`
/// stays `NULL`. Text and blobs are converted numerically first, as in SQLite.
fn negate_cell(cell: &Cell) -> Cell {
    match numeric(cell) {
        None => Cell::Null,
        Some(Number::Int(value)) => match value.checked_neg() {
            Some(value) => Cell::Integer(value),
            None => Cell::Real(-(value as f64)),
        },
        Some(Number::Real(value)) => Cell::Real(-value),
    }
}

/// Arithmetic with SQLite rules: `NULL` propagates, integer operators truncate
/// and fall back to a real on overflow, and division by zero yields `NULL`.
fn arithmetic(op: BinaryOp, left: &Cell, right: &Cell) -> Cell {
    let (Some(left), Some(right)) = (numeric(left), numeric(right))
    else {
        return Cell::Null;
    };
    match op {
        BinaryOp::Add => match (left, right) {
            (Number::Int(left), Number::Int(right)) => match left.checked_add(right) {
                Some(value) => Cell::Integer(value),
                None => Cell::Real(left as f64 + right as f64),
            },
            (left, right) => Cell::Real(number_f64(left) + number_f64(right)),
        },
        BinaryOp::Subtract => match (left, right) {
            (Number::Int(left), Number::Int(right)) => match left.checked_sub(right) {
                Some(value) => Cell::Integer(value),
                None => Cell::Real(left as f64 - right as f64),
            },
            (left, right) => Cell::Real(number_f64(left) - number_f64(right)),
        },
        BinaryOp::Multiply => match (left, right) {
            (Number::Int(left), Number::Int(right)) => match left.checked_mul(right) {
                Some(value) => Cell::Integer(value),
                None => Cell::Real(left as f64 * right as f64),
            },
            (left, right) => Cell::Real(number_f64(left) * number_f64(right)),
        },
        BinaryOp::Divide => match (left, right) {
            // SQLite returns NULL rather than infinity for any zero divisor.
            (_, Number::Int(0)) | (_, Number::Real(0.0)) => Cell::Null,
            (Number::Int(left), Number::Int(right)) => match left.checked_div(right) {
                Some(value) => Cell::Integer(value),
                None => Cell::Real(left as f64 / right as f64),
            },
            (left, right) => Cell::Real(number_f64(left) / number_f64(right)),
        },
        // `%` converts both operands to 64-bit integers, keeps the dividend's
        // sign, returns a real when either operand was real, and yields NULL
        // when the truncated divisor is zero.
        BinaryOp::Modulo => {
            let real = matches!(&left, Number::Real(_)) || matches!(&right, Number::Real(_));
            let (left, right) = (number_i64(left), number_i64(right));
            if right == 0 {
                Cell::Null
            }
            else {
                // `i64::MIN % -1` overflows, and SQLite answers `0` for it.
                let remainder = left.checked_rem(right).unwrap_or(0);
                if real { Cell::Real(remainder as f64) } else { Cell::Integer(remainder) }
            }
        }
        // `||` never reaches arithmetic: it is evaluated by its own branch.
        BinaryOp::Concat => Cell::Null,
        // `->` / `->>` 同样由独立分支求值，这里只是穷尽匹配。
        BinaryOp::Arrow | BinaryOp::ArrowText => Cell::Null,
        BinaryOp::ShiftLeft => Cell::Integer(shift(number_i64(left), number_i64(right), true)),
        BinaryOp::ShiftRight => Cell::Integer(shift(number_i64(left), number_i64(right), false)),
        BinaryOp::BitAnd => Cell::Integer(number_i64(left) & number_i64(right)),
        BinaryOp::BitOr => Cell::Integer(number_i64(left) | number_i64(right)),
    }
}

/// `~`: one's complement of the 64-bit integer conversion, `NULL` stays `NULL`.
fn bit_not(cell: &Cell) -> Cell {
    match numeric(cell) {
        Some(number) => Cell::Integer(!number_i64(number)),
        None => Cell::Null,
    }
}

fn number_i64(number: Number) -> i64 {
    match number {
        Number::Int(value) => value,
        Number::Real(value) => value as i64,
    }
}

/// SQLite shifts 64-bit integers: a negative count reverses the direction,
/// counts of 64 or more saturate, and `>>` keeps the sign bit.
fn shift(value: i64, count: i64, left: bool) -> i64 {
    let magnitude = count.unsigned_abs();
    let leftward = left != (count < 0);
    if leftward {
        if magnitude >= 64 { 0 } else { ((value as u64) << magnitude) as i64 }
    }
    else if magnitude >= 64 {
        if value < 0 { -1 } else { 0 }
    }
    else {
        value >> magnitude
    }
}

/// `IN` with three-valued logic: a hit is true (false for `NOT IN`), a miss
/// with any `NULL` candidate is unknown, and a `NULL` probe is always unknown.
fn in_result(value: &Cell, list: &[Cell], negated: bool) -> Cell {
    if matches!(value, Cell::Null) {
        return Cell::Null;
    }
    let mut saw_null = false;
    for item in list {
        if matches!(item, Cell::Null) {
            saw_null = true;
        }
        else if compare_values(value, item, CompareOp::Eq, Collation::Binary) == Some(true) {
            return Cell::Integer(i64::from(!negated));
        }
    }
    if saw_null { Cell::Null } else { Cell::Integer(i64::from(negated)) }
}

/// Canonical key set for an `IN (SELECT ...)` value list, or `None` when an
/// item has no canonical signature, so membership has to scan the list.
fn in_list_keys(values: &[Cell]) -> Option<HashSet<ProbeKey>> {
    let mut keys = HashSet::with_capacity(values.len());
    for value in values {
        match probe_key(value, Collation::Binary) {
            Some(key) if !matches!(key, ProbeKey::Wide(_)) => {
                keys.insert(key);
            }
            _ => return None,
        }
    }
    Some(keys)
}

/// Membership with the same three-valued result as [`in_result`], using the
/// canonical key set when the whole list had one.
fn in_result_cached(value: &Cell, list: &[Cell], keys: Option<&HashSet<ProbeKey>>, saw_null: bool, negated: bool) -> Cell {
    if matches!(value, Cell::Null) {
        return Cell::Null;
    }
    if let Some(keys) = keys
        && let Some(key) = probe_key(value, Collation::Binary)
        && !matches!(key, ProbeKey::Wide(_))
    {
        return if keys.contains(&key) {
            Cell::Integer(i64::from(!negated))
        }
        else if saw_null {
            Cell::Null
        }
        else {
            Cell::Integer(i64::from(negated))
        };
    }
    in_result(value, list, negated)
}

/// Canonical hash signature of one output row, or `None` when a cell falls
/// outside the canonical form and dedup must fall back to the linear scan.
fn row_signature(row: &[Cell], collations: &[Collation]) -> Option<Vec<ProbeKey>> {
    row.iter()
        .enumerate()
        .map(|(position, cell)| match probe_key(cell, collations.get(position).copied().unwrap_or_default()) {
            Some(part) if !matches!(part, ProbeKey::Wide(_)) => Some(part),
            _ => None,
        })
        .collect()
}

/// Row equality under the engine's comparison rules and the given collations.
fn rows_equal_with(left: &[Cell], right: &[Cell], collations: &[Collation]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).enumerate().all(|(position, (first, second))| {
            compare_cells_with(first, second, collations.get(position).copied().unwrap_or_default()) == Ordering::Equal
        })
}

fn eval_function(name: ScalarFunction, args: &[Cell]) -> Cell {
    match name {
        // `abs()` 对整数保持整数，对实数与文本一律走二进制浮点，所以
        // `typeof(abs('-7'))` 是 `real` 而不是 `integer`。
        ScalarFunction::Abs => match &args[0] {
            Cell::Null => Cell::Null,
            Cell::Integer(value) => match value.checked_abs() {
                Some(value) => Cell::Integer(value),
                None => Cell::Real(-(*value as f64)),
            },
            cell => match numeric(cell) {
                Some(Number::Int(value)) => Cell::Real((value as f64).abs()),
                Some(Number::Real(value)) => Cell::Real(value.abs()),
                None => Cell::Null,
            },
        },
        ScalarFunction::Coalesce => args.iter().find(|cell| !matches!(cell, Cell::Null)).cloned().unwrap_or(Cell::Null),
        ScalarFunction::Length => match &args[0] {
            Cell::Null => Cell::Null,
            Cell::Blob(bytes) => Cell::Integer(bytes.len() as i64),
            Cell::Text(text) => Cell::Integer(text.chars().count() as i64),
            other => Cell::Integer(number_text(other).chars().count() as i64),
        },
        ScalarFunction::Typeof => Cell::Text(
            match &args[0] {
                Cell::Null => "null",
                Cell::Integer(_) => "integer",
                Cell::Real(_) => "real",
                Cell::Text(_) => "text",
                Cell::Blob(_) => "blob",
            }
            .to_string(),
        ),
        ScalarFunction::Lower => match &args[0] {
            Cell::Text(text) => Cell::Text(text.to_ascii_lowercase()),
            other => other.clone(),
        },
        ScalarFunction::Upper => match &args[0] {
            Cell::Text(text) => Cell::Text(text.to_ascii_uppercase()),
            other => other.clone(),
        },
        ScalarFunction::Substr => substr_cell(args),
        ScalarFunction::Round => round_cell(args),
        // `hex()` hashes the text form, so `hex(255)` is `323535` and a blob
        // keeps its bytes; `NULL` becomes the empty string.
        ScalarFunction::Hex => Cell::Text(match text_form(&args[0]) {
            Some(text) => text.bytes().map(|byte| format!("{byte:02X}")).collect(),
            None => String::new(),
        }),
        ScalarFunction::Quote => Cell::Text(quote_text(&args[0])),
        ScalarFunction::Random => Cell::Integer(next_random()),
        ScalarFunction::RandomBlob => Cell::Blob(random_blob(&args[0])),
        ScalarFunction::Instr => instr_cell(args),
        ScalarFunction::Trim => trim_cell(args, true, true),
        ScalarFunction::Ltrim => trim_cell(args, true, false),
        ScalarFunction::Rtrim => trim_cell(args, false, true),
        ScalarFunction::Replace => replace_cell(args),
        ScalarFunction::IfNull => match &args[0] {
            Cell::Null => args[1].clone(),
            value => value.clone(),
        },
        // `nullif(x, y)` 与 `x = y` 共用比较语义，比较结果未知时返回 `x`。
        ScalarFunction::NullIf => match compare_values(&args[0], &args[1], CompareOp::Eq, Collation::Binary) {
            Some(true) => Cell::Null,
            _ => args[0].clone(),
        },
        ScalarFunction::Printf => printf_cell(args),
        ScalarFunction::Char => Cell::Text(args.iter().map(|cell| character_for_code(integer_argument(cell))).collect()),
        ScalarFunction::Unicode => match &args[0] {
            Cell::Null => Cell::Null,
            cell => match cast_text(cell).chars().next() {
                Some(character) => Cell::Integer(character as i64),
                None => Cell::Null,
            },
        },
        ScalarFunction::Soundex => soundex_cell(&args[0]),
        ScalarFunction::Min => min_max_cell(args, false),
        ScalarFunction::Max => min_max_cell(args, true),
        ScalarFunction::Ceil => integral_or_real(args, f64::ceil),
        ScalarFunction::Floor => integral_or_real(args, f64::floor),
        ScalarFunction::Trunc => integral_or_real(args, f64::trunc),
        ScalarFunction::Sqrt => real_function(args, f64::sqrt),
        ScalarFunction::Pow => real_function2(args, f64::powf),
        ScalarFunction::Mod => real_function2(args, |left, right| left % right),
        ScalarFunction::Exp => real_function(args, f64::exp),
        ScalarFunction::Ln => positive_real_function(args, f64::ln),
        ScalarFunction::Log => log_cell(args),
        ScalarFunction::Log2 => positive_real_function(args, f64::log2),
        ScalarFunction::Log10 => positive_real_function(args, f64::log10),
        ScalarFunction::Sin => real_function(args, f64::sin),
        ScalarFunction::Cos => real_function(args, f64::cos),
        ScalarFunction::Tan => real_function(args, f64::tan),
        ScalarFunction::Asin => real_function(args, f64::asin),
        ScalarFunction::Acos => real_function(args, f64::acos),
        ScalarFunction::Atan => real_function(args, f64::atan),
        ScalarFunction::Atan2 => real_function2(args, f64::atan2),
        ScalarFunction::Sinh => real_function(args, f64::sinh),
        ScalarFunction::Cosh => real_function(args, f64::cosh),
        ScalarFunction::Tanh => real_function(args, f64::tanh),
        ScalarFunction::Asinh => real_function(args, f64::asinh),
        ScalarFunction::Acosh => real_function(args, f64::acosh),
        ScalarFunction::Atanh => real_function(args, f64::atanh),
        ScalarFunction::Degrees => real_function(args, f64::to_degrees),
        ScalarFunction::Radians => real_function(args, f64::to_radians),
        ScalarFunction::Pi => Cell::Real(std::f64::consts::PI),
        ScalarFunction::Sign => sign_cell(args),
        ScalarFunction::Date => datetime::eval(DateTimeFunction::Date, args),
        ScalarFunction::Time => datetime::eval(DateTimeFunction::Time, args),
        ScalarFunction::Datetime => datetime::eval(DateTimeFunction::Datetime, args),
        ScalarFunction::JulianDay => datetime::eval(DateTimeFunction::JulianDay, args),
        ScalarFunction::UnixEpoch => datetime::eval(DateTimeFunction::UnixEpoch, args),
        ScalarFunction::Strftime => datetime::eval(DateTimeFunction::Strftime, args),
        ScalarFunction::Timediff => datetime::eval(DateTimeFunction::Timediff, args),
        // JSON 函数在进入本函数前已交给 JSON 模块，这里只为穷尽匹配。
        ScalarFunction::JsonValid
        | ScalarFunction::JsonType
        | ScalarFunction::JsonArray
        | ScalarFunction::JsonObject
        | ScalarFunction::JsonQuote
        | ScalarFunction::JsonExtract
        | ScalarFunction::JsonArrayLength
        | ScalarFunction::JsonRemove
        | ScalarFunction::JsonSet
        | ScalarFunction::JsonInsert
        | ScalarFunction::JsonReplace
        | ScalarFunction::JsonPatch => Cell::Null,
    }
}

/// 多参数 `min` / `max`：任一参数为 `NULL` 时返回 `NULL`。`min` 在相等时
/// 取后一个参数，`max` 在相等时保留前一个参数，与 SQLite 的比较循环一致。
fn min_max_cell(args: &[Cell], want_max: bool) -> Cell {
    if args.iter().any(|cell| matches!(cell, Cell::Null)) {
        return Cell::Null;
    }
    let mut result = &args[0];
    for candidate in &args[1..] {
        let replace = if want_max {
            compare_values(result, candidate, CompareOp::Lt, Collation::Binary) == Some(true)
        }
        else {
            compare_values(result, candidate, CompareOp::GtEq, Collation::Binary) == Some(true)
        };
        if replace {
            result = candidate;
        }
    }
    result.clone()
}

fn character_for_code(code: i64) -> char {
    u32::try_from(code).ok().and_then(char::from_u32).unwrap_or('\u{FFFD}')
}

/// `soundex`：首字母大写后按 SQLite 的分组追加数字，最多四位，不足补 `0`。
fn soundex_cell(cell: &Cell) -> Cell {
    let text = cast_text(cell);
    let mut letters = text.chars().filter(|character| character.is_ascii_alphabetic());
    let Some(first) = letters.next()
    else {
        return Cell::Text("?000".to_string());
    };
    let mut encoded = String::new();
    encoded.push(first.to_ascii_uppercase());
    let mut previous = soundex_code(first);
    for letter in letters {
        let code = soundex_code(letter);
        if code == 0 {
            previous = 0;
            continue;
        }
        if code != previous {
            encoded.push((b'0' + code) as char);
            if encoded.len() == 4 {
                break;
            }
        }
        previous = code;
    }
    while encoded.len() < 4 {
        encoded.push('0');
    }
    Cell::Text(encoded)
}

fn soundex_code(character: char) -> u8 {
    match character.to_ascii_uppercase() {
        'B' | 'F' | 'P' | 'V' => 1,
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => 2,
        'D' | 'T' => 3,
        'L' => 4,
        'M' | 'N' => 5,
        'R' => 6,
        _ => 0,
    }
}

fn integer_argument(cell: &Cell) -> i64 {
    match cast_numeric(cell) {
        Cell::Integer(value) => value,
        Cell::Real(value) => value as i64,
        _ => 0,
    }
}

fn real_argument(cell: &Cell) -> f64 {
    match cast_numeric(cell) {
        Cell::Integer(value) => value as f64,
        Cell::Real(value) => value,
        _ => 0.0,
    }
}

/// `printf` / `format`：`NULL` 格式串返回 `NULL`，位置参数写法同样返回 `NULL`。
fn printf_cell(args: &[Cell]) -> Cell {
    let Some(format) = text_form(&args[0])
    else {
        return Cell::Null;
    };
    if format.contains('$') {
        return Cell::Null;
    }
    let characters: Vec<char> = format.chars().collect();
    let mut output = String::new();
    let mut index = 0usize;
    let mut argument = 1usize;
    while index < characters.len() {
        if characters[index] != '%' {
            output.push(characters[index]);
            index += 1;
            continue;
        }
        index += 1;
        if index >= characters.len() {
            output.push('%');
            break;
        }
        if characters[index] == '%' {
            output.push('%');
            index += 1;
            continue;
        }
        let mut flags = String::new();
        while index < characters.len() && "-+ 0#".contains(characters[index]) {
            flags.push(characters[index]);
            index += 1;
        }
        let mut width = 0usize;
        while index < characters.len() && characters[index].is_ascii_digit() {
            width = width.saturating_mul(10).saturating_add((characters[index] as u8 - b'0') as usize);
            index += 1;
        }
        let mut precision = None;
        if index < characters.len() && characters[index] == '.' {
            index += 1;
            let mut digits = 0usize;
            while index < characters.len() && characters[index].is_ascii_digit() {
                digits = digits.saturating_mul(10).saturating_add((characters[index] as u8 - b'0') as usize);
                index += 1;
            }
            precision = Some(digits);
        }
        while index < characters.len() && "lhL".contains(characters[index]) {
            index += 1;
        }
        if index >= characters.len() {
            output.push('%');
            break;
        }
        let conversion = characters[index];
        index += 1;
        // `%n` 不消费参数也不产出字符，解析继续。
        if conversion == 'n' {
            continue;
        }
        if !"diufFeEgGxXocszqQwp".contains(conversion) {
            // 未知转换终止解析：已有输出原样返回，否则整体为 NULL。
            return if output.is_empty() { Cell::Null } else { Cell::Text(output) };
        }
        let cell = args.get(argument).cloned().unwrap_or(Cell::Null);
        argument += 1;
        let rendered = render_conversion(conversion, &cell, &flags, width, precision);
        output.push_str(&rendered);
    }
    Cell::Text(output)
}

fn render_conversion(conversion: char, cell: &Cell, flags: &str, width: usize, precision: Option<usize>) -> String {
    let left = flags.contains('-');
    let zero = flags.contains('0') && !left;
    let plus = flags.contains('+');
    let space = flags.contains(' ');
    let alternate = flags.contains('#');
    match conversion {
        'd' | 'i' => {
            let value = integer_argument(cell);
            let digits = format!("{}", value.unsigned_abs());
            let digits = apply_integer_precision(digits, precision);
            let sign = if value < 0 {
                "-"
            }
            else if plus {
                "+"
            }
            else if space {
                " "
            }
            else {
                ""
            };
            pad_text(&format!("{sign}{digits}"), width, left, zero, sign.len())
        }
        'u' => {
            let value = integer_argument(cell) as u64;
            let digits = apply_integer_precision(value.to_string(), precision);
            pad_text(&digits, width, left, zero, 0)
        }
        'x' | 'X' => {
            let digits = format!("{:x}", integer_argument(cell) as u64);
            let digits = apply_integer_precision(digits, precision);
            let digits = if conversion == 'X' { digits.to_ascii_uppercase() } else { digits };
            let prefix = if alternate && integer_argument(cell) != 0 { if conversion == 'X' { "0X" } else { "0x" } } else { "" };
            pad_text(&format!("{prefix}{digits}"), width, left, zero, prefix.len())
        }
        'o' => {
            let digits = format!("{:o}", integer_argument(cell) as u64);
            let digits = apply_integer_precision(digits, precision);
            let prefix = if alternate && !digits.starts_with('0') { "0" } else { "" };
            pad_text(&format!("{prefix}{digits}"), width, left, zero, prefix.len())
        }
        'f' | 'F' => {
            let value = real_argument(cell);
            let keep = precision.unwrap_or(6);
            let (integer, fraction) = fixed_parts(value.abs(), keep);
            let mut body = integer;
            if keep > 0 {
                body.push('.');
                body.push_str(&fraction);
            }
            else if alternate {
                body.push('.');
            }
            let sign = if value.is_sign_negative() || value < 0.0 {
                "-"
            }
            else if plus {
                "+"
            }
            else if space {
                " "
            }
            else {
                ""
            };
            pad_text(&format!("{sign}{body}"), width, left, zero, sign.len())
        }
        'e' | 'E' => {
            let value = real_argument(cell);
            let keep = precision.unwrap_or(6);
            let (integer, fraction, exponent) = scientific_parts(value.abs(), keep);
            let letter = if conversion == 'E' { 'E' } else { 'e' };
            let sign = if value.is_sign_negative() || value < 0.0 {
                "-"
            }
            else if plus {
                "+"
            }
            else if space {
                " "
            }
            else {
                ""
            };
            let body = if fraction.is_empty() {
                format!("{integer}{letter}{}{:02}", if exponent < 0 { '-' } else { '+' }, exponent.unsigned_abs())
            }
            else {
                format!("{integer}.{fraction}{letter}{}{:02}", if exponent < 0 { '-' } else { '+' }, exponent.unsigned_abs())
            };
            pad_text(&format!("{sign}{body}"), width, left, zero, sign.len())
        }
        'g' | 'G' => {
            let value = real_argument(cell);
            let significant = precision.unwrap_or(6).max(1);
            let (integer, fraction, exponent) = scientific_parts(value.abs(), significant - 1);
            let mut body = if exponent < -4 || exponent >= significant as i32 {
                let fraction = trim_decimal_zeros(&fraction, alternate);
                let letter = if conversion == 'G' { 'E' } else { 'e' };
                let mantissa = if fraction.is_empty() { integer } else { format!("{integer}.{fraction}") };
                format!("{mantissa}{letter}{}{:02}", if exponent < 0 { '-' } else { '+' }, exponent.unsigned_abs())
            }
            else {
                let keep = (significant as i32 - 1 - exponent).max(0) as usize;
                let (integer, fraction) = fixed_parts(value.abs(), keep);
                let fraction = trim_decimal_zeros(&fraction, alternate);
                if fraction.is_empty() { if alternate { format!("{integer}.") } else { integer } } else { format!("{integer}.{fraction}") }
            };
            if conversion == 'G' {
                body = body.to_ascii_uppercase();
            }
            let sign = if value.is_sign_negative() || value < 0.0 {
                "-"
            }
            else if plus {
                "+"
            }
            else if space {
                " "
            }
            else {
                ""
            };
            pad_text(&format!("{sign}{body}"), width, left, zero, sign.len())
        }
        'c' => {
            let first: String = cast_text(cell).chars().next().map(|character| character.to_string()).unwrap_or_else(|| "\u{0000}".to_string());
            // 精度对 `%c` 表示重复次数，零与一都只输出一个字符。
            let body = first.repeat(precision.unwrap_or(1).max(1));
            // 字符串类转换忽略 `0` 标志，补位一律用空格。
            pad_text(&body, width, left, false, 0)
        }
        's' | 'z' => {
            let body = match (conversion, cell) {
                ('z', Cell::Null) => String::new(),
                _ => cast_text(cell),
            };
            let body = truncate_characters(body, precision);
            pad_text(&body, width, left, false, 0)
        }
        'q' | 'w' => {
            let body = match cell {
                Cell::Null => "(NULL)".to_string(),
                _ => {
                    let text = truncate_characters(cast_text(cell), precision);
                    if conversion == 'q' { text.replace('\'', "''") } else { text }
                }
            };
            pad_text(&body, width, left, false, 0)
        }
        'Q' => {
            let body = match cell {
                Cell::Null => "NULL".to_string(),
                _ => {
                    let text = truncate_characters(cast_text(cell), precision);
                    format!("'{}'", text.replace('\'', "''"))
                }
            };
            pad_text(&body, width, left, false, 0)
        }
        'p' => {
            let digits = format!("{:X}", integer_argument(cell) as u64);
            let digits = apply_integer_precision(digits, precision);
            let prefix = if alternate && integer_argument(cell) != 0 { "0x" } else { "" };
            pad_text(&format!("{prefix}{digits}"), width, left, zero, prefix.len())
        }
        // SQLite 对未知转换按文本渲染，这里保持同样的宽松行为。
        _ => pad_text(&cast_text(cell), width, left, zero, 0),
    }
}

fn apply_integer_precision(digits: String, precision: Option<usize>) -> String {
    match precision {
        Some(minimum) if digits.len() < minimum => format!("{}{digits}", "0".repeat(minimum - digits.len())),
        _ => digits,
    }
}

fn truncate_characters(text: String, precision: Option<usize>) -> String {
    match precision {
        Some(limit) => text.chars().take(limit).collect(),
        None => text,
    }
}

fn pad_text(body: &str, width: usize, left: bool, zero: bool, prefix_len: usize) -> String {
    if left {
        let count = body.chars().count();
        if count >= width {
            return body.to_string();
        }
        return format!("{body}{}", " ".repeat(width - count));
    }
    if zero {
        let prefix: String = body.chars().take(prefix_len).collect();
        let rest: String = body.chars().skip(prefix_len).collect();
        // 符号计入宽度，`0x` / 八进制前导 `0` 一类前缀不计入宽度。
        let sign_like = prefix.starts_with('-') || prefix.starts_with('+') || prefix.starts_with(' ');
        let measured = if sign_like { body.chars().count() } else { rest.chars().count() };
        if measured >= width {
            return body.to_string();
        }
        return format!("{prefix}{}{rest}", "0".repeat(width - measured));
    }
    let count = body.chars().count();
    if count >= width {
        return body.to_string();
    }
    format!("{}{body}", " ".repeat(width - count))
}

fn trim_decimal_zeros(fraction: &str, keep_zero: bool) -> String {
    if keep_zero {
        return fraction.to_string();
    }
    fraction.trim_end_matches('0').to_string()
}

/// 把 `value` 按十进制四舍五入到 `keep` 位小数，返回整数段与小数段。
fn fixed_parts(value: f64, keep: usize) -> (String, String) {
    let probe = format!("{value:.precision$}", precision = keep + 20);
    let (probe_integer, probe_fraction) = probe.split_once('.').unwrap_or((probe.as_str(), ""));
    let tail: String = probe_fraction.chars().skip(keep).collect();
    if tail.starts_with('5') && tail.chars().skip(1).all(|character| character == '0') {
        let mut digits: String = probe_integer.chars().chain(probe_fraction.chars().take(keep)).collect();
        digits = increment_decimal(&digits);
        let split = digits.len() - keep;
        return (digits[..split].to_string(), digits[split..].to_string());
    }
    let rounded = format!("{value:.precision$}", precision = keep);
    match rounded.split_once('.') {
        Some((integer, fraction)) => (integer.to_string(), fraction.to_string()),
        None => (rounded, String::new()),
    }
}

/// 把 `value` 规格化为 `整数位.小数位 e 指数`，尾数按十进制四舍五入。
fn scientific_parts(value: f64, keep: usize) -> (String, String, i32) {
    if value == 0.0 {
        return ("0".to_string(), "0".repeat(keep), 0);
    }
    let probe = format!("{value:.precision$e}", precision = keep + 20);
    let (probe_mantissa, probe_exponent) = probe.split_once('e').unwrap_or((probe.as_str(), "0"));
    let probe_digits: String = probe_mantissa.chars().filter(|character| character.is_ascii_digit()).collect();
    let tail: String = probe_digits.chars().skip(keep + 1).collect();
    let exponent: i32 = probe_exponent.parse().unwrap_or(0);
    let digits = if tail.starts_with('5') && tail.chars().skip(1).all(|character| character == '0') {
        increment_decimal(&probe_digits.chars().take(keep + 1).collect::<String>())
    }
    else {
        let rounded = format!("{value:.precision$e}", precision = keep);
        rounded
            .split_once('e')
            .map(|(mantissa, _)| mantissa.chars().filter(|character| character.is_ascii_digit()).collect())
            .unwrap_or_else(|| "0".to_string())
    };
    if digits.len() > keep + 1 {
        let digit = digits.chars().take(keep + 1).collect::<String>();
        return (digit[..1].to_string(), digit[1..].to_string(), exponent + 1);
    }
    (digits[..1].to_string(), digits[1..].to_string(), exponent)
}

/// `trim` 家族：按字符集去除两端字符，第二个参数缺省时只去除空格。
fn trim_cell(args: &[Cell], left: bool, right: bool) -> Cell {
    let Some(text) = text_form(&args[0])
    else {
        return Cell::Null;
    };
    let set = match args.get(1) {
        Some(cell) => match text_form(cell) {
            Some(set) => set,
            None => return Cell::Null,
        },
        None => " ".to_string(),
    };
    if set.is_empty() {
        return Cell::Text(text);
    }
    let characters: Vec<char> = text.chars().collect();
    let mut start = 0;
    let mut end = characters.len();
    if left {
        while start < end && set.chars().any(|candidate| candidate == characters[start]) {
            start += 1;
        }
    }
    if right {
        while end > start && set.chars().any(|candidate| candidate == characters[end - 1]) {
            end -= 1;
        }
    }
    Cell::Text(characters[start..end].iter().collect())
}

/// `replace(x, y, z)`：空 `y` 原样返回 `x`，任一参数为 `NULL` 时返回 `NULL`。
fn replace_cell(args: &[Cell]) -> Cell {
    let (Some(text), Some(pattern), Some(replacement)) = (text_form(&args[0]), text_form(&args[1]), text_form(&args[2]))
    else {
        return Cell::Null;
    };
    if pattern.is_empty() {
        return Cell::Text(text);
    }
    Cell::Text(text.replace(&pattern, &replacement))
}

/// `instr(x, y)`: the one-based character position of the first `y` inside
/// `x`, or zero when `y` does not occur. Either `NULL` propagates.
fn instr_cell(args: &[Cell]) -> Cell {
    match (text_form(&args[0]), text_form(&args[1])) {
        (Some(haystack), Some(needle)) => match haystack.find(&needle) {
            Some(byte) => Cell::Integer(haystack[..byte].chars().count() as i64 + 1),
            None => Cell::Integer(0),
        },
        _ => Cell::Null,
    }
}

/// `quote()` renders one SQL literal: text is single-quoted with doubled
/// quotes, blobs become `X'..'`, `NULL` becomes the word `NULL`, and reals use
/// the 15-digit form when it round-trips.
fn quote_text(cell: &Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_string(),
        Cell::Integer(value) => value.to_string(),
        Cell::Real(value) => quote_real(*value),
        Cell::Text(text) => format!("'{}'", text.replace('\'', "''")),
        Cell::Blob(bytes) => {
            let digits: String = bytes.iter().map(|byte| format!("{byte:02X}")).collect();
            format!("X'{digits}'")
        }
    }
}

fn quote_real(value: f64) -> String {
    let text = sqlite_real_text(value);
    if text.parse::<f64>() == Ok(value) {
        return text;
    }
    // SQLite falls back to 18 fractional digits when the short form loses
    // precision, with a sign and at least two exponent digits.
    let formatted = format!("{value:.18e}");
    match formatted.split_once('e') {
        Some((mantissa, exponent)) => {
            let exponent: i32 = exponent.parse().unwrap_or(0);
            format!("{mantissa}e{}{:02}", if exponent < 0 { '-' } else { '+' }, exponent.abs())
        }
        None => formatted,
    }
}

/// `random()` returns any 64-bit integer, so a counter-driven xorshift is
/// enough: tests only check the type and range.
fn next_random() -> i64 {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut state = STATE.load(Ordering::Relaxed);
    if state == 0 {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_nanos() as u64);
        state = nanos ^ 0x9e37_79b9_7f4a_7c15;
    }
    // xorshift64*: cheap, deterministic and good enough for a random value.
    let mut next = state ^ (state >> 12);
    next ^= next << 25;
    next ^= next >> 27;
    STATE.store(next, Ordering::Relaxed);
    (next.wrapping_mul(0x2545_f491_4f6c_dd1d)) as i64
}

/// `randomblob(n)` yields `n` random bytes; `n` below one means a single byte,
/// which is also what a `NULL` argument produces.
fn random_blob(size: &Cell) -> Vec<u8> {
    let count = match numeric(size) {
        Some(number) => number_i64(number).max(1) as usize,
        None => 1,
    };
    let mut bytes = Vec::with_capacity(count);
    while bytes.len() < count {
        let mut value = next_random();
        for _ in 0..8 {
            if bytes.len() == count {
                break;
            }
            bytes.push(value as u8);
            value >>= 8;
        }
    }
    bytes
}

/// Text form used by `length()` for numeric input.
fn number_text(cell: &Cell) -> String {
    match cell {
        Cell::Integer(value) => value.to_string(),
        Cell::Real(value) => {
            if value.is_finite() && *value == value.trunc() && value.abs() < 1e15 {
                format!("{value:.1}")
            }
            else {
                value.to_string()
            }
        }
        Cell::Text(text) => text.clone(),
        Cell::Blob(_) => String::new(),
        Cell::Null => String::new(),
    }
}

fn substr_cell(args: &[Cell]) -> Cell {
    let start = match numeric(&args[1]) {
        Some(number) => match number {
            Number::Int(value) => value,
            Number::Real(value) => value as i64,
        },
        None => return Cell::Null,
    };
    let count = match args.get(2) {
        None => None,
        Some(cell) => match numeric(cell) {
            Some(Number::Int(value)) => Some(value),
            Some(Number::Real(value)) => Some(value as i64),
            None => return Cell::Null,
        },
    };
    match &args[0] {
        Cell::Null => Cell::Null,
        Cell::Blob(bytes) => {
            let (begin, end) = substr_bounds(bytes.len(), start, count);
            Cell::Blob(bytes[begin..end].to_vec())
        }
        _ => {
            let text = match &args[0] {
                Cell::Text(text) => text.clone(),
                other => number_text(other),
            };
            let chars = text.chars().collect::<Vec<_>>();
            let (begin, end) = substr_bounds(chars.len(), start, count);
            Cell::Text(chars[begin..end].iter().collect())
        }
    }
}

/// SQLite `substr` bounds: positive start is 1-based, non-positive start counts
/// from the right, and a negative length runs backwards from the start point.
fn substr_bounds(length: usize, start: i64, count: Option<i64>) -> (usize, usize) {
    let length = length as i64;
    let begin = if start > 0 {
        start - 1
    }
    else if start == 0 {
        -1
    }
    else {
        length + start
    };
    let (begin, end) = match count {
        None => (begin, length),
        Some(count) if count >= 0 => (begin, begin + count),
        Some(count) => (begin + count, begin),
    };
    (begin.clamp(0, length) as usize, end.clamp(0, length) as usize)
}

fn round_cell(args: &[Cell]) -> Cell {
    let value = match numeric(&args[0]) {
        Some(Number::Int(value)) => value as f64,
        Some(Number::Real(value)) => value,
        None => return Cell::Null,
    };
    let digits = match args.get(1) {
        None => 0,
        Some(cell) => match numeric(cell) {
            Some(Number::Int(value)) => value,
            Some(Number::Real(value)) => value.trunc() as i64,
            None => return Cell::Null,
        },
    };
    Cell::Real(round_digits(value, digits))
}

/// SQLite's `round()`: the digit count is clamped to `0..=30`, a zero count
/// truncates `value ± 0.5` toward zero, and every other count re-reads the
/// fixed-point text its `printf` produces, which rounds an exact decimal tie
/// away from zero.
fn round_digits(value: f64, digits: i64) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let digits = digits.clamp(0, 30) as usize;
    if digits == 0 {
        return if value < 0.0 { (value - 0.5).trunc() } else { (value + 0.5).trunc() };
    }
    let magnitude = value.abs();
    let short = format!("{magnitude:.digits$}");
    // A tie is a `5` directly after the kept digits with nothing but zeros
    // behind it; Rust's own formatting rounds that tie to even, SQLite rounds
    // it away from zero.
    let wide = format!("{:.*}", digits + 25, magnitude);
    let fraction = wide.split_once('.').map(|(_, fraction)| fraction).unwrap_or("");
    let tie = fraction.len() > digits && fraction.as_bytes()[digits] == b'5' && fraction[digits + 1..].bytes().all(|byte| byte == b'0');
    let text = if tie { increment_decimal(&short) } else { short };
    let rounded: f64 = text.parse().unwrap_or(magnitude);
    if value < 0.0 { -rounded } else { rounded }
}

/// 数学函数的取数口径：整数与实数原样，文本必须整体可解析为数值，`NULL`
/// 与 blob 视为无值。SQLite 对文本套用 numeric affinity，因此 `'2abc'` 是
/// `NULL` 而不是前缀 `2`。
fn math_number(cell: &Cell) -> Option<Number> {
    match cell {
        Cell::Null => None,
        Cell::Integer(value) => Some(Number::Int(*value)),
        Cell::Real(value) => Some(Number::Real(*value)),
        Cell::Text(text) => parse_full_number(text),
        Cell::Blob(_) => None,
    }
}

/// 整体解析一段文本：两端空白可有，中途出现任何非数值字符即拒绝。整数
/// 字面量返回 [`Number::Int`]，带小数点或指数的返回 [`Number::Real`]。
fn parse_full_number(text: &str) -> Option<Number> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(value) = trimmed.parse::<i64>() {
        return Some(Number::Int(value));
    }
    match trimmed.parse::<f64>() {
        Ok(value) if value.is_finite() => Some(Number::Real(value)),
        _ => None,
    }
}

/// 数学结果统一出口：`NaN` 记为 `NULL`，正负无穷保留为实数，与 SQLite 的
/// `sqlite3_result_double` 一致。
fn real_result(value: f64) -> Cell {
    if value.is_nan() { Cell::Null } else { Cell::Real(value) }
}

/// 一元数学函数：入参按 [`math_number`] 取数，输出恒为实数。
fn real_function(args: &[Cell], function: fn(f64) -> f64) -> Cell {
    match args.first().and_then(math_number) {
        Some(Number::Int(value)) => real_result(function(value as f64)),
        Some(Number::Real(value)) => real_result(function(value)),
        None => Cell::Null,
    }
}

/// 二元数学函数：两个入参都按 [`math_number`] 取数，输出恒为实数。
fn real_function2(args: &[Cell], function: fn(f64, f64) -> f64) -> Cell {
    let left = match args.first().and_then(math_number) {
        Some(Number::Int(value)) => value as f64,
        Some(Number::Real(value)) => value,
        None => return Cell::Null,
    };
    let right = match args.get(1).and_then(math_number) {
        Some(Number::Int(value)) => value as f64,
        Some(Number::Real(value)) => value,
        None => return Cell::Null,
    };
    real_result(function(left, right))
}

/// 定义域为正数的数学函数（`ln`、`log2`、`log10`）：非正入参返回 `NULL`，
/// 不把 `-inf` 泄漏成实数。
fn positive_real_function(args: &[Cell], function: fn(f64) -> f64) -> Cell {
    let value = match args.first().and_then(math_number) {
        Some(Number::Int(value)) => value as f64,
        Some(Number::Real(value)) => value,
        None => return Cell::Null,
    };
    if value <= 0.0 {
        return Cell::Null;
    }
    real_result(function(value))
}

/// `ceil` / `floor` / `trunc`：整数入参原样返回整数，实数入参返回取整后的
/// 实数，非数值文本与 `NULL` 返回 `NULL`。
fn integral_or_real(args: &[Cell], function: fn(f64) -> f64) -> Cell {
    match args.first().and_then(math_number) {
        Some(Number::Int(value)) => Cell::Integer(value),
        Some(Number::Real(value)) => real_result(function(value)),
        None => Cell::Null,
    }
}

/// `sign()`：返回 `-1`、`0` 或 `1`，恒为整数；入参不是数值时返回 `NULL`。
fn sign_cell(args: &[Cell]) -> Cell {
    match args.first().and_then(math_number) {
        Some(Number::Int(value)) => Cell::Integer(value.signum()),
        Some(Number::Real(value)) => {
            let sign = if value > 0.0 {
                1
            }
            else if value < 0.0 {
                -1
            }
            else {
                0
            };
            Cell::Integer(sign)
        }
        None => Cell::Null,
    }
}

/// `log()`：单参是十进制对数，双参时底数必须大于 1 且真数必须为正，否则
/// 返回 `NULL`（底数为 1 时 SQLite 同样返回 `NULL` 而不是无穷）。
fn log_cell(args: &[Cell]) -> Cell {
    if args.len() == 1 {
        let value = match args.first().and_then(math_number) {
            Some(Number::Int(value)) => value as f64,
            Some(Number::Real(value)) => value,
            None => return Cell::Null,
        };
        if value <= 0.0 {
            return Cell::Null;
        }
        return real_result(value.log10());
    }
    let base = match args.first().and_then(math_number) {
        Some(Number::Int(value)) => value as f64,
        Some(Number::Real(value)) => value,
        None => return Cell::Null,
    };
    let value = match args.get(1).and_then(math_number) {
        Some(Number::Int(value)) => value as f64,
        Some(Number::Real(value)) => value,
        None => return Cell::Null,
    };
    if base <= 1.0 || value <= 0.0 {
        return Cell::Null;
    }
    real_result(value.ln() / base.ln())
}

/// Increments the last decimal digit of a non-negative fixed-point string,
/// carrying through `9`s and into the integer part.
fn increment_decimal(text: &str) -> String {
    let mut characters: Vec<char> = text.chars().collect();
    let mut index = characters.len();
    loop {
        if index == 0 {
            characters.insert(0, '1');
            break;
        }
        index -= 1;
        match characters[index] {
            '.' => continue,
            '9' => characters[index] = '0',
            digit => {
                characters[index] = char::from(digit as u8 + 1);
                break;
            }
        }
    }
    characters.into_iter().collect()
}

fn self_contained_arity(
    def: &TableDef,
    table: &str,
    columns: Option<&[String]>,
    values: &[crate::WriteValue],
) -> Result<Vec<Option<crate::WriteValue>>, SqlError> {
    let mut slots = vec![None; def.columns.len()];
    match columns {
        Some(names) => {
            if names.len() != values.len() {
                return Err(SqlError::new(
                    "arity-mismatch",
                    format!("insert into `{table}` has {} columns but {} values", names.len(), values.len()),
                )
                .in_statement("insert"));
            }
            let mut seen = Vec::new();
            for (name, value) in names.iter().zip(values) {
                let index = def.columns.iter().position(|column| column.name.eq_ignore_ascii_case(name)).ok_or_else(|| {
                    SqlError::new("unknown-column", format!("unknown column `{name}` in table `{table}`")).in_statement("insert")
                })?;
                if seen.contains(&index) {
                    return Err(
                        SqlError::new("duplicate-column", format!("column `{name}` is listed twice in the insert")).in_statement("insert")
                    );
                }
                seen.push(index);
                slots[index] = Some(value.clone());
            }
        }
        None => {
            if values.len() != def.columns.len() {
                return Err(SqlError::new(
                    "arity-mismatch",
                    format!("insert into `{table}` expects {} values but received {}", def.columns.len(), values.len()),
                )
                .in_statement("insert"));
            }
            for (index, value) in values.iter().enumerate() {
                slots[index] = Some(value.clone());
            }
        }
    }
    Ok(slots)
}

/// Resolves every `CHECK` of one `CREATE TABLE` against the declared columns.
///
/// The table does not exist yet, so a synthetic source exposing its column
/// names stands in for it. A reference to an undeclared column therefore fails
/// the statement with `unknown-column`, and a subquery is rejected by name
/// because SQLite forbids one inside a check.
fn resolve_checks(
    table: &str,
    columns: &[ColumnSpec],
    checks: &[CheckConstraint],
    statement: &'static str,
    without_rowid: bool,
) -> Result<Vec<CheckDef>, SqlError> {
    if checks.is_empty() {
        return Ok(Vec::new());
    }
    let names: Vec<String> = columns.iter().map(|column| column.name.clone()).collect();
    let width = names.len();
    // A base-table source also exposes the hidden rowid slot, so `CHECK
    // (rowid > 0)` resolves exactly like SQLite accepts it.
    let def = TableDef {
        name: table.to_string(),
        columns: columns.to_vec(),
        rowid_alias: None,
        primary_key_index: false,
        primary_key: Vec::new(),
        table_constraints: Vec::new(),
        checks: Vec::new(),
        foreign_keys: Vec::new(),
        strict: false,
        without_rowid,
        temp: false,
    };
    let source = Source {
        label: table.to_string(),
        columns: SourceColumns::Table(&def),
        offset: 0,
        join: None,
        star_hidden: vec![false; width],
        unqualified_hidden: vec![false; width],
        star_override: vec![None; width],
    };
    let relation = Relation { sources: vec![source], using: vec![Vec::new()], using_slots: Vec::new(), trigger: None, excluded: false };
    let mut resolved = Vec::with_capacity(checks.len());
    for check in checks {
        let mut aggregates = Vec::new();
        let mut windows = Vec::new();
        let expr = resolve_expr(&check.expr, &relation, &[], &[], &mut windows, &mut aggregates, false, false)?;
        if expr.contains_subquery() {
            let mut error = SqlError::new("unsupported-clause", "subqueries prohibited in CHECK constraints").in_statement(statement);
            error.clause = Some("CHECK".to_string());
            return Err(error);
        }
        resolved.push(CheckDef { name: check.name.clone(), expr, text: check.text.clone() });
    }
    Ok(resolved)
}

/// Resolves every `REFERENCES` clause of one `CREATE TABLE` against the
/// declared child columns.
///
/// Only the child side is validated here: SQLite allows a foreign key to name a
/// parent table that does not exist yet, and reports an undeclared child column
/// as `SQLITE_ERROR` with the same message the reference engine uses.
fn resolve_foreign_keys(columns: &[ColumnSpec], keys: &[ForeignKey]) -> Result<Vec<ForeignKeyDef>, SqlError> {
    let mut resolved = Vec::with_capacity(keys.len());
    for key in keys {
        let mut positions = Vec::with_capacity(key.columns.len());
        for name in &key.columns {
            let position = columns.iter().position(|column| column.name.eq_ignore_ascii_case(name)).ok_or_else(|| {
                SqlError::new("SQLITE_ERROR", format!("unknown column \"{name}\" in foreign key definition")).in_statement("create-table")
            })?;
            positions.push(position);
        }
        resolved.push(ForeignKeyDef {
            columns: positions,
            parent: key.parent.to_ascii_lowercase(),
            parent_display: key.parent.clone(),
            parent_columns: key.parent_columns.clone(),
            on_delete: foreign_key_action_text(key.on_delete),
            on_update: foreign_key_action_text(key.on_update),
            on_delete_action: key.on_delete,
            on_update_action: key.on_update,
            deferred: key.deferred,
        });
    }
    Ok(resolved)
}

/// Spells one referential action the way `PRAGMA foreign_key_list` reports it.
fn foreign_key_action_text(action: ForeignKeyAction) -> String {
    match action {
        ForeignKeyAction::NoAction => "NO ACTION",
        ForeignKeyAction::Restrict => "RESTRICT",
        ForeignKeyAction::SetNull => "SET NULL",
        ForeignKeyAction::SetDefault => "SET DEFAULT",
        ForeignKeyAction::Cascade => "CASCADE",
    }
    .to_string()
}

/// Finds the table one `PRAGMA` refers to. An unqualified name searches the
/// temp schema first, then `main`, matching SQLite's connection search order.
fn pragma_table_def<'a>(engine: &'a Engine, schema: Option<&str>, name: &str) -> Option<&'a TableDef> {
    let key = name.to_ascii_lowercase();
    let wants_temp = schema.map_or(true, |slot| slot.eq_ignore_ascii_case("temp"));
    let wants_main = schema.map_or(true, |slot| slot.eq_ignore_ascii_case("main"));
    if wants_temp {
        if let Some(position) = engine.temp_lookup.get(&key) {
            return Some(&engine.temp_tables[*position].def);
        }
    }
    if wants_main {
        if let Some(position) = engine.table_lookup.get(&key) {
            return Some(&engine.tables[*position]);
        }
    }
    None
}

/// Finds the view one `PRAGMA` refers to, preferring the temp schema.
fn pragma_view_def<'a>(engine: &'a Engine, schema: Option<&str>, name: &str) -> Option<&'a ViewDef> {
    let key = name.to_ascii_lowercase();
    let wants_temp = schema.map_or(true, |slot| slot.eq_ignore_ascii_case("temp"));
    let wants_main = schema.map_or(true, |slot| slot.eq_ignore_ascii_case("main"));
    if wants_temp {
        if let Some(view) = engine.temp_views.get(&key) {
            return Some(&view.def);
        }
    }
    if wants_main {
        if let Some(view) = engine.views.get(&key) {
            return Some(view);
        }
    }
    None
}

/// The declared type text `PRAGMA table_info` reports: SQLite normalises the
/// four storage-class keywords to upper case and echoes everything else as it
/// was written.
fn pragma_declared_type(declared: &str) -> String {
    let text = declared.trim();
    match text.to_ascii_lowercase().as_str() {
        "integer" | "text" | "real" | "blob" => text.to_ascii_uppercase(),
        _ => text.to_string(),
    }
}

/// Automatic indexes one table owns, in the writer's numbering order. Each
/// entry is (suffix number, keyed column positions, `PRAGMA` origin).
fn autoindex_definitions(def: &TableDef) -> Vec<(usize, Vec<usize>, &'static str)> {
    let mut number = 0usize;
    let mut definitions = Vec::new();
    for (position, column) in def.columns.iter().enumerate() {
        if (column.primary_key || column.unique) && def.rowid_alias != Some(position) {
            number += 1;
            definitions.push((number, vec![position], if column.primary_key { "pk" } else { "u" }));
        }
    }
    for constraint in &def.table_constraints {
        if constraint.primary && (def.rowid_alias.is_some() || def.without_rowid) {
            continue;
        }
        number += 1;
        definitions.push((number, constraint.columns.clone(), if constraint.primary { "pk" } else { "u" }));
    }
    definitions
}

/// `PRAGMA table_info(t)` / `PRAGMA table_xinfo(t)`.
fn pragma_table_info(engine: &Engine, schema: Option<&str>, table: &str, extended: bool) -> Result<ExecResult, SqlError> {
    let mut columns =
        vec!["cid".to_string(), "name".to_string(), "type".to_string(), "notnull".to_string(), "dflt_value".to_string(), "pk".to_string()];
    if extended {
        columns.push("hidden".to_string());
    }
    let mut rows = Vec::new();
    if let Some(def) = pragma_table_def(engine, schema, table) {
        let key_positions = def.primary_key_positions().unwrap_or_default();
        for (position, column) in def.columns.iter().enumerate() {
            let pk = key_positions.iter().position(|candidate| *candidate == position).map(|slot| slot as i64 + 1).unwrap_or(0);
            let mut row = vec![
                Cell::Integer(position as i64),
                Cell::Text(column.name.clone()),
                Cell::Text(pragma_declared_type(&column.declared_type)),
                Cell::Integer(i64::from(column.not_null)),
                column.default_text.clone().map(Cell::Text).unwrap_or(Cell::Null),
                Cell::Integer(pk),
            ];
            if extended {
                row.push(Cell::Integer(0));
            }
            rows.push(row);
        }
    }
    else if let Some(view) = pragma_view_def(engine, schema, table) {
        // SQLite infers a view column type from a plain column reference; this
        // engine reports the output names with an empty type.
        let (names, _) = engine.view_output(view)?;
        for (position, name) in names.iter().enumerate() {
            let mut row = vec![
                Cell::Integer(position as i64),
                Cell::Text(name.clone()),
                Cell::Text(String::new()),
                Cell::Integer(0),
                Cell::Null,
                Cell::Integer(0),
            ];
            if extended {
                row.push(Cell::Integer(0));
            }
            rows.push(row);
        }
    }
    Ok(ExecResult { statement: "pragma", columns, rows, affected: 0 })
}

/// `PRAGMA index_list(t)`: explicit indexes newest first, then the automatic
/// constraint indexes in reverse numbering order.
fn pragma_index_list(engine: &Engine, table: &str) -> ExecResult {
    let key = table.to_ascii_lowercase();
    let mut explicit: Vec<&IndexDef> = engine.indexes.values().filter(|index| index.table == key).collect();
    explicit.sort_by(|left, right| right.order.cmp(&left.order));
    let mut rows = Vec::new();
    for index in explicit {
        rows.push(vec![
            Cell::Integer(rows.len() as i64),
            Cell::Text(index.name.clone()),
            Cell::Integer(i64::from(index.unique)),
            Cell::Text("c".to_string()),
            Cell::Integer(0),
        ]);
    }
    if let Some(def) = pragma_table_def(engine, None, table) {
        for (number, _, origin) in autoindex_definitions(def).into_iter().rev() {
            rows.push(vec![
                Cell::Integer(rows.len() as i64),
                Cell::Text(format!("sqlite_autoindex_{}_{}", def.name, number)),
                Cell::Integer(1),
                Cell::Text(origin.to_string()),
                Cell::Integer(0),
            ]);
        }
    }
    ExecResult {
        statement: "pragma",
        columns: vec!["seq".to_string(), "name".to_string(), "unique".to_string(), "origin".to_string(), "partial".to_string()],
        rows,
        affected: 0,
    }
}

/// `PRAGMA index_info(i)`: `seqno`, `cid` and the column name of every key
/// column, in key order.
fn pragma_index_info(engine: &Engine, index: &str) -> ExecResult {
    let mut rows = Vec::new();
    if let Some(definition) = engine.indexes.get(&index.to_ascii_lowercase()) {
        if let Some(def) = pragma_table_def(engine, None, &definition.table) {
            for (seqno, position) in definition.columns.iter().enumerate() {
                rows.push(vec![Cell::Integer(seqno as i64), Cell::Integer(*position as i64), Cell::Text(def.columns[*position].name.clone())]);
            }
        }
    }
    else {
        for def in engine.tables.iter().chain(engine.temp_tables.iter().map(|temp| &temp.def)) {
            for (number, positions, _) in autoindex_definitions(def) {
                if !format!("sqlite_autoindex_{}_{}", def.name, number).eq_ignore_ascii_case(index) {
                    continue;
                }
                for (seqno, position) in positions.iter().enumerate() {
                    rows.push(vec![
                        Cell::Integer(seqno as i64),
                        Cell::Integer(*position as i64),
                        Cell::Text(def.columns[*position].name.clone()),
                    ]);
                }
            }
        }
    }
    ExecResult { statement: "pragma", columns: vec!["seqno".to_string(), "cid".to_string(), "name".to_string()], rows, affected: 0 }
}

/// `PRAGMA foreign_key_list(t)`: SQLite prepends each new key, so the list is
/// newest first.
fn pragma_foreign_key_list(engine: &Engine, table: &str) -> ExecResult {
    let mut rows = Vec::new();
    if let Some(def) = pragma_table_def(engine, None, table) {
        for (id, key) in def.foreign_keys.iter().rev().enumerate() {
            for (seq, position) in key.columns.iter().enumerate() {
                rows.push(vec![
                    Cell::Integer(id as i64),
                    Cell::Integer(seq as i64),
                    Cell::Text(key.parent_display.clone()),
                    Cell::Text(def.columns[*position].name.clone()),
                    key.parent_columns.get(seq).map(|name| Cell::Text(name.clone())).unwrap_or(Cell::Null),
                    Cell::Text(key.on_update.clone()),
                    Cell::Text(key.on_delete.clone()),
                    Cell::Text("NONE".to_string()),
                ]);
            }
        }
    }
    ExecResult {
        statement: "pragma",
        columns: vec![
            "id".to_string(),
            "seq".to_string(),
            "table".to_string(),
            "from".to_string(),
            "to".to_string(),
            "on_update".to_string(),
            "on_delete".to_string(),
            "match".to_string(),
        ],
        rows,
        affected: 0,
    }
}

/// Whether one parsed expression mentions `column`; `qualifiers` restricts the
/// match to `NEW` / `OLD` references when provided.
fn expression_mentions_column(expr: &Expr, column: &str, qualifiers: Option<&[&str]>) -> bool {
    match expr {
        Expr::Column(reference) => {
            if !reference.name.eq_ignore_ascii_case(column) {
                return false;
            }
            match (reference.qualifier.as_deref(), qualifiers) {
                (None, None) => true,
                (None, Some(_)) => false,
                (Some(qualifier), Some(allowed)) => allowed.iter().any(|allowed| qualifier.eq_ignore_ascii_case(allowed)),
                (Some(_), None) => true,
            }
        }
        Expr::Literal(_) | Expr::Parameter { .. } => false,
        Expr::Unary { value, .. } => expression_mentions_column(value, column, qualifiers),
        Expr::Binary { left, right, .. } | Expr::Compare { left, right, .. } | Expr::And(left, right) | Expr::Or(left, right) => {
            expression_mentions_column(left, column, qualifiers) || expression_mentions_column(right, column, qualifiers)
        }
        Expr::IsDistinct { left, right, .. } => {
            expression_mentions_column(left, column, qualifiers) || expression_mentions_column(right, column, qualifiers)
        }
        Expr::Glob { value, pattern, .. } => {
            expression_mentions_column(value, column, qualifiers) || expression_mentions_column(pattern, column, qualifiers)
        }
        Expr::IsNull { value, .. }
        | Expr::IsTruth { value, .. }
        | Expr::Not(value)
        | Expr::Alias { value, .. }
        | Expr::Cast { value, .. }
        | Expr::Collate { value, .. } => expression_mentions_column(value, column, qualifiers),
        Expr::Case { operand, branches, otherwise } => {
            operand.as_ref().is_some_and(|value| expression_mentions_column(value, column, qualifiers))
                || branches.iter().any(|(condition, value)| {
                    expression_mentions_column(condition, column, qualifiers) || expression_mentions_column(value, column, qualifiers)
                })
                || otherwise.as_ref().is_some_and(|value| expression_mentions_column(value, column, qualifiers))
        }
        Expr::In { value, list, .. } => {
            expression_mentions_column(value, column, qualifiers)
                || list.iter().any(|item| expression_mentions_column(item, column, qualifiers))
        }
        Expr::Function { args, .. } | Expr::HostFunction { args, .. } => {
            args.iter().any(|argument| expression_mentions_column(argument, column, qualifiers))
        }
        Expr::Subquery(statement) | Expr::Exists(statement) => statement_mentions_column(statement, column, qualifiers),
        Expr::InSubquery { value, query, .. } => {
            expression_mentions_column(value, column, qualifiers) || statement_mentions_column(query, column, qualifiers)
        }
        Expr::Aggregate { argument, separator, .. } => {
            argument.as_ref().is_some_and(|value| expression_mentions_column(value, column, qualifiers))
                || separator.as_ref().is_some_and(|value| expression_mentions_column(value, column, qualifiers))
        }
        Expr::Like { value, pattern, escape, .. } => {
            expression_mentions_column(value, column, qualifiers)
                || expression_mentions_column(pattern, column, qualifiers)
                || escape.as_ref().is_some_and(|value| expression_mentions_column(value, column, qualifiers))
        }
        Expr::Window { spec, .. } => window_mentions_column(spec, column, qualifiers),
    }
}

/// Whether one projection mentions the column.
fn select_items_mention_column(items: &SelectItems, column: &str, qualifiers: Option<&[&str]>) -> bool {
    match items {
        SelectItems::Named(items) => items.iter().any(|item| expression_mentions_column(item, column, qualifiers)),
        SelectItems::Star | SelectItems::CountAll => false,
    }
}

/// Whether one window specification mentions the column in its partitioning,
/// ordering or frame offsets.
fn window_mentions_column(spec: &WindowSpec, column: &str, qualifiers: Option<&[&str]>) -> bool {
    let bound = |edge: &FrameBound| edge.offset.as_ref().is_some_and(|value| expression_mentions_column(value, column, qualifiers));
    spec.partition.iter().any(|expr| expression_mentions_column(expr, column, qualifiers))
        || spec.order.iter().any(|key| expression_mentions_column(&key.expr, column, qualifiers))
        || spec.frame.as_ref().is_some_and(|frame| bound(&frame.start) || bound(&frame.end))
}

/// Whether one `SELECT` core mentions the column.
fn core_mentions_column(core: &SelectCore, column: &str, qualifiers: Option<&[&str]>) -> bool {
    select_items_mention_column(&core.items, column, qualifiers)
        || core.from.iter().any(|source| source.query.as_ref().is_some_and(|query| statement_mentions_column(query, column, qualifiers)))
        || core.filter.as_ref().is_some_and(|expr| expression_mentions_column(expr, column, qualifiers))
        || core.group.iter().any(|expr| expression_mentions_column(expr, column, qualifiers))
        || core.having.as_ref().is_some_and(|expr| expression_mentions_column(expr, column, qualifiers))
        || core.windows.iter().any(|window| window_mentions_column(&window.spec, column, qualifiers))
}

/// Whether one statement mentions the column; used to refuse dropping a column
/// still read by a view or a trigger body.
fn statement_mentions_column(statement: &Statement, column: &str, qualifiers: Option<&[&str]>) -> bool {
    match statement {
        Statement::Select { with, first, compounds, order, .. } => {
            with.iter().any(|entry| statement_mentions_column(&entry.query, column, qualifiers))
                || core_mentions_column(first, column, qualifiers)
                || compounds.iter().any(|term| core_mentions_column(&term.core, column, qualifiers))
                || order.iter().any(|key| expression_mentions_column(&key.expr, column, qualifiers))
        }
        Statement::Insert { returning, .. } => returning.as_ref().is_some_and(|items| select_items_mention_column(items, column, qualifiers)),
        Statement::Update { assignments, filter, returning, .. } => {
            assignments.iter().any(|assignment| expression_mentions_column(&assignment.value, column, qualifiers))
                || filter.as_ref().is_some_and(|expr| expression_mentions_column(expr, column, qualifiers))
                || returning.as_ref().is_some_and(|items| select_items_mention_column(items, column, qualifiers))
        }
        Statement::Delete { filter, returning, .. } => {
            filter.as_ref().is_some_and(|expr| expression_mentions_column(expr, column, qualifiers))
                || returning.as_ref().is_some_and(|items| select_items_mention_column(items, column, qualifiers))
        }
        _ => false,
    }
}

/// The error SQLite raises when a parent key is not a unique key of its table.
fn foreign_key_mismatch(child: &TableDef, key: &ForeignKeyDef, statement: &'static str) -> SqlError {
    SqlError::new("SQLITE_ERROR", format!("foreign key mismatch - \"{}\" referencing \"{}\"", child.name, key.parent_display))
        .in_statement(statement)
}

/// The error SQLite raises when a non-NULL child key has no parent row.
fn foreign_key_violation(statement: &'static str) -> SqlError {
    SqlError::new("SQLITE_CONSTRAINT_FOREIGNKEY", "FOREIGN KEY constraint failed").in_statement(statement)
}

/// Selects the cells at `positions` from one decoded row.
fn pick(cells: &[Cell], positions: &[usize]) -> Vec<Cell> {
    positions.iter().map(|position| cells[*position].clone()).collect()
}

/// Whether one child key equals the parent key held by `parent_cells`.
fn keys_equal(child_values: &[Cell], parent_cells: &[Cell], parent_columns: &[usize]) -> bool {
    child_values.iter().zip(parent_columns.iter()).all(|(child, position)| compare_cells(child, &parent_cells[*position]) == Ordering::Equal)
}

/// Copies one pre-statement row and applies an `UPDATE` assignment to it.
fn apply_assignment(old: &[Cell], values: &[UpdateValue]) -> Vec<Cell> {
    let mut next = old.to_vec();
    for (index, value) in values.iter().enumerate() {
        if let UpdateValue::Set(value) = value {
            next[index] = write_value_cell(value);
        }
    }
    next
}

/// The pre-statement cells of one rowid; the plan only contains live rowids.
fn row_of<'a>(rows: &'a [(i64, Vec<Cell>)], rowid: i64) -> &'a [Cell] {
    rows.iter().find(|(id, _)| *id == rowid).map(|(_, cells)| cells.as_slice()).unwrap_or(&[])
}

/// Whether two column-position lists name the same key, ignoring order.
fn same_positions(left: &[usize], right: &[usize]) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_unstable();
    right.sort_unstable();
    left == right
}

/// Builds the row a `CHECK` is evaluated against right after an insert.
///
/// Missing slots take the column default, the rowid alias is filled from the
/// rowid the writer assigned, and one extra cell carries the hidden rowid so a
/// check that names `rowid` reads it.
fn check_row(def: &TableDef, slots: &[Option<crate::WriteValue>], rowid: i64) -> Vec<Cell> {
    let mut cells = Vec::with_capacity(def.columns.len() + 1);
    for (index, slot) in slots.iter().enumerate() {
        let column = &def.columns[index];
        let value = match slot {
            Some(value) => value.clone(),
            None => column.default_value.clone().unwrap_or(crate::WriteValue::Null),
        };
        cells.push(write_value_cell(&crate::apply_write_affinity(&column.declared_type, value)));
    }
    if let Some(alias) = def.rowid_alias {
        if matches!(cells.get(alias), Some(Cell::Null)) {
            cells[alias] = Cell::Integer(rowid);
        }
    }
    cells.push(Cell::Integer(rowid));
    cells
}

/// `INSERT OR IGNORE` 吞掉 UNIQUE / NOT NULL / CHECK 冲突，其余错误照常上报。
fn ignoreable_insert_error(error: &SqlError) -> bool {
    let message = error.message.as_str();
    message.contains("UNIQUE constraint failed")
        || message.contains("NOT NULL constraint failed")
        || message.contains("CHECK constraint failed")
}

/// Reports one `STRICT` storage-class violation: the value is first converted
/// by the column affinity, then has to land in the declared storage class.
fn strict_value_violation(def: &TableDef, position: usize, value: &crate::WriteValue, statement: &'static str) -> Option<SqlError> {
    if !def.strict || matches!(value, crate::WriteValue::Null) {
        return None;
    }
    let column = &def.columns[position];
    let converted = crate::apply_write_affinity(&column.declared_type, value.clone());
    let declared = column.declared_type.to_ascii_uppercase();
    let conforms = match declared.as_str() {
        "INT" | "INTEGER" => matches!(converted, crate::WriteValue::Integer(_)),
        "REAL" => matches!(converted, crate::WriteValue::Real(_)),
        "TEXT" => matches!(converted, crate::WriteValue::Text(_)),
        "BLOB" => matches!(converted, crate::WriteValue::Blob(_)),
        _ => true,
    };
    if conforms {
        return None;
    }
    let stored = match converted {
        crate::WriteValue::Real(_) => "REAL",
        crate::WriteValue::Text(_) => "TEXT",
        crate::WriteValue::Blob(_) => "BLOB",
        _ => "INTEGER",
    };
    Some(
        SqlError::new("constraint-datatype", format!("cannot store {stored} value in {declared} column {}.{}", def.name, column.name))
            .in_statement(statement),
    )
}

fn cell_write_value(cell: &Cell) -> crate::WriteValue {
    match cell {
        Cell::Null => crate::WriteValue::Null,
        Cell::Integer(value) => crate::WriteValue::Integer(*value),
        Cell::Real(value) => crate::WriteValue::Real(*value),
        Cell::Text(value) => crate::WriteValue::Text(value.clone()),
        Cell::Blob(value) => crate::WriteValue::Blob(value.clone()),
    }
}

/// Inserts one row into an in-memory temp table and returns its rowid.
///
/// The rowid is explicit when the table has an `INTEGER PRIMARY KEY` alias and
/// the statement supplied one, and `max(rowid) + 1` otherwise. NOT NULL and the
/// primary key / UNIQUE keys are enforced here because temp tables never reach
/// the format-3 writer.
fn temp_insert_row(
    def: &TableDef,
    rows: &mut BTreeMap<i64, Vec<Cell>>,
    slots: &[Option<crate::WriteValue>],
    keys: &[UniqueKey],
) -> Result<i64, SqlError> {
    let mut rowid = rows.keys().next_back().copied().unwrap_or(0) + 1;
    if let Some(alias) = def.rowid_alias {
        match slots.get(alias).and_then(|slot| slot.as_ref()) {
            Some(crate::WriteValue::Integer(value)) => rowid = *value,
            Some(crate::WriteValue::Null) | None => {}
            Some(_) => return Err(SqlError::new("datatype-mismatch", "datatype mismatch").in_statement("insert")),
        }
    }
    let cells = check_row(def, slots, rowid);
    // `check_row` appends the rowid as a hidden slot for CHECK/RETURNING
    // evaluation; the stored temp row keeps only the declared columns and the
    // row map key carries the rowid.
    let cells: Vec<Cell> = cells.into_iter().take(def.columns.len()).collect();
    for (position, column) in def.columns.iter().enumerate() {
        if column.not_null && matches!(cells[position], Cell::Null) {
            return Err(
                SqlError::new("not-null-constraint", format!("NOT NULL constraint failed: {}.{}", def.name, column.name)).in_statement("insert")
            );
        }
    }
    let live: Vec<(i64, Vec<Cell>)> = rows.iter().map(|(rowid, cells)| (*rowid, cells.clone())).collect();
    if let Some(label) = unique_conflict(keys, &cells, rowid, &live, i64::MIN) {
        return Err(SqlError::new("unique-constraint", format!("UNIQUE constraint failed: {label}")).in_statement("insert"));
    }
    rows.insert(rowid, cells);
    Ok(rowid)
}

/// Declared column positions of a managed index whose terms are all plain
/// columns, or `None` for a real expression key.
fn managed_term_positions(index: &IndexDef) -> Option<Vec<usize>> {
    index
        .terms
        .iter()
        .map(|term| match term {
            ResolvedExpr::Column { source: 0, index: column } => Some(*column),
            _ => None,
        })
        .collect()
}

/// Whether one managed index is the one an `ON CONFLICT (columns)` target names.
fn managed_target_matches(target: &ManagedTarget, positions: &[usize], predicate: Option<&ResolvedExpr>) -> bool {
    if target.index.predicate.as_ref() != predicate {
        return false;
    }
    match managed_term_positions(&target.index) {
        Some(terms) => !terms.is_empty() && same_positions(&terms, positions),
        None => false,
    }
}

/// `t.a, t.b` fragment of a managed index, matching SQLite's uniqueness text.
fn managed_index_label(def: &TableDef, index: &IndexDef) -> String {
    match managed_term_positions(index) {
        Some(positions) if !positions.is_empty() => {
            positions.iter().map(|position| format!("{}.{}", def.name, def.columns[*position].name)).collect::<Vec<_>>().join(", ")
        }
        _ => format!("index '{}'", index.name),
    }
}

/// Whether two computed managed-index keys collide, with SQLite's rule that a
/// `NULL` component never conflicts.
fn managed_keys_conflict(first: &[Cell], second: &[Cell], collations: &[Collation]) -> bool {
    first.len() == second.len()
        && first.iter().zip(second).enumerate().all(|(position, (left, right))| {
            !matches!(left, Cell::Null)
                && !matches!(right, Cell::Null)
                && compare_cells_with(left, right, collations.get(position).copied().unwrap_or_default()) == Ordering::Equal
        })
}

/// Whether one supplied upsert row collides with one stored row on `key`.
///
/// A `NULL` in any key column never conflicts, exactly like a unique index.
fn upsert_key_conflicts(key: &UniqueKey, candidate: &[Cell], candidate_rowid: i64, existing_rowid: i64, existing: &[Cell]) -> bool {
    match key {
        UniqueKey::Rowid(_) => candidate_rowid == existing_rowid,
        UniqueKey::Columns(positions, _, collations) => positions.iter().enumerate().all(|(index, position)| {
            let first = &candidate[*position];
            let second = &existing[*position];
            !matches!(first, Cell::Null)
                && !matches!(second, Cell::Null)
                && compare_cells_with(first, second, collations.get(index).copied().unwrap_or_default()) == Ordering::Equal
        }),
    }
}

/// Reports the first uniqueness rule the candidate row violates against the
/// live table, skipping the row being replaced.
fn unique_conflict(keys: &[UniqueKey], candidate: &[Cell], rowid: i64, live: &[(i64, Vec<Cell>)], skip: i64) -> Option<String> {
    for key in keys {
        let wanted = key.values(candidate, rowid);
        for (other_rowid, other_cells) in live {
            if *other_rowid == skip {
                continue;
            }
            let other = key.values(other_cells, *other_rowid);
            let collations = match key {
                UniqueKey::Rowid(_) => &[][..],
                UniqueKey::Columns(_, _, collations) => collations.as_slice(),
            };
            let collides = wanted.iter().zip(&other).enumerate().all(|(index, (first, second))| {
                !matches!(first, Cell::Null)
                    && !matches!(second, Cell::Null)
                    && compare_cells_with(first, second, collations.get(index).copied().unwrap_or_default()) == Ordering::Equal
            });
            if collides {
                return Some(key.label().to_string());
            }
        }
    }
    None
}

/// Converts a storage rejection of an `UPDATE` / `DELETE` into a structured
/// error, naming the table when the writer does not say which key failed.
fn write_failure(error: database_types::Error, statement: &'static str, def: &TableDef) -> SqlError {
    let mapped = storage_failure(error);
    let message = if mapped.code == "unique-constraint" {
        format!("UNIQUE constraint failed in table `{}`", def.name)
    }
    else if mapped.code == "not-null-constraint" || mapped.message.contains("NOT NULL") {
        format!("NOT NULL constraint failed in table `{}`", def.name)
    }
    else if mapped.message.contains("integer") || mapped.message.contains("datatype") {
        return SqlError::new("datatype-mismatch", "datatype mismatch").in_statement(statement);
    }
    else {
        mapped.message
    };
    SqlError::new(mapped.code, message).in_statement(statement)
}

/// 管理型索引键里是否存在按排序规则等价的重复项，`NULL` 键不算冲突。
fn managed_unique_violation(entries: &[(Vec<crate::WriteValue>, i64)], collations: &[Collation]) -> bool {
    for right in 1..entries.len() {
        for left in 0..right {
            let collides = entries[left].0.iter().zip(&entries[right].0).enumerate().all(|(position, (first, second))| {
                if matches!(first, crate::WriteValue::Null) || matches!(second, crate::WriteValue::Null) {
                    return false;
                }
                let collation = collations.get(position).copied().unwrap_or_default();
                match (first, second) {
                    (crate::WriteValue::Text(first), crate::WriteValue::Text(second)) => collation.compare_text(first, second).is_eq(),
                    _ => first == second,
                }
            });
            if collides {
                return true;
            }
        }
    }
    false
}

fn write_value_cell(value: &crate::WriteValue) -> Cell {
    match value {
        crate::WriteValue::Null => Cell::Null,
        crate::WriteValue::Integer(value) => Cell::Integer(*value),
        crate::WriteValue::Real(value) => Cell::Real(*value),
        crate::WriteValue::Text(value) => Cell::Text(value.clone()),
        crate::WriteValue::Blob(value) => Cell::Blob(value.clone()),
    }
}

fn decode_row(payload: &[u8], def: &TableDef, rowid: i64) -> Result<Vec<Cell>, SqlError> {
    decode_row_masked(payload, def, rowid, None)
}

/// Decodes one stored row, keeping only the declared columns `keep` flags.
///
/// A skipped position still walks its serial type and body bounds, so a corrupt
/// payload fails exactly as in the full decode, but its text or blob value is
/// never materialized. `None` keeps every column.
fn decode_row_masked(payload: &[u8], def: &TableDef, rowid: i64, keep: Option<&[bool]>) -> Result<Vec<Cell>, SqlError> {
    let mut cells = decode_cells_kept(payload, keep)?;
    // A record written before `ALTER TABLE ADD COLUMN` is short; SQLite reads
    // the missing trailing columns as their declared default.
    while cells.len() < def.columns.len() {
        let kept = keep.is_none_or(|mask| mask.get(cells.len()).copied().unwrap_or(false));
        let value = if kept {
            let column = &def.columns[cells.len()];
            let value = column.default_value.clone().unwrap_or(crate::WriteValue::Null);
            write_value_cell(&crate::apply_write_affinity(&column.declared_type, value))
        }
        else {
            Cell::Null
        };
        cells.push(value);
    }
    cells.truncate(def.columns.len());
    if let Some(index) = def.rowid_alias {
        if matches!(cells[index], Cell::Null) {
            cells[index] = Cell::Integer(rowid);
        }
    }
    Ok(cells)
}

/// Whether a decoded row set made with `broad` already carries every column a
/// `narrow` request needs. `None` is the full decode and covers any request.
fn mask_covers(broad: Option<&[bool]>, narrow: Option<&[bool]>) -> bool {
    match (broad, narrow) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(broad), Some(narrow)) => {
            narrow.iter().enumerate().all(|(position, needed)| !*needed || broad.get(position).copied().unwrap_or(false))
        }
    }
}

/// 一次惰性游标读取返回的行窗口。
pub(crate) struct LazyBatch {
    /// 本窗口的行,保持扫描顺序。
    pub(crate) rows: Vec<Vec<Cell>>,
    /// 扫描是否已经到达表末尾。
    pub(crate) done: bool,
}

/// 一个已经解析好的惰性单表扫描计划。
///
/// 计划里的表达式都已解析成 owned 形态,不借用引擎目录;执行期只需要逐行
/// 求值。整页快照与 b-tree 遍历状态在 [`LazyTableScan`] 里。
pub(crate) struct LazyCursor {
    plan: LazyPlan,
    scan: LazyTableScan,
    budget: Budget,
    parameters: Vec<Cell>,
    finished: bool,
}

impl LazyCursor {
    /// 打开时推导出的结果列标签。
    pub(crate) fn columns(&self) -> &[String] {
        &self.plan.labels
    }
}

struct LazyPlan {
    /// 被扫描的表定义,用于解码存储记录。
    def: TableDef,
    /// 结果列标签。
    labels: Vec<String>,
    /// `SELECT *` 暴露的列数,不含隐藏 rowid。
    visible_width: usize,
    /// 命名投影;`None` 表示 `SELECT *`。
    projection: Option<Vec<ResolvedExpr>>,
    /// 逐行 `WHERE` 谓词。
    predicate: Option<ResolvedExpr>,
    /// 求值上下文里的列目录。
    shape_columns: Vec<(String, String)>,
    /// 解码时只需保留的列。
    mask: Option<Vec<bool>>,
}

/// 按表 b-tree 页顺序分批解码的扫描器。
///
/// `pages` 是打开游标时克隆的整页快照,后续写入只改引擎自己的页缓冲,因此
/// 扫描期间看到的内容始终是打开时刻的一致状态。内部页只用来定位叶子页,
/// 每片叶子页的载荷按行解码后进入队列,`next_rows` 每次只取要求的行数。
struct LazyTableScan {
    pages: Arc<Vec<u8>>,
    page_size: usize,
    usable: usize,
    page_count: usize,
    /// 尚未展开的页,栈顶是下一个要访问的子树。
    pending: Vec<u32>,
    /// 当前叶子页尚未返回的行。
    leaf: VecDeque<TableRow>,
    /// 页号→是否已访问,与存储层扫描同样的重复页检测。
    visited: Vec<bool>,
    visited_pages: usize,
    limits: TableLimits,
    total_payload: usize,
    total_rows: usize,
    last_rowid: Option<i64>,
    done: bool,
}

impl LazyTableScan {
    fn new(pages: Arc<Vec<u8>>, root_page: u32) -> Result<Self, SqlError> {
        if pages.is_empty() {
            return Err(corrupt_error("sqlite table root in empty database"));
        }
        let page_size = validate_database(&pages).map_err(storage_failure)?;
        let reserved = usize::from(*pages.get(20).ok_or_else(|| corrupt_error("sqlite header truncated"))?);
        let usable = page_size.checked_sub(reserved).filter(|usable| *usable >= 257).ok_or_else(|| corrupt_error("sqlite page size invalid"))?;
        let page_count = pages.len() / page_size;
        if page_count == 0 || root_page == 0 || root_page as usize > page_count {
            return Err(corrupt_error("sqlite table root page out of bounds"));
        }
        Ok(Self {
            pages,
            page_size,
            usable,
            page_count,
            pending: vec![root_page],
            leaf: VecDeque::new(),
            visited: vec![false; page_count + 1],
            visited_pages: 0,
            limits: TableLimits::default(),
            total_payload: 0,
            total_rows: 0,
            last_rowid: None,
            done: false,
        })
    }

    /// 取至多 `limit` 行,不足时继续展开后续叶子页。
    fn next_rows(&mut self, limit: usize) -> Result<Vec<TableRow>, SqlError> {
        let mut rows = Vec::new();
        while rows.len() < limit {
            match self.leaf.pop_front() {
                Some(row) => rows.push(row),
                None => {
                    if !self.load_leaf()? {
                        break;
                    }
                }
            }
        }
        Ok(rows)
    }

    /// 展开内部页直到拿到一片叶子页;整棵树读完时返回 `false`。
    fn load_leaf(&mut self) -> Result<bool, SqlError> {
        loop {
            let Some(number) = self.pending.pop()
            else {
                self.done = true;
                return Ok(false);
            };
            let pages = Arc::clone(&self.pages);
            let page = lazy_page(&pages, self.page_size, self.page_count, number)?;
            if self.visited_pages >= self.limits.max_pages {
                return Err(storage_error("sqlite table page limit exceeded"));
            }
            if self.visited[number as usize] {
                return Err(corrupt_error("sqlite repeated b-tree or overflow page"));
            }
            self.visited[number as usize] = true;
            self.visited_pages += 1;
            let header = if number == 1 { 100 } else { 0 };
            let kind = *page.get(header).ok_or_else(|| corrupt_error("sqlite b-tree header truncated"))?;
            let header_size = match kind {
                13 => 8,
                5 => 12,
                _ => return Err(corrupt_error("sqlite table b-tree page type invalid")),
            };
            let count = lazy_read_u16(page, header + 3)?;
            let pointers_end = header + header_size + count.saturating_mul(2);
            if pointers_end > page.len() || *page.get(header + 7).ok_or_else(|| corrupt_error("sqlite b-tree header truncated"))? > 60 {
                return Err(corrupt_error("sqlite b-tree header invalid"));
            }
            let raw_start = lazy_read_u16(page, header + 5)?;
            let content_start = if raw_start == 0 { 65536 } else { raw_start };
            if content_start < pointers_end || content_start > page.len() {
                return Err(corrupt_error("sqlite b-tree content boundary invalid"));
            }
            let mut children = Vec::new();
            let mut rows = Vec::new();
            for index in 0..count {
                let cell = lazy_read_u16(page, header + header_size + index.saturating_mul(2))?;
                if cell < content_start || cell >= page.len() {
                    return Err(corrupt_error("sqlite cell pointer out of bounds"));
                }
                if kind == 5 {
                    let child = lazy_read_u32(page, cell)?;
                    let tail = page.get(cell + 4..).ok_or_else(|| corrupt_error("sqlite interior cell truncated"))?;
                    decode_varint(tail).map_err(storage_failure)?;
                    children.push(child);
                    continue;
                }
                let (length, length_size) = decode_varint(&page[cell..]).map_err(storage_failure)?;
                let length = usize::try_from(length).map_err(|_| storage_error("sqlite payload size exceeds host"))?;
                if length > self.limits.max_payload_bytes {
                    return Err(storage_error("sqlite table payload limit exceeded"));
                }
                let total = self.total_payload.checked_add(length).ok_or_else(|| storage_error("sqlite table total payload overflow"))?;
                if total > self.limits.max_total_payload_bytes {
                    return Err(storage_error("sqlite table total payload limit exceeded"));
                }
                self.total_payload = total;
                if self.total_rows >= self.limits.max_rows {
                    return Err(storage_error("sqlite table row limit exceeded"));
                }
                let tail = page.get(cell + length_size..).ok_or_else(|| corrupt_error("sqlite table cell truncated"))?;
                let (rowid, rowid_size) = decode_varint(tail).map_err(storage_failure)?;
                let rowid = rowid as i64;
                if self.last_rowid.is_some_and(|last| rowid <= last) {
                    return Err(corrupt_error("sqlite table rows out of order"));
                }
                self.last_rowid = Some(rowid);
                self.total_rows += 1;
                let start = cell.checked_add(length_size).and_then(|start| start.checked_add(rowid_size)).ok_or_else(|| corrupt_error("sqlite cell length overflow"))?;
                let payload = lazy_cell_payload(
                    &pages,
                    self.page_size,
                    self.page_count,
                    self.usable,
                    page,
                    start,
                    length,
                    &mut self.visited,
                    &mut self.visited_pages,
                    &self.limits,
                )?;
                rows.push(TableRow { rowid, payload });
            }
            if kind == 5 {
                // 内部页最后还有一个指向最右子树的指针,它不属于任何 cell。
                children.push(lazy_read_u32(page, header + 8)?);
                for child in children.into_iter().rev() {
                    self.pending.push(child);
                }
            }
            else {
                self.leaf.extend(rows);
                if !self.leaf.is_empty() {
                    return Ok(true);
                }
            }
        }
    }
}

/// 判断 `ORDER BY` 是否与 b-tree 的 rowid 升序扫描完全一致。
///
/// 只接受 `*` 或裸列投影,避免投影别名改变 `ORDER BY` 的绑定目标;排序键
/// 必须是隐式 rowid 或 `INTEGER PRIMARY KEY` 别名,且不能 `DESC` 或覆盖
/// `NULLS` 顺序。
fn lazy_order_matches_scan(def: &TableDef, entry: &TableRef, items: &SelectItems, order: &[OrderKey]) -> bool {
    if order.is_empty() {
        return true;
    }
    if !matches!(items, SelectItems::Star)
        && !matches!(items, SelectItems::Named(exprs) if exprs.iter().all(|expr| matches!(expr, Expr::Column(_))))
    {
        return false;
    }
    let alias_name = def.rowid_alias.map(|index| def.columns[index].name.as_str());
    let shadowed = |name: &str| def.columns.iter().any(|column| column.name.eq_ignore_ascii_case(name));
    for key in order {
        if key.descending || key.nulls_first.is_some() {
            return false;
        }
        let Expr::Column(column) = &key.expr
        else {
            return false;
        };
        if column.qualifier.as_deref().is_some_and(|qualifier| !qualifier.eq_ignore_ascii_case(entry.label())) {
            return false;
        }
        let names_alias = alias_name.is_some_and(|name| column.name.eq_ignore_ascii_case(name));
        let names_hidden_rowid = is_rowid_alias(&column.name) && !shadowed(&column.name);
        if !names_alias && !names_hidden_rowid {
            return false;
        }
    }
    true
}

/// 取一页完整内容;页号越界按损坏处理。
fn lazy_page(pages: &[u8], page_size: usize, page_count: usize, number: u32) -> Result<&[u8], SqlError> {
    let index = number as usize;
    if index == 0 || index > page_count {
        return Err(corrupt_error("sqlite page reference out of bounds"));
    }
    let start = (index - 1) * page_size;
    pages.get(start..start + page_size).ok_or_else(|| corrupt_error("sqlite page truncated"))
}

/// 与上游 SQLite 的 U 值公式一致,返回载荷在本页存放的本地字节数。
fn lazy_local(usable: usize, length: usize) -> usize {
    let maximum = usable - 35;
    let minimum = ((usable - 12) * 32 / 255) - 23;
    if length <= maximum {
        return length;
    }
    let candidate = minimum + (length - minimum) % (usable - 4);
    if candidate <= maximum { candidate } else { minimum }
}

/// 组装一行载荷,溢出页按链读取,并记录访问过的溢出页。
#[allow(clippy::too_many_arguments)]
fn lazy_cell_payload(
    pages: &[u8],
    page_size: usize,
    page_count: usize,
    usable: usize,
    page: &[u8],
    offset: usize,
    length: usize,
    visited: &mut [bool],
    visited_pages: &mut usize,
    limits: &TableLimits,
) -> Result<Vec<u8>, SqlError> {
    let local = lazy_local(usable, length);
    let end = offset.checked_add(local).ok_or_else(|| corrupt_error("sqlite cell length overflow"))?;
    let initial = page.get(offset..end).ok_or_else(|| corrupt_error("sqlite cell payload truncated"))?;
    let mut payload = Vec::new();
    payload.try_reserve_exact(length).map_err(|_| storage_error("sqlite payload allocation failed"))?;
    payload.extend_from_slice(initial);
    if local == length {
        return Ok(payload);
    }
    let mut next = lazy_read_u32(page, end)?;
    while payload.len() < length {
        if *visited_pages >= limits.max_pages {
            return Err(storage_error("sqlite table page limit exceeded"));
        }
        let index = next as usize;
        if index == 0 || index > page_count {
            return Err(corrupt_error("sqlite page reference out of bounds"));
        }
        if visited[index] {
            return Err(corrupt_error("sqlite repeated b-tree or overflow page"));
        }
        visited[index] = true;
        *visited_pages += 1;
        let start = (index - 1) * page_size;
        let overflow = pages.get(start..start + usable).ok_or_else(|| corrupt_error("sqlite page truncated"))?;
        next = lazy_read_u32(overflow, 0)?;
        let count = (length - payload.len()).min(usable - 4);
        payload.extend_from_slice(overflow.get(4..4 + count).ok_or_else(|| corrupt_error("sqlite overflow payload truncated"))?);
    }
    if next != 0 {
        return Err(corrupt_error("sqlite overflow chain exceeds payload"));
    }
    Ok(payload)
}

fn lazy_read_u16(bytes: &[u8], offset: usize) -> Result<usize, SqlError> {
    let value = bytes.get(offset..offset + 2).ok_or_else(|| corrupt_error("sqlite b-tree field truncated"))?;
    Ok(usize::from(u16::from_be_bytes(value.try_into().expect("two bytes"))))
}

fn lazy_read_u32(bytes: &[u8], offset: usize) -> Result<u32, SqlError> {
    let value = bytes.get(offset..offset + 4).ok_or_else(|| corrupt_error("sqlite page field truncated"))?;
    Ok(u32::from_be_bytes(value.try_into().expect("four bytes")))
}

/// Decodes stored row payloads into merged rows, appending each hidden rowid.
fn decode_stored_rows(stored: &[TableRow], def: &TableDef, keep: Option<&[bool]>) -> Result<Vec<Vec<Cell>>, SqlError> {
    stored
        .iter()
        .map(|row| {
            let mut cells = decode_row_masked(&row.payload, def, row.rowid, keep)?;
            // Rowid tables expose the implicit integer primary key through
            // `rowid`, `_rowid_` and `oid`.
            cells.push(Cell::Integer(row.rowid));
            Ok(cells)
        })
        .collect()
}

/// Collects the column references one expression can read, refusing any node
/// whose reads cannot be attributed from the raw AST.
///
/// Subqueries may read the enclosing row through a correlation, and window
/// functions carry their own partition and order expressions; both keep the
/// full decode instead of guessing.
fn collect_unmasked_columns<'a>(expr: &'a Expr, out: &mut Vec<&'a ColumnRef>) -> bool {
    match expr {
        Expr::Column(column) => {
            out.push(column);
            true
        }
        Expr::Literal(_) | Expr::Parameter { .. } => true,
        Expr::Unary { value, .. }
        | Expr::IsNull { value, .. }
        | Expr::IsTruth { value, .. }
        | Expr::Not(value)
        | Expr::Cast { value, .. }
        | Expr::Collate { value, .. }
        | Expr::Alias { value, .. } => collect_unmasked_columns(value, out),
        Expr::Binary { left, right, .. }
        | Expr::Compare { left, right, .. }
        | Expr::IsDistinct { left, right, .. }
        | Expr::Glob { value: left, pattern: right, .. } => collect_unmasked_columns(left, out) && collect_unmasked_columns(right, out),
        Expr::And(left, right) | Expr::Or(left, right) => collect_unmasked_columns(left, out) && collect_unmasked_columns(right, out),
        Expr::Case { operand, branches, otherwise } => {
            operand.as_ref().is_none_or(|operand| collect_unmasked_columns(operand, out))
                && branches.iter().all(|(condition, result)| collect_unmasked_columns(condition, out) && collect_unmasked_columns(result, out))
                && otherwise.as_ref().is_none_or(|otherwise| collect_unmasked_columns(otherwise, out))
        }
        Expr::In { value, list, .. } => collect_unmasked_columns(value, out) && list.iter().all(|item| collect_unmasked_columns(item, out)),
        Expr::Function { args, .. } | Expr::HostFunction { args, .. } => args.iter().all(|arg| collect_unmasked_columns(arg, out)),
        Expr::Aggregate { argument, separator, filter, order, .. } => {
            argument.as_ref().is_none_or(|argument| collect_unmasked_columns(argument, out))
                && separator.as_ref().is_none_or(|separator| collect_unmasked_columns(separator, out))
                && filter.as_ref().is_none_or(|filter| collect_unmasked_columns(filter, out))
                && order.iter().all(|key| collect_unmasked_columns(&key.expr, out))
        }
        Expr::Like { value, pattern, escape, .. } => {
            collect_unmasked_columns(value, out)
                && collect_unmasked_columns(pattern, out)
                && escape.as_ref().is_none_or(|escape| collect_unmasked_columns(escape, out))
        }
        Expr::Subquery(_) | Expr::Exists(_) | Expr::InSubquery { .. } | Expr::Window { .. } => false,
    }
}

/// Decodes one record payload, materializing only the positions `keep` flags.
///
/// Mirrors the storage record decoder for layout validation (header and body
/// bounds, reserved serial types, column limit, trailing bytes) while skipping
/// the text and blob conversion of columns the statement never reads.
fn decode_cells_kept(payload: &[u8], keep: Option<&[bool]>) -> Result<Vec<Cell>, SqlError> {
    let limits = RecordLimits { max_columns: 1024, max_payload_bytes: 16 * 1024 * 1024 };
    let corrupt = |message: &str| SqlError::new("corrupt", message).in_statement("select");
    let limit = |message: &str| SqlError::new("unsupported", message).in_statement("select");
    if payload.len() > limits.max_payload_bytes {
        return Err(limit("sqlite record payload limit exceeded"));
    }
    let (header_size, mut header_offset) = decode_varint(payload).map_err(|error| corrupt(&error.to_string()))?;
    let header_size = usize::try_from(header_size).map_err(|_| corrupt("sqlite record header size overflow"))?;
    if header_size < header_offset || header_size > payload.len() {
        return Err(corrupt("sqlite record header size invalid"));
    }
    let mut body_offset = header_size;
    let mut cells = Vec::new();
    while header_offset < header_size {
        if cells.len() >= limits.max_columns {
            return Err(limit("sqlite record column limit exceeded"));
        }
        let (serial, consumed) = decode_varint(&payload[header_offset..header_size]).map_err(|error| corrupt(&error.to_string()))?;
        header_offset += consumed;
        let length = match serial {
            0 | 8 | 9 => 0,
            1 => 1,
            2 => 2,
            3 => 3,
            4 => 4,
            5 => 6,
            6 | 7 => 8,
            10 | 11 => return Err(corrupt("sqlite reserved serial type")),
            serial => usize::try_from((serial - 12) / 2).map_err(|_| corrupt("sqlite serial length overflow"))?,
        };
        let end = body_offset.checked_add(length).filter(|end| *end <= payload.len()).ok_or_else(|| corrupt("truncated sqlite record body"))?;
        let bytes = &payload[body_offset..end];
        let position = cells.len();
        let kept = keep.is_none_or(|mask| mask.get(position).copied().unwrap_or(false));
        cells.push(if kept {
            match serial {
                0 => Cell::Null,
                1..=6 => Cell::Integer(signed_record_integer(bytes)),
                7 => Cell::Real(f64::from_bits(u64::from_be_bytes(bytes.try_into().expect("eight-byte real")))),
                8 => Cell::Integer(0),
                9 => Cell::Integer(1),
                serial if serial % 2 == 0 => Cell::Blob(bytes.to_vec()),
                _ => Cell::Text(record_text(bytes)?),
            }
        }
        else {
            Cell::Null
        });
        body_offset = end;
    }
    if body_offset != payload.len() {
        return Err(corrupt("sqlite record has trailing body bytes"));
    }
    Ok(cells)
}

/// Decodes one record text field exactly as the storage decoder does.
fn record_text(bytes: &[u8]) -> Result<String, SqlError> {
    std::str::from_utf8(bytes).map(str::to_owned).map_err(|_| SqlError::new("corrupt", "sqlite non-unicode UTF-8 text").in_statement("select"))
}

/// Sign-extends a big-endian SQLite integer field of one to eight bytes.
fn signed_record_integer(bytes: &[u8]) -> i64 {
    let mut value = if bytes[0] & 0x80 != 0 { u64::MAX } else { 0 };
    for byte in bytes {
        value = (value << 8) | u64::from(*byte);
    }
    value as i64
}

/// Decodes a raw record into cells without expanding an implicit rowid alias.
fn decode_cells(payload: &[u8]) -> Result<Vec<Cell>, SqlError> {
    let limits = RecordLimits { max_columns: 1024, max_payload_bytes: 16 * 1024 * 1024 };
    let values = decode_record(payload, TextEncoding::Utf8, limits)
        .map_err(|error| storage_failure(error).in_statement("select"))?;
    // 文本字段直接移交解码结果里的 `String`,不再多复制一次。
    Ok(values
        .into_iter()
        .map(|value| match value {
            crate::RecordValue::Null => Cell::Null,
            crate::RecordValue::Integer(value) => Cell::Integer(value),
            crate::RecordValue::Real(value) => Cell::Real(value),
            crate::RecordValue::Text(text) => Cell::Text(text),
            crate::RecordValue::Blob(bytes) => Cell::Blob(bytes.to_vec()),
        })
        .collect())
}

/// Serializes results as a JSON array for the wasm host.
pub fn results_json(results: &[ExecResult]) -> String {
    let mut out = String::from("[");
    for (index, result) in results.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&result.to_json());
    }
    out.push(']');
    out
}

fn cell_json(cell: &Cell) -> String {
    match cell {
        Cell::Null => "null".to_string(),
        Cell::Integer(value) => value.to_string(),
        Cell::Real(value) => {
            if value.is_finite() {
                // Keep the value visibly real (`1.0`, `9.223372036854776e18`)
                // so a whole-valued real never degrades into an integer when
                // the JSON envelope is parsed back.
                format!("{value:?}")
            }
            else {
                "null".to_string()
            }
        }
        Cell::Text(value) => json_string(value),
        Cell::Blob(bytes) => {
            let mut hex = String::with_capacity(bytes.len() * 2 + 12);
            hex.push_str("{\"$blob\":\"");
            for byte in bytes {
                hex.push_str(&format!("{byte:02x}"));
            }
            hex.push_str("\"}");
            hex
        }
    }
}

/// One step of the schematic program returned by `EXPLAIN`.
struct ProgramStep {
    opcode: &'static str,
    p1: i64,
    p2: i64,
    p4: Option<String>,
    p5: i64,
}

impl ProgramStep {
    fn plain(opcode: &'static str, p1: i64) -> Self {
        Self { opcode, p1, p2: 0, p4: None, p5: 0 }
    }

    fn next(p1: i64) -> Self {
        Self { opcode: "Next", p1, p2: 0, p4: None, p5: 1 }
    }

    fn read(cursor: i64, label: String) -> Self {
        Self { opcode: "OpenRead", p1: cursor, p2: 0, p4: Some(label), p5: 0 }
    }

    fn write(name: &str) -> Self {
        Self { opcode: "OpenWrite", p1: 0, p2: 0, p4: Some(name.to_string()), p5: 0 }
    }

    fn named(opcode: &'static str, name: &str) -> Self {
        Self { opcode, p1: 0, p2: 0, p4: Some(name.to_string()), p5: 0 }
    }
}

/// `FROM` label used by `EXPLAIN` rows. A derived table has no name, so the
/// empty label becomes `(subquery)` to stay readable.
fn table_ref_label(table: &TableRef) -> String {
    match table.label() {
        label if label.is_empty() => "(subquery)".to_string(),
        label => label.to_string(),
    }
}

fn push_plan_row(rows: &mut Vec<Vec<Cell>>, next_id: &mut i64, detail: &str) {
    rows.push(vec![Cell::Integer(*next_id), Cell::Integer(0), Cell::Integer(0), Cell::Text(detail.to_string())]);
    *next_id += 1;
}

fn program_row(addr: i64, opcode: &str, p1: i64, p2: i64, p3: i64, p4: Option<&str>, p5: i64) -> Vec<Cell> {
    vec![
        Cell::Integer(addr),
        Cell::Text(opcode.to_string()),
        Cell::Integer(p1),
        Cell::Integer(p2),
        Cell::Integer(p3),
        p4.map(|p4| Cell::Text(p4.to_string())).unwrap_or(Cell::Null),
        Cell::Integer(p5),
        Cell::Null,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use database_udf::Result as UdfResult;
    use database_udf::{HostFunctionHandle, UdfError, UdfType, UdfValue};

    #[test]
    fn from_writer_reopens_an_existing_image() {
        let path = std::env::temp_dir().join(format!("sqlite-engine-reopen-{}.db", std::process::id()));
        let mut engine = Engine::new(None).expect("blank engine");
        engine
            .execute_batch(&[
                "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)".to_string(),
                "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))".to_string(),
                "CREATE INDEX child_parent ON child(parent_id)".to_string(),
                "INSERT INTO parent (id, name) VALUES (1, 'ada')".to_string(),
                "INSERT INTO child (id, parent_id) VALUES (1, 1)".to_string(),
            ])
            .expect("seed image");
        let image = engine.snapshot().expect("image").to_vec();
        std::fs::write(&path, &image).expect("write image");
        drop(engine);

        let writer = SqliteWriter::open(&path).expect("open image");
        let mut reopened = Engine::from_writer(writer).expect("rebuild catalog");
        assert_eq!(
            reopened.execute_one("SELECT id, name FROM parent").expect("parent read").rows,
            vec![vec![Cell::Integer(1), Cell::Text("ada".to_string())]]
        );
        assert_eq!(reopened.execute_one("SELECT count(*) FROM child").expect("child read").rows, vec![vec![Cell::Integer(1)]]);
        // The rebuilt catalog still carries the foreign key, so an orphan is
        // rejected exactly as before the file was closed.
        assert_eq!(
            reopened.execute_one("INSERT INTO child (id, parent_id) VALUES (2, 9)").expect_err("orphan insert").message,
            "FOREIGN KEY constraint failed"
        );
        // Reading a reopened image never rewrites it.
        assert_eq!(reopened.snapshot().expect("image"), image.as_slice());
        std::fs::remove_file(&path).expect("cleanup image");
    }

    /// `DISTINCT` 聚合参数必须使用参数表达式的生效排序规则，而不是二进制比较。
    #[test]
    fn aggregate_distinct_honours_argument_collation() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine
            .execute_batch(&[
                "CREATE TABLE t(s TEXT COLLATE NOCASE)".to_string(),
                "INSERT INTO t VALUES ('a'), ('A'), ('b'), ('B')".to_string(),
            ])
            .expect("seed image");
        let result = engine.execute_one("SELECT count(DISTINCT s), count(s) FROM t").expect("aggregate executes");
        assert_eq!(result.rows, vec![vec![Cell::Integer(2), Cell::Integer(4)]]);
        let joined = engine.execute_one("SELECT group_concat(DISTINCT s) FROM t").expect("group_concat executes");
        assert_eq!(joined.rows, vec![vec![Cell::Text("a,b".to_string())]]);
    }

    /// `PRAGMA integrity_check` walks the image rather than echoing a fixed
    /// answer, so it has to stay `ok` across tables, indexes and freed pages.
    #[test]
    fn integrity_check_reports_ok_for_an_image_with_a_freelist() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine
            .execute_batch(&[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE)".to_string(),
                "CREATE INDEX t_b ON t(b)".to_string(),
                "INSERT INTO t VALUES (1, 'x'), (2, 'y')".to_string(),
                "CREATE TABLE dropped(z TEXT)".to_string(),
                "INSERT INTO dropped VALUES ('z')".to_string(),
                "DROP TABLE dropped".to_string(),
            ])
            .expect("seed image");
        let result = engine.execute_one("PRAGMA integrity_check").expect("check executes");
        assert_eq!(result.columns, vec!["integrity_check".to_string()]);
        assert_eq!(result.rows, vec![vec![Cell::Text("ok".to_string())]]);
    }

    /// 全表 `UPDATE` 会为每一行走一次删除加插入。删除留下的空洞由
    /// `take_freeblock` 复用，剩余 1 到 3 字节记为页内碎片。多轮之后页头
    /// 的 `nFrag`、指针数组与 cell content 边界必须仍然自洽，否则读取侧
    /// 会报 “b-tree header invalid”。
    #[test]
    fn repeated_full_table_updates_keep_the_btree_valid() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(id INTEGER PRIMARY KEY, s TEXT)").expect("create table");
        for id in 1..=400 {
            engine.execute_one(&format!("INSERT INTO t VALUES ({id}, 'aa')")).expect("insert row");
        }
        for round in 0..60u32 {
            engine.execute_one("UPDATE t SET s = CASE WHEN s = 'aa' THEN 'a' ELSE 'aa' END").expect("update row");
            let check = engine.execute_one("PRAGMA integrity_check").expect("check executes");
            assert_eq!(check.rows, vec![vec![Cell::Text("ok".to_string())]], "integrity broke on round {round}");
        }
        let rows = engine.execute_one("SELECT id, s FROM t WHERE id <= 5").expect("read rows");
        assert_eq!(
            rows.rows,
            vec![
                vec![Cell::Integer(1), Cell::Text("aa".to_string())],
                vec![Cell::Integer(2), Cell::Text("aa".to_string())],
                vec![Cell::Integer(3), Cell::Text("aa".to_string())],
                vec![Cell::Integer(4), Cell::Text("aa".to_string())],
                vec![Cell::Integer(5), Cell::Text("aa".to_string())],
            ]
        );
    }

    /// `EXPLAIN` reports the engine's own steps, `EXPLAIN QUERY PLAN` reports
    /// the accesses that will run, and neither executes the statement.
    #[test]
    fn explain_reports_a_program_and_a_scan_plan_without_executing() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_batch(&["CREATE TABLE t(a, b)".to_string(), "INSERT INTO t VALUES (1, 'x')".to_string()]).expect("seed image");
        let program = engine.execute_one("EXPLAIN SELECT a FROM t WHERE b = 'x'").expect("explain executes");
        assert_eq!(
            program.columns,
            vec![
                "addr".to_string(),
                "opcode".to_string(),
                "p1".to_string(),
                "p2".to_string(),
                "p3".to_string(),
                "p4".to_string(),
                "p5".to_string(),
                "comment".to_string(),
            ]
        );
        assert!(program.rows.iter().any(|row| matches!(&row[1], Cell::Text(opcode) if opcode == "OpenRead")));

        let plan = engine.execute_one("EXPLAIN QUERY PLAN SELECT a FROM t WHERE b = 'x'").expect("plan executes");
        assert_eq!(plan.columns, vec!["id".to_string(), "parent".to_string(), "notused".to_string(), "detail".to_string()]);
        assert_eq!(plan.rows, vec![vec![Cell::Integer(1), Cell::Integer(0), Cell::Integer(0), Cell::Text("SCAN t".to_string())]]);

        engine.execute_one("EXPLAIN INSERT INTO t VALUES (2, 'y')").expect("explain insert");
        let count = engine.execute_one("SELECT count(*) FROM t").expect("count");
        assert_eq!(count.rows[0][0], Cell::Integer(1));
    }

    /// `EXPLAIN` still rejects a source table that does not exist.
    #[test]
    fn explain_rejects_a_missing_source_table() {
        let mut engine = Engine::new(None).expect("blank engine");
        let error = engine.execute_one("EXPLAIN SELECT * FROM nope").expect_err("missing table");
        assert!(error.to_string().contains("no such table: nope"), "unexpected error: {error}");
    }

    /// Two-column table with one all-`NULL` row, inserted in rowid order.
    fn engine() -> Engine {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(a INTEGER, b INTEGER)").expect("create table");
        engine.execute_one("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (NULL, NULL), (5, 0)").expect("insert rows");
        engine
    }

    /// First column of every row the predicate keeps, in rowid order.
    fn first_column(sql: &str) -> Vec<Cell> {
        let mut engine = engine();
        let result = engine.execute_one(sql).expect("select executes");
        result.rows.into_iter().map(|mut row| row.remove(0)).collect()
    }

    #[test]
    fn between_includes_both_bounds() {
        assert_eq!(first_column("SELECT a FROM t WHERE a BETWEEN 1 AND 3"), vec![Cell::Integer(1), Cell::Integer(2), Cell::Integer(3)]);
    }

    #[test]
    fn between_accepts_arithmetic_bounds() {
        assert_eq!(first_column("SELECT a FROM t WHERE a BETWEEN b - 2 AND b + 2"), vec![Cell::Integer(1), Cell::Integer(2), Cell::Integer(3)]);
    }

    #[test]
    fn not_between_excludes_the_range_and_keeps_nulls_out() {
        assert_eq!(first_column("SELECT a FROM t WHERE a NOT BETWEEN 1 AND 3"), vec![Cell::Integer(5)]);
    }

    #[test]
    fn between_with_a_null_bound_follows_and_semantics() {
        // A `NULL` bound leaves the test unknown unless the other bound is
        // already false, exactly like `>= ... AND <= ...`.
        assert_eq!(first_column("SELECT a FROM t WHERE a BETWEEN b AND 3"), vec![Cell::Integer(1), Cell::Integer(2), Cell::Integer(3)]);
        assert_eq!(first_column("SELECT a FROM t WHERE a NOT BETWEEN b AND 3"), vec![Cell::Integer(5)]);
        assert_eq!(first_column("SELECT a FROM t WHERE a NOT BETWEEN NULL AND 3"), vec![Cell::Integer(5)]);
        assert_eq!(first_column("SELECT a FROM t WHERE a BETWEEN NULL AND 3"), Vec::<Cell>::new());
    }

    #[test]
    fn not_of_a_null_comparison_stays_null() {
        // The `NULL` row makes `a = 1` unknown, and `NOT unknown` is still
        // unknown, so it is never returned.
        assert_eq!(first_column("SELECT a FROM t WHERE NOT (a = 1)"), vec![Cell::Integer(2), Cell::Integer(3), Cell::Integer(5)]);
    }

    #[test]
    fn like_matches_sqlite() {
        let mut engine = Engine::new(None).expect("blank engine");
        let like = |engine: &mut Engine, sql: &str| engine.execute_one(sql).expect("like query").rows[0][0].clone();
        assert_eq!(like(&mut engine, "SELECT 'abc' LIKE 'A%'"), Cell::Integer(1));
        // Only ASCII letters fold case.
        assert_eq!(like(&mut engine, "SELECT 'Ü' LIKE 'ü'"), Cell::Integer(0));
        assert_eq!(like(&mut engine, "SELECT 'ü' LIKE 'Ü'"), Cell::Integer(0));
        assert_eq!(like(&mut engine, "SELECT 'ab' LIKE 'a_'"), Cell::Integer(1));
        assert_eq!(like(&mut engine, "SELECT 'abc' LIKE 'a_'"), Cell::Integer(0));
        assert_eq!(like(&mut engine, "SELECT '' LIKE '%'"), Cell::Integer(1));
        assert_eq!(like(&mut engine, "SELECT '' LIKE ''"), Cell::Integer(1));
        assert_eq!(like(&mut engine, "SELECT NULL LIKE 'a%'"), Cell::Null);
        assert_eq!(like(&mut engine, "SELECT 'x' LIKE NULL"), Cell::Null);
        assert_eq!(like(&mut engine, "SELECT 'x' NOT LIKE 'a%'"), Cell::Integer(1));
        assert_eq!(like(&mut engine, "SELECT 'abc' NOT LIKE 'a%'"), Cell::Integer(0));
        assert_eq!(like(&mut engine, "SELECT 1 LIKE '1'"), Cell::Integer(1));
        // `ESCAPE` quotes the next wildcard character.
        assert_eq!(like(&mut engine, "SELECT 'a%b' LIKE 'a\\%b' ESCAPE '\\'"), Cell::Integer(1));
        assert_eq!(like(&mut engine, "SELECT 'axb' LIKE 'a\\%b' ESCAPE '\\'"), Cell::Integer(0));
        assert_eq!(like(&mut engine, "SELECT 'a_b' LIKE 'a\\_b' ESCAPE '\\'"), Cell::Integer(1));
    }

    #[test]
    fn cast_follows_sqlite_conversions() {
        let mut engine = Engine::new(None).expect("blank engine");
        // Text to integer keeps the leading integer prefix; `1e3` has none
        // beyond `1`, and a value without digits converts to zero.
        assert_eq!(engine.execute_one("SELECT CAST('1.5' AS INTEGER)").expect("text integer").rows, vec![vec![Cell::Integer(1)]]);
        assert_eq!(engine.execute_one("SELECT CAST('abc' AS INTEGER)").expect("text without digits").rows, vec![vec![Cell::Integer(0)]]);
        assert_eq!(engine.execute_one("SELECT CAST('1e3' AS INTEGER)").expect("exponent text").rows, vec![vec![Cell::Integer(1)]]);
        assert_eq!(engine.execute_one("SELECT CAST(' 12abc' AS INTEGER)").expect("padded text").rows, vec![vec![Cell::Integer(12)]]);
        // Real to integer truncates toward zero rather than rounding.
        assert_eq!(engine.execute_one("SELECT CAST(1.9 AS INTEGER)").expect("real integer").rows, vec![vec![Cell::Integer(1)]]);
        assert_eq!(engine.execute_one("SELECT CAST(-1.9 AS INTEGER)").expect("negative real integer").rows, vec![vec![Cell::Integer(-1)]]);
        assert_eq!(engine.execute_one("SELECT CAST('3.7' AS REAL)").expect("text real").rows, vec![vec![Cell::Real(3.7)]]);
        // `NULL` stays `NULL` through any cast.
        assert_eq!(engine.execute_one("SELECT CAST(NULL AS INTEGER)").expect("null integer").rows, vec![vec![Cell::Null]]);
        assert_eq!(engine.execute_one("SELECT CAST(1 AS TEXT)").expect("integer text").rows, vec![vec![Cell::Text("1".to_string())]]);
        // A blob cast takes the UTF-8 bytes of the text form.
        assert_eq!(engine.execute_one("SELECT CAST(123 AS BLOB)").expect("integer blob").rows, vec![vec![Cell::Blob(b"123".to_vec())]]);
        // Numeric keeps an integral value as an integer.
        assert_eq!(engine.execute_one("SELECT CAST('1e3' AS NUMERIC)").expect("numeric text").rows, vec![vec![Cell::Integer(1000)]]);
        let unsupported = engine.execute_one("SELECT CAST(1 AS FANCYTYPE)").expect_err("unknown target type");
        assert_eq!(unsupported.code, "unsupported-type");
    }

    #[test]
    fn parenthesized_disjunction_keeps_both_branches() {
        assert_eq!(first_column("SELECT a FROM t WHERE (a = 1) OR (a > 3)"), vec![Cell::Integer(1), Cell::Integer(5)]);
        assert_eq!(first_column("SELECT a FROM t WHERE (a = 1 OR a = 2) AND b IS NOT NULL"), vec![Cell::Integer(1), Cell::Integer(2)]);
    }

    #[test]
    fn negative_limit_means_no_upper_bound() {
        // SQLite: a negative `LIMIT` returns every row, while `-0` returns none.
        assert_eq!(
            first_column("SELECT a FROM t LIMIT -2"),
            vec![Cell::Integer(1), Cell::Integer(2), Cell::Integer(3), Cell::Null, Cell::Integer(5)]
        );
        assert_eq!(first_column("SELECT a FROM t LIMIT -0"), Vec::<Cell>::new());
    }

    #[test]
    fn limit_placeholders_take_bound_values() {
        let mut engine = engine();
        let bound = engine
            .execute_with_parameters("SELECT a FROM t LIMIT ? OFFSET ?", &[Cell::Integer(2), Cell::Integer(1)])
            .expect("positional limit");
        assert_eq!(bound[0].rows, vec![vec![Cell::Integer(2)], vec![Cell::Integer(3)]]);
        let named = engine.execute_with_parameters("SELECT a FROM t LIMIT :n", &[Cell::Integer(2)]).expect("named limit");
        assert_eq!(named[0].rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]]);
        // Negative values clamp like SQLite: no upper bound, no skipped rows.
        let unlimited = engine.execute_with_parameters("SELECT a FROM t LIMIT ?", &[Cell::Integer(-1)]).expect("negative limit");
        assert_eq!(unlimited[0].rows.len(), 5);
        let offset = engine.execute_with_parameters("SELECT a FROM t LIMIT 2 OFFSET ?", &[Cell::Integer(-1)]).expect("negative offset");
        assert_eq!(offset[0].rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]]);
        let oversized = engine.execute_with_parameters("SELECT a FROM t LIMIT ?", &[Cell::Integer(999)]).expect("oversized limit");
        assert_eq!(oversized[0].rows.len(), 5);
        // An unbound placeholder is NULL, which SQLite rejects as a limit.
        assert_eq!(engine.execute_one("SELECT a FROM t LIMIT ?").expect_err("unbound").code, "datatype-mismatch");
    }

    #[test]
    fn first_value_frame_clamps_to_the_partition() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE f(a INTEGER); INSERT INTO f VALUES (1), (2), (3);").expect("seed");
        let results = engine
            .execute_one("SELECT a, first_value(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM f ORDER BY a")
            .expect("frame");
        assert_eq!(
            results.rows,
            vec![vec![Cell::Integer(1), Cell::Integer(1)], vec![Cell::Integer(2), Cell::Integer(1)], vec![Cell::Integer(3), Cell::Integer(2)],]
        );
    }

    #[test]
    fn implicit_rowid_aliases_resolve() {
        let mut engine = engine();
        let result = engine.execute_one("SELECT rowid, _rowid_, oid FROM t").expect("rowid aliases resolve");
        assert_eq!(result.columns, vec!["rowid".to_string(), "_rowid_".to_string(), "oid".to_string()]);
        assert_eq!(result.rows[0], vec![Cell::Integer(1), Cell::Integer(1), Cell::Integer(1)]);
        assert_eq!(result.rows[4], vec![Cell::Integer(5), Cell::Integer(5), Cell::Integer(5)]);
        // `SELECT *` never exposes the hidden rowid slot.
        let star = engine.execute_one("SELECT * FROM t").expect("star runs");
        assert_eq!(star.columns.len(), 2);
    }

    #[test]
    fn compound_select_folds_left_to_right() {
        // SQLite gives every set operator the same precedence: `(A UNION B)
        // INTERSECT C` returns `3`, while `A UNION (B INTERSECT C)` would
        // return `1, 2, 3`.
        assert_eq!(
            first_column("SELECT a FROM t WHERE a <= 2 UNION SELECT a FROM t WHERE a >= 2 INTERSECT SELECT a FROM t WHERE a = 3"),
            vec![Cell::Integer(3)]
        );
    }

    #[test]
    fn select_without_from_projects_literals() {
        let mut engine = Engine::new(None).expect("blank engine");
        assert_eq!(engine.execute_one("SELECT 1").expect("constant select").rows, vec![vec![Cell::Integer(1)]]);
        assert_eq!(engine.execute_one("SELECT 1, 2 + 3").expect("two constants").rows, vec![vec![Cell::Integer(1), Cell::Integer(5)]]);
        let aliased = engine.execute_one("SELECT 1 AS a").expect("aliased constant");
        assert_eq!(aliased.columns, vec!["a".to_string()]);
        // `LIMIT` applies to a constant projection too; a negative limit means
        // every row while `-0` still truncates to none.
        assert_eq!(engine.execute_one("SELECT 1 LIMIT 0").expect("zero limit").rows.len(), 0);
        assert_eq!(engine.execute_one("SELECT 1 LIMIT -1").expect("negative limit").rows.len(), 1);
    }

    #[test]
    fn select_without_from_rejects_column_references() {
        let mut engine = Engine::new(None).expect("blank engine");
        // SQLite reports `no such column`; a column name must never resolve to
        // `NULL` just because the projection has no `FROM` clause.
        let error = engine.execute_one("SELECT a").expect_err("no FROM has no columns");
        assert_eq!(error.code, "unknown-column");
        assert!(error.message.contains("unknown column"), "message: {}", error.message);
        assert_eq!(engine.execute_one("SELECT a AS x").expect_err("aliased column").code, "unknown-column");
        // `*` without `FROM` is a different rejection: there are no tables.
        let star = engine.execute_one("SELECT *").expect_err("star needs a table");
        assert_eq!(star.code, "no-tables");
    }

    #[test]
    fn compound_without_from_matches_sqlite() {
        let mut engine = Engine::new(None).expect("blank engine");
        assert_eq!(engine.execute_one("SELECT 1 UNION SELECT 2").expect("union").rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]]);
        assert_eq!(engine.execute_one("SELECT 1 UNION ALL SELECT 1").expect("union all").rows.len(), 2);
        assert_eq!(engine.execute_one("SELECT 1 INTERSECT SELECT 1").expect("intersect").rows, vec![vec![Cell::Integer(1)]]);
        assert_eq!(engine.execute_one("SELECT 1 EXCEPT SELECT 2").expect("except").rows, vec![vec![Cell::Integer(1)]]);
        // Every set operator shares one precedence and folds left to right:
        // `(1 UNION 2) INTERSECT 2` keeps `2` only.
        assert_eq!(
            engine.execute_one("SELECT 1 UNION SELECT 2 INTERSECT SELECT 2").expect("left-to-right chain").rows,
            vec![vec![Cell::Integer(2)]]
        );
        // SQLite only accepts `ALL` after `UNION`.
        assert!(engine.execute_one("SELECT 1 INTERSECT ALL SELECT 1").is_err());
        assert!(engine.execute_one("SELECT 1 EXCEPT ALL SELECT 1").is_err());
    }

    #[test]
    fn derived_table_without_alias_projects_its_row() {
        let mut engine = Engine::new(None).expect("blank engine");
        let result = engine.execute_one("SELECT * FROM (SELECT 1)").expect("derived table");
        assert_eq!(result.rows, vec![vec![Cell::Integer(1)]]);
        assert_eq!(engine.execute_one("SELECT 1 FROM (SELECT 1)").expect("derived with constant").rows.len(), 1);
    }

    #[test]
    fn text_primary_key_keeps_the_table_as_a_rowid_table() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE tp(k TEXT PRIMARY KEY, v INTEGER)").expect("text primary key");
        engine.execute_one("INSERT INTO tp VALUES ('a', 1), ('b', 2)").expect("insert text keys");
        assert_eq!(
            engine.execute_one("SELECT k, v FROM tp").expect("select text keys").rows,
            vec![vec![Cell::Text("a".to_string()), Cell::Integer(1)], vec![Cell::Text("b".to_string()), Cell::Integer(2)]]
        );
        // The key is an automatic unique index, not the rowid.
        assert_eq!(engine.execute_one("SELECT rowid FROM tp").expect("rowid").rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]]);
        let error = engine.execute_one("INSERT INTO tp VALUES ('a', 3)").expect_err("duplicate text key");
        assert_eq!(error.code, "unique-constraint");
    }

    #[test]
    fn table_level_primary_key_supports_composite_keys() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE pkab(a INTEGER, b INTEGER, PRIMARY KEY (a, b))").expect("composite key");
        engine.execute_one("INSERT INTO pkab VALUES (1, 1), (2, 2)").expect("insert composite keys");
        assert_eq!(
            engine.execute_one("SELECT a, b FROM pkab").expect("select composite keys").rows,
            vec![vec![Cell::Integer(1), Cell::Integer(1)], vec![Cell::Integer(2), Cell::Integer(2)]]
        );
        let error = engine.execute_one("INSERT INTO pkab VALUES (1, 1)").expect_err("duplicate composite key");
        assert_eq!(error.code, "unique-constraint");
        // A lone declared INTEGER key is still the rowid alias.
        engine.execute_one("CREATE TABLE ta(a INTEGER, PRIMARY KEY (a))").expect("alias key");
        engine.execute_one("INSERT INTO ta VALUES (5)").expect("insert alias");
        assert_eq!(engine.execute_one("SELECT rowid, a FROM ta").expect("alias rowid").rows, vec![vec![Cell::Integer(5), Cell::Integer(5)]]);
        let empty = engine.execute_one("CREATE TABLE pkx(a INTEGER, b INTEGER, PRIMARY KEY (a))").expect("qualified key");
        assert_eq!(empty.affected, 1);
    }

    #[test]
    fn is_truth_uses_sqlite_truthiness_and_never_returns_null() {
        let mut engine = Engine::new(None).expect("blank engine");
        let truth = |engine: &mut Engine, sql: &str| engine.execute_one(sql).expect("truth query").rows[0][0].clone();
        // `IS TRUE` is truthiness, not equality with 1: any non-zero number matches.
        assert_eq!(truth(&mut engine, "SELECT 1 IS TRUE"), Cell::Integer(1));
        assert_eq!(truth(&mut engine, "SELECT 2 IS TRUE"), Cell::Integer(1));
        assert_eq!(truth(&mut engine, "SELECT -1 IS TRUE"), Cell::Integer(1));
        assert_eq!(truth(&mut engine, "SELECT 0 IS TRUE"), Cell::Integer(0));
        assert_eq!(truth(&mut engine, "SELECT 0.5 IS TRUE"), Cell::Integer(1));
        assert_eq!(truth(&mut engine, "SELECT 0.0 IS TRUE"), Cell::Integer(0));
        // Text converts numerically; non-numeric text and blobs are false.
        assert_eq!(truth(&mut engine, "SELECT '1' IS TRUE"), Cell::Integer(1));
        assert_eq!(truth(&mut engine, "SELECT '0' IS TRUE"), Cell::Integer(0));
        assert_eq!(truth(&mut engine, "SELECT '' IS TRUE"), Cell::Integer(0));
        assert_eq!(truth(&mut engine, "SELECT 'a' IS TRUE"), Cell::Integer(0));
        assert_eq!(truth(&mut engine, "SELECT x'01' IS TRUE"), Cell::Integer(0));
        // `IS FALSE` is the negation of the truthiness test.
        assert_eq!(truth(&mut engine, "SELECT 1 IS FALSE"), Cell::Integer(0));
        assert_eq!(truth(&mut engine, "SELECT 2 IS FALSE"), Cell::Integer(0));
        assert_eq!(truth(&mut engine, "SELECT 0 IS FALSE"), Cell::Integer(1));
        // Neither `IS TRUE` nor `IS FALSE` is ever NULL; `IS NOT ...` just flips it.
        assert_eq!(truth(&mut engine, "SELECT NULL IS TRUE"), Cell::Integer(0));
        assert_eq!(truth(&mut engine, "SELECT NULL IS NOT TRUE"), Cell::Integer(1));
        assert_eq!(truth(&mut engine, "SELECT NULL IS FALSE"), Cell::Integer(0));
        assert_eq!(truth(&mut engine, "SELECT NULL IS NOT FALSE"), Cell::Integer(1));
        // The reported column label spells the operator back out.
        let labelled = engine.execute_one("SELECT 1 IS TRUE, NULL IS NOT FALSE").expect("labels");
        assert_eq!(labelled.columns, vec!["1 IS TRUE".to_string(), "NULL IS NOT FALSE".to_string()]);
        assert_eq!(labelled.rows, vec![vec![Cell::Integer(1), Cell::Integer(1)]]);
    }

    #[test]
    fn insert_default_values_uses_the_declared_defaults() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE dv(a INTEGER DEFAULT 7, b TEXT DEFAULT 'x', c INTEGER)").expect("defaults");
        let inserted = engine.execute_one("INSERT INTO dv DEFAULT VALUES").expect("default values");
        assert_eq!(inserted.affected, 1);
        // `DEFAULT VALUES` reports neither columns nor rows, like SQLite.
        assert!(inserted.columns.is_empty());
        assert!(inserted.rows.is_empty());
        assert_eq!(
            engine.execute_one("SELECT a, b, c FROM dv").expect("row").rows,
            vec![vec![Cell::Integer(7), Cell::Text("x".to_string()), Cell::Null]]
        );
        // A column without a declared default still becomes NULL.
        engine.execute_one("CREATE TABLE plain(a, b)").expect("plain");
        engine.execute_one("INSERT INTO plain DEFAULT VALUES").expect("plain default values");
        assert_eq!(engine.execute_one("SELECT a, b FROM plain").expect("plain row").rows, vec![vec![Cell::Null, Cell::Null]]);
        // An INTEGER primary key is the rowid alias, so the default row gets rowid 1.
        engine.execute_one("CREATE TABLE dvpk(a INTEGER PRIMARY KEY, b DEFAULT 3)").expect("alias defaults");
        engine.execute_one("INSERT INTO dvpk DEFAULT VALUES").expect("alias default values");
        assert_eq!(
            engine.execute_one("SELECT rowid, a, b FROM dvpk").expect("alias row").rows,
            vec![vec![Cell::Integer(1), Cell::Integer(1), Cell::Integer(3)]]
        );
        // `NOT NULL` without a default cannot be satisfied by a default row.
        engine.execute_one("CREATE TABLE nn(a TEXT NOT NULL, b)").expect("not null");
        let failed = engine.execute_one("INSERT INTO nn DEFAULT VALUES").expect_err("not null violation");
        assert_eq!(failed.code, "not-null-constraint");
        // An explicit column list may not be combined with `DEFAULT VALUES`.
        assert_eq!(engine.execute_one("INSERT INTO dv(a) DEFAULT VALUES").expect_err("column list").code, "syntax");
    }

    #[test]
    fn insert_values_accepts_bound_parameters() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)").expect("users");

        let inserted = engine
            .execute_with_parameters("INSERT INTO users (id, name) VALUES (?1, ?2)", &[Cell::Integer(1), Cell::Text("ada".to_string())])
            .expect("parameter insert");
        assert_eq!(inserted.len(), 1);
        assert_eq!(inserted[0].affected, 1);
        assert_eq!(inserted[0].rows, vec![vec![Cell::Integer(1)]]);
        assert_eq!(
            engine.execute_one("SELECT id, name FROM users").expect("read").rows,
            vec![vec![Cell::Integer(1), Cell::Text("ada".to_string())]]
        );

        // Every tuple of one multi-row statement binds its own placeholders.
        engine
            .execute_with_parameters(
                "INSERT INTO users (id, name) VALUES (?1, ?2), (?3, ?4)",
                &[Cell::Integer(2), Cell::Text("bob".to_string()), Cell::Integer(3), Cell::Text("cy".to_string())],
            )
            .expect("multi-row insert");
        assert_eq!(engine.execute_one("SELECT count(*) FROM users").expect("count").rows, vec![vec![Cell::Integer(3)]]);

        // Named placeholders address the same binding order.
        engine
            .execute_with_parameters("INSERT INTO users (id, name) VALUES (:id, :name)", &[Cell::Integer(4), Cell::Text("dee".to_string())])
            .expect("named insert");
        assert_eq!(engine.execute_one("SELECT id FROM users WHERE id = 4").expect("read").rows, vec![vec![Cell::Integer(4)]]);

        // Literal tuples keep working, and `DEFAULT VALUES` still ignores bindings.
        engine.execute_one("INSERT INTO users (id, name) VALUES (5, 'eve')").expect("literal insert");
        engine.execute_one("CREATE TABLE defaults_only(a INTEGER DEFAULT 9)").expect("defaults table");
        engine.execute_with_parameters("INSERT INTO defaults_only DEFAULT VALUES", &[Cell::Integer(6)]).expect("default values");
        assert_eq!(engine.execute_one("SELECT a FROM defaults_only").expect("default row").rows, vec![vec![Cell::Integer(9)]]);
    }

    #[test]
    fn last_insert_rowid_follows_sqlite_row_by_row() {
        let mut engine = Engine::new(None).expect("blank engine");
        assert_eq!(engine.last_insert_rowid(), 0);
        engine.execute_one("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT NOT NULL CHECK(length(a) > 0))").expect("create t");
        engine.execute_one("CREATE TABLE plain(a TEXT)").expect("create plain");

        engine.execute_one("INSERT INTO t(a) VALUES ('x')").expect("single insert");
        assert_eq!(engine.last_insert_rowid(), 1);
        engine.execute_one("INSERT INTO t(a) VALUES ('y'), ('z')").expect("multi insert");
        assert_eq!(engine.last_insert_rowid(), 3);
        engine.execute_one("INSERT INTO t(id, a) VALUES (99, 'q')").expect("explicit rowid");
        assert_eq!(engine.last_insert_rowid(), 99);

        // Not one row of this statement passes, so the previous value stands.
        assert!(engine.execute_one("INSERT INTO t(a) VALUES ('')").is_err());
        assert_eq!(engine.last_insert_rowid(), 99);

        // The first tuple passes and records rowid 100, the second fails, and
        // the rollback removes the row without undoing that record.
        assert!(engine.execute_one("INSERT INTO t(a) VALUES ('p'), ('')").is_err());
        assert_eq!(engine.last_insert_rowid(), 100);
        assert_eq!(engine.execute_one("SELECT count(*) FROM t").expect("count").rows, vec![vec![Cell::Integer(4)]]);
        assert_eq!(
            engine.execute_one("SELECT id FROM t ORDER BY id").expect("rowids").rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)], vec![Cell::Integer(3)], vec![Cell::Integer(99)]]
        );

        // The rolled-back rowid 100 is reused before the statement rolls back
        // again, so the recorded value stays at 100.
        assert!(engine.execute_one("INSERT INTO t(a) VALUES ('p'), (NULL)").is_err());
        assert_eq!(engine.last_insert_rowid(), 100);

        engine.execute_one("INSERT INTO plain(a) VALUES ('k'), ('l')").expect("plain multi insert");
        assert_eq!(engine.last_insert_rowid(), 2);

        // Updates and deletes never move the recorded rowid.
        engine.execute_one("UPDATE t SET a = 'u' WHERE id = 2").expect("update");
        engine.execute_one("DELETE FROM t WHERE id = 2").expect("delete");
        assert_eq!(engine.last_insert_rowid(), 2);

        // A reopened image is a fresh connection, so the value starts over.
        let path = std::env::temp_dir().join(format!("sqlite-engine-rowid-{}.db", std::process::id()));
        std::fs::write(&path, engine.snapshot().expect("image")).expect("write image");
        drop(engine);
        let writer = SqliteWriter::open(&path).expect("open image");
        let reopened = Engine::from_writer(writer).expect("rebuild catalog");
        assert_eq!(reopened.last_insert_rowid(), 0);
        std::fs::remove_file(&path).expect("cleanup image");
    }

    #[test]
    fn concat_binds_tighter_than_arithmetic() {
        let mut engine = Engine::new(None).expect("blank engine");
        let value = |engine: &mut Engine, sql: &str| engine.execute_one(sql).expect("concat query").rows[0][0].clone();
        assert_eq!(value(&mut engine, "SELECT 1 || 2 * 3"), Cell::Integer(36));
        assert_eq!(value(&mut engine, "SELECT 1 || 2 + 3"), Cell::Integer(15));
        assert_eq!(value(&mut engine, "SELECT 2 + 3 || 4"), Cell::Integer(36));
        assert_eq!(value(&mut engine, "SELECT 'a' || 'b' = 'ab'"), Cell::Integer(1));
        assert_eq!(value(&mut engine, "SELECT 1 << 2 || 3"), Cell::Integer(8388608));
        // `||` is tighter than `/` as well, so the text side is the divisor.
        assert_eq!(value(&mut engine, "SELECT 7 / 2 || 'x'"), Cell::Integer(3));
        // Left-associative, and looser than unary minus.
        assert_eq!(value(&mut engine, "SELECT 1 || 2 || 3"), Cell::Text("123".to_string()));
        assert_eq!(value(&mut engine, "SELECT -1 || 2"), Cell::Text("-12".to_string()));
    }

    #[test]
    fn concat_produces_text_like_sqlite() {
        let mut engine = Engine::new(None).expect("blank engine");
        let value = |engine: &mut Engine, sql: &str| engine.execute_one(sql).expect("concat query").rows[0][0].clone();
        assert_eq!(value(&mut engine, "SELECT 1 || 2"), Cell::Text("12".to_string()));
        assert_eq!(value(&mut engine, "SELECT 1.5 || 'x'"), Cell::Text("1.5x".to_string()));
        assert_eq!(value(&mut engine, "SELECT 2.0 || ''"), Cell::Text("2.0".to_string()));
        assert_eq!(value(&mut engine, "SELECT 1e300 || ''"), Cell::Text("1.0e+300".to_string()));
        assert_eq!(value(&mut engine, "SELECT 1e-5 || ''"), Cell::Text("1.0e-05".to_string()));
        assert_eq!(value(&mut engine, "SELECT typeof(1 || 2)"), Cell::Text("text".to_string()));
        // `NULL` propagates through concatenation.
        assert_eq!(value(&mut engine, "SELECT NULL || 'x'"), Cell::Null);
        assert_eq!(value(&mut engine, "SELECT 'x' || NULL"), Cell::Null);
        // Blobs concatenate through their text form.
        assert_eq!(value(&mut engine, "SELECT x'6162' || 'c'"), Cell::Text("abc".to_string()));
        assert_eq!(value(&mut engine, "SELECT x'31' || 1"), Cell::Text("11".to_string()));
    }

    #[test]
    fn bitwise_operators_follow_sqlite() {
        let mut engine = Engine::new(None).expect("blank engine");
        let value = |engine: &mut Engine, sql: &str| engine.execute_one(sql).expect("bitwise query").rows[0][0].clone();
        assert_eq!(value(&mut engine, "SELECT 5 & 3"), Cell::Integer(1));
        // SQLite supports `|` as bitwise or even though it has no `^`.
        assert_eq!(value(&mut engine, "SELECT 1 | 2"), Cell::Integer(3));
        assert_eq!(value(&mut engine, "SELECT typeof(1 | 2)"), Cell::Text("integer".to_string()));
        assert_eq!(value(&mut engine, "SELECT ~1"), Cell::Integer(-2));
        assert_eq!(value(&mut engine, "SELECT ~1 || 2"), Cell::Text("-22".to_string()));
        // One left-associative level, looser than `+` and tighter than `=`.
        assert_eq!(value(&mut engine, "SELECT 1 | 2 & 3"), Cell::Integer(3));
        assert_eq!(value(&mut engine, "SELECT 5 & 3 << 1"), Cell::Integer(2));
        assert_eq!(value(&mut engine, "SELECT 1 + 2 | 4"), Cell::Integer(7));
        assert_eq!(value(&mut engine, "SELECT 1 & 2 = 0"), Cell::Integer(1));
        // Operands convert to 64-bit integers through the numeric prefix.
        assert_eq!(value(&mut engine, "SELECT '2abc' & 3"), Cell::Integer(2));
        assert_eq!(value(&mut engine, "SELECT 1.9 & 3"), Cell::Integer(1));
        assert_eq!(value(&mut engine, "SELECT x'33' & 3"), Cell::Integer(3));
        assert_eq!(value(&mut engine, "SELECT ~'2abc'"), Cell::Integer(-3));
        assert_eq!(value(&mut engine, "SELECT ~x'31'"), Cell::Integer(-2));
        assert_eq!(value(&mut engine, "SELECT ~NULL"), Cell::Null);
        assert_eq!(value(&mut engine, "SELECT NULL & 3"), Cell::Null);
        // `^` is not a SQLite token and stays a syntax error.
        assert_eq!(engine.execute_one("SELECT 5 ^ 3").expect_err("no xor").code, "syntax");
    }

    #[test]
    fn shift_operators_follow_sqlite() {
        let mut engine = Engine::new(None).expect("blank engine");
        let value = |engine: &mut Engine, sql: &str| engine.execute_one(sql).expect("shift query").rows[0][0].clone();
        assert_eq!(value(&mut engine, "SELECT 1 << 2 + 3"), Cell::Integer(32));
        assert_eq!(value(&mut engine, "SELECT -8 >> 1"), Cell::Integer(-4));
        assert_eq!(value(&mut engine, "SELECT 1 << 63"), Cell::Integer(i64::MIN));
        assert_eq!(value(&mut engine, "SELECT 1 << 64"), Cell::Integer(0));
        assert_eq!(value(&mut engine, "SELECT -8 >> 65"), Cell::Integer(-1));
        // A negative count reverses the direction.
        assert_eq!(value(&mut engine, "SELECT 16 >> -1"), Cell::Integer(32));
        assert_eq!(value(&mut engine, "SELECT 1.9 << 1"), Cell::Integer(2));
        assert_eq!(value(&mut engine, "SELECT x'32' << 1"), Cell::Integer(4));
        assert_eq!(value(&mut engine, "SELECT NULL << 1"), Cell::Null);
    }

    #[test]
    fn numeric_prefix_reaches_casts_truthiness_and_aggregates() {
        let mut engine = engine();
        // Arithmetic takes the longest numeric prefix.
        assert_eq!(engine.execute_one("SELECT '2abc' + 1").expect("text add").rows, vec![vec![Cell::Integer(3)]]);
        assert_eq!(engine.execute_one("SELECT 'abc' + 1").expect("textless add").rows, vec![vec![Cell::Integer(1)]]);
        assert_eq!(engine.execute_one("SELECT ' 12abc' * 2").expect("padded multiply").rows, vec![vec![Cell::Integer(24)]]);
        // Blobs convert through their text form everywhere.
        assert_eq!(engine.execute_one("SELECT x'31' + 1").expect("blob add").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(engine.execute_one("SELECT CAST(x'31' AS INTEGER)").expect("blob integer").rows, vec![vec![Cell::Integer(1)]]);
        assert_eq!(engine.execute_one("SELECT CAST('2abc' AS NUMERIC)").expect("text numeric").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(engine.execute_one("SELECT CAST(x'312e35' AS NUMERIC)").expect("blob numeric").rows, vec![vec![Cell::Real(1.5)]]);
        // A real becomes text through the same `%!.15g` rendering as `||`.
        assert_eq!(
            engine.execute_one("SELECT CAST(0.30000000000000004 AS TEXT)").expect("real text").rows,
            vec![vec![Cell::Text("0.3".to_string())]]
        );
        assert_eq!(
            engine.execute_one("SELECT CAST(1e300 AS TEXT)").expect("huge real text").rows,
            vec![vec![Cell::Text("1.0e+300".to_string())]]
        );
        // `IS TRUE` runs the same numeric prefix.
        assert_eq!(engine.execute_one("SELECT '2abc' IS TRUE").expect("text truth").rows, vec![vec![Cell::Integer(1)]]);
        assert_eq!(engine.execute_one("SELECT '0abc' IS TRUE").expect("zero truth").rows, vec![vec![Cell::Integer(0)]]);
        // Comparisons between a literal and a number keep the storage-class
        // ordering instead of coercing the text.
        assert_eq!(engine.execute_one("SELECT '2abc' = 2").expect("mixed compare").rows, vec![vec![Cell::Integer(0)]]);
        // Aggregates convert with the prefix rule; text and blob inputs switch
        // the sum to a real even when they hold whole numbers.
        engine.execute_one("CREATE TABLE v(x)").expect("create v");
        engine.execute_one("INSERT INTO v VALUES ('2abc'), ('10'), (NULL), (x'31')").expect("insert v");
        assert_eq!(
            engine.execute_one("SELECT sum(x), avg(x), count(x) FROM v").expect("v aggregate").rows,
            vec![vec![Cell::Real(13.0), Cell::Real(13.0 / 3.0), Cell::Integer(3)]]
        );
        // An all-integer column keeps the sum integral.
        assert_eq!(engine.execute_one("SELECT sum(a) FROM t").expect("t sum").rows, vec![vec![Cell::Integer(11)]]);
        assert_eq!(engine.execute_one("SELECT sum(a) FROM t WHERE a > 100").expect("empty sum").rows, vec![vec![Cell::Null]]);
        // `total()` is the real-valued sibling of `sum()` and never returns NULL.
        assert_eq!(
            engine.execute_one("SELECT total(x), typeof(total(x)) FROM v").expect("total").rows,
            vec![vec![Cell::Real(13.0), Cell::Text("real".to_string())]]
        );
        assert_eq!(
            engine.execute_one("SELECT total(x), sum(x) FROM v WHERE 0").expect("empty total").rows,
            vec![vec![Cell::Real(0.0), Cell::Null]]
        );
        assert_eq!(
            engine.execute_one("SELECT total(NULL), total(1), total(' 12abc')").expect("total literals").rows,
            vec![vec![Cell::Real(0.0), Cell::Real(1.0), Cell::Real(12.0)]]
        );
    }

    #[test]
    fn string_and_inspection_functions_follow_sqlite() {
        let mut engine = Engine::new(None).expect("blank engine");
        let value = |engine: &mut Engine, sql: &str| engine.execute_one(sql).expect("function query").rows[0][0].clone();
        engine.execute_one("CREATE TABLE t(x)").expect("create t");
        engine.execute_one("INSERT INTO t VALUES ('a'), (NULL), ('b'), (3), (x'63')").expect("insert t");
        assert_eq!(value(&mut engine, "SELECT group_concat(x) FROM t"), Cell::Text("a,b,3,c".to_string()));
        assert_eq!(value(&mut engine, "SELECT group_concat(x, '-') FROM t"), Cell::Text("a-b-3-c".to_string()));
        // A present but NULL separator acts like an empty one, not the default.
        assert_eq!(value(&mut engine, "SELECT group_concat(x, NULL) FROM t"), Cell::Text("ab3c".to_string()));
        assert_eq!(value(&mut engine, "SELECT group_concat(x, 1) FROM t"), Cell::Text("a1b131c".to_string()));
        assert_eq!(value(&mut engine, "SELECT typeof(group_concat(x)) FROM t"), Cell::Text("text".to_string()));
        assert_eq!(value(&mut engine, "SELECT group_concat(x) FROM t WHERE 0"), Cell::Null);
        // `hex()` hashes the text form, so numbers are not treated as bytes.
        assert_eq!(value(&mut engine, "SELECT hex(255)"), Cell::Text("323535".to_string()));
        assert_eq!(value(&mut engine, "SELECT hex(-1)"), Cell::Text("2D31".to_string()));
        assert_eq!(value(&mut engine, "SELECT hex('abc')"), Cell::Text("616263".to_string()));
        assert_eq!(value(&mut engine, "SELECT hex(x'0102')"), Cell::Text("0102".to_string()));
        assert_eq!(value(&mut engine, "SELECT hex(1.5)"), Cell::Text("312E35".to_string()));
        assert_eq!(value(&mut engine, "SELECT hex(NULL)"), Cell::Text(String::new()));
        // `quote()` emits a literal per storage class.
        assert_eq!(value(&mut engine, "SELECT quote('It''s')"), Cell::Text("'It''s'".to_string()));
        assert_eq!(value(&mut engine, "SELECT quote(NULL)"), Cell::Text("NULL".to_string()));
        assert_eq!(value(&mut engine, "SELECT quote(x'0102')"), Cell::Text("X'0102'".to_string()));
        assert_eq!(value(&mut engine, "SELECT quote(1.5)"), Cell::Text("1.5".to_string()));
        assert_eq!(value(&mut engine, "SELECT quote(42)"), Cell::Text("42".to_string()));
        assert_eq!(value(&mut engine, "SELECT quote('')"), Cell::Text("''".to_string()));
        // Values whose 15-digit form does not round-trip fall back to 18 digits.
        assert_eq!(value(&mut engine, "SELECT quote(1.0 / 3.0)"), Cell::Text("3.333333333333333148e-01".to_string()));
        // `random()` and `randomblob()` only guarantee their shape.
        assert_eq!(value(&mut engine, "SELECT typeof(random())"), Cell::Text("integer".to_string()));
        assert_eq!(
            engine.execute_one("SELECT length(randomblob(4)), typeof(randomblob(4))").expect("randomblob").rows,
            vec![vec![Cell::Integer(4), Cell::Text("blob".to_string())]]
        );
        assert_eq!(value(&mut engine, "SELECT length(randomblob(NULL))"), Cell::Integer(1));
    }

    #[test]
    fn round_matches_sqlite_fixed_point_rounding() {
        let mut engine = Engine::new(None).expect("blank engine");
        let value = |engine: &mut Engine, sql: &str| engine.execute_one(sql).expect("round query").rows[0][0].clone();
        // Every expected value below was measured on better-sqlite3 3.53.2 and
        // CPython sqlite3 3.49.1.
        assert_eq!(value(&mut engine, "SELECT round(1.2345, 3)"), Cell::Real(1.234));
        // A negative digit count is clamped to zero instead of scaling down.
        assert_eq!(value(&mut engine, "SELECT round(123.456, -1)"), Cell::Real(123.0));
        assert_eq!(value(&mut engine, "SELECT round(123.456, -5)"), Cell::Real(123.0));
        // Zero digits use the truncating fast path, not ties-to-even.
        assert_eq!(value(&mut engine, "SELECT round(2.5)"), Cell::Real(3.0));
        assert_eq!(value(&mut engine, "SELECT round(-2.5)"), Cell::Real(-3.0));
        assert_eq!(value(&mut engine, "SELECT round(0.5)"), Cell::Real(1.0));
        // Exact decimal ties round away from zero, unlike Rust's own formatting.
        assert_eq!(value(&mut engine, "SELECT round(0.25, 1)"), Cell::Real(0.3));
        assert_eq!(value(&mut engine, "SELECT round(1.25, 1)"), Cell::Real(1.3));
        assert_eq!(value(&mut engine, "SELECT round(-1.25, 1)"), Cell::Real(-1.3));
        // Values just below a tie follow their binary value.
        assert_eq!(value(&mut engine, "SELECT round(2.675, 2)"), Cell::Real(2.67));
        assert_eq!(value(&mut engine, "SELECT round(1.005, 2)"), Cell::Real(1.0));
        assert_eq!(value(&mut engine, "SELECT round(0.615, 2)"), Cell::Real(0.61));
        assert_eq!(value(&mut engine, "SELECT round(0.35, 1)"), Cell::Real(0.3));
        assert_eq!(value(&mut engine, "SELECT round(9.999, 2)"), Cell::Real(10.0));
        // More than 30 digits is the same as 30, and text coerces like SQLite.
        assert_eq!(value(&mut engine, "SELECT round(1.23456, 40)"), Cell::Real(1.23456));
        assert_eq!(value(&mut engine, "SELECT round('1.5', 1)"), Cell::Real(1.5));
        assert_eq!(value(&mut engine, "SELECT round('abc', 1)"), Cell::Real(0.0));
        assert_eq!(value(&mut engine, "SELECT round(1.2345, 3.9)"), Cell::Real(1.234));
        assert_eq!(value(&mut engine, "SELECT round(NULL, 1)"), Cell::Null);
        assert_eq!(value(&mut engine, "SELECT round(1.5, NULL)"), Cell::Null);
        assert_eq!(value(&mut engine, "SELECT typeof(round(2.5, 1))"), Cell::Text("real".to_string()));
    }

    #[test]
    fn instr_returns_one_based_character_positions() {
        let mut engine = Engine::new(None).expect("blank engine");
        let value = |engine: &mut Engine, sql: &str| engine.execute_one(sql).expect("instr query").rows[0][0].clone();
        // Measured on better-sqlite3 3.53.2 and CPython sqlite3 3.49.1.
        assert_eq!(value(&mut engine, "SELECT instr('abcabc', 'bc')"), Cell::Integer(2));
        assert_eq!(value(&mut engine, "SELECT instr('abc', 'abc')"), Cell::Integer(1));
        assert_eq!(value(&mut engine, "SELECT instr('abc', 'z')"), Cell::Integer(0));
        assert_eq!(value(&mut engine, "SELECT instr('abc', 'abcd')"), Cell::Integer(0));
        // An empty needle matches at the first character, an empty haystack
        // with a non-empty needle does not.
        assert_eq!(value(&mut engine, "SELECT instr('abc', '')"), Cell::Integer(1));
        assert_eq!(value(&mut engine, "SELECT instr('', 'a')"), Cell::Integer(0));
        assert_eq!(value(&mut engine, "SELECT instr('', '')"), Cell::Integer(1));
        // Positions are counted in characters, not bytes; bound text keeps
        // every code point intact.
        let unicode = engine
            .execute_with_parameters("SELECT instr(?1, ?2)", &[Cell::Text("αβγδ".to_string()), Cell::Text("γ".to_string())])
            .expect("bound instr");
        assert_eq!(unicode[0].rows, vec![vec![Cell::Integer(3)]]);
        let han = engine
            .execute_with_parameters("SELECT instr(?1, ?2)", &[Cell::Text("你好世界".to_string()), Cell::Text("好".to_string())])
            .expect("bound instr");
        assert_eq!(han[0].rows, vec![vec![Cell::Integer(2)]]);
        // Numeric arguments use their SQLite text form.
        assert_eq!(value(&mut engine, "SELECT instr(12345, 34)"), Cell::Integer(3));
        assert_eq!(value(&mut engine, "SELECT instr(1.5, '.')"), Cell::Integer(2));
        assert_eq!(value(&mut engine, "SELECT typeof(instr('a', 'a'))"), Cell::Text("integer".to_string()));
        assert_eq!(value(&mut engine, "SELECT instr(NULL, 'a')"), Cell::Null);
        assert_eq!(value(&mut engine, "SELECT instr('a', NULL)"), Cell::Null);
        assert_eq!(
            engine.execute_one("SELECT instr('a', 'b', 'c')").expect_err("three arguments").message,
            "wrong number of arguments to function instr()"
        );
    }

    fn text(value: &str) -> Cell {
        Cell::Text(value.to_string())
    }

    #[test]
    fn sqlite_master_reports_creation_order_and_automatic_indexes() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE a(x INTEGER, y TEXT)").expect("create a");
        engine.execute_one("CREATE INDEX ia ON a(x)").expect("create ia");
        engine.execute_one("CREATE TABLE b(z TEXT PRIMARY KEY, w TEXT UNIQUE)").expect("create b");

        let result = engine.execute_one("SELECT type, name, tbl_name, rootpage, sql FROM sqlite_master").expect("select master");
        assert_eq!(
            result.columns,
            vec!["type".to_string(), "name".to_string(), "tbl_name".to_string(), "rootpage".to_string(), "sql".to_string()]
        );
        let rows = result.rows;
        assert_eq!(rows.len(), 5);
        // Row order is creation order, which is rootpage order here as in
        // upstream SQLite: table, explicit index, table, then its two
        // automatic constraint indexes.
        assert_eq!(rows[0][0..4], [text("table"), text("a"), text("a"), Cell::Integer(2)]);
        assert_eq!(rows[1][0..4], [text("index"), text("ia"), text("a"), Cell::Integer(3)]);
        assert_eq!(rows[2][0..4], [text("table"), text("b"), text("b"), Cell::Integer(4)]);
        assert_eq!(rows[3][0..4], [text("index"), text("sqlite_autoindex_b_1"), text("b"), Cell::Integer(5)]);
        assert_eq!(rows[4][0..4], [text("index"), text("sqlite_autoindex_b_2"), text("b"), Cell::Integer(6)]);
        // SQLite stores the statement as written; the SQL surface passes it
        // through verbatim instead of a re-rendered form.
        assert_eq!(rows[0][4], text("CREATE TABLE a(x INTEGER, y TEXT)"));
        assert_eq!(rows[1][4], text("CREATE INDEX ia ON a(x)"));
        // Automatic indexes have no SQL text, exactly like SQLite.
        assert_eq!(rows[3][4], Cell::Null);
        assert_eq!(rows[4][4], Cell::Null);

        // Filters and the `sqlite_schema` alias resolve to the same rows.
        let tables = engine.execute_one("SELECT name FROM sqlite_master WHERE type = 'table'").expect("filter tables");
        assert_eq!(tables.rows, vec![vec![text("a")], vec![text("b")]]);
        let autoindex = engine.execute_one("SELECT sql FROM sqlite_master WHERE name = 'sqlite_autoindex_b_1'").expect("filter autoindex");
        assert_eq!(autoindex.rows, vec![vec![Cell::Null]]);
        let alias = engine.execute_one("SELECT count(*) FROM sqlite_schema").expect("schema alias");
        assert_eq!(alias.rows, vec![vec![Cell::Integer(5)]]);
        // `sqlite_master` is an ordinary rowid table: `rowid` resolves and
        // `SELECT *` still projects only the five stored columns.
        let rowids = engine.execute_one("SELECT rowid, name FROM sqlite_master ORDER BY rowid").expect("master rowids");
        assert_eq!(
            rowids.rows,
            vec![
                vec![Cell::Integer(1), text("a")],
                vec![Cell::Integer(2), text("ia")],
                vec![Cell::Integer(3), text("b")],
                vec![Cell::Integer(4), text("sqlite_autoindex_b_1")],
                vec![Cell::Integer(5), text("sqlite_autoindex_b_2")],
            ]
        );
        let star = engine.execute_one("SELECT * FROM sqlite_master").expect("star");
        assert_eq!(star.columns.len(), 5);
        assert_eq!(star.rows[0].len(), 5);
        let temporary = engine.execute_one("SELECT count(*) FROM sqlite_temp_master").expect("temp master");
        assert_eq!(temporary.rows, vec![vec![Cell::Integer(0)]]);
    }

    #[test]
    fn schema_tables_are_read_only_and_reserved_names_are_rejected() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(a INTEGER)").expect("create t");

        let insert = engine.execute_one("INSERT INTO sqlite_master VALUES ('table', 'x', 'x', 0, 'x')").expect_err("read only");
        assert_eq!(insert.code, "readonly-system-table");
        assert_eq!(insert.statement, Some("insert"));
        let sequence = engine.execute_one("INSERT INTO sqlite_sequence VALUES ('t', 1)").expect_err("read only");
        assert_eq!(sequence.code, "readonly-system-table");

        let table = engine.execute_one("CREATE TABLE sqlite_foo(a INTEGER)").expect_err("reserved table");
        assert_eq!(table.code, "reserved-name");
        assert_eq!(table.message, "object name reserved for internal use: sqlite_foo");
        let index = engine.execute_one("CREATE INDEX sqlite_idx ON t(a)").expect_err("reserved index");
        assert_eq!(index.code, "reserved-name");
        let automatic = engine.execute_one("CREATE INDEX im ON sqlite_master(name)").expect_err("unindexable");
        assert_eq!(automatic.code, "readonly-system-table");
        assert_eq!(automatic.message, "table sqlite_master may not be indexed");
    }

    #[test]
    fn autoincrement_tracks_sqlite_sequence() {
        let mut engine = Engine::new(None).expect("blank engine");
        let value = |engine: &mut Engine, sql: &str| engine.execute_one(sql).expect("query").rows[0][0].clone();
        engine.execute_one("CREATE TABLE c(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)").expect("create c");

        // `sqlite_sequence` is created by CREATE TABLE, immediately after the
        // table and before any later object.
        let master = engine.execute_one("SELECT type, name, rootpage FROM sqlite_master").expect("select master");
        assert_eq!(
            master.rows,
            vec![vec![text("table"), text("c"), Cell::Integer(2)], vec![text("table"), text("sqlite_sequence"), Cell::Integer(3)]]
        );
        let empty = engine.execute_one("SELECT * FROM sqlite_sequence").expect("empty sequence");
        assert_eq!(empty.columns, vec!["name".to_string(), "seq".to_string()]);
        assert_eq!(empty.rows, Vec::<Vec<Cell>>::new());

        engine.execute_one("INSERT INTO c(v) VALUES ('a'), ('b')").expect("auto insert");
        assert_eq!(engine.execute_one("SELECT id FROM c").expect("select ids").rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]]);
        assert_eq!(
            engine.execute_one("SELECT name, seq FROM sqlite_sequence").expect("sequence").rows,
            vec![vec![text("c"), Cell::Integer(2)]]
        );

        // An explicit larger rowid raises the high-water mark, then automatic
        // ids continue above it; a lower explicit rowid never lowers it.
        engine.execute_one("INSERT INTO c(id, v) VALUES (100, 'explicit')").expect("explicit 100");
        assert_eq!(value(&mut engine, "SELECT seq FROM sqlite_sequence"), Cell::Integer(100));
        engine.execute_one("INSERT INTO c(v) VALUES ('auto')").expect("auto after 100");
        assert_eq!(value(&mut engine, "SELECT max(id) FROM c"), Cell::Integer(101));
        engine.execute_one("INSERT INTO c(id, v) VALUES (5, 'low')").expect("explicit 5");
        assert_eq!(value(&mut engine, "SELECT seq FROM sqlite_sequence"), Cell::Integer(101));
        engine.execute_one("INSERT INTO c(id, v) VALUES (NULL, 'null')").expect("null alias");
        assert_eq!(value(&mut engine, "SELECT max(id) FROM c"), Cell::Integer(102));

        // Without an AUTOINCREMENT table the sequence table does not exist.
        let missing = Engine::new(None).expect("fresh engine").execute_one("SELECT * FROM sqlite_sequence").expect_err("absent");
        assert_eq!(missing.code, "unknown-table");
        assert_eq!(missing.message, "no such table: sqlite_sequence");
    }

    #[test]
    fn autoincrement_must_decorate_an_integer_primary_key() {
        let mut engine = Engine::new(None).expect("blank engine");
        let text_key = engine.execute_one("CREATE TABLE e(a TEXT PRIMARY KEY AUTOINCREMENT)").expect_err("text key");
        assert_eq!(text_key.code, "autoincrement-integer-primary-key");
        assert_eq!(text_key.message, "AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY");
        let no_key = engine.execute_one("CREATE TABLE f(a INTEGER AUTOINCREMENT)").expect_err("missing key");
        assert_eq!(no_key.code, "syntax");
        assert_eq!(no_key.message, "near \"AUTOINCREMENT\": syntax error");
    }

    /// Builds an engine from a fixed setup script.
    fn joined_engine(statements: &[&str]) -> Engine {
        let mut engine = Engine::new(None).expect("blank engine");
        for statement in statements {
            engine.execute_one(statement).expect("setup statement");
        }
        engine
    }

    const LEFT_RIGHT: &[&str] = &[
        "CREATE TABLE l(id INTEGER, lv TEXT)",
        "INSERT INTO l VALUES (1,'a'),(2,'b')",
        "CREATE TABLE r(id INTEGER, rv TEXT)",
        "INSERT INTO r VALUES (2,'B'),(3,'C')",
    ];

    #[test]
    fn right_join_using_merges_the_column_once() {
        let mut engine = joined_engine(LEFT_RIGHT);
        let merged = engine.execute_one("SELECT id, lv, rv FROM l RIGHT JOIN r USING(id)").expect("merged columns");
        assert_eq!(merged.columns, vec!["id", "lv", "rv"]);
        assert_eq!(merged.rows, vec![vec![Cell::Integer(2), text("b"), text("B")], vec![Cell::Integer(3), Cell::Null, text("C")]]);
        // `*` repeats the `USING` column neither for the left nor the right side.
        let star = engine.execute_one("SELECT * FROM l RIGHT JOIN r USING(id)").expect("star");
        assert_eq!(star.columns, vec!["id", "lv", "rv"]);
        assert_eq!(star.rows, merged.rows);
        assert_eq!(
            engine.execute_one("SELECT id FROM l RIGHT JOIN r USING(id)").expect("merged id").rows,
            vec![vec![Cell::Integer(2)], vec![Cell::Integer(3)]]
        );
        // A table-qualified reference still addresses the original column.
        let qualified = engine.execute_one("SELECT l.id, r.id FROM l RIGHT JOIN r USING(id)").expect("qualified");
        assert_eq!(qualified.columns, vec!["id", "id"]);
        assert_eq!(qualified.rows, vec![vec![Cell::Integer(2), Cell::Integer(2)], vec![Cell::Null, Cell::Integer(3)]]);
    }

    #[test]
    fn inner_join_on_right_rowid_alias_keeps_left_major_order() {
        let mut engine = joined_engine(&[
            "CREATE TABLE l(id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO l VALUES (1,'a'),(2,'b'),(3,'c')",
            "CREATE TABLE r(id INTEGER PRIMARY KEY, w TEXT)",
            "INSERT INTO r VALUES (1,'X'),(2,'Y')",
        ]);
        // `r.id` is the right source's rowid alias, so the hash fast path
        // probes by rowid. The emitted pairs and their left-major order match
        // what the nested scan produces; the unmatched left row (id 3) drops.
        let joined = engine.execute_one("SELECT l.v, r.w FROM l JOIN r ON r.id = l.id").expect("rowid join");
        assert_eq!(joined.rows, vec![vec![text("a"), text("X")], vec![text("b"), text("Y")]]);
        // The implicit `rowid` name hits the same fast path.
        assert_eq!(
            engine.execute_one("SELECT l.v, r.w FROM l JOIN r ON r.rowid = l.id").expect("implicit rowid join").rows,
            vec![vec![text("a"), text("X")], vec![text("b"), text("Y")]]
        );
        // A non-integer probe (real via arithmetic) falls back to the nested
        // scan but still yields the identical result.
        assert_eq!(
            engine.execute_one("SELECT l.v, r.w FROM l JOIN r ON r.id = l.id * 1.0").expect("real probe join").rows,
            vec![vec![text("a"), text("X")], vec![text("b"), text("Y")]]
        );
        // A compound `ON` is not a single equality, so the generic path runs.
        assert_eq!(
            engine.execute_one("SELECT l.v, r.w FROM l JOIN r ON r.id = l.id AND r.w <> 'Y'").expect("compound on join").rows,
            vec![vec![text("a"), text("X")]]
        );
    }

    #[test]
    fn count_star_join_counts_without_materializing_rows() {
        let mut engine = joined_engine(&[
            "CREATE TABLE l(id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO l VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')",
            "CREATE TABLE r(id INTEGER PRIMARY KEY, w TEXT)",
            "INSERT INTO r VALUES (1,'X'),(2,'Y'),(4,'Z')",
        ]);
        // 裸 `count(*)` 的内连接走计数路径,不物化匹配行。
        assert_eq!(
            engine.execute_one("SELECT count(*) AS n FROM l JOIN r ON r.id = l.id").expect("count join").rows,
            vec![vec![Cell::Integer(3)]]
        );
        // `WHERE` 合取仍在完整行上生效。
        assert_eq!(
            engine.execute_one("SELECT count(*) AS n FROM l JOIN r ON r.id = l.id WHERE r.w <> 'Y'").expect("filtered").rows,
            vec![vec![Cell::Integer(2)]]
        );
        // 逗号乘积走计数路径,行数与通用路径一致。
        assert_eq!(engine.execute_one("SELECT count(*) AS n FROM l, r").expect("product").rows, vec![vec![Cell::Integer(12)]]);
        assert!(engine.execute_one("SELECT count(*) AS n FROM l, r LIMIT 0").expect("limit zero").rows.is_empty());
        // 外连接不走计数路径,但结果仍按通用路径计算。
        assert_eq!(
            engine.execute_one("SELECT count(*) AS n FROM l LEFT JOIN r ON r.id = l.id").expect("left join").rows,
            vec![vec![Cell::Integer(4)]]
        );
    }

    #[test]
    fn count_star_product_and_subquery_count_without_materializing_rows() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL)",
            "INSERT INTO t VALUES (1,1.0),(2,2.0),(3,3.0)",
        ]);
        assert_eq!(
            engine.execute_one("SELECT count(*) AS n FROM t WHERE score > 1.5").expect("count filter").rows,
            vec![vec![Cell::Integer(2)]]
        );
        // 同表内外层共享一次扫描,行集仍按 `IN` 子查询的结果计算。
        assert_eq!(
            engine.execute_one("SELECT count(*) AS n FROM t WHERE id IN (SELECT id FROM t WHERE score > 1.5)").expect("in subquery").rows,
            vec![vec![Cell::Integer(2)]]
        );
    }

    #[test]
    fn same_table_subqueries_reuse_a_covering_decode_and_redecode_narrower_masks() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, score REAL)",
            "INSERT INTO t VALUES (1,'a',1.0),(2,'b',2.0),(3,'c',3.0)",
        ]);
        // 外层掩码为全量解码,内层只要 `score`:内层复用外层已解码的行。
        assert_eq!(
            engine
                .execute_one("SELECT id, label, score FROM t WHERE id IN (SELECT id FROM t WHERE score > 1.5) ORDER BY id")
                .expect("covering reuse")
                .rows,
            vec![
                vec![Cell::Integer(2), Cell::Text("b".to_string()), Cell::Real(2.0)],
                vec![Cell::Integer(3), Cell::Text("c".to_string()), Cell::Real(3.0)],
            ]
        );
        // 两个互不覆盖的窄掩码各自解码,先落缓存的那份不能让后者读到 NULL 占位。
        assert_eq!(
            engine
                .execute_one("SELECT (SELECT count(*) FROM t WHERE score > 1.5), (SELECT count(*) FROM t WHERE label = 'a')")
                .expect("incomparable masks")
                .rows,
            vec![vec![Cell::Integer(2), Cell::Integer(1)]]
        );
        // 窄掩码先落缓存后,同一语句里更宽的同表请求仍要重新解码。
        assert_eq!(
            engine
                .execute_one(
                    "SELECT (SELECT count(*) FROM t WHERE score > 1.5) + (SELECT count(*) FROM t WHERE id IN (SELECT id FROM t WHERE label = 'a'))"
                )
                .expect("wider after narrower")
                .rows,
            vec![vec![Cell::Integer(3)]]
        );
        // 同表相关子查询:内层按外层当前行求值,共享行集不改变结果。
        assert_eq!(
            engine
                .execute_one("SELECT id FROM t AS o WHERE EXISTS (SELECT 1 FROM t AS u WHERE u.score = o.score + 1.0)")
                .expect("correlated same table")
                .rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]]
        );
    }

    #[test]
    fn index_range_reads_sparse_candidates_like_the_full_scan() {
        let mut plain = Engine::new(None).expect("blank");
        let mut indexed = Engine::new(None).expect("blank");
        for engine in [&mut plain, &mut indexed] {
            engine.execute_one("CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT)").expect("create");
            for row in 1..=64i64 {
                let label = if row % 8 == 0 { "hit".to_string() } else { format!("row-{row}") };
                engine.execute_one(&format!("INSERT INTO t VALUES ({row}, '{label}')")).expect("insert");
            }
        }
        indexed.execute_one("CREATE INDEX tl ON t (label)").expect("index");
        for sql in [
            "SELECT id FROM t WHERE label = 'hit'",
            "SELECT id FROM t WHERE label BETWEEN 'hit' AND 'hit' ORDER BY label",
            "SELECT id FROM t WHERE label >= 'hit'",
        ] {
            assert_eq!(plain.execute_one(sql).expect("plain").rows, indexed.execute_one(sql).expect("indexed").rows, "{sql}");
        }
    }

    /// 事务内的索引重建推迟到提交或读镜像时执行,读镜像前必须先落下。
    ///
    /// 这里在事务内取镜像并直接重开索引树,确认写语句本身没有把半成品的
    /// 索引页交出去,提交也不再改动镜像。
    #[test]
    fn deferred_index_rebuilds_land_before_the_image_is_read() {
        let mut engine = Engine::new(None).expect("blank");
        engine.execute_one("CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT)").expect("create");
        engine.execute_one("CREATE INDEX tl ON t (label)").expect("index");
        engine.execute_one("BEGIN").expect("begin");
        for row in 1..=32i64 {
            engine.execute_one(&format!("INSERT INTO t VALUES ({row}, 'row-{row}')")).expect("insert");
        }
        let image = engine.snapshot().expect("image").to_vec();
        let objects = crate::read_schema(&image, TableLimits::default()).expect("schema");
        let index = objects.iter().find(|object| object.kind == SchemaObjectKind::Index).expect("index object");
        let root = u32::try_from(index.root_page.expect("index root")).expect("root page");
        let entries = read_index_table_with(&image, root, TableLimits::default(), &[Collation::Binary]).expect("index tree");
        assert_eq!(entries.len(), 32, "the index tree must already hold the inserts made so far");
        engine.execute_one("COMMIT").expect("commit");
        assert_eq!(engine.snapshot().expect("image"), image.as_slice(), "commit must not add work of its own");
    }

    /// 整事务回滚丢掉推迟的重建,镜像退回事务前的索引树。
    #[test]
    fn deferred_index_rebuilds_are_dropped_on_rollback() {
        let mut engine = Engine::new(None).expect("blank");
        engine.execute_one("CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT)").expect("create");
        engine.execute_one("CREATE INDEX tl ON t (label)").expect("index");
        engine.execute_one("INSERT INTO t VALUES (1, 'kept')").expect("seed");
        let before = engine.snapshot().expect("image").to_vec();
        engine.execute_one("BEGIN").expect("begin");
        engine.execute_one("INSERT INTO t VALUES (2, 'dropped')").expect("insert");
        engine.execute_one("ROLLBACK").expect("rollback");
        assert_eq!(engine.snapshot().expect("image"), before.as_slice(), "rollback restores the pre-transaction image");
        // autocommit 仍然立即重建,事务外的镜像不进入待重建状态。
        engine.execute_one("INSERT INTO t VALUES (3, 'later')").expect("autocommit insert");
        assert_eq!(engine.execute_one("SELECT count(*) FROM t").expect("rows").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(engine.execute_one("PRAGMA integrity_check").expect("check").rows, vec![vec![text("ok")]]);
    }

    #[test]
    fn select_rowid_alias_equality_fetches_the_single_row() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT)",
            "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')",
        ]);
        // A hit on the rowid alias is bounded to the one candidate row.
        assert_eq!(engine.execute_one("SELECT label FROM t WHERE id = 2").expect("point hit").rows, vec![vec![text("b")]]);
        // A miss yields no rows, exactly like the full scan.
        assert!(engine.execute_one("SELECT label FROM t WHERE id = 99").expect("point miss").rows.is_empty());
        // The implicit `rowid` name is pinned too.
        assert_eq!(engine.execute_one("SELECT label FROM t WHERE rowid = 3").expect("rowid hit").rows, vec![vec![text("c")]]);
        // Reversed operand order pins as well.
        assert_eq!(engine.execute_one("SELECT label FROM t WHERE 1 = id").expect("reversed pin").rows, vec![vec![text("a")]]);
        // An `OR` of two rowid equalities is not a top-level conjunct, so both
        // rows come back through the full scan.
        assert_eq!(
            engine.execute_one("SELECT label FROM t WHERE id = 1 OR id = 3").expect("or scan").rows,
            vec![vec![text("a")], vec![text("c")]]
        );
        // A pinned equality combined with another conjunct keeps the filter.
        assert!(engine.execute_one("SELECT label FROM t WHERE id = 2 AND label = 'zzz'").expect("pin plus filter").rows.is_empty());
    }

    #[test]
    fn select_indexed_equality_matches_the_full_scan() {
        // Duplicate labels make the index non-unique, so the probe returns
        // more than one rowid and their order must stay the full scan's rowid
        // order.
        let statements: &[&str] = &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, score REAL)",
            "INSERT INTO t VALUES (5,'a',1.5),(2,'b',2.5),(9,'a',3.5),(4,'c',NULL)",
        ];
        let mut plain = joined_engine(statements);
        let mut indexed = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, score REAL)",
            "INSERT INTO t VALUES (5,'a',1.5),(2,'b',2.5),(9,'a',3.5),(4,'c',NULL)",
            "CREATE INDEX tl ON t (label)",
        ]);
        for sql in [
            "SELECT id, label FROM t WHERE label = 'a'",
            "SELECT id FROM t WHERE label = 'b'",
            "SELECT id FROM t WHERE label = 'zzz'",
            "SELECT id FROM t WHERE label = 'a' AND score > 2",
            "SELECT id, label, score FROM t WHERE label = 'a' ORDER BY id DESC",
        ] {
            assert_eq!(plain.execute_one(sql).expect("plain").rows, indexed.execute_one(sql).expect("indexed").rows, "{sql}");
        }
        // Affinity mismatch keeps the full scan but never changes the answer.
        assert_eq!(
            indexed.execute_one("SELECT id FROM t WHERE label = 5").expect("text affinity").rows,
            plain.execute_one("SELECT id FROM t WHERE label = 5").expect("plain affinity").rows
        );
    }

    #[test]
    fn unique_index_point_lookup_and_multi_column_probe() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)",
            "CREATE UNIQUE INDEX tu ON t (a)",
            "CREATE INDEX tab ON t (a, b)",
            "INSERT INTO t VALUES (3,'x',1),(1,'y',2),(7,'z',3)",
        ]);
        // The unique index probe finds the single row.
        assert_eq!(engine.execute_one("SELECT id FROM t WHERE a = 'x'").expect("unique").rows, vec![vec![Cell::Integer(3)]]);
        // The multi-column probe covers both key columns.
        assert_eq!(engine.execute_one("SELECT id FROM t WHERE a = 'x' AND b = 1").expect("multi").rows, vec![vec![Cell::Integer(3)]]);
        // A partial multi-column key falls back to the full scan.
        assert!(engine.execute_one("SELECT id FROM t WHERE a = 'x' AND b = 99").expect("miss").rows.is_empty());
        // A predicate on a non-leading column keeps the full scan.
        assert_eq!(engine.execute_one("SELECT id FROM t WHERE b = 2").expect("non-leading").rows, vec![vec![Cell::Integer(1)]]);
    }

    #[test]
    fn index_probe_follows_writes() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT)",
            "CREATE INDEX tl ON t (label)",
            "INSERT INTO t VALUES (1,'a'),(2,'b')",
        ]);
        assert_eq!(engine.execute_one("SELECT id FROM t WHERE label = 'b'").expect("b").rows, vec![vec![Cell::Integer(2)]]);
        engine.execute_one("UPDATE t SET label = 'c' WHERE id = 2").expect("update");
        assert!(engine.execute_one("SELECT id FROM t WHERE label = 'b'").expect("stale miss").rows.is_empty());
        assert_eq!(engine.execute_one("SELECT id FROM t WHERE label = 'c'").expect("new key").rows, vec![vec![Cell::Integer(2)]]);
        engine.execute_one("DELETE FROM t WHERE id = 1").expect("delete");
        assert!(engine.execute_one("SELECT id FROM t WHERE label = 'a'").expect("deleted").rows.is_empty());
    }

    #[test]
    fn upsert_updates_in_place_and_keeps_unassigned_columns() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, score REAL)",
            "INSERT INTO t VALUES (1,'a',1.5),(2,'b',2.5)",
        ]);
        // Two supplied rows conflict with the same stored row inside one
        // statement: the second one must see the first one's write.
        engine
            .execute_one(
                "INSERT INTO t (id,label,score) VALUES (2,'bee',20),(2,'boo',30) ON CONFLICT(id) DO UPDATE SET score = excluded.score",
            )
            .expect("multi upsert");
        assert_eq!(
            engine.execute_one("SELECT id, label, score FROM t ORDER BY id").expect("rows").rows,
            vec![vec![Cell::Integer(1), text("a"), Cell::Real(1.5)], vec![Cell::Integer(2), text("b"), Cell::Real(30.0)]]
        );
        // A fresh INSERT attaches a new row, and a later upsert still finds it.
        engine.execute_one("INSERT INTO t (id,label,score) VALUES (3,'c',3.5)").expect("insert");
        engine
            .execute_one("INSERT INTO t (id,label,score) VALUES (3,'x',4.5) ON CONFLICT(id) DO UPDATE SET score = excluded.score")
            .expect("upsert new");
        assert_eq!(
            engine.execute_one("SELECT label, score FROM t WHERE id = 3").expect("new row").rows,
            vec![vec![text("c"), Cell::Real(4.5)]]
        );
        // A trigger on the table keeps the reload path honest.
        engine.execute_one("CREATE TABLE audit(score REAL)").expect("audit");
        engine.execute_one("CREATE TRIGGER t_audit AFTER UPDATE ON t BEGIN INSERT INTO audit VALUES (NEW.score); END").expect("trigger");
        engine
            .execute_one("INSERT INTO t (id,label,score) VALUES (3,'x',9.5) ON CONFLICT(id) DO UPDATE SET score = excluded.score")
            .expect("upsert with trigger");
        assert_eq!(engine.execute_one("SELECT score FROM audit").expect("audit rows").rows, vec![vec![Cell::Real(9.5)]]);
    }

    #[test]
    fn rowid_keyed_upsert_honors_do_nothing_and_the_update_predicate() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, score REAL)",
            "INSERT INTO t VALUES (1,'a',1.5),(2,'b',2.5)",
        ]);
        engine.execute_one("INSERT INTO t (id,label,score) VALUES (1,'a',9) ON CONFLICT(id) DO NOTHING").expect("do nothing");
        engine
            .execute_one("INSERT INTO t (id,label,score) VALUES (2,'b',8) ON CONFLICT(id) DO UPDATE SET score = excluded.score WHERE excluded.score > score")
            .expect("predicate true");
        engine
            .execute_one("INSERT INTO t (id,label,score) VALUES (1,'a',1) ON CONFLICT(id) DO UPDATE SET score = excluded.score WHERE excluded.score > score")
            .expect("predicate false");
        engine.execute_one("INSERT INTO t (id,label,score) VALUES (3,'c',3.5) ON CONFLICT(id) DO UPDATE SET score = excluded.score").expect("insert");
        assert_eq!(
            engine.execute_one("SELECT id, label, score FROM t ORDER BY id").expect("rows").rows,
            vec![
                vec![Cell::Integer(1), text("a"), Cell::Real(1.5)],
                vec![Cell::Integer(2), text("b"), Cell::Real(8.0)],
                vec![Cell::Integer(3), text("c"), Cell::Real(3.5)],
            ]
        );
    }

    #[test]
    fn rowid_keyed_upsert_still_reports_other_unique_conflicts() {
        let mut engine = joined_engine(&["CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT UNIQUE)", "INSERT INTO t VALUES (1,'a')"]);
        let error = engine
            .execute_one("INSERT INTO t (id,label) VALUES (2,'a') ON CONFLICT(id) DO UPDATE SET label = excluded.label")
            .expect_err("a column UNIQUE constraint outside the conflict target must still abort");
        assert_eq!(error.code, "unique-constraint");
    }

    #[test]
    fn real_bound_rowid_upsert_takes_integer_affinity_before_conflict_detection() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, score REAL)",
            "INSERT INTO t VALUES (1,'a',1.5),(2,'b',2.5)",
        ]);
        // SQLite applies the column affinity to a bound value before conflict
        // detection: `2.0` into the rowid alias becomes the integer `2`, so the
        // upsert rewrites row 2 instead of colliding and surfacing `UNIQUE`.
        engine
            .execute_with_parameters(
                "INSERT INTO t (id,label,score) VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET score = excluded.score",
                &[Cell::Real(2.0), text("bee"), Cell::Real(30.0)],
            )
            .expect("real rowid upsert");
        assert_eq!(
            engine.execute_one("SELECT id, label, score FROM t ORDER BY id").expect("rows").rows,
            vec![vec![Cell::Integer(1), text("a"), Cell::Real(1.5)], vec![Cell::Integer(2), text("b"), Cell::Real(30.0)]]
        );
    }

    #[test]
    fn lean_upsert_uses_defaults_for_excluded_columns_outside_the_insert_list() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT DEFAULT 'd', score REAL)",
            "INSERT INTO t VALUES (1,'a',1.5)",
        ]);
        // `excluded.label` is the would-be-inserted row, so an unsupplied column
        // reads its default: the rewrite sets `label` to the default `'d'`.
        engine
            .execute_one("INSERT INTO t (id,score) VALUES (1,9.5) ON CONFLICT(id) DO UPDATE SET label = excluded.label, score = excluded.score")
            .expect("excluded default");
        assert_eq!(
            engine.execute_one("SELECT label, score FROM t WHERE id = 1").expect("row").rows,
            vec![vec![text("d"), Cell::Real(9.5)]]
        );
    }

    #[test]
    fn bound_point_lookups_return_the_same_rows_as_their_literal_forms() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, n INTEGER)",
            "INSERT INTO t VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40)",
            "CREATE INDEX tl ON t (label)",
            "CREATE INDEX tn ON t (n)",
        ]);
        // 下推键既可以是字面量，也可以是执行期已知的参数；两条路径必须给出同一行集，
        // 只是参数形态不再退化成整表扫描。
        for (literal, bound) in [
            ("SELECT label FROM t WHERE id = 3", "SELECT label FROM t WHERE id = ?"),
            ("SELECT id FROM t WHERE label = 'c'", "SELECT id FROM t WHERE label = ?"),
            ("SELECT id FROM t WHERE n >= 20 AND n <= 30", "SELECT id FROM t WHERE n >= ? AND n <= ?"),
        ] {
            let expected = engine.execute_one(literal).expect("literal form").rows;
            let parameters: &[Cell] = if literal.contains("id = 3") {
                &[Cell::Integer(3)]
            }
            else if literal.contains("label") {
                &[text("c")]
            }
            else {
                &[Cell::Integer(20), Cell::Integer(30)]
            };
            let actual = engine.execute_with_parameters(bound, parameters).expect("bound form").remove(0).rows;
            assert_eq!(actual, expected, "`{bound}` must match `{literal}`");
        }
    }

    #[test]
    fn bound_lookup_keeps_the_full_scan_result_for_a_mixed_storage_class() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT)",
            "INSERT INTO t VALUES (1,'5'),(2,'6')",
            "CREATE INDEX tl ON t (label)",
        ]);
        // 文本列与整数比较时亲和类不符，探针必须放弃下推。此处只锁定「下推不改变
        // 结果」：字面量形态与绑定形态必须给出同一行集，具体行集由全表谓词决定。
        let literal = engine.execute_one("SELECT id FROM t WHERE label = 5").expect("literal form").rows;
        let bound = engine.execute_with_parameters("SELECT id FROM t WHERE label = ?", &[Cell::Integer(5)]).expect("bound form").remove(0).rows;
        assert_eq!(bound, literal, "the probe must not narrow a mixed storage-class comparison");
    }

    #[test]
    fn integral_real_keys_match_their_literal_forms() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, n INTEGER)",
            "INSERT INTO t VALUES (1,'a',10),(2,'b',20),(3,'c',30)",
            "CREATE INDEX tn ON t (n)",
        ]);
        // JS 绑定的数字是 REAL 存储类；无小数部分时亲和转换后应命中同一行集。
        for (bound_sql, parameters, literal_sql) in [
            ("SELECT label FROM t WHERE id = ?", vec![Cell::Real(3.0)], "SELECT label FROM t WHERE id = 3.0"),
            ("SELECT label FROM t WHERE id = ?", vec![Cell::Real(3.5)], "SELECT label FROM t WHERE id = 3.5"),
            ("SELECT id FROM t WHERE n = ?", vec![Cell::Real(20.0)], "SELECT id FROM t WHERE n = 20.0"),
            (
                "SELECT id FROM t WHERE n BETWEEN ? AND ?",
                vec![Cell::Real(10.0), Cell::Real(20.0)],
                "SELECT id FROM t WHERE n BETWEEN 10.0 AND 20.0",
            ),
        ] {
            let bound = engine.execute_with_parameters(bound_sql, &parameters).expect("bound form").remove(0).rows;
            let literal = engine.execute_one(literal_sql).expect("literal form").rows;
            assert_eq!(bound, literal, "`{bound_sql}` must match `{literal_sql}`");
        }
        // 整数索引探针确实吃到了 REAL 键：缓存里应留下该索引的键集。
        assert!(!engine.index_probes.borrow().is_empty(), "an integral REAL key must still use the index probe");
    }

    #[test]
    fn narrowed_update_still_sees_conflicts_outside_the_pinned_row() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT UNIQUE)",
            "INSERT INTO t VALUES (1,'a'),(2,'b')",
        ]);
        // 收窄只对「唯一键仅 rowid」生效；带额外唯一键的表保留整表判定，冲突照常报出。
        let error = engine
            .execute_with_parameters("UPDATE t SET label = ? WHERE id = ?", &[text("b"), Cell::Integer(1)])
            .expect_err("a UNIQUE conflict outside the narrowed row must still abort");
        assert_eq!(error.code, "unique-constraint");
    }

    #[test]
    fn narrowed_update_never_applies_when_the_rowid_changes() {
        let mut engine = joined_engine(&["CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT)", "INSERT INTO t VALUES (1,'a'),(2,'b')"]);
        let error = engine
            .execute_with_parameters("UPDATE t SET id = ? WHERE id = ?", &[Cell::Integer(2), Cell::Integer(1)])
            .expect_err("moving a rowid onto an existing row must conflict");
        assert_eq!(error.code, "unique-constraint");
    }

    #[test]
    fn narrowed_delete_only_narrows_a_rowid_equality() {
        let mut engine = joined_engine(&["CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)", "INSERT INTO t VALUES (1,5),(2,5),(3,6)"]);
        // 非 rowid 谓词必须删掉全部匹配行，不能被单行候选截断。
        engine.execute_one("DELETE FROM t WHERE n = 5").expect("delete every match");
        assert_eq!(engine.execute_one("SELECT id FROM t").expect("rows").rows, vec![vec![Cell::Integer(3)]]);
    }

    #[test]
    fn indexed_delete_reads_only_the_probed_rows() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, n INTEGER)",
            "INSERT INTO t VALUES (1,'a',5),(2,'a',6),(3,'b',7),(4,'c',8),(5,'c',9)",
            "CREATE INDEX tl ON t (label)",
        ]);
        // 非唯一索引等值:两行同键都要删掉,不能只删候选里的第一行。
        assert_eq!(engine.execute_one("DELETE FROM t WHERE label = 'a'").expect("delete by index").affected, 2);
        assert_eq!(
            engine.execute_one("SELECT id FROM t").expect("rows").rows,
            vec![vec![Cell::Integer(3)], vec![Cell::Integer(4)], vec![Cell::Integer(5)]]
        );
        // 首列范围同样走索引候选,并且只删命中范围的行。
        assert_eq!(engine.execute_one("DELETE FROM t WHERE label >= 'c'").expect("delete range").affected, 2);
        assert_eq!(engine.execute_one("SELECT id FROM t").expect("rows").rows, vec![vec![Cell::Integer(3)]]);
        // 索引列之外的谓词没有候选,仍按整表判定正确。
        assert_eq!(engine.execute_one("DELETE FROM t WHERE n > 0").expect("delete by column").affected, 1);
        assert!(engine.execute_one("SELECT id FROM t").expect("rows").rows.is_empty());
    }

    #[test]
    fn indexed_update_narrows_only_when_rowid_is_the_only_key() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, n INTEGER)",
            "INSERT INTO t VALUES (1,'a',1),(2,'a',2),(3,'b',3)",
            "CREATE INDEX tl ON t (label)",
        ]);
        // 唯一键只有 rowid 且不改 rowid:按索引谓词收窄,两个同键行都更新。
        assert_eq!(engine.execute_one("UPDATE t SET n = 9 WHERE label = 'a'").expect("update by index").affected, 2);
        assert_eq!(
            engine.execute_one("SELECT id, n FROM t").expect("rows").rows,
            vec![vec![Cell::Integer(1), Cell::Integer(9)], vec![Cell::Integer(2), Cell::Integer(9)], vec![Cell::Integer(3), Cell::Integer(3)]]
        );
        // 带额外唯一键的表不收窄,冲突判定仍然看得到整表。
        let mut unique = joined_engine(&[
            "CREATE TABLE u(id INTEGER PRIMARY KEY, label TEXT UNIQUE, n INTEGER)",
            "INSERT INTO u VALUES (1,'a',1),(2,'b',2)",
            "CREATE INDEX ul ON u (label)",
        ]);
        let error = unique.execute_one("UPDATE u SET label = 'b' WHERE label = 'a'").expect_err("unique conflict");
        assert_eq!(error.code, "unique-constraint");
        // 不冲突的按索引更新照常生效。
        assert_eq!(unique.execute_one("UPDATE u SET n = 5 WHERE label = 'b'").expect("plain update").affected, 1);
        assert_eq!(unique.execute_one("SELECT n FROM u WHERE id = 2").expect("row").rows, vec![vec![Cell::Integer(5)]]);
    }

    #[test]
    fn index_range_probe_matches_the_full_scan() {
        let base: &[&str] = &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, n INTEGER)",
            "INSERT INTO t VALUES (5,'e',5),(2,'b',2),(9,'i',9),(1,'a',1),(4,'d',4),(3,'c',3)",
        ];
        let mut plain = joined_engine(base);
        let mut indexed = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, n INTEGER)",
            "INSERT INTO t VALUES (5,'e',5),(2,'b',2),(9,'i',9),(1,'a',1),(4,'d',4),(3,'c',3)",
            "CREATE INDEX tl ON t (label)",
            "CREATE INDEX tn ON t (n)",
        ]);
        for sql in [
            "SELECT id, label FROM t WHERE label > 'b'",
            "SELECT id FROM t WHERE label >= 'b' AND label < 'd'",
            "SELECT id FROM t WHERE label <= 'c'",
            // The literal on the left must flip the comparison.
            "SELECT id FROM t WHERE 'c' < label",
            "SELECT id FROM t WHERE label BETWEEN 'b' AND 'd'",
            "SELECT id, label FROM t WHERE label > 'a' ORDER BY label DESC",
            // Integer affinity goes through the same probe.
            "SELECT id FROM t WHERE n > 2 AND n <= 4",
            // A range the probe cannot use (affinity mismatch, OR) still matches.
            "SELECT id FROM t WHERE label > 2",
            "SELECT id FROM t WHERE label < 'c' OR n = 9",
        ] {
            assert_eq!(plain.execute_one(sql).expect("plain").rows, indexed.execute_one(sql).expect("indexed").rows, "{sql}");
        }
        // Writes invalidate the cached entries, exactly like the equality probe.
        assert!(indexed.execute_one("SELECT id FROM t WHERE label > 'y'").expect("primed miss").rows.is_empty());
        indexed.execute_one("UPDATE t SET label = 'zz' WHERE id = 1").expect("update");
        assert_eq!(
            indexed.execute_one("SELECT id FROM t WHERE label > 'y'").expect("new key hit").rows,
            vec![vec![Cell::Integer(1)]]
        );
        indexed.execute_one("DELETE FROM t WHERE id = 1").expect("delete");
        assert!(indexed.execute_one("SELECT id FROM t WHERE label > 'y'").expect("deleted").rows.is_empty());
    }

    #[test]
    fn subquery_memo_keeps_results_and_invalidates_per_statement() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)",
            "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
            "CREATE TABLE u(id INTEGER PRIMARY KEY, keep INTEGER)",
            "INSERT INTO u VALUES (1,1),(3,0)",
        ]);
        // Uncorrelated `IN`: the memo is built once and reused for every row.
        assert_eq!(
            engine.execute_one("SELECT id FROM t WHERE n IN (SELECT n FROM t WHERE n > 10)").expect("in").rows,
            vec![vec![Cell::Integer(2)], vec![Cell::Integer(3)]]
        );
        assert_eq!(
            engine.execute_one("SELECT count(*) FROM t WHERE id IN (SELECT id FROM t WHERE n > 10)").expect("in count").rows,
            vec![vec![Cell::Integer(2)]]
        );
        // Correlated `EXISTS` must not be memoized across outer rows.
        assert_eq!(
            engine.execute_one("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id AND u.keep = 1)").expect("correlated").rows,
            vec![vec![Cell::Integer(1)]]
        );
        // A later statement sees the new rows: the memo never survives a statement.
        engine.execute_one("INSERT INTO t VALUES (4,40)").expect("insert");
        assert_eq!(
            engine.execute_one("SELECT id FROM t WHERE n IN (SELECT n FROM t WHERE n > 10)").expect("in again").rows,
            vec![vec![Cell::Integer(2)], vec![Cell::Integer(3)], vec![Cell::Integer(4)]]
        );
        // `IN` with a NULL on both sides keeps SQLite's three-valued result.
        engine.execute_one("CREATE TABLE m(id INTEGER, v INTEGER)").expect("create m");
        engine.execute_one("INSERT INTO m VALUES (1,NULL),(2,5),(3,NULL)").expect("insert m");
        assert_eq!(
            engine.execute_one("SELECT id FROM m WHERE v IN (SELECT v FROM m WHERE id = 1)").expect("null in").rows,
            Vec::<Vec<Cell>>::new()
        );
        assert_eq!(
            engine.execute_one("SELECT id FROM m WHERE v NOT IN (SELECT v FROM m WHERE id = 2)").expect("null not in").rows,
            Vec::<Vec<Cell>>::new()
        );
    }

    #[test]
    fn distinct_and_union_keep_first_occurrence_with_hash_signatures() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, n INTEGER, r REAL)",
            "INSERT INTO t VALUES (1,'b',1,1.5),(2,'a',2,2.0),(3,'b',2,NULL),(4,'a',2,2.0),(5,'c',3,3.5)",
        ]);
        assert_eq!(
            engine.execute_one("SELECT DISTINCT k FROM t").expect("distinct text").rows,
            vec![vec![text("b")], vec![text("a")], vec![text("c")]]
        );
        // A whole-valued real compares equal to the same integer, so `2.0` and
        // `2` collapse into one output row.
        assert_eq!(
            engine.execute_one("SELECT DISTINCT n FROM t").expect("distinct numbers").rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)], vec![Cell::Integer(3)]]
        );
        assert_eq!(
            engine.execute_one("SELECT DISTINCT r FROM t").expect("distinct real").rows,
            vec![vec![Cell::Real(1.5)], vec![Cell::Real(2.0)], vec![Cell::Null], vec![Cell::Real(3.5)]]
        );
        // `UNION` deduplicates the same way and keeps first-seen order.
        assert_eq!(
            engine.execute_one("SELECT k FROM t WHERE id < 3 UNION SELECT k FROM t WHERE id > 2").expect("union").rows,
            vec![vec![text("b")], vec![text("a")], vec![text("c")]]
        );
        // `INTERSECT` and `EXCEPT` still use the same equality.
        assert_eq!(
            engine.execute_one("SELECT k FROM t INTERSECT SELECT k FROM t WHERE n = 2").expect("intersect").rows,
            vec![vec![text("b")], vec![text("a")]]
        );
    }

    #[test]
    fn order_by_rowid_limit_reads_only_the_prefix() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')",
        ]);
        // The rowid-alias order with a limit is the scan prefix itself.
        assert_eq!(
            engine.execute_one("SELECT id, v FROM t ORDER BY id LIMIT 2").expect("head").rows,
            vec![vec![Cell::Integer(1), text("a")], vec![Cell::Integer(2), text("b")]]
        );
        assert_eq!(
            engine.execute_one("SELECT id, v FROM t ORDER BY rowid LIMIT 1 OFFSET 3").expect("offset head").rows,
            vec![vec![Cell::Integer(4), text("d")]]
        );
        // `count(*)` over an ordered limit still counts the whole table.
        assert_eq!(engine.execute_one("SELECT count(*) FROM t ORDER BY id LIMIT 2").expect("count").rows, vec![vec![Cell::Integer(5)]]);
        // A `WHERE` can drop prefix rows, so the bounded read must not apply.
        assert_eq!(
            engine.execute_one("SELECT id FROM t WHERE v <> 'a' ORDER BY id LIMIT 2").expect("filtered").rows,
            vec![vec![Cell::Integer(2)], vec![Cell::Integer(3)]]
        );
        // An aliased projection re-points the order name at another column.
        assert_eq!(
            engine.execute_one("SELECT v AS id FROM t ORDER BY id LIMIT 2").expect("alias order").rows,
            vec![vec![text("a")], vec![text("b")]]
        );
    }

    #[test]
    fn group_by_hash_keeps_first_appearance_order() {
        let mut engine = joined_engine(&[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, n INTEGER, r REAL)",
            "INSERT INTO t VALUES (1,'b',1,1.5),(2,'a',1,2.5),(3,'b',2,2.0),(4,'a',1,7.0),(5,'c',2,NULL)",
        ]);
        // Text keys: groups appear in first-row order, not sorted.
        assert_eq!(
            engine.execute_one("SELECT k, count(*) FROM t GROUP BY k").expect("text groups").rows,
            vec![vec![text("b"), Cell::Integer(2)], vec![text("a"), Cell::Integer(2)], vec![text("c"), Cell::Integer(1)]]
        );
        // A NULL key forms its own group, and a whole-valued real compares
        // equal to the same integer, so `2.0` and `2` share one group.
        assert_eq!(
            engine.execute_one("SELECT count(*) FROM t GROUP BY COALESCE(r, n)").expect("mixed keys").rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(1)], vec![Cell::Integer(2)], vec![Cell::Integer(1)]]
        );
        assert_eq!(
            engine.execute_one("SELECT count(*) FROM t GROUP BY r").expect("null group").rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(1)], vec![Cell::Integer(1)], vec![Cell::Integer(1)], vec![Cell::Integer(1)]]
        );
    }

    #[test]
    fn full_join_using_keeps_matched_pairs_and_unmatched_rows() {
        let mut engine = joined_engine(LEFT_RIGHT);
        let full = engine.execute_one("SELECT * FROM l FULL OUTER JOIN r USING(id)").expect("full");
        assert_eq!(full.columns, vec!["id", "lv", "rv"]);
        assert_eq!(
            full.rows,
            vec![
                vec![Cell::Integer(1), text("a"), Cell::Null],
                vec![Cell::Integer(2), text("b"), text("B")],
                vec![Cell::Integer(3), Cell::Null, text("C")],
            ]
        );
        assert_eq!(engine.execute_one("SELECT count(*) FROM l FULL OUTER JOIN r USING(id)").expect("count").rows, vec![vec![Cell::Integer(3)]]);

        // `FULL` keeps the multiplicity of matched pairs instead of deduplicating rows.
        let mut duplicates = joined_engine(&[
            "CREATE TABLE d1(id INTEGER)",
            "INSERT INTO d1 VALUES (1),(1)",
            "CREATE TABLE d2(id INTEGER)",
            "INSERT INTO d2 VALUES (1),(1)",
        ]);
        assert_eq!(
            duplicates.execute_one("SELECT id FROM d1 FULL JOIN d2 USING(id)").expect("duplicate pairs").rows,
            vec![vec![Cell::Integer(1)]; 4]
        );

        // Unmatched rows from both sides are kept, including left-side duplicates.
        let mut uneven = joined_engine(&[
            "CREATE TABLE e1(id INTEGER)",
            "INSERT INTO e1 VALUES (1),(1)",
            "CREATE TABLE e2(id INTEGER)",
            "INSERT INTO e2 VALUES (2)",
        ]);
        assert_eq!(
            uneven.execute_one("SELECT id FROM e1 FULL JOIN e2 USING(id)").expect("unmatched rows").rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(1)], vec![Cell::Integer(2)]]
        );
    }

    #[test]
    fn natural_join_matches_shared_column_names() {
        let mut engine = joined_engine(LEFT_RIGHT);
        let inner = engine.execute_one("SELECT * FROM l NATURAL JOIN r").expect("natural");
        assert_eq!(inner.columns, vec!["id", "lv", "rv"]);
        assert_eq!(inner.rows, vec![vec![Cell::Integer(2), text("b"), text("B")]]);
        assert_eq!(
            engine.execute_one("SELECT * FROM l NATURAL LEFT JOIN r").expect("natural left").rows,
            vec![vec![Cell::Integer(1), text("a"), Cell::Null], vec![Cell::Integer(2), text("b"), text("B")]]
        );
        assert_eq!(
            engine.execute_one("SELECT * FROM l NATURAL RIGHT JOIN r").expect("natural right").rows,
            vec![vec![Cell::Integer(2), text("b"), text("B")], vec![Cell::Integer(3), Cell::Null, text("C")]]
        );

        // With no shared name `NATURAL` degrades to a Cartesian product.
        let mut product = joined_engine(&[
            "CREATE TABLE n1(x INTEGER)",
            "INSERT INTO n1 VALUES (1),(2)",
            "CREATE TABLE n2(y INTEGER)",
            "INSERT INTO n2 VALUES (10),(20)",
        ]);
        let cartesian = product.execute_one("SELECT * FROM n1 NATURAL JOIN n2").expect("natural product");
        assert_eq!(cartesian.columns, vec!["x", "y"]);
        assert_eq!(
            cartesian.rows,
            vec![
                vec![Cell::Integer(1), Cell::Integer(10)],
                vec![Cell::Integer(1), Cell::Integer(20)],
                vec![Cell::Integer(2), Cell::Integer(10)],
                vec![Cell::Integer(2), Cell::Integer(20)],
            ]
        );

        // Name matching folds ASCII case and keeps the left table's spelling.
        let mut folded = joined_engine(&[
            "CREATE TABLE t(a INTEGER, b TEXT)",
            "INSERT INTO t VALUES (1,'x')",
            "CREATE TABLE u(A INTEGER, B TEXT, c TEXT)",
            "INSERT INTO u VALUES (1,'x','y')",
        ]);
        let natural = folded.execute_one("SELECT * FROM t NATURAL JOIN u").expect("natural case fold");
        assert_eq!(natural.columns, vec!["a", "b", "c"]);
        assert_eq!(natural.rows, vec![vec![Cell::Integer(1), text("x"), text("y")]]);
    }

    #[test]
    fn using_columns_follow_left_table_order_and_fold_case() {
        let setup = [
            "CREATE TABLE p(a INTEGER, b INTEGER, c TEXT)",
            "INSERT INTO p VALUES (1,2,'p')",
            "CREATE TABLE q(c TEXT, id INTEGER, b INTEGER, d TEXT)",
            "INSERT INTO q VALUES ('p',9,2,'q')",
        ];
        let mut engine = joined_engine(&setup);
        // The merged column keeps the position of the left table's column, and
        // listing the `USING` columns in another order changes nothing.
        for sql in ["SELECT * FROM p JOIN q USING(b,c)", "SELECT * FROM p JOIN q USING(c,b)"] {
            let result = engine.execute_one(sql).expect("multi-column using");
            assert_eq!(result.columns, vec!["a", "b", "c", "id", "d"]);
            assert_eq!(result.rows, vec![vec![Cell::Integer(1), Cell::Integer(2), text("p"), Cell::Integer(9), text("q")]]);
        }

        let mut folded = joined_engine(&[
            "CREATE TABLE t2(a INTEGER)",
            "INSERT INTO t2 VALUES (1)",
            "CREATE TABLE u2(A INTEGER)",
            "INSERT INTO u2 VALUES (1)",
        ]);
        let result = folded.execute_one("SELECT * FROM t2 JOIN u2 USING(a)").expect("case fold");
        assert_eq!(result.columns, vec!["a"]);
        assert_eq!(result.rows, vec![vec![Cell::Integer(1)]]);
    }

    #[test]
    fn using_and_natural_reject_invalid_clauses() {
        let setup = ["CREATE TABLE one(x INTEGER)", "INSERT INTO one VALUES (1)", "CREATE TABLE two(x INTEGER)", "INSERT INTO two VALUES (1)"];
        let mut engine = joined_engine(&setup);

        let empty = engine.execute_one("SELECT * FROM one JOIN two USING()").expect_err("empty using");
        assert_eq!(empty.code, "syntax");
        assert_eq!(empty.message, "near \")\": syntax error");

        let missing = engine.execute_one("SELECT * FROM one JOIN two USING(nope)").expect_err("missing using column");
        assert_eq!(missing.code, "unknown-column");
        assert_eq!(missing.message, "cannot join using column nope - column not present in both tables");

        for sql in ["SELECT * FROM one NATURAL JOIN two ON one.x = two.x", "SELECT * FROM one NATURAL JOIN two USING(x)"] {
            let error = engine.execute_one(sql).expect_err("natural with clause");
            assert_eq!(error.code, "syntax");
            assert_eq!(error.message, "a NATURAL join may not have an ON or USING clause");
        }

        // `USING` compares whole identifiers: `i` is not the column `ID`.
        let mut mismatched = joined_engine(&[
            "CREATE TABLE t(i INTEGER, j TEXT)",
            "INSERT INTO t VALUES (1,'y')",
            "CREATE TABLE u(ID INTEGER, J TEXT, k TEXT)",
            "INSERT INTO u VALUES (1,'y','z')",
        ]);
        let error = mismatched.execute_one("SELECT * FROM t JOIN u USING(i,j)").expect_err("name mismatch");
        assert_eq!(error.code, "unknown-column");
        assert_eq!(error.message, "cannot join using column i - column not present in both tables");

        // A join without `ON` or `USING` still pairs every row like a product;
        // `RIGHT` only changes which unmatched rows SQLite keeps.
        let mut product = joined_engine(LEFT_RIGHT);
        let result = product.execute_one("SELECT * FROM l RIGHT JOIN r").expect("right without on");
        assert_eq!(result.columns, vec!["id", "lv", "id", "rv"]);
        assert_eq!(
            result.rows,
            vec![
                vec![Cell::Integer(1), text("a"), Cell::Integer(2), text("B")],
                vec![Cell::Integer(1), text("a"), Cell::Integer(3), text("C")],
                vec![Cell::Integer(2), text("b"), Cell::Integer(2), text("B")],
                vec![Cell::Integer(2), text("b"), Cell::Integer(3), text("C")],
            ]
        );
    }

    #[test]
    fn chained_using_joins_keep_the_merged_column() {
        let setup = [
            "CREATE TABLE m1(a INTEGER, b INTEGER)",
            "INSERT INTO m1 VALUES (1,2)",
            "CREATE TABLE m2(a INTEGER, c INTEGER)",
            "INSERT INTO m2 VALUES (1,3)",
            "CREATE TABLE m3(b INTEGER, a INTEGER, d INTEGER)",
            "INSERT INTO m3 VALUES (2,1,4)",
        ];
        let mut engine = joined_engine(&setup);
        let expected = vec![vec![Cell::Integer(1), Cell::Integer(2), Cell::Integer(3), Cell::Integer(2), Cell::Integer(4)]];

        let chained = engine.execute_one("SELECT * FROM m1 JOIN m2 USING(a) JOIN m3 USING(a)").expect("chained using");
        assert_eq!(chained.columns, vec!["a", "b", "c", "b", "d"]);
        assert_eq!(chained.rows, expected);

        let right = engine.execute_one("SELECT * FROM m1 JOIN m2 USING(a) RIGHT JOIN m3 USING(a)").expect("chained right");
        assert_eq!(right.columns, vec!["a", "b", "c", "b", "d"]);
        assert_eq!(right.rows, expected);
    }

    #[test]
    fn update_reads_every_right_hand_side_from_the_pre_statement_row() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(a INTEGER, b INTEGER)").expect("create");
        engine.execute_one("INSERT INTO t VALUES (1,2)").expect("insert");
        engine.execute_one("UPDATE t SET a = b, b = a").expect("swap");
        assert_eq!(engine.execute_one("SELECT a, b FROM t").expect("select").rows, vec![vec![Cell::Integer(2), Cell::Integer(1)]]);

        // A three-step rotation proves the snapshot is taken once per row, not
        // per assignment: `c = a` keeps the original `a`, not the new `b`.
        let mut chain = Engine::new(None).expect("blank engine");
        chain.execute_one("CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER)").expect("create");
        chain.execute_one("INSERT INTO t VALUES (1,2,3)").expect("insert");
        chain.execute_one("UPDATE t SET a = b, b = c, c = a").expect("rotate");
        assert_eq!(
            chain.execute_one("SELECT a, b, c FROM t").expect("select").rows,
            vec![vec![Cell::Integer(2), Cell::Integer(3), Cell::Integer(1)]]
        );
    }

    #[test]
    fn update_keeps_the_last_of_repeated_assignments() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(a INTEGER, b INTEGER)").expect("create");
        engine.execute_one("INSERT INTO t VALUES (1,2)").expect("insert");
        engine.execute_one("UPDATE t SET a = 1, a = a + 1").expect("duplicate");
        assert_eq!(engine.execute_one("SELECT a FROM t").expect("select").rows, vec![vec![Cell::Integer(2)]]);
        engine.execute_one("UPDATE t SET a = 7, a = b").expect("duplicate");
        assert_eq!(engine.execute_one("SELECT a, b FROM t").expect("select").rows, vec![vec![Cell::Integer(2), Cell::Integer(2)]]);
    }

    #[test]
    fn update_reports_the_exact_affected_count() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(a INTEGER, b TEXT)").expect("create");
        engine.execute_one("INSERT INTO t VALUES (1,'x'),(2,'y'),(3,'z')").expect("insert");
        assert_eq!(engine.execute_one("UPDATE t SET b = 'q' WHERE a = 99").expect("update").affected, 0);
        assert_eq!(engine.execute_one("UPDATE t SET b = 'q' WHERE a = 2").expect("update").affected, 1);
        assert_eq!(engine.execute_one("UPDATE t SET b = 'q' WHERE 1").expect("update").affected, 3);
        // A same-value assignment still counts as a touched row.
        assert_eq!(engine.execute_one("UPDATE t SET b = b").expect("update").affected, 3);
    }

    #[test]
    fn update_rolls_back_every_row_when_a_check_fails() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER CHECK(v > 0))").expect("create");
        engine.execute_one("INSERT INTO t VALUES (1,5),(2,6),(3,7)").expect("insert");
        let before = engine.execute_one("SELECT id, v FROM t").expect("select").rows;
        let error = engine.execute_one("UPDATE t SET v = v - 10").expect_err("check must fail");
        assert_eq!(error.code, "check-constraint");
        assert_eq!(error.clause.as_deref(), Some("CHECK"));
        assert_eq!(engine.execute_one("SELECT id, v FROM t").expect("select").rows, before);
    }

    #[test]
    fn insert_constraint_failures_keep_distinct_codes() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine
            .execute_one("CREATE TABLE t(id INTEGER PRIMARY KEY, u TEXT UNIQUE, n TEXT NOT NULL, c INTEGER CHECK(c > 0))")
            .expect("create");
        engine.execute_one("INSERT INTO t VALUES (1, 'a', 'x', 1)").expect("insert");

        assert_eq!(engine.execute_one("INSERT INTO t VALUES (1, 'b', 'y', 2)").expect_err("rowid").code, "unique-constraint");
        assert_eq!(engine.execute_one("INSERT INTO t VALUES (2, 'a', 'y', 2)").expect_err("unique column").code, "unique-constraint");
        assert_eq!(engine.execute_one("INSERT INTO t VALUES (3, 'c', NULL, 3)").expect_err("not null").code, "not-null-constraint");
        assert_eq!(engine.execute_one("INSERT INTO t VALUES (4, 'd', 'z', 0)").expect_err("check").code, "check-constraint");

        engine.execute_one("CREATE TABLE parent(id INTEGER PRIMARY KEY)").expect("parent");
        engine.execute_one("CREATE TABLE child(pid INTEGER REFERENCES parent(id))").expect("child");
        assert_eq!(
            engine.execute_one("INSERT INTO child VALUES (7)").expect_err("foreign key").code,
            "SQLITE_CONSTRAINT_FOREIGNKEY"
        );
    }

    #[test]
    fn update_of_a_rowid_alias_runs_in_ascending_order() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").expect("create");
        engine.execute_one("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").expect("insert");
        engine.execute_one("UPDATE t SET id = id - 1").expect("shift down");
        assert_eq!(
            engine.execute_one("SELECT id, v FROM t").expect("select").rows,
            vec![vec![Cell::Integer(0), text("a")], vec![Cell::Integer(1), text("b")], vec![Cell::Integer(2), text("c")],]
        );
        let before = engine.execute_one("SELECT id FROM t").expect("select").rows;
        let error = engine.execute_one("UPDATE t SET id = id + 1").expect_err("shift up collides");
        assert_eq!(error.code, "unique-constraint");
        assert_eq!(error.message, "UNIQUE constraint failed: t.id");
        assert_eq!(engine.execute_one("SELECT id FROM t").expect("select").rows, before);
    }

    #[test]
    fn delete_reports_affected_and_recycles_the_maximum_rowid() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(v TEXT)").expect("create");
        engine.execute_one("INSERT INTO t VALUES ('a'),('b'),('c')").expect("insert");
        assert_eq!(engine.execute_one("DELETE FROM t WHERE rowid = 2").expect("delete").affected, 1);
        assert_eq!(
            engine.execute_one("SELECT rowid, v FROM t").expect("select").rows,
            vec![vec![Cell::Integer(1), text("a")], vec![Cell::Integer(3), text("c")]]
        );
        // The next rowid is the largest live rowid plus one, so the vacated
        // maximum is recycled exactly as SQLite does without `AUTOINCREMENT`.
        let inserted = engine.execute_one("INSERT INTO t VALUES ('d')").expect("insert");
        assert_eq!(inserted.rows, vec![vec![Cell::Integer(4)]]);
        assert_eq!(engine.execute_one("DELETE FROM t").expect("delete").affected, 3);
        assert!(engine.execute_one("SELECT v FROM t").expect("select").rows.is_empty());
        let again = engine.execute_one("INSERT INTO t VALUES ('e')").expect("insert");
        assert_eq!(again.rows, vec![vec![Cell::Integer(1)]]);
    }

    #[test]
    fn returning_reports_new_values_for_update_and_old_values_for_delete() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)").expect("create");
        engine.execute_one("INSERT INTO t VALUES (1,10),(2,20),(3,30)").expect("insert");
        let updated = engine.execute_one("UPDATE t SET v = v + 1 WHERE id >= 2 RETURNING id, v").expect("update returning");
        assert_eq!(updated.columns, vec!["id", "v"]);
        assert_eq!(updated.affected, 2);
        assert_eq!(updated.rows, vec![vec![Cell::Integer(2), Cell::Integer(21)], vec![Cell::Integer(3), Cell::Integer(31)]]);
        // `RETURNING *` expands to the declared columns only, never the rowid.
        let starred = engine.execute_one("UPDATE t SET v = v WHERE id = 1 RETURNING *").expect("star returning");
        assert_eq!(starred.columns, vec!["id", "v"]);
        assert_eq!(starred.rows, vec![vec![Cell::Integer(1), Cell::Integer(10)]]);
        let removed = engine.execute_one("DELETE FROM t RETURNING id").expect("delete returning");
        assert_eq!(removed.affected, 3);
        assert_eq!(removed.rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)], vec![Cell::Integer(3)]]);
        let empty = engine.execute_one("UPDATE t SET v = 1 WHERE id = 99 RETURNING id").expect("empty returning");
        assert_eq!(empty.affected, 0);
        assert!(empty.rows.is_empty());
    }

    #[test]
    fn update_and_delete_reject_shapes_sqlite_also_rejects() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(a INTEGER, b INTEGER)").expect("create");
        engine.execute_one("INSERT INTO t VALUES (1,2)").expect("insert");
        // SQLite accepts a duplicate left-hand column (last one wins) but not a
        // table qualifier on the left of `=`.
        assert_eq!(engine.execute_one("UPDATE t SET t.a = 1").expect_err("qualified lhs").code, "syntax");
        // `DEFAULT` has no meaning in an `UPDATE`; SQLite reports a syntax error
        // and this engine names the clause instead.
        let default = engine.execute_one("UPDATE t SET a = DEFAULT").expect_err("default");
        assert_eq!(default.code, "unsupported-clause");
        assert_eq!(default.clause.as_deref(), Some("DEFAULT"));
        let conflict = engine.execute_one("UPDATE OR REPLACE t SET a = 1").expect_err("conflict clause");
        assert_eq!(conflict.code, "unsupported-clause");
    }

    #[test]
    fn drop_table_clears_catalog_indexes_and_sequence_row() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE c(id INTEGER PRIMARY KEY AUTOINCREMENT, k TEXT UNIQUE)").expect("create c");
        engine.execute_one("CREATE INDEX ix ON c(k)").expect("create ix");
        engine.execute_one("INSERT INTO c(k) VALUES ('x')").expect("insert");
        assert_eq!(
            engine.execute_one("SELECT name, seq FROM sqlite_sequence").expect("sequence").rows,
            vec![vec![text("c"), Cell::Integer(1)]]
        );
        assert_eq!(engine.execute_one("SELECT count(*) FROM sqlite_master").expect("count").rows, vec![vec![Cell::Integer(4)]]);

        let dropped = engine.execute_one("DROP TABLE c").expect("drop c");
        assert_eq!(dropped.statement, "drop-table");
        assert_eq!(dropped.affected, 1);
        // The catalog row, the explicit index row and the automatic `UNIQUE`
        // row all disappear in the same statement; only `sqlite_sequence`
        // remains, plus no sequence row for `c`.
        assert_eq!(
            engine.execute_one("SELECT type, name FROM sqlite_master").expect("master").rows,
            vec![vec![text("table"), text("sqlite_sequence")]]
        );
        assert_eq!(engine.execute_one("SELECT * FROM sqlite_sequence").expect("sequence").rows, Vec::<Vec<Cell>>::new());
        assert_eq!(engine.execute_one("SELECT * FROM c").expect_err("gone").code, "unknown-table");
        assert_eq!(engine.execute_one("DROP TABLE IF EXISTS c").expect("if exists").affected, 0);

        // A second table reuses the freed root page, as SQLite does.
        engine.execute_one("CREATE TABLE d(a INTEGER)").expect("create d");
        assert_eq!(engine.execute_one("SELECT rootpage FROM sqlite_master WHERE name = 'd'").expect("root").rows, vec![vec![Cell::Integer(2)]]);
    }

    #[test]
    fn drop_table_rejects_system_tables_and_missing_names() {
        let mut engine = Engine::new(None).expect("blank engine");
        let master = engine.execute_one("DROP TABLE sqlite_master").expect_err("master");
        assert_eq!(master.code, "readonly-system-table");
        assert_eq!(master.message, "table sqlite_master may not be dropped");
        // `sqlite_sequence` only rejects once it exists; before that SQLite
        // reports an ordinary missing table.
        assert_eq!(engine.execute_one("DROP TABLE sqlite_sequence").expect_err("absent sequence").code, "unknown-table");
        engine.execute_one("CREATE TABLE c(id INTEGER PRIMARY KEY AUTOINCREMENT)").expect("create c");
        assert_eq!(engine.execute_one("DROP TABLE sqlite_sequence").expect_err("sequence").code, "readonly-system-table");
        assert_eq!(engine.execute_one("DROP TABLE nope").expect_err("missing").code, "unknown-table");
        assert_eq!(engine.execute_one("DROP TABLE nope.c").expect_err("unknown schema").code, "unknown-database");
        engine.execute_one("DROP TABLE main.c").expect("drop main.c");
        assert_eq!(engine.execute_one("SELECT * FROM main.c").expect_err("gone").code, "unknown-table");
    }

    #[test]
    fn drop_index_removes_explicit_indexes_only() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(a TEXT UNIQUE, b INTEGER)").expect("create t");
        engine.execute_one("CREATE INDEX ix ON t(b)").expect("create ix");
        let automatic = engine.execute_one("DROP INDEX sqlite_autoindex_t_1").expect_err("autoindex");
        assert_eq!(automatic.code, "autoindex-immutable");
        assert_eq!(automatic.message, "index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped");
        // Even `IF EXISTS` does not soften the refusal, matching SQLite.
        assert_eq!(
            engine.execute_one("DROP INDEX IF EXISTS sqlite_autoindex_t_1").expect_err("autoindex if exists").code,
            "autoindex-immutable"
        );
        assert_eq!(engine.execute_one("DROP INDEX nope").expect_err("missing").code, "unknown-index");
        assert_eq!(engine.execute_one("DROP INDEX IF EXISTS nope").expect("if exists").affected, 0);

        let dropped = engine.execute_one("DROP INDEX ix").expect("drop ix");
        assert_eq!(dropped.statement, "drop-index");
        assert_eq!(
            engine.execute_one("SELECT name FROM sqlite_master WHERE type = 'index'").expect("indexes").rows,
            vec![vec![text("sqlite_autoindex_t_1")]]
        );
        // The automatic index still enforces the `UNIQUE` constraint.
        engine.execute_one("INSERT INTO t VALUES ('a', 1)").expect("insert");
        let duplicate = engine.execute_one("INSERT INTO t VALUES ('a', 2)").expect_err("duplicate key");
        // The writer's automatic-index check names the constraint; the facade
        // surfaces it as a storage rejection, as it did before this change.
        assert!(duplicate.message.contains("UNIQUE constraint failed"), "{}", duplicate.message);
        assert_eq!(engine.execute_one("SELECT count(*) FROM t").expect("count").rows, vec![vec![Cell::Integer(1)]]);
    }

    #[test]
    fn rename_table_rewrites_stored_text_and_automatic_index_names() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t ( a INTEGER , b TEXT UNIQUE )").expect("create");
        engine.execute_one("CREATE INDEX Ix ON t ( b )").expect("index");
        let renamed = engine.execute_one("ALTER TABLE t RENAME TO u").expect("rename");
        assert_eq!(renamed.statement, "alter-table");

        let rows = engine.execute_one("SELECT name, tbl_name, sql FROM sqlite_master").expect("master").rows;
        // SQLite rewrites only the table-name token, quoting the new name and
        // leaving every other byte of the stored DDL untouched.
        assert_eq!(rows[0], vec![text("u"), text("u"), text("CREATE TABLE \"u\" ( a INTEGER , b TEXT UNIQUE )")]);
        // The automatic index follows the table name, keeps its creation order
        // (it predates the explicit index) and has no SQL text.
        assert_eq!(rows[1], vec![text("sqlite_autoindex_u_1"), text("u"), Cell::Null]);
        assert_eq!(rows[2], vec![text("Ix"), text("u"), text("CREATE INDEX Ix ON \"u\" ( b )")]);

        engine.execute_one("INSERT INTO u VALUES (1, 'x')").expect("insert after rename");
        assert_eq!(engine.execute_one("SELECT a FROM u").expect("select").rows, vec![vec![Cell::Integer(1)]]);
        engine.execute_one("INSERT INTO u VALUES (2, 'x')").expect_err("unique survives");
        assert_eq!(engine.execute_one("ALTER TABLE u RENAME TO u").expect_err("exists").code, "table-exists");
        assert_eq!(engine.execute_one("ALTER TABLE u RENAME TO sqlite_x").expect_err("reserved").code, "reserved-name");
        assert_eq!(engine.execute_one("ALTER TABLE nope RENAME TO v").expect_err("missing").code, "unknown-table");
        assert_eq!(engine.execute_one("ALTER TABLE sqlite_master RENAME TO v").expect_err("system").code, "readonly-system-table");
    }

    #[test]
    fn rename_table_moves_the_sequence_row() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE a(x INTEGER PRIMARY KEY AUTOINCREMENT)").expect("create");
        engine.execute_one("INSERT INTO a DEFAULT VALUES").expect("insert");
        engine.execute_one("ALTER TABLE a RENAME TO b").expect("rename");
        assert_eq!(
            engine.execute_one("SELECT name, seq FROM sqlite_sequence").expect("sequence").rows,
            vec![vec![text("b"), Cell::Integer(1)]]
        );
        // A new insert continues above the high-water mark.
        engine.execute_one("INSERT INTO b DEFAULT VALUES").expect("insert after rename");
        assert_eq!(engine.execute_one("SELECT max(x) FROM b").expect("max").rows, vec![vec![Cell::Integer(2)]]);
    }

    #[test]
    fn rename_column_rewrites_checks_and_index_text() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(t INTEGER, x TEXT DEFAULT 'a', y TEXT, CHECK (t > 0 AND x <> 't'))").expect("create");
        engine.execute_one("CREATE INDEX ix ON t(x, t)").expect("index");
        engine.execute_one("ALTER TABLE t RENAME COLUMN t TO tt").expect("rename column");

        let rows = engine.execute_one("SELECT name, sql FROM sqlite_master").expect("master").rows;
        let table_sql = "CREATE TABLE t(tt INTEGER, x TEXT DEFAULT 'a', y TEXT, CHECK (tt > 0 AND x <> 't'))";
        assert_eq!(rows[0], vec![text("t"), text(table_sql)]);
        // The string literal `'t'` is untouched; the identifier in the index
        // column list is not.
        assert_eq!(rows[1], vec![text("ix"), text("CREATE INDEX ix ON t(x, tt)")]);
        assert_eq!(engine.execute_one("SELECT tt FROM t").expect("select").rows, Vec::<Vec<Cell>>::new());
        engine.execute_one("INSERT INTO t(tt) VALUES (1)").expect("insert");
        assert_eq!(engine.execute_one("INSERT INTO t(tt) VALUES (-1)").expect_err("check").code, "check-constraint");
        assert_eq!(engine.execute_one("ALTER TABLE t RENAME COLUMN nope TO x").expect_err("missing col").code, "unknown-column");
        assert_eq!(engine.execute_one("ALTER TABLE t RENAME COLUMN tt TO x").expect_err("duplicate").code, "duplicate-column");
    }

    #[test]
    fn add_column_defaults_existing_rows_and_extends_select_order() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(a INTEGER)").expect("create");
        engine.execute_one("INSERT INTO t VALUES (1), (2)").expect("insert");
        let added = engine.execute_one("ALTER TABLE t ADD COLUMN c INTEGER NOT NULL DEFAULT 5").expect("add");
        assert_eq!(added.statement, "alter-table");
        assert_eq!(
            engine.execute_one("SELECT * FROM t").expect("select").rows,
            vec![vec![Cell::Integer(1), Cell::Integer(5)], vec![Cell::Integer(2), Cell::Integer(5)]]
        );
        // The stored text gains exactly the definition as written, appended
        // before the closing parenthesis.
        assert_eq!(
            engine.execute_one("SELECT sql FROM sqlite_master WHERE name = 't'").expect("sql").rows,
            vec![vec![text("CREATE TABLE t(a INTEGER, c INTEGER NOT NULL DEFAULT 5)")]]
        );
        // The default applies to later inserts that omit the column too.
        engine.execute_one("INSERT INTO t(a) VALUES (3)").expect("insert");
        assert_eq!(engine.execute_one("SELECT c FROM t WHERE a = 3").expect("default").rows, vec![vec![Cell::Integer(5)]]);
        // Stale rows now update through the new column.
        engine.execute_one("UPDATE t SET c = c + 1 WHERE a = 1").expect("update");
        assert_eq!(engine.execute_one("SELECT c FROM t WHERE a = 1").expect("updated").rows, vec![vec![Cell::Integer(6)]]);
    }

    #[test]
    fn add_column_rejects_shapes_sqlite_also_rejects() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(a INTEGER)").expect("create");
        // An empty table accepts the definition; only a stored row would read
        // back NULL, so the refusal is conditional.
        engine.execute_one("ALTER TABLE t ADD COLUMN b TEXT NOT NULL").expect("empty table accepts");
        engine.execute_one("INSERT INTO t VALUES (1, 'x')").expect("insert");
        let not_null = engine.execute_one("ALTER TABLE t ADD COLUMN c TEXT NOT NULL").expect_err("not null");
        assert_eq!(not_null.code, "invalid-alter");
        assert_eq!(not_null.message, "Cannot add a NOT NULL column with default value NULL");
        let primary = engine.execute_one("ALTER TABLE t ADD COLUMN p INTEGER PRIMARY KEY").expect_err("primary key");
        assert_eq!(primary.message, "Cannot add a PRIMARY KEY column");
        let unique = engine.execute_one("ALTER TABLE t ADD COLUMN q TEXT UNIQUE").expect_err("unique");
        assert_eq!(unique.message, "Cannot add a UNIQUE column");
        let duplicate = engine.execute_one("ALTER TABLE t ADD COLUMN a TEXT").expect_err("duplicate");
        assert_eq!(duplicate.code, "duplicate-column");
        assert_eq!(duplicate.message, "duplicate column name: a");
    }

    #[test]
    fn drop_column_removes_values_and_rewrites_stored_text() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, note TEXT)").expect("create");
        engine.execute("INSERT INTO t (a,b,note) VALUES ('x', 1, 'n'), ('y', 2, NULL)").expect("insert");
        let dropped = engine.execute_one("ALTER TABLE t DROP COLUMN b").expect("drop");
        assert_eq!(dropped.statement, "alter-table");
        assert_eq!(dropped.affected, 1);
        // SQLite removes the definition and the separating comma, leaving every
        // other byte of the stored DDL untouched.
        assert_eq!(
            engine.execute_one("SELECT sql FROM sqlite_master WHERE name = 't'").expect("sql").rows,
            vec![vec![text("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, note TEXT)")]]
        );
        assert_eq!(
            engine.execute_one("SELECT id, a, note FROM t ORDER BY id").expect("rows").rows,
            vec![vec![Cell::Integer(1), text("x"), text("n")], vec![Cell::Integer(2), text("y"), Cell::Null],]
        );
        // The shortened table keeps inserting and updating in place.
        engine.execute_one("INSERT INTO t (a, note) VALUES ('z', 'm')").expect("insert");
        engine.execute_one("UPDATE t SET note = 'q' WHERE id = 1").expect("update");
        assert_eq!(
            engine.execute_one("SELECT a, note FROM t ORDER BY id").expect("rows").rows,
            vec![vec![text("x"), text("q")], vec![text("y"), Cell::Null], vec![text("z"), text("m")]]
        );
        let primary = engine.execute_one("ALTER TABLE t DROP COLUMN id").expect_err("primary key");
        assert_eq!(primary.code, "invalid-alter");
        let missing = engine.execute_one("ALTER TABLE t DROP COLUMN nope").expect_err("missing column");
        assert_eq!(missing.code, "unknown-column");
        engine.execute("CREATE TABLE u (a TEXT UNIQUE, b TEXT)").expect("unique");
        assert_eq!(engine.execute_one("ALTER TABLE u DROP COLUMN a").expect_err("unique").code, "invalid-alter");
        engine.execute("CREATE TABLE v (a TEXT)").expect("single column");
        assert_eq!(engine.execute_one("ALTER TABLE v DROP COLUMN a").expect_err("last column").code, "invalid-alter");
        engine.execute("CREATE TABLE w (a TEXT, b TEXT); CREATE INDEX wx ON w (a)").expect("indexed");
        assert_eq!(engine.execute_one("ALTER TABLE w DROP COLUMN a").expect_err("index").code, "invalid-alter");
        engine.execute("CREATE TABLE x (a TEXT, b TEXT, CHECK (a <> ''))").expect("table check");
        assert_eq!(engine.execute_one("ALTER TABLE x DROP COLUMN a").expect_err("table check").code, "invalid-alter");
        engine.execute("CREATE TABLE y (a TEXT, b TEXT, CHECK (b <> ''))").expect("unrelated check");
        engine.execute_one("ALTER TABLE y DROP COLUMN a").expect("unrelated check survives");
        assert_eq!(
            engine.execute_one("SELECT sql FROM sqlite_master WHERE name = 'y'").expect("sql").rows,
            vec![vec![text("CREATE TABLE y (b TEXT, CHECK (b <> ''))")]]
        );
        engine.execute("CREATE TABLE z (a TEXT CHECK (a <> ''), b TEXT)").expect("column check");
        engine.execute_one("ALTER TABLE z DROP COLUMN a").expect("column check belongs to the column");
    }

    #[test]
    fn temp_tables_shadow_main_and_vanish_with_the_engine() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TEMP TABLE t (a INTEGER PRIMARY KEY, b TEXT)").expect("create temp");
        assert_eq!(
            engine.execute_one("SELECT type, name, tbl_name, rootpage, sql FROM sqlite_temp_master").expect("temp master").rows,
            vec![vec![text("table"), text("t"), text("t"), Cell::Integer(2), text("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)"),]]
        );
        assert!(engine.execute_one("SELECT name FROM sqlite_master").expect("master").rows.is_empty());
        // Both schemas may own the name, and the temp table answers first.
        engine.execute("CREATE TABLE t (a INTEGER)").expect("main create");
        engine.execute("INSERT INTO t VALUES (9, 'temp')").expect("temp insert");
        assert_eq!(engine.execute_one("SELECT b FROM t").expect("select").rows, vec![vec![text("temp")]]);
        engine.execute_one("UPDATE t SET b = 'changed'").expect("temp update");
        assert_eq!(engine.execute_one("SELECT b FROM t").expect("select").rows, vec![vec![text("changed")]]);
        engine.execute_one("DELETE FROM t").expect("temp delete");
        assert!(engine.execute_one("SELECT a FROM t").expect("select").rows.is_empty());
        // `DROP TABLE` removes the temp object first and leaves main alone.
        engine.execute_one("DROP TABLE t").expect("drop temp");
        assert!(engine.execute_one("SELECT name FROM sqlite_temp_master").expect("temp master").rows.is_empty());
        assert_eq!(engine.execute_one("SELECT name FROM sqlite_master").expect("master").rows, vec![vec![text("t")]]);
        engine.execute("INSERT INTO t VALUES (1)").expect("main insert");
        // The temp schema belongs to the connection: a fresh engine is empty.
        let mut fresh = Engine::new(None).expect("fresh engine");
        assert!(fresh.execute_one("SELECT name FROM sqlite_temp_master").expect("temp master").rows.is_empty());
    }

    #[test]
    fn temp_pages_are_reused_after_a_drop() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TEMP TABLE a (x INTEGER); CREATE TEMP TABLE b (y INTEGER)").expect("create");
        engine.execute_one("DROP TABLE a").expect("drop");
        engine.execute("CREATE TEMP TABLE c (z INTEGER)").expect("create again");
        assert_eq!(
            engine.execute_one("SELECT name, rootpage FROM sqlite_temp_master").expect("temp master").rows,
            vec![vec![text("b"), Cell::Integer(3)], vec![text("c"), Cell::Integer(2)]]
        );
    }

    #[test]
    fn attached_databases_resolve_qualified_names_and_list_themselves() {
        let mut engine = Engine::new(None).expect("blank engine");
        let attached = engine.execute_one("ATTACH DATABASE ':memory:' AS aux").expect("attach");
        assert_eq!(attached.statement, "attach");
        assert_eq!(attached.affected, 0);
        // The temp slot is reserved but only listed once the temp schema exists.
        assert_eq!(
            engine.execute_one("PRAGMA database_list").expect("database list").rows,
            vec![vec![Cell::Integer(0), text("main"), text("")], vec![Cell::Integer(2), text("aux"), text("")],]
        );
        engine.execute("CREATE TEMP TABLE tt (v INTEGER)").expect("temp");
        engine.execute("CREATE TABLE local (v INTEGER)").expect("main");
        engine.execute("INSERT INTO local VALUES (1)").expect("main insert");
        engine.execute("CREATE TABLE aux.remote (v INTEGER)").expect("aux create");
        engine.execute("INSERT INTO aux.remote VALUES (2)").expect("aux insert");
        assert_eq!(engine.execute_one("SELECT * FROM aux.remote").expect("qualified").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(
            engine.execute_one("SELECT type, name FROM aux.sqlite_master").expect("aux master").rows,
            vec![vec![text("table"), text("remote")]]
        );
        // A schema-qualified source joins a main table in the same statement.
        assert_eq!(
            engine.execute_one("SELECT r.v, l.v FROM aux.remote AS r JOIN local AS l ON r.rowid = l.rowid").expect("cross-database join").rows,
            vec![vec![Cell::Integer(2), Cell::Integer(1)]]
        );
        // An unqualified name falls through temp and main to the attached
        // database, and inserts there as well.
        assert_eq!(engine.execute_one("SELECT * FROM remote").expect("fallback select").rows, vec![vec![Cell::Integer(2)]]);
        engine.execute("INSERT INTO remote VALUES (3)").expect("fallback insert");
        assert_eq!(engine.execute_one("SELECT count(*) FROM aux.remote").expect("count").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(
            engine.execute_one("PRAGMA database_list").expect("database list").rows,
            vec![
                vec![Cell::Integer(0), text("main"), text("")],
                vec![Cell::Integer(1), text("temp"), text("")],
                vec![Cell::Integer(2), text("aux"), text("")],
            ]
        );
    }

    #[test]
    fn main_and_temp_qualifiers_pin_the_schema() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TEMP TABLE t (v TEXT); CREATE TABLE t (v TEXT)").expect("both schemas");
        engine.execute("INSERT INTO temp.t VALUES ('temp'); INSERT INTO main.t VALUES ('main')").expect("inserts");
        assert_eq!(engine.execute_one("SELECT v FROM t").expect("default").rows, vec![vec![text("temp")]]);
        assert_eq!(engine.execute_one("SELECT v FROM main.t").expect("main").rows, vec![vec![text("main")]]);
        assert_eq!(engine.execute_one("SELECT v FROM temp.t").expect("temp").rows, vec![vec![text("temp")]]);
        engine.execute_one("UPDATE t SET v = 'changed'").expect("update temp");
        assert_eq!(engine.execute_one("SELECT v FROM main.t").expect("main untouched").rows, vec![vec![text("main")]]);
        engine.execute_one("DELETE FROM main.t").expect("delete main");
        assert!(engine.execute_one("SELECT v FROM main.t").expect("main empty").rows.is_empty());
        engine.execute_one("DROP TABLE temp.t").expect("drop temp");
        assert!(engine.execute_one("SELECT v FROM t").expect("main fallback").rows.is_empty());
        assert!(engine.execute_one("SELECT name FROM temp.sqlite_temp_master").expect("temp master").rows.is_empty());
        assert_eq!(engine.execute_one("SELECT * FROM main.missing").expect_err("missing").code, "unknown-table");
        assert_eq!(engine.execute_one("SELECT * FROM temp.missing").expect_err("missing temp").code, "unknown-table");
    }

    #[test]
    fn attached_files_are_created_and_keep_their_rows_across_reattach() {
        let dir = std::env::temp_dir().join(format!("sqlite-attach-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let file = dir.join("attached.sqlite3");
        let file_text = file.to_string_lossy().replace('\\', "/");
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one(&format!("ATTACH DATABASE '{file_text}' AS aux")).expect("attach");
        assert!(file.exists(), "attach creates the file");
        assert_eq!(std::fs::metadata(&file).expect("metadata").len(), 0, "sqlite leaves it empty until a write");
        engine.execute("CREATE TABLE aux.t (v INTEGER); INSERT INTO aux.t VALUES (7)").expect("write");
        assert!(std::fs::metadata(&file).expect("metadata").len() > 0, "a write materializes the image");
        engine.execute_one("DETACH DATABASE aux").expect("detach");
        engine.execute_one(&format!("ATTACH DATABASE '{file_text}' AS again")).expect("reattach");
        assert_eq!(engine.execute_one("SELECT * FROM again.t").expect("read").rows, vec![vec![Cell::Integer(7)]]);
        engine.execute_one("DETACH DATABASE again").expect("detach again");
        assert_eq!(engine.execute_one("DETACH DATABASE again").expect_err("gone").code, "unknown-database");
        assert_eq!(engine.execute_one("DETACH DATABASE main").expect_err("main").code, "cannot-detach");
        let _ = std::fs::remove_file(&file);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn attaching_more_than_ten_databases_is_rejected() {
        let mut engine = Engine::new(None).expect("blank engine");
        for index in 0..10 {
            engine.execute_one(&format!("ATTACH ':memory:' AS a{index}")).expect("attach");
        }
        let error = engine.execute_one("ATTACH ':memory:' AS extra").expect_err("limit");
        assert_eq!(error.message, "too many attached databases - max 10");
        assert_eq!(engine.execute_one("ATTACH ':memory:' AS a0").expect_err("duplicate").code, "database-in-use");
        engine.execute_one("DETACH DATABASE a0").expect("detach");
        engine.execute_one("ATTACH ':memory:' AS a10").expect("room again");
    }

    #[test]
    fn order_by_prefers_a_projection_alias() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(a INTEGER, b INTEGER)").expect("create");
        engine.execute_one("INSERT INTO t VALUES (2, 9), (1, 8), (3, 7)").expect("insert");
        // A bare ORDER BY name resolves to the output label first, exactly like
        // SQLite, so `ORDER BY a` sorts the projected `b` values.
        assert_eq!(
            engine.execute_one("SELECT b AS a FROM t ORDER BY a").expect("alias order").rows,
            vec![vec![Cell::Integer(7)], vec![Cell::Integer(8)], vec![Cell::Integer(9)]]
        );
        assert_eq!(
            engine.execute_one("SELECT a AS z FROM t ORDER BY z DESC").expect("alias descending").rows,
            vec![vec![Cell::Integer(3)], vec![Cell::Integer(2)], vec![Cell::Integer(1)]]
        );
        // A qualified reference still reads the base table column.
        assert_eq!(
            engine.execute_one("SELECT b AS a FROM t ORDER BY t.a").expect("qualified order").rows,
            vec![vec![Cell::Integer(8)], vec![Cell::Integer(9)], vec![Cell::Integer(7)]]
        );
    }

    #[test]
    fn pragma_foreign_keys_reports_and_switches_session_state() {
        let mut engine = Engine::new(None).expect("blank engine");
        // better-sqlite3 opens every connection with foreign keys enabled.
        let queried = engine.execute_one("PRAGMA foreign_keys").expect("query");
        assert_eq!(queried.statement, "pragma");
        assert_eq!(queried.columns, vec!["foreign_keys".to_string()]);
        assert_eq!(queried.rows, vec![vec![Cell::Integer(1)]]);
        let disabled = engine.execute_one("PRAGMA foreign_keys = OFF").expect("disable");
        assert!(disabled.columns.is_empty());
        assert!(disabled.rows.is_empty());
        let queried = engine.execute_one("PRAGMA foreign_keys").expect("query");
        assert_eq!(queried.rows, vec![vec![Cell::Integer(0)]]);
        engine.execute_one("PRAGMA foreign_keys(ON)").expect("parenthesized form");
        let queried = engine.execute_one("PRAGMA foreign_keys").expect("query");
        assert_eq!(queried.rows, vec![vec![Cell::Integer(1)]]);
        engine.execute_one("PRAGMA foreign_keys = 0").expect("set zero");
        let queried = engine.execute_one("PRAGMA foreign_keys").expect("query");
        assert_eq!(queried.rows, vec![vec![Cell::Integer(0)]]);
    }

    #[test]
    fn foreign_keys_reject_orphans_and_removed_references() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)").expect("create parent");
        engine.execute_one("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))").expect("create child");
        engine.execute_one("INSERT INTO parent VALUES (1, 'a'), (2, 'b')").expect("parent rows");
        engine.execute_one("INSERT INTO child VALUES (10, 1)").expect("child row");
        let orphan = engine.execute_one("INSERT INTO child VALUES (11, 99)").expect_err("orphan");
        assert_eq!(orphan.code, "SQLITE_CONSTRAINT_FOREIGNKEY");
        assert_eq!(orphan.message, "FOREIGN KEY constraint failed");
        engine.execute_one("INSERT INTO child VALUES (12, NULL)").expect("null key passes");
        let removed = engine.execute_one("DELETE FROM parent WHERE id = 1").expect_err("referenced delete");
        assert_eq!(removed.code, "SQLITE_CONSTRAINT_FOREIGNKEY");
        let rewritten = engine.execute_one("UPDATE parent SET id = 7 WHERE id = 1").expect_err("referenced update");
        assert_eq!(rewritten.code, "SQLITE_CONSTRAINT_FOREIGNKEY");
        // Assigning an unrelated parent column does not rewrite the key.
        engine.execute_one("UPDATE parent SET name = 'c' WHERE id = 1").expect("non-key update");
        let child_update = engine.execute_one("UPDATE child SET pid = 88 WHERE id = 10").expect_err("child update");
        assert_eq!(child_update.code, "SQLITE_CONSTRAINT_FOREIGNKEY");
        engine.execute_one("UPDATE child SET pid = NULL WHERE id = 10").expect("child update to null");
        // The failed statements left no trace, like SQLite's implicit rollback.
        assert_eq!(engine.execute_one("SELECT count(*) FROM child").expect("count").rows, vec![vec![Cell::Integer(2)]]);
        // Disabling the pragma lets every previously rejected statement pass.
        engine.execute_one("PRAGMA foreign_keys = OFF").expect("disable");
        engine.execute_one("INSERT INTO child VALUES (11, 99)").expect("orphan allowed");
        engine.execute_one("DELETE FROM parent WHERE id = 1").expect("referenced delete allowed");
        engine.execute_one("UPDATE parent SET id = 7 WHERE id = 2").expect("referenced update allowed");
    }

    #[test]
    fn foreign_keys_report_missing_parents_and_mismatched_keys() {
        let mut engine = Engine::new(None).expect("blank engine");
        // A forward reference is legal at DDL time; the DML fails until the
        // parent exists.
        engine.execute_one("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))").expect("forward reference");
        let missing = engine.execute_one("INSERT INTO child VALUES (1, NULL)").expect_err("missing parent");
        assert_eq!(missing.code, "SQLITE_ERROR");
        assert_eq!(missing.message, "no such table: main.parent");
        let invalid = engine
            .execute_one("CREATE TABLE bad (id INTEGER PRIMARY KEY, FOREIGN KEY (ghost) REFERENCES parent(id))")
            .expect_err("bad child column");
        assert_eq!(invalid.code, "SQLITE_ERROR");
        assert_eq!(invalid.message, "unknown column \"ghost\" in foreign key definition");
        engine.execute_one("CREATE TABLE parent (id INTEGER)").expect("parent without a key");
        let mismatch = engine.execute_one("INSERT INTO child VALUES (2, NULL)").expect_err("mismatch");
        assert_eq!(mismatch.code, "SQLITE_ERROR");
        assert_eq!(mismatch.message, "foreign key mismatch - \"child\" referencing \"parent\"");
    }

    #[test]
    fn composite_foreign_keys_use_the_parent_primary_key() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE parent (a INTEGER, b INTEGER, PRIMARY KEY (a, b))").expect("composite parent");
        engine
            .execute_one("CREATE TABLE child (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, FOREIGN KEY (x, y) REFERENCES parent)")
            .expect("composite child");
        engine.execute_one("INSERT INTO parent VALUES (1, 2)").expect("parent row");
        engine.execute_one("INSERT INTO child VALUES (1, 1, 2)").expect("matching composite");
        let orphan = engine.execute_one("INSERT INTO child VALUES (2, 1, 3)").expect_err("composite orphan");
        assert_eq!(orphan.code, "SQLITE_CONSTRAINT_FOREIGNKEY");
        engine.execute_one("INSERT INTO child VALUES (3, 1, NULL)").expect("partial null passes");
        let partial = engine
            .execute_one("CREATE TABLE single (id INTEGER PRIMARY KEY, x INTEGER, FOREIGN KEY (x) REFERENCES parent(a))")
            .expect("create single");
        assert_eq!(partial.statement, "create-table");
        let mismatch = engine.execute_one("INSERT INTO single VALUES (1, 1)").expect_err("single column against a composite key");
        assert_eq!(mismatch.code, "SQLITE_ERROR");
        assert_eq!(mismatch.message, "foreign key mismatch - \"single\" referencing \"parent\"");
    }

    #[test]
    fn self_references_may_point_at_later_rows_of_the_same_statement() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES t(id))").expect("create");
        engine.execute_one("INSERT INTO t VALUES (1, NULL), (2, 1)").expect("parent first");
        engine.execute_one("INSERT INTO t VALUES (3, 4), (4, NULL)").expect("child first");
        let orphan = engine.execute_one("INSERT INTO t VALUES (5, 9)").expect_err("self orphan");
        assert_eq!(orphan.code, "SQLITE_CONSTRAINT_FOREIGNKEY");
    }

    #[test]
    fn referential_actions_run_and_deferred_keys_wait_for_commit() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE p (id INTEGER PRIMARY KEY)").expect("create parent");
        engine
            .execute_one("CREATE TABLE c1 (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)")
            .expect("cascade");
        engine
            .execute_one("CREATE TABLE c2 (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE SET NULL ON UPDATE SET NULL)")
            .expect("set null");
        engine
            .execute_one(
                "CREATE TABLE c3 (id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 9 REFERENCES p(id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)",
            )
            .expect("set default");
        engine.execute_one("INSERT INTO p VALUES (1), (9)").expect("parents");
        engine.execute_one("INSERT INTO c1 VALUES (2, 1)").expect("cascade child");
        engine.execute_one("INSERT INTO c2 VALUES (3, 1)").expect("null child");
        engine.execute_one("INSERT INTO c3 VALUES (4, 1)").expect("default child");
        engine.execute_one("UPDATE p SET id = 5 WHERE id = 1").expect("cascade update");
        assert_eq!(engine.execute_one("SELECT id, pid FROM c1").expect("cascade rows").rows, vec![vec![Cell::Integer(2), Cell::Integer(5)]]);
        assert_eq!(engine.execute_one("SELECT id, pid FROM c2").expect("null rows").rows, vec![vec![Cell::Integer(3), Cell::Null]]);
        assert_eq!(engine.execute_one("SELECT id, pid FROM c3").expect("default rows").rows, vec![vec![Cell::Integer(4), Cell::Integer(9)]]);
        engine.execute_one("DELETE FROM p WHERE id = 5").expect("cascade delete");
        assert_eq!(engine.execute_one("SELECT id FROM c1").expect("cascade delete rows").rows, Vec::<Vec<Cell>>::new());
        assert_eq!(
            engine.execute_one("SELECT id, pid FROM c2").expect("null rows after delete").rows,
            vec![vec![Cell::Integer(3), Cell::Null]]
        );
        assert_eq!(
            engine.execute_one("SELECT id, pid FROM c3").expect("default rows after delete").rows,
            vec![vec![Cell::Integer(4), Cell::Integer(9)]]
        );
        let deferred = engine
            .execute_one("CREATE TABLE c4 (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)")
            .expect("deferred accepted");
        assert_eq!(deferred.statement, "create-table");
        // Autocommit: the statement boundary is also the commit boundary.
        let orphan = engine.execute_one("INSERT INTO c4 VALUES (5, 77)").expect_err("orphan");
        assert_eq!(orphan.message, "FOREIGN KEY constraint failed");
        // In a transaction the reference may be repaired before COMMIT.
        engine.execute_one("BEGIN").expect("begin");
        engine.execute_one("INSERT INTO c4 VALUES (5, 77)").expect("deferred insert");
        engine.execute_one("INSERT INTO p VALUES (77)").expect("repair parent");
        engine.execute_one("COMMIT").expect("commit repaired");
        assert_eq!(engine.execute_one("SELECT pid FROM c4").expect("deferred row").rows, vec![vec![Cell::Integer(77)]]);
        // An unresolvable reference fails COMMIT and keeps the transaction open.
        engine.execute_one("BEGIN").expect("begin again");
        engine.execute_one("INSERT INTO c4 VALUES (6, 88)").expect("deferred insert again");
        let violation = engine.execute_one("COMMIT").expect_err("commit fails");
        assert_eq!(violation.message, "FOREIGN KEY constraint failed");
        assert!(engine.in_transaction(), "a deferred failure must leave the transaction open");
        engine.execute_one("INSERT INTO p VALUES (88)").expect("repair after failure");
        engine.execute_one("COMMIT").expect("second commit");
    }

    #[test]
    fn recursive_ctes_iterate_until_the_working_set_is_empty() {
        let mut engine = Engine::new(None).expect("blank engine");
        let walked = engine
            .execute_one("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 5) SELECT x FROM c")
            .expect("recursive walk");
        assert_eq!(
            walked.rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)], vec![Cell::Integer(3)], vec![Cell::Integer(4)], vec![Cell::Integer(5)]]
        );
        // The keyword is not required when the body reads itself.
        let bare = engine
            .execute_one("WITH c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3) SELECT x FROM c")
            .expect("recursive without the keyword");
        assert_eq!(bare.rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)], vec![Cell::Integer(3)]]);
        // `UNION` removes duplicates and therefore terminates a self-cycle.
        let cycle = engine.execute_one("WITH RECURSIVE c(x) AS (SELECT 1 UNION SELECT x FROM c) SELECT x FROM c").expect("distinct cycle");
        assert_eq!(cycle.rows, vec![vec![Cell::Integer(1)]]);
        let factorial = engine
            .execute_one("WITH RECURSIVE c(n, f) AS (SELECT 1, 1 UNION ALL SELECT n+1, f*(n+1) FROM c WHERE n < 5) SELECT n, f FROM c")
            .expect("factorial");
        assert_eq!(
            factorial.rows,
            vec![
                vec![Cell::Integer(1), Cell::Integer(1)],
                vec![Cell::Integer(2), Cell::Integer(2)],
                vec![Cell::Integer(3), Cell::Integer(6)],
                vec![Cell::Integer(4), Cell::Integer(24)],
                vec![Cell::Integer(5), Cell::Integer(120)],
            ]
        );
    }

    #[test]
    fn recursive_ctes_keep_duplicates_for_union_all() {
        let mut engine = Engine::new(None).expect("blank engine");
        let duplicated =
            engine.execute_one("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x FROM c LIMIT 3) SELECT x FROM c").expect("duplicates");
        assert_eq!(duplicated.rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(1)], vec![Cell::Integer(1)]]);
        let seeded = engine
            .execute_one(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3) SELECT x FROM c ORDER BY x",
            )
            .expect("seed duplicates");
        assert_eq!(
            seeded.rows,
            vec![
                vec![Cell::Integer(1)],
                vec![Cell::Integer(1)],
                vec![Cell::Integer(2)],
                vec![Cell::Integer(2)],
                vec![Cell::Integer(3)],
                vec![Cell::Integer(3)]
            ]
        );
    }

    #[test]
    fn infinite_recursion_is_cut_off_by_the_enclosing_limit() {
        let mut engine = Engine::new(None).expect("blank engine");
        let limited = engine
            .execute_one("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT x FROM c LIMIT 5")
            .expect("bounded recursion");
        assert_eq!(
            limited.rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)], vec![Cell::Integer(3)], vec![Cell::Integer(4)], vec![Cell::Integer(5)]]
        );
        let offset = engine
            .execute_one("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT x FROM c LIMIT 2 OFFSET 3")
            .expect("bounded recursion with offset");
        assert_eq!(offset.rows, vec![vec![Cell::Integer(4)], vec![Cell::Integer(5)]]);
    }

    #[test]
    fn recursive_ctes_can_read_live_tables_and_earlier_entries() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE edges (parent INTEGER, child INTEGER)").expect("create edges");
        engine.execute_one("INSERT INTO edges VALUES (1,2), (1,3), (2,4), (3,5), (5,6)").expect("edge rows");
        let tree = engine
            .execute_one("WITH RECURSIVE d(id, depth) AS (SELECT 1, 0 UNION ALL SELECT e.child, d.depth+1 FROM edges e, d WHERE e.parent = d.id) SELECT id, depth FROM d ORDER BY id")
            .expect("tree walk");
        assert_eq!(
            tree.rows,
            vec![
                vec![Cell::Integer(1), Cell::Integer(0)],
                vec![Cell::Integer(2), Cell::Integer(1)],
                vec![Cell::Integer(3), Cell::Integer(1)],
                vec![Cell::Integer(4), Cell::Integer(2)],
                vec![Cell::Integer(5), Cell::Integer(2)],
                vec![Cell::Integer(6), Cell::Integer(3)],
            ]
        );
        let chained = engine
            .execute_one("WITH a(k) AS (SELECT 2), b(x) AS (SELECT 1 UNION ALL SELECT x+k FROM b, a WHERE x < 9) SELECT x FROM b ORDER BY x")
            .expect("earlier entry");
        assert_eq!(
            chained.rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(3)], vec![Cell::Integer(5)], vec![Cell::Integer(7)], vec![Cell::Integer(9)]]
        );
    }

    #[test]
    fn malformed_recursive_ctes_report_a_circular_reference() {
        let mut engine = Engine::new(None).expect("blank engine");
        let mixed = engine
            .execute_one("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3 UNION ALL SELECT 9) SELECT x FROM c")
            .expect_err("non-recursive term after a recursive one");
        assert_eq!(mixed.code, "SQLITE_ERROR");
        assert_eq!(mixed.message, "circular reference: c");
        let self_seed = engine.execute_one("WITH c(x) AS (SELECT x FROM c) SELECT x FROM c").expect_err("seed reads itself");
        assert_eq!(self_seed.message, "circular reference: c");
    }

    #[test]
    fn transactions_restore_rows_and_schema() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(a INTEGER); BEGIN; INSERT INTO t VALUES (1); CREATE TABLE gone(b INTEGER); ROLLBACK").expect("script");
        assert_eq!(engine.execute_one("SELECT count(*) FROM t").expect("read").rows, vec![vec![Cell::Integer(0)]]);
        assert!(engine.execute_one("SELECT b FROM gone").is_err());

        engine
            .execute("BEGIN; INSERT INTO t VALUES (2); SAVEPOINT s; INSERT INTO t VALUES (3); ROLLBACK TO s; RELEASE s; COMMIT")
            .expect("script");
        assert_eq!(engine.execute_one("SELECT a FROM t").expect("read").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(engine.execute_one("COMMIT").expect_err("no transaction").message, "cannot commit - no transaction is active");
    }

    #[test]
    fn distinct_collapses_before_limit() {
        let mut engine = engine();
        assert_eq!(
            engine.execute_one("SELECT DISTINCT b FROM t ORDER BY b LIMIT 2").expect("select").rows,
            vec![vec![Cell::Null], vec![Cell::Integer(0)]]
        );
    }

    #[test]
    fn strict_tables_reject_values_outside_the_declared_type() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE s(a INTEGER, b TEXT, c BLOB) STRICT").expect("strict table");
        engine.execute("INSERT INTO s VALUES ('12', 5, X'00')").expect("affinity keeps strict rows valid");
        assert_eq!(engine.execute_one("SELECT a, b FROM s").expect("read").rows, vec![vec![Cell::Integer(12), Cell::Text("5".to_string())]]);
        let real = engine.execute_one("INSERT INTO s(a) VALUES (1.5)").expect_err("real into integer");
        assert_eq!(real.code, "constraint-datatype");
        assert_eq!(real.message, "cannot store REAL value in INTEGER column s.a");
        let text = engine.execute_one("INSERT INTO s(c) VALUES ('z')").expect_err("text into blob");
        assert_eq!(text.message, "cannot store TEXT value in BLOB column s.c");
        let unknown = engine.execute_one("CREATE TABLE bad(a varchar(10)) STRICT").expect_err("bad datatype");
        assert_eq!(unknown.message, "unknown datatype for bad.a: \"varchar(10)\"");
    }

    #[test]
    fn views_rerun_their_query_and_share_the_object_namespace() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE base(a INTEGER, b TEXT)").expect("base");
        engine.execute("INSERT INTO base VALUES (1, 'x'), (2, 'y')").expect("rows");
        engine.execute("CREATE VIEW v(c) AS SELECT a FROM base WHERE a > 1").expect("view");
        assert_eq!(engine.execute_one("SELECT c FROM v ORDER BY c").expect("read view").rows, vec![vec![Cell::Integer(2)]]);
        engine.execute("INSERT INTO base VALUES (3, 'z')").expect("later row");
        assert_eq!(engine.execute_one("SELECT count(*) FROM v").expect("count view").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(
            engine.execute_one("SELECT type, name, tbl_name, rootpage FROM sqlite_master WHERE name = 'v'").expect("master").rows,
            vec![vec![Cell::Text("view".to_string()), Cell::Text("v".to_string()), Cell::Text("v".to_string()), Cell::Integer(0)]]
        );
        let modify = engine.execute_one("INSERT INTO v VALUES (4)").expect_err("view is read only");
        assert_eq!(modify.code, "readonly-view");
        engine.execute("DROP VIEW v").expect("drop view");
        let missing = engine.execute_one("SELECT * FROM v").expect_err("dropped");
        assert_eq!(missing.code, "unknown-table");
    }

    #[test]
    fn triggers_fire_once_and_keep_catalog_rows() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE scratch(id INTEGER, label TEXT)").expect("scratch");
        engine.execute("CREATE TABLE audit(id INTEGER)").expect("audit");
        engine.execute("CREATE TRIGGER probe AFTER INSERT ON scratch BEGIN INSERT INTO audit VALUES (1); END").expect("trigger");
        engine
            .execute("CREATE TRIGGER probe_self AFTER INSERT ON scratch BEGIN INSERT INTO scratch VALUES (9, 'self'); END")
            .expect("self trigger");
        engine.execute("INSERT INTO scratch VALUES (1, 'x')").expect("insert");
        // One user insert plus one body insert. The body insert fires the
        // other trigger but not the one whose body is running, because
        // recursive triggers are off.
        assert_eq!(engine.execute_one("SELECT count(*) FROM scratch").expect("count scratch").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(engine.execute_one("SELECT count(*) FROM audit").expect("count audit").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(
            engine.execute_one("SELECT type, name, tbl_name FROM sqlite_master WHERE name = 'probe'").expect("master").rows,
            vec![vec![Cell::Text("trigger".to_string()), Cell::Text("probe".to_string()), Cell::Text("scratch".to_string())]]
        );
        engine.execute("DROP TRIGGER probe").expect("drop trigger");
        engine.execute("INSERT INTO scratch VALUES (2, 'y')").expect("insert after drop");
        assert_eq!(engine.execute_one("SELECT count(*) FROM audit").expect("count after drop").rows, vec![vec![Cell::Integer(2)]]);
    }

    #[test]
    fn trigger_timings_see_new_and_old_rows() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT)").expect("t");
        engine.execute("CREATE TABLE log(v)").expect("log");
        engine.execute("CREATE TRIGGER bi BEFORE INSERT ON t BEGIN INSERT INTO log VALUES (new.a); END").expect("before insert");
        engine.execute("CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.rowid); END").expect("after insert");
        engine.execute("INSERT INTO t VALUES (5, 'seven', 'x')").expect("insert");
        assert_eq!(
            engine.execute_one("SELECT v FROM log").expect("log rows").rows,
            vec![vec![Cell::Text("seven".to_string())], vec![Cell::Integer(5)]]
        );
        engine
            .execute("CREATE TRIGGER au AFTER UPDATE ON t BEGIN INSERT INTO log VALUES (old.a); INSERT INTO log VALUES (new.a); END")
            .expect("after update");
        engine.execute("UPDATE t SET a = 'nine' WHERE id = 5").expect("update");
        assert_eq!(
            engine.execute_one("SELECT v FROM log WHERE rowid > 2").expect("update log").rows,
            vec![vec![Cell::Text("seven".to_string())], vec![Cell::Text("nine".to_string())]]
        );
        engine.execute("CREATE TRIGGER bd BEFORE DELETE ON t BEGIN INSERT INTO log VALUES (old.id); END").expect("before delete");
        engine.execute("DELETE FROM t WHERE id = 5").expect("delete");
        assert_eq!(engine.execute_one("SELECT v FROM log WHERE rowid = 5").expect("delete log").rows, vec![vec![Cell::Integer(5)]]);
    }

    #[test]
    fn trigger_when_clause_filters_rows() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)").expect("t");
        engine.execute("CREATE TABLE log(v)").expect("log");
        engine.execute("CREATE TRIGGER w AFTER INSERT ON t WHEN new.a > 5 BEGIN INSERT INTO log VALUES (new.a); END").expect("when trigger");
        engine.execute("INSERT INTO t VALUES (1, 10), (2, 3)").expect("insert");
        assert_eq!(engine.execute_one("SELECT v FROM log").expect("log").rows, vec![vec![Cell::Integer(10)]]);
        engine.execute("CREATE TRIGGER wu AFTER UPDATE ON t WHEN old.a = 10 BEGIN INSERT INTO log VALUES (new.a); END").expect("when update");
        engine.execute("UPDATE t SET a = 11 WHERE id = 1").expect("update");
        assert_eq!(
            engine.execute_one("SELECT v FROM log ORDER BY rowid").expect("log rows").rows,
            vec![vec![Cell::Integer(10)], vec![Cell::Integer(11)]]
        );
    }

    #[test]
    fn trigger_creation_reports_sqlite_diagnostics() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a)").expect("t");
        engine.execute("CREATE VIEW vv AS SELECT id, a FROM t").expect("view");
        engine.execute("CREATE TRIGGER d AFTER INSERT ON t BEGIN SELECT 1; END").expect("first trigger");
        let duplicate = engine.execute_one("CREATE TRIGGER d AFTER INSERT ON t BEGIN SELECT 1; END").expect_err("duplicate");
        assert_eq!(duplicate.code, "trigger-exists");
        assert_eq!(duplicate.message, "trigger d already exists");
        // The trigger namespace is separate from tables: a trigger may carry
        // the name of a table.
        engine.execute("CREATE TRIGGER t AFTER INSERT ON t BEGIN SELECT 1; END").expect("trigger named t");
        engine.execute("DROP TRIGGER t").expect("drop trigger named t");
        let missing = engine.execute_one("CREATE TRIGGER m BEFORE INSERT ON missing BEGIN SELECT 1; END").expect_err("missing");
        assert_eq!(missing.message, "no such table: main.missing");
        let after_view = engine.execute_one("CREATE TRIGGER tv AFTER INSERT ON vv BEGIN SELECT 1; END").expect_err("after view");
        assert_eq!(after_view.message, "cannot create AFTER trigger on view: vv");
        let before_view = engine.execute_one("CREATE TRIGGER tv2 BEFORE INSERT ON vv BEGIN SELECT 1; END").expect_err("before view");
        assert_eq!(before_view.message, "cannot create BEFORE trigger on view: vv");
        let instead_table = engine.execute_one("CREATE TRIGGER tt INSTEAD OF INSERT ON t BEGIN SELECT 1; END").expect_err("instead");
        assert_eq!(instead_table.message, "cannot create INSTEAD OF trigger on table: t");
        let no_timing = engine.execute_one("CREATE TRIGGER nt ON t BEGIN SELECT 1; END").expect_err("no timing");
        assert_eq!(no_timing.code, "syntax");
        assert_eq!(no_timing.message, "near \"ON\": syntax error");
        // `DROP TRIGGER` on an object that is not a trigger keeps SQLite's
        // message instead of pointing at the other object family.
        engine.execute("CREATE TABLE u(id INTEGER)").expect("u");
        let wrong = engine.execute_one("DROP TRIGGER u").expect_err("drop table name");
        assert_eq!(wrong.message, "no such trigger: u");
        let absent = engine.execute_one("DROP TRIGGER nope").expect_err("drop missing");
        assert_eq!(absent.message, "no such trigger: nope");
        engine.execute("DROP TRIGGER IF EXISTS nope").expect("drop if exists");
    }

    #[test]
    fn same_event_triggers_fire_newest_first() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a)").expect("t");
        engine.execute("CREATE TABLE log(v)").expect("log");
        for value in 1..=3 {
            engine.execute(&format!("CREATE TRIGGER o{value} AFTER INSERT ON t BEGIN INSERT INTO log VALUES ({value}); END")).expect("trigger");
        }
        engine.execute("INSERT INTO t VALUES (1, 1)").expect("insert");
        assert_eq!(
            engine.execute_one("SELECT v FROM log ORDER BY rowid").expect("log").rows,
            vec![vec![Cell::Integer(3)], vec![Cell::Integer(2)], vec![Cell::Integer(1)]]
        );
    }

    #[test]
    fn trigger_bodies_fire_other_tables_but_skip_active_triggers() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, a)").expect("u");
        engine.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, a)").expect("b");
        engine.execute("CREATE TRIGGER ta AFTER INSERT ON u BEGIN INSERT INTO b VALUES (new.id, new.a); END").expect("ta");
        engine.execute("CREATE TRIGGER tb AFTER INSERT ON b BEGIN INSERT INTO u VALUES (new.id + 100, 1); END").expect("tb");
        engine.execute("INSERT INTO u VALUES (1, 1)").expect("insert");
        assert_eq!(
            engine.execute_one("SELECT id FROM u ORDER BY id").expect("u rows").rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(101)]]
        );
        assert_eq!(engine.execute_one("SELECT id FROM b ORDER BY id").expect("b rows").rows, vec![vec![Cell::Integer(1)]]);
    }

    #[test]
    fn recursive_triggers_pragma_allows_loops_until_the_depth_limit() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE s(id INTEGER PRIMARY KEY, a)").expect("s");
        engine.execute("CREATE TRIGGER loop AFTER INSERT ON s BEGIN INSERT INTO s VALUES (new.id + 1000, new.a); END").expect("loop");
        engine.execute("INSERT INTO s VALUES (1, 1)").expect("recursive off");
        // The trigger fires once for the user row; its own insert does not
        // re-enter the trigger while `recursive_triggers` is off.
        assert_eq!(
            engine.execute_one("SELECT id FROM s ORDER BY id").expect("count").rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(1001)]]
        );
        let query = engine.execute_one("PRAGMA recursive_triggers").expect("pragma read");
        assert_eq!(query.rows, vec![vec![Cell::Integer(0)]]);
        engine.execute("PRAGMA recursive_triggers = ON").expect("pragma write");
        assert_eq!(engine.execute_one("PRAGMA recursive_triggers").expect("pragma read").rows, vec![vec![Cell::Integer(1)]]);
        let runaway = engine.execute_one("INSERT INTO s VALUES (2, 2)").expect_err("depth limit");
        assert_eq!(runaway.code, "trigger-depth");
        assert_eq!(runaway.message, "too many levels of trigger recursion");
        // The failed statement leaves nothing behind.
        assert_eq!(
            engine.execute_one("SELECT id FROM s ORDER BY id").expect("count after").rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(1001)]]
        );
    }

    #[test]
    fn instead_of_triggers_run_view_dml_and_report_zero_changes() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE base(id INTEGER PRIMARY KEY, a, b)").expect("base");
        engine.execute("CREATE VIEW v AS SELECT id, a FROM base").expect("view");
        let plain = engine.execute_one("INSERT INTO v VALUES (1, 2)").expect_err("plain view");
        assert_eq!(plain.message, "cannot modify v because it is a view");
        engine
            .execute("CREATE TRIGGER vi INSTEAD OF INSERT ON v BEGIN INSERT INTO base(id, a) VALUES (new.id, new.a); END")
            .expect("instead insert");
        let inserted = engine.execute("INSERT INTO v VALUES (7, 8)").expect("view insert").pop().expect("result");
        assert_eq!(inserted.affected, 0);
        assert_eq!(engine.execute_one("SELECT id, a FROM base").expect("base rows").rows, vec![vec![Cell::Integer(7), Cell::Integer(8)]]);
        engine
            .execute("CREATE TRIGGER vu INSTEAD OF UPDATE ON v BEGIN UPDATE base SET a = new.a WHERE id = old.id; END")
            .expect("instead update");
        let updated = engine.execute("UPDATE v SET a = 20 WHERE id = 7").expect("view update").pop().expect("result");
        assert_eq!(updated.affected, 0);
        assert_eq!(
            engine.execute_one("SELECT id, a FROM base").expect("base after update").rows,
            vec![vec![Cell::Integer(7), Cell::Integer(20)]]
        );
        engine.execute("CREATE TRIGGER vd INSTEAD OF DELETE ON v BEGIN DELETE FROM base WHERE id = old.id; END").expect("instead delete");
        let deleted = engine.execute("DELETE FROM v WHERE id = 7").expect("view delete").pop().expect("result");
        assert_eq!(deleted.affected, 0);
        assert_eq!(engine.execute_one("SELECT count(*) FROM base").expect("base after delete").rows, vec![vec![Cell::Integer(0)]]);
    }

    #[test]
    fn a_failing_trigger_row_rolls_the_whole_statement_back() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a)").expect("t");
        engine.execute("CREATE TABLE log(v)").expect("log");
        engine.execute("CREATE TRIGGER bad AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.zzz); END").expect("trigger");
        let failed = engine.execute_one("INSERT INTO t VALUES (1, 1), (2, 2)").expect_err("missing column");
        assert_eq!(failed.code, "unknown-column");
        assert_eq!(failed.message, "no such column: new.zzz");
        assert_eq!(engine.execute_one("SELECT count(*) FROM t").expect("t rows").rows, vec![vec![Cell::Integer(0)]]);
        // `new` and `old` sides do not exist for the events that lack them.
        engine.execute("CREATE TRIGGER bad_old AFTER INSERT ON t BEGIN INSERT INTO log VALUES (old.a); END").expect("old trigger");
        let wrong_side = engine.execute_one("INSERT INTO t VALUES (3, 3)").expect_err("old on insert");
        assert_eq!(wrong_side.message, "no such column: old.a");
    }

    #[test]
    fn temp_triggers_live_in_temp_master_and_die_with_the_table() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE m(id INTEGER PRIMARY KEY, a)").expect("m");
        engine.execute("CREATE TEMP TABLE tt(id INTEGER PRIMARY KEY, a)").expect("temp table");
        engine.execute("CREATE TRIGGER temp.xt AFTER INSERT ON tt BEGIN INSERT INTO m VALUES (new.id, new.a); END").expect("temp trigger");
        assert_eq!(
            engine.execute_one("SELECT type, name FROM sqlite_temp_master").expect("temp master").rows,
            vec![
                vec![Cell::Text("table".to_string()), Cell::Text("tt".to_string())],
                vec![Cell::Text("trigger".to_string()), Cell::Text("xt".to_string())],
            ]
        );
        assert_eq!(
            engine.execute_one("SELECT count(*) FROM sqlite_master WHERE type = 'trigger'").expect("main master").rows,
            vec![vec![Cell::Integer(0)]]
        );
        engine.execute("INSERT INTO tt VALUES (1, 2)").expect("temp insert");
        assert_eq!(engine.execute_one("SELECT id, a FROM m").expect("m rows").rows, vec![vec![Cell::Integer(1), Cell::Integer(2)]]);
        engine.execute("DROP TABLE tt").expect("drop temp");
        assert_eq!(
            engine.execute_one("SELECT count(*) FROM sqlite_temp_master WHERE type = 'trigger'").expect("temp triggers").rows,
            vec![vec![Cell::Integer(0)]]
        );
    }

    #[test]
    fn update_of_column_list_limits_the_event() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a, b)").expect("t");
        engine.execute("CREATE TABLE log(v)").expect("log");
        engine.execute("CREATE TRIGGER uo AFTER UPDATE OF a ON t BEGIN INSERT INTO log VALUES (new.a); END").expect("update of");
        engine.execute("INSERT INTO t VALUES (1, 'start', 'start')").expect("seed");
        engine.execute("UPDATE t SET b = 'x'").expect("update b");
        assert_eq!(engine.execute_one("SELECT count(*) FROM log").expect("log after b").rows, vec![vec![Cell::Integer(0)]]);
        engine.execute("UPDATE t SET a = 'y'").expect("update a");
        assert_eq!(engine.execute_one("SELECT v FROM log").expect("log after a").rows, vec![vec![Cell::Text("y".to_string())]]);
    }

    /// 宿主适配器:把文本转成大写,其余值原样返回。
    struct UpperAdapter;

    impl HostRuntimeAdapter for UpperAdapter {
        fn invoke_scalar(&self, _handle: &HostFunctionHandle, args: &[UdfValue]) -> UdfResult<UdfValue> {
            Ok(match &args[0] {
                UdfValue::Text(text) => UdfValue::Text(text.to_ascii_uppercase()),
                other => other.clone(),
            })
        }

        fn invoke_batch(&self, _handle: &HostFunctionHandle, _batches: &[Vec<UdfValue>]) -> UdfResult<Vec<UdfValue>> {
            Err(UdfError::UnsupportedEffect)
        }
    }

    fn scalar_definition(name: &str, args: Vec<UdfType>, returns: UdfType) -> HostMicroDefinition {
        HostMicroDefinition::from_scalar(name, 1, args, returns, 7, format!("host/{name}"), 1, [0u8; 32])
            .expect("definition is valid")
    }

    /// 未注册的宿主名保持 `unknown-function`;注册后按折叠名解析并求值,
    /// 元数不符在执行前拒绝。
    #[test]
    fn host_function_registration_dispatches_and_folds_the_name() {
        let mut engine = Engine::new(None).expect("blank engine");
        let missing = engine.execute_one("SELECT shout('x')").expect_err("unregistered host function");
        assert_eq!(missing.code, "unknown-function");

        engine
            .register_host_function(scalar_definition("shout", vec![UdfType::Text], UdfType::Text), Arc::new(UpperAdapter))
            .expect("register host function");
        assert_eq!(engine.host_function_names(), vec!["shout".to_string()]);
        assert_eq!(engine.execute_one("SELECT SHOUT('x')").expect("host call").rows, vec![vec![Cell::Text("X".to_string())]]);
        let wrong = engine.execute_one("SELECT shout('x', 'y')").expect_err("arity mismatch");
        assert_eq!(wrong.code, "wrong-arguments");
    }

    /// 折叠名与内建函数同名同元数时宿主实现优先;`count(*)` 特殊形态仍走内建。
    #[test]
    fn host_function_replaces_a_builtin_of_the_same_arity() {
        struct Constant;
        impl HostRuntimeAdapter for Constant {
            fn invoke_scalar(&self, _handle: &HostFunctionHandle, _args: &[UdfValue]) -> UdfResult<UdfValue> {
                Ok(UdfValue::I64(7))
            }

            fn invoke_batch(&self, _handle: &HostFunctionHandle, _batches: &[Vec<UdfValue>]) -> UdfResult<Vec<UdfValue>> {
                Err(UdfError::UnsupportedEffect)
            }
        }
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(a)").expect("t");
        engine.execute("INSERT INTO t VALUES ('abc'), ('de')").expect("seed");
        engine
            .register_host_function(scalar_definition("length", vec![UdfType::Any], UdfType::I64), Arc::new(Constant))
            .expect("register host function");
        let rows = engine.execute_one("SELECT length(a) FROM t").expect("host call").rows;
        assert_eq!(rows, vec![vec![Cell::Integer(7)], vec![Cell::Integer(7)]]);
        assert_eq!(engine.execute_one("SELECT count(*) FROM t").expect("count stays builtin").rows, vec![vec![Cell::Integer(2)]]);
    }

    /// 一次完整的 `VACUUM`:重建后 freelist 清空、镜像变小,schema 与行不变。
    #[test]
    fn vacuum_rebuilds_the_image_and_clears_the_freelist() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine
            .execute(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
                 CREATE INDEX t_v ON t(v);
                 CREATE TABLE wo(k TEXT PRIMARY KEY, n INTEGER) WITHOUT ROWID;
                 CREATE TABLE ex(a INT, b TEXT);
                 CREATE INDEX ex_lower ON ex(lower(b));
                 CREATE TABLE col(c TEXT COLLATE NOCASE);
                 CREATE TABLE ai(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
                 CREATE TABLE orphan(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
                 CREATE VIEW vv AS SELECT id, v FROM t;
                 CREATE TABLE log(x);
                 CREATE TABLE dropped(z);
                 INSERT INTO dropped VALUES ('z');
                 DROP TABLE orphan",
            )
            .expect("schema");
        for value in 1..=200 {
            engine.execute(&format!("INSERT INTO t VALUES ({value}, 'v{value}')")).expect("row");
            engine.execute(&format!("INSERT INTO ex VALUES ({value}, 'B{value}')")).expect("expression row");
        }
        engine.execute("INSERT INTO wo VALUES ('k', 1); INSERT INTO col VALUES ('Q')").expect("side tables");
        engine.execute("INSERT INTO ai(v) VALUES ('a'), ('b'), ('c'); DELETE FROM ai WHERE id = 3").expect("autoincrement high-water mark");
        engine.execute("DELETE FROM t WHERE id % 2 = 0; DROP TABLE dropped").expect("free pages");
        engine
            .execute("PRAGMA user_version = 7; PRAGMA application_id = 42; CREATE TEMP TABLE tt(x); INSERT INTO tt VALUES (1)")
            .expect("session state");
        engine.execute("CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.v); END").expect("trigger");
        let before = engine.snapshot().expect("image").to_vec();
        assert!(database_formats::sqlite::read_freelist_count(&before) > 0, "the drop must leave freelist pages");

        engine.execute_one("VACUUM").expect("vacuum");
        let after = engine.snapshot().expect("image").to_vec();
        assert_eq!(database_formats::sqlite::read_freelist_count(&after), 0);
        assert!(after.len() < before.len(), "vacuum must compact the image: {} -> {}", before.len(), after.len());
        assert_eq!(engine.execute_one("PRAGMA integrity_check").expect("check").rows, vec![vec![text("ok")]]);
        assert_eq!(engine.execute_one("SELECT count(*) FROM t").expect("rows").rows, vec![vec![Cell::Integer(100)]]);
        assert_eq!(engine.execute_one("SELECT * FROM wo").expect("without rowid").rows, vec![vec![text("k"), Cell::Integer(1)]]);
        assert_eq!(engine.execute_one("SELECT a FROM ex WHERE lower(b) = 'b7'").expect("expression index").rows, vec![vec![Cell::Integer(7)]]);
        assert_eq!(engine.execute_one("SELECT c FROM col WHERE c = 'q'").expect("collation").rows, vec![vec![text("Q")]]);
        assert_eq!(engine.execute_one("SELECT count(*) FROM vv").expect("view").rows, vec![vec![Cell::Integer(100)]]);
        assert_eq!(engine.execute_one("PRAGMA user_version").expect("version").rows, vec![vec![Cell::Integer(7)]]);
        assert_eq!(engine.execute_one("PRAGMA application_id").expect("application").rows, vec![vec![Cell::Integer(42)]]);
        // 临时对象跟随会话,不参与主库重建。
        assert_eq!(engine.execute_one("SELECT * FROM tt").expect("temp table").rows, vec![vec![Cell::Integer(1)]]);
        // AUTOINCREMENT 高水位跨重建保留,下一行继续从 4 开始。
        assert_eq!(
            engine.execute_one("SELECT seq FROM sqlite_sequence WHERE name = 'ai'").expect("sequence").rows,
            vec![vec![Cell::Integer(3)]]
        );
        // 最后一个 AUTOINCREMENT 表被删后留下的空 `sqlite_sequence` 也照旧存在。
        let sequence_tables = engine.execute_one("SELECT count(*) FROM sqlite_sequence").expect("sequence table kept").rows;
        assert_eq!(sequence_tables, vec![vec![Cell::Integer(1)]]);
        engine.execute("INSERT INTO ai(v) VALUES ('d')").expect("next autoincrement row");
        assert_eq!(engine.execute_one("SELECT max(id) FROM ai").expect("id").rows, vec![vec![Cell::Integer(4)]]);
        // 触发器在重建后照常触发。
        engine.execute("INSERT INTO t VALUES (201, 'after')").expect("insert after vacuum");
        assert_eq!(engine.execute_one("SELECT x FROM log").expect("log").rows, vec![vec![text("after")]]);
        // `CREATE` 文本与表名逐字节保留。
        let master = engine
            .execute_one("SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name IN ('t','wo','ex','col') ORDER BY name")
            .expect("master");
        assert_eq!(
            master.rows,
            vec![
                vec![text("col"), text("CREATE TABLE col(c TEXT COLLATE NOCASE)")],
                vec![text("ex"), text("CREATE TABLE ex(a INT, b TEXT)")],
                vec![text("t"), text("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")],
                vec![text("wo"), text("CREATE TABLE wo(k TEXT PRIMARY KEY, n INTEGER) WITHOUT ROWID")],
            ]
        );
    }

    /// `VACUUM INTO` 写出重建副本,源镜像逐字节不变。
    #[test]
    fn vacuum_into_writes_a_rebuilt_copy_without_touching_the_source() {
        let directory = temp_directory("vacuum-into");
        let target = directory.join("copy.db");
        let mut engine = Engine::new(None).expect("blank engine");
        engine
            .execute(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
                 CREATE INDEX t_v ON t(v);
                 CREATE TABLE wo(k TEXT PRIMARY KEY) WITHOUT ROWID;
                 CREATE TABLE discarded(z)",
            )
            .expect("schema");
        for value in 1..=50 {
            engine.execute(&format!("INSERT INTO t VALUES ({value}, 'v{value}')")).expect("row");
        }
        engine.execute("INSERT INTO wo VALUES ('a'); DROP TABLE discarded").expect("free pages");
        let before = engine.snapshot().expect("image").to_vec();
        engine.execute_one(&format!("VACUUM INTO '{}'", target.display())).expect("vacuum into");
        assert_eq!(engine.snapshot().expect("image"), before.as_slice(), "VACUUM INTO must leave the source image alone");

        let bytes = std::fs::read(&target).expect("target written");
        assert_eq!(database_formats::sqlite::read_freelist_count(&bytes), 0);
        let mut copy = Engine::from_writer(SqliteWriter::from_pages(bytes).expect("open copy")).expect("copy catalog");
        assert_eq!(copy.execute_one("PRAGMA integrity_check").expect("check").rows, vec![vec![text("ok")]]);
        assert_eq!(copy.execute_one("SELECT count(*) FROM t").expect("rows").rows, vec![vec![Cell::Integer(50)]]);
        assert_eq!(copy.execute_one("SELECT * FROM wo").expect("without rowid").rows, vec![vec![text("a")]]);

        // `INTO` 目标可以是参数,槽位与绑定规则和其他语句一致。
        let bound = directory.join("bound.db");
        engine.execute_with_parameters("VACUUM INTO ?", &[Cell::Text(bound.display().to_string())]).expect("bound target");
        assert!(bound.exists(), "a bound target writes the same rebuilt copy");
        std::fs::remove_dir_all(&directory).expect("cleanup");
    }

    /// 目标已存在(含与源同路径)、父目录缺失与非文本目标按上游拒绝。
    #[test]
    fn vacuum_into_rejects_an_existing_or_unusable_target() {
        let directory = temp_directory("vacuum-into-errors");
        let target = directory.join("taken.db");
        std::fs::write(&target, b"occupied").expect("seed target");
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(a)").expect("table");

        let existing = engine.execute_one(&format!("VACUUM INTO '{}'", target.display())).expect_err("existing target");
        assert_eq!(existing.code, "SQLITE_ERROR");
        assert_eq!(existing.message, "output file already exists");
        assert_eq!(std::fs::read(&target).expect("target kept"), b"occupied");

        let missing =
            engine.execute_one(&format!("VACUUM INTO '{}'", directory.join("missing").join("x.db").display())).expect_err("missing parent");
        assert_eq!(missing.code, "SQLITE_CANTOPEN");
        assert!(missing.message.starts_with("unable to open database: "), "unexpected message: {}", missing.message);

        let directory_target = engine.execute_one(&format!("VACUUM INTO '{}'", directory.display())).expect_err("directory target");
        assert_eq!(directory_target.code, "SQLITE_CANTOPEN");

        let not_text = engine.execute_one("VACUUM INTO 5").expect_err("numeric target");
        assert_eq!(not_text.code, "SQLITE_ERROR");
        assert_eq!(not_text.message, "non-text filename");

        // 与源同路径同样按“已存在”拒绝,源文件一个字节都不改。
        let source = directory.join("source.db");
        let original = engine.snapshot().expect("image").to_vec();
        std::fs::write(&source, &original).expect("seed source");
        let same = engine.execute_one(&format!("VACUUM INTO '{}'", source.display())).expect_err("same source");
        assert_eq!(same.message, "output file already exists");
        assert_eq!(std::fs::read(&source).expect("source kept"), original);

        // 空文件名与 `:memory:` 与上游一样成功且不落盘。
        engine.execute_one("VACUUM INTO ''").expect("empty target");
        engine.execute_one("VACUUM INTO ':memory:'").expect("memory target");
        assert_eq!(std::fs::read_dir(&directory).expect("list").count(), 2);
        std::fs::remove_dir_all(&directory).expect("cleanup");
    }

    /// `VACUUM` 在事务里(含只有 `SAVEPOINT` 的作用域)被拒绝。
    #[test]
    fn vacuum_is_rejected_inside_a_transaction() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(a)").expect("table");
        engine.execute("BEGIN").expect("transaction");
        for statement in ["VACUUM", "VACUUM INTO 'ignored.db'"] {
            let error = engine.execute_one(statement).expect_err("transaction blocks vacuum");
            assert_eq!(error.code, "SQLITE_ERROR");
            assert_eq!(error.message, "cannot VACUUM from within a transaction");
        }
        engine.execute("ROLLBACK").expect("rollback");

        engine.execute("SAVEPOINT s").expect("savepoint");
        assert_eq!(engine.execute_one("VACUUM").expect_err("savepoint scope").message, "cannot VACUUM from within a transaction");
        engine.execute("RELEASE s").expect("release");
        engine.execute_one("VACUUM").expect("autocommit vacuum");
    }

    /// `VACUUM temp` 是空操作,`VACUUM temp INTO` 写出空库。
    #[test]
    fn vacuum_temp_is_a_no_op_and_vacuum_temp_into_writes_an_empty_database() {
        let directory = temp_directory("vacuum-temp");
        let target = directory.join("empty.db");
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(a); CREATE TEMP TABLE tt(x); INSERT INTO tt VALUES (1)").expect("schema");
        engine.execute_one("VACUUM temp").expect("temp vacuum");
        assert_eq!(engine.execute_one("SELECT * FROM tt").expect("temp table").rows, vec![vec![Cell::Integer(1)]]);
        engine.execute_one(&format!("VACUUM temp INTO '{}'", target.display())).expect("temp into");
        let mut copy = Engine::from_writer(SqliteWriter::open(&target).expect("open empty copy")).expect("copy catalog");
        assert_eq!(copy.execute_one("SELECT count(*) FROM sqlite_master").expect("schema rows").rows, vec![vec![Cell::Integer(0)]]);
        std::fs::remove_dir_all(&directory).expect("cleanup");
    }

    /// `VACUUM aux` 重建附加库并落回它自己的文件。
    #[test]
    fn vacuum_rebuilds_an_attached_database() {
        let directory = temp_directory("vacuum-attached");
        let file = directory.join("aux.db");
        let mut engine = Engine::new(None).expect("blank engine");
        engine
            .execute(&format!(
                "ATTACH DATABASE '{}' AS aux;
                 CREATE TABLE aux.t(id INTEGER PRIMARY KEY, v TEXT);
                 CREATE TABLE aux.discarded(z);
                 INSERT INTO aux.t VALUES (1, 'a'), (2, 'b');
                 DROP TABLE aux.discarded",
                file.display()
            ))
            .expect("attached schema");
        engine.execute_one("VACUUM aux").expect("vacuum attached");
        assert_eq!(
            engine.execute_one("SELECT id, v FROM aux.t ORDER BY id").expect("attached rows").rows,
            vec![vec![Cell::Integer(1), text("a")], vec![Cell::Integer(2), text("b")]]
        );
        let mut reopened = Engine::from_writer(SqliteWriter::open(&file).expect("open attached file")).expect("attached catalog");
        assert_eq!(reopened.execute_one("SELECT count(*) FROM t").expect("file rows").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(reopened.execute_one("PRAGMA integrity_check").expect("check").rows, vec![vec![text("ok")]]);
        std::fs::remove_dir_all(&directory).expect("cleanup");
    }

    /// `VACUUM INTO` 的产物由真实 SQLite 打开并通过 `integrity_check`。
    #[test]
    fn vacuum_into_is_readable_by_upstream_sqlite() {
        let Some(python) = reference_python()
        else {
            eprintln!("skipped: no python3/python with the sqlite3 module");
            return;
        };
        let directory = temp_directory("vacuum-upstream");
        let target = directory.join("upstream.db");
        let mut engine = Engine::new(None).expect("blank engine");
        engine
            .execute(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
                 CREATE INDEX t_v ON t(v);
                 CREATE TABLE wo(k TEXT PRIMARY KEY, n INTEGER) WITHOUT ROWID;
                 CREATE TABLE ex(a INT, b TEXT);
                 CREATE INDEX ex_lower ON ex(lower(b));
                 CREATE TABLE col(c TEXT COLLATE NOCASE);
                 CREATE VIEW vv AS SELECT id, v FROM t;
                 CREATE TABLE log(x);
                 CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.v); END",
            )
            .expect("schema");
        for value in 1..=40 {
            engine.execute(&format!("INSERT INTO t VALUES ({value}, 'v{value}')")).expect("row");
            engine.execute(&format!("INSERT INTO ex VALUES ({value}, 'B{value}')")).expect("expression row");
        }
        engine.execute("INSERT INTO wo VALUES ('k', 7); INSERT INTO col VALUES ('Q')").expect("side tables");
        engine.execute(&format!("VACUUM INTO '{}'", target.display())).expect("vacuum into");

        let script = r#"
import sqlite3, sys
db = sqlite3.connect(sys.argv[1])
assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",), "integrity_check"
assert db.execute("SELECT count(*) FROM t").fetchone() == (40,), "rows"
assert db.execute("SELECT count(*) FROM ex WHERE lower(b) = 'b7'").fetchone() == (1,), "expression index"
assert db.execute("SELECT c FROM col WHERE c = 'q'").fetchone() == ("Q",), "collation"
assert db.execute("SELECT count(*) FROM vv").fetchone() == (40,), "view"
assert db.execute("SELECT name FROM sqlite_master WHERE name = 'tr'").fetchone() == ("tr",), "trigger"
db.execute("INSERT INTO t VALUES (41, 'upstream')")
assert db.execute("SELECT count(*) FROM log WHERE x = 'upstream'").fetchone() == (1,), "trigger body"
db.close()
"#;
        let output = std::process::Command::new(python)
            .args(["-c", script, target.to_str().expect("utf-8 path")])
            .output()
            .expect("spawn upstream sqlite");
        assert!(output.status.success(), "upstream SQLite rejected the vacuum image: {}", String::from_utf8_lossy(&output.stderr));
        std::fs::remove_dir_all(&directory).expect("cleanup");
    }

    /// 每次调用生成一个唯一的临时目录,由调用方负责删除。
    fn temp_directory(label: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let stamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock is ahead of unix epoch").as_nanos();
        let directory =
            std::env::temp_dir().join(format!("sqlite-{label}-{}-{stamp}-{}", std::process::id(), SEQUENCE.fetch_add(1, Ordering::SeqCst)));
        std::fs::create_dir_all(&directory).expect("create temp directory");
        directory
    }

    /// 只读一列的投影必须与整行解码得到同一批行:未读列的掩码只是不构造
    /// `Cell`,不能改变 WHERE、ORDER BY、DISTINCT 或聚合看到的值。
    #[test]
    fn column_masks_preserve_projected_rows() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine
            .execute_batch(&[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, score REAL, payload BLOB, note TEXT)".to_string(),
                "INSERT INTO t VALUES (1, 'alpha', 1.5, x'0102', NULL), (2, 'beta', NULL, NULL, 'x'), (3, NULL, 2.25, x'ff', 'y')".to_string(),
            ])
            .expect("seed");
        let full = engine.execute_one("SELECT id, label, score, payload, note FROM t").expect("full decode").rows;
        assert_eq!(
            engine.execute_one("SELECT id FROM t").expect("id projection").rows,
            full.iter().map(|row| vec![row[0].clone()]).collect::<Vec<_>>()
        );
        assert_eq!(
            engine.execute_one("SELECT label FROM t ORDER BY id DESC").expect("label projection").rows,
            full.iter().rev().map(|row| vec![row[1].clone()]).collect::<Vec<_>>()
        );
        assert_eq!(
            engine.execute_one("SELECT count(*), sum(score), min(score), max(score) FROM t").expect("aggregate").rows,
            vec![vec![Cell::Integer(3), Cell::Real(3.75), Cell::Real(1.5), Cell::Real(2.25)]]
        );
        // An unprojected column still filters and sorts by its decoded value.
        assert_eq!(engine.execute_one("SELECT id FROM t WHERE label = 'beta'").expect("filter").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(
            engine.execute_one("SELECT id FROM t ORDER BY label DESC").expect("sort").rows,
            vec![vec![Cell::Integer(2)], vec![Cell::Integer(1)], vec![Cell::Integer(3)]]
        );
        assert_eq!(
            engine.execute_one("SELECT DISTINCT label FROM t ORDER BY label").expect("distinct").rows,
            vec![vec![Cell::Null], vec![Cell::Text("alpha".to_string())], vec![Cell::Text("beta".to_string())]]
        );
        // A short record from `ALTER TABLE ADD COLUMN` reads its default through
        // the same mask when the new column is the only projection.
        engine.execute_one("ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'd'").expect("add column");
        assert_eq!(
            engine.execute_one("SELECT extra FROM t ORDER BY id").expect("default").rows,
            vec![vec![Cell::Text("d".to_string())]; 3]
        );
    }

    /// 连接计数走裁剪后的行集与 rowid 探针时,行数必须与非连接的全扫描
    /// `WHERE` 路径逐行一致。
    #[test]
    fn pruned_join_count_matches_nested_scan() {
        let mut engine = Engine::new(None).expect("blank engine");
        let mut batch = vec!["CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT, score REAL)".to_string()];
        for row in 1..=200 {
            batch.push(format!("INSERT INTO items VALUES ({row}, 'row-{row}', {})", row as f64 / 4.0));
        }
        engine.execute_batch(&batch).expect("seed");
        let joined = engine.execute_one("SELECT count(*) FROM items JOIN items AS b ON b.id = items.id % 7").expect("join").rows;
        let nested = engine.execute_one("SELECT count(*) FROM items, items AS b WHERE b.id = items.id % 7").expect("nested").rows;
        assert_eq!(joined, nested);
        // Rowids 7, 14, ..., 196 probe key 0, which has no row, so 200 - 28 remain.
        assert_eq!(joined, vec![vec![Cell::Integer(172)]]);
        // Materializing the join keeps the left-major order and both columns.
        assert_eq!(
            engine
                .execute_one(
                    "SELECT items.id, b.label FROM items JOIN items AS b ON b.id = items.id % 7 WHERE items.id <= 20 ORDER BY items.id"
                )
                .expect("join projection")
                .rows,
            engine
                .execute_one("SELECT items.id, b.label FROM items, items AS b WHERE b.id = items.id % 7 AND items.id <= 20 ORDER BY items.id")
                .expect("nested projection")
                .rows
        );
    }

    /// 索引区间读取只解码投影与谓词需要的列时,行集和按索引首列的顺序都
    /// 必须与去掉索引后的全扫描一致。
    #[test]
    fn masked_index_range_matches_full_scan() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine
            .execute_batch(&[
                "CREATE TABLE words(id INTEGER PRIMARY KEY, label TEXT, score REAL)".to_string(),
                "CREATE UNIQUE INDEX words_label ON words(label)".to_string(),
                "INSERT INTO words VALUES (1, 'b', 1.0), (2, 'a', 2.0), (3, 'aa', 3.0), (4, 'ab', 4.0), (5, 'a10', 5.0), (6, 'b0', 6.0)"
                    .to_string(),
            ])
            .expect("seed");
        let sql = "SELECT id FROM words WHERE label BETWEEN 'a' AND 'b' ORDER BY label";
        let indexed = engine.execute_one(sql).expect("index range").rows;
        engine.execute_one("DROP INDEX words_label").expect("drop index");
        let scanned = engine.execute_one(sql).expect("full scan").rows;
        assert_eq!(indexed, scanned);
        assert_eq!(
            indexed,
            vec![vec![Cell::Integer(2)], vec![Cell::Integer(5)], vec![Cell::Integer(3)], vec![Cell::Integer(4)], vec![Cell::Integer(1)]]
        );
    }

    /// 第一个装有 `sqlite3` 模块的解释器,与互操作用例同一探测口径。
    fn reference_python() -> Option<String> {
        ["python3", "python"].into_iter().find_map(|candidate| {
            std::process::Command::new(candidate)
                .args(["-c", "import sqlite3"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
                .then(|| candidate.to_string())
        })
    }
}
