//! In-memory execution layer: SQL subset over the pure-Rust format-3 writer.
//!
//! The engine keeps the whole database image in `SqliteWriter::pages()`. It
//! never imports an upstream SQLite engine; [`Engine::from_writer`] wraps an
//! image read by `SqliteWriter::open`, and [`Engine::save`] flushes the current
//! image back to a file.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::{
    ColumnSpec, DropIndexOutcome, RecordLimits, SchemaObjectKind, SqliteWriter, TableLimits, TextEncoding, UpdateValue, decode_library_version,
    decode_record,
};

use super::error::SqlError;
use super::json_string;
use super::sql::{
    AggregateFunction, AlterAction, Assignment, BinaryOp, CastType, CheckConstraint, ColumnRef, CommonTable, CompareOp, CompoundOp, CompoundTerm,
    DropColumnConflict, Expr, ForeignKey, FrameBound, FrameEdge, FrameUnits, IndexColumn, InsertValue, JoinKind, Limit, Literal, NamedWindow,
    OrderKey, ScalarFunction, SelectCore, SelectItems, Statement, TableRef, TransactionAction, TriggerEvent, TriggerTiming, UnaryOp,
    WindowFunction, WindowSpec, core_references, parse, rewrite_create_index_column, rewrite_create_index_table,
    rewrite_create_table_column, rewrite_create_table_drop_column, rewrite_create_table_name, rewrite_view_column,
    rewrite_view_table, statement_references,
    strip_schema_qualifiers,
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
    /// `CHECK` constraints, table- and column-level, in declaration order.
    checks: Vec<CheckDef>,
    /// `REFERENCES` clauses, table- and column-level, in declaration order.
    foreign_keys: Vec<ForeignKeyDef>,
    /// `STRICT` table option was present, so every non-NULL value has to
    /// conform to its column's declared type after affinity.
    strict: bool,
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
        let declared: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.primary_key)
            .map(|(position, _)| position)
            .collect();
        if declared.is_empty() {
            None
        }
        else {
            Some(declared)
        }
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
struct IndexDef {
    /// Lowercase name of the indexed table.
    table: String,
    /// Indexed column positions, in key order.
    columns: Vec<usize>,
    /// The index enforces uniqueness.
    unique: bool,
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

/// Sentinel source indexes that mark a `NEW` / `OLD` reference in a resolved
/// expression. They never collide with a `FROM` source position.
const TRIGGER_SOURCE_NEW: usize = usize::MAX;
const TRIGGER_SOURCE_OLD: usize = usize::MAX - 1;


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
        | Statement::Pragma { schema: slot, .. } => clear(slot, schema),
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
    }
}

/// Evaluates an optional view-DML predicate against one output row.
fn row_matches(
    predicate: &Option<ResolvedExpr>,
    row: &[Cell],
    engine: &Engine,
    shape: &ScopeShape,
    budget: &Budget,
) -> Result<bool, SqlError> {
    let Some(predicate) = predicate else {
        return Ok(true);
    };
    let ctx = EvalContext {
        engine,
        outer: &[],
        current: shape,
        aggregates: &[],
        windows: &[],
        window_row: 0,
        budget,
        ctes: None,
    };
    Ok(truthiness(&predicate.eval(row, &ctx)?) == Some(true))
}

/// Whether a definition's event covers an incoming statement event.
fn trigger_event_matches(defined: &TriggerEvent, incoming: &TriggerEvent) -> bool {
    match (defined, incoming) {
        (TriggerEvent::Insert, TriggerEvent::Insert) | (TriggerEvent::Delete, TriggerEvent::Delete) => true,
        (TriggerEvent::Update(defined), TriggerEvent::Update(assigned)) => {
            defined.is_empty()
                || defined.iter().any(|column| assigned.iter().any(|assigned| assigned.eq_ignore_ascii_case(column)))
        }
        _ => false,
    }
}

/// One uniqueness rule an `UPDATE` re-checks against the live table.
enum UniqueKey {
    /// The hidden rowid, reported under `t.rowid` or `t.<alias>`.
    Rowid(String),
    /// One or more column positions, plus the `t.a, t.b` message fragment.
    Columns(Vec<usize>, String),
}

impl UniqueKey {
    /// The key values of one row; `cells` holds the declared columns only.
    fn values(&self, cells: &[Cell], rowid: i64) -> Vec<Cell> {
        match self {
            Self::Rowid(_) => vec![Cell::Integer(rowid)],
            Self::Columns(positions, _) => positions.iter().map(|position| cells[*position].clone()).collect(),
        }
    }

    fn label(&self) -> &str {
        match self {
            Self::Rowid(label) | Self::Columns(_, label) => label,
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
}

/// Everything a `DELETE` will remove.
struct DeletePlan {
    removed: Vec<i64>,
    /// `RETURNING` rows, evaluated against the row as it was.
    returning: Vec<Vec<Cell>>,
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
        resolve_expr(expr, &self.relation, &[], &[], &mut windows, &mut aggregates, false, false)
            .map_err(|error| error.in_statement(statement))
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
            SelectItems::CountAll => Err(SqlError::new("unsupported-clause", "`RETURNING count(*)` is not supported")
                .in_clause("RETURNING")
                .in_statement(statement)),
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
        let writer = SqliteWriter::create(page_size).map_err(|error| SqlError::new("storage", error.to_string()))?;
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
            foreign_keys: true,
            recursive_triggers: false,
            last_insert_rowid: 0,
            transaction: None,
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
                    return Err(SqlError::new("transaction-active", "cannot start a transaction within a transaction")
                        .in_statement("begin"));
                }
                self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("begin"))?;
                for attached in &mut self.attached {
                    attached.engine.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("begin"))?;
                }
                self.transaction = Some(TransactionState { from_begin: true, savepoints: Vec::new() });
            }
            TransactionAction::Commit => {
                if self.transaction.take().is_none() {
                    return Err(SqlError::new("transaction-missing", "cannot commit - no transaction is active")
                        .in_statement("commit"));
                }
                self.writer.commit_all().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("commit"))?;
                for attached in &mut self.attached {
                    attached
                        .engine
                        .writer
                        .commit_all()
                        .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("commit"))?;
                }
                self.persist_attached_databases()?;
            }
            TransactionAction::Rollback => {
                if self.transaction.take().is_none() {
                    return Err(SqlError::new("transaction-missing", "cannot rollback - no transaction is active")
                        .in_statement("rollback"));
                }
                self.writer
                    .rollback_all()
                    .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("rollback"))?;
                self.reload_catalog()?;
                for index in 0..self.attached.len() {
                    self.attached[index]
                        .engine
                        .writer
                        .rollback_all()
                        .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("rollback"))?;
                    self.attached[index].dirty = false;
                    self.attached[index].engine.reload_catalog()?;
                }
            }
            TransactionAction::Savepoint(name) => {
                if self.transaction.is_none() {
                    // A savepoint outside `BEGIN` opens its own transaction.
                    self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("savepoint"))?;
                    for attached in &mut self.attached {
                        attached
                            .engine
                            .writer
                            .begin()
                            .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("savepoint"))?;
                    }
                    self.transaction = Some(TransactionState { from_begin: false, savepoints: Vec::new() });
                }
                self.writer
                    .savepoint(&name)
                    .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("savepoint"))?;
                for attached in &mut self.attached {
                    attached
                        .engine
                        .writer
                        .savepoint(&name)
                        .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("savepoint"))?;
                }
                self.transaction.as_mut().expect("savepoint opened a transaction").savepoints.push(name);
            }
            TransactionAction::Release(name) => {
                let index = self.savepoint_position(&name, "release")?;
                self.writer
                    .release(&name)
                    .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("release"))?;
                for attached in &mut self.attached {
                    attached
                        .engine
                        .writer
                        .release(&name)
                        .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("release"))?;
                }
                let closing = {
                    let state = self.transaction.as_mut().expect("an open savepoint implies a transaction");
                    state.savepoints.truncate(index);
                    state.savepoints.is_empty() && !state.from_begin
                };
                if closing {
                    // Releasing the last savepoint of a transaction that no
                    // `BEGIN` opened commits it, as in SQLite.
                    self.writer.commit_all().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("release"))?;
                    self.transaction = None;
                    self.persist_attached_databases()?;
                }
            }
            TransactionAction::RollbackTo(name) => {
                let index = self.savepoint_position(&name, "rollback-to")?;
                self.writer
                    .rollback_to(&name)
                    .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("rollback-to"))?;
                self.reload_catalog()?;
                for index in 0..self.attached.len() {
                    self.attached[index]
                        .engine
                        .writer
                        .rollback_to(&name)
                        .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("rollback-to"))?;
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
        state
            .savepoints
            .iter()
            .rposition(|saved| saved.eq_ignore_ascii_case(name))
            .ok_or_else(missing)
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

    /// Replays the stored `CREATE TABLE` / `CREATE INDEX` text of every
    /// schema row into the in-memory catalog without touching storage.
    fn load_catalog(&mut self) -> Result<(), SqlError> {
        let objects = self.writer.schema_objects(TableLimits::default()).map_err(storage_error)?;
        for object in objects {
            let Some(sql) = object.sql.as_deref() else { continue };
            let mut statements = parse(sql)?.into_iter();
            match statements.next() {
                Some(Statement::CreateTable { name, columns, primary_key, checks, foreign_keys, strict, .. }) => {
                    let def = Self::catalog_table(name.clone(), columns, primary_key, checks, foreign_keys, strict)?;
                    let key = name.to_ascii_lowercase();
                    if !self.table_lookup.contains_key(&key) {
                        let position = self.tables.len();
                        self.table_lookup.insert(key, position);
                        self.tables.push(def);
                    }
                }
                Some(Statement::CreateIndex { name, table, unique, columns, .. }) => {
                    let positions = match self.tables.iter().find(|def| def.name.eq_ignore_ascii_case(&table)) {
                        Some(def) => {
                            let mut positions = Vec::with_capacity(columns.len());
                            for column in &columns {
                                match def.columns.iter().position(|existing| existing.name.eq_ignore_ascii_case(&column.name)) {
                                    Some(position) => positions.push(position),
                                    None => break,
                                }
                            }
                            positions
                        }
                        None => continue,
                    };
                    if positions.len() != columns.len() {
                        continue;
                    }
                    self.indexes.insert(name.to_ascii_lowercase(), IndexDef { table: table.to_ascii_lowercase(), columns: positions, unique });
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
        columns: Vec<ColumnSpec>,
        primary_key: Vec<String>,
        checks: Vec<CheckConstraint>,
        foreign_keys: Vec<ForeignKey>,
        strict: bool,
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
            let position = columns
                .iter()
                .position(|existing| existing.name.eq_ignore_ascii_case(column))
                .ok_or_else(|| {
                    SqlError::new("unknown-column", format!("unknown column `{column}` in table `{name}`")).in_statement("create-table")
                })?;
            key_positions.push(position);
        }
        // A lone INTEGER key column is the rowid alias whether the key is
        // declared inline or as a table constraint.
        let rowid_alias = match key_positions.as_slice() {
            [position] if columns[*position].declared_type.eq_ignore_ascii_case("INTEGER") => Some(*position),
            _ => columns.iter().position(|column| column.primary_key && column.declared_type.eq_ignore_ascii_case("INTEGER")),
        };
        let primary_key_index = !primary_key.is_empty() && rowid_alias.is_none();
        // Checks are resolved before the table exists, so an unknown column
        // fails the statement instead of leaving a half-built table behind,
        // exactly like upstream.
        let resolved_checks = resolve_checks(&name, &columns, &checks, "create-table")?;
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
            checks: resolved_checks,
            foreign_keys: resolved_foreign_keys,
            strict,
            temp: false,
        })
    }


    /// Executes exactly one statement.
    pub fn execute_one(&mut self, statement: &str) -> Result<ExecResult, SqlError> {
        let statements = parse(statement)?;
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
        let statements = parse(sql)?;
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

    /// Current database image, suitable for writing to a `.sqlite3` file.
    ///
    /// Index b-trees are rebuilt after every insert that touches an indexed
    /// table, so the exported image stays consistent with its `sqlite_master`
    /// entries without calling `SqliteWriter::save`.
    pub fn snapshot(&self) -> &[u8] {
        self.writer.pages()
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
        self.writer.save(path).map_err(|error| SqlError::new("storage", error.to_string()))
    }

    /// Configured page size.
    pub fn page_size(&self) -> u32 {
        self.writer.page_size() as u32
    }

    /// Format-3 library version stamped in the image header.
    pub fn sqlite_version(&self) -> Result<String, SqlError> {
        decode_library_version(self.writer.pages()).map_err(|error| SqlError::new("storage", error.to_string()))
    }

    /// Materializes one reserved table with the labels and row order SQLite
    /// reports. `sqlite_master` is read straight from the writer's schema
    /// b-tree, so `type`, `sql`, `rootpage`, `rowid` and ordering are the
    /// on-disk values rather than a second copy that could drift.
    fn system_rows(&self, system: SystemTable, name: &str) -> Result<(Vec<String>, Vec<Vec<Cell>>, bool), SqlError> {
        let master_labels = || {
            vec!["type".to_string(), "name".to_string(), "tbl_name".to_string(), "rootpage".to_string(), "sql".to_string()]
        };
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
        let Some((schema, table)) = fired else {
            return self.run_statement(statement);
        };
        if !self.target_is_local(schema, &table) {
            return self.run_statement(statement);
        }
        let saved_events = std::mem::take(&mut self.pending_events);
        // A trigger that fails has to undo the statement that fired it, like
        // SQLite's per-statement rollback, so the whole statement runs inside
        // one writer level owned here.
        self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("trigger"))?;
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
                        self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("trigger"))?;
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
                lower == "main"
                    || lower == "temp"
                    || self.alias.as_deref().is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
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
            .filter(|(_, trigger)| {
                trigger.table == table && trigger.timing == timing && trigger_event_matches(&trigger.event, event)
            })
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
            Relation { sources: Vec::new(), using: Vec::new(), using_slots: Vec::new(), trigger: self.trigger_frames.last() };
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
            Relation { sources: Vec::new(), using: Vec::new(), using_slots: Vec::new(), trigger: self.trigger_frames.last() };
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
        for timing in [TriggerTiming::Before, TriggerTiming::After] {
            self.pending_events.push(PendingTriggerEvent {
                table: table.to_string(),
                timing,
                event: TriggerEvent::Update(assigned.to_vec()),
                columns: columns.to_vec(),
                new: Some(new.clone()),
                old: Some(old.clone()),
            });
        }
    }

    /// Queues the `BEFORE` and `AFTER` events of one deleted row.
    fn queue_delete_events(&mut self, table: &str, columns: &[String], rowid: i64, old: &[Cell]) {
        let old = frame_cells(old, columns.len() - 1, rowid);
        for timing in [TriggerTiming::Before, TriggerTiming::After] {
            self.pending_events.push(PendingTriggerEvent {
                table: table.to_string(),
                timing,
                event: TriggerEvent::Delete,
                columns: columns.to_vec(),
                new: None,
                old: Some(old.clone()),
            });
        }
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
                        let position = names
                            .iter()
                            .position(|column| column.eq_ignore_ascii_case(name))
                            .ok_or_else(|| {
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
            return Err(SqlError::new("unsupported-clause", "RETURNING on an INSTEAD OF view update is not supported yet")
                .in_statement("update"));
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
                Some(filter) => {
                    Some(resolve_expr(filter, &relation, &[], &[], &mut windows, &mut aggregates, false, false)?)
                }
                None => None,
            };
            let mut targets = Vec::with_capacity(assignments.len());
            for assignment in assignments {
                let index = names
                    .iter()
                    .position(|column| column.eq_ignore_ascii_case(&assignment.column))
                    .ok_or_else(|| {
                        SqlError::new("unknown-column", format!("table {table} has no column named {}", assignment.column))
                            .in_statement("update")
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
            return Err(SqlError::new("unsupported-clause", "RETURNING on an INSTEAD OF view delete is not supported yet")
                .in_statement("delete"));
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
                Some(filter) => {
                    Some(resolve_expr(filter, &relation, &[], &[], &mut windows, &mut aggregates, false, false)?)
                }
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
        let Some(schema) = leading_schema(&statement).map(str::to_owned) else {
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
            self.attached[index]
                .engine
                .save(&path)
                .map_err(|error| SqlError::new("storage", error.to_string()))?;
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
                        return Err(
                            SqlError::new("unable-to-open", format!("unable to open database: {file}")).in_statement("attach")
                        );
                    }
                }
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&requested)
                    .map_err(|_| SqlError::new("unable-to-open", format!("unable to open database: {file}")))?;
                SqliteWriter::create(self.page_size() as usize).map_err(|error| SqlError::new("storage", error.to_string()))?
            };
            let engine = Engine::from_writer(writer)?;
            (engine, Some(path.clone()), path.to_string_lossy().into_owned())
        };
        let mut engine = Box::new(engine);
        engine.alias = Some(name.to_string());
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
        let Some(index) = self.attached_lookup.get(&key).copied() else {
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

    fn run_statement_inner(&mut self, statement: Statement) -> Result<ExecResult, SqlError> {
        match statement {
            Statement::Transaction { action } => self.transaction(action),
            Statement::Attach { file, name } => self.attach_database(&file, &name),
            Statement::Detach { name } => self.detach_database(&name),
            Statement::CreateTable { schema: _, name, if_not_exists, columns, primary_key, checks, foreign_keys, strict, temp, sql } => {
                let key = name.to_ascii_lowercase();
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
                    let mut def = Self::catalog_table(name, columns, primary_key, checks, foreign_keys, strict)?;
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
                    checks.clone(),
                    foreign_keys.clone(),
                    strict,
                )?;
                self.writer
                    .create_table_with_primary_key_sql(&name, &columns, &primary_key, Some(&sql))
                    .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("create-table"))?;
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
                let temp_target = temp || self.scope == TableScope::Temp
                    || schema.as_deref().is_some_and(|qualifier| qualifier.eq_ignore_ascii_case("temp"));
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
                    self.temp_views.insert(
                        key,
                        TempView { def: ViewDef { name, columns, query: *query }, sql: stored, master_rowid },
                    );
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
                    .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("create-view"))?;
                self.views.insert(key, ViewDef { name, columns, query: *query });
                Ok(ExecResult { statement: "create-view", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::DropView { schema, name, if_exists } => {
                let key = name.to_ascii_lowercase();
                let temp_target = self.scope == TableScope::Temp
                    || schema.as_deref().is_some_and(|qualifier| qualifier.eq_ignore_ascii_case("temp"));
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
                self.writer
                    .drop_view(&name)
                    .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("drop-view"))?;
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
                let view_target = if temp_target.is_some() || main_target.is_some() {
                    false
                }
                else {
                    self.views.contains_key(&table_key)
                };
                if temp_target.is_none() && main_target.is_none() && !view_target {
                    let qualifier = match self.scope {
                        TableScope::Temp => "temp",
                        _ => "main",
                    };
                    return Err(SqlError::new("unknown-table", format!("no such table: {qualifier}.{table}"))
                        .in_statement("create-trigger"));
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
                        .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("create-trigger"))?;
                    0
                };
                self.triggers.insert(
                    key,
                    TriggerDef { name, table: table_key, timing, event, when, body, order, temp, sql, master_rowid },
                );
                Ok(ExecResult { statement: "create-trigger", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::DropTrigger { schema: _, name, if_exists } => {
                let key = name.to_ascii_lowercase();
                let Some(trigger) = self.triggers.remove(&key) else {
                    if if_exists {
                        return Ok(ExecResult { statement: "drop-trigger", columns: Vec::new(), rows: Vec::new(), affected: 0 });
                    }
                    return Err(SqlError::new("unknown-trigger", format!("no such trigger: {name}")).in_statement("drop-trigger"));
                };
                if !trigger.temp {
                    self.writer
                        .drop_trigger(&trigger.name)
                        .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("drop-trigger"))?;
                }
                Ok(ExecResult { statement: "drop-trigger", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::Insert { schema: _, table, columns, rows, default_values, returning } => {
                let key = table.to_ascii_lowercase();
                check_readonly_table(&table, "insert")?;
                if self.views.contains_key(&key) {
                    return self.run_view_insert(&table, &key, columns.as_deref(), &rows, default_values);
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
                // A `STRICT` table rejects a value whose post-affinity storage
                // class differs from the declared type, before any row is
                // written.
                for slots in &slots_by_row {
                    for (position, slot) in slots.iter().enumerate() {
                        if let Some(value) = slot {
                            if let Some(error) = strict_value_violation(def, position, value, "insert") {
                                return Err(error);
                            }
                        }
                    }
                }
                // One row at a time inside a transaction: a failing `CHECK`
                // must undo every row of a multi-row statement, as SQLite does.
                // Temp tables snapshot their row map instead of opening one.
                let temp_snapshot = temp_position.map(|position| self.temp_tables[position].rows.clone());
                if temp_position.is_none() {
                    self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("insert"))?;
                }
                let temp_keys = if temp_position.is_some() { self.unique_keys(def, &key) } else { Vec::new() };
                let check_shape = ScopeShape { columns: Vec::new() };
                let check_budget = Budget::new();
                let queue_triggers = self.table_has_triggers(&key);
                let trigger_columns = trigger_frame_columns(def);
                let mut rowids = Vec::with_capacity(slots_by_row.len());
                let mut last_written: Option<i64> = None;
                let mut returned: Vec<Vec<Cell>> = Vec::new();
                let mut failure = None;
                for slots in &slots_by_row {
                    // A `BEFORE INSERT` trigger sees the row before it is
                    // written: defaults are already applied and an unassigned
                    // rowid is `-1`, exactly like SQLite.
                    if queue_triggers {
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
                    let rowid = if let Some(position) = temp_position {
                        let table = &mut self.temp_tables[position];
                        match temp_insert_row(def, &mut table.rows, slots, &temp_keys) {
                            Ok(rowid) => rowid,
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        }
                    }
                    else {
                        match self.writer.insert(&table, slots) {
                            Ok(rowid) => rowid,
                            Err(error) => {
                                failure = Some(SqlError::new("storage", error.to_string()).in_statement("insert"));
                                break;
                            }
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
                                "constraint-failed",
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
                if failure.is_none() && self.foreign_keys {
                    if let Err(error) = self.check_insert_foreign_keys(def, &rowids, "insert") {
                        failure = Some(error);
                    }
                }
                if let Some(rowid) = last_written {
                    self.last_insert_rowid = rowid;
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
                    self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("insert"))?;
                }
                // Explicit indexes and the automatic ones built for column-level
                // `PRIMARY KEY` / `UNIQUE` both live only as key sets until they
                // are materialized into b-tree pages, so a write to either kind
                // of indexed table has to rebuild them before the image is read.
                let aliased = def.rowid_alias;
                let constraint_indexed = def.primary_key_index
                    || def
                        .columns
                        .iter()
                        .enumerate()
                        .any(|(position, column)| (column.primary_key || column.unique) && Some(position) != aliased);
                if temp_position.is_none() && (constraint_indexed || self.indexes.values().any(|index| index.table == key)) {
                    self.writer
                        .materialize_indexes()
                        .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("insert"))?;
                }
                if let Some(plan) = &returning {
                    return Ok(ExecResult {
                        statement: "insert",
                        columns: plan.columns.clone(),
                        rows: returned,
                        affected: slots_by_row.len(),
                    });
                }
                if default_values {
                    // `DEFAULT VALUES` reports neither columns nor rows, like
                    // SQLite; the stored row is written the same way.
                    return Ok(ExecResult { statement: "insert", columns: Vec::new(), rows: Vec::new(), affected: slots_by_row.len() });
                }
                let rows = rowids.into_iter().map(|rowid| vec![Cell::Integer(rowid)]).collect();
                Ok(ExecResult { statement: "insert", columns: vec!["rowid".to_string()], rows, affected: slots_by_row.len() })
            }
            Statement::CreateIndex { schema: _, name, table, if_not_exists, unique, columns, sql } => {
                let table_key = table.to_ascii_lowercase();
                if is_schema_table(&table) {
                    return Err(SqlError::new("readonly-system-table", format!("table {table} may not be indexed")).in_statement("create-index"));
                }
                if self.views.contains_key(&table_key) {
                    return Err(SqlError::new("readonly-view", "views may not be indexed").in_statement("create-index"));
                }
                check_reserved_object_name(&name, "create-index")?;
                let position = *self
                    .table_lookup
                    .get(&table_key)
                    .ok_or_else(|| SqlError::new("unknown-table", format!("unknown table `{table}`")).in_statement("create-index"))?;
                let def = &self.tables[position];
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
                    return Err(SqlError::new("index-exists", format!("there is already an object named `{name}`")).in_statement("create-index"));
                }
                let mut positions = Vec::with_capacity(columns.len());
                for column in &columns {
                    let position = def
                        .columns
                        .iter()
                        .position(|existing| existing.name.eq_ignore_ascii_case(&column.name))
                        .ok_or_else(|| {
                            SqlError::new("unknown-column", format!("unknown column `{}` in table `{table}`", column.name)).in_statement("create-index")
                        })?;
                    positions.push(position);
                }
                if unique {
                    self.ensure_unique_keys(&table, def, &columns)?;
                }
                let names = columns.iter().map(|column| column.name.clone()).collect::<Vec<_>>();
                self.writer
                    .create_index_with_sql(&name, &table, &names, unique, Some(&sql))
                    .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("create-index"))?;
                self.indexes.insert(key, IndexDef { table: table_key, columns: positions, unique });
                Ok(ExecResult { statement: "create-index", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::Update { schema: _, table, alias, assignments, filter, returning } => {
                check_readonly_table(&table, "update")?;
                let key = table.to_ascii_lowercase();
                if self.views.contains_key(&key) {
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
                let shape = TargetShape::new(def, alias.as_deref().unwrap_or(&table), self.trigger_frames.last());
                let mut targets: Vec<(usize, ResolvedExpr)> = Vec::with_capacity(assignments.len());
                for assignment in &assignments {
                    let index = def
                        .columns
                        .iter()
                        .position(|column| column.name.eq_ignore_ascii_case(&assignment.column))
                        .ok_or_else(|| {
                            SqlError::new("unknown-column", format!("unknown column `{}` in table `{table}`", assignment.column)).in_statement("update")
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
                let budget = Budget::new();
                let needs_index = self.has_indexes(def, &key);
                let plan = self.plan_update(def, &key, &targets, predicate.as_ref(), returning.as_ref(), &shape.scope, &budget)?;
                // Old row values are needed to tell which referenced keys this
                // statement rewrites, so they are read before the transaction.
                let old_rows = self.live_rows(def, "update")?;
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
                let old_lookup: BTreeMap<i64, Vec<Cell>> = old_rows.iter().cloned().collect();
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
                    self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("update"))?;
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
                            self.queue_update_events(&key, def, &trigger_columns, &assigned_names, *rowid, &old_cells, &new_cells);
                        }
                        if let Err(error) = self.writer.update(&table, *rowid, values) {
                            let _ = self.writer.rollback();
                            return Err(write_failure(error, "update", def));
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
                    self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("update"))?;
                }
                if temp_position.is_none() && needs_index {
                    self.writer
                        .materialize_indexes()
                        .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("update"))?;
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
                let needs_index = self.has_indexes(def, &key);
                let plan = self.plan_delete(def, predicate.as_ref(), returning.as_ref(), &shape.scope, &budget)?;
                // The deleted rows' referenced keys are only visible before the
                // removal, so the snapshot is taken ahead of the transaction.
                let old_rows = self.live_rows(def, "delete")?;
                // A temp table snapshots its row map instead of opening a
                // writer transaction, so a failing referential check undoes the
                // removal exactly like a rollback would.
                let temp_snapshot = temp_position.map(|position| self.temp_tables[position].rows.clone());
                let queue_triggers = self.table_has_triggers(&key);
                let trigger_columns = trigger_frame_columns(def);
                let old_lookup: BTreeMap<i64, Vec<Cell>> = old_rows.iter().cloned().collect();
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
                    self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("delete"))?;
                    for rowid in &plan.removed {
                        if queue_triggers {
                            let old_cells = old_lookup.get(rowid).cloned().unwrap_or_default();
                            self.queue_delete_events(&key, &trigger_columns, *rowid, &old_cells);
                        }
                        if let Err(error) = self.writer.delete(&table, *rowid) {
                            let _ = self.writer.rollback();
                            return Err(write_failure(error, "delete", def));
                        }
                    }
                }
                if self.foreign_keys {
                    if let Err(error) = self.check_delete_foreign_keys(def, &old_rows, &plan.removed, "delete") {
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
                    self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("delete"))?;
                }
                if temp_position.is_none() && needs_index {
                    self.writer
                        .materialize_indexes()
                        .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("delete"))?;
                }
                Ok(ExecResult {
                    statement: "delete",
                    columns: returning.as_ref().map(|plan| plan.columns.clone()).unwrap_or_default(),
                    rows: plan.returning,
                    affected: plan.removed.len(),
                })
            }
            Statement::Pragma { schema: _, name, value } => {
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
                        format!("unsupported pragma `{name}`; this engine supports `foreign_keys` and `recursive_triggers`"),
                    )
                    .in_clause("PRAGMA")
                    .in_statement("pragma"));
                }
                let flag = if recursive { &mut self.recursive_triggers } else { &mut self.foreign_keys };
                match value {
                    None => Ok(ExecResult {
                        statement: "pragma",
                        columns: vec![name.clone()],
                        rows: vec![vec![Cell::Integer(i64::from(*flag))]],
                        affected: 0,
                    }),
                    Some(text) => {
                        // SQLite reads the value as a boolean: `on` / `true`
                        // / `yes` and any positive integer enable it, `off` /
                        // `false` / `no` / zero and non-numeric text disable
                        // it.
                        *flag = boolean(&text);
                        Ok(ExecResult { statement: "pragma", columns: Vec::new(), rows: Vec::new(), affected: 0 })
                    }
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
                    return Err(SqlError::new("unknown-table", format!("no such table: {}", self.scoped_name(&name))).in_statement("drop-table"));
                }
                if name.eq_ignore_ascii_case("sqlite_sequence") {
                    return Err(SqlError::new("readonly-system-table", format!("table {name} may not be dropped")).in_statement("drop-table"));
                }
                let key = name.to_ascii_lowercase();
                let position = *self
                    .table_lookup
                    .get(&key)
                    .ok_or_else(|| SqlError::new("unknown-table", format!("no such table: {name}")).in_statement("drop-table"))?;
                self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("drop-table"))?;
                match self.writer.drop_table(&name) {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = self.writer.rollback();
                        return Err(SqlError::new("unknown-table", format!("no such table: {name}")).in_statement("drop-table"));
                    }
                    Err(error) => {
                        let _ = self.writer.rollback();
                        return Err(SqlError::new("storage", error.to_string()).in_statement("drop-table"));
                    }
                }
                self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("drop-table"))?;
                self.tables.remove(position);
                self.reindex_tables();
                self.indexes.retain(|_, index| index.table != key);
                self.triggers.retain(|_, trigger| trigger.table != key);
                Ok(ExecResult { statement: "drop-table", columns: Vec::new(), rows: Vec::new(), affected: 1 })
            }
            Statement::DropIndex { schema: _, name, if_exists } => {
                self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("drop-index"))?;
                let outcome = match self.writer.drop_index(&name) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        let _ = self.writer.rollback();
                        return Err(SqlError::new("storage", error.to_string()).in_statement("drop-index"));
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
                self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("drop-index"))?;
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
        self.writer.schema_objects(TableLimits::default()).map_err(|error| SqlError::new("storage", error.to_string()).in_statement(statement))
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
                if self.table_lookup.contains_key(&new_key)
                    || self.indexes.contains_key(&new_key)
                    || self.views.contains_key(&new_key)
                {
                    return Err(SqlError::new(
                        "table-exists",
                        format!("there is already another table or index with this name: {new_name}"),
                    )
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
                    let Some(view) = self.views.get(&object.name.to_ascii_lowercase()) else { continue };
                    let Some(text) = &object.sql else { continue };
                    if !statement_references(&view.query, &old_name) {
                        continue;
                    }
                    if let Some(rewritten) = rewrite_view_table(text, &old_name, &new_name) {
                        pending_views.push((view.name.clone(), rewritten));
                    }
                }
                self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
                if let Err(error) = self.writer.rename_table_schema(&old_name, &new_name, &new_sql, &pending) {
                    let _ = self.writer.rollback();
                    return Err(SqlError::new("storage", error.to_string()).in_statement("alter-table"));
                }
                self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
                if !pending_views.is_empty() {
                    self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
                    for (name, text) in &pending_views {
                        if let Err(error) = self.writer.rewrite_view_sql(name, text) {
                            let _ = self.writer.rollback();
                            return Err(SqlError::new("storage", error.to_string()).in_statement("alter-table"));
                        }
                    }
                    self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
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
                    let Some(view) = self.views.get(&object.name.to_ascii_lowercase()) else { continue };
                    let Some(text) = &object.sql else { continue };
                    if !statement_mentions_column(&view.query, &old_column, None) {
                        continue;
                    }
                    if let Some(rewritten) = rewrite_view_column(text, &old_column, &new) {
                        pending_views.push((view.name.clone(), rewritten));
                    }
                }
                self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
                if let Err(error) = self.writer.rename_column_schema(&name, &new_sql, &pending) {
                    let _ = self.writer.rollback();
                    return Err(SqlError::new("storage", error.to_string()).in_statement("alter-table"));
                }
                self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
                if !pending_views.is_empty() {
                    self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
                    for (view, text) in &pending_views {
                        if let Err(error) = self.writer.rewrite_view_sql(view, text) {
                            let _ = self.writer.rollback();
                            return Err(SqlError::new("storage", error.to_string()).in_statement("alter-table"));
                        }
                    }
                    self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
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
                    return Err(SqlError::new("duplicate-column", format!("duplicate column name: {}", column.name))
                        .in_statement("alter-table"));
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
                    .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?
                    .is_empty();
                if column.not_null && matches!(column.default_value, None | Some(crate::WriteValue::Null)) && has_rows {
                    return Err(SqlError::new("invalid-alter", "Cannot add a NOT NULL column with default value NULL")
                        .in_statement("alter-table"));
                }
                let mut columns = def.columns.clone();
                columns.push(column.clone());
                let resolved = resolve_checks(&def.name, &columns, &checks, "alter-table")?;
                let resolved_foreign_keys = resolve_foreign_keys(&columns, &foreign_keys)?;
                let open = stored
                    .rfind(')')
                    .ok_or_else(|| SqlError::new("storage", "sqlite stored table text has no column list").in_statement("alter-table"))?;
                let new_sql = format!("{}, {}{}", &stored[..open], sql, &stored[open..]);
                self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
                if let Err(error) = self.writer.rewrite_table_sql(&def.name, &new_sql) {
                    let _ = self.writer.rollback();
                    return Err(SqlError::new("storage", error.to_string()).in_statement("alter-table"));
                }
                self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
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
                    return Err(SqlError::new("invalid-alter", format!("cannot drop PRIMARY KEY column: \"{column}\"")).in_statement("alter-table"));
                }
                if def.columns[column_position].unique {
                    return Err(SqlError::new("invalid-alter", format!("cannot drop UNIQUE column: \"{column}\"")).in_statement("alter-table"));
                }
                if def.columns.len() == 1 {
                    return Err(SqlError::new("invalid-alter", format!("cannot drop column \"{column}\": no other columns exist"))
                        .in_statement("alter-table"));
                }
                if let Some((name, _)) = self
                    .indexes
                    .iter()
                    .find(|(_, index)| index.table == key && index.columns.contains(&column_position))
                {
                    return Err(SqlError::new(
                        "invalid-alter",
                        format!("error in index {name} after drop column: no such column: {column}"),
                    )
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
                if let Some((name, _)) = self
                    .views
                    .iter()
                    .find(|(_, view)| statement_mentions_column(&view.query, &column, None))
                {
                    return Err(SqlError::new(
                        "invalid-alter",
                        format!("error in view {name} after drop column: no such column: {column}"),
                    )
                    .in_statement("alter-table"));
                }
                if let Some((name, _)) = self
                    .triggers
                    .iter()
                    .find(|(_, trigger)| {
                        trigger.table == key
                            && trigger.body.iter().any(|statement| {
                                statement_mentions_column(statement, &column, Some(&["new", "old"]))
                            })
                    })
                {
                    return Err(SqlError::new(
                        "invalid-alter",
                        format!("error in trigger {name} after drop column: no such column: {column}"),
                    )
                    .in_statement("alter-table"));
                }
                // The catalog is re-derived from the rewritten DDL so checks,
                // foreign keys and the rowid alias all follow the new column
                // positions instead of being patched by hand.
                let mut statements = parse(&new_sql).map_err(|_| {
                    SqlError::new("storage", "sqlite stored table text unrecognized").in_statement("alter-table")
                })?;
                let Some(Statement::CreateTable { name, columns, primary_key, checks, foreign_keys, strict, .. }) =
                    statements.drain(..).next()
                else {
                    return Err(SqlError::new("storage", "sqlite stored table text unrecognized").in_statement("alter-table"));
                };
                let derived = Self::catalog_table(name, columns, primary_key, checks, foreign_keys, strict)?;
                self.writer.begin().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
                if let Err(error) = self.writer.drop_column(&def.name, column_position) {
                    let _ = self.writer.rollback();
                    return Err(SqlError::new("storage", error.to_string()).in_statement("alter-table"));
                }
                if let Err(error) = self.writer.rewrite_table_sql(&def.name, &new_sql) {
                    let _ = self.writer.rollback();
                    return Err(SqlError::new("storage", error.to_string()).in_statement("alter-table"));
                }
                self.writer.commit().map_err(|error| SqlError::new("storage", error.to_string()).in_statement("alter-table"))?;
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
        for column in columns {
            let position = def
                .columns
                .iter()
                .position(|existing| existing.name.eq_ignore_ascii_case(&column.name))
                .ok_or_else(|| {
                    SqlError::new("unknown-column", format!("unknown column `{}` in table `{table}`", column.name)).in_statement("create-index")
                })?;
            positions.push(position);
        }
        let mut rows = Vec::new();
        for row in self.writer.table_rows(table, TableLimits::default()).map_err(|error| {
            SqlError::new("storage", error.to_string()).in_statement("create-index")
        })? {
            rows.push(decode_row(&row.payload, def, row.rowid)?);
        }
        for right in 1..rows.len() {
            for left in 0..right {
                let collides = positions.iter().all(|position| {
                    let (first, second) = (&rows[left][*position], &rows[right][*position]);
                    !matches!(first, Cell::Null) && !matches!(second, Cell::Null) && compare_cells(first, second) == Ordering::Equal
                });
                if collides {
                    let names = positions
                        .iter()
                        .map(|position| format!("{table}.{}", def.columns[*position].name))
                        .collect::<Vec<_>>()
                        .join(", ");
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
        }
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
        let stored = self
            .writer
            .table_rows(&def.name, TableLimits::default())
            .map_err(|error| SqlError::new("storage", error.to_string()).in_statement(statement))?;
        let mut rows = Vec::with_capacity(stored.len());
        for row in &stored {
            rows.push((row.rowid, decode_row(&row.payload, def, row.rowid)?));
        }
        rows.sort_by_key(|(rowid, _)| *rowid);
        Ok(rows)
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
            parent
                .primary_key_positions()
                .ok_or_else(|| foreign_key_mismatch(child, key, statement))?
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
        let declared: Vec<usize> = parent
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.primary_key)
            .map(|(position, _)| position)
            .collect();
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
        self.indexes
            .values()
            .any(|index| index.table == parent_key && index.unique && same_positions(&index.columns, positions))
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
        let mut keys = vec![UniqueKey::Rowid(rowid_label)];
        if def.primary_key_index && !def.primary_key.is_empty() {
            keys.push(UniqueKey::Columns(def.primary_key.clone(), label(&def.primary_key)));
        }
        for (position, column) in def.columns.iter().enumerate() {
            if Some(position) == def.rowid_alias {
                continue;
            }
            if column.primary_key || column.unique {
                keys.push(UniqueKey::Columns(vec![position], label(&[position])));
            }
        }
        for index in self.indexes.values() {
            if !def.temp && index.table == table_key && index.unique {
                keys.push(UniqueKey::Columns(index.columns.clone(), label(&index.columns)));
            }
        }
        keys
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
            || (!def.temp && self.indexes.values().any(|index| index.table == table_key))
    }

    /// Plans one `UPDATE` without touching storage: every right-hand side is
    /// evaluated against the pre-statement row, assignments apply left to right
    /// (last one wins), and each row is checked against the *current* table, so
    /// a rowid shift behaves exactly like SQLite's ascending-order pass.
    fn plan_update(
        &self,
        def: &TableDef,
        table_key: &str,
        targets: &[(usize, ResolvedExpr)],
        predicate: Option<&ResolvedExpr>,
        returning: Option<&ReturningPlan>,
        shape: &ScopeShape,
        budget: &Budget,
    ) -> Result<UpdatePlan, SqlError> {
        let width = def.columns.len();
        let keys = self.unique_keys(def, table_key);
        let live = self.live_rows(def, "update")?;
        let mut state = live.clone();
        let mut writes = Vec::new();
        let mut returned = Vec::new();
        let mut affected = 0usize;
        for (rowid, cells) in &live {
            let mut eval_cells = cells.clone();
            eval_cells.push(Cell::Integer(*rowid));
            let ctx = EvalContext {
                engine: self,
                outer: &[],
                current: shape,
                aggregates: &[],
                windows: &[],
                window_row: 0,
                budget,
                ctes: None,
            };
            if let Some(predicate) = predicate {
                if truthiness(&predicate.eval(&eval_cells, &ctx)?) != Some(true) {
                    continue;
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
                                SqlError::new("constraint-failed", format!("NOT NULL constraint failed: {}.{}", def.name, column.name))
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
                        return Err(SqlError::new(
                            "constraint-failed",
                            format!("NOT NULL constraint failed: {}.{}", def.name, column.name),
                        )
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
                    return Err(SqlError::new("constraint-failed", format!("CHECK constraint failed: {name}"))
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
                .map(|(index, _)| {
                    if assigned[index] { UpdateValue::Set(cell_write_value(&next[index])) } else { UpdateValue::Keep }
                })
                .collect();
            writes.push((*rowid, values));
            affected += 1;
        }
        Ok(UpdatePlan { writes, returning: returned, affected })
    }

    /// Plans one `DELETE`: the matching rowids in ascending order, plus any
    /// `RETURNING` rows, which see the row as it was before removal.
    fn plan_delete(
        &self,
        def: &TableDef,
        predicate: Option<&ResolvedExpr>,
        returning: Option<&ReturningPlan>,
        shape: &ScopeShape,
        budget: &Budget,
    ) -> Result<DeletePlan, SqlError> {
        let live = self.live_rows(def, "delete")?;
        let mut removed = Vec::new();
        let mut returned = Vec::new();
        for (rowid, cells) in &live {
            let mut eval_cells = cells.clone();
            eval_cells.push(Cell::Integer(*rowid));
            let ctx = EvalContext {
                engine: self,
                outer: &[],
                current: shape,
                aggregates: &[],
                windows: &[],
                window_row: 0,
                budget,
                ctes: None,
            };
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
        Ok(DeletePlan { removed, returning: returned })
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
        let mut sources = Vec::with_capacity(from.len());
        let mut tables: Vec<Vec<Vec<Cell>>> = Vec::with_capacity(from.len());
        let mut offset = 0;
        for table in from {
            let (columns, rows) = self.select_source(table, budget, ctes)?;
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
        let mut relation = Relation { sources, using: vec![Vec::new(); from.len()], using_slots: Vec::new(), trigger: None };
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
                    let resolved = resolve_expr(
                        on,
                        &relation,
                        &outer_shapes,
                        &[],
                        &mut scratch_windows,
                        &mut scratch_aggregates,
                        false,
                        false,
                    )?;
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
        let (projection, labels): (Option<Vec<ResolvedExpr>>, Vec<String>) = match items {
            SelectItems::Named(exprs) => {
                let nodes = exprs
                    .iter()
                    .map(|expr| resolve_expr(expr, &relation, &outer_shapes, named, &mut windows, &mut specs, true, true))
                    .collect::<Result<Vec<_>, _>>()?;
                (Some(nodes), exprs.iter().map(|expr| match expr {
                    // A bare column is named after the schema's own spelling,
                    // so `SELECT A FROM t` reports `a`.
                    Expr::Column(reference) => relation.resolved_column_name(reference).unwrap_or_else(|| expr.label()),
                    _ => expr.label(),
                }).collect())
            }
            SelectItems::CountAll => (None, vec!["count(*)".to_string()]),
            SelectItems::Star => (None, relation.star_columns()),
        };
        let output_width = labels.len();
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
                group_keys.push(GroupKey::Projected(position as usize - 1));
                continue;
            }
            let mut scratch_aggregates = Vec::new();
            let mut scratch_windows = Vec::new();
            group_keys.push(GroupKey::Expression(resolve_expr(
                expr,
                &relation,
                &outer_shapes,
                &[],
                &mut scratch_windows,
                &mut scratch_aggregates,
                false,
                false,
            )?));
        }
        // `HAVING` shares the projection's aggregate list, so it may add an
        // aggregate that the projection does not mention. Window functions are
        // not allowed there; SQLite reports that as a `misuse`.
        let having_expr = match having {
            Some(expr) => Some(resolve_expr(expr, &relation, &outer_shapes, named, &mut windows, &mut specs, true, false)?),
            None => None,
        };
        let keys: Vec<(SortKey, bool)> = order
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
                    return Ok((SortKey::Projected(position as usize - 1), entry.descending));
                }
                // An unqualified name that matches a projection alias sorts by
                // the output column; SQLite prefers the alias over a same-named
                // base column.
                if let Expr::Column(column) = &entry.expr {
                    if column.qualifier.is_none() {
                        if let Some(position) = labels.iter().position(|label| label.eq_ignore_ascii_case(&column.name)) {
                            return Ok((SortKey::Projected(position), entry.descending));
                        }
                    }
                }
                Ok((
                    SortKey::Expression(resolve_expr(
                        &entry.expr,
                        &relation,
                        &outer_shapes,
                        named,
                        &mut windows,
                        &mut specs,
                        true,
                        true,
                    )?),
                    entry.descending,
                ))
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
        let ctx = EvalContext {
            engine: self,
            outer,
            current: &shape,
            aggregates: &empty_aggregates,
            windows: &[],
            window_row: 0,
            budget,
            ctes,
        };
        let mut conjuncts = Vec::new();
        if let Some(predicate) = predicate {
            split_conjuncts(predicate, &mut conjuncts);
        }
        let rows = if joined {
            join_rows(&relation, &ons, &tables, &conjuncts, take, &ctx)?
        }
        else {
            cross_product(&tables, &relation, &conjuncts, take, &ctx)?
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
                let group_ctx = EvalContext {
                    engine: self,
                    outer,
                    current: &shape,
                    aggregates: &aggregates,
                    windows: &[],
                    window_row: 0,
                    budget,
                    ctes,
                };
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
                out = retain_first_distinct(out);
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
            let mut seen: Vec<Vec<Cell>> = Vec::with_capacity(rows.len());
            let mut kept: Vec<usize> = Vec::with_capacity(rows.len());
            for &index in &order {
                if !seen.contains(&outputs[index]) {
                    seen.push(outputs[index].clone());
                    kept.push(index);
                }
            }
            let (start, end) = limit_range(kept.len(), limit);
            let out: Vec<Vec<Cell>> = kept[start..end].iter().map(|index| outputs[*index].clone()).collect();
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
        let Some(limit) = limit else { return Ok(None) };
        let relation = Relation { sources: Vec::new(), using: Vec::new(), using_slots: Vec::new(), trigger: None };
        let shape = relation.shape();
        let outer_shapes: Vec<ScopeShape> = outer.iter().map(Scope::shape).collect();
        let mut aggregates = Vec::new();
        let mut windows = Vec::new();
        let empty: Vec<Cell> = Vec::new();
        let ctx = EvalContext {
            engine: self,
            outer,
            current: &shape,
            aggregates: &empty,
            windows: &[],
            window_row: 0,
            budget,
            ctes,
        };
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
                    format!(
                        "SELECTs to the left and right of {} do not have the same number of result columns",
                        term.op.text()
                    ),
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
                    .find_map(|(index, descending)| {
                        let ordering = compare_cells(&left[*index], &right[*index]);
                        (ordering != Ordering::Equal).then(|| if *descending { ordering.reverse() } else { ordering })
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
    ) -> Result<(Vec<Vec<Cell>>, Vec<Vec<Cell>>, Vec<bool>), SqlError> {
        let mut values: Vec<Vec<Cell>> = vec![Vec::new(); windows.len()];
        let mut keys: Vec<Vec<Cell>> = vec![Vec::new(); inputs.len()];
        let mut descending: Vec<bool> = Vec::new();
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
                let ctx = EvalContext {
                    engine: self,
                    outer,
                    current: shape,
                    aggregates: &row.aggregates,
                    windows: &[],
                    window_row: 0,
                    budget,
                    ctes,
                };
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
            descending.resize(descending.len() + window.partition.len(), false);
            descending.extend(window.order.iter().map(|(_, descending)| *descending));
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
            order.sort_by(|left, right| compare_window_rows(&window_partitions[*left], &window_orders[*left], &window_partitions[*right], &window_orders[*right], &window.order));
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
                    column[indices[slot]] = if len == 1 {
                        Cell::Real(0.0)
                    } else {
                        Cell::Real(peer_start[slot] as f64 / (len - 1) as f64)
                    };
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
                self.evaluate_frame_value(ValuePick::First, value, None, window, indices, orders, inputs, column, &peer_start, &peer_end, shape, outer, budget, ctes)?;
            }
            ResolvedWindowFunction::LastValue(value) => {
                self.evaluate_frame_value(ValuePick::Last, value, None, window, indices, orders, inputs, column, &peer_start, &peer_end, shape, outer, budget, ctes)?;
            }
            ResolvedWindowFunction::NthValue { value, position } => {
                self.evaluate_frame_value(ValuePick::Nth, value, Some(position), window, indices, orders, inputs, column, &peer_start, &peer_end, shape, outer, budget, ctes)?;
            }
            ResolvedWindowFunction::Aggregate { function, argument, separator } => {
                self.evaluate_frame_aggregate(*function, argument.as_ref(), separator.as_ref(), window, indices, orders, inputs, column, &peer_start, &peer_end, shape, outer, budget, ctes)?;
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
        EvalContext {
            engine: self,
            outer,
            current: shape,
            aggregates: &inputs[index].aggregates,
            windows: &[],
            window_row: 0,
            budget,
            ctes,
        }
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
        shape: &ScopeShape,
        outer: &[Scope],
        budget: &Budget,
        ctes: Option<&CteEnv>,
    ) -> Result<(), SqlError> {
        let len = indices.len();
        for slot in 0..len {
            let ctx = self.window_context(inputs, indices[slot], shape, outer, budget, ctes);
            let (start, end) = self.frame_bounds(window, slot, len, indices, orders, peer_start, peer_end, &ctx)?;
            let wanted = match pick {
                ValuePick::First => start,
                ValuePick::Last => end,
                ValuePick::Nth => {
                    let position = position.expect("nth_value carries a position");
                    match nth_position(&position.eval(&inputs[indices[slot]].values, &ctx)?)? {
                        0 => unreachable!("nth_position rejects zero"),
                        nth => start.saturating_add(nth - 1),
                    }
                }
            };
            if wanted < start || wanted > end || wanted < 0 || wanted as usize >= len {
                column[indices[slot]] = Cell::Null;
                continue;
            }
            let target_ctx = self.window_context(inputs, indices[wanted as usize], shape, outer, budget, ctes);
            column[indices[slot]] = value.eval(&inputs[indices[wanted as usize]].values, &target_ctx)?;
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
        window: &ResolvedWindow,
        indices: &[usize],
        orders: &[Vec<Cell>],
        inputs: &[WindowRow],
        column: &mut [Cell],
        peer_start: &[usize],
        peer_end: &[usize],
        shape: &ScopeShape,
        outer: &[Scope],
        budget: &Budget,
        ctes: Option<&CteEnv>,
    ) -> Result<(), SqlError> {
        let len = indices.len();
        for slot in 0..len {
            let ctx = self.window_context(inputs, indices[slot], shape, outer, budget, ctes);
            let (start, end) = self.frame_bounds(window, slot, len, indices, orders, peer_start, peer_end, &ctx)?;
            let mut values = Vec::new();
            let mut separators = Vec::new();
            for position in start.max(0)..=end.min(len as i64 - 1) {
                let row = &inputs[indices[position as usize]];
                let row_ctx = self.window_context(inputs, indices[position as usize], shape, outer, budget, ctes);
                match argument {
                    Some(argument) => values.push(argument.eval(&row.values, &row_ctx)?),
                    None => values.push(Cell::Null),
                }
                if let Some(separator) = separator {
                    separators.push(separator.eval(&row.values, &row_ctx)?);
                }
            }
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
        _ctx: &EvalContext,
    ) -> Result<(i64, i64), SqlError> {
        let Some(frame) = &window.frame else {
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
        let descending = window.order.first().is_some_and(|(_, descending)| *descending);
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
            let mut error = SqlError::new("invalid-frame", "RANGE with offset PRECEDING/FOLLOWING requires one ORDER BY expression").in_statement("select");
            error.clause = Some("RANGE".to_string());
            return Err(error);
        }
        // A non-numeric current value can only match its own peers; unbounded
        // edges still reach the edges of the partition.
        let Some(current) = coerced else {
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
        let mut first = None;
        let mut last = None;
        for position in 0..len {
            let Some(value) = orders[indices[position]].first().and_then(numeric) else { continue };
            let value = number_f64(value);
            if value >= low && value <= high {
                first.get_or_insert(position);
                last = Some(position);
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
            let bound = pushdown
                .as_ref()
                .filter(|(name, _)| name.eq_ignore_ascii_case(&entry.name))
                .map(|(_, bound)| *bound);
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
        let Statement::Select { first, compounds, .. } = entry.query.as_ref() else {
            return Ok(None);
        };
        let name = entry.name.as_str();
        if core_references(first, name) {
            return Err(circular_reference(name));
        }
        let Some(start) = compounds.iter().position(|term| core_references(&term.core, name)) else {
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
        let Statement::Select { with: inner_with, first, compounds, order, limit, .. } = entry.query.as_ref() else {
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
                    .find_map(|(index, descending)| {
                        let ordering = compare_cells(&left[*index], &right[*index]);
                        (ordering != Ordering::Equal).then(|| if *descending { ordering.reverse() } else { ordering })
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
        let Some((offset, Some(count))) = self.eval_limit(limit, outer, budget, parent)? else {
            return Ok(None);
        };
        Ok(Some((source.name.to_ascii_lowercase(), offset.saturating_add(count))))
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
    ) -> Result<(SourceColumns<'b>, Vec<Vec<Cell>>), SqlError> {
        if let Some(query) = &table.query {
            let result = self.select_statement(query, &[], budget, ctes)?;
            let names = unique_output_names(&result.columns);
            return Ok((SourceColumns::Materialized { names, rowid: false }, result.rows));
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
        let Some(scope) = scope else {
            let qualifier = table.schema.as_deref().unwrap_or_default();
            let index = match self.attached_lookup.get(&qualifier.to_ascii_lowercase()).copied() {
                Some(index) => index,
                None => {
                    return Err(SqlError::new("unknown-table", format!("no such table: {qualifier}.{}", table.name))
                        .in_statement("select"));
                }
            };
            return self.attached_source(index, qualifier, table, budget);
        };
        if table.schema.is_none() {
            if let Some(materialized) = ctes.and_then(|env| env.lookup(&table.name)) {
                return Ok((
                    SourceColumns::Materialized { names: materialized.columns.clone(), rowid: false },
                    materialized.rows.clone(),
                ));
            }
        }
        if let Some(system) = system_for_scope(scope, &table.name) {
            let (columns, rows, rowid) = self.system_rows(system, &table.name)?;
            return Ok((SourceColumns::Materialized { names: columns, rowid }, rows));
        }
        let key = table.name.to_ascii_lowercase();
        if scope != TableScope::Main {
            if let Some(view) = self.temp_views.get(&key) {
                let _depth = ViewDepthGuard::enter()?;
                let (names, rows) = self.expand_view(&view.def, budget)?;
                return Ok((SourceColumns::Materialized { names, rowid: false }, rows));
            }
        }
        if scope != TableScope::Temp {
            if let Some(view) = self.views.get(&key) {
                // A view re-runs its stored query against the current contents
                // of every base table, exactly like SQLite. Views are not
                // correlated, so they see no outer row or CTE from this query.
                let _depth = ViewDepthGuard::enter()?;
                let (names, rows) = self.expand_view(view, budget)?;
                return Ok((SourceColumns::Materialized { names, rowid: false }, rows));
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
                return Ok((SourceColumns::Table(def), decoded));
            }
        }
        let position = if scope == TableScope::Temp { None } else { self.table_lookup.get(&key).copied() };
        if let Some(position) = position {
            let def = &self.tables[position];
            let stored = self
                .writer
                .table_rows(&def.name, TableLimits::default())
                .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("select"))?;
            let decoded = stored
                .iter()
                .map(|row| {
                    let mut cells = decode_row(&row.payload, def, row.rowid)?;
                    // Rowid tables expose the implicit integer primary key
                    // through `rowid`, `_rowid_` and `oid`.
                    cells.push(Cell::Integer(row.rowid));
                    Ok(cells)
                })
                .collect::<Result<Vec<Vec<Cell>>, SqlError>>()?;
            return Ok((SourceColumns::Table(def), decoded));
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
    ) -> Result<(SourceColumns<'b>, Vec<Vec<Cell>>), SqlError> {
        let engine = &self.attached[index].engine;
        if let Some(system) = SystemTable::from_name(&table.name) {
            let (columns, rows, rowid) = engine.system_rows(system, &table.name)?;
            return Ok((SourceColumns::Materialized { names: columns, rowid }, rows));
        }
        let key = table.name.to_ascii_lowercase();
        if let Some(view) = engine.temp_views.get(&key) {
            let _depth = ViewDepthGuard::enter()?;
            let (names, rows) = engine.expand_view(&view.def, budget)?;
            return Ok((SourceColumns::Materialized { names, rowid: false }, rows));
        }
        if let Some(view) = engine.views.get(&key) {
            let _depth = ViewDepthGuard::enter()?;
            let (names, rows) = engine.expand_view(view, budget)?;
            return Ok((SourceColumns::Materialized { names, rowid: false }, rows));
        }
        if let Some(position) = engine.table_lookup.get(&key).copied() {
            let def = &engine.tables[position];
            let stored = engine
                .writer
                .table_rows(&def.name, TableLimits::default())
                .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("select"))?;
            let decoded = stored
                .iter()
                .map(|row| {
                    let mut cells = decode_row(&row.payload, def, row.rowid)?;
                    cells.push(Cell::Integer(row.rowid));
                    Ok(cells)
                })
                .collect::<Result<Vec<Vec<Cell>>, SqlError>>()?;
            return Ok((SourceColumns::Table(def), decoded));
        }
        Err(SqlError::new("unknown-table", format!("no such table: {qualifier}.{}", table.name)).in_statement("select"))
    }

    fn select_statement(
        &self,
        statement: &Statement,
        outer: &[Scope],
        budget: &Budget,
        ctes: Option<&CteEnv>,
    ) -> Result<ExecResult, SqlError> {
        match statement {
            Statement::Select { with, recursive, first, compounds, order, limit } => {
                self.run_select(with, *recursive, first, compounds, order.clone(), limit.clone(), outer, budget, ctes)
            }
            other => Err(SqlError::new(
                "unsupported-statement",
                format!("a subquery must be a `SELECT` but found `{}`", other.name()),
            )
            .in_statement("select")),
        }
    }
}

/// Resolves the trailing `ORDER BY` of a compound result.
///
/// SQLite only lets a compound query order by an output column, either by
/// 1-based ordinal or by its result label; source columns are out of scope
/// once the cores have been combined.
fn compound_order_keys(order: &[OrderKey], columns: &[String]) -> Result<Vec<(usize, bool)>, SqlError> {
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
                return Ok((position as usize - 1, entry.descending));
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
                        [index] => Ok((*index, entry.descending)),
                        [] => Err(SqlError::new(
                            "unknown-column",
                            format!("ORDER BY term `{}` does not match any column in the result set", reference.name),
                        )
                        .in_statement("select")),
                        _ => Err(SqlError::new(
                            "ambiguous-column",
                            format!("ORDER BY term `{}` is ambiguous in the result set", reference.name),
                        )
                        .in_statement("select")),
                    };
                }
            }
            let mut error =
                SqlError::new("unsupported-clause", "a compound `SELECT` can only order by an output column name or ordinal").in_statement("select");
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
        self.tables
            .get(&name.to_ascii_lowercase())
            .or_else(|| self.parent.and_then(|parent| parent.lookup(name)))
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
        ScopeShape {
            columns: self.columns.iter().map(|column| (column.label.clone(), column.name.clone())).collect(),
        }
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
    let mut kept = Vec::new();
    for row in rows {
        if !rows_contain(&kept, row) {
            kept.push(row.clone());
        }
    }
    kept
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
        Expr::IsNull { value, .. } | Expr::IsTruth { value, .. } | Expr::Not(value) => row_preserving_expr(value),
        Expr::Case { operand, branches, otherwise } => {
            operand.as_ref().map_or(true, |expr| row_preserving_expr(expr))
                && branches.iter().all(|(condition, result)| row_preserving_expr(condition) && row_preserving_expr(result))
                && otherwise.as_ref().map_or(true, |expr| row_preserving_expr(expr))
        }
        Expr::In { value, list, .. } => row_preserving_expr(value) && list.iter().all(row_preserving_expr),
        Expr::Function { args, .. } => args.iter().all(row_preserving_expr),
        Expr::Alias { value, .. } | Expr::Cast { value, .. } => row_preserving_expr(value),
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
    /// unnamed slot holding the rowid. Derived tables and CTEs have no rowid,
    /// while `sqlite_master` and `sqlite_sequence` do.
    fn hidden_rowid(&self) -> bool {
        match self {
            Self::Table(_) => true,
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
        if let Some(local) = (0..self.visible_width()).find(|local| {
            !self.unqualified_hidden[*local] && self.columns.name_at(*local).eq_ignore_ascii_case(name)
        }) {
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
            let at = self.sources.iter().position(|source| source.label.eq_ignore_ascii_case(qualifier)).ok_or_else(|| {
                SqlError::new("unknown-table", format!("unknown table `{qualifier}` in the `FROM` clause")).in_statement("select")
            })?;
            let source = &self.sources[at];
            let index = source.index(&reference.name).ok_or_else(|| {
                SqlError::new(
                    "unknown-column",
                    format!("unknown column `{}` in table `{}`", reference.display(), source.label),
                )
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
        if local < self.sources[source].visible_width() {
            Some(self.sources[source].columns.name_at(local).to_string())
        }
        else {
            None
        }
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
                        SqlError::new(
                            "unknown-column",
                            format!("cannot join using column {name} - column not present in both tables"),
                        )
                        .in_statement("select")
                    })?;
                let left = match self.using_operand(&name, index) {
                    Some(operand) => operand,
                    None => {
                        return Err(SqlError::new(
                            "unknown-column",
                            format!("cannot join using column {name} - column not present in both tables"),
                        )
                        .in_statement("select"))
                    }
                };
                let (left_index, label, owner, source, matched) = match left {
                    UsingOperand::Slot(position) => {
                        let slot = &self.using_slots[position];
                        let (label, owner, source, matched) = (slot.label.clone(), slot.owner, slot.source, slot.name.clone());
                        self.using_slots[position].nameable = false;
                        if let Some((owner_source, owner_local)) = owner {
                            self.sources[owner_source].star_override[owner_local] =
                                Some(self.base_width() + self.using_slots.len());
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
}

/// A projection / `WHERE` / `ORDER BY` expression with every column resolved
/// to a merged row index.
#[derive(Clone)]
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
    Compare { left: Box<ResolvedExpr>, op: CompareOp, right: Box<ResolvedExpr> },
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
        /// `WHEN condition THEN result` pairs.
        branches: Vec<(ResolvedExpr, ResolvedExpr)>,
        /// `ELSE result`; absent means `NULL`.
        otherwise: Option<Box<ResolvedExpr>>,
    },
    /// `x IN (...)` or `x NOT IN (...)`.
    In { value: Box<ResolvedExpr>, list: Vec<ResolvedExpr>, negated: bool },
    /// Scalar function call.
    Function { name: ScalarFunction, args: Vec<ResolvedExpr> },
    /// `CAST(expr AS type)`.
    Cast { value: Box<ResolvedExpr>, target: CastType },
    /// `x [NOT] LIKE y [ESCAPE z]`.
    Like { value: Box<ResolvedExpr>, pattern: Box<ResolvedExpr>, negated: bool, escape: Option<Box<ResolvedExpr>> },
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
    order: Vec<(ResolvedExpr, bool)>,
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
    Aggregate { function: AggregateFunction, argument: Option<ResolvedExpr>, separator: Option<ResolvedExpr> },
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
    /// `ROWS` or `RANGE`.
    units: FrameUnits,
    /// Starting edge.
    start: ResolvedFrameBound,
    /// Ending edge.
    end: ResolvedFrameBound,
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
            Self::OuterColumn { depth, index } => ctx.outer[*depth].columns[*index].value.clone(),
            Self::AggregateRef(index) => ctx.aggregates[*index].clone(),
            Self::Window(index) => ctx
                .windows
                .get(*index)
                .and_then(|column| column.get(ctx.window_row))
                .cloned()
                .unwrap_or(Cell::Null),
            Self::Unary { op, value } => {
                let value = value.eval(values, ctx)?;
                match op {
                    UnaryOp::Negate => negate_cell(&value),
                    UnaryOp::Plus => value,
                    UnaryOp::BitNot => bit_not(&value),
                }
            }
            Self::Binary { left, op: BinaryOp::Concat, right } => concat(&left.eval(values, ctx)?, &right.eval(values, ctx)?),
            Self::Binary { left, op, right } => arithmetic(*op, &left.eval(values, ctx)?, &right.eval(values, ctx)?),
            Self::Compare { left, op, right } => match compare_values(&left.eval(values, ctx)?, &right.eval(values, ctx)?, *op) {
                Some(true) => Cell::Integer(1),
                Some(false) => Cell::Integer(0),
                None => Cell::Null,
            },
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
            Self::Case { operand, branches, otherwise } => {
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
                            if compare_values(&base, &candidate.eval(values, ctx)?, CompareOp::Eq) == Some(true) {
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
            Self::In { value, list, negated } => {
                let value = value.eval(values, ctx)?;
                let list = list.iter().map(|item| item.eval(values, ctx)).collect::<Result<Vec<_>, _>>()?;
                in_result(&value, &list, *negated)
            }
            Self::Function { name, args } => {
                let values = args.iter().map(|arg| arg.eval(values, ctx)).collect::<Result<Vec<_>, _>>()?;
                eval_function(*name, &values)
            }
            Self::Cast { value, target } => cast_cell(&value.eval(values, ctx)?, *target),
            Self::Like { value, pattern, negated, escape } => {
                let value = value.eval(values, ctx)?;
                let pattern = pattern.eval(values, ctx)?;
                let escape = match escape {
                    Some(escape) => {
                        let text = text_form(&escape.eval(values, ctx)?);
                        match text.as_deref().map(|text| text.chars().collect::<Vec<_>>()) {
                            Some(chars) if chars.len() == 1 => Some(chars[0]),
                            Some(_) => {
                                return Err(SqlError::new("invalid-escape", "ESCAPE expression must be a single character").in_statement("select"));
                            }
                            None => None,
                        }
                    }
                    None => None,
                };
                // `LIKE` with a NULL value, pattern or escape yields NULL.
                let (Some(value), Some(pattern)) = (text_form(&value), text_form(&pattern)) else {
                    return Ok(Cell::Null);
                };
                let characters: Vec<char> = value.chars().collect();
                let pattern: Vec<char> = pattern.chars().collect();
                let matched = like_match(&characters, &pattern, escape);
                Cell::Integer(i64::from(matched != *negated))
            }
            Self::Subquery(query) => {
                let result = run_subquery(query, values, ctx)?;
                check_single_column(&result)?;
                result.rows.first().map_or(Cell::Null, |row| row[0].clone())
            }
            Self::Exists(query) => {
                // Existence is two-valued: no result value is consumed, so a
                // surrounding `NOT` can never turn this into `NULL`.
                let result = run_subquery(query, values, ctx)?;
                Cell::Integer(i64::from(!result.rows.is_empty()))
            }
            Self::InSubquery { value, query, negated } => {
                let value = value.eval(values, ctx)?;
                let result = run_subquery(query, values, ctx)?;
                check_single_column(&result)?;
                let list = result.rows.iter().filter_map(|row| row.first().cloned()).collect::<Vec<_>>();
                in_result(&value, &list, *negated)
            }
        })
    }

    /// Appends the source indices this expression reads.
    fn collect_sources(&self, out: &mut Vec<usize>) {
        match self {
            Self::Cell(_) | Self::OuterColumn { .. } | Self::AggregateRef(_) | Self::TriggerColumn { .. } => {}
            // A subquery's own scope is resolved when it runs, so it cannot be
            // attributed to a single source here; `contains_subquery` makes
            // such a conjunct wait for the complete row instead.
            Self::Subquery(_) | Self::Exists(_) => {}
            Self::Column { source, .. } => out.push(*source),
            Self::Unary { value, .. } | Self::Not(value) | Self::IsNull { value, .. } | Self::IsTruth { value, .. } => {
                value.collect_sources(out)
            }
            Self::Binary { left, right, .. } | Self::Compare { left, right, .. } | Self::And(left, right) | Self::Or(left, right) => {
                left.collect_sources(out);
                right.collect_sources(out);
            }
            Self::Case { operand, branches, otherwise } => {
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
            Self::Function { args, .. } => {
                for arg in args {
                    arg.collect_sources(out);
                }
            }
            Self::Cast { value, .. } => value.collect_sources(out),
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
            Self::Unary { value, .. } | Self::Not(value) | Self::IsNull { value, .. } | Self::IsTruth { value, .. } => {
                value.contains_subquery()
            }
            Self::Binary { left, right, .. } | Self::Compare { left, right, .. } | Self::And(left, right) | Self::Or(left, right) => {
                left.contains_subquery() || right.contains_subquery()
            }
            Self::Case { operand, branches, otherwise } => {
                operand.as_ref().is_some_and(|operand| operand.contains_subquery())
                    || branches.iter().any(|(condition, result)| condition.contains_subquery() || result.contains_subquery())
                    || otherwise.as_ref().is_some_and(|otherwise| otherwise.contains_subquery())
            }
            Self::In { value, list, .. } => value.contains_subquery() || list.iter().any(ResolvedExpr::contains_subquery),
            Self::Function { args, .. } => args.iter().any(ResolvedExpr::contains_subquery),
            Self::Cast { value, .. } => value.contains_subquery(),
            Self::Like { value, pattern, escape, .. } => {
                value.contains_subquery()
                    || pattern.contains_subquery()
                    || escape.as_ref().is_some_and(|escape| escape.contains_subquery())
            }
            Self::Cell(_) | Self::Parameter(_) | Self::Column { .. } | Self::OuterColumn { .. } | Self::AggregateRef(_) | Self::Window(_) | Self::TriggerColumn { .. } => false,
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

/// One `GROUP BY` term.
enum GroupKey {
    /// A one-based projection ordinal.
    Projected(usize),
    /// Expression over the merged source row.
    Expression(ResolvedExpr),
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
        Expr::Not(inner) => ResolvedExpr::Not(Box::new(resolve_expr(
            inner,
            relation,
            outer,
            named,
            windows,
            aggregates,
            allow_aggregate,
            allow_window,
        )?)),
        Expr::And(left, right) => ResolvedExpr::And(
            Box::new(resolve_expr(left, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            Box::new(resolve_expr(right, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
        ),
        Expr::Or(left, right) => ResolvedExpr::Or(
            Box::new(resolve_expr(left, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            Box::new(resolve_expr(right, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
        ),
        Expr::Case { operand, branches, otherwise } => ResolvedExpr::Case {
            operand: operand
                .as_ref()
                .map(|inner| {
                    Ok::<_, SqlError>(Box::new(resolve_expr(
                        inner, relation, outer, named, windows, aggregates, allow_aggregate, allow_window,
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
                        inner, relation, outer, named, windows, aggregates, allow_aggregate, allow_window,
                    )?))
                })
                .transpose()?,
        },
        Expr::In { value, list, negated } => ResolvedExpr::In {
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
        Expr::Cast { value, target } => ResolvedExpr::Cast {
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            target: *target,
        },
        Expr::Like { value, pattern, negated, escape } => ResolvedExpr::Like {
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            pattern: Box::new(resolve_expr(pattern, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            negated: *negated,
            escape: match escape {
                Some(escape) => Some(Box::new(resolve_expr(
                    escape, relation, outer, named, windows, aggregates, allow_aggregate, allow_window,
                )?)),
                None => None,
            },
        },
        Expr::Subquery(query) => ResolvedExpr::Subquery(query.clone()),
        Expr::Exists(query) => ResolvedExpr::Exists(query.clone()),
        Expr::InSubquery { value, query, negated } => ResolvedExpr::InSubquery {
            value: Box::new(resolve_expr(value, relation, outer, named, windows, aggregates, allow_aggregate, allow_window)?),
            query: query.clone(),
            negated: *negated,
        },
        Expr::Aggregate { function, argument, separator } => {
            if !allow_aggregate {
                let mut error =
                    SqlError::new("misuse-of-aggregate", format!("misuse of aggregate function {}()", function.name())).in_statement("select");
                error.clause = Some(function.name().to_ascii_uppercase());
                return Err(error);
            }
            // An aggregate argument is evaluated per row, so it may not contain
            // another aggregate; subqueries inside it are still allowed.
            let argument = argument
                .as_ref()
                .map(|arg| resolve_expr(arg, relation, outer, named, windows, aggregates, false, false))
                .transpose()?;
            let separator = separator
                .as_ref()
                .map(|sep| resolve_expr(sep, relation, outer, named, windows, aggregates, false, false))
                .transpose()?;
            let index = aggregates.len();
            aggregates.push(AggregateSpec { function: *function, argument, separator });
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
            Ok((
                resolve_expr(&key.expr, relation, outer, named, windows, aggregates, true, false)?,
                key.descending,
            ))
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
            let definition = named
                .iter()
                .rev()
                .find(|window| window.name.eq_ignore_ascii_case(base))
                .ok_or_else(|| {
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
                    return Err(SqlError::new("invalid-frame", format!("frame {which} offset must be a non-negative integer")).in_statement("select"));
                }
            },
            None => None,
        };
        Ok(ResolvedFrameBound { edge: bound.edge, offset })
    };
    let start = resolve_bound(&frame.start, "starting")?;
    let end = resolve_bound(&frame.end, "ending")?;
    Ok(ResolvedFrame { units: frame.units, start, end })
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
        WindowFunction::Aggregate { function, argument: value, separator } => ResolvedWindowFunction::Aggregate {
            function: *function,
            argument: value.as_ref().map(|expr| argument(expr)).transpose()?,
            separator: separator.as_ref().map(|expr| argument(expr)).transpose()?,
        },
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
                return Err(SqlError::new(
                    "ambiguous-column",
                    format!("ambiguous column `{}` in an enclosing `SELECT`", reference.display()),
                )
                .in_statement("select"))
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
        return Err(SqlError::new(
            "arity-mismatch",
            format!("sub-select returns {} columns - expected 1", result.columns.len()),
        )
        .in_statement("select"));
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
fn compare_window_rows(left_partition: &[Cell], left_order: &[Cell], right_partition: &[Cell], right_order: &[Cell], order: &[(ResolvedExpr, bool)]) -> Ordering {
    for (left, right) in left_partition.iter().zip(right_partition) {
        let ordering = compare_cells(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    for (index, (_, descending)) in order.iter().enumerate() {
        let ordering = compare_cells(&left_order[index], &right_order[index]);
        let ordering = if *descending { ordering.reverse() } else { ordering };
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
        FrameEdge::UnboundedPreceding => if descending { f64::INFINITY } else { f64::NEG_INFINITY },
        FrameEdge::UnboundedFollowing => if descending { f64::NEG_INFINITY } else { f64::INFINITY },
        FrameEdge::CurrentRow => current,
        FrameEdge::Preceding => if descending { current + offset } else { current - offset },
        FrameEdge::Following => if descending { current - offset } else { current + offset },
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
        FrameEdge::Following => position
            .saturating_add(range_offset(bound, which)?.unwrap_or(0))
            .min(len.saturating_sub(1) as i64),
    })
}

/// Bucket count of `ntile(n)`; SQLite rejects zero and negative counts.
fn ntile_buckets(cell: &Cell) -> Result<usize, SqlError> {
    match numeric(cell) {
        Some(Number::Int(value)) if value > 0 => Ok(value as usize),
        Some(Number::Real(value)) if value.is_finite() && value.fract() == 0.0 && value > 0.0 && value <= usize::MAX as f64 => Ok(value as usize),
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
        Number::Real(value) if value.is_finite() && value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 => Some(value as i64),
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
fn retain_first_distinct(rows: Vec<Vec<Cell>>) -> Vec<Vec<Cell>> {
    let mut seen: Vec<Vec<Cell>> = Vec::with_capacity(rows.len());
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if !seen.contains(&row) {
            seen.push(row.clone());
            out.push(row);
        }
    }
    out
}

/// Computes one whole-result aggregate over the filtered rows.
///
/// Empty input follows SQLite: `count` is `0`, every other aggregate is `NULL`.
fn compute_aggregate(spec: &AggregateSpec, rows: &[Vec<Cell>], ctx: &EvalContext) -> Result<Cell, SqlError> {
    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        values.push(match &spec.argument {
            Some(argument) => argument.eval(row, ctx)?,
            None => Cell::Null,
        });
    }
    let separators = match &spec.separator {
        Some(separator) => {
            let mut cells = Vec::with_capacity(rows.len());
            for row in rows {
                cells.push(separator.eval(row, ctx)?);
            }
            Some(cells)
        }
        None => None,
    };
    Ok(aggregate_cells(spec.function, &values, separators.as_deref()))
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
                let Some(number) = numeric(cell) else { continue };
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
                let Some(value) = text_form(value) else { continue };
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
    let mut groups: Vec<(Vec<Cell>, Vec<Vec<Cell>>)> = Vec::new();
    for row in rows {
        let key = group_keys
            .iter()
            .map(|key| match key {
                GroupKey::Projected(index) => match projection {
                    Some(nodes) => match nodes.get(*index) {
                        Some(node) => node.eval(&row, ctx),
                        None => Ok(Cell::Null),
                    },
                    None => Ok(row.get(*index).cloned().unwrap_or(Cell::Null)),
                },
                GroupKey::Expression(expr) => expr.eval(&row, ctx),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let bucket = groups
            .iter_mut()
            .find(|(existing, _)| existing.len() == key.len() && existing.iter().zip(&key).all(|(left, right)| compare_cells(left, right) == Ordering::Equal));
        match bucket {
            Some((_, rows)) => {
                budget.charge(1)?;
                rows.push(row);
            }
            None => {
                budget.charge(1)?;
                groups.push((key, vec![row]));
            }
        }
    }
    Ok(groups.into_iter().map(|(_, rows)| rows).collect())
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
    tables: &[Vec<Vec<Cell>>],
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
        if unconditional {
            for left in &rows {
                for values in &tables[index] {
                    let mut merged = left.clone();
                    merged[source.offset..source.offset + width].clone_from_slice(values);
                    ctx.budget.charge(1)?;
                    next.push(merged);
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
                        None => pairs.iter().all(|pair| compare_values(&merged[pair.left], &merged[pair.right], CompareOp::Eq) == Some(true)),
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
                .filter(|sources| {
                    !sources.is_empty() && sources.iter().all(|source| *source == candidate || bound[*source])
                })
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
fn cross_product(
    tables: &[Vec<Vec<Cell>>],
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
    emit_rows(0, tables, relation, &order, conjuncts, &depths, take, ctx, &mut merged, &mut rows)?;
    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
fn emit_rows(
    slot: usize,
    tables: &[Vec<Vec<Cell>>],
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
    for values in &tables[order[slot]] {
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
/// compares with the `BINARY` collation, and blobs compare byte by byte.
fn compare_cells(left: &Cell, right: &Cell) -> Ordering {
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
        (Cell::Text(left), Cell::Text(right)) => left.as_bytes().cmp(right.as_bytes()),
        (Cell::Blob(left), Cell::Blob(right)) => left.cmp(right),
        _ => Equal,
    }
}

/// Applies one comparison operator. `None` means the result is SQL `NULL`.
fn compare_values(left: &Cell, right: &Cell, operator: CompareOp) -> Option<bool> {
    use Ordering::{Equal, Greater, Less};
    if matches!(left, Cell::Null) || matches!(right, Cell::Null) {
        return None;
    }
    let ordering = compare_cells(left, right);
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

fn compare_key_rows(left: &[Cell], right: &[Cell], keys: &[(SortKey, bool)]) -> Ordering {
    for (index, (_, descending)) in keys.iter().enumerate() {
        let ordering = compare_cells(&left[index], &right[index]);
        let ordering = if *descending { ordering.reverse() } else { ordering };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

/// Compares two composite window sort keys, honouring each column's direction.
fn compare_ordered(left: &[Cell], right: &[Cell], descending: &[bool]) -> Ordering {
    for (index, descending) in descending.iter().enumerate() {
        let ordering = compare_cells(&left[index], &right[index]);
        let ordering = if *descending { ordering.reverse() } else { ordering };
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
        Number::Real(value)
            if value.is_finite() && value.fract() == 0.0 && (i64::MIN as f64..=i64::MAX as f64).contains(&value) =>
        {
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

/// `LIKE` folds case only for ASCII letters; every other character compares
/// exactly, which is why `Ü` never matches `ü`.
fn like_eq(left: char, right: char) -> bool {
    if left.is_ascii() && right.is_ascii() {
        left.eq_ignore_ascii_case(&right)
    }
    else {
        left == right
    }
}

/// Matches one `LIKE` pattern against a value, one character at a time.
///
/// `%` matches any run including the empty one, `_` matches exactly one
/// character, and the `ESCAPE` character turns the next pattern character into
/// a literal.
fn like_match(value: &[char], pattern: &[char], escape: Option<char>) -> bool {
    let Some((first, rest)) = pattern.split_first() else {
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
    let Some((candidate, tail)) = value.split_first() else {
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
    let (Some(left), Some(right)) = (numeric(left), numeric(right)) else {
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
        },
        // `||` never reaches arithmetic: it is evaluated by its own branch.
        BinaryOp::Concat => Cell::Null,
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
        if magnitude >= 64 {
            0
        }
        else {
            ((value as u64) << magnitude) as i64
        }
    }
    else if magnitude >= 64 {
        if value < 0 {
            -1
        }
        else {
            0
        }
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
        else if compare_values(value, item, CompareOp::Eq) == Some(true) {
            return Cell::Integer(i64::from(!negated));
        }
    }
    if saw_null { Cell::Null } else { Cell::Integer(i64::from(negated)) }
}

fn eval_function(name: ScalarFunction, args: &[Cell]) -> Cell {
    match name {
        ScalarFunction::Abs => match numeric(&args[0]) {
            None => Cell::Null,
            Some(Number::Int(value)) => match value.checked_abs() {
                Some(value) => Cell::Integer(value),
                None => Cell::Real(-(value as f64)),
            },
            Some(Number::Real(value)) => Cell::Real(value.abs()),
        },
        ScalarFunction::Coalesce => {
            args.iter().find(|cell| !matches!(cell, Cell::Null)).cloned().unwrap_or(Cell::Null)
        }
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
    }
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
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
    let begin = if start > 0 { start - 1 } else if start == 0 { -1 } else { length + start };
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
    let tie = fraction.len() > digits
        && fraction.as_bytes()[digits] == b'5'
        && fraction[digits + 1..].bytes().all(|byte| byte == b'0');
    let text = if tie { increment_decimal(&short) } else { short };
    let rounded: f64 = text.parse().unwrap_or(magnitude);
    if value < 0.0 {
        -rounded
    }
    else {
        rounded
    }
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
            },
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
                    return Err(SqlError::new("duplicate-column", format!("column `{name}` is listed twice in the insert")).in_statement("insert"));
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
        checks: Vec::new(),
        foreign_keys: Vec::new(),
        strict: false,
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
    let relation = Relation { sources: vec![source], using: vec![Vec::new()], using_slots: Vec::new(), trigger: None };
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
        });
    }
    Ok(resolved)
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
        Expr::IsNull { value, .. } | Expr::IsTruth { value, .. } | Expr::Not(value) | Expr::Alias { value, .. } | Expr::Cast { value, .. } => {
            expression_mentions_column(value, column, qualifiers)
        }
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
        Expr::Function { args, .. } => args.iter().any(|argument| expression_mentions_column(argument, column, qualifiers)),
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
        || core.from.iter().any(|source| {
            source.query.as_ref().is_some_and(|query| statement_mentions_column(query, column, qualifiers))
        })
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
        Statement::Insert { returning, .. } => {
            returning.as_ref().is_some_and(|items| select_items_mention_column(items, column, qualifiers))
        }
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
    SqlError::new(
        "SQLITE_ERROR",
        format!("foreign key mismatch - \"{}\" referencing \"{}\"", child.name, key.parent_display),
    )
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
    Some(SqlError::new(
        "constraint-datatype",
        format!("cannot store {stored} value in {declared} column {}.{}", def.name, column.name),
    )
    .in_statement(statement))
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
            return Err(SqlError::new("constraint-failed", format!("NOT NULL constraint failed: {}.{}", def.name, column.name))
                .in_statement("insert"));
        }
    }
    let live: Vec<(i64, Vec<Cell>)> = rows.iter().map(|(rowid, cells)| (*rowid, cells.clone())).collect();
    if let Some(label) = unique_conflict(keys, &cells, rowid, &live, i64::MIN) {
        return Err(SqlError::new("unique-constraint", format!("UNIQUE constraint failed: {label}")).in_statement("insert"));
    }
    rows.insert(rowid, cells);
    Ok(rowid)
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
            let collides = wanted.iter().zip(&other).all(|(first, second)| {
                !matches!(first, Cell::Null) && !matches!(second, Cell::Null) && compare_cells(first, second) == Ordering::Equal
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
fn write_failure(error: impl std::fmt::Display, statement: &'static str, def: &TableDef) -> SqlError {
    let message = error.to_string();
    if message.contains("UNIQUE constraint failed") {
        SqlError::new("unique-constraint", format!("UNIQUE constraint failed in table `{}`", def.name))
    }
    else if message.contains("NOT NULL") {
        SqlError::new("constraint-failed", format!("NOT NULL constraint failed in table `{}`", def.name))
    }
    else if message.contains("integer") || message.contains("datatype") {
        SqlError::new("datatype-mismatch", "datatype mismatch")
    }
    else {
        SqlError::new("storage", message)
    }
    .in_statement(statement)
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
    let mut cells = decode_cells(payload)?;
    // A record written before `ALTER TABLE ADD COLUMN` is short; SQLite reads
    // the missing trailing columns as their declared default.
    while cells.len() < def.columns.len() {
        let column = &def.columns[cells.len()];
        let value = column.default_value.clone().unwrap_or(crate::WriteValue::Null);
        cells.push(write_value_cell(&crate::apply_write_affinity(&column.declared_type, value)));
    }
    cells.truncate(def.columns.len());
    if let Some(index) = def.rowid_alias {
        if matches!(cells[index], Cell::Null) {
            cells[index] = Cell::Integer(rowid);
        }
    }
    Ok(cells)
}
/// Decodes a raw record into cells without expanding an implicit rowid alias.
fn decode_cells(payload: &[u8]) -> Result<Vec<Cell>, SqlError> {
    let limits = RecordLimits { max_columns: 1024, max_payload_bytes: 16 * 1024 * 1024 };
    let values = decode_record(payload, TextEncoding::Utf8, limits)
        .map_err(|error| SqlError::new("storage", error.to_string()).in_statement("select"))?;
    Ok(values
        .iter()
        .map(|value| match value {
            crate::RecordValue::Null => Cell::Null,
            crate::RecordValue::Integer(value) => Cell::Integer(*value),
            crate::RecordValue::Real(value) => Cell::Real(*value),
            crate::RecordValue::Text(value) => Cell::Text(value.clone()),
            crate::RecordValue::Blob(value) => Cell::Blob(value.to_vec()),
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let image = engine.snapshot().to_vec();
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
        assert_eq!(reopened.snapshot(), image.as_slice());
        std::fs::remove_file(&path).expect("cleanup image");
    }

    /// Two-column table with one all-`NULL` row, inserted in rowid order.
    fn engine() -> Engine {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(a INTEGER, b INTEGER)").expect("create table");
        engine
            .execute_one("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (NULL, NULL), (5, 0)")
            .expect("insert rows");
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
        assert_eq!(
            first_column("SELECT a FROM t WHERE a BETWEEN b - 2 AND b + 2"),
            vec![Cell::Integer(1), Cell::Integer(2), Cell::Integer(3)]
        );
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
        assert_eq!(
            engine.execute_one("SELECT CAST(123 AS BLOB)").expect("integer blob").rows,
            vec![vec![Cell::Blob(b"123".to_vec())]]
        );
        // Numeric keeps an integral value as an integer.
        assert_eq!(engine.execute_one("SELECT CAST('1e3' AS NUMERIC)").expect("numeric text").rows, vec![vec![Cell::Integer(1000)]]);
        let unsupported = engine.execute_one("SELECT CAST(1 AS FANCYTYPE)").expect_err("unknown target type");
        assert_eq!(unsupported.code, "unsupported-type");
    }

    #[test]
    fn parenthesized_disjunction_keeps_both_branches() {
        assert_eq!(first_column("SELECT a FROM t WHERE (a = 1) OR (a > 3)"), vec![Cell::Integer(1), Cell::Integer(5)]);
        assert_eq!(
            first_column("SELECT a FROM t WHERE (a = 1 OR a = 2) AND b IS NOT NULL"),
            vec![Cell::Integer(1), Cell::Integer(2)]
        );
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
        let named = engine
            .execute_with_parameters("SELECT a FROM t LIMIT :n", &[Cell::Integer(2)])
            .expect("named limit");
        assert_eq!(named[0].rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]]);
        // Negative values clamp like SQLite: no upper bound, no skipped rows.
        let unlimited = engine
            .execute_with_parameters("SELECT a FROM t LIMIT ?", &[Cell::Integer(-1)])
            .expect("negative limit");
        assert_eq!(unlimited[0].rows.len(), 5);
        let offset = engine
            .execute_with_parameters("SELECT a FROM t LIMIT 2 OFFSET ?", &[Cell::Integer(-1)])
            .expect("negative offset");
        assert_eq!(offset[0].rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]]);
        let oversized = engine
            .execute_with_parameters("SELECT a FROM t LIMIT ?", &[Cell::Integer(999)])
            .expect("oversized limit");
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
            vec![
                vec![Cell::Integer(1), Cell::Integer(1)],
                vec![Cell::Integer(2), Cell::Integer(1)],
                vec![Cell::Integer(3), Cell::Integer(2)],
            ]
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
        assert_eq!(
            engine.execute_one("SELECT 1, 2 + 3").expect("two constants").rows,
            vec![vec![Cell::Integer(1), Cell::Integer(5)]]
        );
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
        assert_eq!(
            engine.execute_one("SELECT 1 UNION SELECT 2").expect("union").rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]]
        );
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
        assert_eq!(error.code, "storage");
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
        assert_eq!(error.code, "storage");
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
        assert_eq!(
            engine.execute_one("SELECT a, b FROM plain").expect("plain row").rows,
            vec![vec![Cell::Null, Cell::Null]]
        );
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
        assert_eq!(failed.code, "storage");
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
            vec![
                vec![Cell::Integer(1)],
                vec![Cell::Integer(2)],
                vec![Cell::Integer(3)],
                vec![Cell::Integer(99)]
            ]
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
        std::fs::write(&path, engine.snapshot()).expect("write image");
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
        assert_eq!(engine.execute_one("SELECT CAST(0.30000000000000004 AS TEXT)").expect("real text").rows, vec![vec![Cell::Text("0.3".to_string())]]);
        assert_eq!(engine.execute_one("SELECT CAST(1e300 AS TEXT)").expect("huge real text").rows, vec![vec![Cell::Text("1.0e+300".to_string())]]);
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
        assert_eq!(engine.execute_one("SELECT total(x), typeof(total(x)) FROM v").expect("total").rows, vec![vec![Cell::Real(13.0), Cell::Text("real".to_string())]]);
        assert_eq!(engine.execute_one("SELECT total(x), sum(x) FROM v WHERE 0").expect("empty total").rows, vec![vec![Cell::Real(0.0), Cell::Null]]);
        assert_eq!(engine.execute_one("SELECT total(NULL), total(1), total(' 12abc')").expect("total literals").rows, vec![vec![Cell::Real(0.0), Cell::Real(1.0), Cell::Real(12.0)]]);
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
        assert_eq!(engine.execute_one("SELECT length(randomblob(4)), typeof(randomblob(4))").expect("randomblob").rows, vec![vec![Cell::Integer(4), Cell::Text("blob".to_string())]]);
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
        assert_eq!(result.columns, vec!["type".to_string(), "name".to_string(), "tbl_name".to_string(), "rootpage".to_string(), "sql".to_string()]);
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
        assert_eq!(rowids.rows, vec![
            vec![Cell::Integer(1), text("a")],
            vec![Cell::Integer(2), text("ia")],
            vec![Cell::Integer(3), text("b")],
            vec![Cell::Integer(4), text("sqlite_autoindex_b_1")],
            vec![Cell::Integer(5), text("sqlite_autoindex_b_2")],
        ]);
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
        assert_eq!(master.rows, vec![vec![text("table"), text("c"), Cell::Integer(2)], vec![text("table"), text("sqlite_sequence"), Cell::Integer(3)]]);
        let empty = engine.execute_one("SELECT * FROM sqlite_sequence").expect("empty sequence");
        assert_eq!(empty.columns, vec!["name".to_string(), "seq".to_string()]);
        assert_eq!(empty.rows, Vec::<Vec<Cell>>::new());

        engine.execute_one("INSERT INTO c(v) VALUES ('a'), ('b')").expect("auto insert");
        assert_eq!(engine.execute_one("SELECT id FROM c").expect("select ids").rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]]);
        assert_eq!(engine.execute_one("SELECT name, seq FROM sqlite_sequence").expect("sequence").rows, vec![vec![text("c"), Cell::Integer(2)]]);

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
        assert_eq!(
            merged.rows,
            vec![vec![Cell::Integer(2), text("b"), text("B")], vec![Cell::Integer(3), Cell::Null, text("C")]]
        );
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
        assert_eq!(
            qualified.rows,
            vec![vec![Cell::Integer(2), Cell::Integer(2)], vec![Cell::Null, Cell::Integer(3)]]
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
        assert_eq!(
            engine.execute_one("SELECT count(*) FROM l FULL OUTER JOIN r USING(id)").expect("count").rows,
            vec![vec![Cell::Integer(3)]]
        );

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
        assert_eq!(error.code, "constraint-failed");
        assert_eq!(error.clause.as_deref(), Some("CHECK"));
        assert_eq!(engine.execute_one("SELECT id, v FROM t").expect("select").rows, before);
    }

    #[test]
    fn update_of_a_rowid_alias_runs_in_ascending_order() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").expect("create");
        engine.execute_one("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").expect("insert");
        engine.execute_one("UPDATE t SET id = id - 1").expect("shift down");
        assert_eq!(
            engine.execute_one("SELECT id, v FROM t").expect("select").rows,
            vec![
                vec![Cell::Integer(0), text("a")],
                vec![Cell::Integer(1), text("b")],
                vec![Cell::Integer(2), text("c")],
            ]
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
        assert_eq!(
            updated.rows,
            vec![vec![Cell::Integer(2), Cell::Integer(21)], vec![Cell::Integer(3), Cell::Integer(31)]]
        );
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
        assert_eq!(engine.execute_one("SELECT name, seq FROM sqlite_sequence").expect("sequence").rows, vec![vec![text("c"), Cell::Integer(1)]]);
        assert_eq!(engine.execute_one("SELECT count(*) FROM sqlite_master").expect("count").rows, vec![vec![Cell::Integer(4)]]);

        let dropped = engine.execute_one("DROP TABLE c").expect("drop c");
        assert_eq!(dropped.statement, "drop-table");
        assert_eq!(dropped.affected, 1);
        // The catalog row, the explicit index row and the automatic `UNIQUE`
        // row all disappear in the same statement; only `sqlite_sequence`
        // remains, plus no sequence row for `c`.
        assert_eq!(engine.execute_one("SELECT type, name FROM sqlite_master").expect("master").rows, vec![vec![text("table"), text("sqlite_sequence")]]);
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
        assert_eq!(engine.execute_one("DROP INDEX IF EXISTS sqlite_autoindex_t_1").expect_err("autoindex if exists").code, "autoindex-immutable");
        assert_eq!(engine.execute_one("DROP INDEX nope").expect_err("missing").code, "unknown-index");
        assert_eq!(engine.execute_one("DROP INDEX IF EXISTS nope").expect("if exists").affected, 0);

        let dropped = engine.execute_one("DROP INDEX ix").expect("drop ix");
        assert_eq!(dropped.statement, "drop-index");
        assert_eq!(engine.execute_one("SELECT name FROM sqlite_master WHERE type = 'index'").expect("indexes").rows, vec![vec![text("sqlite_autoindex_t_1")]]);
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
        assert_eq!(engine.execute_one("SELECT name, seq FROM sqlite_sequence").expect("sequence").rows, vec![vec![text("b"), Cell::Integer(1)]]);
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
        assert_eq!(engine.execute_one("INSERT INTO t(tt) VALUES (-1)").expect_err("check").code, "constraint-failed");
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
            vec![
                vec![Cell::Integer(1), text("x"), text("n")],
                vec![Cell::Integer(2), text("y"), Cell::Null],
            ]
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
            vec![vec![
                text("table"),
                text("t"),
                text("t"),
                Cell::Integer(2),
                text("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)"),
            ]]
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
            vec![
                vec![Cell::Integer(0), text("main"), text("")],
                vec![Cell::Integer(2), text("aux"), text("")],
            ]
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
            engine.execute_one("SELECT r.v, l.v FROM aux.remote AS r JOIN local AS l ON r.rowid = l.rowid")
                .expect("cross-database join")
                .rows,
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
        let invalid = engine.execute_one("CREATE TABLE bad (id INTEGER PRIMARY KEY, FOREIGN KEY (ghost) REFERENCES parent(id))").expect_err("bad child column");
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
        engine.execute_one("CREATE TABLE child (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, FOREIGN KEY (x, y) REFERENCES parent)").expect("composite child");
        engine.execute_one("INSERT INTO parent VALUES (1, 2)").expect("parent row");
        engine.execute_one("INSERT INTO child VALUES (1, 1, 2)").expect("matching composite");
        let orphan = engine.execute_one("INSERT INTO child VALUES (2, 1, 3)").expect_err("composite orphan");
        assert_eq!(orphan.code, "SQLITE_CONSTRAINT_FOREIGNKEY");
        engine.execute_one("INSERT INTO child VALUES (3, 1, NULL)").expect("partial null passes");
        let partial = engine.execute_one("CREATE TABLE single (id INTEGER PRIMARY KEY, x INTEGER, FOREIGN KEY (x) REFERENCES parent(a))").expect("create single");
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
    fn cascading_and_deferred_foreign_keys_are_rejected_explicitly() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute_one("CREATE TABLE parent (id INTEGER PRIMARY KEY)").expect("create parent");
        let cascade = engine
            .execute_one("CREATE TABLE c1 (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)")
            .expect_err("cascade");
        assert_eq!(cascade.code, "unsupported-foreign-key-action");
        assert_eq!(cascade.clause.as_deref(), Some("CASCADE"));
        let set_null = engine
            .execute_one("CREATE TABLE c2 (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE SET NULL)")
            .expect_err("set null");
        assert_eq!(set_null.code, "unsupported-foreign-key-action");
        let deferred = engine
            .execute_one("CREATE TABLE c3 (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)")
            .expect_err("deferred");
        assert_eq!(deferred.code, "unsupported-deferred-foreign-key");
        // Immediate deferral spellings keep working.
        engine.execute_one("CREATE TABLE c4 (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) NOT DEFERRABLE)").expect("not deferrable");
        engine.execute_one("ALTER TABLE c4 ADD COLUMN other INTEGER REFERENCES parent(id)").expect("add column with a foreign key");
    }

    #[test]
    fn recursive_ctes_iterate_until_the_working_set_is_empty() {
        let mut engine = Engine::new(None).expect("blank engine");
        let walked = engine
            .execute_one("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 5) SELECT x FROM c")
            .expect("recursive walk");
        assert_eq!(walked.rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)], vec![Cell::Integer(3)], vec![Cell::Integer(4)], vec![Cell::Integer(5)]]);
        // The keyword is not required when the body reads itself.
        let bare = engine
            .execute_one("WITH c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3) SELECT x FROM c")
            .expect("recursive without the keyword");
        assert_eq!(bare.rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)], vec![Cell::Integer(3)]]);
        // `UNION` removes duplicates and therefore terminates a self-cycle.
        let cycle = engine
            .execute_one("WITH RECURSIVE c(x) AS (SELECT 1 UNION SELECT x FROM c) SELECT x FROM c")
            .expect("distinct cycle");
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
        let duplicated = engine
            .execute_one("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x FROM c LIMIT 3) SELECT x FROM c")
            .expect("duplicates");
        assert_eq!(duplicated.rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(1)], vec![Cell::Integer(1)]]);
        let seeded = engine
            .execute_one("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3) SELECT x FROM c ORDER BY x")
            .expect("seed duplicates");
        assert_eq!(seeded.rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(1)], vec![Cell::Integer(2)], vec![Cell::Integer(2)], vec![Cell::Integer(3)], vec![Cell::Integer(3)]]);
    }

    #[test]
    fn infinite_recursion_is_cut_off_by_the_enclosing_limit() {
        let mut engine = Engine::new(None).expect("blank engine");
        let limited = engine
            .execute_one("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT x FROM c LIMIT 5")
            .expect("bounded recursion");
        assert_eq!(limited.rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)], vec![Cell::Integer(3)], vec![Cell::Integer(4)], vec![Cell::Integer(5)]]);
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
        assert_eq!(chained.rows, vec![vec![Cell::Integer(1)], vec![Cell::Integer(3)], vec![Cell::Integer(5)], vec![Cell::Integer(7)], vec![Cell::Integer(9)]]);
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
        engine
            .execute("CREATE TABLE t(a INTEGER); BEGIN; INSERT INTO t VALUES (1); CREATE TABLE gone(b INTEGER); ROLLBACK")
            .expect("script");
        assert_eq!(engine.execute_one("SELECT count(*) FROM t").expect("read").rows, vec![vec![Cell::Integer(0)]]);
        assert!(engine.execute_one("SELECT b FROM gone").is_err());

        engine
            .execute("BEGIN; INSERT INTO t VALUES (2); SAVEPOINT s; INSERT INTO t VALUES (3); ROLLBACK TO s; RELEASE s; COMMIT")
            .expect("script");
        assert_eq!(engine.execute_one("SELECT a FROM t").expect("read").rows, vec![vec![Cell::Integer(2)]]);
        assert_eq!(
            engine.execute_one("COMMIT").expect_err("no transaction").message,
            "cannot commit - no transaction is active"
        );
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
        assert_eq!(
            engine.execute_one("SELECT a, b FROM s").expect("read").rows,
            vec![vec![Cell::Integer(12), Cell::Text("5".to_string())]]
        );
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
        engine
            .execute("CREATE TRIGGER bi BEFORE INSERT ON t BEGIN INSERT INTO log VALUES (new.a); END")
            .expect("before insert");
        engine
            .execute("CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.rowid); END")
            .expect("after insert");
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
        engine
            .execute("CREATE TRIGGER bd BEFORE DELETE ON t BEGIN INSERT INTO log VALUES (old.id); END")
            .expect("before delete");
        engine.execute("DELETE FROM t WHERE id = 5").expect("delete");
        assert_eq!(
            engine.execute_one("SELECT v FROM log WHERE rowid = 5").expect("delete log").rows,
            vec![vec![Cell::Integer(5)]]
        );
    }

    #[test]
    fn trigger_when_clause_filters_rows() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)").expect("t");
        engine.execute("CREATE TABLE log(v)").expect("log");
        engine
            .execute("CREATE TRIGGER w AFTER INSERT ON t WHEN new.a > 5 BEGIN INSERT INTO log VALUES (new.a); END")
            .expect("when trigger");
        engine.execute("INSERT INTO t VALUES (1, 10), (2, 3)").expect("insert");
        assert_eq!(engine.execute_one("SELECT v FROM log").expect("log").rows, vec![vec![Cell::Integer(10)]]);
        engine
            .execute("CREATE TRIGGER wu AFTER UPDATE ON t WHEN old.a = 10 BEGIN INSERT INTO log VALUES (new.a); END")
            .expect("when update");
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
            engine
                .execute(&format!("CREATE TRIGGER o{value} AFTER INSERT ON t BEGIN INSERT INTO log VALUES ({value}); END"))
                .expect("trigger");
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
        engine
            .execute("CREATE TRIGGER ta AFTER INSERT ON u BEGIN INSERT INTO b VALUES (new.id, new.a); END")
            .expect("ta");
        engine
            .execute("CREATE TRIGGER tb AFTER INSERT ON b BEGIN INSERT INTO u VALUES (new.id + 100, 1); END")
            .expect("tb");
        engine.execute("INSERT INTO u VALUES (1, 1)").expect("insert");
        assert_eq!(
            engine.execute_one("SELECT id FROM u ORDER BY id").expect("u rows").rows,
            vec![vec![Cell::Integer(1)], vec![Cell::Integer(101)]]
        );
        assert_eq!(
            engine.execute_one("SELECT id FROM b ORDER BY id").expect("b rows").rows,
            vec![vec![Cell::Integer(1)]]
        );
    }

    #[test]
    fn recursive_triggers_pragma_allows_loops_until_the_depth_limit() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE s(id INTEGER PRIMARY KEY, a)").expect("s");
        engine
            .execute("CREATE TRIGGER loop AFTER INSERT ON s BEGIN INSERT INTO s VALUES (new.id + 1000, new.a); END")
            .expect("loop");
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
        assert_eq!(
            engine.execute_one("SELECT id, a FROM base").expect("base rows").rows,
            vec![vec![Cell::Integer(7), Cell::Integer(8)]]
        );
        engine
            .execute("CREATE TRIGGER vu INSTEAD OF UPDATE ON v BEGIN UPDATE base SET a = new.a WHERE id = old.id; END")
            .expect("instead update");
        let updated = engine.execute("UPDATE v SET a = 20 WHERE id = 7").expect("view update").pop().expect("result");
        assert_eq!(updated.affected, 0);
        assert_eq!(
            engine.execute_one("SELECT id, a FROM base").expect("base after update").rows,
            vec![vec![Cell::Integer(7), Cell::Integer(20)]]
        );
        engine
            .execute("CREATE TRIGGER vd INSTEAD OF DELETE ON v BEGIN DELETE FROM base WHERE id = old.id; END")
            .expect("instead delete");
        let deleted = engine.execute("DELETE FROM v WHERE id = 7").expect("view delete").pop().expect("result");
        assert_eq!(deleted.affected, 0);
        assert_eq!(engine.execute_one("SELECT count(*) FROM base").expect("base after delete").rows, vec![vec![Cell::Integer(0)]]);
    }

    #[test]
    fn a_failing_trigger_row_rolls_the_whole_statement_back() {
        let mut engine = Engine::new(None).expect("blank engine");
        engine.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a)").expect("t");
        engine.execute("CREATE TABLE log(v)").expect("log");
        engine
            .execute("CREATE TRIGGER bad AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.zzz); END")
            .expect("trigger");
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
        engine
            .execute("CREATE TRIGGER temp.xt AFTER INSERT ON tt BEGIN INSERT INTO m VALUES (new.id, new.a); END")
            .expect("temp trigger");
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
        assert_eq!(
            engine.execute_one("SELECT id, a FROM m").expect("m rows").rows,
            vec![vec![Cell::Integer(1), Cell::Integer(2)]]
        );
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
        engine
            .execute("CREATE TRIGGER uo AFTER UPDATE OF a ON t BEGIN INSERT INTO log VALUES (new.a); END")
            .expect("update of");
        engine.execute("INSERT INTO t VALUES (1, 'start', 'start')").expect("seed");
        engine.execute("UPDATE t SET b = 'x'").expect("update b");
        assert_eq!(engine.execute_one("SELECT count(*) FROM log").expect("log after b").rows, vec![vec![Cell::Integer(0)]]);
        engine.execute("UPDATE t SET a = 'y'").expect("update a");
        assert_eq!(
            engine.execute_one("SELECT v FROM log").expect("log after a").rows,
            vec![vec![Cell::Text("y".to_string())]]
        );
    }
}
