//! SQLite execution surface for the ODM adapter.
//!
//! SQL 解析与执行全部在 `sqlite` 产品门面（`projects/facades/sqlite`）内完成；
//! 本模块只负责持有连接、做参数/结果类型映射与错误转译。不引入 `rusqlite`，
//! 也不在本 crate 里实现任何 SQL 语义。
//!
//! 门面当前尚未提供、因此仍 fail-closed 的能力：
//!
//! - **文件打开**：`Engine` 只有 `new`（建空库），没有从既有 format-3 镜像构造的
//!   入口；`SqliteWriter::open` 能读既有镜像，但 `Engine` 没有 `from_writer`。
//! - **事务**：`Engine` 没有 `begin` / `commit` / `rollback`，事务只在
//!   `SqliteWriter` 内部使用。适配器的 `begin_immediate` / `commit` / `rollback`
//!   因此无法映射，`authority_commit` 与 Managed Push 的原子提交同样受阻。
//! - **`last_insert_rowid`**：门面只回 `columns` / `rows` / `affected`，没有取值
//!   入口，该字段恒为 `0`。

use std::{path::Path, sync::Mutex};

use database_formats::sqlite::{ENGINE_LIBRARY_VERSION, ENGINE_LIBRARY_VERSION_NUMBER};
use sqlite::{Cell, Engine, SchemaObjectKind, SqlError, Statement, TableLimits, parse, read_schema};

/// One SQLite storage-class value with lossless integer and binary payloads.
#[derive(Debug, Clone, PartialEq)]
pub enum SqliteValue {
    /// SQL NULL.
    Null,
    /// Signed SQLite INTEGER.
    Integer(i64),
    /// SQLite REAL.
    Real(f64),
    /// SQLite TEXT bytes.
    Text(Vec<u8>),
    /// SQLite BLOB bytes.
    Blob(Vec<u8>),
}

impl SqliteValue {
    /// Returns the storage-class label.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Integer(_) => "integer",
            Self::Real(_) => "real",
            Self::Text(_) => "text",
            Self::Blob(_) => "blob",
        }
    }
}

impl std::fmt::Display for SqliteValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => Ok(()),
            Self::Integer(value) => write!(formatter, "{value}"),
            Self::Real(value) => write!(formatter, "{value}"),
            Self::Text(value) => formatter.write_str(&String::from_utf8_lossy(value)),
            Self::Blob(value) => {
                formatter.write_str("x'")?;
                for byte in value {
                    write!(formatter, "{byte:02x}")?;
                }
                formatter.write_str("'")
            }
        }
    }
}

/// Result of executing exactly one SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub struct StatementResult {
    /// Result column labels in order.
    pub columns: Vec<String>,
    /// Materialized rows preserving SQLite storage classes.
    pub rows: Vec<Vec<SqliteValue>>,
    /// Rows changed by the statement.
    pub changes: u64,
    /// Last insert rowid for the connection after execution.
    ///
    /// 门面没有暴露该值，当前恒为 `0`（见模块文档的缺口说明）。
    pub last_insert_rowid: i64,
}

/// One observed column in the physical catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogColumn {
    /// Physical column name.
    pub name: String,
    /// Declared SQLite type text.
    pub type_name: String,
    /// Whether the column accepts NULL.
    pub nullable: bool,
    /// Whether the column participates in the primary key.
    pub primary_key: bool,
}

/// One observed physical table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogTable {
    /// Physical table name.
    pub name: String,
    /// Observed columns in physical order.
    pub columns: Vec<CatalogColumn>,
}

/// Observed catalog snapshot returned by [`SqliteProvider::inspect_catalog`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogSnapshot {
    /// Observed user tables (SQLite internal tables excluded).
    pub tables: Vec<CatalogTable>,
}

impl CatalogSnapshot {
    /// Find an observed table by physical name.
    pub fn table(&self, name: &str) -> Option<&CatalogTable> {
        self.tables.iter().find(|table| table.name == name)
    }
}

/// Structured provider failure.
#[derive(Debug)]
pub struct SqliteError {
    message: String,
}

impl SqliteError {
    /// Build a policy or contract failure without a native result code.
    pub fn policy(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

impl std::fmt::Display for SqliteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for SqliteError {}

/// 门面缺口：点名缺的能力、门面坐标与需要的最小复现。
fn facade_gap(action: &str, capability: &str, coordinate: &str, repro: &str) -> SqliteError {
    SqliteError::policy(format!("iris-adapter-sqlite: {action} requires `{capability}` from the sqlite facade at {coordinate}; repro: {repro}"))
}

/// SQLite handle backed by the `sqlite` product facade.
///
/// Historically named `RusqliteProvider`; the type alias keeps call sites compiling.
pub struct SqliteProvider {
    engine: Mutex<Engine>,
}

/// Historical name kept as a type alias during the facade cutover.
pub type RusqliteProvider = SqliteProvider;

impl std::fmt::Debug for SqliteProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SqliteProvider").finish_non_exhaustive()
    }
}

impl SqliteProvider {
    /// Open an in-memory SQLite database backed by the facade engine.
    pub fn open_in_memory() -> Result<Self, SqliteError> {
        let engine = Engine::new(None).map_err(map_sql_error)?;
        Ok(Self { engine: Mutex::new(engine) })
    }

    /// Open or create a file-backed SQLite database.
    ///
    /// 门面还没有把既有镜像装回 `Engine` 的入口，这里保持 fail-closed 并点名缺口。
    pub fn open(path: &Path) -> Result<Self, SqliteError> {
        Err(facade_gap(
            "open",
            "Engine::from_writer",
            "projects/facades/sqlite/src/sql/engine.rs Engine::new (blank image only)",
            &format!("SqliteWriter::open({}) then execute a SELECT", path.display()),
        ))
    }

    /// Returns the product facade library version label (not an official SQLite build).
    pub fn sqlite_version(&self) -> Result<String, SqliteError> {
        Ok(ENGINE_LIBRARY_VERSION.to_owned())
    }

    /// Returns a stable source id derived from the product version number.
    pub fn source_id(&self) -> Result<String, SqliteError> {
        Ok(format!("sqlite-product:{ENGINE_LIBRARY_VERSION_NUMBER}"))
    }

    /// Executes exactly one statement with positional bind parameters.
    pub fn execute_one(&self, sql: &str, params: &[SqliteValue]) -> Result<StatementResult, SqliteError> {
        let statements = parse(sql).map_err(map_sql_error)?;
        if statements.len() != 1 {
            return Err(SqliteError::policy(format!("execute_one expects exactly one statement but found {}", statements.len())));
        }
        let cells = params.iter().map(to_cell).collect::<Result<Vec<_>, _>>()?;
        let mut results = self.engine.lock().expect("sqlite mutex").execute_with_parameters(sql, &cells).map_err(map_sql_error)?;
        let result = results.pop().expect("single statement returns one result");
        Ok(StatementResult {
            columns: result.columns,
            rows: result.rows.into_iter().map(|row| row.into_iter().map(from_cell).collect()).collect(),
            changes: result.affected as u64,
            last_insert_rowid: 0,
        })
    }

    /// Executes a multi-statement batch and discards result rows.
    pub fn execute_batch(&self, sql: &str) -> Result<(), SqliteError> {
        self.engine.lock().expect("sqlite mutex").execute(sql).map_err(map_sql_error)?;
        Ok(())
    }

    /// Starts an immediate write transaction.
    pub fn begin_immediate(&self) -> Result<(), SqliteError> {
        Err(facade_gap(
            "begin_immediate",
            "Engine::begin / Engine::commit / Engine::rollback",
            "projects/facades/sqlite/src/sql/engine.rs Engine (no transaction methods)",
            "Engine::new(None), execute INSERT, roll back, then SELECT sees the rolled-back row",
        ))
    }

    /// Commits the active transaction.
    pub fn commit(&self) -> Result<(), SqliteError> {
        Err(facade_gap(
            "commit",
            "Engine::begin / Engine::commit / Engine::rollback",
            "projects/facades/sqlite/src/sql/engine.rs Engine (no transaction methods)",
            "Engine::new(None), begin, execute INSERT, commit, reopen the writer and read the row",
        ))
    }

    /// Rolls back the active transaction.
    pub fn rollback(&self) -> Result<(), SqliteError> {
        Err(facade_gap(
            "rollback",
            "Engine::begin / Engine::commit / Engine::rollback",
            "projects/facades/sqlite/src/sql/engine.rs Engine (no transaction methods)",
            "Engine::new(None), begin, execute INSERT, roll back, then SELECT finds no row",
        ))
    }

    /// Inspects live catalog metadata.
    pub fn inspect_catalog(&self) -> Result<CatalogSnapshot, SqliteError> {
        let engine = self.engine.lock().expect("sqlite mutex");
        let objects =
            read_schema(engine.snapshot(), TableLimits::default()).map_err(|error| SqliteError::policy(format!("sqlite facade: {error}")))?;
        let mut tables = Vec::new();
        for object in objects {
            if object.kind != SchemaObjectKind::Table || object.name.starts_with("sqlite_") {
                continue;
            }
            let Some(sql) = object.sql
            else {
                continue;
            };
            let Some(Statement::CreateTable { columns, primary_key, .. }) = parse(&sql).map_err(map_sql_error)?.into_iter().next()
            else {
                return Err(SqliteError::policy(format!("catalog entry `{}` is not a CREATE TABLE statement", object.name)));
            };
            let columns = columns
                .into_iter()
                .map(|column| CatalogColumn {
                    primary_key: column.primary_key || primary_key.iter().any(|key| key.eq_ignore_ascii_case(&column.name)),
                    nullable: !column.not_null,
                    name: column.name,
                    type_name: column.declared_type,
                })
                .collect();
            tables.push(CatalogTable { name: object.name, columns });
        }
        Ok(CatalogSnapshot { tables })
    }
}

fn to_cell(value: &SqliteValue) -> Result<Cell, SqliteError> {
    Ok(match value {
        SqliteValue::Null => Cell::Null,
        SqliteValue::Integer(value) => Cell::Integer(*value),
        SqliteValue::Real(value) => Cell::Real(*value),
        SqliteValue::Text(value) => {
            Cell::Text(String::from_utf8(value.clone()).map_err(|_| SqliteError::policy("sqlite TEXT parameter is not valid UTF-8"))?)
        }
        SqliteValue::Blob(value) => Cell::Blob(value.clone()),
    })
}

fn from_cell(cell: Cell) -> SqliteValue {
    match cell {
        Cell::Null => SqliteValue::Null,
        Cell::Integer(value) => SqliteValue::Integer(value),
        Cell::Real(value) => SqliteValue::Real(value),
        Cell::Text(value) => SqliteValue::Text(value.into_bytes()),
        Cell::Blob(value) => SqliteValue::Blob(value),
    }
}

fn map_sql_error(error: SqlError) -> SqliteError {
    SqliteError::policy(error.message)
}
