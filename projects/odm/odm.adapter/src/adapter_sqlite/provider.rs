//! SQLite execution surface for the ODM adapter.
//!
//! SQL 解析与执行全部在 `sqlite` 产品门面（`projects/facades/sqlite`）内完成；
//! 本模块只负责持有连接、做参数/结果类型映射与错误转译。不引入 `rusqlite`，
//! 也不在本 crate 里实现任何 SQL 语义。
//!
//! 连接形态：
//!
//! - **内存库**：`Engine::new` 建空镜像。
//! - **文件库**：既有镜像经 `SqliteWriter::open` + `Engine::from_writer` 装回；
//!   新文件先建空镜像，首次写入时再落盘。每条写语句在非事务状态下写回，
//!   `commit` 后也写回。
//! - **事务**：门面的 `BEGIN` / `COMMIT` / `ROLLBACK` 语句直接驱动
//!   `Engine` 的 writer 快照层，不再 fail-closed。

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use database_formats::sqlite::{ENGINE_LIBRARY_VERSION, ENGINE_LIBRARY_VERSION_NUMBER};
use sqlite::{Cell, Engine, SchemaObjectKind, SqlError, SqliteWriter, Statement, TableLimits, parse, read_schema};

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

/// SQLite handle backed by the `sqlite` product facade.
///
/// Historically named `RusqliteProvider`; the type alias keeps call sites compiling.
pub struct SqliteProvider {
    engine: Mutex<Engine>,
    /// File the image is written back to, `None` for in-memory databases.
    path: Option<PathBuf>,
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
        Ok(Self { engine: Mutex::new(engine), path: None })
    }

    /// Open or create a file-backed SQLite database.
    ///
    /// An existing format-3 image is read with `SqliteWriter::open` and wrapped
    /// by `Engine::from_writer`; a missing file starts as a blank image and is
    /// written back on the first mutation.
    pub fn open(path: &Path) -> Result<Self, SqliteError> {
        let engine = if path.exists() {
            let writer = SqliteWriter::open(path).map_err(|error| SqliteError::policy(format!("sqlite facade image read: {error}")))?;
            Engine::from_writer(writer).map_err(map_sql_error)?
        }
        else {
            Engine::new(None).map_err(map_sql_error)?
        };
        Ok(Self { engine: Mutex::new(engine), path: Some(path.to_path_buf()) })
    }

    /// Writes the in-memory image back to the file when the handle is
    /// file-backed and no explicit transaction is open.
    fn persist(engine: &mut Engine, path: Option<&Path>) -> Result<(), SqliteError> {
        if let Some(path) = path {
            if !engine.in_transaction() {
                engine.save(path).map_err(map_sql_error)?;
            }
        }
        Ok(())
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
        let mut engine = self.engine.lock().expect("sqlite mutex");
        let mut results = engine.execute_with_parameters(sql, &cells).map_err(map_sql_error)?;
        let last_insert_rowid = engine.last_insert_rowid();
        let result = results.pop().expect("single statement returns one result");
        Self::persist(&mut engine, self.path.as_deref())?;
        Ok(StatementResult {
            columns: result.columns,
            rows: result.rows.into_iter().map(|row| row.into_iter().map(from_cell).collect()).collect(),
            changes: result.affected as u64,
            last_insert_rowid,
        })
    }

    /// Executes a multi-statement batch and discards result rows.
    pub fn execute_batch(&self, sql: &str) -> Result<(), SqliteError> {
        let mut engine = self.engine.lock().expect("sqlite mutex");
        engine.execute(sql).map_err(map_sql_error)?;
        Self::persist(&mut engine, self.path.as_deref())
    }

    /// Starts an immediate write transaction.
    pub fn begin_immediate(&self) -> Result<(), SqliteError> {
        let mut engine = self.engine.lock().expect("sqlite mutex");
        engine.execute_one("BEGIN IMMEDIATE").map_err(map_sql_error)?;
        Ok(())
    }

    /// Commits the active transaction.
    pub fn commit(&self) -> Result<(), SqliteError> {
        let mut engine = self.engine.lock().expect("sqlite mutex");
        engine.execute_one("COMMIT").map_err(map_sql_error)?;
        if let Some(path) = self.path.as_deref() {
            engine.save(path).map_err(map_sql_error)?;
        }
        Ok(())
    }

    /// Rolls back the active transaction.
    pub fn rollback(&self) -> Result<(), SqliteError> {
        let mut engine = self.engine.lock().expect("sqlite mutex");
        engine.execute_one("ROLLBACK").map_err(map_sql_error)?;
        Ok(())
    }

    /// Whether an explicit transaction is currently open.
    pub fn in_transaction(&self) -> bool {
        self.engine.lock().expect("sqlite mutex").in_transaction()
    }

    /// Inspects live catalog metadata.
    pub fn inspect_catalog(&self) -> Result<CatalogSnapshot, SqliteError> {
        let mut engine = self.engine.lock().expect("sqlite mutex");
        let objects = read_schema(engine.snapshot().map_err(map_sql_error)?, TableLimits::default())
            .map_err(|error| SqliteError::policy(format!("sqlite facade: {error}")))?;
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
