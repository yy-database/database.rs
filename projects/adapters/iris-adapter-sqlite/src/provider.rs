//! SQLite execution surface for the Iris adapter.
//!
//! Bundled `rusqlite` / official SQLite engines are **forbidden**. Format-3
//! continuation (L3) belongs to the `sqlite` product facade. Until that kernel
//! is wired here, open and execute paths fail closed with a stable policy error.

use std::path::Path;

use sqlite::{ENGINE_LIBRARY_VERSION, ENGINE_LIBRARY_VERSION_NUMBER};

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

fn continuation_pending(action: &str) -> SqliteError {
    SqliteError::policy(format!(
        "iris-adapter-sqlite: {action} requires SQLite product L3 continuation runtime (no rusqlite); see database compatibility levels"
    ))
}

/// Native SQLite handle placeholder until the product L3 kernel is wired.
///
/// Historically named `RusqliteProvider`; the type alias keeps call sites compiling
/// while the implementation stays fail-closed.
pub struct SqliteProvider {
    _private: (),
}

/// Historical name kept as a type alias during the L3 cutover.
pub type RusqliteProvider = SqliteProvider;

impl std::fmt::Debug for SqliteProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SqliteProvider").finish_non_exhaustive()
    }
}

impl SqliteProvider {
    /// Open an in-memory SQLite database.
    pub fn open_in_memory() -> Result<Self, SqliteError> {
        Err(continuation_pending("open_in_memory"))
    }

    /// Open or create a file-backed SQLite database.
    pub fn open(path: &Path) -> Result<Self, SqliteError> {
        let _ = path;
        Err(continuation_pending("open"))
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
        let _ = (sql, params);
        Err(continuation_pending("execute_one"))
    }

    /// Executes a multi-statement batch and discards result rows.
    pub fn execute_batch(&self, sql: &str) -> Result<(), SqliteError> {
        let _ = sql;
        Err(continuation_pending("execute_batch"))
    }

    /// Starts an immediate write transaction.
    pub fn begin_immediate(&self) -> Result<(), SqliteError> {
        Err(continuation_pending("begin_immediate"))
    }

    /// Commits the active transaction.
    pub fn commit(&self) -> Result<(), SqliteError> {
        Err(continuation_pending("commit"))
    }

    /// Rolls back the active transaction.
    pub fn rollback(&self) -> Result<(), SqliteError> {
        Err(continuation_pending("rollback"))
    }

    /// Inspects live catalog metadata.
    pub fn inspect_catalog(&self) -> Result<CatalogSnapshot, SqliteError> {
        Err(continuation_pending("inspect_catalog"))
    }
}
