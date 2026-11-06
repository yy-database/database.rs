//! SQLite product facade: format-3 tooling and the shared SQL execution core.
//!
//! | Level | Capability | Entry point |
//! |-------|------------|-------------|
//! | L2 | Read-only **file import** | [`SqliteDatabase`] |
//! | L2 | Writable **snapshot builder** | [`SqliteWriter`] |
//! | L3 | **SQL execution** over format-3 without the SQLite C library | [`sql::Engine`] |
//!
//! The Node-API, PyO3 and WebAssembly bindings share [`sql::Engine`] and only
//! map it onto their own conventions. YYDB owns no SQL dialect. No official
//! SQLite C library. No YYDB engine.

#![deny(missing_docs)]

pub mod sql;

pub use sql::{Cell, ColumnRef, Engine, ExecResult, IndexColumn, Literal, SelectItems, SqlError, Statement, TableRef, parse, results_json};

pub use database_storage::sqlite::{
    ColumnSpec, DropIndexOutcome, SchemaObject, SchemaObjectKind, SqliteDatabase, SqliteWriter, TableLimits, TableRow, TextEncoding, UpdateValue, WriteValue,
    apply_write_affinity,
    decode_record, decode_varint, read_existing, read_index_table, read_named_index, read_named_table, read_named_without_rowid, read_schema,
    read_table, sqlite_library_version,
};

pub use database_formats::sqlite::{
    ENGINE_LIBRARY_VERSION, ENGINE_LIBRARY_VERSION_NUMBER, MAGIC, RecordLimits, RecordValue, blank_database, decode_library_version,
    validate_database,
};

/// Adapter identifier used in logs and diagnostics.
pub const BACKEND_ID: &str = "sqlite";

/// Short label for the binary-compatibility constraint.
pub const CONSTRAINT: &str = "upstream SQLite format-3 engine";
