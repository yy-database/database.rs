//! SQLite product facade: format-3 file inspection and writable snapshot tooling.
//!
//! This crate is **not** the YYDB SQL dialect. Embedded SQL over `.yydb` lives in
//! [`yydb::sql::SqliteConnection`]. Here we only own upstream SQLite bytes:
//!
//! | Capability | Entry point | Storage |
//! |------------|-------------|---------|
//! | Read-only **file import** | [`SqliteDatabase`] | existing `.sqlite` / format-3 bytes |
//! | Writable **snapshot builder** | [`SqliteWriter`] | new format-3 files for migration / fixtures |
//!
//! No SQLite C library. No YYDB engine dependency.

#![deny(missing_docs)]

pub use sqlite_storage::{
    ColumnSpec, SchemaObject, SchemaObjectKind, SqliteDatabase, SqliteWriter, TableLimits, TableRow, TextEncoding, UpdateValue, WriteValue,
    decode_record, decode_varint, read_existing, read_named_table, read_schema, read_table, sqlite_library_version,
};

pub use sqlite_formats::{
    ENGINE_LIBRARY_VERSION, ENGINE_LIBRARY_VERSION_NUMBER, MAGIC, RecordLimits, RecordValue, blank_database, decode_library_version,
    validate_database,
};

/// Adapter identifier used in logs and diagnostics.
pub const BACKEND_ID: &str = "sqlite";

/// Short label for the binary-compatibility constraint.
pub const CONSTRAINT: &str = "upstream SQLite format-3 engine";

/// Why this crate does not execute SQL.
pub const WHY_NOT_ENGINE: &str = "SQLite product owns format import/export only. SQL over .yydb is yydb::sql.";
