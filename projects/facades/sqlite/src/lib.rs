//! SQLite product facade: format-3 L2 tooling and the future L3 continuation kernel.
//!
//! | Level | Capability | Entry point |
//! |-------|------------|-------------|
//! | L2 | Read-only **file import** | [`SqliteDatabase`] |
//! | L2 | Writable **snapshot builder** | [`SqliteWriter`] |
//! | L3 | **Continuation runtime** (planned) | execution over format-3 without `rusqlite` |
//!
//! YYDB no longer owns any SQL dialect. Former `yydb::sql::SqliteConnection` work
//! moves into this product's L3 plan. No official SQLite C library. No YYDB engine.

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

/// Why SQL execution is not exposed on the L2 surface yet.
pub const WHY_NOT_ENGINE: &str = "L2 is format import/export only. L3 continuation (including SQL) is the SQLite product roadmap. YYDB has no SQL.";
