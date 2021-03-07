//! SQLite read-only storage runtime.
//!
//! This crate sits above `sqlite-formats`: format primitives for header,
//! record, and encoding come from there. Everything that needs to open a file,
//! traverse a B-tree, or build a complete row payload lives here.
#![warn(missing_docs)]

mod database;
mod journal;
mod schema;
mod table;
mod wal;
mod writer;

pub use crate::sqlite::{
    database::{SqliteDatabase, read_existing},
    schema::{
        SchemaObject, SchemaObjectKind, ViewProjection, parse_view, read_named_index, read_named_table, read_named_without_rowid, read_schema,
        read_schema_with_rowids, validate_strict_rows,
    },
    table::{TableLimits, TableRow, read_index_table, read_index_table_with, read_table, read_table_range},
    writer::{Collation, ColumnSpec, DropIndexOutcome, SqliteWriter, UpdateValue, apply_write_affinity},
};

// Re-exported from sqlite-formats for convenient one-crate consumption.
pub use database_formats::sqlite::{TextEncoding, WriteValue, decode_record, decode_varint, encode_record, sqlite_library_version};
