//! PostgreSQL wire error types.
//!
//! The canonical definitions live in `database-protocol-pgsql` so the
//! protocol layer can return them without depending on this crate.

pub use database_protocol_pgsql::{MessageField, Notice, PostgresError};
