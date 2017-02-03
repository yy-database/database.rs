//! SQL execution core shared by the Node-API, PyO3 and WebAssembly hosts.
//!
//! The module owns the fail-closed statement subset, its structured rejections
//! and the JSON envelope. Hosts only map the same primitives onto their own
//! conventions:
//!
//! - [`Engine::execute_one`] executes exactly one statement.
//! - [`Engine::execute_batch`] executes a list of statements in order.
//! - [`Engine::execute`] executes a semicolon-separated script.
//!
//! Every statement runs over the pure-Rust format-3 writer, so the resulting
//! image is a real `.sqlite3` file, not a simulation layer.

pub(crate) mod datetime;
pub mod engine;
pub mod error;
pub(crate) mod json;
pub mod sql;

pub use engine::{Cell, Engine, ExecResult, results_json};
pub use error::SqlError;
pub use sql::{ColumnRef, IndexColumn, Literal, SelectItems, Statement, TableRef, parse};

pub(crate) fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            character if (character as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", character as u32)),
            character => out.push(character),
        }
    }
    out.push('"');
    out
}
