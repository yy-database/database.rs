//! Structured errors shared by the three hosts.
//!
//! Rejections keep the historical message text, and add machine-readable
//! fields so a corpus runner can tell `unsupported` apart from a wrong result:
//!
//! | field | meaning |
//! |-------|---------|
//! | `code` | stable identifier such as `unsupported-statement`, `unsupported-clause`, `unknown-table` |
//! | `statement` | the statement being parsed or executed (`create-table`, `insert`, `select`) |
//! | `clause` | the offending clause when one is recognized (`GROUP`, `LIMIT`, `GENERATED`) |
//! | `position` | byte offset inside the submitted SQL text |

use std::fmt;

/// One rejection from the parser or the execution layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlError {
    /// Stable machine-readable identifier.
    pub code: &'static str,
    /// Human-readable message, matching the documented rejection texts.
    pub message: String,
    /// Statement kind that produced the error.
    pub statement: Option<&'static str>,
    /// Offending clause, uppercased when it is a keyword.
    pub clause: Option<String>,
    /// Byte offset inside the submitted SQL text.
    pub position: Option<usize>,
}

impl SqlError {
    /// Builds an error with a code and a message.
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), statement: None, clause: None, position: None }
    }

    /// Builds a plain syntax error.
    pub fn syntax(message: impl Into<String>, position: Option<usize>) -> Self {
        let mut error = Self::new("syntax", message);
        error.position = position;
        error
    }

    /// Attaches a byte offset.
    pub fn at(mut self, position: usize) -> Self {
        self.position = Some(position);
        self
    }

    /// Attaches the statement kind.
    pub fn in_statement(mut self, statement: &'static str) -> Self {
        self.statement = Some(statement);
        self
    }

    /// Attaches the offending clause.
    pub fn in_clause(mut self, clause: impl Into<String>) -> Self {
        self.clause = Some(clause.into());
        self
    }

    /// Serializes the error as the documented JSON envelope.
    pub fn to_json(&self) -> String {
        let mut out = String::from("{\"code\":");
        out.push_str(&crate::sql::json_string(self.code));
        out.push_str(",\"message\":");
        out.push_str(&crate::sql::json_string(&self.message));
        out.push_str(",\"statement\":");
        out.push_str(&self.statement.map_or_else(|| "null".to_string(), crate::sql::json_string));
        out.push_str(",\"clause\":");
        out.push_str(&self.clause.as_deref().map_or_else(|| "null".to_string(), crate::sql::json_string));
        out.push_str(",\"position\":");
        out.push_str(&self.position.map_or_else(|| "null".to_string(), |value| value.to_string()));
        out.push('}');
        out
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for SqlError {}
