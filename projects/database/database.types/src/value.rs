//! Runtime value envelope.

use super::*;
use crate::hex;

/// 最小类型化值集合。
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Value {
    /// SQL-style null.
    Null,
    /// Boolean scalar.
    Bool(bool),
    /// Signed 64-bit integer.
    I64(i64),
    /// Unsigned 64-bit integer.
    U64(u64),
    /// IEEE-754 double.
    F64(f64),
    /// Small inline bytes (allowed; prefer CAS when large).
    Bytes(Vec<u8>),
    /// UTF-8 text scalar.
    Text(String),
    /// UUID scalar.
    Uuid(uuid::Uuid),
    /// Logical vector (often materialized via [`ObjectRef`]).
    Vector(Vector),
    /// CAS handle under `<hash-prefix>/<chunk_hash>.blob`.
    Object(ObjectRef),
    /// Chunked logical file described by its descriptor and manifest object.
    File(FileDescriptor),
    /// Nested row embedded in query projection results (not persisted inline).
    Row(BTreeMap<String, Value>),
    /// Calendar date without time or zone.
    Date(Date),
    /// Wall-clock time with nanosecond resolution, no zone.
    Time(Time),
    /// Absolute UTC instant with microsecond resolution.
    Timestamp(Timestamp),
    /// Exact fixed-point decimal with declared precision and scale.
    Decimal(Decimal),
}

impl Value {
    /// Scalar kind label used by catalog, storage, and wire encodings.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::I64(_) => "i64",
            Self::U64(_) => "u64",
            Self::F64(_) => "f64",
            Self::Bytes(_) => "bytes",
            Self::Text(_) => "text",
            Self::Uuid(_) => "uuid",
            Self::Vector(_) => "vector",
            Self::Object(_) => "object",
            Self::File(_) => "file",
            Self::Row(_) => "row",
            Self::Date(_) => "date",
            Self::Time(_) => "time",
            Self::Timestamp(_) => "timestamp",
            Self::Decimal(_) => "decimal",
        }
    }

    /// Exact key text used by unique-constraint comparison.
    ///
    /// Values are length-prefixed so concatenated keys stay unambiguous.
    pub fn key_component(&self) -> String {
        match self {
            Self::Null => "n".to_owned(),
            Self::Bool(value) => format!("b:{value}"),
            Self::I64(value) => format!("i:{value}"),
            Self::U64(value) => format!("u:{value}"),
            Self::F64(value) => format!("f:{:016x}", value.to_bits()),
            Self::Bytes(bytes) => format!("y:{}:{}", bytes.len(), hex::encode_vec(bytes)),
            Self::Text(text) => format!("t:{}:{text}", text.len()),
            Self::Uuid(uuid) => format!("g:{uuid}"),
            Self::Date(date) => format!("d:{}", date.days()),
            Self::Time(time) => format!("h:{}", time.nanos()),
            Self::Timestamp(timestamp) => format!("s:{}", timestamp.unix_micros()),
            Self::Decimal(decimal) => {
                format!("m:{}:{}:{}", decimal.precision(), decimal.scale(), decimal.unscaled())
            }
            Self::Object(object) => {
                format!("o:{}:{}:{}", object.algo.as_str(), object.hash_hex(), object.size)
            }
            Self::Row(_) | Self::Vector(_) | Self::File(_) => {
                format!("x:{}", self.kind())
            }
        }
    }
}
