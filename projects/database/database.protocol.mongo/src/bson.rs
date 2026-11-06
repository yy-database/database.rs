//! 足以支撑 MongoDB OP_MSG 线处理的 BSON 文档序列化 / 解析。
//!
//! 覆盖 BSON 规范的全部元素类型：Document (0x03)、Array (0x04)、String (0x02)、
//! Int32 (0x10)、Int64 (0x12)、Double (0x01)、Decimal128 (0x13)、Bool (0x08)、
//! Null (0x0A)、Undefined (0x06)、ObjectId (0x07)、Binary (0x05，保留子类型)、
//! Date (0x09)、Timestamp (0x11)、Regex (0x0B)、Code (0x0D)、CodeWScope (0x0F)、
//! DbPointer (0x0C)、Symbol (0x0E)、MinKey (0xFF) 与 MaxKey (0x7F)。
//!
//! `mongod 8.0.9` 会把这些类型原样回显在 `find` / `explain` 应答里，缺任何一种
//! 都会让真机应答整条不可解码。

use std::{
    cmp::Ordering,
    io::{self, Cursor, Read, Write},
};

use crate::decimal128::Decimal128;

/// 带子类型的二进制载荷。
///
/// `bytes` 只存线上载荷本身，不含外层长度与子类型字节。已废弃的子类型
/// `0x02` 在线上还会内嵌一份自身长度，解码时剥离、编码时还原，因此往返
/// 字节完全一致。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryData {
    /// BSON 二进制子类型（`0x00`、`0x02`、`0x05`、`0x80` 等）。
    pub subtype: u8,
    /// 线上载荷字节，不含外层长度与子类型。
    pub bytes: Vec<u8>,
}

impl BinaryData {
    /// 组装一份二进制载荷。
    pub fn new(subtype: u8, bytes: Vec<u8>) -> Self {
        Self { subtype, bytes }
    }
}

/// 本协议栈支持的 BSON 值变体。
#[derive(Debug, Clone, PartialEq)]
pub enum BsonValue {
    /// Embedded document (0x03).
    Document(BsonDocument),
    /// Array (0x04).
    Array(Vec<BsonValue>),
    /// UTF-8 string (0x02).
    String(String),
    /// 32-bit signed integer (0x10).
    Int32(i32),
    /// 64-bit signed integer (0x12).
    Int64(i64),
    /// IEEE-754 double (0x01).
    Double(f64),
    /// Boolean (0x08).
    Bool(bool),
    /// Null placeholder (0x0A).
    Null,
    /// 已废弃的 BSON undefined (0x06)。
    ///
    /// `mongod` 对多键索引上空数组的索引键会写成 undefined，因此唯一索引
    /// 冲突应答里的 `keyValue: { a: undefined }` 必须能原样复现。
    Undefined,
    /// 12-byte ObjectId (0x07).
    ObjectId([u8; 12]),
    /// Binary payload (0x05) 连同其子类型。
    Binary(BinaryData),
    /// BSON Date (0x09)：自 Unix 纪元起的毫秒数，始终按 UTC 解释。
    DateTime(i64),
    /// BSON Timestamp (0x11)：秒级时间戳 `t` 与自增计数 `i`。
    ///
    /// 线上字节先写 `i` 再写 `t`，但排序与比较按 `(t, i)` 字典序进行。
    Timestamp {
        /// 自 Unix 纪元起的秒数。
        t: u32,
        /// 同一秒内的自增计数。
        i: u32,
    },
    /// Decimal128 (0x13)。
    Decimal128(Decimal128),
    /// 正则表达式 (0x0B)：模式与选项分开存储，选项按 `mongod` 的字母序回显。
    Regex {
        /// 正则模式，不含分隔符。
        pattern: String,
        /// 选项字母（`i`、`m`、`s`、`x` 等）。
        options: String,
    },
    /// JavaScript 代码 (0x0D)。
    Code(String),
    /// 带作用域的 JavaScript 代码 (0x0F)。
    CodeWithScope {
        /// 代码文本。
        code: String,
        /// 代码作用域文档。
        scope: BsonDocument,
    },
    /// 已废弃的 DBPointer (0x0C)：命名空间与目标 ObjectId。
    DbPointer {
        /// 目标命名空间 `<database>.<collection>`。
        namespace: String,
        /// 目标文档的 ObjectId。
        id: [u8; 12],
    },
    /// 已废弃的 Symbol (0x0E)，文本语义与 String 相同。
    Symbol(String),
    /// MinKey (0xFF)：小于任何其他值。
    MinKey,
    /// MaxKey (0x7F)：大于任何其他值。
    MaxKey,
}

impl BsonValue {
    /// BSON 规范里的类型排序位次。
    ///
    /// 顺序取自 `mongod 8.0.9` 的 `sort` 结果：`MinKey` 最小、`MaxKey` 最大，
    /// `Date` 排在 `Bool` 之后、`Timestamp` 之前，`Regex` / `Code` / `CodeWScope`
    /// 依次排在其后。`Symbol` 与 `String` 同位次并按文本比较。
    fn type_rank(&self) -> u8 {
        match self {
            Self::MinKey => 0,
            Self::Undefined => 1,
            Self::Null => 2,
            Self::Double(_) | Self::Int32(_) | Self::Int64(_) | Self::Decimal128(_) => 3,
            Self::String(_) | Self::Symbol(_) => 4,
            Self::Document(_) => 5,
            Self::Array(_) => 6,
            Self::Binary(_) => 7,
            Self::ObjectId(_) => 8,
            Self::Bool(_) => 9,
            Self::DateTime(_) => 10,
            Self::Timestamp { .. } => 11,
            Self::Regex { .. } => 12,
            // DBPointer 已废弃且真机排序位次无法用现代驱动构造语料，这里固定在
            // Regex 与 Code 之间，保证比较是全序。
            Self::DbPointer { .. } => 13,
            Self::Code(_) => 14,
            Self::CodeWithScope { .. } => 15,
            Self::MaxKey => 16,
        }
    }

    /// BSON 规范比较顺序。
    ///
    /// `MinKey < 一切 < MaxKey`；`Date` 按毫秒比较；`Timestamp` 按 `(t, i)` 比较；
    /// 各数值类型共用一个数值空间；`Symbol` 与同文本的 `String` 相等。该顺序
    /// 直接决定索引键与 `sort` 的结果。
    pub fn compare(&self, other: &BsonValue) -> Ordering {
        let left_rank = self.type_rank();
        let right_rank = other.type_rank();
        if left_rank != right_rank {
            return left_rank.cmp(&right_rank);
        }
        match (self, other) {
            (Self::Double(_) | Self::Int32(_) | Self::Int64(_) | Self::Decimal128(_), _) => compare_numbers(self, other),
            (Self::String(left), Self::String(right)) | (Self::Symbol(left), Self::Symbol(right)) => left.cmp(right),
            (Self::String(left), Self::Symbol(right)) | (Self::Symbol(left), Self::String(right)) => left.cmp(right),
            (Self::Document(left), Self::Document(right)) => compare_documents(left, right),
            (Self::Array(left), Self::Array(right)) => {
                for (left, right) in left.iter().zip(right.iter()) {
                    let ordering = left.compare(right);
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
                left.len().cmp(&right.len())
            }
            (Self::Binary(left), Self::Binary(right)) => {
                (left.bytes.len(), left.subtype, &left.bytes).cmp(&(right.bytes.len(), right.subtype, &right.bytes))
            }
            (Self::ObjectId(left), Self::ObjectId(right)) => left.cmp(right),
            (Self::Bool(left), Self::Bool(right)) => left.cmp(right),
            (Self::DateTime(left), Self::DateTime(right)) => left.cmp(right),
            (Self::Timestamp { t: left_t, i: left_i }, Self::Timestamp { t: right_t, i: right_i }) => {
                left_t.cmp(right_t).then(left_i.cmp(right_i))
            }
            (Self::Regex { pattern: left_pattern, options: left_options }, Self::Regex { pattern: right_pattern, options: right_options }) => {
                left_pattern.cmp(right_pattern).then(left_options.cmp(right_options))
            }
            (Self::Code(left), Self::Code(right)) => left.cmp(right),
            (Self::CodeWithScope { code: left_code, scope: left_scope }, Self::CodeWithScope { code: right_code, scope: right_scope }) => {
                left_code.cmp(right_code).then_with(|| compare_documents(left_scope, right_scope))
            }
            (Self::DbPointer { namespace: left, id: left_id }, Self::DbPointer { namespace: right, id: right_id }) => {
                left.cmp(right).then(left_id.cmp(right_id))
            }
            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for BsonValue {
    /// 始终返回 [`BsonValue::compare`] 的结果，`NaN` 与跨类型值也有确定位次。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.compare(other))
    }
}

/// 同一数值空间内的比较：整数对整数精确比较，其余按 `f64`，decimal128 内部
/// 按十进制精确比较。
fn compare_numbers(left: &BsonValue, right: &BsonValue) -> Ordering {
    match (left, right) {
        (BsonValue::Decimal128(left), BsonValue::Decimal128(right)) => left.compare(right),
        (BsonValue::Decimal128(left), _) => compare_f64(left.as_f64(), number_to_f64(right)),
        (_, BsonValue::Decimal128(right)) => compare_f64(number_to_f64(left), right.as_f64()),
        _ => match (integral_of(left), integral_of(right)) {
            (Some(left), Some(right)) => left.cmp(&right),
            _ => compare_f64(number_to_f64(left), number_to_f64(right)),
        },
    }
}

/// 数值变体的 `f64` 近似；非数值返回 `NaN`。
fn number_to_f64(value: &BsonValue) -> f64 {
    match value {
        BsonValue::Int32(number) => *number as f64,
        BsonValue::Int64(number) => *number as f64,
        BsonValue::Double(number) => *number,
        BsonValue::Decimal128(number) => number.as_f64(),
        _ => f64::NAN,
    }
}

/// 精确整数视图，供整数与整数比较使用。
fn integral_of(value: &BsonValue) -> Option<i64> {
    match value {
        BsonValue::Int32(number) => Some(*number as i64),
        BsonValue::Int64(number) => Some(*number),
        BsonValue::Double(number) => integral_value(*number),
        BsonValue::Decimal128(number) => number.as_i64(),
        _ => None,
    }
}

/// `NaN` 排在所有数值之前，与 `mongod` 的排序一致。
fn compare_f64(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
    }
}

/// 文档比较：先比字段名，再比字段值，最后比字段数量。
fn compare_documents(left: &BsonDocument, right: &BsonDocument) -> Ordering {
    for ((left_key, left_value), (right_key, right_value)) in left.fields.iter().zip(right.fields.iter()) {
        let ordering = left_key.cmp(right_key).then_with(|| left_value.compare(right_value));
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.fields.len().cmp(&right.fields.len())
}

/// A BSON document preserves field insertion order (unlike a HashMap) because BSON
/// itself is an ordered key/value mapping.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BsonDocument {
    /// Ordered key/value pairs. `pub(crate)` so the gateway can iterate over inserts
    /// and build writeResponses.
    pub fields: Vec<(String, BsonValue)>,
}

impl BsonDocument {
    /// Empty document.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one key/value pair, preserving insertion order.
    pub fn push(&mut self, key: impl Into<String>, value: BsonValue) {
        self.fields.push((key.into(), value));
    }

    /// Ordered key/value pairs as written on the wire.
    pub fn fields(&self) -> &[(String, BsonValue)] {
        &self.fields
    }

    /// First value under `key`, if present.
    pub fn get(&self, key: &str) -> Option<&BsonValue> {
        self.fields.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// Convenience getter for command names — returns the first key whose value is any
    /// type (we only need the key name for dispatch).
    pub fn command_name(&self) -> Option<&str> {
        self.fields.first().map(|(k, _)| k.as_str())
    }

    /// Extract the `$db` field as a UTF-8 string, defaulting to `"admin"`.
    pub fn database(&self) -> String {
        match self.get("$db") {
            Some(BsonValue::String(db)) => db.clone(),
            _ => "admin".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

fn write_cstring<W: Write>(writer: &mut W, value: &str) -> io::Result<()> {
    writer.write_all(value.as_bytes())?;
    writer.write_all(b"\x00")?;
    Ok(())
}

fn write_string<W: Write>(writer: &mut W, value: &str) -> io::Result<()> {
    let bytes = value.as_bytes();
    writer.write_all(&(bytes.len() as i32 + 1).to_le_bytes())?;
    writer.write_all(bytes)?;
    writer.write_all(b"\x00")?;
    Ok(())
}

fn encode_document<W: Write>(writer: &mut W, doc: &BsonDocument) -> io::Result<()> {
    let mut cursor = Cursor::new(Vec::<u8>::new());
    // Placeholder length
    cursor.write_all(&0_u32.to_le_bytes())?;
    for (key, value) in &doc.fields {
        encode_field(&mut cursor, key, value)?;
    }
    cursor.write_all(&0_u8.to_le_bytes())?; // terminator
    let bytes = cursor.into_inner();
    let len = bytes.len() as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&bytes[4..])?;
    Ok(())
}

fn encode_field<W: Write>(writer: &mut W, key: &str, value: &BsonValue) -> io::Result<()> {
    match value {
        BsonValue::Double(v) => {
            writer.write_all(&0x01_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            writer.write_all(&v.to_le_bytes())?;
        }
        BsonValue::String(v) => {
            writer.write_all(&0x02_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            write_string(writer, v)?;
        }
        BsonValue::Document(doc) => {
            writer.write_all(&0x03_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            encode_document(writer, doc)?;
        }
        BsonValue::Array(items) => {
            writer.write_all(&0x04_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            let mut sub = BsonDocument::new();
            for (i, item) in items.iter().enumerate() {
                sub.push(i.to_string(), item.clone());
            }
            encode_document(writer, &sub)?;
        }
        BsonValue::Binary(v) => {
            writer.write_all(&0x05_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            // BSON 规范：子类型 0x02（已废弃的旧式二进制）在载荷前再内嵌一份
            // 自身长度。存储侧只保留载荷，这里按长度还原。
            let wire_len = if v.subtype == 0x02 { v.bytes.len() + 4 } else { v.bytes.len() };
            writer.write_all(&(wire_len as i32).to_le_bytes())?;
            writer.write_all(&v.subtype.to_le_bytes())?;
            if v.subtype == 0x02 {
                writer.write_all(&(v.bytes.len() as i32).to_le_bytes())?;
            }
            writer.write_all(&v.bytes)?;
        }
        BsonValue::ObjectId(bytes) => {
            writer.write_all(&0x07_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            writer.write_all(bytes)?;
        }
        BsonValue::Bool(v) => {
            writer.write_all(&0x08_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            writer.write_all(&if *v { 1_u8 } else { 0_u8 }.to_le_bytes())?;
        }
        BsonValue::Null => {
            writer.write_all(&0x0A_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
        }
        BsonValue::Undefined => {
            writer.write_all(&0x06_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
        }
        BsonValue::Int32(v) => {
            writer.write_all(&0x10_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            writer.write_all(&v.to_le_bytes())?;
        }
        BsonValue::Int64(v) => {
            writer.write_all(&0x12_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            writer.write_all(&v.to_le_bytes())?;
        }
        BsonValue::DateTime(v) => {
            writer.write_all(&0x09_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            writer.write_all(&v.to_le_bytes())?;
        }
        BsonValue::Timestamp { t, i } => {
            writer.write_all(&0x11_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            // 线上顺序是先 `i` 后 `t`。
            writer.write_all(&i.to_le_bytes())?;
            writer.write_all(&t.to_le_bytes())?;
        }
        BsonValue::Decimal128(v) => {
            writer.write_all(&0x13_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            writer.write_all(&v.bytes())?;
        }
        BsonValue::Regex { pattern, options } => {
            writer.write_all(&0x0B_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            write_cstring(writer, pattern)?;
            write_cstring(writer, options)?;
        }
        BsonValue::Code(code) => {
            writer.write_all(&0x0D_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            write_string(writer, code)?;
        }
        BsonValue::CodeWithScope { code, scope } => {
            writer.write_all(&0x0F_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            // 总长度包含自身 4 字节，后面紧跟代码字符串与作用域文档。
            let mut payload = Vec::new();
            payload.write_all(&0_u32.to_le_bytes())?;
            write_string(&mut payload, code)?;
            encode_document(&mut payload, scope)?;
            let total = payload.len() as u32;
            writer.write_all(&total.to_le_bytes())?;
            writer.write_all(&payload[4..])?;
        }
        BsonValue::DbPointer { namespace, id } => {
            writer.write_all(&0x0C_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            write_string(writer, namespace)?;
            writer.write_all(id)?;
        }
        BsonValue::Symbol(text) => {
            writer.write_all(&0x0E_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
            write_string(writer, text)?;
        }
        BsonValue::MinKey => {
            writer.write_all(&0xFF_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
        }
        BsonValue::MaxKey => {
            writer.write_all(&0x7F_u8.to_le_bytes())?;
            write_cstring(writer, key)?;
        }
    }
    Ok(())
}

/// Encodes a [`BsonDocument`] into a freshly allocated byte vector.
pub fn encode_bson(doc: &BsonDocument) -> Vec<u8> {
    let mut out = Vec::new();
    encode_document(&mut out, doc).expect("Vec writer never fails");
    out
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Errors that can occur while decoding BSON or the OP_MSG body that wraps it.
#[derive(Debug)]
pub enum BsonError {
    /// I/O failure reading bytes from the source buffer.
    Io(io::Error),
    /// The BSON document was malformed (wrong length, unknown type, etc.).
    Malformed(String),
    /// A field we need for command dispatch was missing or not the expected type.
    MissingField(String),
}

impl From<io::Error> for BsonError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Parses a BSON document from a byte slice.
pub fn parse_bson(input: &[u8]) -> Result<BsonDocument, BsonError> {
    let mut cursor = Cursor::new(input);
    let doc_len = read_i32(&mut cursor)?;
    if doc_len < 0 {
        return Err(BsonError::Malformed("negative BSON document length".into()));
    }
    if doc_len < 5 {
        return Err(BsonError::Malformed("BSON document too short".into()));
    }
    if doc_len as usize > input.len() {
        return Err(BsonError::Malformed("BSON length exceeds input".into()));
    }
    let mut doc = BsonDocument::new();
    loop {
        let elem_type = match read_u8(&mut cursor) {
            Ok(t) => t,
            Err(_) => {
                // Expected terminator at end
                let pos = cursor.position() as usize;
                if pos < input.len() && input[pos] == 0 {
                    break;
                }
                return Err(BsonError::Malformed("unexpected end of BSON".into()));
            }
        };
        if elem_type == 0 {
            break;
        }
        let key = read_cstring(&mut cursor)?;
        let value = parse_value(&mut cursor, elem_type)?;
        doc.push(key, value);
    }
    Ok(doc)
}

fn parse_value<R: Read>(cursor: &mut R, elem_type: u8) -> Result<BsonValue, BsonError> {
    match elem_type {
        0x01 => Ok(BsonValue::Double(read_f64(cursor)?)),
        0x02 => Ok(BsonValue::String(read_string(cursor)?)),
        0x03 => Ok(BsonValue::Document(parse_sub_document(cursor)?)),
        0x04 => Ok(BsonValue::Array(parse_sub_document(cursor)?.fields.into_iter().map(|(_, v)| v).collect())),
        0x05 => {
            let len = read_i32(cursor)?;
            if len < 0 {
                return Err(BsonError::Malformed("negative BSON binary length".into()));
            }
            let subtype = read_u8(cursor)?;
            let mut data = vec![0_u8; len as usize];
            cursor.read_exact(&mut data)?;
            if subtype == 0x02 {
                // 旧式子类型在载荷内再内嵌一份自身长度：解码时剥离，并拒绝内
                // 外长度不一致的流。
                if len < 4 {
                    return Err(BsonError::Malformed("legacy BSON binary is shorter than its length field".into()));
                }
                let embedded = read_i32(&mut Cursor::new(&data[..4]))?;
                if embedded < 0 || embedded as usize + 4 != len as usize {
                    return Err(BsonError::Malformed("legacy BSON binary length mismatch".into()));
                }
                data.drain(..4);
            }
            Ok(BsonValue::Binary(BinaryData::new(subtype, data)))
        }
        0x06 => Ok(BsonValue::Undefined),
        0x07 => {
            let mut bytes = [0_u8; 12];
            cursor.read_exact(&mut bytes)?;
            Ok(BsonValue::ObjectId(bytes))
        }
        0x08 => Ok(BsonValue::Bool(read_u8(cursor)? != 0)),
        0x09 => Ok(BsonValue::DateTime(read_i64(cursor)?)),
        0x0A => Ok(BsonValue::Null),
        0x0B => Ok(BsonValue::Regex { pattern: read_cstring(cursor)?, options: read_cstring(cursor)? }),
        0x0C => {
            let namespace = read_string(cursor)?;
            let mut id = [0_u8; 12];
            cursor.read_exact(&mut id)?;
            Ok(BsonValue::DbPointer { namespace, id })
        }
        0x0D => Ok(BsonValue::Code(read_string(cursor)?)),
        0x0E => Ok(BsonValue::Symbol(read_string(cursor)?)),
        0x0F => {
            let len = read_i32(cursor)?;
            if len < 4 {
                return Err(BsonError::Malformed("BSON code-with-scope is shorter than its length field".into()));
            }
            let mut buf = vec![0_u8; (len - 4) as usize];
            cursor.read_exact(&mut buf)?;
            let mut inner = Cursor::new(buf.as_slice());
            let code = read_string(&mut inner)?;
            let scope = parse_sub_document(&mut inner)?;
            if inner.position() as usize != buf.len() {
                return Err(BsonError::Malformed("BSON code-with-scope length mismatch".into()));
            }
            Ok(BsonValue::CodeWithScope { code, scope })
        }
        0x10 => Ok(BsonValue::Int32(read_i32(cursor)?)),
        0x11 => Ok(BsonValue::Timestamp { i: read_i32(cursor)? as u32, t: read_i32(cursor)? as u32 }),
        0x12 => Ok(BsonValue::Int64(read_i64(cursor)?)),
        0x13 => {
            let mut bytes = [0_u8; 16];
            cursor.read_exact(&mut bytes)?;
            Ok(BsonValue::Decimal128(Decimal128::from_bytes(bytes)))
        }
        0x7F => Ok(BsonValue::MaxKey),
        0xFF => Ok(BsonValue::MinKey),
        other => Err(BsonError::Malformed(format!("unsupported BSON element type {other:#x}"))),
    }
}

fn parse_sub_document<R: Read>(cursor: &mut R) -> Result<BsonDocument, BsonError> {
    let len = read_i32(cursor)?;
    if len < 0 {
        return Err(BsonError::Malformed("negative sub-document length".into()));
    }
    if len < 5 {
        return Err(BsonError::Malformed("sub-document too short".into()));
    }
    let mut buf = vec![0_u8; (len - 4) as usize];
    cursor.read_exact(&mut buf)?;
    let mut sub = Cursor::new(buf.as_slice());
    let mut doc = BsonDocument::new();
    loop {
        let elem_type = match read_u8(&mut sub) {
            Ok(t) => t,
            Err(_) => break,
        };
        if elem_type == 0 {
            break;
        }
        let key = read_cstring(&mut sub)?;
        let value = parse_value(&mut sub, elem_type)?;
        doc.push(key, value);
    }
    Ok(doc)
}

fn read_u8<R: Read>(r: &mut R) -> io::Result<u8> {
    let mut buf = [0_u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}
fn read_i32<R: Read>(r: &mut R) -> io::Result<i32> {
    let mut buf = [0_u8; 4];
    r.read_exact(&mut buf)?;
    Ok(i32::from_le_bytes(buf))
}
fn read_i64<R: Read>(r: &mut R) -> io::Result<i64> {
    let mut buf = [0_u8; 8];
    r.read_exact(&mut buf)?;
    Ok(i64::from_le_bytes(buf))
}
fn read_f64<R: Read>(r: &mut R) -> io::Result<f64> {
    let mut buf = [0_u8; 8];
    r.read_exact(&mut buf)?;
    Ok(f64::from_le_bytes(buf))
}

fn read_cstring<R: Read>(r: &mut R) -> Result<String, BsonError> {
    let mut out = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        r.read_exact(&mut byte)?;
        if byte[0] == 0 {
            break;
        }
        out.push(byte[0]);
    }
    String::from_utf8(out).map_err(|_| BsonError::Malformed("invalid UTF-8 in cstring".into()))
}

fn read_string<R: Read>(r: &mut R) -> Result<String, BsonError> {
    let len = read_i32(r)?;
    if len < 1 {
        return Err(BsonError::Malformed("invalid BSON string length".into()));
    }
    let mut buf = vec![0_u8; (len - 1) as usize];
    r.read_exact(&mut buf)?;
    let mut term = [0_u8; 1];
    r.read_exact(&mut term)?;
    String::from_utf8(buf).map_err(|_| BsonError::Malformed("invalid UTF-8 in BSON string".into()))
}

// ---------------------------------------------------------------------------
// 值 → 键字节：供上层 KV 存储路由使用
// ---------------------------------------------------------------------------

/// 把 BSON 值序列化为稳定键字节，供文档经 KV 存储路由。
/// 不同类型使用不同编码前缀，避免两个不同的 MongoDB 值撞上同一字节模式。
///
/// 数值共用同一键空间：MongoDB 按数值比较 `int32`、`int64`、`double` 与
/// `decimal128`，因此 `1`、`1.0`、`NumberLong(1)` 与 `NumberDecimal("1.0")`
/// 必须落到同一份文档，并作为 `_id` 判重时相撞。整数值统一编码为整数键，
/// 只有非整数用类型前缀键。`Symbol` 与同文本的 `String` 也共用键空间。
pub fn value_to_key(value: &BsonValue) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        BsonValue::ObjectId(bytes) => {
            out.push(b'o');
            out.extend_from_slice(bytes);
        }
        BsonValue::String(s) | BsonValue::Symbol(s) => {
            out.push(b's');
            out.extend_from_slice(s.as_bytes());
        }
        BsonValue::Int32(v) => out.extend_from_slice(&integral_key(*v as i64)),
        BsonValue::Int64(v) => out.extend_from_slice(&integral_key(*v)),
        BsonValue::Double(v) => match integral_value(*v) {
            Some(integer) => out.extend_from_slice(&integral_key(integer)),
            None => {
                out.push(b'd');
                out.extend_from_slice(&double_bits(*v).to_be_bytes());
            }
        },
        BsonValue::Decimal128(v) => match v.as_i64() {
            Some(integer) => out.extend_from_slice(&integral_key(integer)),
            None => {
                out.push(b'D');
                let (sign, exponent, coefficient) = v.key_parts();
                out.push(sign);
                out.extend_from_slice(&exponent.to_be_bytes());
                out.extend_from_slice(&coefficient.to_be_bytes());
            }
        },
        BsonValue::DateTime(v) => {
            out.push(b't');
            out.extend_from_slice(&v.to_be_bytes());
        }
        BsonValue::Timestamp { t, i } => {
            out.push(b'T');
            out.extend_from_slice(&t.to_be_bytes());
            out.extend_from_slice(&i.to_be_bytes());
        }
        BsonValue::MinKey => {
            out.push(b'm');
        }
        BsonValue::MaxKey => {
            out.push(b'M');
        }
        BsonValue::Bool(v) => {
            out.push(b'b');
            out.push(if *v { 1 } else { 0 });
        }
        BsonValue::Null => {
            out.push(b'n');
        }
        BsonValue::Undefined => {
            out.push(b'u');
        }
        other => {
            // Fallback: encode the whole value as BSON bytes for stable hashing.
            out.push(b'x');
            let mut buf = Vec::new();
            let mut doc = BsonDocument::new();
            doc.push("_id".to_string(), other.clone());
            encode_document(&mut buf, &doc).expect("Vec writer never fails");
            out.extend_from_slice(&buf);
        }
    }
    out
}

/// 返回 double 精确表示的 64 位整数；无法精确表示时返回 `None`。
///
/// MongoDB 里 `1.0` 与 `NumberLong(1)` 是同一个 `_id`，而 `1e20` 超出 `i64`
/// 可表示范围，必须保留独立键。
pub fn integral_value(value: f64) -> Option<i64> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    let integer = value as i64;
    (integer as f64 == value).then_some(integer)
}

/// 所有整数值共用的保序键前缀。
fn integral_key(value: i64) -> [u8; 9] {
    let mut key = [0_u8; 9];
    key[0] = b'n';
    key[1..].copy_from_slice(&((value as u64) ^ (1_u64 << 63)).to_be_bytes());
    key
}

/// 带小数 double 的规范位模式（`-0.0` 归并到 `0.0`）。
pub fn double_bits(value: f64) -> u64 {
    if value == 0.0 { 0 } else { value.to_bits() }
}
