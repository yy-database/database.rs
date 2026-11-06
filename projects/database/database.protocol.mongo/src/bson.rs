//! 足以支撑 MongoDB OP_MSG 线处理的 BSON 文档序列化 / 解析。
//!
//! 覆盖元素类型：Document (0x03)、Array (0x04)、String (0x02)、Int32 (0x10)、
//! Int64 (0x12)、Double (0x01)、Bool (0x08)、Null (0x0A)、Undefined (0x06)、
//! ObjectId (0x07) 与 Binary (0x05，保留子类型)。

use std::io::{self, Cursor, Read, Write};

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
        0x0A => Ok(BsonValue::Null),
        0x10 => Ok(BsonValue::Int32(read_i32(cursor)?)),
        0x12 => Ok(BsonValue::Int64(read_i64(cursor)?)),
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
/// 数值共用同一键空间：MongoDB 按数值比较 `int32`、`int64` 与 `double`，
/// 因此 `1`、`1.0` 与 `NumberLong(1)` 必须落到同一份文档，并作为 `_id`
/// 判重时相撞。整数值统一编码为整数键，只有带小数的 double 用位模式键。
pub fn value_to_key(value: &BsonValue) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        BsonValue::ObjectId(bytes) => {
            out.push(b'o');
            out.extend_from_slice(bytes);
        }
        BsonValue::String(s) => {
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
