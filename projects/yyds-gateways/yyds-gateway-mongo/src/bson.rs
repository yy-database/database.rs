//! Minimal BSON document serializer / parser sufficient for MongoDB OP_MSG
//! wire handling inside the YYDS Mongo gateway.
//!
//! Only the element types we actually need are covered: Document (0x03),
//! Array (0x04), String (0x02), Int32 (0x10), Int64 (0x12), Double (0x01),
//! Bool (0x08), Null (0x0A), ObjectId (0x07), and Binary (0x05).

use std::io::{self, Cursor, Read, Write};

/// BSON value variants supported by this gateway.
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
    /// 12-byte ObjectId (0x07).
    ObjectId([u8; 12]),
    /// Binary payload (0x05, subtype 0x00 = generic).
    Binary(Vec<u8>),
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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, key: impl Into<String>, value: BsonValue) {
        self.fields.push((key.into(), value));
    }

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
            writer.write_all(&(v.len() as i32).to_le_bytes())?;
            writer.write_all(&0x00_u8.to_le_bytes())?;
            writer.write_all(v)?;
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
    let doc_len = read_u32(&mut cursor)?;
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
            let _subtype = read_u8(cursor)?;
            let mut data = vec![0_u8; len as usize];
            cursor.read_exact(&mut data)?;
            Ok(BsonValue::Binary(data))
        }
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
    let len = read_u32(cursor)?;
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
fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut buf = [0_u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
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
// Value → key-bytes coercion for YYDS KV storage
// ---------------------------------------------------------------------------

/// Serializes a BSON value into a stable key bytes buffer so documents can be routed
/// through the YYDS KV store. Different types get different encodings so two distinct
/// MongoDB values cannot collide on the same byte pattern.
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
        BsonValue::Int32(v) => {
            out.push(b'i');
            out.extend_from_slice(&v.to_le_bytes());
        }
        BsonValue::Int64(v) => {
            out.push(b'l');
            out.extend_from_slice(&v.to_le_bytes());
        }
        BsonValue::Double(v) => {
            out.push(b'd');
            out.extend_from_slice(&v.to_le_bytes());
        }
        BsonValue::Bool(v) => {
            out.push(b'b');
            out.push(if *v { 1 } else { 0 });
        }
        BsonValue::Null => {
            out.push(b'n');
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
