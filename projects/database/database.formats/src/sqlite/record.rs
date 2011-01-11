//! SQLite record 载荷解码，与集群 KV 执行面无关。

use database_types::{Error, Result};

/// Database text encoding specified by the format-3 header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextEncoding {
    /// UTF-8.
    Utf8,
    /// UTF-16 little endian.
    Utf16Le,
    /// UTF-16 big endian.
    Utf16Be,
}

/// A decoded SQLite record field. Blob bytes borrow the supplied payload.
#[derive(Debug, PartialEq)]
pub enum RecordValue<'input> {
    /// SQL NULL.
    Null,
    /// Signed SQLite integer.
    Integer(i64),
    /// IEEE-754 binary64 value.
    Real(f64),
    /// Binary bytes without UTF-8 conversion.
    Blob(&'input [u8]),
    /// Text decoded using the database encoding.
    Text(String),
}

/// Resource limits for a single materialized record payload.
#[derive(Clone, Copy, Debug)]
pub struct RecordLimits {
    /// Maximum payload size, including header.
    pub max_payload_bytes: usize,
    /// Maximum fields in a record.
    pub max_columns: usize,
}

impl Default for RecordLimits {
    fn default() -> Self {
        Self { max_payload_bytes: 1_048_576, max_columns: 1024 }
    }
}

/// Decodes a one-to-nine-byte SQLite unsigned varint and its consumed length.
pub fn decode_varint(bytes: &[u8]) -> Result<(u64, usize)> {
    let mut value = 0u64;
    for index in 0..9 {
        let byte = *bytes.get(index).ok_or(Error::Corrupt("truncated sqlite varint"))?;
        if index == 8 {
            return Ok(((value << 8) | u64::from(byte), 9));
        }
        value = (value << 7) | u64::from(byte & 0x7f);
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }
    unreachable!()
}

/// Decodes one complete record payload after page/overflow assembly.
/// It does not traverse b-trees, recover journals, or execute SQL.
pub fn decode_record(payload: &[u8], encoding: TextEncoding, limits: RecordLimits) -> Result<Vec<RecordValue<'_>>> {
    if payload.len() > limits.max_payload_bytes {
        return Err(Error::Unsupported("sqlite record payload limit exceeded"));
    }
    let (header_size, mut header_offset) = decode_varint(payload)?;
    let header_size = usize::try_from(header_size).map_err(|_| Error::Corrupt("sqlite record header size overflow"))?;
    if header_size < header_offset || header_size > payload.len() {
        return Err(Error::Corrupt("sqlite record header size invalid"));
    }
    let mut body_offset = header_size;
    let mut values = Vec::new();
    while header_offset < header_size {
        if values.len() >= limits.max_columns {
            return Err(Error::Unsupported("sqlite record column limit exceeded"));
        }
        let (serial, consumed) = decode_varint(&payload[header_offset..header_size])?;
        header_offset += consumed;
        let length = match serial {
            0 | 8 | 9 => 0,
            1 => 1,
            2 => 2,
            3 => 3,
            4 => 4,
            5 => 6,
            6 | 7 => 8,
            10 | 11 => return Err(Error::Corrupt("sqlite reserved serial type")),
            serial => usize::try_from((serial - 12) / 2).map_err(|_| Error::Corrupt("sqlite serial length overflow"))?,
        };
        let end = body_offset.checked_add(length).filter(|end| *end <= payload.len()).ok_or(Error::Corrupt("truncated sqlite record body"))?;
        let bytes = &payload[body_offset..end];
        let value = match serial {
            0 => RecordValue::Null,
            1..=6 => RecordValue::Integer(signed_integer(bytes)),
            7 => RecordValue::Real(f64::from_bits(u64::from_be_bytes(bytes.try_into().expect("eight-byte real")))),
            8 => RecordValue::Integer(0),
            9 => RecordValue::Integer(1),
            serial if serial % 2 == 0 => RecordValue::Blob(bytes),
            _ => RecordValue::Text(decode_text(bytes, encoding)?),
        };
        values.push(value);
        body_offset = end;
    }
    if body_offset != payload.len() {
        return Err(Error::Corrupt("sqlite record has trailing body bytes"));
    }
    Ok(values)
}

/// An owned value that can be encoded into a SQLite record payload.
#[derive(Clone, Debug, PartialEq)]
pub enum WriteValue {
    /// SQL NULL.
    Null,
    /// Signed SQLite integer.
    Integer(i64),
    /// IEEE-754 binary64 value.
    Real(f64),
    /// Binary bytes.
    Blob(Vec<u8>),
    /// UTF-8 text.
    Text(String),
}

/// Appends one SQLite unsigned varint (one to nine bytes) to `out`.
pub fn encode_varint_into(out: &mut Vec<u8>, value: u64) {
    if value > 0x00ff_ffff_ffff_ffff {
        let mut bytes = [0u8; 9];
        for (index, byte) in bytes.iter_mut().take(8).enumerate() {
            *byte = (((value >> (57 - 7 * index)) & 0x7f) as u8) | 0x80;
        }
        bytes[8] = (value & 0xff) as u8;
        out.extend_from_slice(&bytes);
        return;
    }
    let mut bytes = [0u8; 8];
    let mut index = 7;
    bytes[index] = (value & 0x7f) as u8;
    let mut rest = value >> 7;
    while rest > 0 {
        index -= 1;
        bytes[index] = ((rest & 0x7f) as u8) | 0x80;
        rest >>= 7;
    }
    out.extend_from_slice(&bytes[index..]);
}

/// Returns the serial type used for `value` and appends its body bytes to `body`.
fn serial_and_body(value: &WriteValue, encoding: TextEncoding, body: &mut Vec<u8>) -> u64 {
    match value {
        WriteValue::Null => 0,
        WriteValue::Integer(number) => {
            if *number == 0 {
                return 8;
            }
            if *number == 1 {
                return 9;
            }
            let width = signed_width(*number);
            // Serial 5 stores six bytes, so five- and six-byte values share it;
            // seven- and eight-byte values need the eight-byte serial 6.
            let serial = if width <= 4 {
                width as u64
            }
            else if width <= 6 {
                5
            }
            else {
                6
            };
            let bytes = (*number).to_be_bytes();
            body.extend_from_slice(&bytes[8 - width_clamp(serial)..]);
            serial
        }
        WriteValue::Real(real) => {
            body.extend_from_slice(&real.to_bits().to_be_bytes());
            7
        }
        WriteValue::Blob(bytes) => {
            body.extend_from_slice(bytes);
            12 + 2 * bytes.len() as u64
        }
        WriteValue::Text(text) => {
            let bytes = encode_text(text, encoding);
            let serial = 13 + 2 * bytes.len() as u64;
            body.extend_from_slice(&bytes);
            serial
        }
    }
}

/// Smallest two's-complement width in bytes that holds `number`.
///
/// Positive values whose top magnitude bit is set need an extra sign byte
/// (128 stores as `0x0080`, not `0x80`), and negative values are bounded by
/// the signed minimum of each width (-129 needs two bytes).
fn signed_width(number: i64) -> usize {
    for width in [1usize, 2, 3, 4, 6, 8] {
        let bits = width * 8 - 1;
        let min = if bits == 63 { i64::MIN } else { -(1i64 << bits) };
        let max = if bits == 63 { i64::MAX } else { (1i64 << bits) - 1 };
        if number >= min && number <= max {
            return width;
        }
    }
    8
}

/// Body width in bytes of the integer serial chosen for `serial`.
fn width_clamp(serial: u64) -> usize {
    match serial {
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        _ => 8,
    }
}

/// Encodes text in the database encoding, including surrogate pairs for
/// characters outside the basic multilingual plane.
fn encode_text(text: &str, encoding: TextEncoding) -> Vec<u8> {
    if encoding == TextEncoding::Utf8 {
        return text.as_bytes().to_vec();
    }
    let mut bytes = Vec::with_capacity(text.len() * 2);
    for unit in text.encode_utf16() {
        match encoding {
            TextEncoding::Utf16Le => bytes.extend_from_slice(&unit.to_le_bytes()),
            TextEncoding::Utf16Be => bytes.extend_from_slice(&unit.to_be_bytes()),
            TextEncoding::Utf8 => unreachable!(),
        }
    }
    bytes
}

/// Encodes a complete record payload (header size varint, serial header, body).
/// Text fields are stored in `encoding`, matching the database header.
pub fn encode_record(values: &[WriteValue], encoding: TextEncoding) -> Vec<u8> {
    let mut header = Vec::new();
    let mut body = Vec::new();
    for value in values {
        let serial = serial_and_body(value, encoding, &mut body);
        encode_varint_into(&mut header, serial);
    }
    let mut payload = Vec::with_capacity(header.len() + body.len() + 1);
    encode_varint_into(&mut payload, (header.len() + 1) as u64);
    payload.extend_from_slice(&header);
    payload.extend_from_slice(&body);
    payload
}

fn signed_integer(bytes: &[u8]) -> i64 {
    let mut value = if bytes[0] & 0x80 != 0 { u64::MAX } else { 0 };
    for byte in bytes {
        value = (value << 8) | u64::from(*byte)
    }
    value as i64
}

fn decode_text(bytes: &[u8], encoding: TextEncoding) -> Result<String> {
    if encoding == TextEncoding::Utf8 {
        return std::str::from_utf8(bytes).map(str::to_owned).map_err(|_| Error::Unsupported("sqlite non-unicode UTF-8 text"));
    }
    if bytes.len() % 2 != 0 {
        return Err(Error::Corrupt("sqlite odd UTF-16 byte length"));
    }
    let words = bytes.chunks_exact(2).map(|chunk| match encoding {
        TextEncoding::Utf16Le => u16::from_le_bytes([chunk[0], chunk[1]]),
        TextEncoding::Utf16Be => u16::from_be_bytes([chunk[0], chunk[1]]),
        TextEncoding::Utf8 => unreachable!(),
    });
    char::decode_utf16(words).collect::<std::result::Result<String, _>>().map_err(|_| Error::Unsupported("sqlite non-unicode UTF-16 text"))
}
