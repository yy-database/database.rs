//! `.yykv` shard snapshot codec.

use std::{
    collections::{BTreeMap, HashSet},
    io::{Cursor, Read, Write},
};

use database_types::cluster::{Error, Result};

use super::{InlineValue, Key, ObjectRef, Record, StoredValue};

/// `.yykv` snapshot magic including the format version.
///
/// Version 3 added a body checksum. Version 2 files are rejected explicitly.
pub const MAGIC: &[u8] = b"YYKV\x03";
/// Legacy v2 magic (no checksum) — open must refuse and ask for recreate.
pub const LEGACY_MAGIC: &[u8] = b"YYKV\x02";

/// Decoded snapshot body (no path / IO).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Next revision to allocate.
    pub next_revision: u64,
    /// Records keyed for O(log n) lookup.
    pub records: BTreeMap<Key, Record>,
}

/// Encode an in-memory record map to `.yykv` bytes.
pub fn encode_snapshot(records: &BTreeMap<Key, Record>, next_revision: u64) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    body.write_all(&next_revision.to_le_bytes())?;
    write_count(&mut body, records.len())?;
    for record in records.values() {
        write_bytes(&mut body, record.key.namespace.0.as_bytes())?;
        write_bytes(&mut body, &record.key.bytes)?;
        body.write_all(&record.revision.to_le_bytes())?;
        let encoded_expiry = record.expires_at.map(|deadline| deadline as u64).unwrap_or(0);
        body.write_all(&encoded_expiry.to_le_bytes())?;
        match &record.value {
            StoredValue::Inline(value) => {
                body.write_all(&[0])?;
                write_bytes(&mut body, &value.0)?;
            }
            StoredValue::Object(value) => {
                body.write_all(&[1])?;
                write_bytes(&mut body, value.hash_hex.as_bytes())?;
            }
        }
    }
    let mut bytes = Vec::with_capacity(MAGIC.len() + 8 + body.len());
    bytes.write_all(MAGIC)?;
    bytes.write_all(&checksum(&body).to_le_bytes())?;
    bytes.write_all(&body)?;
    Ok(bytes)
}

/// Decode `.yykv` bytes into a [`Snapshot`].
pub fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot> {
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0_u8; MAGIC.len()];
    read_exact(&mut cursor, &mut magic)?;
    if magic == LEGACY_MAGIC {
        return Err(Error::Corrupt("legacy yykv v2 snapshot has no checksum: delete the shard file and let the node recreate it"));
    }
    if magic != MAGIC {
        return Err(Error::Corrupt("unknown yykv header"));
    }
    let expected_checksum = read_u64(&mut cursor)?;
    let body_start = MAGIC.len() + 8;
    let body = bytes.get(body_start..).ok_or(Error::Corrupt("truncated yykv checksum"))?;
    if checksum(body) != expected_checksum {
        return Err(Error::Corrupt("yykv checksum mismatch"));
    }
    let mut cursor = Cursor::new(body);
    let next_revision = read_u64(&mut cursor)?;
    let count = read_u32(&mut cursor)? as usize;
    let mut records = BTreeMap::new();
    let mut revisions = HashSet::with_capacity(count);
    for _ in 0..count {
        let namespace = String::from_utf8(read_bytes(&mut cursor)?).map_err(|_| Error::Corrupt("shard namespace utf8"))?;
        let key = read_bytes(&mut cursor)?;
        let revision = read_u64(&mut cursor)?;
        if revision == 0 || revision > next_revision || !revisions.insert(revision) {
            return Err(Error::Corrupt("invalid shard revision"));
        }
        let encoded_expiry = read_u64(&mut cursor)?;
        let expires_at = (encoded_expiry != 0).then_some(encoded_expiry as i64);
        let value = match read_u8(&mut cursor)? {
            0 => StoredValue::Inline(InlineValue(read_bytes(&mut cursor)?)),
            1 => StoredValue::Object(ObjectRef {
                hash_hex: String::from_utf8(read_bytes(&mut cursor)?).map_err(|_| Error::Corrupt("shard object hash utf8"))?,
            }),
            _ => return Err(Error::Corrupt("invalid shard value kind")),
        };
        let record = Record { key: Key::new(namespace, key), value, revision, expires_at };
        if records.insert(record.key.clone(), record).is_some() {
            return Err(Error::Corrupt("duplicate shard key"));
        }
    }
    if cursor.position() != body.len() as u64 {
        return Err(Error::Corrupt("trailing shard bytes"));
    }
    Ok(Snapshot { next_revision, records })
}

/// FNV-1a over the snapshot body (integrity, not security).
pub fn checksum(body: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in body {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn write_count(bytes: &mut Vec<u8>, count: usize) -> Result<()> {
    let count = u32::try_from(count).map_err(|_| Error::Unsupported("too many shard records"))?;
    bytes.write_all(&count.to_le_bytes())?;
    Ok(())
}

fn write_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| Error::Unsupported("shard value too large"))?;
    bytes.write_all(&length.to_le_bytes())?;
    bytes.write_all(value)?;
    Ok(())
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut value = [0_u8; 1];
    read_exact(cursor, &mut value)?;
    Ok(value[0])
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut value = [0_u8; 4];
    read_exact(cursor, &mut value)?;
    Ok(u32::from_le_bytes(value))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut value = [0_u8; 8];
    read_exact(cursor, &mut value)?;
    Ok(u64::from_le_bytes(value))
}

fn read_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>> {
    let length = read_u32(cursor)? as usize;
    let position = cursor.position() as usize;
    let end = position.checked_add(length).ok_or(Error::Corrupt("shard length overflow"))?;
    if end > cursor.get_ref().len() {
        return Err(Error::Corrupt("unexpected shard eof"));
    }
    let mut value = vec![0_u8; length];
    read_exact(cursor, &mut value)?;
    Ok(value)
}

fn read_exact(cursor: &mut Cursor<&[u8]>, bytes: &mut [u8]) -> Result<()> {
    cursor.read_exact(bytes).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof { Error::Corrupt("unexpected shard eof") } else { Error::Io(error) }
    })
}
