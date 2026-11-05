//! `.yykv` shard snapshot codec.

use std::{
    collections::{BTreeMap, HashSet},
    io::{Cursor, Read, Write},
};

use database_types::cluster::{Error, Result};
use database_types::ReasonCode;

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
///
/// `next_revision` is the next revision to allocate, following the PostgreSQL
/// shard contract: an empty shard starts at `1`, allocated revisions are
/// strictly below it, and the writer bumps it after every allocation.
pub fn encode_snapshot(records: &BTreeMap<Key, Record>, next_revision: u64) -> Result<Vec<u8>> {
    if next_revision == 0 {
        return Err(Error::Unsupported("yykv next_revision must be at least 1"));
    }
    if records.values().any(|record| record.revision == 0 || record.revision >= next_revision) {
        return Err(Error::Unsupported("yykv revision must be below next_revision"));
    }
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

/// Decode `.yykv` bytes into a [`Snapshot`], accepting legacy revision layouts.
///
/// Use [`decode_snapshot_strict`] when the caller must reject pre-contract
/// shards instead of normalizing them.
pub fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot> {
    decode_with(bytes, RevisionMode::LegacyTolerant, ValueMode::Any)
}

/// Decode `.yykv` bytes while enforcing the PostgreSQL revision contract.
///
/// Rejects legacy v2 magic, an empty shard recorded as revision `0`, and any
/// record sitting on `next_revision`. Corruption carries [`ReasonCode`] so a
/// host can branch on the failure class without parsing text.
pub fn decode_snapshot_strict(bytes: &[u8]) -> Result<Snapshot> {
    decode_with(bytes, RevisionMode::Contract, ValueMode::Any)
}

/// Strict decode that also rejects object-backed values.
///
/// Use this on a node that has no object store bound: an inline-only reader
/// must not silently treat a hash reference as row data.
pub fn decode_snapshot_inline_only(bytes: &[u8]) -> Result<Snapshot> {
    decode_with(bytes, RevisionMode::Contract, ValueMode::InlineOnly)
}

#[derive(Clone, Copy)]
enum RevisionMode {
    Contract,
    LegacyTolerant,
}

#[derive(Clone, Copy)]
enum ValueMode {
    Any,
    InlineOnly,
}

fn corrupt(reason: ReasonCode, message: &'static str) -> Error {
    Error::CorruptReason { reason, message }
}

fn decode_with(bytes: &[u8], revision_mode: RevisionMode, value_mode: ValueMode) -> Result<Snapshot> {
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0_u8; MAGIC.len()];
    read_exact(&mut cursor, &mut magic)?;
    if magic == LEGACY_MAGIC {
        return Err(corrupt(
            ReasonCode::LegacyVersion,
            "legacy yykv v2 snapshot has no checksum: delete the shard file and let the node recreate it",
        ));
    }
    if magic != MAGIC {
        return Err(corrupt(ReasonCode::BadMagic, "unknown yykv header"));
    }
    let expected_checksum = read_u64(&mut cursor)?;
    let body_start = MAGIC.len() + 8;
    let body = bytes.get(body_start..).ok_or_else(|| corrupt(ReasonCode::Truncated, "truncated yykv checksum"))?;
    if checksum(body) != expected_checksum {
        return Err(corrupt(ReasonCode::ChecksumMismatch, "yykv checksum mismatch"));
    }
    let mut cursor = Cursor::new(body);
    let mut next_revision = read_u64(&mut cursor)?;
    if next_revision == 0 && matches!(revision_mode, RevisionMode::Contract) {
        return Err(corrupt(ReasonCode::LegacyVersion, "legacy yykv revision layout: next_revision is zero"));
    }
    let count = read_u32(&mut cursor)? as usize;
    let mut records = BTreeMap::new();
    let mut revisions = HashSet::with_capacity(count);
    let mut legacy_revision = false;
    for _ in 0..count {
        let namespace = String::from_utf8(read_bytes(&mut cursor)?)
            .map_err(|_| corrupt(ReasonCode::UnknownTag, "shard namespace utf8"))?;
        let key = read_bytes(&mut cursor)?;
        let revision = read_u64(&mut cursor)?;
        if revision == 0 || revision > next_revision || !revisions.insert(revision) {
            return Err(Error::Corrupt("invalid shard revision"));
        }
        // Shards written before the PostgreSQL contract used
        // `next_revision` as the last allocated revision, so its newest
        // record can match it. Normalize that on read.
        legacy_revision |= revision == next_revision;
        if legacy_revision && matches!(revision_mode, RevisionMode::Contract) {
            return Err(corrupt(ReasonCode::LegacyVersion, "legacy yykv revision layout: record sits on next_revision"));
        }
        let encoded_expiry = read_u64(&mut cursor)?;
        let expires_at = (encoded_expiry != 0).then_some(encoded_expiry as i64);
        let value = match read_u8(&mut cursor)? {
            0 => StoredValue::Inline(InlineValue(read_bytes(&mut cursor)?)),
            1 => {
                if matches!(value_mode, ValueMode::InlineOnly) {
                    return Err(Error::Unsupported("yykv object-backed value requires an object store"));
                }
                StoredValue::Object(ObjectRef {
                    hash_hex: String::from_utf8(read_bytes(&mut cursor)?)
                        .map_err(|_| corrupt(ReasonCode::UnknownTag, "shard object hash utf8"))?,
                })
            }
            _ => return Err(corrupt(ReasonCode::UnknownTag, "invalid shard value kind")),
        };
        let record = Record { key: Key::new(namespace, key), value, revision, expires_at };
        if records.insert(record.key.clone(), record).is_some() {
            return Err(corrupt(ReasonCode::DuplicateKey, "duplicate shard key"));
        }
    }
    if cursor.position() != body.len() as u64 {
        return Err(corrupt(ReasonCode::TrailingBytes, "trailing shard bytes"));
    }
    if next_revision == 0 {
        // Legacy empty shard: the next allocation was stored as `0`.
        next_revision = 1;
    } else if legacy_revision {
        next_revision =
            next_revision.checked_add(1).ok_or(Error::Corrupt("legacy shard revision space exhausted"))?;
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
    let end = position.checked_add(length).ok_or_else(|| corrupt(ReasonCode::Truncated, "shard length overflow"))?;
    if end > cursor.get_ref().len() {
        return Err(corrupt(ReasonCode::Truncated, "unexpected shard eof"));
    }
    let mut value = vec![0_u8; length];
    read_exact(cursor, &mut value)?;
    Ok(value)
}

fn read_exact(cursor: &mut Cursor<&[u8]>, bytes: &mut [u8]) -> Result<()> {
    cursor.read_exact(bytes).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            corrupt(ReasonCode::Truncated, "unexpected shard eof")
        }
        else {
            Error::Io(error)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrap(body: Vec<u8>) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&checksum(&body).to_le_bytes());
        bytes.extend_from_slice(&body);
        bytes
    }

    fn legacy_body(next_revision: u64, revision: Option<u64>) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&next_revision.to_le_bytes());
        write_count(&mut body, usize::from(revision.is_some())).expect("count");
        if let Some(revision) = revision {
            let key = Key::new("ns", b"k".to_vec());
            write_bytes(&mut body, key.namespace.0.as_bytes()).expect("namespace");
            write_bytes(&mut body, &key.bytes).expect("key");
            body.extend_from_slice(&revision.to_le_bytes());
            body.extend_from_slice(&0_u64.to_le_bytes());
            body.push(0);
            write_bytes(&mut body, b"v").expect("value");
        }
        body
    }

    #[test]
    fn legacy_empty_shard_zero_becomes_one() {
        let snapshot = decode_snapshot(&wrap(legacy_body(0, None))).expect("decode");
        assert_eq!(snapshot.next_revision, 1);
    }

    #[test]
    fn legacy_last_allocated_revision_is_bumped() {
        let snapshot = decode_snapshot(&wrap(legacy_body(1, Some(1)))).expect("decode");
        assert_eq!(snapshot.next_revision, 2);
    }

    #[test]
    fn new_format_next_revision_is_kept() {
        let snapshot = decode_snapshot(&wrap(legacy_body(2, Some(1)))).expect("decode");
        assert_eq!(snapshot.next_revision, 2);
    }

    fn push_record(body: &mut Vec<u8>, namespace: &str, key: &[u8], revision: u64, tag: u8, value: &[u8]) {
        write_bytes(body, namespace.as_bytes()).expect("namespace");
        write_bytes(body, key).expect("key");
        body.extend_from_slice(&revision.to_le_bytes());
        body.extend_from_slice(&0_u64.to_le_bytes());
        body.push(tag);
        if tag == 0 || tag == 1 {
            write_bytes(body, value).expect("value");
        }
    }

    fn reason_of(bytes: &[u8]) -> ReasonCode {
        match decode_snapshot_strict(bytes) {
            Err(error) => error.reason().expect("decoder must classify the failure"),
            Ok(_) => panic!("strict decode must reject corrupted bytes"),
        }
    }

    #[test]
    fn strict_decode_rejects_legacy_layout() {
        assert_eq!(reason_of(&wrap(legacy_body(0, None))), ReasonCode::LegacyVersion);
        assert_eq!(reason_of(&wrap(legacy_body(1, Some(1)))), ReasonCode::LegacyVersion);
    }

    #[test]
    fn strict_decode_classifies_bad_magic() {
        assert_eq!(reason_of(b"XXXX\x03not-a-shard"), ReasonCode::BadMagic);
    }

    #[test]
    fn strict_decode_classifies_legacy_magic() {
        let mut bytes = LEGACY_MAGIC.to_vec();
        bytes.extend_from_slice(&[0; 8]);
        assert_eq!(reason_of(&bytes), ReasonCode::LegacyVersion);
    }

    #[test]
    fn strict_decode_classifies_truncation() {
        assert_eq!(reason_of(MAGIC), ReasonCode::Truncated);
        let mut body = Vec::new();
        body.extend_from_slice(&2_u64.to_le_bytes());
        assert_eq!(reason_of(&wrap(body)), ReasonCode::Truncated);
    }

    #[test]
    fn strict_decode_classifies_a_checksum_mismatch() {
        let mut bytes = wrap(legacy_body(2, None));
        let body_start = MAGIC.len() + 8;
        bytes[body_start] ^= 0xff;
        assert_eq!(reason_of(&bytes), ReasonCode::ChecksumMismatch);
    }

    #[test]
    fn strict_decode_classifies_unknown_tags() {
        let mut body = Vec::new();
        body.extend_from_slice(&2_u64.to_le_bytes());
        write_count(&mut body, 1).expect("count");
        push_record(&mut body, "ns", b"k", 1, 9, b"");
        assert_eq!(reason_of(&wrap(body)), ReasonCode::UnknownTag);
    }

    #[test]
    fn strict_decode_classifies_duplicate_keys() {
        let mut body = Vec::new();
        body.extend_from_slice(&3_u64.to_le_bytes());
        write_count(&mut body, 2).expect("count");
        push_record(&mut body, "ns", b"k", 1, 0, b"a");
        push_record(&mut body, "ns", b"k", 2, 0, b"b");
        assert_eq!(reason_of(&wrap(body)), ReasonCode::DuplicateKey);
    }

    #[test]
    fn strict_decode_classifies_trailing_bytes() {
        let mut body = legacy_body(2, None);
        body.push(0);
        assert_eq!(reason_of(&wrap(body)), ReasonCode::TrailingBytes);
    }

    #[test]
    fn inline_only_decode_rejects_object_values() {
        let mut body = Vec::new();
        body.extend_from_slice(&2_u64.to_le_bytes());
        write_count(&mut body, 1).expect("count");
        push_record(&mut body, "ns", b"k", 1, 1, b"hash");
        assert!(matches!(decode_snapshot_inline_only(&wrap(body)), Err(Error::Unsupported(_))));
    }
}
