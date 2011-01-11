//! `YYRL` durable replica-log frame codec.

use std::io::Write;

use database_types::cluster::{Error, LeaderTerm, LogIndex, ReplicationEntry, Result};

/// Replication log magic.
pub const MAGIC: &[u8] = b"YYRL\x01";
/// Append-entry frame kind.
pub const APPEND: u8 = 1;
/// Commit-marker frame kind.
pub const COMMIT: u8 = 2;
/// Bytes before the frame body.
pub const FRAME_HEADER: usize = 1 + 4;
/// Trailing checksum length.
pub const FRAME_TRAILER: usize = 4;

/// Encode one replication entry body (without frame wrapper).
pub fn encode_entry(entry: &ReplicationEntry) -> Result<Vec<u8>> {
    let payload_len = u32::try_from(entry.payload.len()).map_err(|_| Error::Unsupported("replication payload too large"))?;
    let mut body = Vec::with_capacity(20 + entry.payload.len());
    body.extend_from_slice(&entry.index.0.to_le_bytes());
    body.extend_from_slice(&entry.term.0.to_le_bytes());
    body.extend_from_slice(&payload_len.to_le_bytes());
    body.extend_from_slice(&entry.payload);
    Ok(body)
}

/// Decode one replication entry body.
pub fn decode_entry(body: &[u8]) -> Result<ReplicationEntry> {
    if body.len() < 20 {
        return Err(Error::Corrupt("truncated replication entry"));
    }
    let index = LogIndex(u64::from_le_bytes(body[..8].try_into().unwrap()));
    let term = LeaderTerm(u64::from_le_bytes(body[8..16].try_into().unwrap()));
    let payload_len = u32::from_le_bytes(body[16..20].try_into().unwrap()) as usize;
    if body.len() != 20 + payload_len {
        return Err(Error::Corrupt("invalid replication payload length"));
    }
    Ok(ReplicationEntry { index, term, payload: body[20..].to_vec() })
}

/// Build a checksummed frame around `body`.
pub fn encode_frame(kind: u8, body: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(body.len()).map_err(|_| Error::Unsupported("replication frame too large"))?;
    let mut frame = Vec::with_capacity(FRAME_HEADER + body.len() + FRAME_TRAILER);
    frame.push(kind);
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(body);
    let sum = checksum(&frame);
    frame.extend_from_slice(&sum.to_le_bytes());
    Ok(frame)
}

/// FNV-1a 32-bit over frame bytes before the trailer.
pub fn checksum(bytes: &[u8]) -> u32 {
    let mut value = 0x811c9dc5_u32;
    for byte in bytes {
        value ^= u32::from(*byte);
        value = value.wrapping_mul(0x01000193);
    }
    value
}

/// Write magic-only empty log.
pub fn encode_empty_log() -> Vec<u8> {
    let mut out = Vec::with_capacity(MAGIC.len());
    out.write_all(MAGIC).expect("vec write");
    out
}
