use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use yyds_types::{Error, LeaderTerm, LogIndex, ReplicationEntry, Result};

const MAGIC: &[u8] = b"YYRL\x01";
const APPEND: u8 = 1;
const COMMIT: u8 = 2;
const FRAME_HEADER: usize = 1 + 4;
const FRAME_TRAILER: usize = 4;

/// A durable append and commit log for one YYDS shard.
///
/// This format persists log entries and the committed prefix only. It does not
/// implement transport, leader election, quorum acknowledgement, snapshots,
/// truncation, or consensus recovery.
#[derive(Debug)]
pub struct DurableReplicationLog {
    path: PathBuf,
    entries: Vec<ReplicationEntry>,
    committed: LogIndex,
}

impl DurableReplicationLog {
    /// Opens an existing log or creates a new log at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let bytes = fs::read(&path)?;
            decode(&path, &bytes)
        }
        else {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }
            let log = Self { path, entries: Vec::new(), committed: LogIndex(0) };
            log.initialize()?;
            Ok(log)
        }
    }

    /// Returns the backing replication log path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns all recovered entries in log order.
    pub fn entries(&self) -> &[ReplicationEntry] {
        &self.entries
    }

    /// Returns the recovered committed prefix.
    pub fn committed(&self) -> LogIndex {
        self.committed
    }

    /// Iterates entries covered by the durable committed prefix.
    pub fn committed_entries(&self) -> impl Iterator<Item = &ReplicationEntry> {
        self.entries.iter().take(self.committed.0 as usize)
    }

    /// Appends the next contiguous entry and synchronizes it to disk.
    pub fn append(&mut self, entry: ReplicationEntry) -> Result<()> {
        let expected = LogIndex(self.entries.len() as u64 + 1);
        if entry.index != expected {
            return Err(Error::Corrupt("non-contiguous replication index"));
        }
        if entry.term.0 == 0 {
            return Err(Error::Corrupt("replication entry has zero term"));
        }
        let body = encode_entry(&entry)?;
        append_frame(&self.path, APPEND, &body)?;
        self.entries.push(entry);
        Ok(())
    }

    /// Commits the next contiguous entry and synchronizes the marker to disk.
    pub fn commit(&mut self, index: LogIndex) -> Result<()> {
        let expected = LogIndex(self.committed.0.checked_add(1).ok_or(Error::Corrupt("replication commit exhausted"))?);
        if index != expected {
            return Err(Error::Corrupt("non-contiguous replication commit"));
        }
        if !self.entries.iter().any(|entry| entry.index == index) {
            return Err(Error::Corrupt("replication commit has unknown index"));
        }
        append_frame(&self.path, COMMIT, &index.0.to_le_bytes())?;
        self.committed = index;
        Ok(())
    }

    fn initialize(&self) -> Result<()> {
        let mut file = OpenOptions::new().create_new(true).write(true).open(&self.path)?;
        file.write_all(MAGIC)?;
        file.sync_all()?;
        Ok(())
    }
}

fn append_frame(path: &Path, kind: u8, body: &[u8]) -> Result<()> {
    let length = u32::try_from(body.len()).map_err(|_| Error::Unsupported("replication frame too large"))?;
    let mut frame = Vec::with_capacity(FRAME_HEADER + body.len() + FRAME_TRAILER);
    frame.push(kind);
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(body);
    frame.extend_from_slice(&checksum(&frame).to_le_bytes());
    let mut file = OpenOptions::new().append(true).open(path)?;
    file.write_all(&frame)?;
    file.sync_all()?;
    Ok(())
}

fn decode(path: &Path, bytes: &[u8]) -> Result<DurableReplicationLog> {
    if bytes.len() < MAGIC.len() || &bytes[..MAGIC.len()] != MAGIC {
        return Err(Error::Corrupt("unknown yyds replication log header"));
    }
    let mut entries = Vec::new();
    let mut committed = LogIndex(0);
    let mut offset = MAGIC.len();
    while offset < bytes.len() {
        let frame_start = offset;
        if bytes.len() - offset < FRAME_HEADER {
            break;
        }
        let kind = bytes[offset];
        let length = u32::from_le_bytes(bytes[offset + 1..offset + 5].try_into().unwrap()) as usize;
        let body_start = offset + FRAME_HEADER;
        let body_end = match body_start.checked_add(length) {
            Some(value) => value,
            None => break,
        };
        let frame_end = match body_end.checked_add(FRAME_TRAILER) {
            Some(value) => value,
            None => break,
        };
        if frame_end > bytes.len() {
            break;
        }
        let expected = u32::from_le_bytes(bytes[body_end..frame_end].try_into().unwrap());
        if checksum(&bytes[frame_start..body_end]) != expected {
            return Err(Error::Corrupt("replication frame checksum"));
        }
        let body = &bytes[body_start..body_end];
        match kind {
            APPEND => {
                let entry = decode_entry(body)?;
                let expected_index = LogIndex(entries.len() as u64 + 1);
                if entry.index != expected_index || entry.term.0 == 0 {
                    return Err(Error::Corrupt("invalid replication entry sequence"));
                }
                entries.push(entry);
            }
            COMMIT => {
                if body.len() != 8 {
                    return Err(Error::Corrupt("invalid replication commit frame"));
                }
                let index = LogIndex(u64::from_le_bytes(body.try_into().unwrap()));
                let expected = LogIndex(committed.0.checked_add(1).ok_or(Error::Corrupt("replication commit exhausted"))?);
                if index != expected || !entries.iter().any(|entry| entry.index == index) {
                    return Err(Error::Corrupt("invalid replication commit sequence"));
                }
                committed = index;
            }
            _ => return Err(Error::Corrupt("unknown replication frame kind")),
        }
        offset = frame_end;
    }
    if offset < bytes.len() {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(offset as u64)?;
        file.sync_all()?;
    }
    Ok(DurableReplicationLog { path: path.to_path_buf(), entries, committed })
}

fn encode_entry(entry: &ReplicationEntry) -> Result<Vec<u8>> {
    let payload_len = u32::try_from(entry.payload.len()).map_err(|_| Error::Unsupported("replication payload too large"))?;
    let mut body = Vec::with_capacity(20 + entry.payload.len());
    body.extend_from_slice(&entry.index.0.to_le_bytes());
    body.extend_from_slice(&entry.term.0.to_le_bytes());
    body.extend_from_slice(&payload_len.to_le_bytes());
    body.extend_from_slice(&entry.payload);
    Ok(body)
}

fn decode_entry(body: &[u8]) -> Result<ReplicationEntry> {
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

fn checksum(bytes: &[u8]) -> u32 {
    let mut value = 0x811c9dc5_u32;
    for byte in bytes {
        value ^= u32::from(*byte);
        value = value.wrapping_mul(0x01000193);
    }
    value
}
