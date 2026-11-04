#![deny(missing_debug_implementations)]
#![doc = include_str!("../readme.md")]

use std::{
    fmt,
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

const MAX_IDENTITY_BYTES: u64 = 4096;

/// Persisted cluster and node identities, independent of addresses and shard ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeIdentity {
    cluster_id: String,
    node_id: String,
}

impl NodeIdentity {
    /// Validates operator-supplied ASCII identifiers of at most 128 bytes.
    pub fn new(cluster_id: impl Into<String>, node_id: impl Into<String>) -> Result<Self, LeaseError> {
        let identity = Self { cluster_id: cluster_id.into(), node_id: node_id.into() };
        identity.validate()?;
        Ok(identity)
    }

    /// Cluster identity, not a routing epoch or leader term.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Node identity, not a shard identity or process id.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    fn validate(&self) -> Result<(), LeaseError> {
        for identifier in [&self.cluster_id, &self.node_id] {
            if identifier.is_empty()
                || identifier.len() > 128
                || !identifier.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
            {
                return Err(LeaseError::InvalidIdentity);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IdentityFile {
    version: u32,
    identity: NodeIdentity,
}

/// Failure to establish or inspect local node ownership.
#[derive(Debug)]
pub enum LeaseError {
    /// Filesystem or operating-system lock failure.
    Io(io::Error),
    /// Another handle or process owns the node directory.
    Busy,
    /// Identifiers must contain only ASCII letters, digits, underscores and hyphens.
    InvalidIdentity,
    /// The existing identity must never be implicitly reassigned.
    IdentityMismatch,
    /// Persisted identity is invalid, unsupported, oversized or incomplete.
    CorruptIdentity,
}

impl fmt::Display for LeaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "node ownership I/O error: {error}"),
            Self::Busy => formatter.write_str("node data directory is already owned"),
            Self::InvalidIdentity => formatter.write_str("invalid cluster or node identity"),
            Self::IdentityMismatch => formatter.write_str("node data directory identity mismatch"),
            Self::CorruptIdentity => formatter.write_str("corrupt or unsupported node identity"),
        }
    }
}

impl std::error::Error for LeaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for LeaseError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Point-in-time local directory ownership, not distributed health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalOwnership {
    /// No process held the lock at the instant it was probed.
    Available,
    /// Another handle held the lock at the instant it was probed.
    Held,
}

/// Persisted identity and a point-in-time local ownership probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStatus {
    /// Canonical directory path.
    pub directory: PathBuf,
    /// Identity read from the versioned identity file.
    pub identity: NodeIdentity,
    /// Local ownership only, not node or cluster readiness.
    pub ownership: LocalOwnership,
}

/// Exclusive ownership of a node directory until this object is dropped.
#[derive(Debug)]
pub struct NodeLease {
    directory: PathBuf,
    identity: NodeIdentity,
    _lock: File,
}

impl NodeLease {
    /// Acquires ownership before verifying or initializing the immutable identity.
    pub fn acquire(directory: impl AsRef<Path>, identity: NodeIdentity) -> Result<Self, LeaseError> {
        identity.validate()?;
        fs::create_dir_all(directory.as_ref())?;
        let directory = fs::canonicalize(directory)?;
        let lock = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(directory.join("node.lock"))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(LeaseError::Busy),
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
        let path = directory.join("node.json");
        match read_identity(&path) {
            Ok(stored) if stored == identity => {}
            Ok(_) => return Err(LeaseError::IdentityMismatch),
            Err(LeaseError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                let bytes = serde_json::to_vec(&IdentityFile { version: 1, identity: identity.clone() })
                    .map_err(|_| LeaseError::CorruptIdentity)?;
                let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
                output.write_all(&bytes)?;
                output.sync_all()?;
                #[cfg(unix)]
                File::open(&directory)?.sync_all()?;
            }
            Err(error) => return Err(error),
        }
        Ok(Self { directory, identity, _lock: lock })
    }

    /// Canonical directory owned by this lease.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Immutable cluster and node identity for this lease.
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// Inspects existing state without creating or modifying files.
    pub fn inspect(directory: impl AsRef<Path>) -> Result<NodeStatus, LeaseError> {
        let directory = fs::canonicalize(directory)?;
        let lock = OpenOptions::new().read(true).write(true).open(directory.join("node.lock"))?;
        let ownership = match lock.try_lock() {
            Ok(()) => LocalOwnership::Available,
            Err(TryLockError::WouldBlock) => LocalOwnership::Held,
            Err(TryLockError::Error(error)) => return Err(error.into()),
        };
        let identity = read_identity(&directory.join("node.json"))?;
        Ok(NodeStatus { directory, identity, ownership })
    }
}

fn read_identity(path: &Path) -> Result<NodeIdentity, LeaseError> {
    let mut input = File::open(path)?;
    if !input.metadata()?.is_file() {
        return Err(LeaseError::CorruptIdentity);
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut input).take(MAX_IDENTITY_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_IDENTITY_BYTES {
        return Err(LeaseError::CorruptIdentity);
    }
    let stored: IdentityFile = serde_json::from_slice(&bytes).map_err(|_| LeaseError::CorruptIdentity)?;
    if stored.version != 1 || stored.identity.validate().is_err() {
        return Err(LeaseError::CorruptIdentity);
    }
    Ok(stored.identity)
}
