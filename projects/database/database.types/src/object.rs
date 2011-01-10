//! Object identity, manifests and vector payloads.

use super::*;
use crate::hex;

/// Database-owned VOS schema document and version (database truth).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaVersion {
    /// Monotonic schema generation stored in the database.
    pub version: u32,
    /// Canonical VOS schema document text.
    pub document: String,
}

/// Content digest algorithm recorded on [`ObjectRef`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HashAlgo {
    /// BLAKE3-256 (default).
    Blake3,
}

impl HashAlgo {
    /// Stable name for diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blake3 => "blake3",
        }
    }

    /// Parses a name produced by [`Self::as_str`].
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "blake3" => Some(Self::Blake3),
            _ => None,
        }
    }
}

/// What a CAS object represents (same path layout for all kinds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ObjectKind {
    /// Opaque blob / file chunk.
    Blob,
    /// Encoded vector payload.
    VectorPayload,
    /// Immutable chunk manifest of a logical file.
    FileManifest,
    /// Rebuildable ANN graph segment.
    AnnSegment,
}

impl ObjectKind {
    /// Stable name used by tagged JSON and diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blob => "blob",
            Self::VectorPayload => "vector-payload",
            Self::FileManifest => "file-manifest",
            Self::AnnSegment => "ann-segment",
        }
    }

    /// Parses a name produced by [`Self::as_str`].
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "blob" => Some(Self::Blob),
            "vector-payload" => Some(Self::VectorPayload),
            "file-manifest" => Some(Self::FileManifest),
            "ann-segment" => Some(Self::AnnSegment),
            _ => None,
        }
    }
}

/// Runtime residency hint (cache policy; not a second on-disk format).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
    /// Prefer keeping the object in the process hot set.
    Hot,
    /// On-disk CAS only until faulted in.
    Cold,
}

/// Content-addressed object handle (rows store this, not raw payloads).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectRef {
    /// Digest algorithm.
    pub algo: HashAlgo,
    /// Raw digest bytes (32 for BLAKE3).
    pub hash: [u8; 32],
    /// Payload size in bytes.
    pub size: u64,
    /// Semantic kind.
    pub kind: ObjectKind,
}

impl ObjectRef {
    /// Lowercase hex encoding of [`Self::hash`].
    pub fn hash_hex(&self) -> String {
        hex::encode(self.hash)
    }

    /// `hash-2` directory segment (first two hex characters).
    pub fn hash_prefix2(&self) -> String {
        self.hash_hex().chars().take(2).collect()
    }
}

/// Ordered chunk list for a logical file (or single-chunk small file).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkManifest {
    /// Nominal chunk size used when writing (last chunk may be shorter).
    pub chunk_size: u32,
    /// Total logical byte length.
    pub total_size: u64,
    /// Chunks in order; each points at `objects/hash-2/*.bytes`.
    pub chunks: Vec<ObjectRef>,
}

impl ChunkManifest {
    /// Strict JSON payload published under [`ObjectKind::FileManifest`].
    ///
    /// The payload names only chunk identity and length: chunk layout is a
    /// storage detail and never enters the file's content identity.
    pub fn to_payload_bytes(&self) -> Result<Vec<u8>> {
        let chunks = self.chunks.iter().map(|chunk| serde_json::json!({ "hash": chunk.hash_hex(), "size": chunk.size })).collect::<Vec<_>>();
        serde_json::to_vec(&serde_json::json!({
            "chunk_size": self.chunk_size,
            "total_size": self.total_size,
            "chunks": chunks,
        }))
        .map_err(|_| Error::Corrupt("file manifest payload encode failed"))
    }

    /// Decodes and validates a manifest payload.
    ///
    /// A manifest is accepted whole or rejected whole: unknown members, a
    /// non-positive `chunk_size`, a short non-final chunk, a chunk whose length
    /// exceeds `chunk_size`, or chunk lengths that do not add up to
    /// `total_size` are all corruption instead of a partially usable manifest.
    pub fn from_payload_bytes(bytes: &[u8]) -> Result<Self> {
        let json: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| Error::ObjectCorrupt { message: "file manifest payload is not valid JSON".into() })?;
        let object = json.as_object().ok_or_else(|| Error::ObjectCorrupt { message: "file manifest payload must be a JSON object".into() })?;
        let members = ["chunk_size", "total_size", "chunks"];
        if object.len() != members.len() || !members.iter().all(|member| object.contains_key(*member)) {
            return Err(Error::ObjectCorrupt { message: "file manifest has unknown or missing members".into() });
        }
        let chunk_size = object
            .get("chunk_size")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| Error::ObjectCorrupt { message: "file manifest chunk_size must be a positive u32".into() })?;
        let total_size = object
            .get("total_size")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| Error::ObjectCorrupt { message: "file manifest total_size must be a u64".into() })?;
        let chunk_entries = object
            .get("chunks")
            .and_then(serde_json::Value::as_array)
            .filter(|entries| !entries.is_empty())
            .ok_or_else(|| Error::ObjectCorrupt { message: "file manifest requires at least one chunk".into() })?;
        let mut chunks = Vec::with_capacity(chunk_entries.len());
        let mut sum = 0_u64;
        for (index, entry) in chunk_entries.iter().enumerate() {
            let chunk = entry
                .as_object()
                .filter(|chunk| chunk.len() == 2 && chunk.contains_key("hash") && chunk.contains_key("size"))
                .ok_or_else(|| Error::ObjectCorrupt { message: format!("file manifest chunk {index} is malformed") })?;
            let hash =
                chunk.get("hash").and_then(serde_json::Value::as_str).and_then(hex::decode_lower_32).ok_or_else(|| Error::ObjectCorrupt {
                    message: format!("file manifest chunk {index} hash must be 64 lowercase hex digits"),
                })?;
            let size = chunk
                .get("size")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| Error::ObjectCorrupt { message: format!("file manifest chunk {index} size must be a u64") })?;
            let is_last = index + 1 == chunk_entries.len();
            if size > u64::from(chunk_size) || (!is_last && size != u64::from(chunk_size)) {
                return Err(Error::ObjectCorrupt {
                    message: format!("file manifest chunk {index} length {size} does not match chunk_size {chunk_size}"),
                });
            }
            sum = sum.checked_add(size).ok_or_else(|| Error::ObjectCorrupt { message: "file manifest chunk lengths overflow".into() })?;
            chunks.push(ObjectRef { algo: HashAlgo::Blake3, hash, size, kind: ObjectKind::Blob });
        }
        if sum != total_size {
            return Err(Error::ObjectCorrupt {
                message: format!("file manifest chunk lengths add up to {sum}, expected total_size {total_size}"),
            });
        }
        Ok(Self { chunk_size, total_size, chunks })
    }
}

/// Reference to the immutable manifest object that lists a file's chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileManifestRef {
    /// BLAKE3 digest of the manifest payload.
    pub hash: [u8; 32],
    /// Manifest payload size in bytes.
    pub size: u64,
}

impl FileManifestRef {
    /// Lowercase hex encoding of the manifest digest.
    pub fn hash_hex(&self) -> String {
        hex::encode(self.hash)
    }
}

/// Row value of a `file` column: content identity, metadata, and manifest.
///
/// Identity and metadata are independent: renaming a file or relabelling its
/// media type keeps [`Self::digest`], [`Self::octets`] and the manifest, while
/// two descriptors with the same digest share one manifest and one chunk set.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileDescriptor {
    /// Lowercase hexadecimal SHA-256 over the complete file octets.
    pub digest: String,
    /// Total octet count of the complete file.
    pub octets: u64,
    /// Original file name; never part of the identity.
    pub name: Option<String>,
    /// Declared media type; never part of the identity.
    pub media_type: Option<String>,
    /// Immutable chunk manifest published under [`ObjectKind::FileManifest`].
    pub manifest: FileManifestRef,
}

impl FileDescriptor {
    /// Object reference of the manifest payload.
    pub fn manifest_object(&self) -> ObjectRef {
        ObjectRef { algo: HashAlgo::Blake3, hash: self.manifest.hash, size: self.manifest.size, kind: ObjectKind::FileManifest }
    }

    /// Whether an offset starts at or beyond end of file.
    pub fn is_eof(&self, offset: u64) -> bool {
        offset >= self.octets
    }

    /// Resolves a read request against the file length.
    ///
    /// A request starting before end of file is clamped to the remaining
    /// octets, and a request starting at or beyond end of file yields an empty
    /// window at that offset instead of reading past the end.
    pub fn resolve_window(&self, offset: u64, length: u64) -> (u64, u64) {
        if self.is_eof(offset) {
            return (offset, 0);
        }
        (offset, length.min(self.octets - offset))
    }
}

/// Fixed-dimension float vector (logical value; prefer CAS for persistence).
#[derive(Debug, Clone, PartialEq)]
pub struct Vector {
    /// Dimension (must match `data.len()`).
    pub dim: u32,
    /// Contiguous `f32` components.
    pub data: Arc<[f32]>,
}

impl Vector {
    /// Create a vector; `data.len()` must fit in `u32` and becomes `dim`.
    pub fn new(data: impl Into<Arc<[f32]>>) -> Result<Self> {
        let data = data.into();
        let dim = u32::try_from(data.len()).map_err(|_| Error::ObjectCorrupt { message: "vector dimension exceeds u32".into() })?;
        Ok(Self { dim, data })
    }

    /// Encode as little-endian `f32` bytes for CAS storage.
    pub fn to_le_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.data.len() * 4);
        for value in self.data.iter() {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    /// Decode little-endian `f32` bytes.
    pub fn from_le_bytes(bytes: &[u8]) -> Result<Self> {
        if !bytes.len().is_multiple_of(4) {
            return Err(Error::ObjectCorrupt { message: "vector payload length not multiple of 4".into() });
        }
        let mut data = Vec::with_capacity(bytes.len() / 4);
        for chunk in bytes.as_chunks::<4>().0 {
            data.push(f32::from_le_bytes(*chunk));
        }
        Self::new(data)
    }
}
