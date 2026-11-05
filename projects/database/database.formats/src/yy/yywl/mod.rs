//! `YYWL` page-image WAL 帧编解码，含 `.yydx` 的 `BlobRefs` manifest delta。

pub mod blob_refs;
pub mod wal;

pub use blob_refs::{
    BLOB_REF_OP_DELETE, BLOB_REF_OP_PUT, BlobChunkRef, BlobManifestDelta, FRAME_BLOB_REFS, canonical_blob_refs_bytes, commit_digest,
    encode_blob_refs_body,
};
pub use wal::{
    MAX_WAL_RECORD_BYTES, WAL_HEADER_BYTES, WAL_MAGIC, WAL_RECORD_HEADER_BYTES, WAL_RECORD_TRAILER_BYTES, WalFile, WalFrame, WalHeader,
    WalRecordError, WalScan, WalTail, committed_tail_lsn, committed_transactions, parse_frame, parse_header, parse_wal, parse_wal_recover,
    scan_wal, wal_frame_offsets,
};
