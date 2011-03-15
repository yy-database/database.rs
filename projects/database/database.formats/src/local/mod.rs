//! 嵌入式单文件格式 v0：页头、BTree、WAL、Blob 编解码与校验。

pub mod blob;
pub mod blob_refs;
pub mod crc32c;
pub mod header;
pub mod internal;
pub mod key;
pub mod leaf;
pub mod page;
pub mod wal;

pub use blob::{
    BLOB_CHUNK_DOMAIN, BLOB_CHUNK_KIND_DATA, BLOB_HEADER_BYTES, BLOB_MAGIC, BlobChunkHeader, blob_chunk_hash, blob_payload_hash,
    encode_blob_chunk, parse_blob_chunk, parse_blob_header, read_blob_payload,
};
pub use blob_refs::{
    BLOB_REF_OP_DELETE, BLOB_REF_OP_PUT, BlobChunkRef, BlobManifestDelta, FRAME_BLOB_REFS, canonical_blob_refs_bytes, commit_digest,
    encode_blob_refs_body,
};
pub use crc32c::crc32c;
pub use header::{
    DatabaseHeader, DatabaseHeaderSlot, PAGE_FORMAT_VERSION, PAGE_MAGIC, PAGE_SIZE, SLOT_BYTES, SLOT_KIND_A, SLOT_KIND_B, encode_empty_page0,
    inactive_slot_offset, parse_page0, parse_page0_checked, slot_offset_for_kind,
};
pub use internal::{InternalEntry, InternalPage};
pub use key::{KEY_KIND_USER, TREE_CATALOG, TREE_RECORD, TreeKey};
pub use leaf::{LeafCell, LeafPage};
pub use page::{PAGE_HEADER_LEN, PAGE_PAYLOAD_LEN, PAGE_TYPE_INTERNAL, PAGE_TYPE_LEAF, PageHeader, encode_page};
pub use wal::{
    MAX_WAL_RECORD_BYTES, WAL_HEADER_BYTES, WAL_MAGIC, WAL_RECORD_HEADER_BYTES, WAL_RECORD_TRAILER_BYTES, WalFile, WalFrame, WalHeader,
    WalRecordError, WalScan, WalTail, committed_tail_lsn, committed_transactions, parse_frame, parse_header, parse_wal, parse_wal_recover,
    scan_wal, wal_frame_offsets,
};
