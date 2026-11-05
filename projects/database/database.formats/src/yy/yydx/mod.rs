//! `.yydx` 不可变 Blob chunk 编解码。

pub mod blob;

pub use blob::{
    BLOB_CHUNK_DOMAIN, BLOB_CHUNK_KIND_DATA, BLOB_HEADER_BYTES, BLOB_MAGIC, BlobChunkHeader, blob_chunk_hash, blob_payload_hash,
    encode_blob_chunk, parse_blob_chunk, parse_blob_header, read_blob_payload,
};
