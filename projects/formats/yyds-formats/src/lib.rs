//! On-disk codecs for YYDS (catalog / shard / replication log).
//!
//! Dialect disguise gateways (MySQL / PostgreSQL / Redis / Mongo) do not own
//! storage formats — service-level compatibility stores through these codecs.

#![deny(missing_debug_implementations)]
#![warn(missing_docs)]

mod catalog;
mod key;
mod kv;
mod record;
mod replication;
mod value;

pub use catalog::{MAGIC as CATALOG_MAGIC, decode as decode_catalog, encode as encode_catalog};
pub use key::Key;
pub use kv::{LEGACY_MAGIC as KV_LEGACY_MAGIC, MAGIC as KV_MAGIC, Snapshot, decode_snapshot, encode_snapshot};
pub use record::Record;
pub use replication::{
    APPEND as REPLICATION_APPEND, COMMIT as REPLICATION_COMMIT, FRAME_HEADER as REPLICATION_FRAME_HEADER,
    FRAME_TRAILER as REPLICATION_FRAME_TRAILER, MAGIC as REPLICATION_MAGIC, checksum as replication_checksum,
    decode_entry as decode_replication_entry, encode_empty_log, encode_entry as encode_replication_entry,
    encode_frame as encode_replication_frame,
};
pub use value::{InlineValue, ObjectRef, StoredValue};

pub mod catalog_format {
    //! `.yyds` catalog byte codec.
    pub use crate::catalog::*;
}

pub mod kv_format {
    //! `.yykv` shard snapshot codec.
    pub use crate::kv::*;
}

pub mod replication_format {
    //! `YYRL` replica-log frame codec.
    pub use crate::replication::*;
}
