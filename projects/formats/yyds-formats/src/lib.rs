//! YYDS 磁盘编解码（目录 / 分片 / 复制日志）。
//!
//! MySQL / PostgreSQL / Redis / Mongo 伪装网关不拥有存储格式；服务级兼容
//! 一律落在这些 codec 之上。

#![deny(missing_debug_implementations)]
#![deny(missing_docs)]

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
    //! `.yyds` 目录字节编解码。
    pub use crate::catalog::*;
}

pub mod kv_format {
    //! `.yykv` 分片快照编解码。
    pub use crate::kv::*;
}

pub mod replication_format {
    //! `YYRL` 复制日志帧编解码。
    pub use crate::replication::*;
}
