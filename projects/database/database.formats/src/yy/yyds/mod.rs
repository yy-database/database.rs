//! YYDS 控制面磁盘编解码：`.yyds` 目录文件与 `YYRL` 副本日志帧。

mod catalog;
mod replication;

pub use catalog::{
    MAGIC as CATALOG_MAGIC, decode as decode_catalog, decode_catalog_shard_id, encode as encode_catalog,
};
pub use replication::{
    APPEND as REPLICATION_APPEND, COMMIT as REPLICATION_COMMIT, FRAME_HEADER as REPLICATION_FRAME_HEADER,
    FRAME_TRAILER as REPLICATION_FRAME_TRAILER, MAGIC as REPLICATION_MAGIC, checksum as replication_checksum,
    decode_entry as decode_replication_entry, encode_empty_log, encode_entry as encode_replication_entry,
    encode_frame as encode_replication_frame,
};

/// 目录文件字节编解码（磁盘后缀见 `database_types::cluster::CATALOG_SUFFIX`）。
pub mod catalog_format {
    pub use super::catalog::*;
}

/// 副本日志帧编解码。
pub mod replication_format {
    pub use super::replication::*;
}
