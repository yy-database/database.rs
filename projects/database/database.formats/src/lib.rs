//! `database-formats`：磁盘字节布局（页、记录、WAL、Blob、分片快照、复制日志、SQLite format-3）。
#![deny(missing_docs)]

/// 分布式目录 / 分片 / 复制日志编解码。
pub mod cluster;
/// 嵌入式单文件页 / BTree / WAL / Blob 编解码。
pub mod local;
/// SQLite format-3 只读解析。
pub mod sqlite;
