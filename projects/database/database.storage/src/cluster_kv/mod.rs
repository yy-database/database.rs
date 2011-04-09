//! 分布式分片 KV 存储与复制日志（磁盘后缀见 `SHARD_SUFFIX`）。

mod replication_log;
mod store;

pub use database_formats::cluster::{InlineValue, Key, ObjectRef, Record, StoredValue};
pub use replication_log::DurableReplicationLog;
pub use store::{FileShard, KvStore, MemoryShard, ShardChange, compare_and_put, now_unix_ms};
