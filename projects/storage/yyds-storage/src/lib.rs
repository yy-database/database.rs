#![deny(missing_debug_implementations)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
#![doc = include_str!("../readme.md")]

mod replication_log;
mod store;

pub use replication_log::DurableReplicationLog;
pub use store::{FileShard, KvStore, MemoryShard, ShardChange, compare_and_put, now_unix_ms};
pub use yyds_formats::{InlineValue, Key, ObjectRef, Record, StoredValue};
