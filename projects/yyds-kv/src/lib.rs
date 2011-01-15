#![deny(missing_debug_implementations)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
#![doc = include_str!("../readme.md")]

mod key;
mod record;
mod replication_log;
mod store;
mod value;

pub use crate::{
    key::Key,
    record::Record,
    replication_log::DurableReplicationLog,
    store::{compare_and_put, now_unix_ms, FileShard, KvStore, MemoryShard, ShardChange},
    value::{InlineValue, ObjectRef, StoredValue},
};
