//! 页缓存、WAL 与记录树存储原语（嵌入式单文件）。

pub mod btree;
pub mod doctor;
pub mod file;
pub mod memory;
pub mod pager;
pub mod shm;
pub mod wal_append;

pub use doctor::diagnose_file;
pub use file::{FilePager, PublishStats, main_bytes_from_memory_pager, memory_pager_from_main_bytes};
pub use memory::{MainFileSource, MemoryPager};
pub use pager::DecodedPage;
pub use shm::{
    SHM_BYTES, SHM_MAGIC, ShmBlock, encode_empty_shm, encode_shm, parse_shm, shm_sidecar_path, sync_shm_from_commit, sync_shm_from_wal,
};
pub use wal_append::{
    FileWalSink, RecoveryReport, WalDurability, WalSink, WalWriteStats, WalWriter, encode_frame, encode_wal_header, read_wal_file,
    replay_wal_pages, replay_wal_pages_after, strip_incomplete_txn_after_last_page_image, strip_trailing_txn_commit, truncate_wal_after_frame,
    truncate_wal_records, truncate_wal_to_recoverable_prefix, truncate_wal_torn_tail, wal_recoverable_byte_len, wal_sidecar_path,
};
