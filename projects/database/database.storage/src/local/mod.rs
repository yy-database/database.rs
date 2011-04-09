//! 嵌入式单文件存储面：主文件生命周期、写锁、快照提交、WAL / checkpoint、
//! 持久 CAS Blob I/O 与存储指标。
//!
//! 本模块只懂物理 `YDPG` / `YYWL` / `YYBB` v0 布局，不懂 VOS、查询计划、UDF
//! 或 `Connection`；调用方自行提交不透明元数据行与逻辑记录。

pub mod blob;
pub mod files;
pub mod journal;
pub mod snapshot;

mod database;
mod lock;

pub use blob::BlobStore;
pub use database::{Database, file_is_ydpg, snapshot_from_main_bytes, snapshot_to_main_bytes};
pub use journal::{JournalMode, OpenFlags, shm_path, wal_path};
pub use lock::{LockMode, WriterLock};
pub use snapshot::{PagerKv, RecordDelta, RecordWindow, RowRecord, RowWindow, Snapshot, read_snapshot_windows_with_rows};

// 上层只需一跳拿到的物理布局词表：CAS 块哈希与 WAL blob-manifest 增量属持久化面。
pub use crate::local_io::{FilePager, MemoryPager, PublishStats, WalWriteStats, diagnose_file, pager::DecodedPage};
pub use database_formats::local::{BLOB_REF_OP_DELETE, BLOB_REF_OP_PUT, BlobChunkRef, BlobManifestDelta, blob_chunk_hash};
