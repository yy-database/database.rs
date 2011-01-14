//! 嵌入式 local_io（pager / WAL / SHM）集成测试。
//!
//! format-v0 golden 语料与 `gen_golden.py` 已下沉到
//! `yydb` 产品仓 `projects/yydb.py/packages/yydb/tests/fixtures/format-v0`。

mod batch_put;
mod btree_split;
mod checkpoint_recovery;
mod file_pager;
mod memory_kv;
mod memory_pager;
mod wal_durability;
mod wal_record;
mod wal_recovery;
