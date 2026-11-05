//! YY 家族磁盘格式编解码，一个编解码只归属一个模块。
//!
//! | 模块 | 磁盘文件 | 职责 |
//! |------|----------|------|
//! | [`yydb`] | `.yydb` | 页文件页头、B+ 树页、键编码与校验 |
//! | [`yydx`] | `.yydx` | 不可变 Blob chunk 头与载荷 |
//! | [`yywl`] | WAL 边车 | page-image WAL 帧与 `BlobRefs` manifest delta |
//! | [`yysm`] | `-shm` 边车 | 可重建的共享内存协调块 |
//! | [`yyds`] | `.yyds` | 控制面目录文件与副本日志帧 |
//! | [`yykv`] | `.yykv` | 分片 KV 快照与记录模型 |
//!
//! `.yydb` 与 `.yydx` 的主文件共用 `yydb` 的页格式，只有 `storage_layout`
//! 区分；`.yydx` 独有的部分只有 Blob chunk。

pub mod yydb;
pub mod yyds;
pub mod yydx;
pub mod yykv;
pub mod yysm;
pub mod yywl;
