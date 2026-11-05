//! `database-formats`：磁盘字节布局，YY 家族按文件后缀分模块，SQLite format-3 单列。
#![deny(missing_docs)]

/// YY 家族格式编解码（`.yydb` / `.yydx` / WAL / `-shm` / `.yyds` / `.yykv`）。
pub mod yy;
/// SQLite format-3 只读解析。
pub mod sqlite;
