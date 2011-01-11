//! SQLite format-3 二进制原语（纯 Rust，无文件 I/O、无 SQL 执行）。
//!
//! 与本 crate 其它格式模块同样只拥有磁盘事实：头、记录编码、空白库物化；
//! 不含存储层行为。
#![warn(missing_docs)]

mod format;
mod record;

pub use crate::sqlite::{
    format::{
        DEFAULT_PAGE_SIZE, ENGINE_LIBRARY_VERSION, ENGINE_LIBRARY_VERSION_NUMBER, MAGIC, blank_database, blank_with_page_size,
        bump_change_counter, decode_library_version, format_library_version, read_change_counter, read_freelist_count, read_freelist_trunk,
        read_page_count, read_schema_cookie, set_freelist_count, set_freelist_trunk, set_page_count, set_schema_cookie, stamp_library_version,
        validate_database, validate_header,
    },
    record::{RecordLimits, RecordValue, TextEncoding, WriteValue, decode_record, decode_varint, encode_record, encode_varint_into},
};

/// SQLite format reader library version string.
pub fn sqlite_library_version() -> String {
    ENGINE_LIBRARY_VERSION.to_string()
}
