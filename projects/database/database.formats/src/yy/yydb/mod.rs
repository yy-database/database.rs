//! `.yydb` / `.yydx` 主文件页格式 v0：页头、B+ 树页、键编码与校验。
//!
//! `.yydx` 主文件沿用同一页格式，仅以 header 的 `storage_layout` 区分。

pub mod crc32c;
pub mod header;
pub mod internal;
pub mod key;
pub mod leaf;
pub mod page;

pub use crc32c::crc32c;
pub use header::{
    DatabaseHeader, DatabaseHeaderSlot, PAGE_FORMAT_VERSION, PAGE_MAGIC, PAGE_SIZE, SLOT_BYTES, SLOT_KIND_A, SLOT_KIND_B, encode_empty_page0,
    inactive_slot_offset, parse_page0, parse_page0_checked, slot_offset_for_kind,
};
pub use internal::{InternalEntry, InternalPage};
pub use key::{KEY_KIND_USER, TREE_CATALOG, TREE_RECORD, TreeKey};
pub use leaf::{LeafCell, LeafPage};
pub use page::{PAGE_HEADER_LEN, PAGE_PAYLOAD_LEN, PAGE_TYPE_INTERNAL, PAGE_TYPE_LEAF, PageHeader, encode_page};
