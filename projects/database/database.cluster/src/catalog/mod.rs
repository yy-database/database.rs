//! 目录生命周期、分片放置与所有权 fencing。

mod catalog;
mod format;

pub use catalog::{Catalog, catalog_path, is_catalog_path};
pub use format::{MAGIC, decode, encode};
