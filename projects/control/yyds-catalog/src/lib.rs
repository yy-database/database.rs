#![deny(missing_debug_implementations)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
#![doc = include_str!("../readme.md")]

mod catalog;
mod format;

pub use crate::{
    catalog::{Catalog, catalog_path, is_catalog_path},
    format::{MAGIC, decode, encode},
};
