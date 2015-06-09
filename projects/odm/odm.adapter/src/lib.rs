//! `odm-adapter`：外存与本地数据源适配。
//!
//! 关系库、缓存与文档库适配都归本包；文档库即便不叫 ORM，概念上仍是对象映射。

#![deny(missing_docs)]

mod error;
pub use error::{Error, Result};

pub mod adapter_mysql;
pub mod adapter_postgres;
pub mod adapter_redis;
pub mod adapter_sqlite;
pub mod connector_cluster;
pub mod connector_local;
pub mod host;
