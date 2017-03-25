//! MongoDB 产品门面：文档存储、游标、oplog 与命令语义。
//!
//! 本 crate 只提供库原语，不导出 bin、不实现 server 主循环、不做 CLI；宿主由
//! TypeScript / Python 组合这些原语自行搭建。
//!
//! - [`bson`]：BSON 值 / 文档编解码（来自 `database-protocol-mongo`）
//! - [`json`]：Extended JSON 桥（来自 `database-protocol-mongo`）
//! - [`wire`]：帧头与 `OP_MSG` / `OP_QUERY` 转换（来自 `database-protocol-mongo`）
//! - [`cursor`]：调用方自有的游标表
//! - [`oplog`]：`local.oplog.rs` 条目形态
//! - `store`：`.yyds` catalog + `.yykv` shard 后端（`storage` feature）
//! - `storage`：命令层唯一的存储入口 trait（`storage` feature）
//! - `import`：`mongodump` 形态 `.bson` 流导入（`storage` feature）

#![deny(missing_docs)]

pub use database_protocol_mongo::{bson, json, wire};

pub mod cursor;
pub mod id;
pub mod oplog;

#[cfg(feature = "storage")]
pub mod commands;
#[cfg(feature = "storage")]
pub mod import;
#[cfg(feature = "storage")]
pub mod storage;
#[cfg(feature = "storage")]
pub mod store;

#[cfg(feature = "storage")]
pub use storage::MongoStorage;
#[cfg(feature = "storage")]
pub use store::Store;
