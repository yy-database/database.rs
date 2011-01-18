//! `database-client`：连接、认证、请求/响应传输与结果读取。
#![deny(missing_docs)]

/// MySQL 方言客户端。
pub mod mysql;
/// PostgreSQL 方言客户端。
pub mod pgsql;
/// Redis 方言客户端。
pub mod redis;

/// 共享 YY 远端客户端（嵌入式 / 分布式只差端点 magic / URI）。
pub mod yy;
