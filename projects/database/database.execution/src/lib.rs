//! `database-execution`：连接、游标、算子、写入与事务协调。
//!
//! UDF 合同与注册表已提级到 [`database_udf`]；本包只保留执行程序桥接。

#![deny(missing_docs)]

/// 分布式执行面。
pub mod cluster;
/// 嵌入式引擎（连接 / 会话 / 存储协调）。
pub mod engine;
/// 执行程序表示与求值。
pub mod program;
pub mod sql_exec;
/// 执行程序 ↔ UDF 边界桥接。
pub mod udf_bridge;

/// UDF 能力再导出（合同 + 注册表 + 宿主/原生适配 + 执行桥）。
pub mod udf {
    pub use crate::udf_bridge::*;
    pub use database_udf::*;
}
