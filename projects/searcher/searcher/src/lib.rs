//! `searcher` 是搜索能力域的唯一公开门面。
//!
//! 当前仅再导出类型合同；索引、查询与存储实现按能力包继续补齐。

#![deny(missing_docs)]

/// 搜索合同、命中结果与错误。
pub mod types {
    pub use searcher_types::*;
}
