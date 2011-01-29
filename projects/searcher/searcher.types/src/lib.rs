//! 搜索合同：集合、文档、命中结果与错误。
//!
//! 不暴露分布式存储内部身份，也不包含采集器状态。

#![deny(missing_docs)]

use serde::{Deserialize, Serialize};

/// 集合稳定身份。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollectionId(pub String);

/// 文档稳定身份。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocumentId(pub String);

/// 外部文档版本，用于拒绝乱序更新。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DocumentRevision(pub u64);

/// 已发布的索引代次。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IndexGeneration(pub u64);

/// 排序分数。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Score(pub f64);

/// 单条搜索命中。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    /// 文档身份。
    pub document_id: DocumentId,
    /// 排序分数。
    pub score: Score,
}

/// 一次搜索的结果页。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    /// 命中列表。
    pub hits: Vec<SearchHit>,
    /// 查询所见的索引代次。
    pub generation: IndexGeneration,
}

/// 搜索域错误。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchError {
    /// 集合不存在或未打开。
    CollectionNotFound,
    /// 文档 revision 冲突。
    RevisionConflict,
    /// 索引代次不可用。
    GenerationUnavailable,
    /// 请求非法。
    InvalidRequest(String),
}

/// 搜索操作结果。
pub type Result<T> = std::result::Result<T, SearchError>;
