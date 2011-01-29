//! 查询语义与执行计划。
//!
//! 不直接操作分布式存储内部路径。

#![deny(missing_docs)]

use searcher_rank::Ranker;
use searcher_types::{CollectionId, Result, Score, SearchHit, SearchResult};
use serde::{Deserialize, Serialize};

/// 查询表达式。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryExpr {
    /// 词项。
    Term {
        /// 字段。
        field: String,
        /// 词。
        term: String,
    },
    /// 布尔与。
    And(Vec<QueryExpr>),
    /// 布尔或。
    Or(Vec<QueryExpr>),
}

/// 搜索请求。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchRequest {
    /// 集合。
    pub collection: CollectionId,
    /// 查询。
    pub query: QueryExpr,
    /// 返回条数。
    pub limit: usize,
}

/// 查询执行器。
pub struct SearchExecutor<R> {
    ranker: R,
}

impl<R: Ranker> SearchExecutor<R> {
    /// 使用给定排序器构造执行器。
    pub fn new(ranker: R) -> Self {
        Self { ranker }
    }

    /// 执行搜索。当前返回空结果骨架，供后续接线。
    pub fn search(&self, request: &SearchRequest) -> Result<SearchResult> {
        let _ = (request, self.ranker.score_placeholder());
        Ok(SearchResult { hits: Vec::<SearchHit>::new(), generation: searcher_types::IndexGeneration(0) })
    }
}

/// 占位：确保 `Score` 在本 crate 可见。
pub fn unused_score() -> Score {
    Score(0.0)
}
