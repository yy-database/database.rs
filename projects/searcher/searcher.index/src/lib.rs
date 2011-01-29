//! 倒排索引、不可变段与代次发布。

#![deny(missing_docs)]

use searcher_document::DocumentOp;
use searcher_store::SearchStore;
use searcher_types::{IndexGeneration, Result};

/// 索引写入与发布入口。
pub struct IndexWriter<S> {
    store: S,
}

impl<S: SearchStore> IndexWriter<S> {
    /// 绑定一个搜索存储。
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// 应用一批文档变更到内存缓冲；尚未成为可搜索代次。
    pub fn apply(&mut self, ops: &[DocumentOp]) -> Result<()> {
        self.store.write_documents(ops)
    }

    /// 将缓冲刷成段并原子发布新代次。
    pub fn publish(&mut self) -> Result<IndexGeneration> {
        self.store.publish_generation()
    }
}
