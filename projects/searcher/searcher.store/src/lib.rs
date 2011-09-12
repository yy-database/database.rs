//! 搜索持久化边界。
//!
//! 调用方只看到搜索逻辑接口；分布式分片、租约与键空间映射在此内部完成。
//! 本包不包含采集器。

#![deny(missing_docs)]

use searcher_document::DocumentOp;
use searcher_types::{IndexGeneration, Result};

/// 搜索存储合同。
pub trait SearchStore {
    /// 写入文档变更（尚未发布为可搜索代次）。
    fn write_documents(&mut self, ops: &[DocumentOp]) -> Result<()>;

    /// 原子发布新的索引代次。
    fn publish_generation(&mut self) -> Result<IndexGeneration>;
}

/// 内存实现，便于合同与单元测试。
#[derive(Debug, Default)]
pub struct MemorySearchStore {
    generation: u64,
    pending: usize,
}

impl SearchStore for MemorySearchStore {
    fn write_documents(&mut self, ops: &[DocumentOp]) -> Result<()> {
        self.pending = self.pending.saturating_add(ops.len());
        Ok(())
    }

    fn publish_generation(&mut self) -> Result<IndexGeneration> {
        if self.pending > 0 {
            self.generation = self.generation.saturating_add(1);
            self.pending = 0;
        }
        Ok(IndexGeneration(self.generation))
    }
}
