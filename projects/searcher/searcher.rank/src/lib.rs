//! 评分与排序合同。

#![deny(missing_docs)]

use searcher_types::Score;

/// 排序器。
pub trait Ranker {
    /// 占位分数，供骨架接线。
    fn score_placeholder(&self) -> Score;
}

/// 布尔命中排序：有命中即 1.0。
#[derive(Debug, Default, Clone, Copy)]
pub struct BooleanRanker;

impl Ranker for BooleanRanker {
    fn score_placeholder(&self) -> Score {
        Score(1.0)
    }
}
