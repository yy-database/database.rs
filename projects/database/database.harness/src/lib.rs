//! `database-harness`：测试与基准的判定协议。
//!
//! 负责五件事：语料身份（seed / count / 摘要）、已知差异台账（登记、匹配、
//! 未登记、信号变化、陈旧条目）、观测比对与归类、基准统计与报告行、报告
//! 信封。它不解析线协议、不实现 SQL 或命令语义，也不生成任何期望值；期望值
//! 始终由真实外部引擎、官方语料或冻结基线提供。
//!
//! feature `charts` 打开后经 `plotter-chart` 把基准报告行渲染为分组柱图
//! SVG；默认不开启，判定核心保持零绘图依赖。
//!
//! 依赖方向固定为 `database-harness -> database-types`。本 crate 不反向依赖
//! `database` 门面，也不依赖任何产品 crate。

#![deny(missing_docs)]

pub mod bench;
pub mod compare;
pub mod corpus;
pub mod observe;
pub mod report;

#[cfg(feature = "charts")]
pub mod chart;

pub use bench::{BenchPlan, BenchRow, DeltaRegistry, IntentionalDelta, Stats, per_op_ms, summarize};
pub use compare::{CompareMode, Comparison, Diff, DifferenceClass, classify, compare};
pub use corpus::{
    Baseline, BaselineIdentity, BaselineProblem, BaselineVerdict, CorpusSpec, DifferenceObservation, Digest, KnownDifference, check_baseline,
    check_identity,
};
pub use observe::{Cell, ErrorShape, Observed, ObservedBody, TruthProvenance};
pub use report::{CorpusReport, REPORT_VERSION, ReportEnvelope, render_json};

/// 判定协议的错误。
#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    /// 共享类型层返回的错误。
    #[error(transparent)]
    Types(#[from] database_types::Error),
    /// 摘要不是 64 位十六进制。
    #[error("摘要必须是 64 位十六进制，实际是 {0:?}")]
    InvalidDigest(String),
    /// 报告序列化失败。
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// 图表渲染失败（feature `charts`）。
    #[cfg(feature = "charts")]
    #[error("chart: {0}")]
    Chart(String),
}

/// 判定协议的结果别名。
pub type Result<T = ()> = std::result::Result<T, HarnessError>;
