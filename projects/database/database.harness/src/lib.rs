//! `database-harness`：测试与基准的判定协议。
//!
//! 只负责三件事：语料身份（seed / count / 摘要）、已知差异台账（登记、匹配、
//! 未登记、信号变化、陈旧条目）与报告信封。它不解析线协议、不实现 SQL 或命令
//! 语义，也不生成任何期望值；期望值始终由真实外部引擎、官方语料或冻结基线提供。
//!
//! 依赖方向固定为 `database-harness -> database-types`。本 crate 不反向依赖
//! `database` 门面，也不依赖任何产品 crate。

#![deny(missing_docs)]

pub mod corpus;
pub mod report;

pub use corpus::{
    Baseline, BaselineIdentity, BaselineProblem, BaselineVerdict, CorpusSpec, DifferenceObservation, Digest, KnownDifference, check_baseline,
    check_identity,
};
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
}

/// 判定协议的结果别名。
pub type Result<T = ()> = std::result::Result<T, HarnessError>;
