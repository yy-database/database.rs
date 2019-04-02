//! 报告信封。
//!
//! 信封只承载判定结果，不承载期望值。字段顺序与缩进固定，便于新旧两路在
//! 影子模式下逐字节比较；后续能力以新增字段的方式扩展，不改变既有字段含义。

use serde::{Deserialize, Serialize};

use crate::{
    Result,
    corpus::{BaselineProblem, BaselineVerdict, CorpusSpec, Digest},
};

/// 当前报告信封版本。
pub const REPORT_VERSION: u32 = 1;

/// 语料判定段落。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorpusReport {
    /// 本次运行的语料参数。
    pub spec: CorpusSpec,
    /// 本次运行的语料摘要。
    pub digest: Digest,
    /// 命中的已知差异条目数。
    pub matched: u64,
    /// 判定问题。
    pub problems: Vec<BaselineProblem>,
    /// 问题为空时为真。
    pub clean: bool,
}

impl CorpusReport {
    /// 由台账判定结果构造语料段落。
    pub fn new(spec: CorpusSpec, digest: Digest, verdict: &BaselineVerdict) -> Self {
        Self { spec, digest, matched: verdict.matched, problems: verdict.problems.clone(), clean: verdict.is_clean() }
    }
}

/// 统一报告信封。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportEnvelope {
    /// 信封版本。
    pub version: u32,
    /// 触发本次报告的命令名。
    pub command: String,
    /// 语料判定段落；没有语料步骤时为空。
    pub corpus: Option<CorpusReport>,
}

impl ReportEnvelope {
    /// 以当前版本构造空信封。
    pub fn new(command: impl Into<String>) -> Self {
        Self { version: REPORT_VERSION, command: command.into(), corpus: None }
    }
}

/// 将信封渲染为稳定的 JSON 文本：两空格缩进，末尾带一个换行。
pub fn render_json(report: &ReportEnvelope) -> Result<String> {
    let mut text = serde_json::to_string_pretty(report)?;
    text.push('\n');
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corpus::{BaselineIdentity, check_baseline};

    fn identity() -> BaselineIdentity {
        BaselineIdentity { spec: CorpusSpec { version: 1, seed: 20261106, count: 2 }, digest: Digest::of(b"corpus") }
    }

    #[test]
    fn envelope_has_fixed_shape_and_trailing_newline() {
        let identity = identity();
        let baseline = crate::corpus::Baseline { identity, entries: Vec::new() };
        let verdict = check_baseline(&baseline, &identity, &[]);
        let mut report = ReportEnvelope::new("corpus");
        report.corpus = Some(CorpusReport::new(identity.spec, identity.digest, &verdict));
        let text = render_json(&report).unwrap();
        assert!(text.ends_with("}\n"));
        assert_eq!(
            text,
            format!(
                "{{\n  \"version\": 1,\n  \"command\": \"corpus\",\n  \"corpus\": {{\n    \"spec\": {{\n      \"version\": 1,\n      \"seed\": 20261106,\n      \"count\": 2\n    }},\n    \"digest\": \"{}\",\n    \"matched\": 0,\n    \"problems\": [],\n    \"clean\": true\n  }}\n}}\n",
                identity.digest.to_hex()
            )
        );
    }

    #[test]
    fn envelope_roundtrips_through_json() {
        let mut report = ReportEnvelope::new("corpus");
        report.corpus = None;
        let text = render_json(&report).unwrap();
        let back: ReportEnvelope = serde_json::from_str(&text).unwrap();
        assert_eq!(back, report);
    }
}
