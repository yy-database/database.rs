//! 报告信封。
//!
//! 信封只承载判定结果，不承载期望值。字段顺序与缩进固定，便于新旧两路在
//! 影子模式下逐字节比较；后续能力以新增字段的方式扩展，不改变既有字段含义。

use serde::{Deserialize, Serialize};

use crate::{
    Result,
    corpus::{BaselineProblem, BaselineVerdict, CorpusSpec, Digest},
    gate::GateVerdict,
};

/// 当前报告信封版本。
pub const REPORT_VERSION: u32 = 1;

/// 语料判定段落。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// 一次运行的产物与身份证据。
///
/// 报告只承载已计算结论，但必须能回答“谁跑的、跑的是哪个产物、对照是谁”。
/// 这些字段为空时不允许渲染成验收报告。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunIdentity {
    /// 本次运行标识，由调用方生成且在一次运行内唯一。
    pub run_id: String,
    /// 被测产品名（如 `sqlite`、`redis`）。
    pub product: String,
    /// 被测产品版本。
    pub product_version: String,
    /// 被测源码 revision 或产物摘要。
    pub product_revision: String,
    /// harness 合同版本，例如 `database.harness/v0`。
    pub harness_contract: String,
    /// 独立对照来源（引擎版本、官方语料 revision、采集证据引用）；无对照时为空。
    #[serde(default)]
    pub oracle: Option<String>,
    /// 判定为通过时引用的证据摘要；没有证据时为空。
    #[serde(default)]
    pub evidence: Option<Digest>,
}

/// 统一报告信封。
///
/// 信封承载已计算的判定结论，不在渲染期重新比较业务值。未知字段一律拒绝，
/// 避免调用方传入新证据时被静默丢弃。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportEnvelope {
    /// 信封版本。
    pub version: u32,
    /// 触发本次报告的命令名。
    pub command: String,
    /// 运行产物与身份证据；纯内部草稿可为空，验收报告必须提供。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<RunIdentity>,
    /// 语料判定段落；没有语料步骤时为空。
    #[serde(default)]
    pub corpus: Option<CorpusReport>,
    /// 门禁判定；没有门禁步骤时为空。
    #[serde(default)]
    pub gate: Option<GateVerdict>,
}

impl ReportEnvelope {
    /// 以当前版本构造空信封。
    pub fn new(command: impl Into<String>) -> Self {
        Self { version: REPORT_VERSION, command: command.into(), run: None, corpus: None, gate: None }
    }

    /// 校验信封版本；未知版本不得被渲染成当前合同。
    pub fn validate(&self) -> Result<()> {
        if self.version != REPORT_VERSION {
            return Err(crate::HarnessError::UnsupportedReportVersion { version: self.version });
        }
        if let Some(run) = &self.run {
            for (field, value) in [
                ("run_id", run.run_id.as_str()),
                ("product", run.product.as_str()),
                ("product_version", run.product_version.as_str()),
                ("product_revision", run.product_revision.as_str()),
                ("harness_contract", run.harness_contract.as_str()),
            ] {
                if value.trim().is_empty() {
                    return Err(crate::HarnessError::EmptyReportField { field });
                }
            }
        }
        Ok(())
    }
}

/// 将信封渲染为稳定的 JSON 文本：两空格缩进，末尾带一个换行。
///
/// 渲染前先校验版本；未知版本直接报错，不输出看似合法的报告。
pub fn render_json(report: &ReportEnvelope) -> Result<String> {
    report.validate()?;
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
                "{{\n  \"version\": 1,\n  \"command\": \"corpus\",\n  \"corpus\": {{\n    \"spec\": {{\n      \"version\": 1,\n      \"seed\": 20261106,\n      \"count\": 2\n    }},\n    \"digest\": \"{}\",\n    \"matched\": 0,\n    \"problems\": [],\n    \"clean\": true\n  }},\n  \"gate\": null\n}}\n",
                identity.digest.to_hex()
            )
        );
    }

    #[test]
    fn unknown_version_is_refused() {
        let mut report = ReportEnvelope::new("corpus");
        report.version = 999;
        assert!(matches!(render_json(&report), Err(crate::HarnessError::UnsupportedReportVersion { version: 999 })));
    }

    #[test]
    fn unknown_fields_are_refused_not_dropped() {
        // 旧 renderer 会静默丢掉 verdict/suites；严格信封必须拒绝未知字段。
        let result = serde_json::from_str::<ReportEnvelope>(
            "{\"version\":1,\"command\":\"x\",\"corpus\":null,\"gate\":null,\"verdict\":{\"ok\":false}}",
        );
        assert!(result.is_err());
    }

    #[test]
    fn missing_evidence_sections_default_to_none() {
        let report: ReportEnvelope = serde_json::from_str("{\"version\":1,\"command\":\"x\"}").expect("minimal envelope");
        assert_eq!(report, ReportEnvelope::new("x"));
    }

    #[test]
    fn gate_evidence_roundtrips() {
        let mut report = ReportEnvelope::new("gate");
        report.gate = Some(crate::gate::GateVerdict { ok: false, problems: vec![] });
        let text = render_json(&report).unwrap();
        assert!(text.contains("\"gate\": {"), "{text}");
        let back: ReportEnvelope = serde_json::from_str(&text).unwrap();
        assert_eq!(back, report);
    }

    #[test]
    fn envelope_roundtrips_through_json() {
        let mut report = ReportEnvelope::new("corpus");
        report.corpus = None;
        let text = render_json(&report).unwrap();
        let back: ReportEnvelope = serde_json::from_str(&text).unwrap();
        assert_eq!(back, report);
    }

    #[test]
    fn run_identity_is_optional_but_validated_when_present() {
        let mut report = ReportEnvelope::new("gate");
        report.run = Some(RunIdentity {
            run_id: "run-1".into(),
            product: "sqlite".into(),
            product_version: "0.0.1".into(),
            product_revision: "abc123".into(),
            harness_contract: "database.harness/v0".into(),
            oracle: Some("sqlite 3.45".into()),
            evidence: Some(Digest::of(b"evidence")),
        });
        let text = render_json(&report).unwrap();
        assert!(text.contains("\"run\": {"), "{text}");

        report.run.as_mut().unwrap().product_revision.clear();
        assert!(matches!(render_json(&report), Err(crate::HarnessError::EmptyReportField { field: "product_revision" })));
    }
}
