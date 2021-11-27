//! 门禁判定：消费执行状态、oracle 资格、证据完整性、基线变化与 floors，
//! 逐项给出问题，不用单个 `ok` 掩盖未验证项。
//!
//! 本模块只做判定：接收调用方已经解析好的计数与证据（TAP、pytest、unittest、
//! `node:test` 的输出解析全部留在产品仓），给出失败关闭的门禁结论。它不运行
//! 测试命令、不解析任何测试输出、不持有 floor 数值；floor 数值与各仓指标选择
//! 是产品仓配置，判定算法在此上游统一维护。
//!
//! 失败关闭规则：执行状态至少区分 passed、failed、skipped、unavailable、
//! unverified、timed_out、cancelled、crashed、not_run，各状态分别统计，禁止靠
//! 重试膨胀覆盖量；状态计数之和与声明的 `cases` 不一致（用例下落不明或被重复
//! 计入）记 `tally-inconsistent`；必需 oracle 不可用、证据缺来源、基线陈旧/未登记/变化、必需
//! suite 缺失、部分 suite 未执行（策略未放行时）一律记问题，任一问题即 `ok = false`。

use serde::{Deserialize, Serialize};

/// 单个 suite 的执行状态计数。
///
/// 各计数互不重叠：`cases` 是声明的用例总数，状态计数之和必须等于 `cases`，
/// 不一致即 `tally-inconsistent` 并失败关闭——用例下落不明或被重复计入时，
/// 其余计数不可信，不得凭 floor 达标判通过；`attempts` 是执行尝试数（重试会
/// 大于 `cases`，但通过量仍以 `passed` 计），`assertions` 是断言数。产品按
/// 自己的口径填，未用的状态留 0。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SuiteTally {
    /// 声明的用例总数。
    pub cases: u64,
    /// 断言数。
    pub assertions: u64,
    /// 执行尝试数；与 `cases` 分开统计，禁止靠重试膨胀覆盖量。
    pub attempts: u64,
    /// 通过数。
    pub passed: u64,
    /// 失败数（命令返回非零或断言不成立）。
    pub failed: u64,
    /// 跳过数。
    pub skipped: u64,
    /// 未验证数：跑到了但没有可信对照（oracle 缺失时不得计通过）。
    pub unverified: u64,
    /// 不可用数：被测或依赖不可达。
    pub unavailable: u64,
    /// 超时数。
    pub timed_out: u64,
    /// 取消数。
    pub cancelled: u64,
    /// 崩溃数。
    pub crashed: u64,
    /// 未执行数：声明了 suite 但一条都没跑。
    pub not_run: u64,
}

/// floor 使用的指标。判别值 snake_case（R1 核心类型口径）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    /// 执行用例数。
    Cases,
    /// 断言数。
    Assertions,
    /// 通过数。
    Passed,
}

/// 一条 floor：指标与最小值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Floor {
    /// 比较的指标。
    pub metric: Metric,
    /// 最小值；`value < min` 即未达标。
    pub min: u64,
}

impl SuiteTally {
    /// 按指标取计数。
    pub fn value(&self, metric: Metric) -> u64 {
        match metric {
            Metric::Cases => self.cases,
            Metric::Assertions => self.assertions,
            Metric::Passed => self.passed,
        }
    }
}

/// oracle 资格。
///
/// `required` 为真时，`available` 为假记 oracle 不可用，`provenance` 为空记
/// 证据缺来源——两者都失败关闭，不得回退被测实现或上次结果计通过。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleQualification {
    /// 本 suite 是否必须有真机 / 官方语料 / 冻结基线对照。
    pub required: bool,
    /// 对照是否实际可用。
    pub available: bool,
    /// 来源标识（引擎版本、语料 revision、采集证据引用）；`required` 且为空即证据缺失。
    pub provenance: Option<String>,
}

impl OracleQualification {
    /// 无对照需求的纯计数 suite。
    pub fn not_required() -> Self {
        Self { required: false, available: false, provenance: None }
    }
}

/// 基线信号。判别值 snake_case；与台账问题词汇（`stale`、`unlisted`）同一口径。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaselineSignal {
    /// 本 suite 不依赖冻结基线。
    None,
    /// 与基线一致。
    Matched,
    /// 基线条目陈旧（被测已变化但台账未更新）。
    Stale,
    /// 出现未登记差异。
    Unlisted,
    /// 真实合同变化，需显式重采基线，不得用旧算法绕过。
    Changed,
}

/// 一个 suite 进门禁所需的全部证据。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuiteReport {
    /// suite 稳定命名；`missing_suites` 比对与问题归属都靠它。
    pub name: String,
    /// 执行状态计数。
    pub tally: SuiteTally,
    /// 本 suite 的 floor 列表，按输入顺序比较。
    pub floors: Vec<Floor>,
    /// oracle 资格。
    pub oracle: OracleQualification,
    /// 基线信号。
    pub baseline: BaselineSignal,
}

/// 门禁策略。缺省全部失败关闭。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatePolicy {
    /// 是否允许部分 suite 未执行（`not_run > 0`）。缺省 false。
    pub allow_partial: bool,
    /// 是否允许跳过用例。缺省 false。
    pub allow_skipped: bool,
    /// 是否允许零用例 suite。缺省 false；纯分析调用显式打开，不能把空集合
    /// 当作完整验收通过。
    pub allow_empty_suites: bool,
    /// 必须出现的 suite 名单；缺失即记 `missing-suite`。
    pub required_suites: Vec<String>,
}

impl Default for GatePolicy {
    fn default() -> Self {
        Self { allow_partial: false, allow_skipped: false, allow_empty_suites: false, required_suites: Vec::new() }
    }
}

/// 门禁问题。判别值 kebab-case，与台账问题词汇（`unlisted`、`signal-changed` 等）
/// 同一口径（R7 影子迁移期逐字节对齐，故此处用 kebab-case）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum GateProblem {
    /// 状态计数之和与声明的 `cases` 不一致：用例下落不明（和偏小）或被重复
    /// 计入（和偏大），报告本身不可信。
    TallyInconsistent {
        /// 归属 suite。
        suite: String,
        /// 声明的用例总数。
        cases: u64,
        /// 状态计数实际总和。
        counted: u64,
    },
    /// suite 声明零用例。纯分析集合可以合法，但不能作为完整验收通过。
    EmptySuite {
        /// 归属 suite。
        suite: String,
    },
    /// 同一 suite 名重复出现，计数无法归属。
    DuplicateSuite {
        /// 重复的 suite 名。
        suite: String,
    },
    /// 存在失败用例。
    Failed {
        /// 归属 suite。
        suite: String,
        /// 失败数量。
        count: u64,
    },
    /// 存在跳过用例（策略未放行时）。
    Skipped {
        /// 归属 suite。
        suite: String,
        /// 跳过数量。
        count: u64,
    },
    /// 存在未验证用例（跑到了但无可信对照）。
    Unverified {
        /// 归属 suite。
        suite: String,
        /// 未验证数量。
        count: u64,
    },
    /// 存在不可用用例。
    Unavailable {
        /// 归属 suite。
        suite: String,
        /// 不可用数量。
        count: u64,
    },
    /// 存在超时用例。
    TimedOut {
        /// 归属 suite。
        suite: String,
        /// 超时数量。
        count: u64,
    },
    /// 存在取消用例。
    Cancelled {
        /// 归属 suite。
        suite: String,
        /// 取消数量。
        count: u64,
    },
    /// 存在崩溃用例。
    Crashed {
        /// 归属 suite。
        suite: String,
        /// 崩溃数量。
        count: u64,
    },
    /// 存在未执行用例（策略未放行部分执行时）。
    NotRun {
        /// 归属 suite。
        suite: String,
        /// 未执行数量。
        count: u64,
    },
    /// 必需 oracle 不可用。
    OracleUnavailable {
        /// 归属 suite。
        suite: String,
    },
    /// 必需 oracle 缺来源标识，证据不完整。
    MissingEvidence {
        /// 归属 suite。
        suite: String,
    },
    /// 基线条目陈旧。
    BaselineStale {
        /// 归属 suite。
        suite: String,
    },
    /// 出现未登记差异。
    BaselineUnlisted {
        /// 归属 suite。
        suite: String,
    },
    /// 真实合同变化，需显式重采基线。
    BaselineChanged {
        /// 归属 suite。
        suite: String,
    },
    /// 某指标低于 floor。
    FloorUnmet {
        /// 归属 suite。
        suite: String,
        /// 比较的指标。
        metric: Metric,
        /// 声明的最小值。
        min: u64,
        /// 实际计数。
        actual: u64,
    },
    /// 策略要求的 suite 未出现。
    MissingSuite {
        /// 缺失的 suite 名。
        suite: String,
    },
}

/// 门禁判定结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateVerdict {
    /// 无任何问题才算通过。
    pub ok: bool,
    /// 问题列表：先按 suite 输入顺序，suite 内固定为 tally-inconsistent、failed、
    /// skipped、unverified、unavailable、timed_out、cancelled、crashed、not_run、
    /// oracle-unavailable、missing-evidence、baseline-*、然后按 floors 输入顺序；
    /// 末尾追加 missing-suite。
    pub problems: Vec<GateProblem>,
}

/// 按失败关闭规则判定一组 suite。
pub fn evaluate(suites: &[SuiteReport], policy: &GatePolicy) -> GateVerdict {
    let mut problems = Vec::new();
    for suite in suites {
        let name = suite.name.clone();
        let tally = &suite.tally;
        let counted = tally
            .passed
            .saturating_add(tally.failed)
            .saturating_add(tally.skipped)
            .saturating_add(tally.unverified)
            .saturating_add(tally.unavailable)
            .saturating_add(tally.timed_out)
            .saturating_add(tally.cancelled)
            .saturating_add(tally.crashed)
            .saturating_add(tally.not_run);
        if counted != tally.cases {
            problems.push(GateProblem::TallyInconsistent { suite: name.clone(), cases: tally.cases, counted });
        }
        if tally.cases == 0 && !policy.allow_empty_suites {
            problems.push(GateProblem::EmptySuite { suite: name.clone() });
        }
        if tally.failed > 0 {
            problems.push(GateProblem::Failed { suite: name.clone(), count: tally.failed });
        }
        if tally.skipped > 0 && !policy.allow_skipped {
            problems.push(GateProblem::Skipped { suite: name.clone(), count: tally.skipped });
        }
        if tally.unverified > 0 {
            problems.push(GateProblem::Unverified { suite: name.clone(), count: tally.unverified });
        }
        if tally.unavailable > 0 {
            problems.push(GateProblem::Unavailable { suite: name.clone(), count: tally.unavailable });
        }
        if tally.timed_out > 0 {
            problems.push(GateProblem::TimedOut { suite: name.clone(), count: tally.timed_out });
        }
        if tally.cancelled > 0 {
            problems.push(GateProblem::Cancelled { suite: name.clone(), count: tally.cancelled });
        }
        if tally.crashed > 0 {
            problems.push(GateProblem::Crashed { suite: name.clone(), count: tally.crashed });
        }
        if tally.not_run > 0 && !policy.allow_partial {
            problems.push(GateProblem::NotRun { suite: name.clone(), count: tally.not_run });
        }
        if suite.oracle.required && !suite.oracle.available {
            problems.push(GateProblem::OracleUnavailable { suite: name.clone() });
        }
        if suite.oracle.required && suite.oracle.provenance.as_deref().unwrap_or("").is_empty() {
            problems.push(GateProblem::MissingEvidence { suite: name.clone() });
        }
        match suite.baseline {
            BaselineSignal::Stale => problems.push(GateProblem::BaselineStale { suite: name.clone() }),
            BaselineSignal::Unlisted => problems.push(GateProblem::BaselineUnlisted { suite: name.clone() }),
            BaselineSignal::Changed => problems.push(GateProblem::BaselineChanged { suite: name.clone() }),
            BaselineSignal::None | BaselineSignal::Matched => {}
        }
        for floor in &suite.floors {
            let actual = tally.value(floor.metric);
            if actual < floor.min {
                problems.push(GateProblem::FloorUnmet { suite: name.clone(), metric: floor.metric, min: floor.min, actual });
            }
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    for suite in suites {
        if !seen.insert(suite.name.as_str()) {
            problems.push(GateProblem::DuplicateSuite { suite: suite.name.clone() });
        }
    }
    for required in &policy.required_suites {
        if !suites.iter().any(|suite| &suite.name == required) {
            problems.push(GateProblem::MissingSuite { suite: required.clone() });
        }
    }
    GateVerdict { ok: problems.is_empty(), problems }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tally(cases: u64, assertions: u64, passed: u64, failed: u64, skipped: u64) -> SuiteTally {
        SuiteTally { cases, assertions, attempts: cases, passed, failed, skipped, ..SuiteTally::default() }
    }

    fn floor(metric: Metric, min: u64) -> Floor {
        Floor { metric, min }
    }

    fn suite(name: &str, t: SuiteTally, floors: Vec<Floor>) -> SuiteReport {
        SuiteReport { name: name.to_string(), tally: t, floors, oracle: OracleQualification::not_required(), baseline: BaselineSignal::None }
    }

    #[test]
    fn clean_run_passes() {
        let verdict = evaluate(&[suite("sql", tally(100, 0, 100, 0, 0), vec![floor(Metric::Cases, 90)])], &GatePolicy::default());
        assert!(verdict.ok);
        assert!(verdict.problems.is_empty());
    }

    #[test]
    fn skipped_is_red_unless_allowed() {
        let blocked = evaluate(&[suite("sql", tally(100, 0, 99, 0, 1), vec![floor(Metric::Cases, 90)])], &GatePolicy::default());
        assert_eq!(blocked.problems, vec![GateProblem::Skipped { suite: "sql".into(), count: 1 }]);
        let allowed = evaluate(
            &[suite("sql", tally(100, 0, 99, 0, 1), vec![floor(Metric::Cases, 90)])],
            &GatePolicy { allow_skipped: true, ..GatePolicy::default() },
        );
        assert!(allowed.ok);
    }

    #[test]
    fn unverified_and_unavailable_are_red() {
        let t = SuiteTally {
            cases: 10,
            assertions: 0,
            attempts: 10,
            passed: 7,
            failed: 0,
            skipped: 0,
            unverified: 2,
            unavailable: 1,
            ..SuiteTally::default()
        };
        let verdict = evaluate(&[suite("ext", t, vec![])], &GatePolicy::default());
        assert_eq!(
            verdict.problems,
            vec![GateProblem::Unverified { suite: "ext".into(), count: 2 }, GateProblem::Unavailable { suite: "ext".into(), count: 1 },]
        );
    }

    #[test]
    fn required_oracle_unavailable_and_missing_evidence_fail_closed() {
        let s = SuiteReport {
            name: "ext".into(),
            tally: tally(10, 0, 10, 0, 0),
            floors: vec![],
            oracle: OracleQualification { required: true, available: false, provenance: None },
            baseline: BaselineSignal::None,
        };
        let verdict = evaluate(&[s], &GatePolicy::default());
        assert_eq!(
            verdict.problems,
            vec![GateProblem::OracleUnavailable { suite: "ext".into() }, GateProblem::MissingEvidence { suite: "ext".into() },]
        );
    }

    #[test]
    fn baseline_signals_are_problems() {
        for (signal, expected) in [
            (BaselineSignal::Stale, GateProblem::BaselineStale { suite: "s".into() }),
            (BaselineSignal::Unlisted, GateProblem::BaselineUnlisted { suite: "s".into() }),
            (BaselineSignal::Changed, GateProblem::BaselineChanged { suite: "s".into() }),
        ] {
            let mut s = suite("s", tally(5, 0, 5, 0, 0), vec![]);
            s.baseline = signal;
            let verdict = evaluate(&[s], &GatePolicy::default());
            assert_eq!(verdict.problems, vec![expected]);
        }
    }

    #[test]
    fn floors_report_in_input_order_per_suite() {
        let verdict = evaluate(
            &[suite("m", tally(10, 20, 10, 0, 0), vec![floor(Metric::Assertions, 30), floor(Metric::Cases, 15)])],
            &GatePolicy::default(),
        );
        assert_eq!(
            verdict.problems,
            vec![
                GateProblem::FloorUnmet { suite: "m".into(), metric: Metric::Assertions, min: 30, actual: 20 },
                GateProblem::FloorUnmet { suite: "m".into(), metric: Metric::Cases, min: 15, actual: 10 },
            ]
        );
    }

    #[test]
    fn not_run_red_unless_partial_allowed() {
        let t = SuiteTally { cases: 50, not_run: 50, ..SuiteTally::default() };
        let blocked = evaluate(&[suite("p", t, vec![floor(Metric::Cases, 1)])], &GatePolicy::default());
        assert!(blocked.problems.iter().any(|problem| matches!(problem, GateProblem::NotRun { .. })));
        let allowed = evaluate(&[suite("p", t, vec![])], &GatePolicy { allow_partial: true, ..GatePolicy::default() });
        assert!(allowed.ok);
    }

    #[test]
    fn missing_required_suite_is_red() {
        let verdict = evaluate(
            &[suite("present", tally(5, 0, 5, 0, 0), vec![])],
            &GatePolicy { required_suites: vec!["present".into(), "absent".into()], ..GatePolicy::default() },
        );
        assert_eq!(verdict.problems, vec![GateProblem::MissingSuite { suite: "absent".into() }]);
    }

    #[test]
    fn undercounted_cases_fail_closed() {
        // 声明 100 例，状态计数只覆盖 90 例：10 例下落不明。floor 只看 `cases`
        // 声明值，旧实现在此判 `ok = true`，属失败打开。
        let t = SuiteTally { cases: 100, assertions: 0, attempts: 100, passed: 90, ..SuiteTally::default() };
        let verdict = evaluate(&[suite("leak", t, vec![floor(Metric::Cases, 90)])], &GatePolicy::default());
        assert!(!verdict.ok, "cases 与状态计数不一致必须失败关闭");
        assert_eq!(verdict.problems, vec![GateProblem::TallyInconsistent { suite: "leak".into(), cases: 100, counted: 90 }]);
    }

    #[test]
    fn overcounted_cases_fail_closed() {
        // 状态计数之和（13）大于声明的 `cases`（10）：同一用例被重复计入。
        let t = SuiteTally { cases: 10, assertions: 0, attempts: 10, passed: 10, unverified: 3, ..SuiteTally::default() };
        let verdict = evaluate(&[suite("dup", t, vec![])], &GatePolicy::default());
        assert!(!verdict.ok);
        assert_eq!(
            verdict.problems,
            vec![
                GateProblem::TallyInconsistent { suite: "dup".into(), cases: 10, counted: 13 },
                GateProblem::Unverified { suite: "dup".into(), count: 3 },
            ]
        );
    }

    #[test]
    fn empty_required_suite_is_not_a_full_pass() {
        let empty = SuiteTally::default();
        let verdict = evaluate(
            &[suite("required", empty, vec![])],
            &GatePolicy { required_suites: vec!["required".into()], ..GatePolicy::default() },
        );
        assert!(!verdict.ok);
        assert_eq!(verdict.problems, vec![GateProblem::EmptySuite { suite: "required".into() }]);
        // 纯分析调用显式允许空集合，但这不是完整验收的默认结论。
        let analysis = evaluate(
            &[suite("required", empty, vec![])],
            &GatePolicy { allow_empty_suites: true, ..GatePolicy::default() },
        );
        assert!(analysis.ok);
    }

    #[test]
    fn duplicate_suite_names_fail_closed() {
        let verdict = evaluate(
            &[suite("sql", tally(2, 0, 2, 0, 0), vec![]), suite("sql", tally(2, 0, 2, 0, 0), vec![])],
            &GatePolicy::default(),
        );
        assert!(!verdict.ok);
        assert_eq!(verdict.problems, vec![GateProblem::DuplicateSuite { suite: "sql".into() }]);
    }

    #[test]
    fn tally_consistency_golden_order_and_serde() {
        // golden：多 suite 混合问题，钉死 `tally-inconsistent` 在 suite 内最前、
        // 整体顺序与 kebab-case 判别值形态。
        let ok_tally = SuiteTally { cases: 2, assertions: 0, attempts: 2, passed: 2, ..SuiteTally::default() };
        let bad_tally = SuiteTally { cases: 4, assertions: 0, attempts: 4, passed: 3, failed: 2, ..SuiteTally::default() };
        let verdict = evaluate(
            &[suite("a", ok_tally, vec![]), suite("b", bad_tally, vec![floor(Metric::Passed, 4)])],
            &GatePolicy { required_suites: vec!["a".into(), "c".into()], ..GatePolicy::default() },
        );
        assert!(!verdict.ok);
        assert_eq!(
            verdict.problems,
            vec![
                GateProblem::TallyInconsistent { suite: "b".into(), cases: 4, counted: 5 },
                GateProblem::Failed { suite: "b".into(), count: 2 },
                GateProblem::FloorUnmet { suite: "b".into(), metric: Metric::Passed, min: 4, actual: 3 },
                GateProblem::MissingSuite { suite: "c".into() },
            ]
        );
        let json = serde_json::to_string(&verdict.problems[0]).unwrap();
        assert!(json.contains("\"kind\":\"tally-inconsistent\""), "{json}");
        let problem: GateProblem = serde_json::from_str("{\"kind\":\"tally-inconsistent\",\"suite\":\"b\",\"cases\":4,\"counted\":5}").unwrap();
        assert_eq!(problem, GateProblem::TallyInconsistent { suite: "b".into(), cases: 4, counted: 5 });
    }

    #[test]
    fn serde_shape_is_kebab_kind_and_snake_metric() {
        let verdict = evaluate(&[suite("s", tally(5, 0, 5, 0, 0), vec![floor(Metric::Cases, 10)])], &GatePolicy::default());
        let json = serde_json::to_string(&verdict).unwrap();
        assert!(json.contains("\"kind\":\"floor-unmet\""), "{json}");
        assert!(json.contains("\"metric\":\"cases\""), "{json}");
        let problem: GateProblem = serde_json::from_str("{\"kind\":\"oracle-unavailable\",\"suite\":\"s\"}").unwrap();
        assert_eq!(problem, GateProblem::OracleUnavailable { suite: "s".into() });
        let signal: BaselineSignal = serde_json::from_str("\"unlisted\"").unwrap();
        assert_eq!(signal, BaselineSignal::Unlisted);
    }
}
