//! 门禁判定：套件计数汇总与 floor 判定。
//!
//! 本模块只做判定：接收调用方已经解析好的计数（TAP、pytest、unittest、
//! `node:test` 的输出解析全部留在产品仓），给出 floor 达标与 skipped 即红
//! 的判定。它不运行测试命令、不解析任何测试输出、不持有 floor 数值；
//! floor 数值与各仓指标选择是产品仓配置。
//!
//! 判定语义固化三处既有实现：SQLite `test.ts` 的
//! `fail === 0 && skip === 0 && cases >= floor`、MongoDB
//! `gate-test-count.mjs` 的 cases + assertions 双指标、yyds
//! `assert-test-count.mjs` 的 passed floor。

use serde::{Deserialize, Serialize};

/// 一次运行的计数汇总。
///
/// `cases` 与 `assertions` 是不同指标，迁移时不得静默互换；各仓按自己的
/// 口径填对应字段，未使用的指标留 0。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuiteSummary {
    /// 执行用例数。
    pub cases: u64,
    /// 断言数。
    pub assertions: u64,
    /// 通过数。
    pub passed: u64,
    /// 失败数。
    pub failed: u64,
    /// 跳过数。
    pub skipped: u64,
}

impl SuiteSummary {
    /// 按指标取计数。
    pub fn value(&self, metric: Metric) -> u64 {
        match metric {
            Metric::Cases => self.cases,
            Metric::Assertions => self.assertions,
            Metric::Passed => self.passed,
        }
    }
}

/// floor 使用的指标。
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

/// 门禁问题。判别值 kebab-case，与台账问题词汇（`unlisted`、
/// `signal-changed` 等）同一口径。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum GateProblem {
    /// 存在跳过用例；skipped 即红，不计通过。
    Skipped {
        /// 跳过数量。
        count: u64,
    },
    /// 存在失败用例。
    Failed {
        /// 失败数量。
        count: u64,
    },
    /// 某指标低于 floor。
    FloorUnmet {
        /// 比较的指标。
        metric: Metric,
        /// 声明的最小值。
        min: u64,
        /// 实际计数。
        actual: u64,
    },
}

/// 门禁判定结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateVerdict {
    /// 无任何问题才算通过。
    pub ok: bool,
    /// 问题列表，顺序固定为 skipped、failed、然后按 floors 输入顺序。
    pub problems: Vec<GateProblem>,
}

/// 按固定规则判定一次运行：`skipped > 0` 记 `skipped`，`failed > 0`
/// 记 `failed`，每条 floor 按对应指标比较，`value < min` 记 `floor-unmet`。
pub fn verdict(summary: &SuiteSummary, floors: &[Floor]) -> GateVerdict {
    let mut problems = Vec::new();
    if summary.skipped > 0 {
        problems.push(GateProblem::Skipped { count: summary.skipped });
    }
    if summary.failed > 0 {
        problems.push(GateProblem::Failed { count: summary.failed });
    }
    for floor in floors {
        let actual = summary.value(floor.metric);
        if actual < floor.min {
            problems.push(GateProblem::FloorUnmet { metric: floor.metric, min: floor.min, actual });
        }
    }
    GateVerdict { ok: problems.is_empty(), problems }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(cases: u64, assertions: u64, passed: u64, failed: u64, skipped: u64) -> SuiteSummary {
        SuiteSummary { cases, assertions, passed, failed, skipped }
    }

    fn floor(metric: Metric, min: u64) -> Floor {
        Floor { metric, min }
    }

    #[test]
    fn clean_run_passes() {
        let verdict = verdict(&summary(100, 0, 100, 0, 0), &[floor(Metric::Cases, 90)]);
        assert!(verdict.ok);
        assert!(verdict.problems.is_empty());
    }

    #[test]
    fn skipped_is_red() {
        let verdict = verdict(&summary(100, 0, 99, 0, 1), &[floor(Metric::Cases, 90)]);
        assert!(!verdict.ok);
        assert_eq!(verdict.problems, vec![GateProblem::Skipped { count: 1 }]);
    }

    #[test]
    fn failed_is_red() {
        let verdict = verdict(&summary(100, 0, 98, 2, 0), &[]);
        assert!(!verdict.ok);
        assert_eq!(verdict.problems, vec![GateProblem::Failed { count: 2 }]);
    }

    #[test]
    fn floors_report_in_input_order() {
        // MongoDB 双指标口径：assertions 与 cases 同时比较，两条 floor 都未达标。
        let verdict = verdict(&summary(10, 20, 10, 0, 0), &[floor(Metric::Assertions, 30), floor(Metric::Cases, 15)]);
        assert!(!verdict.ok);
        assert_eq!(verdict.problems.len(), 2);
        assert_eq!(verdict.problems[0], GateProblem::FloorUnmet { metric: Metric::Assertions, min: 30, actual: 20 });
        assert_eq!(verdict.problems[1], GateProblem::FloorUnmet { metric: Metric::Cases, min: 15, actual: 10 });
    }

    #[test]
    fn passed_metric_floor_matches_yyds_semantics() {
        // yyds 口径：按 passed 计数比较 floor。
        let first = verdict(&summary(84, 0, 80, 4, 0), &[floor(Metric::Passed, 80)]);
        assert!(!first.ok);
        assert_eq!(first.problems[0], GateProblem::Failed { count: 4 });
        let second = verdict(&summary(84, 0, 80, 0, 0), &[floor(Metric::Passed, 81)]);
        assert_eq!(second.problems, vec![GateProblem::FloorUnmet { metric: Metric::Passed, min: 81, actual: 80 }]);
    }

    #[test]
    fn verdict_matches_sqlite_layer_rule() {
        // SQLite 层规则 golden：ok 当且仅当 failed == 0 && skipped == 0 && cases >= floor。
        for (cases, failed, skipped) in [(1660u64, 0u64, 0u64), (1659, 0, 0), (1660, 1, 0), (1660, 0, 1), (0, 0, 0)] {
            let expected_ok = failed == 0 && skipped == 0 && cases >= 1660;
            let verdict = verdict(&summary(cases, 0, cases - failed - skipped, failed, skipped), &[floor(Metric::Cases, 1660)]);
            assert_eq!(verdict.ok, expected_ok, "cases {cases} failed {failed} skipped {skipped}");
        }
    }

    #[test]
    fn serde_shape_is_kebab_kind_and_snake_metric() {
        let verdict = verdict(&summary(5, 0, 5, 0, 0), &[floor(Metric::Cases, 10)]);
        let json = serde_json::to_string(&verdict).unwrap();
        assert!(json.contains("\"kind\":\"floor-unmet\""), "{json}");
        assert!(json.contains("\"metric\":\"cases\""), "{json}");
        let problem: GateProblem = serde_json::from_str("{\"kind\":\"skipped\",\"count\":2}").unwrap();
        assert_eq!(problem, GateProblem::Skipped { count: 2 });
        let summary: SuiteSummary = serde_json::from_str("{\"cases\":1,\"assertions\":2,\"passed\":3,\"failed\":4,\"skipped\":5}").unwrap();
        assert_eq!(summary.value(Metric::Assertions), 2);
    }
}
