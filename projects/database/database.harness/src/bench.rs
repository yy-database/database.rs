//! 基准统计与报告行。
//!
//! 只做数学：轮次计划、median/p95/极值、按操作数归一与 intentional-delta
//! 台账。它不运行 workload、不选择对照组、不定义被测操作——那些留在各产品
//! 仓的 bench 脚本里。
//!
//! p95 固化为 nearest-rank：`sorted[min(len-1, ceil(len*0.95)-1)]`，与
//! SQLite / yydb 既有 TS 实现逐字节一致；median 在偶数长度取中间两值平均。
//! 两条公式都有 golden vector 钉死，改动口径必须单独提交并说明理由。

use serde::{Deserialize, Serialize};

/// 轮次计划。`warmup` 轮由调用方执行且不进入样本。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchPlan {
    /// 计入统计的轮数，至少 1。
    pub rounds: u32,
    /// 预热轮数，允许 0。
    pub warmup: u32,
}

/// 一组样本的统计结果。
///
/// 空样本或含非有限值的样本不静默丢弃：全部统计量置为 NaN、`rounds` 保留
/// 样本数，让报告侧显式变红。NaN 无法进 JSON，调用方必须在报告前拦截。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    /// 中位数毫秒。
    pub median_ms: f64,
    /// p95 毫秒，nearest-rank。
    pub p95_ms: f64,
    /// 最小毫秒。
    pub min_ms: f64,
    /// 最大毫秒。
    pub max_ms: f64,
    /// 计入样本数（不含预热）。
    pub rounds: u32,
}

/// 对一组毫秒样本求 median/p95/极值。
#[must_use]
pub fn summarize(samples_ms: &[f64]) -> Stats {
    let rounds = samples_ms.len() as u32;
    if samples_ms.is_empty() || !samples_ms.iter().all(|value| value.is_finite()) {
        return Stats { median_ms: f64::NAN, p95_ms: f64::NAN, min_ms: f64::NAN, max_ms: f64::NAN, rounds };
    }

    let mut sorted = samples_ms.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).expect("all samples are finite"));

    let middle = sorted.len() / 2;
    let median_ms = if sorted.len() % 2 == 0 { (sorted[middle - 1] + sorted[middle]) / 2.0 } else { sorted[middle] };
    let rank = (sorted.len() as f64 * 0.95).ceil() as usize - 1;
    let p95_ms = sorted[rank.min(sorted.len() - 1)];

    Stats { median_ms, p95_ms, min_ms: sorted[0], max_ms: sorted[sorted.len() - 1], rounds }
}

/// 单次操作的归一毫秒（以 median 为基准）。`ops` 为 0 时返回 NaN。
#[must_use]
pub fn per_op_ms(stats: &Stats, ops: u64) -> f64 {
    if ops == 0 { f64::NAN } else { stats.median_ms / ops as f64 }
}

/// 一行基准报告：一个 workload 的本方结果与可选对照组。
///
/// `reference` 为 `None` 时该行必须能在 [`DeltaRegistry`] 里找到 intentional
/// delta 理由，否则报告侧应视为缺对照的意外情况。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchRow {
    /// workload 名，同一报告内唯一。
    pub workload: String,
    /// 数值单位描述，例如 `ms/op`。
    pub unit: String,
    /// 本方统计。
    pub ours: Stats,
    /// 对照组统计。
    pub reference: Option<Stats>,
    /// 对照引擎名，例如 `better-sqlite3`。
    pub reference_engine: Option<String>,
}

/// 一条 intentional delta：某个 workload 有意不配对照组或口径不同。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentionalDelta {
    /// 对应 [`BenchRow::workload`]。
    pub workload: String,
    /// 稳定理由字符串，由各产品仓维护。
    pub reason: String,
}

/// intentional-delta 台账。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaRegistry {
    /// 登记条目，按登记顺序保留。
    entries: Vec<IntentionalDelta>,
}

impl DeltaRegistry {
    /// 由条目构造。
    #[must_use]
    pub fn new(entries: Vec<IntentionalDelta>) -> Self {
        Self { entries }
    }

    /// 登记的条目。
    #[must_use]
    pub fn entries(&self) -> &[IntentionalDelta] {
        &self.entries
    }

    /// 按 workload 查台账。
    #[must_use]
    pub fn find(&self, workload: &str) -> Option<&IntentionalDelta> {
        self.entries.iter().find(|entry| entry.workload == workload)
    }

    /// 一行报告是否可解释：有对照组，或无对照组但有台账理由。
    #[must_use]
    pub fn explains(&self, row: &BenchRow) -> bool {
        row.reference.is_some() || self.find(&row.workload).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_and_p95_match_golden_vectors() {
        let five = summarize(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!((five.median_ms, five.p95_ms, five.min_ms, five.max_ms, five.rounds), (3.0, 5.0, 1.0, 5.0, 5));

        let four = summarize(&[1.0, 2.0, 3.0, 4.0]);
        assert_eq!((four.median_ms, four.p95_ms), (2.5, 4.0));

        let hundred: Vec<f64> = (1..=100).map(|value| value as f64).collect();
        let big = summarize(&hundred);
        assert_eq!((big.median_ms, big.p95_ms), (50.5, 95.0));

        let single = summarize(&[7.5]);
        assert_eq!((single.median_ms, single.p95_ms, single.min_ms, single.max_ms), (7.5, 7.5, 7.5, 7.5));
    }

    #[test]
    fn p95_is_nearest_rank_not_interpolated() {
        // 20 轮：ceil(20*0.95)-1 = 18 → 第 19 小的值。
        let samples: Vec<f64> = (1..=20).map(|value| value as f64).collect();
        assert_eq!(summarize(&samples).p95_ms, 19.0);
    }

    #[test]
    fn input_order_does_not_matter() {
        let forward: Vec<f64> = (1..=20).map(|value| value as f64).collect();
        let mut shuffled = forward.clone();
        shuffled.reverse();
        assert_eq!(summarize(&forward), summarize(&shuffled));
    }

    #[test]
    fn empty_and_non_finite_samples_fail_visibly() {
        assert!(summarize(&[]).median_ms.is_nan());
        let poisoned = summarize(&[1.0, f64::NAN, 3.0]);
        assert!(poisoned.median_ms.is_nan() && poisoned.p95_ms.is_nan() && poisoned.rounds == 3);
    }

    #[test]
    fn per_op_normalizes_the_median() {
        let stats = summarize(&[10.0, 20.0, 30.0]);
        assert_eq!(per_op_ms(&stats, 4), 5.0);
        assert!(per_op_ms(&stats, 0).is_nan());
    }

    #[test]
    fn delta_registry_explains_reference_free_rows() {
        let row = BenchRow { workload: "point-pk".into(), unit: "ms".into(), ours: summarize(&[1.0]), reference: None, reference_engine: None };
        let registry = DeltaRegistry::new(vec![IntentionalDelta { workload: "point-pk".into(), reason: "no comparable index path".into() }]);
        assert!(registry.explains(&row));
        assert!(!registry.explains(&BenchRow { workload: "scan".into(), ..row }));
    }
}
