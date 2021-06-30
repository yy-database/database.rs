//! 基准统计与报告行。
//!
//! 只做数学：轮次计划、median/p95/极值、按操作数归一、intentional-delta
//! 台账，以及跨运行对比所需的最小 provenance（工具链、目标三元组、数据规模、
//! 场景参数、轮数口径）。它不运行 workload、不选择对照组、不定义被测操作，
//! 也不探测宿主环境——provenance 的取值由产品仓的 bench 脚本采集后填入。
//!
//! p95 固化为 nearest-rank：`sorted[min(len-1, ceil(len*0.95)-1)]`，与
//! SQLite / yydb 既有 TS 实现逐字节一致；median 在偶数长度取中间两值平均。
//! 两条公式都有 golden vector 钉死，改动口径必须单独提交并说明理由。
//!
//! provenance 的线格式是 additive 的：全部字段 `serde(default)` 且缺省不
//! 序列化，旧 JSON 照旧可解，既有 N-API JSON 直通绑定（`harnessBenchSummary` /
//! `harnessBenchPerOp` 及其 `Stats` DTO）不需要任何改动。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// 轮次计划。`warmup` 轮由调用方执行且不进入样本。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchPlan {
    /// 计入统计的轮数，至少 1。
    pub rounds: u32,
    /// 预热轮数，允许 0。
    pub warmup: u32,
}

/// 一次基准运行的 provenance：跨运行对比所需的最小信息。
///
/// 全部字段可选，缺省不序列化、缺失按默认值反序列化，线格式 additive。
/// 本 crate 不采集也不校验这些值：版本号拼写、三元组格式由调用方（产品仓
/// bench 脚本）负责，这里只冻结字段名与合并语义。空 provenance 与未记录
/// （`None`）在线上等价，调用方应优先用 [`BenchProvenance::is_empty`] 判断
/// 后整体省略。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BenchProvenance {
    /// 场景标识，例如 `redis-point-get`。同一场景矩阵格在不同运行里必须
    /// 取同一值，它是跨运行对比的连接键；与 [`BenchRow::workload`] 的关系
    /// 是「场景 → 该场景内的 workload」。
    pub scenario: Option<String>,
    /// 场景参数，稳定键值对。`BTreeMap` 保证序列化顺序确定，同一组参数
    /// 序列化出的字节逐字节一致。参数值不同即视为不同场景实例，对比侧
    /// 不得把它们合并；键名由场景矩阵定义方冻结。
    pub scenario_params: BTreeMap<String, String>,
    /// 数据规模描述，例如 `keys=10000,value_bytes=1024`。单位拼写在值里，
    /// 本 crate 不解析；缺失时该行不可与带规模的另一运行直接对比。
    pub data_scale: Option<String>,
    /// Rust 工具链版本（`rustc --version` 原文），区分原生绑定的代码生成。
    pub rustc_version: Option<String>,
    /// JS 运行时版本（`node --version` 原文），区分验收层的宿主。纯 Rust
    /// workload 没有宿主时省略。
    pub node_version: Option<String>,
    /// 目标三元组，例如 `x86_64-pc-windows-msvc`。解释执行或 wasm 宿主
    /// 可能没有原生三元组，允许省略。
    pub target_triple: Option<String>,
    /// 源码修订（git rev），用于把一行数字钉回产生它的产物。短哈希与完整
    /// 40 位十六进制都允许，但跨运行对比前必须归一。
    pub rev: Option<String>,
    /// 轮数口径：本次运行实际使用的测量轮数与预热轮数。[`Stats::rounds`]
    /// 是进入统计的样本数，两者不一致说明报告自相矛盾，对比侧应视为异常。
    pub plan: Option<BenchPlan>,
}

impl BenchProvenance {
    /// 是否所有字段都未记录。为空时序列化结果只剩 `{}`，调用方应改传
    /// `None` 让整个字段从线上消失。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.scenario.is_none()
            && self.scenario_params.is_empty()
            && self.data_scale.is_none()
            && self.rustc_version.is_none()
            && self.node_version.is_none()
            && self.target_triple.is_none()
            && self.rev.is_none()
            && self.plan.is_none()
    }

    /// 逐字段补全：`self` 已有的值保持，未记录的字段取 `fallback` 的；
    /// `scenario_params` 按键合并，同名键以 `self` 为准。用于行级
    /// provenance 覆盖运行级 provenance。
    #[must_use]
    pub fn filled_with(&self, fallback: &Self) -> Self {
        let mut scenario_params = fallback.scenario_params.clone();
        scenario_params.extend(self.scenario_params.clone());
        Self {
            scenario: self.scenario.clone().or_else(|| fallback.scenario.clone()),
            scenario_params,
            data_scale: self.data_scale.clone().or_else(|| fallback.data_scale.clone()),
            rustc_version: self.rustc_version.clone().or_else(|| fallback.rustc_version.clone()),
            node_version: self.node_version.clone().or_else(|| fallback.node_version.clone()),
            target_triple: self.target_triple.clone().or_else(|| fallback.target_triple.clone()),
            rev: self.rev.clone().or_else(|| fallback.rev.clone()),
            plan: self.plan.or(fallback.plan),
        }
    }
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
    /// 本行测量的 provenance。`None` 表示继承 [`BenchReport`] 的运行级
    /// provenance；行级字段逐字段覆盖运行级（见 [`BenchReport::row_provenance`]），
    /// 用于同一报告里个别行换了数据规模或参数的情况。缺省不序列化，
    /// 旧 JSON 无此字段照旧可解。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<BenchProvenance>,
}

/// 一次基准运行的汇总输出：报告行加运行级 provenance。
///
/// 这是跨运行对比的顶层信封：`provenance` 记录一次运行的工具链、目标三元组、
/// 轮数口径等共享事实，行级 [`BenchRow::provenance`] 只登记与运行级不同的
/// 字段。序列化 additive：旧读取方忽略新增字段即可，新读取方可以解析没有
/// `provenance` 的旧文档；`{}` 与只有 `rows` 的文档都能解出空 provenance。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct BenchReport {
    /// 运行级 provenance，适用于所有行。`None` 或空 provenance 表示未记录。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<BenchProvenance>,
    /// 报告行，每个 workload 一行；同一报告内 `workload` 唯一。
    pub rows: Vec<BenchRow>,
}

impl BenchReport {
    /// 某一行生效的 provenance：行级已有的字段保持，未记录的字段取运行级
    /// 补全。两侧都没有时返回空 provenance（[`BenchProvenance::is_empty`]
    /// 为真），调用方据此决定省略还是报警。
    ///
    /// 只读取传入的 `row`，不校验它是否属于 [`BenchReport::rows`]。
    #[must_use]
    pub fn row_provenance(&self, row: &BenchRow) -> BenchProvenance {
        match (&self.provenance, &row.provenance) {
            (None, None) => BenchProvenance::default(),
            (None, Some(own)) | (Some(own), None) => own.clone(),
            (Some(base), Some(own)) => own.filled_with(base),
        }
    }
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
        let row = BenchRow { workload: "point-pk".into(), unit: "ms".into(), ours: summarize(&[1.0]), reference: None, reference_engine: None, provenance: None };
        let registry = DeltaRegistry::new(vec![IntentionalDelta { workload: "point-pk".into(), reason: "no comparable index path".into() }]);
        assert!(registry.explains(&row));
        assert!(!registry.explains(&BenchRow { workload: "scan".into(), ..row }));
    }

    /// 构造一份填满全部字段的 provenance，供线格式测试复用。
    fn full_provenance() -> BenchProvenance {
        let mut params = BTreeMap::new();
        params.insert("batch".to_owned(), "500".to_owned());
        params.insert("value_bytes".to_owned(), "1024".to_owned());
        BenchProvenance {
            scenario: Some("redis-pipeline-set".to_owned()),
            scenario_params: params,
            data_scale: Some("keys=500".to_owned()),
            rustc_version: Some("rustc 1.99.1 (58cf8f9c3)".to_owned()),
            node_version: Some("v22.11.0".to_owned()),
            target_triple: Some("x86_64-pc-windows-msvc".to_owned()),
            rev: Some("269d874".to_owned()),
            plan: Some(BenchPlan { rounds: 5, warmup: 1 }),
        }
    }

    #[test]
    fn provenance_stays_off_the_wire_when_unset() {
        let row = BenchRow { workload: "get-seq".into(), unit: "ms".into(), ours: summarize(&[1.0]), reference: None, reference_engine: None, provenance: None };
        let row_json = serde_json::to_string(&row).expect("row serializes");
        assert!(!row_json.contains("provenance"), "unset row provenance must not appear on the wire: {row_json}");

        let report = BenchReport { provenance: None, rows: vec![row] };
        let report_json = serde_json::to_string(&report).expect("report serializes");
        assert!(!report_json.contains("provenance"), "unset report provenance must not appear on the wire: {report_json}");
    }

    #[test]
    fn provenance_wire_keys_are_frozen_snake_case() {
        let json = serde_json::to_string(&full_provenance()).expect("provenance serializes");
        for key in ["\"scenario\"", "\"scenario_params\"", "\"data_scale\"", "\"rustc_version\"", "\"node_version\"", "\"target_triple\"", "\"rev\"", "\"plan\"", "\"rounds\":5", "\"warmup\":1"] {
            assert!(json.contains(key), "missing frozen wire key {key} in {json}");
        }
    }

    #[test]
    fn old_wire_shapes_still_deserialize_without_provenance() {
        // 既有 N-API 直通绑定产出的旧行格式：五个字段、无 provenance。
        let old_row = r#"{"workload":"get-seq","unit":"ms","ours":{"median_ms":1.0,"p95_ms":1.0,"min_ms":1.0,"max_ms":1.0,"rounds":1},"reference":null,"reference_engine":null}"#;
        let row: BenchRow = serde_json::from_str(old_row).expect("old row json must still parse");
        assert!(row.provenance.is_none());

        let old_report = format!(r#"{{"rows":[{old_row}]}}"#);
        let report: BenchReport = serde_json::from_str(&old_report).expect("rows-only report must parse");
        assert!(report.provenance.is_none());
        assert_eq!(report.rows.len(), 1);

        let empty: BenchReport = serde_json::from_str("{}").expect("empty object must parse");
        assert!(empty.rows.is_empty() && empty.provenance.is_none());
    }

    #[test]
    fn unknown_future_fields_are_ignored() {
        let row_json = r#"{"workload":"get-seq","unit":"ms","ours":{"median_ms":1.0,"p95_ms":1.0,"min_ms":1.0,"max_ms":1.0,"rounds":1},"reference":null,"reference_engine":null,"provenance":{"scenario":"s","machine_id":"host-7"}}"#;
        let row: BenchRow = serde_json::from_str(row_json).expect("unknown provenance key must not break parsing");
        assert_eq!(row.provenance.expect("provenance recorded").scenario.as_deref(), Some("s"));
    }

    #[test]
    fn provenance_roundtrips_with_stable_param_order() {
        let wanted = full_provenance();
        let json = serde_json::to_string(&wanted).expect("serializes");
        assert_eq!(json, serde_json::to_string(&wanted).expect("serializes again"), "serialization must be byte-stable");
        assert!(json.find("\"batch\"").expect("batch key") < json.find("\"value_bytes\"").expect("value_bytes key"), "params serialize sorted");
        assert_eq!(serde_json::from_str::<BenchProvenance>(&json).expect("roundtrip"), wanted);
    }

    #[test]
    fn row_provenance_overrides_report_level_field_by_field() {
        let mut row_level = BenchProvenance::default();
        row_level.scenario = Some("redis-big-value-set".to_owned());
        row_level.rev = Some("row-rev".to_owned());
        row_level.scenario_params.insert("value_bytes".to_owned(), "65536".to_owned());

        let mut run_level = full_provenance();
        run_level.scenario = Some("run-scenario".to_owned());
        run_level.rev = Some("run-rev".to_owned());

        let report = BenchReport {
            provenance: Some(run_level),
            rows: vec![BenchRow { workload: "set-big".into(), unit: "ms".into(), ours: summarize(&[1.0]), reference: None, reference_engine: None, provenance: Some(row_level) }],
        };
        let effective = report.row_provenance(&report.rows[0]);
        assert_eq!(effective.scenario.as_deref(), Some("redis-big-value-set"), "row wins on overlap");
        assert_eq!(effective.rev.as_deref(), Some("row-rev"));
        assert_eq!(effective.rustc_version.as_deref(), Some("rustc 1.99.1 (58cf8f9c3)"), "unset fields fall back to the run");
        assert_eq!(effective.target_triple.as_deref(), Some("x86_64-pc-windows-msvc"));
        assert_eq!(effective.plan, Some(BenchPlan { rounds: 5, warmup: 1 }));
        assert_eq!(effective.scenario_params.get("batch").map(String::as_str), Some("500"), "params merge by key");
        assert_eq!(effective.scenario_params.get("value_bytes").map(String::as_str), Some("65536"), "row param wins");
    }

    #[test]
    fn empty_provenance_is_marked_empty() {
        assert!(BenchProvenance::default().is_empty());
        assert!(!BenchProvenance { rev: Some("x".to_owned()), ..BenchProvenance::default() }.is_empty());
        let mut params_only = BenchProvenance::default();
        params_only.scenario_params.insert("k".to_owned(), "v".to_owned());
        assert!(!params_only.is_empty(), "params alone are a recorded field");
        assert!(!full_provenance().is_empty());
    }
}
