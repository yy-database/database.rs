//! 基准报告图表桥（feature `charts`）。
//!
//! 把 [`crate::bench::BenchRow`] 报告行映射成 `plotter-chart` 的分组柱图：
//! x 轴是 workload，颜色是引擎（本方 / 对照），同组柱体按 `dodge` 分槽。
//! 只做数据映射与 SVG 输出，不碰文件系统、不选输出路径、不决定何时出图——
//! 那些留在各产品仓。
//!
//! SVG 是唯一承诺的图表格式；其余后端（wgpu 等）由调用方在 plotter 侧自行
//! 选择，本模块不引入渲染上下文。

use plotter_chart::{
    Channel, ChannelEncoding, ChartOptions, ChartSpec, DataColumn, DataSnapshot, FieldKind, FieldRef, Mark, PositionAdjust, Value,
};
use plotter_scene::SceneResolver;

use crate::{HarnessError, Result, bench::BenchRow};

/// 图表取哪个统计量做柱高。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchMetric {
    /// 中位数。
    Median,
    /// p95。
    P95,
}

impl BenchMetric {
    fn value(self, stats: &crate::bench::Stats) -> f64 {
        match self {
            Self::Median => stats.median_ms,
            Self::P95 => stats.p95_ms,
        }
    }
}

/// 本方柱的引擎标签。
const OURS_LABEL: &str = "ours";

/// 把报告行渲染为分组柱图 SVG。
///
/// 每行产出「本方」一根柱，有对照组时再加「对照引擎」一根；对照标签取
/// [`BenchRow::reference_engine`]，缺省用 `reference`。空行集报错而不是出
/// 一张空图。NaN 统计量会由 plotter 的 `davinci.chart.non-finite-value`
/// 诊断拒绝，不会画出静默错误的柱。
pub fn bench_chart_svg(rows: &[BenchRow], metric: BenchMetric, title: &str) -> Result<String> {
    if rows.is_empty() {
        return Err(HarnessError::Chart("bench chart needs at least one row".to_owned()));
    }

    let mut workloads: Vec<Value> = Vec::new();
    let mut engines: Vec<Value> = Vec::new();
    let mut millis: Vec<Value> = Vec::new();

    for row in rows {
        workloads.push(Value::text(row.workload.as_str()));
        engines.push(Value::text(OURS_LABEL));
        millis.push(Value::number(metric.value(&row.ours)));

        if let Some(reference) = &row.reference {
            let label = row.reference_engine.as_deref().unwrap_or("reference");
            workloads.push(Value::text(row.workload.as_str()));
            engines.push(Value::text(label));
            millis.push(Value::number(metric.value(reference)));
        }
    }

    let data = DataSnapshot::new(vec![
        DataColumn::new("workload", FieldKind::Nominal, workloads),
        DataColumn::new("engine", FieldKind::Nominal, engines),
        DataColumn::new("ms", FieldKind::Quantitative, millis),
    ]);

    let mut spec = ChartSpec::new(Mark::Bar)
        .with_field(FieldRef::new("workload", FieldKind::Nominal))
        .with_field(FieldRef::new("engine", FieldKind::Nominal))
        .with_field(FieldRef::new("ms", FieldKind::Quantitative))
        .with_encoding(Channel::X, ChannelEncoding::field("workload", FieldKind::Nominal))
        .with_encoding(Channel::Y, ChannelEncoding::field("ms", FieldKind::Quantitative))
        .with_encoding(Channel::Color, ChannelEncoding::field("engine", FieldKind::Nominal))
        .with_position(PositionAdjust::Dodge { channel: Channel::Color });
    spec.title = Some(title.to_owned());

    let lowered = plotter_chart::lower(&spec, &data, &ChartOptions::default()).map_err(|error| HarnessError::Chart(error.to_string()))?;
    let svg = lowered.scene.resolve(&SceneResolver::default()).to_svg().map_err(|error| HarnessError::Chart(error.to_string()))?;
    Ok(svg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench::summarize;

    fn row(workload: &str, ours: &[f64], reference: Option<&[f64]>) -> BenchRow {
        BenchRow {
            workload: workload.to_owned(),
            unit: "ms".to_owned(),
            ours: summarize(ours).expect("valid samples"),
            reference: reference.map(|samples| summarize(samples).expect("valid reference samples")),
            reference_engine: reference.map(|_| "better-sqlite3".to_owned()),
            provenance: None,
        }
    }

    #[test]
    fn grouped_bars_render_as_svg_with_both_engines() {
        let rows = [row("scan", &[10.0, 12.0, 11.0], Some(&[4.0, 5.0, 4.5])), row("point", &[2.0, 1.0, 1.5], None)];
        let svg = bench_chart_svg(&rows, BenchMetric::Median, "bench median").expect("svg");
        assert!(svg.starts_with("<svg"), "output must be an SVG document");
        // 三根柱：scan 两根（ours + better-sqlite3），point 一根。
        assert_eq!(svg.matches("chart-bar-").count(), 3, "one bar per engine row");
        assert!(svg.contains("scan") && svg.contains("point"), "band labels come from the data");
    }

    #[test]
    fn empty_rows_are_refused() {
        assert!(bench_chart_svg(&[], BenchMetric::P95, "empty").is_err());
    }

    #[test]
    fn nan_stats_are_refused_not_silently_plotted() {
        // `summarize` 已经拒绝空样本；这里直接构造一个带 NaN 的 Stats，
        // 证明绘图入口自身也不会把非有限值画成图。
        let nan = crate::bench::Stats { median_ms: f64::NAN, p95_ms: f64::NAN, min_ms: f64::NAN, max_ms: f64::NAN, rounds: 0 };
        let rows = [BenchRow {
            workload: "scan".into(),
            unit: "ms".into(),
            ours: nan,
            reference: None,
            reference_engine: None,
            provenance: None,
        }];
        assert!(bench_chart_svg(&rows, BenchMetric::Median, "nan").is_err());
    }

    #[test]
    fn rendering_is_deterministic() {
        let rows = [row("a", &[1.0, 2.0], Some(&[3.0, 4.0])), row("b", &[5.0], None)];
        let first = bench_chart_svg(&rows, BenchMetric::P95, "t").expect("first");
        let second = bench_chart_svg(&rows, BenchMetric::P95, "t").expect("second");
        assert_eq!(first, second, "same input must produce byte-identical SVG");
    }
}
