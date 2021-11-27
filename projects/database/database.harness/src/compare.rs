//! 结构比对与差异归类。
//!
//! `compare` 只执行调用方声明的比较模式，不内置任何产品字段白名单，也不生成
//! 期望值；`classify` 把比对结果对照已知差异台账归类。期望值缺失来源时，
//! 即使比对一致也只得到 `unverified`，不得计为通过。

use serde::{Deserialize, Serialize};

use crate::{
    corpus::KnownDifference,
    observe::{Cell, ErrorShape, Observed, ObservedBody},
};

/// 单条差异记录里最多保留的 diff 数，超出后置 `truncated`。
const MAX_DIFFS: usize = 16;

/// 比较模式，由调用方声明。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompareMode {
    /// 全结构逐值一致。
    Exact,
    /// 只比较错误结构中的指定字段（顺序即信号顺序）。
    StructuredFields {
        /// 参与比较的字段名，按调用方给定的顺序产出信号。
        fields: Vec<String>,
    },
    /// 原始字节逐字节一致，差异以十六进制偏移呈现。
    WireHex,
}

/// 一条具体差异。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Diff {
    /// 两侧载荷形态不同（如 rows 对 error）。
    BodyKind {
        /// 期望侧形态名。
        expected: String,
        /// 实测侧形态名。
        actual: String,
    },
    /// 行数不同。
    RowCount {
        /// 期望行数。
        expected: usize,
        /// 实测行数。
        actual: usize,
    },
    /// 某行的列数不同。
    ColumnCount {
        /// 行下标，零起。
        row: usize,
        /// 期望列数。
        expected: usize,
        /// 实测列数。
        actual: usize,
    },
    /// 某个单元格不同。
    RowCell {
        /// 行下标，零起。
        row: usize,
        /// 列下标，零起。
        column: usize,
        /// 期望值。
        expected: Cell,
        /// 实测值。
        actual: Cell,
    },
    /// 字节长度不同。
    WireLength {
        /// 期望长度。
        expected: usize,
        /// 实测长度。
        actual: usize,
    },
    /// 某个偏移上的字节不同。
    WireByte {
        /// 字节偏移，零起。
        offset: usize,
        /// 期望字节，十六进制两位。
        expected: String,
        /// 实测字节，十六进制两位。
        actual: String,
    },
    /// 指定字段不一致；`expected` 或 `actual` 为 `None` 表示字段缺失。
    ErrorField {
        /// 字段名。
        field: String,
        /// 期望值。
        expected: Option<String>,
        /// 实测值。
        actual: Option<String>,
    },
    /// 不可用标记的错误码不同。
    UnavailableCode {
        /// 期望码。
        expected: String,
        /// 实测码。
        actual: String,
    },
}

/// 一次比对的完整结果。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Comparison {
    /// 在给定模式下两侧是否一致。不可用时即使错误码相同也不算语义一致。
    pub matched: bool,
    /// 期望侧缺少来源标记时为真；此时即使一致也不算通过。
    pub unverified: bool,
    /// 任一侧不可用时的执行状态证据；不可用是状态，不参与值语义通过。
    pub unavailable: Option<UnavailableEvidence>,
    /// 差异信号：按比较模式从实测值派生的稳定字符串序列，与台账的
    /// `KnownDifference::signal` 同一词汇。一致时为空。
    pub signal: Vec<String>,
    /// 具体差异，最多 [`MAX_DIFFS`] 条。
    pub diffs: Vec<Diff>,
    /// diff 是否因超过上限而被截断。
    pub truncated: bool,
}

/// 一侧或两侧不可用的证据。`None` 表示该侧不是不可用状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnavailableEvidence {
    /// 期望侧错误码；该侧可用时为 `None`。
    pub expected: Option<String>,
    /// 实测侧错误码；该侧可用时为 `None`。
    pub actual: Option<String>,
}

/// 比对两侧观测值。
///
/// `expected` 的来源缺失时结果标 `unverified`。任一侧为 `Unavailable` 时，
/// `unavailable` 记录两侧错误码，比较结果不得作为值语义通过。信号派生规则：
/// `StructuredFields` 按字段顺序产出 `field=value`；其余模式产出一条
/// 形态级标记。信号只由实测侧的值决定，保证同一观测在任何调用方
/// 得到同一字符串。
pub fn compare(expected: &Observed, actual: &Observed, mode: &CompareMode) -> Comparison {
    let unverified = expected.provenance.is_none();
    let mut diffs: Vec<Diff> = Vec::new();
    let mut truncated = false;

    let unavailable = match (&expected.body, &actual.body) {
        (ObservedBody::Unavailable { code: expected_code, .. }, ObservedBody::Unavailable { code: actual_code, .. }) => {
            Some(UnavailableEvidence { expected: Some(expected_code.clone()), actual: Some(actual_code.clone()) })
        }
        (ObservedBody::Unavailable { code, .. }, _) => Some(UnavailableEvidence { expected: Some(code.clone()), actual: None }),
        (_, ObservedBody::Unavailable { code, .. }) => Some(UnavailableEvidence { expected: None, actual: Some(code.clone()) }),
        _ => None,
    };

    match (&expected.body, &actual.body) {
        (ObservedBody::Rows { rows: expected_rows }, ObservedBody::Rows { rows: actual_rows }) => {
            if expected_rows.len() != actual_rows.len() {
                push_diff(&mut diffs, &mut truncated, Diff::RowCount { expected: expected_rows.len(), actual: actual_rows.len() });
            }
            for (row_index, (expected_row, actual_row)) in expected_rows.iter().zip(actual_rows.iter()).enumerate() {
                if expected_row.len() != actual_row.len() {
                    push_diff(
                        &mut diffs,
                        &mut truncated,
                        Diff::ColumnCount { row: row_index, expected: expected_row.len(), actual: actual_row.len() },
                    );
                    continue;
                }
                for (column_index, (expected_cell, actual_cell)) in expected_row.iter().zip(actual_row.iter()).enumerate() {
                    if expected_cell != actual_cell {
                        push_diff(
                            &mut diffs,
                            &mut truncated,
                            Diff::RowCell {
                                row: row_index,
                                column: column_index,
                                expected: expected_cell.clone(),
                                actual: actual_cell.clone(),
                            },
                        );
                    }
                }
            }
        }
        (ObservedBody::Wire { bytes: expected_bytes }, ObservedBody::Wire { bytes: actual_bytes }) => {
            if expected_bytes.len() != actual_bytes.len() {
                push_diff(&mut diffs, &mut truncated, Diff::WireLength { expected: expected_bytes.len(), actual: actual_bytes.len() });
            }
            for (offset, (expected_byte, actual_byte)) in expected_bytes.iter().zip(actual_bytes.iter()).enumerate() {
                if expected_byte != actual_byte {
                    push_diff(
                        &mut diffs,
                        &mut truncated,
                        Diff::WireByte { offset, expected: format!("{expected_byte:02x}"), actual: format!("{actual_byte:02x}") },
                    );
                }
            }
        }
        (ObservedBody::Error(expected_error), ObservedBody::Error(actual_error)) => {
            compare_errors(expected_error, actual_error, mode, &mut diffs, &mut truncated);
        }
        (ObservedBody::Unavailable { code: expected_code, .. }, ObservedBody::Unavailable { code: actual_code, .. }) => {
            if expected_code != actual_code {
                push_diff(&mut diffs, &mut truncated, Diff::UnavailableCode { expected: expected_code.clone(), actual: actual_code.clone() });
            }
        }
        (expected_body, actual_body) => {
            push_diff(
                &mut diffs,
                &mut truncated,
                Diff::BodyKind { expected: body_name(expected_body).to_owned(), actual: body_name(actual_body).to_owned() },
            );
        }
    }

    let matched = diffs.is_empty();
    let signal = if matched { Vec::new() } else { derive_signal(&expected.body, &actual.body, mode) };
    Comparison { matched, unverified, unavailable, signal, diffs, truncated }
}

fn compare_errors(expected: &ErrorShape, actual: &ErrorShape, mode: &CompareMode, diffs: &mut Vec<Diff>, truncated: &mut bool) {
    let fields: Vec<String> = match mode {
        CompareMode::StructuredFields { fields } => fields.clone(),
        // `Exact` 必须比较双方字段集合：只遍历 expected 会漏掉实测新增字段。
        CompareMode::Exact | CompareMode::WireHex => {
            let mut fields: Vec<String> = expected.fields.keys().chain(actual.fields.keys()).cloned().collect();
            fields.sort();
            fields.dedup();
            fields
        }
    };
    for field in &fields {
        let expected_value = expected.fields.get(field);
        let actual_value = actual.fields.get(field);
        if expected_value != actual_value {
            push_diff(
                diffs,
                truncated,
                Diff::ErrorField { field: field.clone(), expected: expected_value.cloned(), actual: actual_value.cloned() },
            );
        }
    }
}

fn derive_signal(expected: &ObservedBody, actual: &ObservedBody, mode: &CompareMode) -> Vec<String> {
    if let (ObservedBody::Error(_), ObservedBody::Error(actual_error), CompareMode::StructuredFields { fields }) = (expected, actual, mode) {
        return fields
            .iter()
            .map(|field| match actual_error.fields.get(field) {
                Some(value) => format!("{field}={value}"),
                None => format!("{field}=<absent>"),
            })
            .collect();
    }
    vec![
        match mode {
            CompareMode::Exact => "differs",
            CompareMode::StructuredFields { .. } => "fields-differ",
            CompareMode::WireHex => "wire-differs",
        }
        .to_owned(),
    ]
}

fn body_name(body: &ObservedBody) -> &'static str {
    match body {
        ObservedBody::Rows { .. } => "rows",
        ObservedBody::Wire { .. } => "wire",
        ObservedBody::Error(_) => "error",
        ObservedBody::Unavailable { .. } => "unavailable",
    }
}

fn push_diff(diffs: &mut Vec<Diff>, truncated: &mut bool, diff: Diff) {
    if diffs.len() < MAX_DIFFS {
        diffs.push(diff);
    }
    else {
        *truncated = true;
    }
}

/// 差异归类。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DifferenceClass {
    /// 一致且期望值带来源。
    Clean,
    /// 一致但期望值缺少来源——不得计为通过。
    Unverified,
    /// 任一侧不可用：保留两侧错误码，不作为值语义通过。
    Unavailable {
        /// 期望侧错误码。
        expected: Option<String>,
        /// 实测侧错误码。
        actual: Option<String>,
    },
    /// 不一致，但当前用例的台账条目信号完全相同。
    Known {
        /// 台账条目的稳定用例键。
        key: String,
        /// 归类原因。
        reason: String,
    },
    /// 不一致且台账无法解释。
    Unlisted,
}

/// 把一次比对对照已知差异台账归类。
///
/// `case_key` 是当前用例的稳定身份。只有台账条目的 `key` 与它相同、且信号
/// 逐字符串完全相同才算命中；不提供身份或身份不匹配一律记未登记，防止用
/// 其他用例的台账条目掩盖差异。
pub fn classify(comparison: &Comparison, case_key: Option<&str>, known: &[KnownDifference]) -> DifferenceClass {
    if let Some(unavailable) = &comparison.unavailable {
        return DifferenceClass::Unavailable { expected: unavailable.expected.clone(), actual: unavailable.actual.clone() };
    }
    if comparison.matched {
        return if comparison.unverified { DifferenceClass::Unverified } else { DifferenceClass::Clean };
    }
    let Some(case_key) = case_key
    else {
        return DifferenceClass::Unlisted;
    };
    known
        .iter()
        .find(|entry| entry.key == case_key && entry.signal == comparison.signal)
        .map(|entry| DifferenceClass::Known { key: entry.key.clone(), reason: entry.reason.clone() })
        .unwrap_or(DifferenceClass::Unlisted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::TruthProvenance;

    fn external() -> Option<TruthProvenance> {
        Some(TruthProvenance::ExternalEngine { id: "better-sqlite3".into(), version: "3.53.2".into() })
    }

    fn rows(values: Vec<Vec<Cell>>) -> Observed {
        Observed { provenance: external(), body: ObservedBody::Rows { rows: values } }
    }

    fn wire(bytes: &[u8]) -> Observed {
        Observed { provenance: external(), body: ObservedBody::Wire { bytes: bytes.to_vec() } }
    }

    fn error(fields: &[(&str, &str)]) -> Observed {
        let mut map = std::collections::BTreeMap::new();
        for (key, value) in fields {
            map.insert((*key).to_owned(), (*value).to_owned());
        }
        Observed { provenance: external(), body: ObservedBody::Error(ErrorShape { fields: map }) }
    }

    #[test]
    fn exact_rows_match_is_clean() {
        let expected = rows(vec![vec![Cell::Int(1), Cell::Text("a".into())]]);
        let actual = rows(vec![vec![Cell::Int(1), Cell::Text("a".into())]]);
        let comparison = compare(&expected, &actual, &CompareMode::Exact);
        assert!(comparison.matched && !comparison.unverified);
        assert_eq!(classify(&comparison, Some("case-1"), &[]), DifferenceClass::Clean);
    }

    #[test]
    fn missing_provenance_is_unverified_not_passed() {
        let mut expected = rows(vec![vec![Cell::Int(7)]]);
        expected.provenance = None;
        let comparison = compare(&expected, &rows(vec![vec![Cell::Int(7)]]), &CompareMode::Exact);
        assert!(comparison.matched);
        assert!(comparison.unverified);
        assert_eq!(classify(&comparison, Some("case-1"), &[]), DifferenceClass::Unverified);
    }

    #[test]
    fn row_cell_mismatch_reports_position_and_signal() {
        let expected = rows(vec![vec![Cell::Int(1), Cell::Text("a".into())]]);
        let actual = rows(vec![vec![Cell::Int(1), Cell::Text("b".into())]]);
        let comparison = compare(&expected, &actual, &CompareMode::Exact);
        assert!(!comparison.matched);
        assert_eq!(comparison.signal, vec!["differs".to_owned()]);
        assert!(matches!(comparison.diffs.as_slice(), [Diff::RowCell { row: 0, column: 1, .. }]));
    }

    #[test]
    fn wire_hex_reports_offset_in_hex() {
        let comparison = compare(&wire(b"\x01\x02\x03"), &wire(b"\x01\xff\x03"), &CompareMode::WireHex);
        assert!(!comparison.matched);
        assert_eq!(comparison.signal, vec!["wire-differs".to_owned()]);
        assert!(matches!(comparison.diffs.as_slice(), [Diff::WireByte { offset: 1, expected, actual }] if expected == "02" && actual == "ff"));
    }

    #[test]
    fn structured_fields_only_compare_listed_subset() {
        let expected = error(&[("code", "42601"), ("severity", "ERROR"), ("message", "syntax error at or near \"X\"")]);
        let actual = error(&[("code", "42601"), ("severity", "ERROR"), ("message", "syntax error at or near \"Y\"")]);
        let mode = CompareMode::StructuredFields { fields: vec!["code".into(), "severity".into()] };
        let comparison = compare(&expected, &actual, &mode);
        assert!(comparison.matched, "未列出的 message 不参与比较");
    }

    #[test]
    fn structured_fields_signal_is_field_value_form() {
        let expected = error(&[("code", "42601")]);
        let actual = error(&[("code", "1101"), ("codeName", "duplicate key")]);
        let mode = CompareMode::StructuredFields { fields: vec!["code".into(), "codeName".into()] };
        let comparison = compare(&expected, &actual, &mode);
        assert!(!comparison.matched);
        assert_eq!(comparison.signal, vec!["code=1101".to_owned(), "codeName=duplicate key".to_owned()]);
        let known = [KnownDifference {
            key: "case-7".into(),
            signal: vec!["code=1101".into(), "codeName=duplicate key".into()],
            reason: "structured-rejection".into(),
        }];
        assert_eq!(classify(&comparison, Some("case-7"), &known), DifferenceClass::Known { key: "case-7".into(), reason: "structured-rejection".into() });
    }

    #[test]
    fn known_requires_the_current_case_identity() {
        let expected = error(&[("code", "42601")]);
        let actual = error(&[("code", "1101")]);
        let mode = CompareMode::StructuredFields { fields: vec!["code".into()] };
        let comparison = compare(&expected, &actual, &mode);
        let known = [KnownDifference { key: "another-case".into(), signal: vec!["code=1101".into()], reason: "structured-rejection".into() }];
        // 信号相同但属于其他用例：不得命中。
        assert_eq!(classify(&comparison, Some("case-7"), &known), DifferenceClass::Unlisted);
        // 没有身份时也不允许按通用信号命中。
        assert_eq!(classify(&comparison, None, &known), DifferenceClass::Unlisted);
    }

    #[test]
    fn exact_error_compares_both_field_sets() {
        let expected = error(&[("code", "X")]);
        let actual = error(&[("code", "X"), ("fatal", "true")]);
        let comparison = compare(&expected, &actual, &CompareMode::Exact);
        assert!(!comparison.matched, "实测新增字段必须报差异");
        assert!(matches!(comparison.diffs.as_slice(), [Diff::ErrorField { field, expected: None, actual: Some(_) }] if field == "fatal"));
    }

    #[test]
    fn unavailable_is_a_state_not_a_clean_match() {
        let expected = Observed {
            provenance: external(),
            body: ObservedBody::Unavailable { code: "offline".into(), reason: "engine down".into() },
        };
        let actual = Observed {
            provenance: None,
            body: ObservedBody::Unavailable { code: "offline".into(), reason: "engine down".into() },
        };
        let comparison = compare(&expected, &actual, &CompareMode::Exact);
        assert!(comparison.matched, "错误码相同在载荷层一致");
        assert!(
            matches!(&comparison.unavailable, Some(evidence) if evidence.expected.as_deref() == Some("offline") && evidence.actual.as_deref() == Some("offline"))
        );
        assert_eq!(
            classify(&comparison, Some("case-1"), &[]),
            DifferenceClass::Unavailable { expected: Some("offline".into()), actual: Some("offline".into()) }
        );
    }

    #[test]
    fn rows_and_wire_payloads_use_named_fields() {
        let value = rows(vec![vec![Cell::Int(1)]]);
        assert_eq!(serde_json::to_string(&value.body).unwrap(), "{\"kind\":\"rows\",\"rows\":[[{\"int\":1}]]}");
        let bytes = wire(&[1, 2]);
        assert_eq!(serde_json::to_string(&bytes.body).unwrap(), "{\"kind\":\"wire\",\"bytes\":[1,2]}");
        let back: ObservedBody = serde_json::from_str("{\"kind\":\"rows\",\"rows\":[]}").unwrap();
        assert_eq!(back, ObservedBody::Rows { rows: Vec::new() });
        assert!(serde_json::from_str::<ObservedBody>("{\"kind\":\"rows\",\"rows\":[],\"extra\":1}").is_err());
    }

    #[test]
    fn partial_signal_match_is_unlisted() {
        let expected = error(&[("code", "42601")]);
        let actual = error(&[("code", "1101")]);
        let mode = CompareMode::StructuredFields { fields: vec!["code".into()] };
        let comparison = compare(&expected, &actual, &mode);
        let known = [KnownDifference { key: "case-7".into(), signal: vec!["code=2201".into()], reason: "other".into() }];
        assert_eq!(classify(&comparison, Some("case-7"), &known), DifferenceClass::Unlisted);
    }

    #[test]
    fn body_kind_mismatch_is_reported() {
        let expected = error(&[("code", "42601")]);
        let comparison = compare(&expected, &wire(b"\x00"), &CompareMode::Exact);
        assert!(!comparison.matched);
        assert!(matches!(comparison.diffs.as_slice(), [Diff::BodyKind { expected, actual }] if expected == "error" && actual == "wire"));
    }

    #[test]
    fn diffs_are_capped_and_marked_truncated() {
        let wide = |offset: i64| rows((0..40).map(|row| vec![Cell::Int(row as i64 + offset)]).collect());
        let comparison = compare(&wide(0), &wide(1), &CompareMode::Exact);
        assert_eq!(comparison.diffs.len(), MAX_DIFFS);
        assert!(comparison.truncated);
    }

    #[test]
    fn cell_serde_roundtrip_keeps_null_and_text_distinct() {
        let cells = vec![Cell::Null, Cell::Text("null".into()), Cell::Blob(vec![0, 255]), Cell::Float(1.5)];
        let json = serde_json::to_string(&cells).unwrap();
        let back: Vec<Cell> = serde_json::from_str(&json).unwrap();
        assert_eq!(cells, back);
        assert_eq!(serde_json::to_string(&Cell::Null).unwrap(), "\"null\"");
    }
}
