//! SQL 值类型、行 TLV 编码与解码。

use super::*;

/// SQL 查询的结果集。
#[derive(Debug, Clone, PartialEq)]
pub struct SqlResult {
    /// 列名（按投影顺序）。
    pub columns: Vec<String>,
    /// 行（每行的列值按 `columns` 顺序）。
    pub rows: Vec<Vec<SqlValue>>,
    /// DML 影响行数；SELECT 保持 0。
    pub affected_rows: u64,
}

/// SQL 层面的值域。
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    /// 64 位有符号整数。整数列必须走这个变体，避免 `i64` 经 `f64` 往返时丢失精度。
    Integer(i64),
    /// 非整数数值（浮点字面量、除法、AVG 等）。
    Number(f64),
    /// 字符串。
    String(String),
    /// 原始 octets。只有声明为 `bytes` 的列（或 `CAST(... AS BYTEA|BLOB)`）才使用这个变体，
    /// 值不在任何位置经过 UTF-8 解码，因此可以承载 NUL、非 UTF-8 字节与空值。
    Bytes(Vec<u8>),
    /// 布尔。
    Boolean(bool),
    /// NULL。
    Null,
}

/// 文本按 Unicode code point 精确比较：相等、排序、LIKE 与分桶共用同一口径。
///
/// gateway 不建模 collation，也不做大小写折叠。`CAST(x AS BINARY)` 因此只是恒等
/// 字节视图，不会带来第二套比较语义。
pub fn eq_string(a: &str, b: &str) -> bool {
    a == b
}

/// Orders two strings by Unicode code point, matching [`eq_string`].
pub fn cmp_string(a: &str, b: &str) -> std::cmp::Ordering {
    a.cmp(b)
}

/// 解析 SQL 数值字面量：不带小数点或指数的文本优先按 `i64` 精确解析，
/// 只有真正的小数/指数形式才转成 `f64`。
pub(crate) fn numeric_literal(text: &str) -> SqlValue {
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        if let Ok(value) = text.parse::<i64>() {
            return SqlValue::Integer(value);
        }
    }
    match text.parse::<f64>() {
        Ok(value) if value.is_finite() => SqlValue::Number(value),
        _ => SqlValue::String(text.to_owned()),
    }
}

impl SqlValue {
    /// 从 KV 层拿到的原始 bytes 推断一个 SqlValue（无类型信息，宽松推断）。
    ///
    /// 如果 bytes 是合法 UTF-8 并且能解析为整数或浮点数，就返回 [`SqlValue::Integer`] /
    /// [`SqlValue::Number`]；否则返回 String。如果上游知道精确类型，应使用
    /// [`Self::from_row_bytes_typed`]。
    pub fn from_row_bytes(bytes: &[u8]) -> Self {
        let text = String::from_utf8_lossy(bytes).into_owned();
        if let Ok(int) = text.parse::<i64>() {
            return SqlValue::Integer(int);
        }
        if let Ok(float) = text.parse::<f64>() {
            if float.is_finite() {
                return SqlValue::Number(float);
            }
        }
        SqlValue::String(text)
    }

    /// 按声明的 `field_type` 精确解码 bytes。
    ///
    /// - `String` 字段永远返回 [`SqlValue::String`]，保留前导零、空串、非数字文本。
    /// - `Bytes` 字段返回 [`SqlValue::Bytes`]，原始 octets 不做 UTF-8 解码，空 bytes 是空值而不是 NULL。
    /// - `Int64`/`Int32` 按整数解析。
    /// - `Float64` 按浮点解析。
    /// - `Bool` 识别 `true`/`false`/`1`/`0`。
    /// - `Date`/`Datetime` 原样保留写入时的日期/时间文本。
    /// - `Unknown` 退回到 [`Self::from_row_bytes`] 的宽松推断。
    ///
    /// 解码失败时降级为 [`SqlValue::String`] 保留原始 UTF-8 文本，
    /// 让上层继续处理而不是 panic。
    pub fn from_row_bytes_typed(bytes: &[u8], field_type: &crate::catalog_bind::SqlFieldType) -> Self {
        let inner = field_type.inner();
        match inner {
            // 字符串类型保留空串语义：空 bytes 是 `''`，不是 NULL。
            crate::catalog_bind::SqlFieldType::String => SqlValue::String(String::from_utf8_lossy(bytes).into_owned()),
            // Exact decimals arrive on the wire as text; keep them textual so
            // drivers observe the same value they would from MySQL.
            crate::catalog_bind::SqlFieldType::Decimal => SqlValue::String(String::from_utf8_lossy(bytes).into_owned()),
            // `DATE`/`DATETIME` columns store the canonical text spelling; the gateway
            // re-emits it verbatim and the driver-side column type turns it into a date.
            crate::catalog_bind::SqlFieldType::Date | crate::catalog_bind::SqlFieldType::Datetime => {
                SqlValue::String(String::from_utf8_lossy(bytes).into_owned())
            }
            crate::catalog_bind::SqlFieldType::Int64 | crate::catalog_bind::SqlFieldType::Int32 => {
                if bytes.is_empty() {
                    return SqlValue::Null;
                }
                let text = String::from_utf8_lossy(bytes).into_owned();
                text.parse::<i64>().map(SqlValue::Integer).unwrap_or(SqlValue::String(text))
            }
            crate::catalog_bind::SqlFieldType::Float64 => {
                if bytes.is_empty() {
                    return SqlValue::Null;
                }
                let text = String::from_utf8_lossy(bytes).into_owned();
                text.parse::<f64>().ok().filter(|f| f.is_finite()).map(SqlValue::Number).unwrap_or(SqlValue::String(text))
            }
            crate::catalog_bind::SqlFieldType::Bool => {
                if bytes.is_empty() {
                    return SqlValue::Null;
                }
                let text = String::from_utf8_lossy(bytes).into_owned();
                match text.as_str() {
                    "true" | "True" | "TRUE" | "1" => SqlValue::Boolean(true),
                    "false" | "False" | "FALSE" | "0" => SqlValue::Boolean(false),
                    _ => SqlValue::String(text),
                }
            }
            // `bytes` 列是原始 octets：不做 UTF-8 解码，NUL、非 UTF-8 字节与空值都原样保留。
            crate::catalog_bind::SqlFieldType::Bytes => SqlValue::Bytes(bytes.to_vec()),
            // 没有类型信息时只能宽松推断。gateway 写入 NULL 时会省略字段，
            // 因此这里把空 bytes 视为空串，真正缺失的字段由调用方映射为 NULL。
            crate::catalog_bind::SqlFieldType::Unknown => Self::from_row_bytes(bytes),
            crate::catalog_bind::SqlFieldType::Optional(_) => unreachable!("inner() strips Optional"),
        }
    }

    /// 从 SQL 字面量构造。
    pub fn from_literal(lit: &SqlBoundLiteral) -> Self {
        match lit {
            SqlBoundLiteral::Integer(value) => SqlValue::Integer(*value),
            SqlBoundLiteral::Number(text) => numeric_literal(text),
            SqlBoundLiteral::String(text) => SqlValue::String(text.clone()),
            SqlBoundLiteral::Bytes(bytes) => SqlValue::Bytes(bytes.clone()),
            SqlBoundLiteral::Boolean(v) => SqlValue::Boolean(*v),
            SqlBoundLiteral::Null => SqlValue::Null,
        }
    }
}

/// 一个 row 上各个字段的 bytes 值，key 是该列在本查询里的 row id。
#[derive(Debug, Default, Clone)]
pub struct RowFieldValues {
    fields: BTreeMap<u64, Vec<u8>>,
}

impl RowFieldValues {
    /// 创建一个空 row。
    pub fn new() -> Self {
        Self { fields: BTreeMap::new() }
    }

    /// 插入/覆盖一个字段值（field_id → bytes）。
    pub fn insert(&mut self, field_id: u64, value: Vec<u8>) {
        self.fields.insert(field_id, value);
    }

    /// 删除一个字段。SQL NULL 在 row 编码里表示为字段缺失，因此
    /// `SET col = NULL` 必须移除已有字段而不是写入空 bytes。
    pub fn remove(&mut self, field_id: u64) {
        self.fields.remove(&field_id);
    }

    /// 按 field_id 取原始 bytes；不存在返回 None。
    pub fn get(&self, field_id: u64) -> Option<&[u8]> {
        self.fields.get(&field_id).map(|v| v.as_slice())
    }

    /// 合并另一行的字段。JOIN 后的一行同时携带两侧表的字段，
    /// 每张表的 row id 段互不重叠，所以按键覆盖即等价于拼行。
    pub fn merge(&mut self, other: &Self) {
        for (field_id, bytes) in &other.fields {
            self.fields.insert(*field_id, bytes.clone());
        }
    }

    /// 按 `field_type` 精确解码 bytes，保留前导零和空串等字符串语义。
    /// 这是读取字段值的唯一入口，避免在已知 catalog 类型时退回无类型推断。
    pub fn value_typed(
        &self,
        field_id: u64,
        field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    ) -> Option<SqlValue> {
        let bytes = self.fields.get(&field_id)?;
        let field_type = field_types.get(&field_id).cloned().unwrap_or(crate::catalog_bind::SqlFieldType::Unknown);
        Some(SqlValue::from_row_bytes_typed(bytes, &field_type))
    }
}

/// TLV row 序列化格式（field_id 大端 u64 + 长度大端 u32 + bytes，字段按 field_id 字典序排列）。
pub fn encode_row(values: &RowFieldValues) -> Vec<u8> {
    let mut out = Vec::new();
    let count = values.fields.len() as u16;
    out.extend_from_slice(&count.to_be_bytes());
    for (field_id, bytes) in &values.fields {
        out.extend_from_slice(&field_id.to_be_bytes());
        let len = bytes.len() as u32;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(bytes);
    }
    out
}

/// 从 TLV 格式反序列化出 RowFieldValues。
pub fn decode_row(data: &[u8]) -> Result<RowFieldValues, SqlFrontendError> {
    let mut cursor = Cursor::new(data);
    let count = cursor.read_u16()?;
    let mut row = RowFieldValues::new();
    for _ in 0..count {
        let field_id = cursor.read_u64()?;
        let len = cursor.read_u32()? as usize;
        let bytes = cursor.read_bytes(len)?;
        row.insert(field_id, bytes);
    }
    Ok(row)
}

/// 部分解码：只收集 needed_ids 里的字段，跳过其他 TLV 条目。
/// 当 needed_ids 为空时表示调用方确实需要整行（例如尚未确定投影）。
pub fn decode_row_fields(data: &[u8], needed_ids: &[u64]) -> Result<RowFieldValues, SqlFrontendError> {
    let mut row = RowFieldValues::new();
    if needed_ids.is_empty() {
        return decode_row(data);
    }
    // 逐行扫描里 `needed_ids` 通常只有 1-8 个字段，线性包含判断比每行新建
    // `HashSet` 更便宜；字段数很大时才退回集合，避免 O(字段数 × 列数)。
    let needed_set: Option<std::collections::HashSet<u64>> =
        (needed_ids.len() > 16).then(|| needed_ids.iter().copied().collect());
    let mut cursor = Cursor::new(data);
    let count = cursor.read_u16()?;
    for _ in 0..count {
        let field_id = cursor.read_u64()?;
        let len = cursor.read_u32()? as usize;
        let needed = match &needed_set {
            Some(set) => set.contains(&field_id),
            None => needed_ids.contains(&field_id),
        };
        if needed {
            let bytes = cursor.read_bytes(len)?;
            row.insert(field_id, bytes);
        }
        else {
            // 跳过：手动推进 len 字节。
            let _ = cursor.read_bytes(len)?;
        }
    }
    Ok(row)
}

/// 从 ScanPlan 里收集执行时需要解码的 field_ids（projection + predicate + order_by + expressions）。
pub fn scan_needed_field_ids(plan: &ScanPlan) -> Vec<u64> {
    let mut ids: Vec<u64> = Vec::new();
    for (fid, _) in &plan.projection_fields {
        if !ids.contains(fid) {
            ids.push(*fid);
        }
    }
    for (expr, _) in &plan.projection_expressions {
        collect_column_ids(expr, &mut ids);
    }
    if let Some(pred) = &plan.remaining_predicate {
        collect_column_ids(pred, &mut ids);
    }
    if let Some(group_exprs) = &plan.group_by {
        for g in group_exprs {
            collect_column_ids(g, &mut ids);
        }
    }
    if let Some(having) = &plan.having {
        collect_column_ids(having, &mut ids);
    }
    if let Some(order_items) = &plan.order_by {
        for item in order_items {
            collect_column_ids(&item.expression, &mut ids);
        }
    }
    ids
}

/// 从 ReadPlan / DeletePlan 的表达式里收集执行时需要解码的 field_ids。
pub fn expression_needed_field_ids(
    projection_fields: &[(u64, String)],
    projection_expressions: &[(SqlCatalogBoundExpression, String)],
    predicate: Option<&SqlCatalogBoundExpression>,
) -> Vec<u64> {
    let mut ids: Vec<u64> = Vec::new();
    for (fid, _) in projection_fields {
        if !ids.contains(fid) {
            ids.push(*fid);
        }
    }
    for (expr, _) in projection_expressions {
        collect_column_ids(expr, &mut ids);
    }
    if let Some(predicate) = predicate {
        collect_column_ids(predicate, &mut ids);
    }
    ids
}
