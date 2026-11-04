//! 用 YYDS 的 `KeyValueExecutor` 执行一个已计划好的 SQL SELECT。
//!
//! 执行流程非常直接：
//!
//! 1. 从 `SqlExecutionPlan` 里拿到 namespace 和 rowid，构造一个 `KeyValueCommand::Get`。
//! 2. 调 `executor.execute_read_batch`（单个 key 也走 batch 以保持原子读接口一致）。
//! 3. 如果 `Get` 返回 `None`（row 不存在），产生空结果。
//! 4. 如果返回 `Some(bytes)`，按 TLV 格式反序列化成 `RowFieldValues`。
//! 5. 用 `eval_expression` 对每个投影求值，再用 `eval_expression` 对 `remaining_predicate` 做
//!    行级过滤（如果有）。
//! 6. 收集列名和行值组成 `SqlResult`。

use std::collections::{BTreeMap, BTreeSet};

use yyds_execution::{BufferedCommit, CommitCondition, KeyValueExecutor, TransactionHandle};
use yyds_types::{KeyValueAction, KeyValueCommand, Namespace};

use crate::{
    catalog_bind::{
        collect_column_ids, SqlCatalogBoundExpression, SqlCatalogBoundOrderByItem, SqlCatalogBoundWindowSpec,
        SqlCatalogOuterColumn, ROW_ID_STRIDE,
    },
    sql::{
        SqlAggregateFunction, SqlBoundBinaryOperator, SqlBoundLiteral, SqlBoundOrderDirection, SqlBoundSetOperator,
        SqlBoundUnaryOperator, SqlBoundWindowFrame, SqlBoundWindowFrameBound, SqlBoundWindowFrameUnit, SqlDialect,
        SqlErrorKind, SqlFrontendError, SqlNullsOrder, SqlScalarFunction, SqlWindowFunction,
    },
    sql_planner::{
        check_write_literal, pk_literal_to_rowid, ConflictAssignment, ConflictPolicy, DeletePlan, DeleteScanPlan,
        InsertSelectPlan, JoinPlan, JoinSourcePlan, ReadPlan, ReturningProjections, ScalarPlan, ScanPlan, SetOperationPlan,
        SqlExecutionPlan, UniqueConstraintPlan, UpdatePlan, UpdateScanPlan, WriteField, WritePlan,
    },
};

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
fn eq_string(a: &str, b: &str) -> bool {
    a == b
}

fn cmp_string(a: &str, b: &str) -> std::cmp::Ordering {
    a.cmp(b)
}

/// 解析 SQL 数值字面量：不带小数点或指数的文本优先按 `i64` 精确解析，
/// 只有真正的小数/指数形式才转成 `f64`。
fn numeric_literal(text: &str) -> SqlValue {
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
    /// - `Unknown` 退回到 [`Self::from_row_bytes`] 的宽松推断。
    ///
    /// 解码失败时降级为 [`SqlValue::String`] 保留原始 UTF-8 文本，
    /// 让上层继续处理而不是 panic。
    pub fn from_row_bytes_typed(bytes: &[u8], field_type: &crate::catalog_bind::SqlFieldType) -> Self {
        let inner = field_type.inner();
        match inner {
            // 字符串类型保留空串语义：空 bytes 是 `''`，不是 NULL。
            crate::catalog_bind::SqlFieldType::String => SqlValue::String(String::from_utf8_lossy(bytes).into_owned()),
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
    let needed: std::collections::HashSet<u64> = needed_ids.iter().copied().collect();
    let mut cursor = Cursor::new(data);
    let count = cursor.read_u16()?;
    for _ in 0..count {
        let field_id = cursor.read_u64()?;
        let len = cursor.read_u32()? as usize;
        if needed.contains(&field_id) {
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
fn scan_needed_field_ids(plan: &ScanPlan) -> Vec<u64> {
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
fn expression_needed_field_ids(
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

/// 执行一个已计划的 SQL 语句。
///
/// SELECT 返回单行结果集（或空集）；INSERT/UPDATE/DELETE 返回空结果集，
/// 影响行数记录在 `SqlResult.affected_rows`。
pub fn execute_plan(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &SqlExecutionPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    // 子查询必须在行级求值前先跑完并物化成值列表，因为 eval_expression 只有 row 上下文。
    let mut plan = plan.clone();
    resolve_subqueries(&mut plan, executor, transaction, dialect)?;
    let plan = &plan;
    match plan {
        SqlExecutionPlan::Read(read_plan) => execute_read(executor, transaction, read_plan, dialect),
        SqlExecutionPlan::Scan(scan_plan) => execute_scan(executor, transaction, scan_plan, dialect),
        SqlExecutionPlan::Join(join_plan) => execute_join(executor, transaction, join_plan, dialect),
        SqlExecutionPlan::Scalar(scalar_plan) => execute_scalar(scalar_plan, dialect),
        SqlExecutionPlan::SetOperation(set_plan) => execute_set_operation(executor, transaction, set_plan, dialect),
        SqlExecutionPlan::Insert(write_plan) => execute_insert(executor, transaction, write_plan, dialect),
        SqlExecutionPlan::MultiInsert(write_plans) => execute_multi_insert(executor, transaction, write_plans, dialect),
        SqlExecutionPlan::InsertSelect(insert_plan) => execute_insert_select(executor, transaction, insert_plan, dialect),
        SqlExecutionPlan::Update(update_plan) => execute_update(executor, transaction, update_plan, dialect),
        SqlExecutionPlan::UpdateScan(update_plan) => execute_update_scan(executor, transaction, update_plan, dialect),
        SqlExecutionPlan::Delete(delete_plan) => execute_delete(executor, transaction, delete_plan, dialect),
        SqlExecutionPlan::DeleteScan(delete_plan) => execute_delete_scan(executor, transaction, delete_plan, dialect),
    }
}

/// 执行计划里所有 `IN (<subquery>)` 并替换成物化后的 `InValues`。
fn resolve_subqueries(
    plan: &mut SqlExecutionPlan,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    dialect: SqlDialect,
) -> Result<(), SqlFrontendError> {
    match plan {
        SqlExecutionPlan::Read(plan) => {
            lower_optional(&mut plan.remaining_predicate, executor, transaction, dialect)?;
            for (expression, _) in &mut plan.projection_expressions {
                lower_in_place(expression, executor, transaction, dialect)?;
            }
        }
        SqlExecutionPlan::Scan(plan) => {
            lower_optional(&mut plan.remaining_predicate, executor, transaction, dialect)?;
            lower_optional(&mut plan.having, executor, transaction, dialect)?;
            if let Some(group_by) = &mut plan.group_by {
                for expression in group_by {
                    lower_in_place(expression, executor, transaction, dialect)?;
                }
            }
            if let Some(order_by) = &mut plan.order_by {
                for item in order_by {
                    lower_in_place(&mut item.expression, executor, transaction, dialect)?;
                }
            }
            for (expression, _) in &mut plan.projection_expressions {
                lower_in_place(expression, executor, transaction, dialect)?;
            }
        }
        SqlExecutionPlan::Join(plan) => {
            lower_optional(&mut plan.base.on, executor, transaction, dialect)?;
            for join in &mut plan.joins {
                lower_optional(&mut join.on, executor, transaction, dialect)?;
            }
            lower_optional(&mut plan.remaining_predicate, executor, transaction, dialect)?;
            if let Some(order_by) = &mut plan.order_by {
                for item in order_by {
                    lower_in_place(&mut item.expression, executor, transaction, dialect)?;
                }
            }
            for (expression, _) in &mut plan.projection_expressions {
                lower_in_place(expression, executor, transaction, dialect)?;
            }
        }
        SqlExecutionPlan::Scalar(plan) => {
            for (expression, _) in &mut plan.projections {
                lower_in_place(expression, executor, transaction, dialect)?;
            }
        }
        SqlExecutionPlan::SetOperation(plan) => {
            for operand in &mut plan.operands {
                resolve_subqueries(operand, executor, transaction, dialect)?;
            }
        }
        SqlExecutionPlan::Insert(plan) => {
            lower_write_plan(plan, executor, transaction, dialect)?;
        }
        SqlExecutionPlan::MultiInsert(plans) => {
            // 同一语句的各行共享冲突策略与 RETURNING 投影，lower 一次再同步即可，
            // 否则同一个子查询会按行数重复执行。
            if let Some((first, rest)) = plans.split_first_mut() {
                lower_write_plan(first, executor, transaction, dialect)?;
                for plan in rest {
                    plan.conflict = first.conflict.clone();
                    plan.returning = first.returning.clone();
                }
            }
        }
        // 源查询在 `execute_insert_select` 内部再次经过 `execute_plan`，子查询在那一步展开。
        SqlExecutionPlan::InsertSelect(plan) => {
            lower_conflict(&mut plan.conflict, executor, transaction, dialect)?;
            lower_returning(&mut plan.returning, executor, transaction, dialect)?;
        }
        SqlExecutionPlan::Update(plan) => {
            lower_optional(&mut plan.remaining_predicate, executor, transaction, dialect)?;
        }
        SqlExecutionPlan::UpdateScan(plan) => {
            lower_optional(&mut plan.predicate, executor, transaction, dialect)?;
        }
        SqlExecutionPlan::Delete(plan) => {
            lower_optional(&mut plan.remaining_predicate, executor, transaction, dialect)?;
        }
        SqlExecutionPlan::DeleteScan(plan) => {
            lower_optional(&mut plan.predicate, executor, transaction, dialect)?;
        }
    }
    Ok(())
}

fn lower_optional(
    slot: &mut Option<SqlCatalogBoundExpression>,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    dialect: SqlDialect,
) -> Result<(), SqlFrontendError> {
    if let Some(expression) = slot.take() {
        *slot = Some(lower_expression(expression, executor, transaction, dialect)?);
    }
    Ok(())
}

/// INSERT 计划里的冲突谓词与 `RETURNING` 投影同样可能带子查询，执行前一并物化。
fn lower_write_plan(
    plan: &mut WritePlan,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    dialect: SqlDialect,
) -> Result<(), SqlFrontendError> {
    lower_conflict(&mut plan.conflict, executor, transaction, dialect)?;
    lower_returning(&mut plan.returning, executor, transaction, dialect)
}

fn lower_conflict(
    conflict: &mut Option<ConflictPolicy>,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    dialect: SqlDialect,
) -> Result<(), SqlFrontendError> {
    if let Some(ConflictPolicy::DoUpdate { predicate, .. }) = conflict {
        lower_optional(predicate, executor, transaction, dialect)?;
    }
    Ok(())
}

fn lower_returning(
    returning: &mut Option<ReturningProjections>,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    dialect: SqlDialect,
) -> Result<(), SqlFrontendError> {
    if let Some(projections) = returning {
        for (expression, _) in projections {
            lower_in_place(expression, executor, transaction, dialect)?;
        }
    }
    Ok(())
}

fn lower_in_place(
    slot: &mut SqlCatalogBoundExpression,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    dialect: SqlDialect,
) -> Result<(), SqlFrontendError> {
    let placeholder = SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Null);
    let expression = std::mem::replace(slot, placeholder);
    *slot = lower_expression(expression, executor, transaction, dialect)?;
    Ok(())
}

/// 递归物化子查询：内层计划先执行，结果必须是单列，再转成 `InValues`。
fn lower_expression(
    expression: SqlCatalogBoundExpression,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    dialect: SqlDialect,
) -> Result<SqlCatalogBoundExpression, SqlFrontendError> {
    Ok(match expression {
        SqlCatalogBoundExpression::Column(_)
        | SqlCatalogBoundExpression::OuterColumn(_)
        | SqlCatalogBoundExpression::Literal(_)
        | SqlCatalogBoundExpression::Wildcard => expression,
        SqlCatalogBoundExpression::Function { name, args, distinct, window } => SqlCatalogBoundExpression::Function {
            name,
            args: args
                .into_iter()
                .map(|arg| lower_expression(arg, executor, transaction, dialect))
                .collect::<Result<Vec<_>, _>>()?,
            distinct,
            window,
        },
        SqlCatalogBoundExpression::Binary { left, operator, right } => SqlCatalogBoundExpression::Binary {
            left: Box::new(lower_expression(*left, executor, transaction, dialect)?),
            operator,
            right: Box::new(lower_expression(*right, executor, transaction, dialect)?),
        },
        SqlCatalogBoundExpression::Unary { operator, expression } => SqlCatalogBoundExpression::Unary {
            operator,
            expression: Box::new(lower_expression(*expression, executor, transaction, dialect)?),
        },
        SqlCatalogBoundExpression::InValues { expression, values, negated } => SqlCatalogBoundExpression::InValues {
            expression: Box::new(lower_expression(*expression, executor, transaction, dialect)?),
            values,
            negated,
        },
        SqlCatalogBoundExpression::InSubquery { expression, plan, negated } => {
            let expression = Box::new(lower_expression(*expression, executor, transaction, dialect)?);
            let mut plan = *plan;
            resolve_subqueries(&mut plan, executor, transaction, dialect)?;
            // 相关子查询读外层列，无法在这里一次物化：保留原计划，由逐行求值用该行
            // 外层列值重跑。
            if crate::sql_planner::plan_reads_outer(&plan) {
                return Ok(SqlCatalogBoundExpression::InSubquery { expression, plan: Box::new(plan), negated });
            }
            let result = execute_plan(executor, transaction, &plan, dialect)?;
            if result.columns.len() != 1 {
                return Err(SqlFrontendError::new(
                    SqlErrorKind::OperandColumns,
                    "IN subquery must select exactly one column",
                ));
            }
            let values = result
                .rows
                .into_iter()
                .map(|row| row.into_iter().next().map(value_to_literal).unwrap_or(SqlBoundLiteral::Null))
                .collect();
            SqlCatalogBoundExpression::InValues { expression, values, negated }
        }
        SqlCatalogBoundExpression::ScalarSubquery { plan } => {
            let mut plan = *plan;
            resolve_subqueries(&mut plan, executor, transaction, dialect)?;
            // 相关标量子查询逐行重算，基数和多列语义在替换外层值后的物化里照旧检查。
            if crate::sql_planner::plan_reads_outer(&plan) {
                return Ok(SqlCatalogBoundExpression::ScalarSubquery { plan: Box::new(plan) });
            }
            let result = execute_plan(executor, transaction, &plan, dialect)?;
            // 标量子查询的基数语义固定在这一层：0 行取值 `NULL`，多行是基数违规，
            // 按执行错误上报而不是静默取首行。列数在绑定期已保证为 1。
            match result.rows.len() {
                0 => SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Null),
                1 => SqlCatalogBoundExpression::Literal(
                    result
                        .rows
                        .into_iter()
                        .next()
                        .and_then(|row| row.into_iter().next())
                        .map(value_to_literal)
                        .unwrap_or(SqlBoundLiteral::Null),
                ),
                _ => {
                    return Err(SqlFrontendError::new(
                        SqlErrorKind::CardinalityViolation,
                        "scalar subquery returned more than one row",
                    ))
                }
            }
        }
        SqlCatalogBoundExpression::Exists { plan, negated } => {
            let mut plan = *plan;
            resolve_subqueries(&mut plan, executor, transaction, dialect)?;
            // 相关 `EXISTS` 逐行重算，存在性判定仍在逐行求值里完成。
            if crate::sql_planner::plan_reads_outer(&plan) {
                return Ok(SqlCatalogBoundExpression::Exists { plan: Box::new(plan), negated });
            }
            let result = execute_plan(executor, transaction, &plan, dialect)?;
            // `EXISTS` 只关心子查询是否有行，投影内容不参与判断。
            SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Boolean(!result.rows.is_empty() != negated))
        }
        SqlCatalogBoundExpression::Case { branches, else_branch } => {
            let bound_branches = branches
                .into_iter()
                .map(|(w, t)| {
                    let w = Box::new(lower_expression(*w, executor, transaction, dialect)?);
                    let t = Box::new(lower_expression(*t, executor, transaction, dialect)?);
                    Ok((w, t))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let bound_else = match else_branch {
                Some(els) => Some(Box::new(lower_expression(*els, executor, transaction, dialect)?)),
                None => None,
            };
            SqlCatalogBoundExpression::Case { branches: bound_branches, else_branch: bound_else }
        }
        SqlCatalogBoundExpression::Cast { expr, target } => {
            SqlCatalogBoundExpression::Cast { expr: Box::new(lower_expression(*expr, executor, transaction, dialect)?), target }
        }
    })
}

/// 把子查询结果值换回 bound literal，让物化后的表达式保持可比较、可复用。
fn value_to_literal(value: SqlValue) -> SqlBoundLiteral {
    match value {
        SqlValue::Integer(value) => SqlBoundLiteral::Integer(value),
        SqlValue::Number(value) => SqlBoundLiteral::Number(format_number(value)),
        SqlValue::String(value) => SqlBoundLiteral::String(value),
        SqlValue::Bytes(value) => SqlBoundLiteral::Bytes(value),
        SqlValue::Boolean(value) => SqlBoundLiteral::Boolean(value),
        SqlValue::Null => SqlBoundLiteral::Null,
    }
}

/// 执行无 FROM 的 SELECT：对常量与标量内建函数求值，产出一行结果。
fn execute_scalar(plan: &ScalarPlan, dialect: SqlDialect) -> Result<SqlResult, SqlFrontendError> {
    let columns: Vec<String> = plan.projections.iter().map(|(_, label)| label.clone()).collect();
    if plan.suppress_row {
        return Ok(SqlResult { columns, rows: Vec::new(), affected_rows: 0 });
    }
    let row_values = RowFieldValues::new();
    let field_types = BTreeMap::new();
    let mut row = Vec::with_capacity(plan.projections.len());
    for (expression, _) in &plan.projections {
        row.push(eval_expression(expression, &row_values, &field_types, dialect)?);
    }
    Ok(SqlResult { columns, rows: vec![row], affected_rows: 0 })
}

/// 执行一个集合运算：每个操作数先完整执行，再按运算符从左到右折叠。
///
/// 每一级运算符的 SQL 语义都在 [`merge_set_rows`] 里实现：`UNION ALL` 保留重复，
/// `UNION` / `INTERSECT` / `EXCEPT` 都先去重再筛选。整个层级合并完成后，
/// `ORDER BY` / `OFFSET` / `LIMIT` 只施加一次，因此整体排序与截断不会作用到中间结果。
fn execute_set_operation(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &SetOperationPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let mut results = Vec::with_capacity(plan.operands.len());
    for operand in &plan.operands {
        results.push(execute_plan(executor, transaction, operand, dialect)?);
    }
    let mut iter = results.into_iter();
    let first = iter.next().ok_or_else(|| SqlFrontendError::syntax("set operation requires at least one operand"))?;
    let columns = first.columns;
    if plan.suppress_rows {
        return Ok(SqlResult { columns, rows: Vec::new(), affected_rows: 0 });
    }
    let mut rows = first.rows;
    for (operator, next) in plan.operators.iter().zip(iter) {
        rows = merge_set_rows(*operator, rows, next.rows);
    }
    if let Some(items) = &plan.order_by {
        let orderings: Vec<(usize, SqlBoundOrderDirection, bool)> = items
            .iter()
            .map(|item| (item.column, item.direction.clone(), order_nulls_first(&item.direction, item.nulls, plan.nulls_first)))
            .collect();
        rows.sort_by(move |a, b| {
            for (column, direction, nulls_first) in &orderings {
                let ordering = compare_values_with_nulls(a.get(*column), b.get(*column), *nulls_first);
                if ordering != std::cmp::Ordering::Equal {
                    return match direction {
                        SqlBoundOrderDirection::Asc => ordering,
                        SqlBoundOrderDirection::Desc => ordering.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    let limit = plan.limit_after_sort;
    let rows: Vec<Vec<SqlValue>> =
        rows.into_iter().skip(plan.offset_after_sort).take(if limit == 0 { usize::MAX } else { limit }).collect();
    Ok(SqlResult { columns, rows, affected_rows: 0 })
}

/// 按一个集合运算符合并两个已执行的结果集，语义完全等同于对应的二元 SQL 运算。
fn merge_set_rows(operator: SqlBoundSetOperator, left: Vec<Vec<SqlValue>>, right: Vec<Vec<SqlValue>>) -> Vec<Vec<SqlValue>> {
    match operator {
        SqlBoundSetOperator::UnionAll => {
            let mut rows = left;
            rows.extend(right);
            rows
        }
        SqlBoundSetOperator::Union => {
            let mut rows = left;
            rows.extend(right);
            retain_distinct_rows(&mut rows);
            rows
        }
        SqlBoundSetOperator::Intersect => {
            let mut rows = left;
            retain_distinct_rows(&mut rows);
            rows.retain(|row| right.iter().any(|candidate| projected_rows_equal(candidate, row)));
            rows
        }
        SqlBoundSetOperator::Except => {
            let mut rows = left;
            retain_distinct_rows(&mut rows);
            rows.retain(|row| !right.iter().any(|candidate| projected_rows_equal(candidate, row)));
            rows
        }
    }
}

/// 保留每行的首次出现，去掉后续重复行（集合运算的 `UNION` / `INTERSECT` / `EXCEPT` 语义）。
fn retain_distinct_rows(rows: &mut Vec<Vec<SqlValue>>) {
    let mut seen: Vec<Vec<SqlValue>> = Vec::new();
    rows.retain(|row| {
        if seen.iter().any(|candidate| projected_rows_equal(candidate, row)) {
            false
        }
        else {
            seen.push(row.clone());
            true
        }
    });
}

fn execute_read(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &ReadPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    if plan.suppress_rows {
        return Ok(SqlResult { columns: read_column_labels(plan), rows: Vec::new(), affected_rows: 0 });
    }
    let namespace = Namespace(plan.namespace.clone());
    let command = KeyValueCommand { namespace, key: plan.rowid.clone(), action: KeyValueAction::Get };
    let batch_result = match transaction {
        Some(handle) => handle
            .execute_read_batch(&[command.clone()])
            .map_err(|e| internal_error(&format!("TransactionHandle.read_batch failed: {e}")))?,
        None => executor
            .execute_read_batch(None, &[command])
            .map_err(|e| internal_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?,
    };
    let result = batch_result.into_iter().next().ok_or_else(|| internal_error("KeyValueExecutor returned empty batch"))?;
    let row_bytes = match result {
        yyds_types::KeyValueResult::Get(Some(bytes)) => bytes,
        _ => return Ok(SqlResult { columns: read_column_labels(plan), rows: Vec::new(), affected_rows: 0 }),
    };
    // 只解码本次读取实际需要的字段；值仍由 `value_typed` 按 catalog 类型解释。
    let needed_ids =
        expression_needed_field_ids(&plan.projection_fields, &plan.projection_expressions, plan.remaining_predicate.as_ref());
    let row = decode_row_fields(&row_bytes, &needed_ids)?;

    if let Some(ref predicate) = plan.remaining_predicate {
        let v = eval_row_expression(predicate, &row, &plan.field_types, dialect, executor, transaction)?;
        if !truthy(&v) {
            return Ok(SqlResult { columns: read_column_labels(plan), rows: Vec::new(), affected_rows: 0 });
        }
    }

    let columns = read_column_labels(plan);
    let mut row_values: Vec<SqlValue> = Vec::new();
    for (field_id, _) in &plan.projection_fields {
        row_values.push(row.value_typed(*field_id, &plan.field_types).unwrap_or(SqlValue::Null));
    }
    for (expr, _) in &plan.projection_expressions {
        row_values.push(eval_row_expression(expr, &row, &plan.field_types, dialect, executor, transaction)?);
    }
    Ok(SqlResult { columns, rows: vec![row_values], affected_rows: 0 })
}

/// 执行一个多表 JOIN 计划：每个来源先整表扫描，再按 JOIN 顺序做嵌套循环合并。
///
/// INNER JOIN 只保留两侧匹配的组合；LEFT JOIN 额外保留左侧未匹配行（右侧字段缺失 → NULL），
/// RIGHT JOIN 额外保留右侧未匹配行（左侧字段缺失 → NULL），FULL JOIN 两侧未匹配行都保留。
/// 未匹配行只携带本侧表的字段，因此投影顺序仍然是「基表在前、JOIN 表在后」。
fn execute_join(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &JoinPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let columns = join_column_labels(plan);
    if plan.suppress_rows {
        return Ok(SqlResult { columns, rows: Vec::new(), affected_rows: 0 });
    }
    let mut rows = scan_join_source(executor, transaction, &plan.base)?;
    for source in &plan.joins {
        let right_rows = scan_join_source(executor, transaction, source)?;
        let keep_left = matches!(source.kind, crate::sql::SqlBoundJoinKind::Left | crate::sql::SqlBoundJoinKind::Full);
        let keep_right = matches!(source.kind, crate::sql::SqlBoundJoinKind::Right | crate::sql::SqlBoundJoinKind::Full);
        let mut right_matched = vec![false; right_rows.len()];
        let mut merged = Vec::new();
        for left in &rows {
            let mut matched_any = false;
            for (index, right) in right_rows.iter().enumerate() {
                let mut row = left.clone();
                row.merge(right);
                if let Some(on) = &source.on {
                    if !truthy(&eval_row_expression(on, &row, &plan.field_types, dialect, executor, transaction)?) {
                        continue;
                    }
                }
                merged.push(row);
                matched_any = true;
                right_matched[index] = true;
            }
            if keep_left && !matched_any {
                // LEFT / FULL JOIN: 左行未匹配任何右行时仍然保留，右表字段缺失 → NULL
                merged.push(left.clone());
            }
        }
        if keep_right {
            for (index, right) in right_rows.iter().enumerate() {
                if !right_matched[index] {
                    // RIGHT / FULL JOIN: 右行未匹配任何左行时仍然保留，左表字段缺失 → NULL
                    merged.push(right.clone());
                }
            }
        }
        rows = merged;
    }

    let mut staged: Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)> = Vec::new();
    for row in &rows {
        if let Some(predicate) = &plan.remaining_predicate {
            if !truthy(&eval_row_expression(predicate, row, &plan.field_types, dialect, executor, transaction)?) {
                continue;
            }
        }
        let sort_keys: Vec<Option<SqlValue>> = match &plan.order_by {
            Some(items) => items
                .iter()
                .map(|item| {
                    Ok(Some(eval_row_expression(&item.expression, row, &plan.field_types, dialect, executor, transaction)?))
                })
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };
        let mut row_values: Vec<SqlValue> = Vec::new();
        for (field_id, _) in &plan.projection_fields {
            row_values.push(row.value_typed(*field_id, &plan.field_types).unwrap_or(SqlValue::Null));
        }
        for (expr, _) in &plan.projection_expressions {
            row_values.push(eval_row_expression(expr, row, &plan.field_types, dialect, executor, transaction)?);
        }
        staged.push((sort_keys, row_values));
    }
    if plan.distinct {
        retain_distinct_projected_rows(&mut staged);
    }

    let nulls_first = plan.nulls_first;
    if let Some(order_items) = &plan.order_by {
        let orderings = order_items
            .iter()
            .map(|item| (item.direction.clone(), order_nulls_first(&item.direction, item.nulls, nulls_first)))
            .collect::<Vec<_>>();
        staged.sort_by(move |a, b| {
            for (idx, (dir, nulls_first)) in orderings.iter().enumerate() {
                let ord = compare_values_with_nulls(a.0[idx].as_ref(), b.0[idx].as_ref(), *nulls_first);
                if ord != std::cmp::Ordering::Equal {
                    return match dir {
                        crate::sql::SqlBoundOrderDirection::Asc => ord,
                        crate::sql::SqlBoundOrderDirection::Desc => ord.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    let offset = plan.offset_after_sort;
    let limit = plan.limit_after_sort;
    let rows: Vec<Vec<SqlValue>> =
        staged.into_iter().skip(offset).take(if limit == 0 { usize::MAX } else { limit }).map(|(_, r)| r).collect();
    Ok(SqlResult { columns, rows, affected_rows: 0 })
}

/// 整表扫描一个 JOIN 来源，只解码该表参与的字段，并按来源的 row id 存进合并行。
fn scan_join_source(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    source: &JoinSourcePlan,
) -> Result<Vec<RowFieldValues>, SqlFrontendError> {
    let namespace = Namespace(source.namespace.clone());
    let scanned = scan_rows(executor, transaction, &namespace, &[], None, None, 0, "scan")?;
    let mut rows = Vec::with_capacity(scanned.len());
    for (_rowid, value_bytes) in scanned {
        if source.needed_field_ids.is_empty() {
            rows.push(RowFieldValues::new());
            continue;
        }
        let decoded = decode_row_fields(&value_bytes, &source.needed_field_ids)?;
        let mut row = RowFieldValues::new();
        for (field_id, row_id) in source.needed_field_ids.iter().zip(source.row_ids.iter()) {
            if let Some(bytes) = decoded.get(*field_id) {
                row.insert(*row_id, bytes.to_vec());
            }
        }
        rows.push(row);
    }
    Ok(rows)
}

/// 扫描一行 namespace。事务存在时必须走事务快照，否则同一事务内的写入对 SELECT 不可见。
fn scan_rows(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    namespace: &Namespace,
    prefix: &[u8],
    after: Option<&[u8]>,
    before: Option<&[u8]>,
    limit: usize,
    operation: &str,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SqlFrontendError> {
    match transaction {
        Some(handle) => handle
            .execute_scan(namespace, prefix, after, before, limit)
            .map_err(|e| internal_error(&format!("TransactionHandle.{operation} failed: {e}"))),
        None => executor
            .execute_scan(None, namespace, prefix, after, before, limit)
            .map_err(|e| internal_error(&format!("KeyValueExecutor.{operation} failed: {e}"))),
    }
}

fn execute_scan(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &ScanPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let columns = scan_column_labels(plan);
    if plan.suppress_rows {
        return Ok(SqlResult { columns, rows: Vec::new(), affected_rows: 0 });
    }
    let namespace = Namespace(plan.namespace.clone());
    let scanned = scan_rows(
        executor,
        transaction,
        &namespace,
        &plan.rowid_prefix,
        plan.rowid_after.as_deref(),
        plan.rowid_before.as_deref(),
        plan.limit,
        "scan",
    )?;

    let needed_ids = scan_needed_field_ids(plan);

    // Top-N heap 优化：ORDER BY + (LIMIT 或 OFFSET) 时，用 BinaryHeap 保留 Top-N，
    // 避免先收集全量 rows 再排序。容量 = offset + limit（limit=0 视为无限，fallback 到 Vec）。
    let use_top_n = plan.order_by.is_some() && (plan.limit_after_sort > 0 || plan.offset_after_sort > 0);

    if plan.has_window {
        // —— 窗口路径：先解码 + 过滤得到输入行集合，再按分区求值，最后套用
        // DISTINCT / ORDER BY / LIMIT ——
        let mut rows: Vec<RowFieldValues> = Vec::new();
        for (_rowid, value_bytes) in scanned {
            let row = decode_row_fields(&value_bytes, &needed_ids)?;
            if let Some(ref predicate) = plan.remaining_predicate {
                let v = eval_row_expression(predicate, &row, &plan.field_types, dialect, executor, transaction)?;
                if !truthy(&v) {
                    continue;
                }
            }
            rows.push(row);
        }

        // 每个窗口投影整列求值一次；其余表达式仍按行求值。
        let mut window_columns: Vec<Option<Vec<SqlValue>>> = Vec::with_capacity(plan.projection_expressions.len());
        for (expr, _) in &plan.projection_expressions {
            window_columns.push(match expr {
                SqlCatalogBoundExpression::Function { window: Some(_), .. } => {
                    Some(eval_window_projection(expr, &rows, &plan.field_types, plan.nulls_first, dialect)?)
                }
                _ => None,
            });
        }

        let mut staged: Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)> = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            let sort_keys: Vec<Option<SqlValue>> = match &plan.order_by {
                Some(items) => items
                    .iter()
                    .map(|item| {
                        Ok(Some(eval_row_expression(&item.expression, row, &plan.field_types, dialect, executor, transaction)?))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                None => Vec::new(),
            };
            let mut row_values: Vec<SqlValue> = Vec::new();
            for (field_id, _) in &plan.projection_fields {
                row_values.push(row.value_typed(*field_id, &plan.field_types).unwrap_or(SqlValue::Null));
            }
            for (position, (expr, _)) in plan.projection_expressions.iter().enumerate() {
                match &window_columns[position] {
                    Some(values) => row_values.push(values[index].clone()),
                    None => row_values.push(eval_row_expression(expr, row, &plan.field_types, dialect, executor, transaction)?),
                }
            }
            staged.push((sort_keys, row_values));
        }
        return Ok(finish_scan_staged(plan, columns, staged));
    }

    if plan.has_aggregate && plan.group_by.is_none() {
        // —— 全局聚合路径（无 GROUP BY）：需要访问所有 post-filter rows，无 heap 收益 ——
        let mut all_rows: Vec<RowFieldValues> = Vec::new();
        for (_rowid, value_bytes) in scanned {
            let row = decode_row_fields(&value_bytes, &needed_ids)?;
            if let Some(ref predicate) = plan.remaining_predicate {
                let v = eval_row_expression(predicate, &row, &plan.field_types, dialect, executor, transaction)?;
                if !truthy(&v) {
                    continue;
                }
            }
            all_rows.push(row);
        }
        let mut result_row: Vec<SqlValue> = Vec::new();
        for (expr, _) in &plan.projection_expressions {
            result_row.push(eval_aggregate(expr, &all_rows, &plan.field_types, dialect)?);
        }
        if let Some(having) = &plan.having {
            let keep = eval_having(having, &all_rows, &[], &plan.field_types, dialect)?;
            if !truthy(&keep) {
                return Ok(SqlResult { columns, rows: Vec::new(), affected_rows: 0 });
            }
        }
        return Ok(SqlResult { columns, rows: vec![result_row], affected_rows: 0 });
    }

    if let Some(group_exprs) = &plan.group_by {
        // —— GROUP BY 路径：扫描后 bucket + 每 bucket 聚合 + HAVING + ORDER BY + LIMIT ——
        let mut all_rows: Vec<RowFieldValues> = Vec::new();
        for (_rowid, value_bytes) in scanned {
            let row = decode_row_fields(&value_bytes, &needed_ids)?;
            if let Some(ref predicate) = plan.remaining_predicate {
                let v = eval_row_expression(predicate, &row, &plan.field_types, dialect, executor, transaction)?;
                if !truthy(&v) {
                    continue;
                }
            }
            all_rows.push(row);
        }

        // Bucket: rows → Vec<(group_key, rows)>
        let mut buckets: std::collections::BTreeMap<String, (Vec<SqlValue>, Vec<RowFieldValues>)> =
            std::collections::BTreeMap::new();
        for row in &all_rows {
            let key_vals: Vec<SqlValue> = group_exprs
                .iter()
                .map(|e| eval_expression(e, row, &plan.field_types, dialect))
                .collect::<Result<Vec<_>, _>>()?;
            let key_str = serialize_group_key(&key_vals);
            buckets.entry(key_str).or_insert_with(|| (key_vals, Vec::new())).1.push(row.clone());
        }

        // 对每个 bucket 计算结果行
        let mut staged: Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)> = Vec::new();

        for (_key_str, (group_key_vals, bucket_rows)) in &buckets {
            let bucket_rows = bucket_rows;

            // projection_fields: 只允许分组列，值直接取自 group key
            let mut row_values: Vec<SqlValue> = Vec::new();
            for (field_id, _display_name) in &plan.projection_fields {
                let mut matched_key_idx: Option<usize> = None;
                for (idx, ge) in group_exprs.iter().enumerate() {
                    if let SqlCatalogBoundExpression::Column(col) = ge {
                        if col.row_id == *field_id {
                            matched_key_idx = Some(idx);
                            break;
                        }
                    }
                }
                let Some(idx) = matched_key_idx
                else {
                    return Err(internal_error("grouped projection references a column outside the GROUP BY key"));
                };
                row_values.push(group_key_vals[idx].clone());
            }
            // projection_expressions — eval_having 支持嵌套聚合与分组列上的标量函数
            for (expr, _) in &plan.projection_expressions {
                row_values.push(eval_having(expr, bucket_rows, group_exprs, &plan.field_types, dialect)?);
            }

            // HAVING — 用 eval_having 处理聚合函数引用
            if let Some(having) = &plan.having {
                let v = eval_having(having, bucket_rows, group_exprs, &plan.field_types, dialect)?;
                if !truthy(&v) {
                    continue;
                }
            }

            // ORDER BY sort_keys — 可以引用 group key 或聚合列
            let sort_keys: Vec<Option<SqlValue>> = match &plan.order_by {
                Some(items) => items
                    .iter()
                    .map(|item| Ok(Some(eval_having(&item.expression, bucket_rows, group_exprs, &plan.field_types, dialect)?)))
                    .collect::<Result<Vec<_>, _>>()?,
                None => Vec::new(),
            };
            staged.push((sort_keys, row_values));
        }

        // Sort if ORDER BY present — multi-column 逐列比较
        let nulls_first = plan.nulls_first;
        if let Some(order_items) = &plan.order_by {
            let orderings = order_items
                .iter()
                .map(|item| (item.direction.clone(), order_nulls_first(&item.direction, item.nulls, nulls_first)))
                .collect::<Vec<_>>();
            staged.sort_by(move |a, b| {
                for (idx, (dir, nulls_first)) in orderings.iter().enumerate() {
                    let ord = compare_values_with_nulls(a.0[idx].as_ref(), b.0[idx].as_ref(), *nulls_first);
                    if ord != std::cmp::Ordering::Equal {
                        return match dir {
                            crate::sql::SqlBoundOrderDirection::Asc => ord,
                            crate::sql::SqlBoundOrderDirection::Desc => ord.reverse(),
                        };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let offset = plan.offset_after_sort;
        let limit = plan.limit_after_sort;
        let rows: Vec<Vec<SqlValue>> =
            staged.into_iter().skip(offset).take(if limit == 0 { usize::MAX } else { limit }).map(|(_, r)| r).collect();
        return Ok(SqlResult { columns, rows, affected_rows: 0 });
    }

    if false && use_top_n {
        // —— Top-N heap 路径（仅单列 ORDER BY，多列已走 Vec 路径） ——
        let item = plan.order_by.as_ref().unwrap().first().unwrap();
        let order_expr = &item.expression;
        let is_asc = matches!(&item.direction, SqlBoundOrderDirection::Asc);
        let heap_capacity = plan.offset_after_sort.max(1) + plan.limit_after_sort;
        let mut heap: std::collections::BinaryHeap<SortableRow> = std::collections::BinaryHeap::with_capacity(heap_capacity);

        for (_rowid, value_bytes) in scanned {
            let row = decode_row_fields(&value_bytes, &needed_ids)?;
            if let Some(ref predicate) = plan.remaining_predicate {
                let v = eval_expression(predicate, &row, &plan.field_types, dialect)?;
                if !truthy(&v) {
                    continue;
                }
            }

            let sort_key = Some(eval_expression(order_expr, &row, &plan.field_types, dialect)?);
            let mut row_values: Vec<SqlValue> = Vec::new();
            for (field_id, _) in &plan.projection_fields {
                row_values.push(row.value_typed(*field_id, &plan.field_types).unwrap_or(SqlValue::Null));
            }
            for (expr, _) in &plan.projection_expressions {
                row_values.push(eval_expression(expr, &row, &plan.field_types, dialect)?);
            }

            heap.push(SortableRow { sort_key, row_values, is_asc });
            if heap.len() > heap_capacity {
                heap.pop(); // 丢掉 heap 顶（ASC 时是最大的，DESC 时是最小的）
            }
        }

        // into_sorted_vec() 按 Ord Greater 优先（即 heap 顶先）返回降序 → reverse 后变成我们要的排序
        let rows: Vec<Vec<SqlValue>> = heap
            .into_sorted_vec()
            .into_iter()
            .rev()
            .skip(plan.offset_after_sort)
            .take(if plan.limit_after_sort == 0 { usize::MAX } else { plan.limit_after_sort })
            .map(|sr| sr.row_values)
            .collect();
        return Ok(SqlResult { columns, rows, affected_rows: 0 });
    }

    // —— 常规 Vec 路径：无 ORDER BY（kv-scan 顺序）或 ORDER BY 但无 LIMIT/OFFSET ——
    let mut staged: Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)> = Vec::new();
    for (_rowid, value_bytes) in scanned {
        let row = decode_row_fields(&value_bytes, &needed_ids)?;
        if let Some(ref predicate) = plan.remaining_predicate {
            let v = eval_row_expression(predicate, &row, &plan.field_types, dialect, executor, transaction)?;
            if !truthy(&v) {
                continue;
            }
        }

        let sort_keys: Vec<Option<SqlValue>> = match &plan.order_by {
            Some(items) => items
                .iter()
                .map(|item| {
                    Ok(Some(eval_row_expression(&item.expression, &row, &plan.field_types, dialect, executor, transaction)?))
                })
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };
        let mut row_values: Vec<SqlValue> = Vec::new();
        for (field_id, _) in &plan.projection_fields {
            row_values.push(row.value_typed(*field_id, &plan.field_types).unwrap_or(SqlValue::Null));
        }
        for (expr, _) in &plan.projection_expressions {
            row_values.push(eval_row_expression(expr, &row, &plan.field_types, dialect, executor, transaction)?);
        }
        staged.push((sort_keys, row_values));
    }
    Ok(finish_scan_staged(plan, columns, staged))
}

/// 对已求值的投影行套用 `DISTINCT`、`ORDER BY` 与 `OFFSET` / `LIMIT`。
///
/// 常规扫描路径与窗口路径共用这一段，保证两条路径的 DISTINCT、NULL 排序和分页
/// 语义只实现一次。
fn finish_scan_staged(
    plan: &ScanPlan,
    columns: Vec<String>,
    mut staged: Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)>,
) -> SqlResult {
    if plan.distinct {
        retain_distinct_projected_rows(&mut staged);
    }

    // Sort if ORDER BY present — multi-column, 逐列比较直到分出胜负。
    let nulls_first = plan.nulls_first;
    if let Some(order_items) = &plan.order_by {
        let orderings = order_items
            .iter()
            .map(|item| (item.direction.clone(), order_nulls_first(&item.direction, item.nulls, nulls_first)))
            .collect::<Vec<_>>();
        staged.sort_by(move |a, b| {
            for (idx, (dir, nulls_first)) in orderings.iter().enumerate() {
                let ord = compare_values_with_nulls(a.0[idx].as_ref(), b.0[idx].as_ref(), *nulls_first);
                if ord != std::cmp::Ordering::Equal {
                    return match dir {
                        crate::sql::SqlBoundOrderDirection::Asc => ord,
                        crate::sql::SqlBoundOrderDirection::Desc => ord.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    let offset = plan.offset_after_sort;
    let limit = plan.limit_after_sort;
    let rows: Vec<Vec<SqlValue>> =
        staged.into_iter().skip(offset).take(if limit == 0 { usize::MAX } else { limit }).map(|(_, r)| r).collect();

    SqlResult { columns, rows, affected_rows: 0 }
}

/// 执行层内部：Top-N heap 用的可排序行。
/// Ord 语义：`is_asc` → sort_key 越大 Greater；`!is_asc` → sort_key 越小 Greater。
/// BinaryHeap（max-heap）会把 Greater 的堆顶 → ASC heap 顶是最大的（被 pop 掉，保留小的 N 个），
/// DESC heap 顶是最小的（被 pop 掉，保留大的 N 个）。
#[derive(Debug)]
struct SortableRow {
    sort_key: Option<SqlValue>,
    row_values: Vec<SqlValue>,
    is_asc: bool,
}

impl PartialEq for SortableRow {
    fn eq(&self, other: &Self) -> bool {
        matches!(compare_values(self.sort_key.as_ref(), other.sort_key.as_ref()), std::cmp::Ordering::Equal)
    }
}
impl Eq for SortableRow {}

impl PartialOrd for SortableRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortableRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let natural = compare_values(self.sort_key.as_ref(), other.sort_key.as_ref());
        if self.is_asc {
            natural
        }
        else {
            natural.reverse()
        }
    }
}

fn execute_insert(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &WritePlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    match &plan.conflict {
        None => insert_row_if_absent(executor, transaction, plan, false, dialect),
        Some(ConflictPolicy::DoNothing) => insert_row_if_absent(executor, transaction, plan, true, dialect),
        Some(ConflictPolicy::DoUpdate { assignments, predicate }) => {
            upsert_row(executor, transaction, plan, assignments, predicate.as_ref(), dialect)
        }
    }
}

/// 无冲突改写的插入：`PutIfAbsent` 保证并发下也只有一行落地。
///
/// `do_nothing` 为真时冲突按「跳过」处理，否则按唯一约束错误上报。
fn insert_row_if_absent(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &WritePlan,
    do_nothing: bool,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let row = write_plan_to_row(plan);
    if let Some(constraint) =
        unique_conflict_on_write(executor, transaction, &plan.namespace, &plan.unique_constraints, &row, None)?
    {
        // `ON CONFLICT DO NOTHING` 对任何唯一冲突都跳过，而不是只跳过主键冲突。
        if do_nothing {
            return returning_result(&plan.returning, &plan.field_types, Vec::new(), 0, dialect);
        }
        return Err(duplicate_key_error(&constraint));
    }
    let bytes = encode_row(&row);
    let affected = match put_if_absent(executor, transaction, &plan.namespace, &plan.rowid, bytes)? {
        true => 1,
        false if do_nothing => 0,
        false => {
            return Err(SqlFrontendError::new(
                SqlErrorKind::DuplicateKey,
                format!("duplicate key value violates unique constraint: {}", plan.namespace),
            ));
        }
    };
    let written = if affected == 1 { vec![row] } else { Vec::new() };
    returning_result(&plan.returning, &plan.field_types, written, affected, dialect)
}

/// `ON CONFLICT DO UPDATE`：命中既有行时按 `SET` 改写，否则插入新行。
///
/// 事务内改写走 read-modify-write：把读到的 pre-image 交给 OCC，另一个事务
/// 并发提交同一行时 COMMIT 会被拒绝，而不是静默覆盖。`WHERE` 不成立时按
/// 0 行处理，既有行保持原值。
fn upsert_row(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &WritePlan,
    assignments: &[ConflictAssignment],
    predicate: Option<&SqlCatalogBoundExpression>,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    // 读到的行可能刚好被并发事务插入，重新读一次即可命中改写路径。
    for _ in 0..2 {
        let existing_bytes = read_row_bytes(executor, transaction, &namespace, &plan.rowid)?;
        let Some(existing_bytes) = existing_bytes
        else {
            let row = write_plan_to_row(plan);
            if let Some(constraint) =
                unique_conflict_on_write(executor, transaction, &plan.namespace, &plan.unique_constraints, &row, None)?
            {
                return Err(duplicate_key_error(&constraint));
            }
            let bytes = encode_row(&row);
            if put_if_absent(executor, transaction, &plan.namespace, &plan.rowid, bytes)? {
                return returning_result(&plan.returning, &plan.field_types, vec![row], 1, dialect);
            }
            continue;
        };
        let existing_row = decode_row(&existing_bytes)?;
        // 赋值右值同时看到既有行（槽 0）与提议写入的 `EXCLUDED` 行（槽 1，列带上
        // `ROW_ID_STRIDE` 偏移），求值与应用由 `conflict_updated_row` 统一承担。
        let Some(row) = conflict_updated_row(&existing_row, plan, assignments, predicate, dialect)?
        else {
            return returning_result(&plan.returning, &plan.field_types, Vec::new(), 0, dialect);
        };
        if let Some(constraint) =
            unique_conflict_on_write(executor, transaction, &plan.namespace, &plan.unique_constraints, &row, Some(&plan.rowid))?
        {
            return Err(duplicate_key_error(&constraint));
        }
        let encoded = encode_row(&row);
        let command =
            KeyValueCommand { namespace: namespace.clone(), key: plan.rowid.clone(), action: KeyValueAction::Put(encoded) };
        match transaction {
            Some(handle) => handle
                .write_if_unchanged(command, Some(existing_bytes))
                .map_err(|e| write_error(format!("TransactionHandle.upsert failed: {e}"), &e))?,
            None => {
                executor.execute(None, command).map_err(|e| internal_error(&format!("KeyValueExecutor.upsert failed: {e}")))?
            }
        };
        return returning_result(&plan.returning, &plan.field_types, vec![row], 1, dialect);
    }
    Err(SqlFrontendError::new(
        SqlErrorKind::Conflict,
        format!("concurrent write to row in {} prevented the conflict update", plan.namespace),
    ))
}

fn put_if_absent(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    namespace: &str,
    rowid: &[u8],
    bytes: Vec<u8>,
) -> Result<bool, SqlFrontendError> {
    let command = KeyValueCommand {
        namespace: Namespace(namespace.to_string()),
        key: rowid.to_vec(),
        action: KeyValueAction::PutIfAbsent(bytes),
    };
    let result = match transaction {
        Some(handle) => handle.execute(command).map_err(|e| internal_error(&format!("TransactionHandle.insert failed: {e}")))?,
        None => executor.execute(None, command).map_err(|e| internal_error(&format!("KeyValueExecutor.insert failed: {e}")))?,
    };
    match result {
        yyds_types::KeyValueResult::PutIfAbsent { revision } => Ok(revision.is_some()),
        other => Err(internal_error(&format!("unexpected storage result for insert: {other:?}"))),
    }
}

fn read_row_bytes(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    namespace: &Namespace,
    rowid: &[u8],
) -> Result<Option<Vec<u8>>, SqlFrontendError> {
    let command = KeyValueCommand { namespace: namespace.clone(), key: rowid.to_vec(), action: KeyValueAction::Get };
    let results = match transaction {
        Some(handle) => handle
            .execute_read_batch(&[command])
            .map_err(|e| internal_error(&format!("TransactionHandle.read_batch failed: {e}")))?,
        None => executor
            .execute_read_batch(None, &[command])
            .map_err(|e| internal_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?,
    };
    Ok(match results.into_iter().next() {
        Some(yyds_types::KeyValueResult::Get(bytes)) => bytes,
        _ => None,
    })
}

/// 把 `RETURNING` 投影应用到真正落库的行上。没有 `RETURNING` 时写语句不返回结果集。
fn returning_result(
    returning: &Option<ReturningProjections>,
    field_types: &BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    written: Vec<RowFieldValues>,
    affected_rows: u64,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let Some(projections) = returning
    else {
        return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows });
    };
    let columns = returning_column_labels(projections);
    let mut rows = Vec::with_capacity(written.len());
    for row in &written {
        let mut values = Vec::with_capacity(projections.len());
        for (expression, _) in projections {
            values.push(eval_expression(expression, row, field_types, dialect)?);
        }
        rows.push(values);
    }
    Ok(SqlResult { columns, rows, affected_rows })
}

fn returning_column_labels(projections: &ReturningProjections) -> Vec<String> {
    projections.iter().map(|(_, label)| label.clone()).collect()
}

fn execute_multi_insert(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plans: &[WritePlan],
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    // 两条以上写入且没有显式事务时，整条语句必须经执行器的 durable 原子提交边界一次发布；
    // 逐条 `execute` 会让崩溃停在语句中途、只落库一半的行。单行语句本身就是一条命令，
    // 保持既有路径。事务路径由 handle 缓冲、COMMIT 时走同一 `commit_batch` 边界，无需改写。
    if transaction.is_none() && plans.len() > 1 {
        return execute_multi_insert_atomic(executor, plans, dialect);
    }
    let columns = plans.first().and_then(|plan| plan.returning.as_ref()).map(returning_column_labels).unwrap_or_default();
    let mut rows = Vec::new();
    let mut total_affected = 0u64;
    for plan in plans {
        let single = execute_insert(executor, transaction, plan, dialect)?;
        rows.extend(single.rows);
        total_affected = total_affected.saturating_add(single.affected_rows);
    }
    Ok(SqlResult { columns, rows, affected_rows: total_affected })
}

/// 批次里一行的折叠结果：最终行值、是否产生写入、以及针对基存 pre-image 的 OCC 条件。
struct BatchedRow {
    /// 折叠后的当前行值。`None` 表示该行在基存中不存在且尚未有赋值折叠进来。
    current: Option<RowFieldValues>,
    /// 是否把一次写入提交进批次（`DO NOTHING` 跳过行与谓词不成立的改写行都是 `false`）。
    applied: bool,
    /// 提交边界内复核的 pre-image 条件。
    condition: CommitCondition,
}

/// 非事务多行 INSERT 的原子发布：按语句顺序把每行折叠成最终值与 OCC 条件，整批交给
/// `KeyValueExecutor::commit_batch` 的单分片 durable 原子边界。
///
/// 冲突策略在提交前折叠：`DO NOTHING` 跳过基存已存在的行（语句内重复主键同样按跳过），
/// `DO UPDATE` 基于 pre-image 求值谓词与赋值（语句内重复主键顺序折叠到同一行，与逐行语义
/// 一致），无策略时语句内重复主键直接报唯一约束错误。预读与提交之间存在并发窗口，条件被
/// 并发提交打断时整批拒绝并用最新快照重排一次；无策略的并发主键冲突则直接上报唯一约束错误。
fn execute_multi_insert_atomic(
    executor: &dyn KeyValueExecutor,
    plans: &[WritePlan],
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let first = plans.first().expect("atomic multi-insert requires at least one plan");
    let namespace = Namespace(first.namespace.clone());
    let unique_conflict = || {
        SqlFrontendError::new(
            SqlErrorKind::DuplicateKey,
            format!("duplicate key value violates unique constraint: {}", first.namespace),
        )
    };
    let needs_preimage = plans.iter().any(|plan| plan.conflict.is_some());
    for attempt in 0..2 {
        // 冲突策略需要先看到基存 pre-image 才能决定跳过还是改写；无策略的行交给提交边界的
        // OCC 条件复核，不做多余预读。
        let mut preimages: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        if needs_preimage {
            let mut rowids: Vec<Vec<u8>> = Vec::new();
            let mut seen = BTreeSet::new();
            for plan in plans {
                if seen.insert(plan.rowid.clone()) {
                    rowids.push(plan.rowid.clone());
                }
            }
            let commands: Vec<KeyValueCommand> = rowids
                .iter()
                .map(|rowid| KeyValueCommand { namespace: namespace.clone(), key: rowid.clone(), action: KeyValueAction::Get })
                .collect();
            let results = executor
                .execute_read_batch(None, &commands)
                .map_err(|e| internal_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?;
            for (rowid, result) in rowids.iter().zip(results) {
                let bytes = match result {
                    yyds_types::KeyValueResult::Get(bytes) => bytes,
                    other => return Err(internal_error(&format!("unexpected storage result for pre-read: {other:?}"))),
                };
                preimages.insert(rowid.clone(), bytes);
            }
        }

        // 按语句首次出现的顺序折叠每个 rowid 的最终行值与 OCC 条件。
        let mut order: Vec<Vec<u8>> = Vec::new();
        let mut folded: BTreeMap<Vec<u8>, BatchedRow> = BTreeMap::new();
        for plan in plans {
            match &plan.conflict {
                None => {
                    if folded.contains_key(&plan.rowid) {
                        return Err(unique_conflict());
                    }
                    order.push(plan.rowid.clone());
                    folded.insert(
                        plan.rowid.clone(),
                        BatchedRow {
                            current: Some(write_plan_to_row(plan)),
                            applied: true,
                            condition: CommitCondition::PutIfAbsent,
                        },
                    );
                }
                Some(ConflictPolicy::DoNothing) => {
                    if folded.contains_key(&plan.rowid) {
                        continue;
                    }
                    let existing = preimages.get(&plan.rowid).cloned().flatten();
                    let (current, applied) = match &existing {
                        Some(bytes) => (Some(decode_row(bytes)?), false),
                        None => (Some(write_plan_to_row(plan)), true),
                    };
                    order.push(plan.rowid.clone());
                    folded.insert(plan.rowid.clone(), BatchedRow { current, applied, condition: CommitCondition::PutIfAbsent });
                }
                Some(ConflictPolicy::DoUpdate { assignments, predicate }) => {
                    if !folded.contains_key(&plan.rowid) {
                        let existing = preimages.get(&plan.rowid).cloned().flatten();
                        let condition = match &existing {
                            Some(bytes) => CommitCondition::ValueEquals(Some(bytes.clone())),
                            None => CommitCondition::PutIfAbsent,
                        };
                        let current = match &existing {
                            Some(bytes) => Some(decode_row(bytes)?),
                            None => None,
                        };
                        order.push(plan.rowid.clone());
                        folded.insert(plan.rowid.clone(), BatchedRow { current, applied: false, condition });
                    }
                    let entry = folded.get_mut(&plan.rowid).expect("entry inserted above");
                    match &entry.current {
                        None => {
                            entry.current = Some(write_plan_to_row(plan));
                            entry.applied = true;
                        }
                        Some(existing_row) => {
                            if let Some(updated) =
                                conflict_updated_row(existing_row, plan, assignments, predicate.as_ref(), dialect)?
                            {
                                entry.current = Some(updated);
                                entry.applied = true;
                            }
                        }
                    }
                }
            }
        }

        if !first.unique_constraints.is_empty() {
            // 基存快照 + 同语句已折叠行共同决定 `UNIQUE` 冲突，整批在提交前拦截。
            let stored = scan_rows(executor, None, &namespace, &[], None, None, 0, "unique_check")?;
            let mut pending: Vec<(Vec<u8>, RowFieldValues)> = Vec::new();
            for rowid in &order {
                let entry = &folded[rowid];
                if !entry.applied {
                    continue;
                }
                let row = entry.current.as_ref().expect("applied entry carries a row");
                let pending_without_self: Vec<(Vec<u8>, RowFieldValues)> =
                    pending.iter().filter(|(pending_rowid, _)| pending_rowid != rowid).cloned().collect();
                if let Some(constraint) =
                    unique_conflict_in_snapshot(&first.unique_constraints, row, Some(rowid), &pending_without_self, &stored)?
                {
                    return Err(duplicate_key_error(&constraint));
                }
                pending.push((rowid.clone(), row.clone()));
            }
        }

        let batch: Vec<BufferedCommit> = order
            .iter()
            .filter_map(|rowid| {
                let entry = &folded[rowid];
                entry.applied.then(|| BufferedCommit {
                    command: KeyValueCommand {
                        namespace: namespace.clone(),
                        key: rowid.clone(),
                        action: KeyValueAction::Put(encode_row(entry.current.as_ref().expect("applied entry carries a row"))),
                    },
                    condition: Some(entry.condition.clone()),
                })
            })
            .collect();
        match executor.commit_batch(None, batch) {
            Ok(()) => {
                let written: Vec<RowFieldValues> = order
                    .iter()
                    .filter_map(|rowid| {
                        let entry = &folded[rowid];
                        entry.applied.then(|| entry.current.clone().expect("applied entry carries a row"))
                    })
                    .collect();
                let affected = written.len() as u64;
                return returning_result(&first.returning, &first.field_types, written, affected, dialect);
            }
            Err(yyds_types::Error::CasConflict { .. }) if needs_preimage && attempt == 0 => continue,
            Err(yyds_types::Error::CasConflict { .. }) if needs_preimage => {
                return Err(SqlFrontendError::new(
                    SqlErrorKind::Conflict,
                    format!("concurrent writes to {} prevented the batch commit", first.namespace),
                ));
            }
            Err(yyds_types::Error::CasConflict { .. }) => return Err(unique_conflict()),
            Err(error) => {
                return Err(internal_error(&format!("KeyValueExecutor.commit_batch failed: {error}")));
            }
        }
    }
    Err(SqlFrontendError::new(
        SqlErrorKind::Conflict,
        format!("concurrent writes to {} prevented the batch commit", first.namespace),
    ))
}

/// 计算 `ON CONFLICT DO UPDATE` 命中既有行后的最终行值。
///
/// 谓词只对既有行求值，不成立时返回 `None` 表示跳过；赋值在既有行与 `EXCLUDED` 行合并后的
/// 作用域里求值（列带 `ROW_ID_STRIDE` 偏移），再统一应用，等价于 PostgreSQL 的「同时赋值」语义。
/// 单行 `upsert_row` 与批量 `execute_multi_insert_atomic` 共用本函数，冲突语义只有一份。
fn conflict_updated_row(
    existing: &RowFieldValues,
    plan: &WritePlan,
    assignments: &[ConflictAssignment],
    predicate: Option<&SqlCatalogBoundExpression>,
    dialect: SqlDialect,
) -> Result<Option<RowFieldValues>, SqlFrontendError> {
    if let Some(predicate) = predicate {
        let value = eval_expression(predicate, existing, &plan.field_types, dialect)?;
        if !truthy(&value) {
            return Ok(None);
        }
    }
    let mut scope = existing.clone();
    let mut scope_types = plan.field_types.clone();
    for field in &plan.fields {
        let excluded_row_id = field.field_id + ROW_ID_STRIDE;
        if let Some(bytes) = sql_value_to_bytes(&field.value) {
            scope.insert(excluded_row_id, bytes);
        }
        scope_types.insert(excluded_row_id, field.field_type.clone());
    }
    let mut updates = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let value = eval_expression(&assignment.value, &scope, &scope_types, dialect)?;
        let literal = value_to_literal(value);
        check_write_literal(&literal, &assignment.field_type, &assignment.field_name, dialect, assignment.capacity)?;
        updates.push((assignment.field_id, sql_value_to_bytes(&literal)));
    }
    let mut row = existing.clone();
    for (field_id, bytes) in updates {
        match bytes {
            Some(bytes) => row.insert(field_id, bytes),
            None => row.remove(field_id),
        }
    }
    Ok(Some(row))
}

/// 执行 `INSERT ... SELECT`：源查询走既有读路径，全部行通过宽度、类型与主键唯一性
/// 校验后才开始写入，保证语句级原子性。
fn execute_insert_select(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &InsertSelectPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let source = execute_plan(executor, transaction, &plan.source, dialect)?;
    let mut write_plans = Vec::with_capacity(source.rows.len());
    for row in source.rows {
        if row.len() != plan.fields.len() {
            return Err(SqlFrontendError::syntax(format!(
                "INSERT ... SELECT produced {} values for {} target columns",
                row.len(),
                plan.fields.len()
            )));
        }
        let literals: Vec<SqlBoundLiteral> = row.into_iter().map(value_to_literal).collect();
        let rowid = pk_literal_to_rowid(&literals[0])?;
        let mut fields = Vec::with_capacity(plan.fields.len());
        for (column, value) in plan.fields.iter().zip(literals) {
            check_write_literal(&value, &column.field_type, &column.field_name, dialect, column.capacity)?;
            fields.push(WriteField {
                field_id: column.field_id,
                value,
                field_type: column.field_type.clone(),
                capacity: column.capacity,
            });
        }
        write_plans.push(WritePlan {
            namespace: plan.namespace.clone(),
            rowid,
            fields,
            field_types: plan.field_types.clone(),
            conflict: plan.conflict.clone(),
            returning: plan.returning.clone(),
            unique_constraints: plan.unique_constraints.clone(),
        });
    }
    // 没有冲突策略时，源行之间与目标表内都不能出现重复主键，预检保证整条语句不发布。
    // 有策略时逐行按策略处理冲突，预检会让 `DO NOTHING` / `DO UPDATE` 失效。
    if plan.conflict.is_none() {
        ensure_insert_targets_absent(executor, transaction, &write_plans)?;
    }
    // 次要 `UNIQUE` 约束与主键一样在写入前整体预检，命中即整条语句拒绝。
    ensure_unique_targets_absent(executor, transaction, &write_plans)?;
    // 源查询没有产出行时写语句仍有结果形状（`RETURNING` 列定义，0 行）。
    if write_plans.is_empty() {
        let columns = plan.returning.as_ref().map(returning_column_labels).unwrap_or_default();
        return Ok(SqlResult { columns, rows: Vec::new(), affected_rows: 0 });
    }
    execute_multi_insert(executor, transaction, &write_plans, dialect)
}

/// 写入前确认源行之间与目标表内都不存在重复主键，让唯一约束冲突整条语句不发布。
fn ensure_insert_targets_absent(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plans: &[WritePlan],
) -> Result<(), SqlFrontendError> {
    let Some(first) = plans.first()
    else {
        return Ok(());
    };
    let conflict = |plan: &WritePlan| {
        SqlFrontendError::new(
            SqlErrorKind::DuplicateKey,
            format!("duplicate key value violates unique constraint: {}", plan.namespace),
        )
    };
    let namespace = Namespace(first.namespace.clone());
    let mut seen = BTreeSet::new();
    let mut commands = Vec::with_capacity(plans.len());
    for plan in plans {
        if !seen.insert(plan.rowid.clone()) {
            return Err(conflict(plan));
        }
        commands.push(KeyValueCommand { namespace: namespace.clone(), key: plan.rowid.clone(), action: KeyValueAction::Get });
    }
    let results = match transaction {
        Some(handle) => handle
            .execute_read_batch(&commands)
            .map_err(|e| internal_error(&format!("TransactionHandle.read_batch failed: {e}")))?,
        None => executor
            .execute_read_batch(None, &commands)
            .map_err(|e| internal_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?,
    };
    if results.iter().any(|result| matches!(result, yyds_types::KeyValueResult::Get(Some(_)))) {
        return Err(conflict(first));
    }
    Ok(())
}

/// 写入前确认候选行之间与目标表内都不存在 `UNIQUE` 冲突。
fn ensure_unique_targets_absent(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plans: &[WritePlan],
) -> Result<(), SqlFrontendError> {
    let Some(first) = plans.first()
    else {
        return Ok(());
    };
    if first.unique_constraints.is_empty() {
        return Ok(());
    }
    let namespace = Namespace(first.namespace.clone());
    let stored = scan_rows(executor, transaction, &namespace, &[], None, None, 0, "unique_check")?;
    let mut pending: Vec<(Vec<u8>, RowFieldValues)> = Vec::new();
    for plan in plans {
        let row = write_plan_to_row(plan);
        let pending_without_self: Vec<(Vec<u8>, RowFieldValues)> =
            pending.iter().filter(|(rowid, _)| rowid != &plan.rowid).cloned().collect();
        if let Some(constraint) =
            unique_conflict_in_snapshot(&first.unique_constraints, &row, Some(&plan.rowid), &pending_without_self, &stored)?
        {
            return Err(duplicate_key_error(&constraint));
        }
        pending.push((plan.rowid.clone(), row));
    }
    Ok(())
}

fn execute_update(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &UpdatePlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    let get_cmd = KeyValueCommand { namespace: namespace.clone(), key: plan.rowid.clone(), action: KeyValueAction::Get };
    let batch_result = match transaction {
        Some(handle) => handle
            .execute_read_batch(&[get_cmd.clone()])
            .map_err(|e| internal_error(&format!("TransactionHandle.read_batch failed: {e}")))?,
        None => executor
            .execute_read_batch(None, &[get_cmd])
            .map_err(|e| internal_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?,
    };
    let existing_bytes = match batch_result.into_iter().next() {
        Some(yyds_types::KeyValueResult::Get(Some(b))) => b,
        _ => return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 }),
    };
    // UPDATE 必须保留未赋值的列，因此这里解码完整 TLV；字段值仍按
    // `plan.field_types` 走类型化解释，不会退回无类型猜测。
    let mut row = decode_row(&existing_bytes)?;

    // 先评估 remaining_predicate —— 不通过就不写，影响 0 行。
    if let Some(ref predicate) = plan.remaining_predicate {
        let v = eval_expression(predicate, &row, &plan.field_types, dialect)?;
        if !truthy(&v) {
            return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 });
        }
    }

    for field in &plan.fields {
        match sql_value_to_bytes(&field.value) {
            Some(bytes) => row.insert(field.field_id, bytes),
            None => row.remove(field.field_id),
        }
    }
    if let Some(constraint) =
        unique_conflict_on_write(executor, transaction, &plan.namespace, &plan.unique_constraints, &row, Some(&plan.rowid))?
    {
        return Err(duplicate_key_error(&constraint));
    }
    let encoded = encode_row(&row);
    let put_cmd = KeyValueCommand { namespace, key: plan.rowid.clone(), action: KeyValueAction::Put(encoded) };
    let put_result = match transaction {
        // 事务内 UPDATE 是 read-modify-write：把读到的 pre-image 交给 OCC，
        // 另一个事务在此期间提交同一行时 COMMIT 会被拒绝，而不是静默覆盖。
        Some(handle) => handle
            .write_if_unchanged(put_cmd, Some(existing_bytes))
            .map_err(|e| write_error(format!("TransactionHandle.update failed: {e}"), &e))?,
        // 单行 UPDATE 的自动提交只有一条命令，单命令即单提交点，无需再包一层批次。
        None => executor.execute(None, put_cmd).map_err(|e| internal_error(&format!("KeyValueExecutor.update failed: {e}")))?,
    };
    let _ = put_result;
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 1 })
}

fn execute_delete(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &DeletePlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());

    // 事务内 DELETE 也要捕获 pre-image 供 OCC 校验；有 remaining_predicate 时还必须
    // 先 Get 拿到行评估，不通过就不删。
    let pre_image = if transaction.is_some() || plan.remaining_predicate.is_some() {
        let get_cmd = KeyValueCommand { namespace: namespace.clone(), key: plan.rowid.clone(), action: KeyValueAction::Get };
        let batch_result = match transaction {
            Some(handle) => handle
                .execute_read_batch(&[get_cmd.clone()])
                .map_err(|e| internal_error(&format!("TransactionHandle.read_batch failed: {e}")))?,
            None => executor
                .execute_read_batch(None, &[get_cmd])
                .map_err(|e| internal_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?,
        };
        let existing_bytes = match batch_result.into_iter().next() {
            Some(yyds_types::KeyValueResult::Get(Some(b))) => b,
            // 行已不存在 — 也是 0 影响行
            _ => return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 }),
        };
        if let Some(ref predicate) = plan.remaining_predicate {
            let needed_ids = expression_needed_field_ids(&[], &[], Some(predicate));
            let row = decode_row_fields(&existing_bytes, &needed_ids)?;
            let v = eval_expression(predicate, &row, &plan.field_types, dialect)?;
            if !truthy(&v) {
                return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 });
            }
        }
        Some(existing_bytes)
    }
    else {
        None
    };

    let action = KeyValueAction::Delete;
    let command = KeyValueCommand { namespace, key: plan.rowid.clone(), action };
    let delete_result = match transaction {
        // 事务内 DELETE 同样交给 OCC：pre-image 已被并发事务改写时 COMMIT 被拒绝。
        Some(handle) => handle
            .write_if_unchanged(command, pre_image)
            .map_err(|e| write_error(format!("TransactionHandle.delete failed: {e}"), &e))?,
        // 单行 DELETE 的自动提交同样是一条命令、一个 durable 提交点。
        None => executor.execute(None, command).map_err(|e| internal_error(&format!("KeyValueExecutor.delete failed: {e}")))?,
    };
    let affected = match delete_result {
        yyds_types::KeyValueResult::Delete { removed: true } => 1,
        _ => 0,
    };
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: affected })
}

/// 一条语句扫描加谓词求值后命中的一行：最终写入命令与扫描时看到的 pre-image。
///
/// `pre_image` 会作为 `CommitCondition::ValueEquals(Some(pre_image))` 交给提交边界复核，
/// 因此读改写与删除共享同一套 OCC 语义，不随语句动词分叉。
struct StatementChange {
    /// 该行最终要提交的命令（`Put` 或 `Delete`）。
    command: KeyValueCommand,
    /// 扫描时读到的基存行值，提交时用作 OCC 条件。
    pre_image: Vec<u8>,
}

/// 把一条 DML 语句的命中行提交到 durable 原子边界。
///
/// 无显式事务时，整条语句必须经一次 `KeyValueExecutor::commit_batch(None, batch)` 发布：
/// 逐行 `execute` 会把同一条语句拆成多个 durable 提交点，中途存储失败或进程退出会留下
/// 前几行已可见、后几行未写的半提交状态。每行携带
/// `CommitCondition::ValueEquals(Some(pre_image))`，并发改写同一行时整批以 `CasConflict`
/// 拒绝并上报 `SqlErrorKind::Conflict`，不静默重试，也不退回逐行写。
///
/// 有显式事务时各行进入 handle 缓冲，由 COMMIT 时同一条 `commit_batch` 边界发布，
/// 这里不提前提交。空变更集不触碰提交边界，直接返回 0。
fn apply_statement_changes(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    changes: Vec<StatementChange>,
    namespace: &Namespace,
    operation: &str,
) -> Result<u64, SqlFrontendError> {
    if changes.is_empty() {
        return Ok(0);
    }
    let affected = changes.len() as u64;
    match transaction {
        Some(handle) => {
            for change in changes {
                handle
                    .write_if_unchanged(change.command, Some(change.pre_image))
                    .map_err(|e| write_error(format!("TransactionHandle.{operation} failed: {e}"), &e))?;
            }
        }
        None => {
            let batch: Vec<BufferedCommit> = changes
                .into_iter()
                .map(|change| BufferedCommit {
                    command: change.command,
                    condition: Some(CommitCondition::ValueEquals(Some(change.pre_image))),
                })
                .collect();
            executor.commit_batch(None, batch).map_err(|error| match error {
                yyds_types::Error::CasConflict { .. } => SqlFrontendError::new(
                    SqlErrorKind::Conflict,
                    format!("concurrent writes to {} prevented the {operation} batch commit", namespace.0),
                ),
                other => internal_error(&format!("KeyValueExecutor.commit_batch failed: {other}")),
            })?;
        }
    }
    Ok(affected)
}

/// 多行 UPDATE：扫描 rowid 区间，逐行判定 WHERE 后改写，返回真实影响行数。
///
/// 命中行先累积成一条语句变更集，再统一走 [`apply_statement_changes`] 的提交边界；
/// 自动提交下只产生一次 durable commit，事务内则交给 handle 缓冲到 COMMIT。
fn execute_update_scan(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &UpdateScanPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    let scanned = scan_rows(
        executor,
        transaction,
        &namespace,
        &plan.rowid_prefix,
        plan.rowid_after.as_deref(),
        plan.rowid_before.as_deref(),
        0,
        "update_scan",
    )?;
    let mut changes = Vec::with_capacity(scanned.len());
    let mut pending: Vec<(Vec<u8>, RowFieldValues)> = Vec::new();
    for (rowid, pre_image) in scanned {
        let mut row = decode_row(&pre_image)?;
        if let Some(predicate) = &plan.predicate {
            let value = eval_expression(predicate, &row, &plan.field_types, dialect)?;
            if !truthy(&value) {
                continue;
            }
        }
        for field in &plan.fields {
            match sql_value_to_bytes(&field.value) {
                Some(bytes) => row.insert(field.field_id, bytes),
                None => row.remove(field.field_id),
            }
        }
        if !plan.unique_constraints.is_empty() {
            // 旧快照仍含本行自身与同语句已处理的行，两者都要从冲突候选里排除。
            let stored = scan_rows(executor, transaction, &namespace, &[], None, None, 0, "unique_check")?;
            let pending_without_self: Vec<(Vec<u8>, RowFieldValues)> =
                pending.iter().filter(|(pending_rowid, _)| pending_rowid != &rowid).cloned().collect();
            if let Some(constraint) =
                unique_conflict_in_snapshot(&plan.unique_constraints, &row, Some(&rowid), &pending_without_self, &stored)?
            {
                return Err(duplicate_key_error(&constraint));
            }
            pending.push((rowid.clone(), row.clone()));
        }
        changes.push(StatementChange {
            command: KeyValueCommand {
                namespace: namespace.clone(),
                key: rowid,
                action: KeyValueAction::Put(encode_row(&row)),
            },
            pre_image,
        });
    }
    let affected = apply_statement_changes(executor, transaction, changes, &namespace, "update_scan")?;
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: affected })
}

/// 多行 DELETE：扫描 rowid 区间，逐行判定 WHERE 后删除，返回真实影响行数。
///
/// 与多行 UPDATE 共用同一条语句变更集与提交边界，删除命中行在一次 durable commit 内发布。
fn execute_delete_scan(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &DeleteScanPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    let scanned = scan_rows(
        executor,
        transaction,
        &namespace,
        &plan.rowid_prefix,
        plan.rowid_after.as_deref(),
        plan.rowid_before.as_deref(),
        0,
        "delete_scan",
    )?;
    let mut changes = Vec::with_capacity(scanned.len());
    for (rowid, pre_image) in scanned {
        if let Some(predicate) = &plan.predicate {
            let row = decode_row(&pre_image)?;
            let value = eval_expression(predicate, &row, &plan.field_types, dialect)?;
            if !truthy(&value) {
                continue;
            }
        }
        changes.push(StatementChange {
            command: KeyValueCommand { namespace: namespace.clone(), key: rowid, action: KeyValueAction::Delete },
            pre_image,
        });
    }
    let affected = apply_statement_changes(executor, transaction, changes, &namespace, "delete_scan")?;
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: affected })
}

fn write_plan_to_row(plan: &WritePlan) -> RowFieldValues {
    let mut row = RowFieldValues::new();
    for field in &plan.fields {
        // NULL 不写入 TLV：字段缺失就是 NULL，空 bytes 留给真正的空串。
        if let Some(bytes) = sql_value_to_bytes(&field.value) {
            row.insert(field.field_id, bytes);
        }
    }
    row
}

/// 把一行的约束列折叠成可比较的元组；任一列为 NULL 时返回 `None`（SQL 的
/// `UNIQUE` 允许多行 NULL）。
fn constraint_tuple(row: &RowFieldValues, columns: &[(u64, String)]) -> Option<Vec<Vec<u8>>> {
    let mut tuple = Vec::with_capacity(columns.len());
    for (field_id, _) in columns {
        tuple.push(row.get(*field_id)?.to_vec());
    }
    Some(tuple)
}

/// 候选行与既有行构成的 `UNIQUE` 冲突描述。
fn unique_conflict_between(
    constraints: &[UniqueConstraintPlan],
    candidate: &RowFieldValues,
    existing: &RowFieldValues,
) -> Option<String> {
    constraints.iter().find_map(|constraint| {
        let left = constraint_tuple(candidate, &constraint.columns)?;
        let right = constraint_tuple(existing, &constraint.columns)?;
        (left == right).then(|| {
            let names: Vec<&str> = constraint.columns.iter().map(|(_, name)| name.as_str()).collect();
            format!("UNIQUE ({})", names.join(", "))
        })
    })
}

/// 在基存快照与同语句已产出的行中查找 `UNIQUE` 冲突。
///
/// `excluded_rowid` 排除当前行自身（UPDATE 时它仍然存在于旧快照里）。
/// `pending` 是同一条语句里已经通过校验的最终行，用来发现语句内重复。
fn unique_conflict_in_snapshot(
    constraints: &[UniqueConstraintPlan],
    candidate: &RowFieldValues,
    excluded_rowid: Option<&[u8]>,
    pending: &[(Vec<u8>, RowFieldValues)],
    stored: &[(Vec<u8>, Vec<u8>)],
) -> Result<Option<String>, SqlFrontendError> {
    for (rowid, row) in pending {
        if excluded_rowid.is_some_and(|excluded| excluded == rowid.as_slice()) {
            continue;
        }
        if let Some(conflict) = unique_conflict_between(constraints, candidate, row) {
            return Ok(Some(conflict));
        }
    }
    for (rowid, bytes) in stored {
        if excluded_rowid.is_some_and(|excluded| excluded == rowid.as_slice()) {
            continue;
        }
        let row = decode_row(bytes)?;
        if let Some(conflict) = unique_conflict_between(constraints, candidate, &row) {
            return Ok(Some(conflict));
        }
    }
    Ok(None)
}

/// 扫描整张表后校验候选行，供单行 INSERT / UPDATE 使用。
fn unique_conflict_on_write(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    namespace: &str,
    constraints: &[UniqueConstraintPlan],
    candidate: &RowFieldValues,
    excluded_rowid: Option<&[u8]>,
) -> Result<Option<String>, SqlFrontendError> {
    if constraints.is_empty() {
        return Ok(None);
    }
    let namespace = Namespace(namespace.to_string());
    let stored = scan_rows(executor, transaction, &namespace, &[], None, None, 0, "unique_check")?;
    unique_conflict_in_snapshot(constraints, candidate, excluded_rowid, &[], &stored)
}

/// `UNIQUE` 冲突统一按 `DuplicateKey` 上报，由方言层映射成 `23505` / `1062`。
fn duplicate_key_error(constraint: &str) -> SqlFrontendError {
    SqlFrontendError::new(SqlErrorKind::DuplicateKey, format!("duplicate key value violates unique constraint {constraint}"))
}

fn sql_value_to_bytes(lit: &SqlBoundLiteral) -> Option<Vec<u8>> {
    match lit {
        SqlBoundLiteral::Integer(value) => Some(value.to_string().into_bytes()),
        SqlBoundLiteral::Number(text) => Some(text.as_bytes().to_vec()),
        SqlBoundLiteral::String(text) => Some(text.as_bytes().to_vec()),
        SqlBoundLiteral::Bytes(bytes) => Some(bytes.clone()),
        SqlBoundLiteral::Boolean(v) => Some((if *v { "1" } else { "0" }).as_bytes().to_vec()),
        SqlBoundLiteral::Null => None,
    }
}

fn read_column_labels(plan: &ReadPlan) -> Vec<String> {
    let mut labels = Vec::new();
    for (_, name) in &plan.projection_fields {
        labels.push(name.clone());
    }
    for (_, name) in &plan.projection_expressions {
        labels.push(name.clone());
    }
    labels
}

fn scan_column_labels(plan: &ScanPlan) -> Vec<String> {
    let mut labels = Vec::new();
    for (_, name) in &plan.projection_fields {
        labels.push(name.clone());
    }
    for (_, name) in &plan.projection_expressions {
        labels.push(name.clone());
    }
    labels
}

fn join_column_labels(plan: &JoinPlan) -> Vec<String> {
    let mut labels = Vec::new();
    for (_, name) in &plan.projection_fields {
        labels.push(name.clone());
    }
    for (_, name) in &plan.projection_expressions {
        labels.push(name.clone());
    }
    labels
}

fn truthy(v: &SqlValue) -> bool {
    match v {
        SqlValue::Boolean(b) => *b,
        SqlValue::Integer(n) => *n != 0,
        SqlValue::Number(n) => *n != 0.0,
        SqlValue::String(s) => !s.is_empty(),
        SqlValue::Bytes(bytes) => !bytes.is_empty(),
        SqlValue::Null => false,
    }
}

/// SQL 三值逻辑里的真值：`NULL` 既不是 true 也不是 false。
fn sql_truth(v: &SqlValue) -> Option<bool> {
    match v {
        SqlValue::Null => None,
        other => Some(truthy(other)),
    }
}

fn sql_and(left: &SqlValue, right: &SqlValue) -> SqlValue {
    match (sql_truth(left), sql_truth(right)) {
        (Some(false), _) | (_, Some(false)) => SqlValue::Boolean(false),
        (Some(true), Some(true)) => SqlValue::Boolean(true),
        _ => SqlValue::Null,
    }
}

fn sql_or(left: &SqlValue, right: &SqlValue) -> SqlValue {
    match (sql_truth(left), sql_truth(right)) {
        (Some(true), _) | (_, Some(true)) => SqlValue::Boolean(true),
        (Some(false), Some(false)) => SqlValue::Boolean(false),
        _ => SqlValue::Null,
    }
}

/// 求值一个窗口函数投影：按 `PARTITION BY` 分桶，桶内按窗口 `ORDER BY` 排序，
/// 再为每一行算出排名值或聚合窗口值。返回值按输入行下标对齐。
fn eval_window_projection(
    expr: &SqlCatalogBoundExpression,
    rows: &[RowFieldValues],
    field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    nulls_first: bool,
    dialect: SqlDialect,
) -> Result<Vec<SqlValue>, SqlFrontendError> {
    let (name, args, spec) = match expr {
        SqlCatalogBoundExpression::Function { name, args, window: Some(spec), .. } => (name, args, spec),
        _ => return Err(internal_error(&format!("window evaluation requires a window function call: {expr:?}"))),
    };
    let ranking = SqlWindowFunction::from_name(name);
    let aggregate = SqlAggregateFunction::from_name(name);
    if ranking.is_none() && aggregate.is_none() {
        return Err(internal_error(&format!("unknown window function: {name}")));
    }
    // binder 已固定 `NTILE` / `LAG` / `LEAD` 的参数形状，这里按同一形状防御性提取，
    // 供分区循环复用；形状不符说明绑定链被绕过，显式报错而不是猜测。
    let ntile_buckets = match ranking {
        Some(SqlWindowFunction::Ntile) => match args.first() {
            Some(SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Integer(buckets))) if *buckets >= 1 => Some(*buckets),
            _ => return Err(internal_error("window function NTILE() requires the bucket count to be an integer literal >= 1")),
        },
        _ => None,
    };
    let lag_lead = match ranking {
        Some(SqlWindowFunction::Lag) | Some(SqlWindowFunction::Lead) => {
            let value = args
                .first()
                .ok_or_else(|| internal_error(&format!("window function {name}() is missing its value argument")))?;
            let offset = match args.get(1) {
                Some(SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Integer(offset))) if *offset >= 0 => *offset,
                Some(_) => {
                    return Err(internal_error(&format!(
                        "window function {name}() requires the row offset to be an integer literal >= 0"
                    )))
                }
                None => 1,
            };
            Some((value, offset, args.get(2)))
        }
        _ => None,
    };

    // 分区 key 复用 GROUP BY 的稳定序列化：两个 NULL 落在同一分区，而 `1` 与 `'1'`
    // 不会混在一起。
    let mut partitions: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        let key_values = spec
            .partition_by
            .iter()
            .map(|partition| eval_expression(partition, row, field_types, dialect))
            .collect::<Result<Vec<_>, _>>()?;
        partitions.entry(serialize_group_key(&key_values)).or_default().push(index);
    }

    let mut values = vec![SqlValue::Null; rows.len()];
    for indices in partitions.into_values() {
        // 窗口 ORDER BY 的排序键：NULL 位置沿用方言默认，与普通 ORDER BY 同一套规则。
        let mut keyed: Vec<(Vec<Option<SqlValue>>, usize)> = Vec::with_capacity(indices.len());
        for index in indices {
            let keys = spec
                .order_by
                .iter()
                .map(|item| Ok(Some(eval_expression(&item.expression, &rows[index], field_types, dialect)?)))
                .collect::<Result<Vec<_>, SqlFrontendError>>()?;
            keyed.push((keys, index));
        }
        let orderings = spec
            .order_by
            .iter()
            .map(|item| (item.direction.clone(), order_nulls_first(&item.direction, item.nulls, nulls_first)))
            .collect::<Vec<_>>();
        keyed.sort_by(|a, b| {
            for (position, (direction, nulls_first)) in orderings.iter().enumerate() {
                let ordering = compare_values_with_nulls(a.0[position].as_ref(), b.0[position].as_ref(), *nulls_first);
                if ordering != std::cmp::Ordering::Equal {
                    return match direction {
                        SqlBoundOrderDirection::Asc => ordering,
                        SqlBoundOrderDirection::Desc => ordering.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });

        // peer 组：排序键完全相同的行共享排名，也决定默认 frame 的上界。
        let mut peer_start = vec![0usize; keyed.len()];
        let mut peer_end = vec![0usize; keyed.len()];
        let mut dense_rank = vec![0i64; keyed.len()];
        let mut start = 0usize;
        let mut group = 0i64;
        while start < keyed.len() {
            let mut end = start + 1;
            while end < keyed.len() && peer_keys_equal(&keyed[start].0, &keyed[end].0) {
                end += 1;
            }
            group += 1;
            for position in start..end {
                peer_start[position] = start;
                peer_end[position] = end;
                dense_rank[position] = group;
            }
            start = end;
        }

        let ordered_rows: Vec<RowFieldValues> = keyed.iter().map(|(_, index)| rows[*index].clone()).collect();
        // RANGE 帧的数值偏移按单列 ORDER BY 的值比较；`DESC` 取负把值轴统一到升序，
        // 只含 `UNBOUNDED` / `CURRENT ROW` 的 RANGE 帧与 ROWS 共用 peer 边界，不需要值轴。
        let range_axis = match spec.frame.as_ref() {
            Some(frame) if frame.unit == SqlBoundWindowFrameUnit::Range && frame.has_value_offset() => {
                let descending =
                    matches!(spec.order_by.first().map(|item| &item.direction), Some(SqlBoundOrderDirection::Desc));
                Some(RangeAxis::build(&keyed, descending, spec.order_by.len())?)
            }
            _ => None,
        };
        for (position, (_, index)) in keyed.iter().enumerate() {
            let value = match ranking {
                Some(SqlWindowFunction::RowNumber) => SqlValue::Integer(i64::try_from(position + 1).unwrap_or(i64::MAX)),
                Some(SqlWindowFunction::Rank) => SqlValue::Integer(i64::try_from(peer_start[position] + 1).unwrap_or(i64::MAX)),
                Some(SqlWindowFunction::DenseRank) => SqlValue::Integer(dense_rank[position]),
                Some(SqlWindowFunction::Ntile) => {
                    // 标准公式 `floor(position * buckets / len) + 1`（position 从 0 起），
                    // 余数分给前面的桶，与 MySQL / PostgreSQL 一致；i128 中间值防溢出。
                    let buckets =
                        ntile_buckets.ok_or_else(|| internal_error("window function NTILE() is missing its bucket count"))?;
                    let bucket = position as i128 * buckets as i128 / ordered_rows.len() as i128 + 1;
                    SqlValue::Integer(i64::try_from(bucket).unwrap_or(i64::MAX))
                }
                Some(SqlWindowFunction::Lag) | Some(SqlWindowFunction::Lead) => {
                    let (value_expr, offset, default) =
                        lag_lead.ok_or_else(|| internal_error(&format!("window function {name}() is missing its arguments")))?;
                    let target = if matches!(ranking, Some(SqlWindowFunction::Lag)) {
                        position as i128 - offset as i128
                    }
                    else {
                        position as i128 + offset as i128
                    };
                    match usize::try_from(target).ok().filter(|target| *target < ordered_rows.len()) {
                        Some(target) => eval_expression(value_expr, &ordered_rows[target], field_types, dialect)?,
                        // 越出分区：有默认值就按当前行求值，否则 SQL NULL。
                        None => match default {
                            Some(default) => eval_expression(default, &rows[*index], field_types, dialect)?,
                            None => SqlValue::Null,
                        },
                    }
                }
                None => {
                    let (frame_start, frame_end) = window_frame_bounds(
                        spec,
                        position,
                        peer_start[position],
                        peer_end[position],
                        ordered_rows.len(),
                        range_axis.as_ref(),
                    )?;
                    let frame =
                        if frame_start < frame_end { &ordered_rows[frame_start..frame_end] } else { &ordered_rows[0..0] };
                    eval_aggregate(expr, frame, field_types, dialect)?
                }
            };
            values[*index] = value;
        }
    }
    Ok(values)
}

/// 计算当前行上的窗口 frame 在排序后分区内的 `[start, end)` 区间。
///
/// 没有显式 frame 时按 SQL 默认：有 `ORDER BY` 的聚合窗口累计到当前行的 peer 组末尾
/// （`RANGE UNBOUNDED PRECEDING TO CURRENT ROW`），没有 `ORDER BY` 时覆盖整个分区。
/// `ROWS` 按物理行位置换算；`RANGE` 按单列 `ORDER BY` 的值距离换算，值轴由调用方
/// 预构建后传入。
fn window_frame_bounds(
    spec: &SqlCatalogBoundWindowSpec,
    position: usize,
    peer_start: usize,
    peer_end: usize,
    len: usize,
    range_axis: Option<&RangeAxis>,
) -> Result<(usize, usize), SqlFrontendError> {
    let Some(frame) = spec.frame.as_ref()
    else {
        return Ok(if spec.order_by.is_empty() { (0, len) } else { (0, peer_end) });
    };
    if frame.unit == SqlBoundWindowFrameUnit::Range {
        return range_frame_bounds(frame, range_axis, position, peer_start, peer_end, len);
    }
    let limit = i64::try_from(len).unwrap_or(i64::MAX);
    let start = frame_bound_position(frame.start, position).clamp(0, limit) as usize;
    let end_exclusive = match frame.end {
        Some(bound) => frame_bound_position(bound, position).clamp(-1, limit).saturating_add(1),
        None => frame_bound_position(SqlBoundWindowFrameBound::CurrentRow, position).saturating_add(1),
    };
    Ok((start, end_exclusive.clamp(0, limit) as usize))
}

/// `RANGE` 帧的边界换算：`CURRENT ROW` 落在当前 peer 组边界，`UNBOUNDED` 落在分区
/// 两端，数值偏移在升序值轴上二分。`NULL` 排序键没有值距离：当前行为 `NULL` 时偏移
/// bound 折回当前 peer 组，非 `NULL` 行的偏移帧只落在自己的非 `NULL` 连续段上，
/// 其他 `NULL` 行不进入带偏移的帧。反向的畸形帧与 `ROWS` 同样退化为空区间，不报错。
fn range_frame_bounds(
    frame: &SqlBoundWindowFrame,
    range_axis: Option<&RangeAxis>,
    position: usize,
    peer_start: usize,
    peer_end: usize,
    len: usize,
) -> Result<(usize, usize), SqlFrontendError> {
    let missing_axis = || internal_error("window RANGE frame is missing its ORDER BY value axis");
    let resolve = |bound: SqlBoundWindowFrameBound, end: bool| -> Result<usize, SqlFrontendError> {
        Ok(match bound {
            SqlBoundWindowFrameBound::UnboundedPreceding => 0,
            SqlBoundWindowFrameBound::UnboundedFollowing => len,
            SqlBoundWindowFrameBound::CurrentRow => {
                if end {
                    peer_end
                }
                else {
                    peer_start
                }
            }
            SqlBoundWindowFrameBound::Preceding(offset) => {
                let axis = range_axis.ok_or_else(missing_axis)?;
                if axis.is_null(position) {
                    if end {
                        peer_end
                    }
                    else {
                        peer_start
                    }
                }
                else {
                    axis.offset_bound(position, offset, false, end)
                }
            }
            SqlBoundWindowFrameBound::Following(offset) => {
                let axis = range_axis.ok_or_else(missing_axis)?;
                if axis.is_null(position) {
                    if end {
                        peer_end
                    }
                    else {
                        peer_start
                    }
                }
                else {
                    axis.offset_bound(position, offset, true, end)
                }
            }
        })
    };
    let start = resolve(frame.start, false)?;
    let end = resolve(frame.end.unwrap_or(SqlBoundWindowFrameBound::CurrentRow), true)?;
    Ok((start, end))
}

/// `RANGE` 帧数值偏移的比较轴：分区内单列 `ORDER BY` 的值按排序顺序排列，`DESC`
/// 取负后统一成升序。整数列用 `i128` 精确轴，含浮点值时用 `f64` 轴。`NULL` 保留为
/// `None`：它没有值距离，但在偏移比较之外仍是一个正常排序值。
enum RangeAxis {
    Integers(Vec<Option<i128>>),
    Numbers(Vec<Option<f64>>),
}

impl RangeAxis {
    /// 从分区排序键构建值轴。带偏移的 `RANGE` 帧按值比较，值距离只有单列
    /// `ORDER BY` 才有唯一依据；`NULL` 键不产生距离，但仍可参与排序，因此保留
    /// `None` 而不是拒绝整个分区。
    fn build(
        keyed: &[(Vec<Option<SqlValue>>, usize)],
        descending: bool,
        order_columns: usize,
    ) -> Result<Self, SqlFrontendError> {
        if order_columns != 1 {
            return Err(SqlFrontendError::unsupported(
                "window RANGE frame with an offset requires exactly one ORDER BY column",
            ));
        }
        let mut integers = Vec::with_capacity(keyed.len());
        let mut numbers = Vec::with_capacity(keyed.len());
        let mut saw_number = false;
        for (keys, _) in keyed {
            match keys.first().and_then(|value| value.as_ref()) {
                Some(SqlValue::Integer(value)) => {
                    let value = i128::from(*value);
                    let value = if descending { -value } else { value };
                    integers.push(Some(value));
                    numbers.push(Some(value as f64));
                }
                Some(SqlValue::Number(value)) => {
                    saw_number = true;
                    numbers.push(Some(if descending { -*value } else { *value }));
                }
                Some(SqlValue::Null) | None => {
                    integers.push(None);
                    numbers.push(None);
                }
                Some(_) => return Err(internal_error("window RANGE frame offsets require the ORDER BY column to be numeric")),
            }
        }
        if saw_number {
            Ok(Self::Numbers(numbers))
        }
        else {
            Ok(Self::Integers(integers))
        }
    }

    /// 某一行的 `ORDER BY` 排序键是否为 `NULL`。
    fn is_null(&self, position: usize) -> bool {
        match self {
            Self::Integers(values) => values[position].is_none(),
            Self::Numbers(values) => values[position].is_none(),
        }
    }

    /// 当前行所在的那一段非 `NULL` 排序键的 `[start, end)`。偏移比较只在同一段
    /// 内有意义：`NULL` 段可能按方言排在前缀或后缀，但始终与数值轴分离。
    fn numeric_run(&self, position: usize) -> (usize, usize) {
        let len = match self {
            Self::Integers(values) => values.len(),
            Self::Numbers(values) => values.len(),
        };
        let mut start = position;
        while start > 0 && !self.is_null(start - 1) {
            start -= 1;
        }
        let mut end = position + 1;
        while end < len && !self.is_null(end) {
            end += 1;
        }
        (start, end)
    }

    /// 把 `n PRECEDING` / `n FOLLOWING` 换算成帧边界：起点 bound 返回第一段内第一个
    /// 值不小于目标的行下标，终点 bound 返回最后一个值不大于目标的行的下一位
    /// （开区间上界）。目标越出整段时该 bound 不满足任何行：起点落在分区尾，
    /// 终点落在分区头，两者都退化成空帧。
    fn offset_bound(&self, position: usize, offset: u64, following: bool, end: bool) -> usize {
        let (run_start, run_end) = self.numeric_run(position);
        match self {
            Self::Integers(values) => {
                let step = i128::try_from(offset).unwrap_or(i128::MAX);
                let current = values[position].expect("offset bounds start from a non-NULL key");
                let target = if following { current.saturating_add(step) } else { current.saturating_sub(step) };
                offset_index(&values[run_start..run_end], run_start, values.len(), target, end, |value, target| {
                    value.cmp(&target)
                })
            }
            Self::Numbers(values) => {
                let step = offset as f64;
                let current = values[position].expect("offset bounds start from a non-NULL key");
                let target = if following { current + step } else { current - step };
                offset_index(&values[run_start..run_end], run_start, values.len(), target, end, |value, target| {
                    value.total_cmp(&target)
                })
            }
        }
    }
}

/// 在一段升序、无 `NULL` 的排序键里定位偏移目标：起点 bound 取第一个不小于目标的
/// 行，终点 bound 取第一个大于目标的行（开区间上界）。段内没有命中时起终点分别
/// 退到分区两端，让调用方得到空帧。
fn offset_index<T: Copy>(
    run: &[Option<T>],
    run_start: usize,
    len: usize,
    target: T,
    end: bool,
    compare: impl Fn(T, T) -> std::cmp::Ordering,
) -> usize {
    // 两种 bound 都按「满足前缀」二分：起点用「小于目标」，终点用「不大于目标」，
    // 命中的第一个下标就是边界。
    let count = if end {
        run.partition_point(|value| value.is_some_and(|value| compare(value, target) != std::cmp::Ordering::Greater))
    }
    else {
        run.partition_point(|value| value.is_some_and(|value| compare(value, target) == std::cmp::Ordering::Less))
    };
    if end && count == 0 {
        // 没有任何行落在目标以内：终点退到分区头。
        return 0;
    }
    if !end && count == run.len() {
        // 没有任何行达到目标：起点退到分区尾。
        return len;
    }
    run_start + count
}

/// 把一个 frame bound 换算成当前行上的绝对行位置。
fn frame_bound_position(bound: SqlBoundWindowFrameBound, position: usize) -> i64 {
    let row = i64::try_from(position).unwrap_or(i64::MAX);
    match bound {
        SqlBoundWindowFrameBound::UnboundedPreceding => 0,
        SqlBoundWindowFrameBound::UnboundedFollowing => i64::MAX,
        SqlBoundWindowFrameBound::CurrentRow => row,
        SqlBoundWindowFrameBound::Preceding(offset) => row - i64::try_from(offset).unwrap_or(i64::MAX),
        SqlBoundWindowFrameBound::Following(offset) => row.saturating_add(i64::try_from(offset).unwrap_or(i64::MAX)),
    }
}

/// 两个窗口排序键是否属于同一个 peer 组。
fn peer_keys_equal(left: &[Option<SqlValue>], right: &[Option<SqlValue>]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| compare_values_with_nulls(left.as_ref(), right.as_ref(), false) == std::cmp::Ordering::Equal)
}

/// 对一个聚合函数在所有收集到的行上求值，返回单个 SqlValue。
fn eval_aggregate(
    expr: &SqlCatalogBoundExpression,
    rows: &[RowFieldValues],
    field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    let (name, args, distinct) = match expr {
        SqlCatalogBoundExpression::Function { name, args, distinct, .. } => (name.clone(), args.clone(), *distinct),
        _ => return Err(internal_error(&format!("eval_aggregate: non-Function expression encountered: {expr:?}"))),
    };

    let agg =
        SqlAggregateFunction::from_name(&name).ok_or_else(|| internal_error(&format!("unknown aggregate function: {name}")))?;

    // `*` 只对 `COUNT(*)` 有意义；`SUM(*)` 一类没有可求值的列。
    if agg != SqlAggregateFunction::COUNT && args.iter().any(|arg| matches!(arg, SqlCatalogBoundExpression::Wildcard)) {
        return Err(internal_error(&format!("{} does not accept `*` as its argument", agg.name())));
    }
    if distinct && agg != SqlAggregateFunction::COUNT {
        return Err(internal_error(&format!("DISTINCT quantifier is only supported on COUNT for now ({})", agg.name())));
    }

    match agg {
        SqlAggregateFunction::COUNT => {
            if args.is_empty() || matches!(args.as_slice(), [SqlCatalogBoundExpression::Wildcard]) {
                // COUNT(*) — total row count after filter (DISTINCT 对 * 无意义).
                Ok(SqlValue::Integer(i64::try_from(rows.len()).unwrap_or(i64::MAX)))
            }
            else {
                if args.len() != 1 {
                    return Err(internal_error("COUNT accepts exactly one argument"));
                }
                if distinct {
                    // COUNT(DISTINCT col) — count distinct non-NULL values.
                    let mut seen: Vec<SqlValue> = Vec::new();
                    for row in rows {
                        let v = eval_expression(&args[0], row, field_types, dialect)?;
                        if !matches!(v, SqlValue::Null) && !seen.contains(&v) {
                            seen.push(v);
                        }
                    }
                    Ok(SqlValue::Integer(i64::try_from(seen.len()).unwrap_or(i64::MAX)))
                }
                else {
                    // COUNT(col) — count rows where col is not NULL.
                    let mut non_null: usize = 0;
                    for row in rows {
                        let v = eval_expression(&args[0], row, field_types, dialect)?;
                        if !matches!(v, SqlValue::Null) {
                            non_null += 1;
                        }
                    }
                    Ok(SqlValue::Integer(i64::try_from(non_null).unwrap_or(i64::MAX)))
                }
            }
        }
        SqlAggregateFunction::SUM => {
            // 整数列用 i128 精确累加，避免 `i64` 先转 `f64` 再求和造成精度丢失。
            // 出现浮点值时按 SQL 的数值提升规则整体转为 f64。
            let mut integer_sum: i128 = 0;
            let mut float_sum: f64 = 0.0;
            let mut has_float = false;
            let mut has_value = false;
            for row in rows {
                match eval_expression(&args[0], row, field_types, dialect)? {
                    SqlValue::Integer(n) => {
                        integer_sum += i128::from(n);
                        float_sum += n as f64;
                        has_value = true;
                    }
                    SqlValue::Number(n) => {
                        has_float = true;
                        float_sum += n;
                        has_value = true;
                    }
                    _ => {}
                }
            }
            if !has_value {
                Ok(SqlValue::Null)
            }
            else if has_float {
                Ok(SqlValue::Number(float_sum))
            }
            else {
                Ok(i64::try_from(integer_sum).map(SqlValue::Integer).unwrap_or(SqlValue::Number(integer_sum as f64)))
            }
        }
        SqlAggregateFunction::AVG => {
            let mut sum: f64 = 0.0;
            let mut count: usize = 0;
            for row in rows {
                let v = eval_expression(&args[0], row, field_types, dialect)?;
                match v {
                    SqlValue::Integer(n) => {
                        sum += n as f64;
                        count += 1;
                    }
                    SqlValue::Number(n) => {
                        sum += n;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count > 0 {
                Ok(SqlValue::Number(sum / count as f64))
            }
            else {
                Ok(SqlValue::Null)
            }
        }
        SqlAggregateFunction::MAX | SqlAggregateFunction::MIN => {
            let want_greater = agg == SqlAggregateFunction::MAX;
            let mut best: Option<SqlValue> = None;
            for row in rows {
                let value = eval_expression(&args[0], row, field_types, dialect)?;
                if !matches!(value, SqlValue::Integer(_) | SqlValue::Number(_)) {
                    continue;
                }
                best = Some(match best {
                    Some(current) => {
                        let order = compare_values(Some(&value), Some(&current));
                        let replace =
                            if want_greater { order == std::cmp::Ordering::Greater } else { order == std::cmp::Ordering::Less };
                        if replace {
                            value
                        }
                        else {
                            current
                        }
                    }
                    None => value,
                });
            }
            Ok(best.unwrap_or(SqlValue::Null))
        }
    }
}

/// Evaluates a clause expression that has no row context (`LIMIT` / `OFFSET`).
///
/// The planner folds these clauses before execution, and it deliberately uses
/// this same evaluator so the fold cannot disagree with runtime evaluation of
/// the same expression (arithmetic overflow, division semantics, function
/// results). The caller only accepts a non-negative integer scalar.
pub(crate) fn eval_constant(expression: &SqlCatalogBoundExpression, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
    eval_expression(expression, &RowFieldValues::new(), &std::collections::BTreeMap::new(), dialect)
}

fn eval_expression(
    expr: &SqlCatalogBoundExpression,
    row: &RowFieldValues,
    field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    Ok(match expr {
        // 缺失字段就是 SQL NULL（写入 NULL 时字段会被省略），比较和投影都按 NULL 处理。
        SqlCatalogBoundExpression::Column(col) => row.value_typed(col.row_id, field_types).unwrap_or(SqlValue::Null),
        SqlCatalogBoundExpression::Literal(lit) => SqlValue::from_literal(lit),
        SqlCatalogBoundExpression::Wildcard => {
            return Err(internal_error("wildcard `*` is only valid as the argument of COUNT"))
        }
        // 外层列应当在逐行求值时先用该行外层值替换掉，走到这里说明替换被漏掉。
        SqlCatalogBoundExpression::OuterColumn(_) => {
            return Err(internal_error("outer column reference was not substituted before evaluation"))
        }
        SqlCatalogBoundExpression::Unary { operator, expression } => {
            let inner = eval_expression(expression, row, field_types, dialect)?;
            eval_unary(*operator, inner)?
        }
        SqlCatalogBoundExpression::Binary { left, operator, right } => {
            let l = eval_expression(left, row, field_types, dialect)?;
            let r = eval_expression(right, row, field_types, dialect)?;
            eval_binary(*operator, &l, &r, dialect)?
        }
        // 窗口函数需要整列上下文，必须由窗口路径批量求值。
        SqlCatalogBoundExpression::Function { window: Some(_), .. } => {
            return Err(internal_error("window function was not evaluated by the window pass"))
        }
        SqlCatalogBoundExpression::Function { name, args, .. } => {
            let scalar = SqlScalarFunction::from_name(name)
                .ok_or_else(|| internal_error(&format!("aggregate function {name} cannot be evaluated on a single row")))?;
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                values.push(eval_expression(arg, row, field_types, dialect)?);
            }
            eval_scalar_function(scalar, &values, dialect)?
        }
        SqlCatalogBoundExpression::InValues { expression, values, negated } => {
            let target = eval_expression(expression, row, field_types, dialect)?;
            eval_in_values(target, values, *negated, dialect)?
        }
        // 子查询应当在执行前被 materialize 成 `InValues`；走到这里说明内部计划漏了 lowering。
        SqlCatalogBoundExpression::InSubquery { .. } => {
            return Err(internal_error("IN subquery was not materialized before execution"))
        }
        SqlCatalogBoundExpression::Exists { .. } => {
            return Err(internal_error("EXISTS predicate was not materialized before execution"))
        }
        SqlCatalogBoundExpression::ScalarSubquery { .. } => {
            return Err(internal_error("scalar subquery was not evaluated before execution"))
        }
        SqlCatalogBoundExpression::Case { branches, else_branch } => {
            eval_case(branches, else_branch, row, field_types, dialect)?
        }
        SqlCatalogBoundExpression::Cast { expr, target } => {
            let inner = eval_expression(expr, row, field_types, dialect)?;
            eval_cast(inner, target, matches!(expr.as_ref(), SqlCatalogBoundExpression::Literal(_)))?
        }
    })
}

/// 在给定行上求值一个可能读取外层列的表达式。
///
/// 不含相关子查询时按普通逐行求值；含相关子查询时，先把该行的外层列值代入子查询计划，
/// 再按普通流程物化并求值。外层每行因此各自重跑一次子查询，不复用其他行的结果。
fn eval_row_expression(
    expr: &SqlCatalogBoundExpression,
    row: &RowFieldValues,
    field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
) -> Result<SqlValue, SqlFrontendError> {
    if !crate::sql_planner::expression_reads_outer(expr) {
        return eval_expression(expr, row, field_types, dialect);
    }
    let substituted = substitute_outer(expr.clone(), row);
    let lowered = lower_expression(substituted, executor, transaction, dialect)?;
    eval_expression(&lowered, row, field_types, dialect)
}

/// 把表达式里的外层列引用替换成该行的字面量值，包括子查询计划内部的外层引用。
///
/// 替换之后子查询不再读外层列，`lower_expression` 就可以按原路径把它物化。
fn substitute_outer(expr: SqlCatalogBoundExpression, row: &RowFieldValues) -> SqlCatalogBoundExpression {
    use SqlCatalogBoundExpression::*;
    match expr {
        OuterColumn(column) => Literal(outer_literal(&column, row)),
        Column(_) | Literal(_) | Wildcard => expr,
        Function { name, args, distinct, window } => Function {
            name,
            args: args.into_iter().map(|arg| substitute_outer(arg, row)).collect(),
            distinct,
            window: window.map(|window| SqlCatalogBoundWindowSpec {
                partition_by: window.partition_by.into_iter().map(|element| substitute_outer(element, row)).collect(),
                order_by: substitute_outer_order_by(window.order_by, row),
                frame: window.frame,
            }),
        },
        Binary { left, operator, right } => {
            Binary { left: Box::new(substitute_outer(*left, row)), operator, right: Box::new(substitute_outer(*right, row)) }
        }
        Unary { operator, expression } => Unary { operator, expression: Box::new(substitute_outer(*expression, row)) },
        InSubquery { expression, plan, negated } => InSubquery {
            expression: Box::new(substitute_outer(*expression, row)),
            plan: Box::new(substitute_outer_plan(*plan, row)),
            negated,
        },
        InValues { expression, values, negated } => {
            InValues { expression: Box::new(substitute_outer(*expression, row)), values, negated }
        }
        Case { branches, else_branch } => Case {
            branches: branches
                .into_iter()
                .map(|(when, then)| (Box::new(substitute_outer(*when, row)), Box::new(substitute_outer(*then, row))))
                .collect(),
            else_branch: else_branch.map(|element| Box::new(substitute_outer(*element, row))),
        },
        Cast { expr, target } => Cast { expr: Box::new(substitute_outer(*expr, row)), target },
        Exists { plan, negated } => Exists { plan: Box::new(substitute_outer_plan(*plan, row)), negated },
        ScalarSubquery { plan } => ScalarSubquery { plan: Box::new(substitute_outer_plan(*plan, row)) },
    }
}

/// 把一个计划里所有外层列引用替换成该行的值。
fn substitute_outer_plan(plan: SqlExecutionPlan, row: &RowFieldValues) -> SqlExecutionPlan {
    let substitute = |expression: SqlCatalogBoundExpression| substitute_outer(expression, row);
    let substitute_projections =
        |expressions: Vec<(SqlCatalogBoundExpression, String)>| -> Vec<(SqlCatalogBoundExpression, String)> {
            expressions.into_iter().map(|(expression, label)| (substitute(expression), label)).collect()
        };
    match plan {
        SqlExecutionPlan::Read(mut plan) => {
            plan.remaining_predicate = plan.remaining_predicate.map(substitute);
            plan.projection_expressions = substitute_projections(plan.projection_expressions);
            SqlExecutionPlan::Read(plan)
        }
        SqlExecutionPlan::Scan(mut plan) => {
            plan.remaining_predicate = plan.remaining_predicate.map(substitute);
            plan.having = plan.having.map(substitute);
            plan.group_by = plan.group_by.map(|items| items.into_iter().map(substitute).collect());
            plan.order_by = substitute_outer_order_by_option(plan.order_by, row);
            plan.projection_expressions = substitute_projections(plan.projection_expressions);
            SqlExecutionPlan::Scan(plan)
        }
        SqlExecutionPlan::Join(mut plan) => {
            plan.base.on = plan.base.on.map(substitute);
            for join in &mut plan.joins {
                join.on = join.on.take().map(substitute);
            }
            plan.remaining_predicate = plan.remaining_predicate.map(substitute);
            plan.order_by = substitute_outer_order_by_option(plan.order_by, row);
            plan.projection_expressions = substitute_projections(plan.projection_expressions);
            SqlExecutionPlan::Join(plan)
        }
        SqlExecutionPlan::Scalar(mut plan) => {
            plan.projections = substitute_projections(plan.projections);
            SqlExecutionPlan::Scalar(plan)
        }
        SqlExecutionPlan::SetOperation(mut plan) => {
            plan.operands = plan.operands.into_iter().map(|operand| substitute_outer_plan(operand, row)).collect();
            SqlExecutionPlan::SetOperation(plan)
        }
        SqlExecutionPlan::InsertSelect(mut plan) => {
            plan.source = Box::new(substitute_outer_plan(*plan.source, row));
            SqlExecutionPlan::InsertSelect(plan)
        }
        // 写语句不接受相关子查询，绑定期已经拒绝，因此没有外层列可替换。
        other => other,
    }
}

fn substitute_outer_order_by(items: Vec<SqlCatalogBoundOrderByItem>, row: &RowFieldValues) -> Vec<SqlCatalogBoundOrderByItem> {
    items
        .into_iter()
        .map(|item| SqlCatalogBoundOrderByItem {
            expression: substitute_outer(item.expression, row),
            direction: item.direction,
            nulls: item.nulls,
        })
        .collect()
}

fn substitute_outer_order_by_option(
    items: Option<Vec<SqlCatalogBoundOrderByItem>>,
    row: &RowFieldValues,
) -> Option<Vec<SqlCatalogBoundOrderByItem>> {
    items.map(|items| substitute_outer_order_by(items, row))
}

/// 外层列在该行上的取值；字段缺失按 SQL NULL 处理。
fn outer_literal(column: &SqlCatalogOuterColumn, row: &RowFieldValues) -> SqlBoundLiteral {
    match row.get(column.column.row_id) {
        Some(bytes) => value_to_literal(SqlValue::from_row_bytes_typed(bytes, &column.field_type)),
        None => SqlBoundLiteral::Null,
    }
}

/// 对 CASE WHEN ... THEN ... [ELSE ...] END 求值。
///
/// 依次检查每个 WHEN 条件：第一个求值为 TRUE 的触发对应 THEN 分支；
/// 条件为 FALSE 或 NULL 都继续下一个分支。全部落空时走 ELSE（若有）
/// 否则返回 NULL。
fn eval_case(
    branches: &[(Box<SqlCatalogBoundExpression>, Box<SqlCatalogBoundExpression>)],
    else_branch: &Option<Box<SqlCatalogBoundExpression>>,
    row: &RowFieldValues,
    field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    for (when, then) in branches {
        let cond = eval_expression(when, row, field_types, dialect)?;
        match cond {
            SqlValue::Boolean(true) => return eval_expression(then, row, field_types, dialect),
            SqlValue::Null | SqlValue::Boolean(false) => {}
            _ => {
                return Err(internal_error("CASE WHEN condition must be boolean"));
            }
        }
    }
    match else_branch {
        Some(els) => eval_expression(els, row, field_types, dialect),
        None => Ok(SqlValue::Null),
    }
}

fn eval_case_having(
    branches: &[(Box<SqlCatalogBoundExpression>, Box<SqlCatalogBoundExpression>)],
    else_branch: &Option<Box<SqlCatalogBoundExpression>>,
    bucket_rows: &[RowFieldValues],
    group_exprs: &[SqlCatalogBoundExpression],
    field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    for (when, then) in branches {
        let cond = eval_having(when, bucket_rows, group_exprs, field_types, dialect)?;
        match cond {
            SqlValue::Boolean(true) => return eval_having(then, bucket_rows, group_exprs, field_types, dialect),
            SqlValue::Null | SqlValue::Boolean(false) => {}
            _ => {
                return Err(internal_error("CASE WHEN condition must be boolean"));
            }
        }
    }
    match else_branch {
        Some(els) => eval_having(els, bucket_rows, group_exprs, field_types, dialect),
        None => Ok(SqlValue::Null),
    }
}

/// CAST(expr AS type_name) 求值。支持 INTEGER/INT, REAL/DOUBLE/FLOAT/NUMBER, TEXT/STRING/VARCHAR/CHAR, BLOB/BINARY 互转。
///
/// `from_literal` 区分两种文本→二进制来源：字面量按十六进制解码（wire 层把二进制
/// 入参渲染成 `CAST('<hex>' AS BINARY)`），其余表达式是恒等字节视图
/// （Iris 用 `CAST(f AS BINARY)` 规避 MySQL collation）。
///
/// 类型不支持或无法转换时返回 NULL（与 SQLite/Postgres CAST 行为一致），而不是报错。
fn eval_cast(value: SqlValue, target: &str, from_literal: bool) -> Result<SqlValue, SqlFrontendError> {
    let t = target.trim().to_ascii_uppercase();
    // 忽略括号里的长度修饰，如 VARCHAR(32)
    let base_type = t.split('(').next().unwrap_or(&t);
    match (value, base_type) {
        (SqlValue::Null, _) => Ok(SqlValue::Null),
        (SqlValue::Integer(n), "INTEGER" | "INT" | "INT4" | "BIGINT" | "SMALLINT") => Ok(SqlValue::Integer(n)),
        (SqlValue::Integer(n), "REAL" | "DOUBLE" | "FLOAT" | "NUMBER") => Ok(SqlValue::Number(n as f64)),
        (SqlValue::Integer(n), "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "CHARACTER") => Ok(SqlValue::String(n.to_string())),
        (SqlValue::Number(n), "INTEGER" | "INT" | "INT4" | "BIGINT" | "SMALLINT") => Ok(SqlValue::Integer(n as i64)),
        (SqlValue::Number(n), "REAL" | "DOUBLE" | "FLOAT" | "NUMBER") => Ok(SqlValue::Number(n)),
        (SqlValue::Number(n), "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "CHARACTER") => Ok(SqlValue::String(format_number(n))),
        (SqlValue::String(s), "INTEGER" | "INT" | "INT4" | "BIGINT" | "SMALLINT") => {
            Ok(s.trim().parse::<i64>().map(SqlValue::Integer).unwrap_or(SqlValue::Null))
        }
        (SqlValue::String(s), "REAL" | "DOUBLE" | "FLOAT" | "NUMBER") => {
            Ok(s.trim().parse::<f64>().ok().filter(|v| v.is_finite()).map(SqlValue::Number).unwrap_or(SqlValue::Null))
        }
        (SqlValue::String(s), "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "CHARACTER") => Ok(SqlValue::String(s)),
        (SqlValue::Boolean(b), "INTEGER" | "INT" | "INT4" | "BIGINT" | "SMALLINT") => {
            Ok(SqlValue::Integer(if b { 1 } else { 0 }))
        }
        (SqlValue::Boolean(b), "REAL" | "DOUBLE" | "FLOAT" | "NUMBER") => Ok(SqlValue::Number(if b { 1.0 } else { 0.0 })),
        (SqlValue::Boolean(b), "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "CHARACTER") => {
            Ok(SqlValue::String(if b { "TRUE".to_string() } else { "FALSE".to_string() }))
        }
        (SqlValue::Bytes(bytes), target) if crate::sql::is_binary_type_name(target) => Ok(SqlValue::Bytes(bytes)),
        (SqlValue::String(text), target) if crate::sql::is_binary_type_name(target) => {
            if from_literal {
                // 十六进制拼写是 text → binary 的唯一无损转换。非法拼写必须报错，
                // 不能像旧实现那样静默返回 NULL。
                crate::sql::decode_binary_literal(&text).map(SqlValue::Bytes)
            }
            else {
                // 非字面量表达式：恒等字节视图，不经过十六进制解释。
                Ok(SqlValue::Bytes(text.into_bytes()))
            }
        }
        (_, "BLOB" | "BINARY") => {
            // 其它类型没有到二进制列的文档化转换，维持 NULL 结果。
            Ok(SqlValue::Null)
        }
        _ => {
            // 不支持的目标类型：静默返回 NULL（SQLite 行为）
            Ok(SqlValue::Null)
        }
    }
}

fn eval_in_values(
    target: SqlValue,
    values: &[SqlBoundLiteral],
    negated: bool,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    if matches!(target, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let mut found = false;
    let mut saw_null = false;
    for literal in values {
        match eval_binary(SqlBoundBinaryOperator::Equal, &target, &SqlValue::from_literal(literal), dialect)? {
            SqlValue::Boolean(true) => {
                found = true;
                break;
            }
            SqlValue::Null => saw_null = true,
            _ => {}
        }
    }
    Ok(if found {
        SqlValue::Boolean(!negated)
    }
    else if saw_null {
        SqlValue::Null
    }
    else {
        SqlValue::Boolean(negated)
    })
}

/// 对一行求值标量内建函数。参数已按从左到右求值完毕。
///
/// `dialect` 参与的函数只有这些真正分叉的语义：`LENGTH` 的字节/字符计数、
/// `CONCAT` 的 NULL 传播、`SUBSTRING` 的越界裁剪与 `TRIM` 的显式字符集。
fn eval_scalar_function(
    function: SqlScalarFunction,
    args: &[SqlValue],
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    use SqlValue::{Integer, Null, Number, String as Text};
    // 二进制实参只在语义明确的函数里有定义。文本谓词把 `CAST(... AS BINARY)` 的
    // 恒等字节视图按 UTF-8 解码（见 [`text_view`]），无法解码时报类型错误；
    // 其余函数没有无损的文本形式，显式报类型错误，而不是悄悄退化成 NULL。
    if !matches!(
        function,
        SqlScalarFunction::Length
            | SqlScalarFunction::Coalesce
            | SqlScalarFunction::Instr
            | SqlScalarFunction::Strpos
            | SqlScalarFunction::Left
            | SqlScalarFunction::Right
    ) && args.iter().any(|value| matches!(value, SqlValue::Bytes(_)))
    {
        return Err(SqlFrontendError::new(
            SqlErrorKind::TypeMismatch,
            format!("{}() is not defined for binary values", function.name()),
        ));
    }
    Ok(match function {
        SqlScalarFunction::Version => Text(crate::version_label()),
        // 未选择数据库 / schema 时，MySQL 与 PostgreSQL 都返回 NULL。
        SqlScalarFunction::Database | SqlScalarFunction::CurrentSchema => Null,
        SqlScalarFunction::Now => Text(current_timestamp()),
        SqlScalarFunction::CurrentDate => Text(current_date()),
        SqlScalarFunction::CurrentTime => Text(current_time()),
        SqlScalarFunction::Curdate | SqlScalarFunction::Curtime => {
            // `CURDATE()` / `CURTIME()` 是 MySQL 拼写，PostgreSQL 只认
            // `CURRENT_DATE` / `CURRENT_TIME`，报 `42883` 而不是语法错误。
            if dialect == SqlDialect::Postgres {
                return Err(SqlFrontendError::new(
                    SqlErrorKind::UnknownFunction,
                    format!(
                        "{}() is a MySQL spelling, PostgreSQL spells it CURRENT_{}",
                        function.name(),
                        if function == SqlScalarFunction::Curdate { "DATE" } else { "TIME" }
                    ),
                ));
            }
            Text(if function == SqlScalarFunction::Curdate { current_date() } else { current_time() })
        }
        SqlScalarFunction::Lower => match scalar_text(args, 0) {
            Some(text) => Text(text.to_lowercase()),
            None => Null,
        },
        SqlScalarFunction::Upper => match scalar_text(args, 0) {
            Some(text) => Text(text.to_uppercase()),
            None => Null,
        },
        // MySQL 的 `LENGTH` 是 UTF-8 字节数，PostgreSQL 的 `length` 是字符数。
        // 二进制值在两种方言下都按 octets 计数，结果一致。
        SqlScalarFunction::Length => match args.first() {
            Some(SqlValue::Bytes(bytes)) => Integer(i64::try_from(bytes.len()).unwrap_or(i64::MAX)),
            _ => match scalar_text(args, 0) {
                Some(text) => Integer(match dialect {
                    SqlDialect::Mysql => i64::try_from(text.len()).unwrap_or(i64::MAX),
                    SqlDialect::Postgres => i64::try_from(text.chars().count()).unwrap_or(i64::MAX),
                }),
                None => Null,
            },
        },
        SqlScalarFunction::CharLength => match scalar_text(args, 0) {
            Some(text) => Integer(i64::try_from(text.chars().count()).unwrap_or(i64::MAX)),
            None => Null,
        },
        SqlScalarFunction::Substring => {
            let (text, start) = match (scalar_text(args, 0), scalar_integer(args, 1)) {
                (Some(text), Some(start)) => (text, start),
                _ => return Ok(Null),
            };
            let length = match args.get(2) {
                None => None,
                Some(Null) => return Ok(Null),
                Some(_) => match scalar_integer(args, 2) {
                    Some(value) => Some(value),
                    None => return Ok(Null),
                },
            };
            substring_value(&text, start, length, dialect)?
        }
        SqlScalarFunction::Trim | SqlScalarFunction::Ltrim | SqlScalarFunction::Rtrim => {
            // 显式 trim 字符集是 PostgreSQL 的 `TRIM(x, chars)` 形式。MySQL 只有
            // `TRIM([BOTH|LEADING|TRAILING] chars FROM x)`，两参数调用在 MySQL 是语法错误。
            if dialect == SqlDialect::Mysql && args.len() > 1 {
                return Err(SqlFrontendError::syntax(format!(
                    "{}() with an explicit trim character set is PostgreSQL syntax",
                    function.name()
                )));
            }
            let text = match scalar_text(args, 0) {
                Some(text) => text,
                None => return Ok(Null),
            };
            let characters = match args.get(1) {
                None => None,
                Some(Null) => return Ok(Null),
                Some(_) => scalar_text(args, 1).map(|set| set.chars().collect::<Vec<_>>()),
            };
            // 默认只删除半角空格，两方言一致（MySQL 的 `TRIM(str)` 与 PostgreSQL 的 `trim(str)`）。
            let is_trimmed = |character: char| match characters.as_ref() {
                Some(set) => set.contains(&character),
                None => character == ' ',
            };
            Text(match function {
                SqlScalarFunction::Ltrim => text.trim_start_matches(is_trimmed).to_string(),
                SqlScalarFunction::Rtrim => text.trim_end_matches(is_trimmed).to_string(),
                _ => text.trim_matches(is_trimmed).to_string(),
            })
        }
        SqlScalarFunction::Replace => {
            let (text, from, to) = match (scalar_text(args, 0), scalar_text(args, 1), scalar_text(args, 2)) {
                (Some(text), Some(from), Some(to)) => (text, from, to),
                _ => return Ok(Null),
            };
            // 两方言都把空搜索串当作“无需替换”，而不是在每个字符之间插入替换串。
            Text(if from.is_empty() { text } else { text.replace(&from, &to) })
        }
        SqlScalarFunction::Concat => {
            let mut joined = String::new();
            for index in 0..args.len() {
                match scalar_text(args, index) {
                    Some(text) => joined.push_str(&text),
                    // MySQL：任一参数为 NULL，结果是 NULL。
                    None if dialect == SqlDialect::Mysql => return Ok(Null),
                    // PostgreSQL 的 `concat` 跳过 NULL 参数，把它当作空串。
                    None => {}
                }
            }
            Text(joined)
        }
        SqlScalarFunction::Coalesce => args.iter().find(|value| **value != Null).cloned().unwrap_or(Null),
        SqlScalarFunction::Instr | SqlScalarFunction::Strpos => {
            // 两种拼写语义相同（1-based 字符位置，未找到为 0），但只在各自方言里可调用。
            let allowed = match dialect {
                SqlDialect::Mysql => SqlScalarFunction::Instr,
                SqlDialect::Postgres => SqlScalarFunction::Strpos,
            };
            if function != allowed {
                let (owner, alternate) = if function == SqlScalarFunction::Instr {
                    ("MySQL", SqlScalarFunction::Strpos)
                }
                else {
                    ("PostgreSQL", SqlScalarFunction::Instr)
                };
                return Err(SqlFrontendError::new(
                    SqlErrorKind::UnknownFunction,
                    format!("{}() is a {owner} spelling, the other dialect spells it {}()", function.name(), alternate.name()),
                ));
            }
            let haystack = match text_view(args, 0)? {
                Some(text) => text,
                None => return Ok(Null),
            };
            let needle = match text_view(args, 1)? {
                Some(text) => text,
                None => return Ok(Null),
            };
            Integer(character_position(&haystack, &needle))
        }
        SqlScalarFunction::Left | SqlScalarFunction::Right => {
            let text = match text_view(args, 0)? {
                Some(text) => text,
                None => return Ok(Null),
            };
            let count = match scalar_integer(args, 1) {
                Some(count) => count,
                None => return Ok(Null),
            };
            Text(edge_characters(&text, count, function == SqlScalarFunction::Right))
        }
        SqlScalarFunction::Abs => match args.first() {
            Some(Integer(value)) => Integer(value.checked_abs().ok_or_else(|| numeric_overflow_error("ABS"))?),
            Some(Number(value)) => Number(value.abs()),
            Some(Null) | None => Null,
            Some(_) => Number(0.0),
        },
        SqlScalarFunction::Round => {
            let digits = match args.get(1) {
                Some(Integer(value)) => Some(*value as f64),
                Some(Number(value)) => Some(*value),
                _ => None,
            };
            match args.first() {
                Some(Integer(value)) => match digits {
                    Some(digits) if digits < 0.0 => {
                        let factor = 10f64.powi(digits.abs() as i32);
                        Number((*value as f64 / factor).round() * factor)
                    }
                    _ => Integer(*value),
                },
                Some(Number(value)) => match digits {
                    Some(digits) => {
                        let factor = 10f64.powi(digits as i32);
                        Number((value * factor).round() / factor)
                    }
                    None => Number(value.round()),
                },
                Some(Null) | None => Null,
                Some(_) => Number(0.0),
            }
        }
    })
}

/// `SUBSTRING(x, start[, length])`，下标从 1 开始，字符（不是字节）计数。
///
/// 两方言共享 1-based 下标与“任一侧 NULL 即 NULL”，差异在于越界与负值：
///
/// - MySQL：`start = 0` 与负长度都返回空串，负起点从字符串结尾倒数后裁剪。
/// - PostgreSQL：`start < 1` 从第 1 个字符开始并把范围裁剪到 `start + length - 1`，
///   负长度报 `22011`，超出结尾返回空串。
fn substring_value(text: &str, start: i64, length: Option<i64>, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
    let characters = text.chars().collect::<Vec<_>>();
    let total = i64::try_from(characters.len()).unwrap_or(i64::MAX);
    let (first, end) = match dialect {
        SqlDialect::Mysql => {
            if start == 0 || length.is_some_and(|length| length < 0) {
                return Ok(SqlValue::String(String::new()));
            }
            let offset = if start > 0 { start - 1 } else { total + start };
            let mut span = length.unwrap_or(total - offset);
            let mut begin = offset;
            if begin < 0 {
                span += begin;
                begin = 0;
            }
            (begin, begin + span)
        }
        SqlDialect::Postgres => {
            if length.is_some_and(|length| length < 0) {
                return Err(SqlFrontendError::new(
                    SqlErrorKind::InvalidSubstringLength,
                    "negative substring length not allowed",
                ));
            }
            let begin = (start - 1).max(0);
            let end = match length {
                Some(length) => (start + length - 1).max(0),
                None => total,
            };
            (begin, end)
        }
    };
    let first = first.clamp(0, total) as usize;
    let end = end.clamp(0, total) as usize;
    Ok(SqlValue::String(if end <= first { String::new() } else { characters[first..end].iter().collect() }))
}

/// 把第 `index` 个参数取成字符串；NULL 或缺失返回 `None`。
fn scalar_text(args: &[SqlValue], index: usize) -> Option<String> {
    match args.get(index) {
        Some(SqlValue::String(text)) => Some(text.clone()),
        Some(SqlValue::Integer(value)) => Some(value.to_string()),
        Some(SqlValue::Number(value)) => Some(format_number(*value)),
        Some(SqlValue::Boolean(value)) => Some(if *value { "1" } else { "0" }.to_string()),
        // 二进制值是原始 octets，没有无损的文本形式。`LENGTH` 这类对二进制有
        // 明确定义的函数在上层单独判定，不经过本函数。
        Some(SqlValue::Bytes(_)) => None,
        Some(SqlValue::Null) | None => None,
    }
}

/// 把第 `index` 个参数取成整数；NULL 或无法解释成整数的文本返回 `None`。
fn scalar_integer(args: &[SqlValue], index: usize) -> Option<i64> {
    match args.get(index)? {
        SqlValue::Integer(value) => Some(*value),
        SqlValue::Number(value) => Some(*value as i64),
        SqlValue::String(text) => text.trim().parse::<i64>().ok(),
        SqlValue::Boolean(value) => Some(i64::from(*value)),
        SqlValue::Bytes(_) => None,
        SqlValue::Null => None,
    }
}

/// 把第 `index` 个参数取成文本视图，供文本谓词使用。
///
/// `CAST(x AS BINARY)` 的恒等字节视图只在能整体按 UTF-8 解码时才参与文本谓词；
/// 真正的二进制列无法解码，报类型不匹配而不是静默近似。NULL 与缺失返回 `None`。
fn text_view(args: &[SqlValue], index: usize) -> Result<Option<String>, SqlFrontendError> {
    match args.get(index) {
        None | Some(SqlValue::Null) => Ok(None),
        Some(SqlValue::Bytes(bytes)) => std::str::from_utf8(bytes).map(|text| Some(text.to_string())).map_err(|_| {
            SqlFrontendError::new(
                SqlErrorKind::TypeMismatch,
                "binary value is not valid UTF-8 text and cannot serve as text".to_string(),
            )
        }),
        _ => Ok(scalar_text(args, index)),
    }
}

/// `INSTR` / `STRPOS` 的 1-based 字符位置；未找到为 0，空 needle 为 1（两方言一致）。
fn character_position(haystack: &str, needle: &str) -> i64 {
    if needle.is_empty() {
        return 1;
    }
    match haystack.find(needle) {
        Some(byte_offset) => i64::try_from(haystack[..byte_offset].chars().count()).map_or(i64::MAX, |chars| chars + 1),
        None => 0,
    }
}

/// `LEFT` / `RIGHT` 按字符计数取一侧片段；非正数计数返回空串，超出长度取整串。
fn edge_characters(text: &str, count: i64, from_end: bool) -> String {
    if count <= 0 {
        return String::new();
    }
    let characters: Vec<char> = text.chars().collect();
    let take = usize::try_from(count).unwrap_or(usize::MAX).min(characters.len());
    if from_end {
        characters[characters.len() - take..].iter().collect()
    }
    else {
        characters[..take].iter().collect()
    }
}

/// 会话时间戳 `YYYY-MM-DD HH:MM:SS`，UTC。
fn current_timestamp() -> String {
    let seconds = current_utc_seconds();
    format!("{} {}", date_of(seconds), time_of(seconds))
}

/// 会话日期 `YYYY-MM-DD`，UTC。
fn current_date() -> String {
    date_of(current_utc_seconds())
}

/// 会话时间 `HH:MM:SS`，UTC。
fn current_time() -> String {
    time_of(current_utc_seconds())
}

/// 当前 UTC 秒数；时钟早于 Unix epoch 时按 0 处理。
fn current_utc_seconds() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|elapsed| elapsed.as_secs() as i64).unwrap_or(0)
}

fn date_of(seconds: i64) -> String {
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    format!("{year:04}-{month:02}-{day:02}")
}

fn time_of(seconds: i64) -> String {
    let remainder = seconds.rem_euclid(86_400);
    format!("{:02}:{:02}:{:02}", remainder / 3600, (remainder % 3600) / 60, remainder % 60)
}

/// Gregorian date from days since 1970-01-01 (Howard Hinnant's civil_from_days).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 { month_prime + 3 } else { month_prime - 9 };
    (if month <= 2 { year + 1 } else { year }, month as u32, day as u32)
}

/// Render a numeric SQL value with no trailing `.0` for integral values.
pub(crate) fn format_number(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    }
    else {
        format!("{value}")
    }
}

fn eval_unary(op: SqlBoundUnaryOperator, operand: SqlValue) -> Result<SqlValue, SqlFrontendError> {
    Ok(match op {
        SqlBoundUnaryOperator::Plus => match operand {
            SqlValue::Integer(n) => SqlValue::Integer(n),
            SqlValue::Number(n) => SqlValue::Number(n),
            _ => SqlValue::Null,
        },
        SqlBoundUnaryOperator::Minus => match operand {
            SqlValue::Integer(n) => SqlValue::Integer(n.checked_neg().ok_or_else(|| numeric_overflow_error("unary minus"))?),
            SqlValue::Number(n) => SqlValue::Number(-n),
            _ => SqlValue::Null,
        },
        // SQL 三值逻辑：`NOT NULL` 仍是 NULL。
        SqlBoundUnaryOperator::Not => match sql_truth(&operand) {
            Some(value) => SqlValue::Boolean(!value),
            None => SqlValue::Null,
        },
        // 存在性判断不是与 NULL 比较，因此结果永远是 TRUE/FALSE。
        SqlBoundUnaryOperator::IsNull => SqlValue::Boolean(matches!(operand, SqlValue::Null)),
        SqlBoundUnaryOperator::IsNotNull => SqlValue::Boolean(!matches!(operand, SqlValue::Null)),
        // 真值判断同样是三值谓词：`NULL IS TRUE` 与 `NULL IS FALSE` 都为 FALSE，
        // 因此永远不会返回 NULL。
        SqlBoundUnaryOperator::IsTruth { value, negated } => SqlValue::Boolean((sql_truth(&operand) == Some(value)) != negated),
    })
}

fn eval_binary(
    op: SqlBoundBinaryOperator,
    left: &SqlValue,
    right: &SqlValue,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    use SqlValue::*;
    // AND / OR 必须先于 NULL 传播处理，否则 `NULL AND FALSE` 会被错误地求成 NULL。
    match op {
        SqlBoundBinaryOperator::And => return Ok(sql_and(left, right)),
        SqlBoundBinaryOperator::Or => return Ok(sql_or(left, right)),
        _ => {}
    }
    if matches!(left, Null) || matches!(right, Null) {
        return Ok(Null);
    }
    Ok(match (left, right) {
        (Integer(a), Integer(b)) => integer_binary(op, *a, *b, dialect)?,
        // 整数与浮点混合比较必须先走精确序关系，否则 2^53 以上的 `i64` 会先被
        // 舍入成 `f64`，导致 `9007199254740993 > 9007199254740992.0` 判成相等。
        (Integer(a), Number(b)) if is_comparison(op) => match compare_integer_float(*a, *b) {
            Some(ordering) => comparison_value(op, ordering),
            None => float_binary(op, *a as f64, *b, dialect)?,
        },
        (Number(a), Integer(b)) if is_comparison(op) => match compare_integer_float(*b, *a) {
            Some(ordering) => comparison_value(op, ordering.reverse()),
            None => float_binary(op, *a, *b as f64, dialect)?,
        },
        (Integer(a), Number(b)) => float_binary(op, *a as f64, *b, dialect)?,
        (Number(a), Integer(b)) => float_binary(op, *a, *b as f64, dialect)?,
        (Number(a), Number(b)) => float_binary(op, *a, *b, dialect)?,
        // utf8mb4 / UTF8 文本按 code point 精确比较，大小写敏感，不使用 collation。
        (String(a), String(b)) => match op {
            SqlBoundBinaryOperator::Equal => Boolean(eq_string(a, b)),
            SqlBoundBinaryOperator::NotEqual => Boolean(!eq_string(a, b)),
            SqlBoundBinaryOperator::Less => Boolean(cmp_string(a, b).is_lt()),
            SqlBoundBinaryOperator::Greater => Boolean(cmp_string(a, b).is_gt()),
            SqlBoundBinaryOperator::LessEqual => Boolean(!cmp_string(a, b).is_gt()),
            SqlBoundBinaryOperator::GreaterEqual => Boolean(!cmp_string(a, b).is_lt()),
            SqlBoundBinaryOperator::Like => Boolean(like_match(a, b)),
            _ => Null,
        },
        // 二进制值按字节序比较。文本没有隐式转成 octets：混合比较返回 NULL，
        // 需要比较时由调用方显式 `CAST`。
        (Bytes(a), Bytes(b)) if is_comparison(op) => comparison_value(op, a.cmp(b)),
        (Boolean(a), Boolean(b)) => match op {
            SqlBoundBinaryOperator::Equal => Boolean(a == b),
            SqlBoundBinaryOperator::NotEqual => Boolean(a != b),
            _ => Null,
        },
        _ => Null,
    })
}

/// 比较类操作符（`=`/`<>`/`<`/`>`/`<=`/`>=`），其余是算术或逻辑操作符。
fn is_comparison(op: SqlBoundBinaryOperator) -> bool {
    matches!(
        op,
        SqlBoundBinaryOperator::Equal
            | SqlBoundBinaryOperator::NotEqual
            | SqlBoundBinaryOperator::Less
            | SqlBoundBinaryOperator::Greater
            | SqlBoundBinaryOperator::LessEqual
            | SqlBoundBinaryOperator::GreaterEqual
    )
}

/// 把一个精确序关系映射成布尔比较结果。调用方保证 `op` 是比较操作符。
fn comparison_value(op: SqlBoundBinaryOperator, ordering: std::cmp::Ordering) -> SqlValue {
    SqlValue::Boolean(match op {
        SqlBoundBinaryOperator::Equal => ordering.is_eq(),
        SqlBoundBinaryOperator::NotEqual => !ordering.is_eq(),
        SqlBoundBinaryOperator::Less => ordering.is_lt(),
        SqlBoundBinaryOperator::Greater => ordering.is_gt(),
        SqlBoundBinaryOperator::LessEqual => ordering.is_le(),
        SqlBoundBinaryOperator::GreaterEqual => ordering.is_ge(),
        _ => false,
    })
}

/// 精确比较 `i64` 与 `f64`。浮点是 NaN 时返回 `None`，调用方保留原有浮点比较语义。
///
/// `i64` 的上下界在 `f64` 中可精确表示：`i64::MIN as f64 == -2^63`，
/// 而 `i64::MAX as f64` 向上舍入到 `2^63`。因此先把浮点截断成整数再比较，
/// 只有整数部分相等时才用小数部分决定序关系，避免 `i64` 先转 `f64` 丢精度。
fn compare_integer_float(integer: i64, float: f64) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if float.is_nan() {
        return None;
    }
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    if float >= TWO_POW_63 {
        return Some(Ordering::Less);
    }
    if float < -TWO_POW_63 {
        return Some(Ordering::Greater);
    }
    let truncated = float.trunc();
    let truncated_integer = truncated as i64;
    match integer.cmp(&truncated_integer) {
        Ordering::Equal if float > truncated => Some(Ordering::Less),
        Ordering::Equal if float < truncated => Some(Ordering::Greater),
        other => Some(other),
    }
}

/// 整数之间的四则与比较：加、减、乘、模保持精确整数，只在溢出时显式报错。
fn integer_binary(op: SqlBoundBinaryOperator, a: i64, b: i64, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
    use SqlValue::{Boolean, Integer, Number};
    let overflow = || numeric_overflow_error("integer arithmetic");
    let divide_by_zero = || match dialect {
        SqlDialect::Postgres => Err(division_by_zero_error()),
        SqlDialect::Mysql => Ok(SqlValue::Null),
    };
    Ok(match op {
        SqlBoundBinaryOperator::Plus => Integer(a.checked_add(b).ok_or_else(overflow)?),
        SqlBoundBinaryOperator::Minus => Integer(a.checked_sub(b).ok_or_else(overflow)?),
        SqlBoundBinaryOperator::Multiply => Integer(a.checked_mul(b).ok_or_else(overflow)?),
        // MySQL INT/INT → 提升到 f64 除法；PostgreSQL INT/INT → 整数截断
        SqlBoundBinaryOperator::Divide => {
            if b == 0 {
                return divide_by_zero();
            }
            match dialect {
                SqlDialect::Mysql => Number(a as f64 / b as f64),
                SqlDialect::Postgres => Integer(a.checked_div(b).ok_or_else(overflow)?),
            }
        }
        // 与 PostgreSQL / MySQL 一致，余数符号跟随被除数。
        SqlBoundBinaryOperator::Modulo => {
            if b == 0 {
                return divide_by_zero();
            }
            Integer(a.checked_rem(b).ok_or_else(overflow)?)
        }
        SqlBoundBinaryOperator::Equal => Boolean(a == b),
        SqlBoundBinaryOperator::NotEqual => Boolean(a != b),
        SqlBoundBinaryOperator::Less => Boolean(a < b),
        SqlBoundBinaryOperator::Greater => Boolean(a > b),
        SqlBoundBinaryOperator::LessEqual => Boolean(a <= b),
        SqlBoundBinaryOperator::GreaterEqual => Boolean(a >= b),
        SqlBoundBinaryOperator::Like | SqlBoundBinaryOperator::And | SqlBoundBinaryOperator::Or => SqlValue::Null,
    })
}

/// 至少一侧是浮点时按 f64 运算；比较也按 f64 语义。
fn float_binary(op: SqlBoundBinaryOperator, a: f64, b: f64, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
    use SqlValue::{Boolean, Number};
    let divide_by_zero = || match dialect {
        SqlDialect::Postgres => Err(division_by_zero_error()),
        SqlDialect::Mysql => Ok(SqlValue::Null),
    };
    Ok(match op {
        SqlBoundBinaryOperator::Plus => Number(a + b),
        SqlBoundBinaryOperator::Minus => Number(a - b),
        SqlBoundBinaryOperator::Multiply => Number(a * b),
        SqlBoundBinaryOperator::Divide => {
            if b == 0.0 {
                return divide_by_zero();
            }
            Number(a / b)
        }
        SqlBoundBinaryOperator::Modulo => {
            if b == 0.0 {
                return divide_by_zero();
            }
            Number(a % b)
        }
        SqlBoundBinaryOperator::Equal => Boolean(a == b),
        SqlBoundBinaryOperator::NotEqual => Boolean(a != b),
        SqlBoundBinaryOperator::Less => Boolean(a < b),
        SqlBoundBinaryOperator::Greater => Boolean(a > b),
        SqlBoundBinaryOperator::LessEqual => Boolean(a <= b),
        SqlBoundBinaryOperator::GreaterEqual => Boolean(a >= b),
        SqlBoundBinaryOperator::Like | SqlBoundBinaryOperator::And | SqlBoundBinaryOperator::Or => SqlValue::Null,
    })
}

/// SQL LIKE 匹配。`%` 匹配 0+ 任意字符，`_` 匹配正好 1 个任意字符。
/// 非通配符字符按 code point 精确比较，与运算符比较共用同一口径。
fn like_match(haystack: &str, pattern: &str) -> bool {
    let haystack: Vec<char> = haystack.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    let n = haystack.len();
    let m = pattern.len();

    // dp[i][j] = haystack[0..i] 能否匹配 pattern[0..j]
    let mut dp = vec![vec![false; m + 1]; n + 1];
    dp[0][0] = true;

    // pattern 前缀全是 % 的情况
    let mut j = 1;
    while j <= m && pattern[j - 1] == '%' {
        dp[0][j] = true;
        j += 1;
    }

    for i in 1..=n {
        for j in 1..=m {
            match pattern[j - 1] {
                '%' => {
                    // % 可以跳过（用 dp[i][j-1]）或吃掉当前 haystack 字符（用 dp[i-1][j]）
                    dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
                }
                '_' => {
                    // _ 必须正好吃掉一个字符
                    dp[i][j] = dp[i - 1][j - 1];
                }
                p => {
                    dp[i][j] = dp[i - 1][j - 1] && haystack[i - 1] == p;
                }
            }
        }
    }
    dp[n][m]
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn read_u16(&mut self) -> Result<u16, SqlFrontendError> {
        if self.remaining() < 2 {
            return Err(internal_error("row TLV truncated reading u16"));
        }
        let value = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, SqlFrontendError> {
        if self.remaining() < 4 {
            return Err(internal_error("row TLV truncated reading u32"));
        }
        let bytes: [u8; 4] = self.data[self.pos..self.pos + 4].try_into().unwrap();
        let value = u32::from_be_bytes(bytes);
        self.pos += 4;
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64, SqlFrontendError> {
        if self.remaining() < 8 {
            return Err(internal_error("row TLV truncated reading u64"));
        }
        let bytes: [u8; 8] = self.data[self.pos..self.pos + 8].try_into().unwrap();
        let value = u64::from_be_bytes(bytes);
        self.pos += 8;
        Ok(value)
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, SqlFrontendError> {
        if self.remaining() < len {
            return Err(internal_error("row TLV truncated reading bytes"));
        }
        let out = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(out)
    }
}

/// 把 GROUP BY 的 key values 序列化成稳定字符串，用作 BTreeMap bucket key。
fn serialize_group_key(values: &[SqlValue]) -> String {
    use std::fmt::Write;
    let mut buf = String::new();
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            buf.push_str("|");
        }
        match v {
            SqlValue::Null => buf.push_str("NULL"),
            SqlValue::Integer(n) => {
                let _ = write!(buf, "I:{n}");
            }
            SqlValue::Number(n) => {
                let _ = write!(buf, "N:{n}");
            }
            SqlValue::String(s) => {
                buf.push_str("S:");
                buf.push_str(s);
            }
            SqlValue::Boolean(b) => {
                buf.push_str(if *b { "B:true" } else { "B:false" });
            }
            SqlValue::Bytes(bytes) => {
                buf.push_str("X:");
                buf.push_str(&crate::sql::encode_hex(bytes));
            }
        }
    }
    buf
}

/// 在 GROUP BY bucket 上下文中求值表达式。
/// 与 `eval_expression` 的区别：Function 会被当作聚合函数对 bucket_rows 求值；
/// Column 从 bucket 首行取（同一 bucket 内 group key 列值相同）。
fn eval_having(
    expr: &SqlCatalogBoundExpression,
    bucket_rows: &[RowFieldValues],
    group_exprs: &[SqlCatalogBoundExpression],
    field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    let first = bucket_rows.first();
    Ok(match expr {
        SqlCatalogBoundExpression::Column(col) => {
            first.and_then(|row| row.value_typed(col.row_id, field_types)).unwrap_or(SqlValue::Null)
        }
        SqlCatalogBoundExpression::Literal(lit) => SqlValue::from_literal(lit),
        SqlCatalogBoundExpression::Wildcard => {
            return Err(internal_error("wildcard `*` is only valid as the argument of COUNT"))
        }
        // 分组路径不接受相关子查询，计划期已拒绝，走到这里是内部漏检。
        SqlCatalogBoundExpression::OuterColumn(_) => {
            return Err(internal_error("outer column reference is not allowed in a grouped expression"))
        }
        SqlCatalogBoundExpression::Function { name, args, .. } => {
            if SqlAggregateFunction::from_name(name).is_some() {
                eval_aggregate(expr, bucket_rows, field_types, dialect)?
            }
            else if let Some(function) = SqlScalarFunction::from_name(name) {
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    values.push(eval_having(arg, bucket_rows, group_exprs, field_types, dialect)?);
                }
                eval_scalar_function(function, &values, dialect)?
            }
            else {
                return Err(internal_error(&format!("unknown function in grouped expression: {name}")));
            }
        }
        SqlCatalogBoundExpression::Unary { operator, expression } => {
            let inner = eval_having(expression, bucket_rows, group_exprs, field_types, dialect)?;
            eval_unary(*operator, inner)?
        }
        SqlCatalogBoundExpression::Binary { left, operator, right } => {
            let l = eval_having(left, bucket_rows, group_exprs, field_types, dialect)?;
            let r = eval_having(right, bucket_rows, group_exprs, field_types, dialect)?;
            eval_binary(*operator, &l, &r, dialect)?
        }
        SqlCatalogBoundExpression::InValues { expression, values, negated } => {
            let target = eval_having(expression, bucket_rows, group_exprs, field_types, dialect)?;
            eval_in_values(target, values, *negated, dialect)?
        }
        SqlCatalogBoundExpression::InSubquery { .. } => {
            return Err(internal_error("IN subquery was not materialized before execution"))
        }
        SqlCatalogBoundExpression::Exists { .. } => {
            return Err(internal_error("EXISTS predicate was not materialized before execution"))
        }
        SqlCatalogBoundExpression::ScalarSubquery { .. } => {
            return Err(internal_error("scalar subquery was not evaluated before execution"))
        }
        SqlCatalogBoundExpression::Case { branches, else_branch } => {
            eval_case_having(branches, else_branch, bucket_rows, group_exprs, field_types, dialect)?
        }
        SqlCatalogBoundExpression::Cast { expr, target } => {
            let inner = eval_having(expr, bucket_rows, group_exprs, field_types, dialect)?;
            eval_cast(inner, target, matches!(expr.as_ref(), SqlCatalogBoundExpression::Literal(_)))?
        }
    })
}

fn internal_error(message: impl Into<String>) -> SqlFrontendError {
    SqlFrontendError::internal(message)
}

fn division_by_zero_error() -> SqlFrontendError {
    SqlFrontendError::new(SqlErrorKind::DivisionByZero, "division by zero")
}

/// 精确整数运算越界。两方言都要报 SQLSTATE `22003`，因此不降级成通用执行错误。
fn numeric_overflow_error(stage: &str) -> SqlFrontendError {
    SqlFrontendError::new(SqlErrorKind::NumericOverflow, format!("{stage} overflowed the integer range"))
}

/// Storage failures that other transactions can resolve by retrying are
/// reported as serialization conflicts rather than generic execution errors.
fn write_error(message: String, error: &yyds_types::Error) -> SqlFrontendError {
    match error {
        yyds_types::Error::CasConflict { .. } => SqlFrontendError::new(SqlErrorKind::Conflict, message),
        _ => SqlFrontendError::internal(message),
    }
}

/// SQL value equality used by `SELECT DISTINCT`.
///
/// `NULL` equals `NULL` for de-duplication, integers and floats compare by exact
/// numeric value (never through a lossy `f64` cast for `i64`), and unrelated
/// storage variants are never collapsed into one row.
fn values_equal(left: &SqlValue, right: &SqlValue) -> bool {
    use std::cmp::Ordering;
    match (left, right) {
        (SqlValue::Integer(left), SqlValue::Integer(right)) => left == right,
        (SqlValue::Integer(integer), SqlValue::Number(float)) => {
            compare_integer_float(*integer, *float) == Some(Ordering::Equal)
        }
        (SqlValue::Number(float), SqlValue::Integer(integer)) => {
            compare_integer_float(*integer, *float) == Some(Ordering::Equal)
        }
        (SqlValue::Number(left), SqlValue::Number(right)) => {
            left.partial_cmp(right).unwrap_or(Ordering::Equal) == Ordering::Equal
        }
        (SqlValue::String(left), SqlValue::String(right)) => eq_string(left, right),
        (SqlValue::Boolean(left), SqlValue::Boolean(right)) => left == right,
        (SqlValue::Null, SqlValue::Null) => true,
        _ => false,
    }
}

fn projected_rows_equal(left: &[SqlValue], right: &[SqlValue]) -> bool {
    left.len() == right.len() && left.iter().zip(right.iter()).all(|(left, right)| values_equal(left, right))
}

/// Keeps the first occurrence of each projected row, preserving scan order.
fn retain_distinct_projected_rows(staged: &mut Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)>) {
    let mut seen: Vec<Vec<SqlValue>> = Vec::new();
    staged.retain(|(_, row)| {
        if seen.iter().any(|candidate| projected_rows_equal(candidate, row)) {
            false
        }
        else {
            seen.push(row.clone());
            true
        }
    });
}

/// NULLs sort last (standard SQL behavior). Mixed types collate equal.
fn compare_values(a: Option<&SqlValue>, b: Option<&SqlValue>) -> std::cmp::Ordering {
    compare_values_with_nulls(a, b, false)
}

/// 解析 ORDER BY 项排序前的 NULL 置前标记：未写 `NULLS FIRST/LAST` 时用方言默认，
/// 显式子句则在 ASC 或 DESC 下分别映射为置前或置后。
fn order_nulls_first(direction: &SqlBoundOrderDirection, explicit: Option<SqlNullsOrder>, dialect_default: bool) -> bool {
    match explicit {
        None => dialect_default,
        Some(SqlNullsOrder::First) => matches!(direction, SqlBoundOrderDirection::Asc),
        Some(SqlNullsOrder::Last) => matches!(direction, SqlBoundOrderDirection::Desc),
    }
}

/// 比较两个排序键，NULL 位置由方言决定：PostgreSQL 把 NULL 视为最大值，
/// MySQL 把 NULL 视为最小值。
fn compare_values_with_nulls(a: Option<&SqlValue>, b: Option<&SqlValue>, nulls_first: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) | (Some(SqlValue::Null), Some(SqlValue::Null)) => Ordering::Equal,
        (None | Some(SqlValue::Null), Some(_)) => {
            if nulls_first {
                Ordering::Less
            }
            else {
                Ordering::Greater
            }
        }
        (Some(_), None | Some(SqlValue::Null)) => {
            if nulls_first {
                Ordering::Greater
            }
            else {
                Ordering::Less
            }
        }
        (Some(x), Some(y)) => match (x, y) {
            (SqlValue::Integer(n1), SqlValue::Integer(n2)) => n1.cmp(n2),
            // 排序同样不能把 `i64` 先转 `f64`，否则超出 2^53 的行会并列。
            (SqlValue::Integer(n1), SqlValue::Number(n2)) => compare_integer_float(*n1, *n2).unwrap_or(Ordering::Equal),
            (SqlValue::Number(n1), SqlValue::Integer(n2)) => {
                compare_integer_float(*n2, *n1).map(Ordering::reverse).unwrap_or(Ordering::Equal)
            }
            (SqlValue::Number(n1), SqlValue::Number(n2)) => n1.partial_cmp(n2).unwrap_or(Ordering::Equal),
            (SqlValue::String(s1), SqlValue::String(s2)) => cmp_string(s1, s2),
            (SqlValue::Boolean(b1), SqlValue::Boolean(b2)) => b1.cmp(b2),
            // 二进制列按原始 octets 的字节序排序，不经过任何文本折叠。
            (SqlValue::Bytes(a), SqlValue::Bytes(b)) => a.cmp(b),
            _ => Ordering::Equal,
        },
    }
}
