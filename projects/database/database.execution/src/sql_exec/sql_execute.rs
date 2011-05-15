//! 用 集群 的 `KeyValueExecutor` 执行一个已计划好的 SQL SELECT。
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

use database_query::{
    catalog_bind::{
        ROW_ID_STRIDE, SqlCatalogBoundExpression, SqlCatalogBoundOrderByItem, SqlCatalogBoundWindowSpec, SqlCatalogOuterColumn,
        collect_column_ids,
    },
    sql::{
        SqlAggregateFunction, SqlBoundBinaryOperator, SqlBoundLiteral, SqlBoundOrderDirection, SqlBoundSetOperator, SqlBoundUnaryOperator,
        SqlBoundWindowFrame, SqlBoundWindowFrameBound, SqlBoundWindowFrameUnit, SqlDialect, SqlErrorKind, SqlFrontendError, SqlNullsOrder,
        SqlScalarFunction, SqlWindowFunction,
    },
    sql_execute::{
        RowFieldValues, SqlResult, SqlValue, compare_values, compare_values_with_nulls, decode_row, decode_row_fields, encode_row, eval_binary,
        eval_cast, eval_expression, eval_in_values, eval_scalar_function, eval_unary, format_number, order_nulls_first, projected_rows_equal,
        retain_distinct_projected_rows,
    },
    sql_planner::{
        ConflictAssignment, ConflictPolicy, DeletePlan, DeleteScanPlan, InsertSelectPlan, JoinPlan, JoinSourcePlan, ReadPlan,
        ReturningProjections, ScalarPlan, ScanPlan, SetOperationPlan, SqlExecutionPlan, UniqueConstraintPlan, UpdatePlan, UpdateScanPlan,
        WriteField, WritePlan, check_write_literal, pk_literal_to_rowid,
    },
};
use database_types::cluster::Namespace;

use crate::cluster::{BufferedCommit, CommitCondition, KeyValueExecutor, TransactionHandle};
use database_query::cluster_kv::{KeyValueAction, KeyValueCommand};

mod outer;
mod read;
mod window;
mod write;

pub(crate) use self::{outer::*, read::*, window::*, write::*};
pub(crate) use database_query::sql_execute::{cmp_string, eq_string, expression_needed_field_ids, scan_needed_field_ids};

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
            args: args.into_iter().map(|arg| lower_expression(arg, executor, transaction, dialect)).collect::<Result<Vec<_>, _>>()?,
            distinct,
            window,
        },
        SqlCatalogBoundExpression::Binary { left, operator, right } => SqlCatalogBoundExpression::Binary {
            left: Box::new(lower_expression(*left, executor, transaction, dialect)?),
            operator,
            right: Box::new(lower_expression(*right, executor, transaction, dialect)?),
        },
        SqlCatalogBoundExpression::Unary { operator, expression } => {
            SqlCatalogBoundExpression::Unary { operator, expression: Box::new(lower_expression(*expression, executor, transaction, dialect)?) }
        }
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
            if database_query::sql_planner::plan_reads_outer(&plan) {
                return Ok(SqlCatalogBoundExpression::InSubquery { expression, plan: Box::new(plan), negated });
            }
            let result = execute_plan(executor, transaction, &plan, dialect)?;
            if result.columns.len() != 1 {
                return Err(SqlFrontendError::new(SqlErrorKind::OperandColumns, "IN subquery must select exactly one column"));
            }
            let values =
                result.rows.into_iter().map(|row| row.into_iter().next().map(value_to_literal).unwrap_or(SqlBoundLiteral::Null)).collect();
            SqlCatalogBoundExpression::InValues { expression, values, negated }
        }
        SqlCatalogBoundExpression::ScalarSubquery { plan } => {
            let mut plan = *plan;
            resolve_subqueries(&mut plan, executor, transaction, dialect)?;
            // 相关标量子查询逐行重算，基数和多列语义在替换外层值后的物化里照旧检查。
            if database_query::sql_planner::plan_reads_outer(&plan) {
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
                _ => return Err(SqlFrontendError::new(SqlErrorKind::CardinalityViolation, "scalar subquery returned more than one row")),
            }
        }
        SqlCatalogBoundExpression::Exists { plan, negated } => {
            let mut plan = *plan;
            resolve_subqueries(&mut plan, executor, transaction, dialect)?;
            // 相关 `EXISTS` 逐行重算，存在性判定仍在逐行求值里完成。
            if database_query::sql_planner::plan_reads_outer(&plan) {
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
    let rows: Vec<Vec<SqlValue>> = rows.into_iter().skip(plan.offset_after_sort).take(if limit == 0 { usize::MAX } else { limit }).collect();
    Ok(SqlResult { columns, rows, affected_rows: 0 })
}

/// 按一个集合运算符合并两个已执行的结果集，语义完全等同于对应的二元 SQL 运算。
///
/// `INTERSECT ALL` / `EXCEPT ALL` 按重数（多重集）合并：每遇到一行右侧出现，
/// 就消耗一次右侧计数，而不是整行去重。实测口径（DuckDB 2.0；SQLite 3.49 对
/// 这两种 `ALL` 拼写直接报语法错误）：左 `{1,1,1,2,2,3}`、右 `{1,1,2,2,2,4}`
/// 时，`INTERSECT ALL` 为 `1,1,2,2`（各取较小重数），`EXCEPT ALL` 为 `1,3`
/// （按次数相减）。
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
        SqlBoundSetOperator::IntersectAll => {
            let mut remaining = right;
            let mut rows = Vec::new();
            for row in left {
                if let Some(index) = remaining.iter().position(|candidate| projected_rows_equal(candidate, &row)) {
                    remaining.swap_remove(index);
                    rows.push(row);
                }
            }
            rows
        }
        SqlBoundSetOperator::Except => {
            let mut rows = left;
            retain_distinct_rows(&mut rows);
            rows.retain(|row| !right.iter().any(|candidate| projected_rows_equal(candidate, row)));
            rows
        }
        SqlBoundSetOperator::ExceptAll => {
            let mut remaining = right;
            let mut rows = Vec::new();
            for row in left {
                match remaining.iter().position(|candidate| projected_rows_equal(candidate, &row)) {
                    Some(index) => {
                        remaining.swap_remove(index);
                    }
                    None => rows.push(row),
                }
            }
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
fn write_error(message: String, error: &database_types::cluster::Error) -> SqlFrontendError {
    match error {
        database_types::cluster::Error::CasConflict { .. } => SqlFrontendError::new(SqlErrorKind::Conflict, message),
        _ => SqlFrontendError::internal(message),
    }
}
