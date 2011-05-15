//! Correlated-subquery lowering: outer-column substitution and HAVING-case evaluation.

use super::*;

/// 在给定行上求值一个可能读取外层列的表达式。
///
/// 不含相关子查询时按普通逐行求值；含相关子查询时，先把该行的外层列值代入子查询计划，
/// 再按普通流程物化并求值。外层每行因此各自重跑一次子查询，不复用其他行的结果。
pub(crate) fn eval_row_expression(
    expr: &SqlCatalogBoundExpression,
    row: &RowFieldValues,
    field_types: &std::collections::BTreeMap<u64, database_query::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
) -> Result<SqlValue, SqlFrontendError> {
    if !database_query::sql_planner::expression_reads_outer(expr) {
        return eval_expression(expr, row, field_types, dialect);
    }
    let substituted = substitute_outer(expr.clone(), row);
    let lowered = lower_expression(substituted, executor, transaction, dialect)?;
    eval_expression(&lowered, row, field_types, dialect)
}

/// 把表达式里的外层列引用替换成该行的字面量值，包括子查询计划内部的外层引用。
///
/// 替换之后子查询不再读外层列，`lower_expression` 就可以按原路径把它物化。
pub(crate) fn substitute_outer(expr: SqlCatalogBoundExpression, row: &RowFieldValues) -> SqlCatalogBoundExpression {
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
        InSubquery { expression, plan, negated } => {
            InSubquery { expression: Box::new(substitute_outer(*expression, row)), plan: Box::new(substitute_outer_plan(*plan, row)), negated }
        }
        InValues { expression, values, negated } => InValues { expression: Box::new(substitute_outer(*expression, row)), values, negated },
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
pub(crate) fn substitute_outer_plan(plan: SqlExecutionPlan, row: &RowFieldValues) -> SqlExecutionPlan {
    let substitute = |expression: SqlCatalogBoundExpression| substitute_outer(expression, row);
    let substitute_projections = |expressions: Vec<(SqlCatalogBoundExpression, String)>| -> Vec<(SqlCatalogBoundExpression, String)> {
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

pub(crate) fn substitute_outer_order_by(items: Vec<SqlCatalogBoundOrderByItem>, row: &RowFieldValues) -> Vec<SqlCatalogBoundOrderByItem> {
    items
        .into_iter()
        .map(|item| SqlCatalogBoundOrderByItem {
            expression: substitute_outer(item.expression, row),
            direction: item.direction,
            nulls: item.nulls,
        })
        .collect()
}

pub(crate) fn substitute_outer_order_by_option(
    items: Option<Vec<SqlCatalogBoundOrderByItem>>,
    row: &RowFieldValues,
) -> Option<Vec<SqlCatalogBoundOrderByItem>> {
    items.map(|items| substitute_outer_order_by(items, row))
}

/// 外层列在该行上的取值；字段缺失按 SQL NULL 处理。
pub(crate) fn outer_literal(column: &SqlCatalogOuterColumn, row: &RowFieldValues) -> SqlBoundLiteral {
    match row.get(column.column.row_id) {
        Some(bytes) => value_to_literal(SqlValue::from_row_bytes_typed(bytes, &column.field_type)),
        None => SqlBoundLiteral::Null,
    }
}

pub(crate) fn eval_case_having(
    branches: &[(Box<SqlCatalogBoundExpression>, Box<SqlCatalogBoundExpression>)],
    else_branch: &Option<Box<SqlCatalogBoundExpression>>,
    bucket_rows: &[RowFieldValues],
    group_exprs: &[SqlCatalogBoundExpression],
    field_types: &std::collections::BTreeMap<u64, database_query::catalog_bind::SqlFieldType>,
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
