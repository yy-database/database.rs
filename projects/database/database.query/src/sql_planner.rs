//! SQL → 集群 KV 计划器。
//!
//! 本模块负责把 catalog-bound 的 `SELECT` 请求翻译为一份可以直接喂给
//! `KeyValueExecutor` 的执行计划。计划本身不做读/写；它只决定：
//!
//! 1. **namespace** — 目标表的字符串标签，形如 `table:{type_id}`。
//! 2. **rowid / rowid_prefix / rowid_after / rowid_before** — 主键查询参数
//!    （WHERE 里 `pk = literal` / `pk > X` / `pk LIKE 'prefix%'` 等）。
//! 3. **projection fields** — 投影需要的字段 ID 列表，`*` 会在此时展开。
//! 4. **remaining predicate** — 从 WHERE 里剥掉 pk 条件后剩下的表达式，
//!    拿到 row 后再做一次过滤。
//!
//! SELECT 生成两条执行路径：
//! - `SqlExecutionPlan::Read` — 有 `pk = literal`，直接单点 Get
//! - `SqlExecutionPlan::Scan` — 其余情况（pk LIKE 前缀、pk 范围、无 pk 条件等），
//!   走 `KeyValueExecutor::execute_scan`

use crate::{
    catalog_bind::{
        SqlCatalog, SqlCatalogBoundColumn, SqlCatalogBoundConflict, SqlCatalogBoundConflictAction, SqlCatalogBoundDelete,
        SqlCatalogBoundExpression, SqlCatalogBoundInsert, SqlCatalogBoundOrderByItem, SqlCatalogBoundProjection, SqlCatalogBoundSelect,
        SqlCatalogBoundSetOperand, SqlCatalogBoundSetOperation, SqlCatalogBoundStatement, SqlCatalogBoundTable, SqlCatalogBoundUpdate,
        SqlCatalogOuterColumn, SqlFieldType, collect_column_ids, reject_unknown_field,
    },
    sql::{
        SqlAggregateFunction, SqlBoundBinaryOperator, SqlBoundExpression, SqlBoundLiteral, SqlBoundOrderByItem, SqlBoundOrderDirection,
        SqlBoundSetOperator, SqlBoundUnaryOperator, SqlDialect, SqlErrorKind, SqlFrontendError, SqlNullsOrder, SqlScalarFunction,
    },
    sql_execute::{SqlValue, eval_constant},
};

/// 翻译一份 catalog-bound statement 为可执行计划（PostgreSQL 排序规则）。
pub fn plan_statement(statement: SqlCatalogBoundStatement, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    plan_statement_for_dialect(statement, catalog, SqlDialect::Postgres)
}

/// 翻译一份 catalog-bound statement 为可执行计划，NULL 排序规则由 `dialect` 决定。
pub fn plan_statement_for_dialect(
    statement: SqlCatalogBoundStatement,
    catalog: &SqlCatalog,
    dialect: SqlDialect,
) -> Result<SqlExecutionPlan, SqlFrontendError> {
    match statement {
        SqlCatalogBoundStatement::Select(select) => plan_select_for_dialect(select, catalog, dialect),
        SqlCatalogBoundStatement::Insert(insert) => plan_insert(insert, catalog, dialect),
        SqlCatalogBoundStatement::Update(update) => plan_update(update, catalog, dialect),
        SqlCatalogBoundStatement::Delete(delete) => plan_delete(delete, catalog),
        SqlCatalogBoundStatement::SetOperation(set) => plan_set_operation(set, catalog, dialect),
    }
}

/// Parse the raw `DEFAULT` literal text stored by the DDL parser into a constant
/// value. Non-constant defaults such as `CURRENT_TIMESTAMP` return `None`.
fn parse_default_value(text: &str) -> Option<DefaultValue> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if text.eq_ignore_ascii_case("null") {
        return Some(DefaultValue::Null);
    }
    if text.eq_ignore_ascii_case("true") {
        return Some(DefaultValue::Boolean(true));
    }
    if text.eq_ignore_ascii_case("false") {
        return Some(DefaultValue::Boolean(false));
    }
    if let Some(inner) = text.strip_prefix('\'').and_then(|rest| rest.strip_suffix('\'')) {
        return Some(DefaultValue::String(inner.to_owned()));
    }
    if let Ok(value) = text.parse::<i64>() {
        return Some(DefaultValue::Integer(value));
    }
    text.parse::<f64>().ok().map(|value| DefaultValue::NumberBits(value.to_bits()))
}

/// 计划一个 SELECT（PostgreSQL 排序规则）。
pub fn plan_select(select: SqlCatalogBoundSelect, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    plan_select_for_dialect(select, catalog, SqlDialect::Postgres)
}

/// 计划一个 SELECT，NULL 排序规则由 `dialect` 决定。
pub fn plan_select_for_dialect(
    select: SqlCatalogBoundSelect,
    catalog: &SqlCatalog,
    dialect: SqlDialect,
) -> Result<SqlExecutionPlan, SqlFrontendError> {
    // `LIMIT` / `OFFSET` 是表达式：在计划阶段按方言折叠成行数，后续所有分支只处理 usize。
    let limit = plan_row_count(select.limit.as_ref(), "LIMIT", dialect)?;
    let offset = plan_row_count(select.offset.as_ref(), "OFFSET", dialect)?;
    validate_distinct_shape(&select)?;
    if !select.joins.is_empty() {
        return plan_join_select(select, limit, offset, catalog, dialect);
    }
    // 无 FROM 的 SELECT 只做常量 / 内建函数求值（驱动探测 VERSION()、SELECT 1 等）。
    let Some(source) = select.source.as_ref()
    else {
        return plan_source_less_select(select, limit, offset);
    };
    let table = find_table(catalog, source.type_id)?;
    let namespace = format!("table:{}", source.type_id);
    let pk_field = primary_key_of(table)?;

    // 1. 从投影里收集字段，同时把 Star 展开。
    let mut projection_fields: Vec<(u64, String)> = Vec::new();
    let mut projection_expressions: Vec<(SqlCatalogBoundExpression, String)> = Vec::new();
    let mut has_aggregate = false;
    let mut has_non_aggregate = false;
    let mut has_window = false;
    for projection in &select.projections {
        match projection {
            SqlCatalogBoundProjection::Star(star_table) => {
                has_non_aggregate = true;
                let star_table = find_table(catalog, star_table.type_id)?;
                for (index, field) in star_table.fields.iter().enumerate() {
                    reject_unknown_field(field)?;
                    let name = source.visible_column_name(index, field);
                    projection_fields.push((source.row_id(field.field_id), name));
                }
            }
            SqlCatalogBoundProjection::Expression { expression, alias } => {
                if expression_has_window(expression) && !is_window_call(expression) {
                    return Err(planner_error("SQL window function must be the entire projection expression, not part of a larger one"));
                }
                match expression {
                    SqlCatalogBoundExpression::Column(col) => {
                        has_non_aggregate = true;
                        projection_fields.push((col.row_id, alias.clone().unwrap_or_else(|| col.visible_name.clone())));
                    }
                    SqlCatalogBoundExpression::Function { window: Some(_), .. } => {
                        has_window = true;
                        projection_expressions.push((expression.clone(), alias.clone().unwrap_or_else(|| scalar_projection_label(expression))));
                    }
                    // 只有真正含聚合的调用走聚合路径；纯标量调用（含嵌套标量实参）
                    // 仍按行求值，否则 `SELECT COALESCE(LOWER(tag), name)` 会被当成
                    // 聚合投影并在执行期报未知聚合函数。
                    SqlCatalogBoundExpression::Function { .. } if expression_has_aggregate(expression) => {
                        has_aggregate = true;
                        projection_expressions
                            .push((expression.clone(), alias.clone().unwrap_or_else(|| expression_default_label(expression))));
                    }
                    other => {
                        has_non_aggregate = true;
                        projection_expressions.push((other.clone(), alias.clone().unwrap_or_else(|| expression_default_label(other))));
                    }
                }
            }
        }
    }

    // 窗口函数只能出现在投影里，也不能与 GROUP BY / 聚合投影混用。
    validate_window_placement(&select)?;
    if has_window && has_aggregate {
        return Err(planner_error("cannot mix window and aggregate projections in the same SELECT"));
    }
    if has_window && select.group_by.is_some() {
        return Err(planner_error("SQL window functions cannot be combined with GROUP BY"));
    }

    // 混合聚合 + 非聚合（无 GROUP BY）→ 拒绝；有 GROUP BY 时由分组校验约束非聚合列。
    if has_aggregate && has_non_aggregate && select.group_by.is_none() {
        return Err(planner_error("cannot mix aggregate and non-aggregate projections without GROUP BY"));
    }
    validate_grouped_select(&select, has_aggregate)?;
    validate_correlated_placement(&select, has_aggregate, has_window)?;
    if select.distinct {
        validate_distinct_order_by(select.order_by.as_ref(), &projection_fields, &projection_expressions)?;
    }

    // 2. 递归解析 predicate 的 AND 树，把 pk 条件提取出来。
    let (pk_equality, pk_prefix, pk_after, pk_before, remaining_predicate) = match select.predicate {
        None => (None, None, None, None, None),
        Some(expr) => extract_pk_range(expr, source.row_id(pk_field.field_id))?,
    };

    // 3. 分流：有精确 pk 等值 → Read；其他全部 → Scan。
    // 先从 catalog table 收集 row_id → SqlFieldType 映射。
    let field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType> =
        table.fields.iter().map(|f| (source.row_id(f.field_id), f.field_type.clone())).collect();
    // Rows written before `ALTER TABLE ... ADD COLUMN ... DEFAULT` are shorter
    // than the current catalog, so the read path fills those columns from the
    // declared default instead of reporting NULL.
    let field_defaults: std::collections::BTreeMap<u64, DefaultValue> = table
        .fields
        .iter()
        .filter_map(|field| field.default.as_deref().and_then(parse_default_value).map(|value| (source.row_id(field.field_id), value)))
        .collect();

    if let Some(lit) = pk_equality {
        // 窗口函数需要看到分区里的其余行，单点读无法求值，必须退回扫描。
        if !has_window {
            let rowid = pk_literal_to_rowid(&lit)?;
            return Ok(SqlExecutionPlan::Read(ReadPlan {
                namespace,
                rowid,
                projection_fields,
                projection_expressions,
                remaining_predicate,
                // `LIMIT 0` 按 SQL 语义返回空结果集，单点读同样不能绕过；`OFFSET >= 1`
                // 会跳过这唯一一行，因此也必须在单点读这一层就抑制。
                suppress_rows: limit == Some(0) || offset.is_some_and(|value| value > 0),
                field_types: field_types.clone(),
                field_defaults,
            }));
        }
    }

    // Scan 路径：prefix / after / before 都是 bytes。
    // ORDER BY / GROUP BY / 聚合 / remaining_predicate 时 kv-scan 不设 limit
    // —— 先 filter/aggregate/sort/group 才能知道前 N 行是哪些。
    // OFFSET 同样不能下推：kv-scan 只能「取前 N 行」，跳过的行仍要真实扫描，
    // 所以有 OFFSET 时改为扫全量，再由 execute 层先 skip 再 take。
    let has_order_by = select.order_by.is_some();
    let has_group_by = select.group_by.is_some();
    let has_predicate = remaining_predicate.is_some();
    let has_offset = offset.unwrap_or(0) > 0;
    let needs_post_filter_limit = has_order_by || has_group_by || has_aggregate || has_predicate || has_window || has_offset;
    let kv_limit = if needs_post_filter_limit { 0 } else { limit.unwrap_or(0) };
    let limit_after_sort = if needs_post_filter_limit { limit.unwrap_or(0) } else { 0 };
    let offset_after_sort = if needs_post_filter_limit { offset.unwrap_or(0) } else { 0 };

    Ok(SqlExecutionPlan::Scan(ScanPlan {
        namespace,
        rowid_prefix: pk_prefix.unwrap_or_default(),
        rowid_after: pk_after,
        rowid_before: pk_before,
        limit: kv_limit,
        projection_fields,
        projection_expressions,
        remaining_predicate,
        group_by: select.group_by,
        having: select.having,
        order_by: select.order_by,
        limit_after_sort,
        offset_after_sort,
        nulls_first: dialect.nulls_sort_first(),
        suppress_rows: limit == Some(0),
        has_aggregate,
        has_window,
        distinct: select.distinct,
        field_types,
        field_defaults,
    }))
}

/// 把 `LIMIT` / `OFFSET` 表达式折叠成计划里的行数。
///
/// SQL 要求这里是非负整数标量。子句在 bind 阶段已经是常量表达式，因此用执行层
/// 同一个求值器按当前方言求值：参数占位符已由参数层渲染成字面量，算术遵循方言
/// 语义，`1/0` 这类错误保留自己的类别而不是被折成一个错误行数。
///
/// NULL、负数和非整数值都显式拒绝。它们不会被改写成「不限行」或「不跳过」，
/// 因为那种改写会悄悄改变结果集。
fn plan_row_count(clause: Option<&SqlCatalogBoundExpression>, keyword: &str, dialect: SqlDialect) -> Result<Option<usize>, SqlFrontendError> {
    let Some(expression) = clause
    else {
        return Ok(None);
    };
    match eval_constant(expression, dialect)? {
        SqlValue::Integer(count) if count < 0 => {
            Err(SqlFrontendError::new(SqlErrorKind::NumericOverflow, format!("SQL {keyword} must not be negative, got {count}")))
        }
        SqlValue::Integer(count) => usize::try_from(count).map(Some).map_err(|_| {
            SqlFrontendError::new(SqlErrorKind::NumericOverflow, format!("SQL {keyword} count {count} exceeds the addressable row range"))
        }),
        SqlValue::Null => {
            Err(SqlFrontendError::type_mismatch(format!("SQL {keyword} must be a non-negative integer, NULL is not a row count")))
        }
        SqlValue::Number(value) => Err(SqlFrontendError::type_mismatch(format!(
            "SQL {keyword} must be an exact non-negative integer, got the numeric value `{value}`"
        ))),
        SqlValue::String(_) => {
            Err(SqlFrontendError::type_mismatch(format!("SQL {keyword} must be an exact non-negative integer, got a text value")))
        }
        SqlValue::Bytes(_) => {
            Err(SqlFrontendError::type_mismatch(format!("SQL {keyword} must be an exact non-negative integer, got a binary value")))
        }
        SqlValue::Boolean(_) => {
            Err(SqlFrontendError::type_mismatch(format!("SQL {keyword} must be an exact non-negative integer, got a boolean value")))
        }
    }
}

/// 计划一个集合运算。每个操作数各自完整计划，合并语义与结果集
/// `ORDER BY` / `LIMIT` / `OFFSET` 由执行层在整个层级上施加一次。
///
/// 相邻两操作数的列数与列类型必须匹配：列数不符按语句错误拒绝，类型不符按类型错误拒绝，
/// 都带方言映射后的错误码，不做隐式转换或截断。校验按合并顺序逐对进行，
/// 因此一条链上任意相邻边界不兼容都会被指出，而不只是最左一对。
fn plan_set_operation(
    set: SqlCatalogBoundSetOperation,
    catalog: &SqlCatalog,
    dialect: SqlDialect,
) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let SqlCatalogBoundSetOperation { operands, operators, order_by, limit, offset } = set;
    if operands.len() < 2 || operators.len() + 1 != operands.len() {
        return Err(SqlFrontendError::syntax("set operation operands and operators do not line up"));
    }
    let limit = plan_row_count(limit.as_ref(), "LIMIT", dialect)?;
    let offset = plan_row_count(offset.as_ref(), "OFFSET", dialect)?;
    let mut planned: Vec<SqlExecutionPlan> = Vec::with_capacity(operands.len());
    for operand in operands {
        planned.push(plan_set_operand(operand, catalog, dialect)?);
    }
    let shapes: Vec<Vec<crate::sql_describe::SqlColumn>> = planned.iter().map(crate::sql_describe::describe_plan).collect();
    for (index, pair) in shapes.windows(2).enumerate() {
        let (left_columns, right_columns) = (&pair[0], &pair[1]);
        if left_columns.len() != right_columns.len() {
            return Err(SqlFrontendError::syntax(format!(
                "set operation column count mismatch at operand {}: it selects {} columns, operand {} selects {}",
                index + 1,
                left_columns.len(),
                index + 2,
                right_columns.len()
            )));
        }
        for (column, (left_column, right_column)) in left_columns.iter().zip(right_columns.iter()).enumerate() {
            let left_type = left_column.field_type.inner();
            let right_type = right_column.field_type.inner();
            if left_type == SqlFieldType::Unknown || right_type == SqlFieldType::Unknown || left_type == right_type {
                continue;
            }
            return Err(SqlFrontendError::type_mismatch(format!(
                "set operation column {} type mismatch at operands {}/{}: `{}` is {left_type:?}, `{}` is {right_type:?}",
                column + 1,
                index + 1,
                index + 2,
                left_column.name,
                right_column.name
            )));
        }
    }
    // 结果列名由最左操作数定义，`ORDER BY` 也按它的形状解析。
    let order_by = resolve_set_operation_order_by(order_by, &shapes[0])?;
    Ok(SqlExecutionPlan::SetOperation(SetOperationPlan {
        operands: planned,
        operators,
        order_by,
        limit_after_sort: limit.unwrap_or(0),
        offset_after_sort: offset.unwrap_or(0),
        nulls_first: dialect.nulls_sort_first(),
        suppress_rows: limit == Some(0),
    }))
}

/// 计划集合运算的一个操作数：叶子走完整 SELECT 计划，`INTERSECT` 组递归计划。
fn plan_set_operand(
    operand: SqlCatalogBoundSetOperand,
    catalog: &SqlCatalog,
    dialect: SqlDialect,
) -> Result<SqlExecutionPlan, SqlFrontendError> {
    match operand {
        SqlCatalogBoundSetOperand::Select(select) => plan_select_for_dialect(*select, catalog, dialect),
        SqlCatalogBoundSetOperand::SetOperation(set) => plan_set_operation(*set, catalog, dialect),
    }
}

/// 把集合运算的 `ORDER BY` 解析成合并结果上的列序号。SQL 只允许在这里引用结果列名
/// 或序号，任意表达式都没有可以求值的行上下文，因此显式拒绝。
fn resolve_set_operation_order_by(
    order_by: Option<Vec<SqlBoundOrderByItem>>,
    columns: &[crate::sql_describe::SqlColumn],
) -> Result<Option<Vec<SqlSetOrderByItem>>, SqlFrontendError> {
    let Some(items) = order_by
    else {
        return Ok(None);
    };
    let mut resolved = Vec::with_capacity(items.len());
    for item in items {
        let column = match &item.expr {
            SqlBoundExpression::Identifier(name) => columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| planner_error(&format!("ORDER BY `{name}` is not a column of the set operation result")))?,
            SqlBoundExpression::Literal(SqlBoundLiteral::Integer(position)) => {
                let position = usize::try_from(*position).ok().filter(|value| *value >= 1 && *value <= columns.len());
                let Some(position) = position
                else {
                    return Err(planner_error("ORDER BY ordinal position is outside the set operation result"));
                };
                position - 1
            }
            _ => {
                return Err(planner_error("set operation ORDER BY accepts result column names and ordinal positions only"));
            }
        };
        resolved.push(SqlSetOrderByItem { column, direction: item.direction, nulls: item.nulls });
    }
    Ok(Some(resolved))
}

/// 计划一个带 JOIN 的 SELECT。
///
/// 基表与每张连接表各自全表扫描，按 JOIN 顺序做嵌套循环合并（INNER / LEFT / RIGHT / FULL），
/// 再套用 WHERE / 投影 / ORDER BY / LIMIT。主键下推、聚合暂不在网关子集内。
fn plan_join_select(
    select: SqlCatalogBoundSelect,
    limit: Option<usize>,
    offset: Option<usize>,
    catalog: &SqlCatalog,
    dialect: SqlDialect,
) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let SqlCatalogBoundSelect { source, joins, projections, predicate, group_by, having, order_by, distinct, .. } = select;
    let base = source.ok_or_else(|| planner_error("SQL JOIN requires a FROM table"))?;
    if group_by.is_some() || having.is_some() {
        return Err(planner_error("SQL JOIN with GROUP BY / HAVING is not in the gateway subset yet"));
    }
    // 窗口函数需要在单表行集合上分区排序，JOIN 合并后没有对应的 lowering。
    let has_window = projections.iter().any(|projection| match projection {
        SqlCatalogBoundProjection::Expression { expression, .. } => expression_has_window(expression),
        SqlCatalogBoundProjection::Star(_) => false,
    }) || predicate.as_ref().is_some_and(expression_has_window)
        || order_by.as_ref().is_some_and(|items| items.iter().any(|item| expression_has_window(&item.expression)));
    if has_window {
        return Err(planner_error("SQL window functions are not supported in JOIN queries"));
    }

    // 1. 投影展开：Star 按来源顺序展开整表，Column 直接映射，其余表达式保留。
    let mut projection_fields: Vec<(u64, String)> = Vec::new();
    let mut projection_expressions: Vec<(SqlCatalogBoundExpression, String)> = Vec::new();
    for projection in projections {
        match projection {
            SqlCatalogBoundProjection::Star(table) => {
                let catalog_table = find_table(catalog, table.type_id)?;
                for (index, field) in catalog_table.fields.iter().enumerate() {
                    reject_unknown_field(field)?;
                    let name = table.visible_column_name(index, field);
                    projection_fields.push((table.row_id(field.field_id), name));
                }
            }
            SqlCatalogBoundProjection::Expression { expression, alias } => match expression {
                SqlCatalogBoundExpression::Column(col) => {
                    projection_fields.push((col.row_id, alias.unwrap_or_else(|| col.visible_name.clone())));
                }
                other => {
                    if expression_has_aggregate(&other) {
                        return Err(planner_error("SQL JOIN with aggregate projections is not in the gateway subset yet"));
                    }
                    let label = alias.unwrap_or_else(|| expression_default_label(&other));
                    projection_expressions.push((other, label));
                }
            },
        }
    }

    if distinct {
        validate_distinct_order_by(order_by.as_ref(), &projection_fields, &projection_expressions)?;
    }

    // 2. 需要解码的字段集合：投影 + WHERE + ORDER BY + 每个 ON 条件。
    let mut needed: Vec<u64> = Vec::new();
    for (field_id, _) in &projection_fields {
        if !needed.contains(field_id) {
            needed.push(*field_id);
        }
    }
    for (expr, _) in &projection_expressions {
        collect_column_ids(expr, &mut needed);
    }
    if let Some(predicate) = &predicate {
        collect_column_ids(predicate, &mut needed);
    }
    if let Some(items) = &order_by {
        for item in items {
            collect_column_ids(&item.expression, &mut needed);
        }
    }
    for join in &joins {
        collect_column_ids(&join.on, &mut needed);
    }

    // 3. 每个来源只解码自己表的字段。
    let mut field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType> = std::collections::BTreeMap::new();
    let mut field_defaults: std::collections::BTreeMap<u64, DefaultValue> = std::collections::BTreeMap::new();
    let base =
        join_source_plan(base, None, crate::sql::SqlBoundJoinKind::Inner, catalog, &needed, &mut field_types, &mut field_defaults)?;
    let mut join_sources = Vec::with_capacity(joins.len());
    for join in joins {
        join_sources.push(join_source_plan(
            join.table,
            Some(join.on),
            join.kind,
            catalog,
            &needed,
            &mut field_types,
            &mut field_defaults,
        )?);
    }

    Ok(SqlExecutionPlan::Join(JoinPlan {
        base,
        joins: join_sources,
        projection_fields,
        projection_expressions,
        remaining_predicate: predicate,
        order_by,
        limit_after_sort: limit.unwrap_or(0),
        offset_after_sort: offset.unwrap_or(0),
        nulls_first: dialect.nulls_sort_first(),
        suppress_rows: limit == Some(0),
        distinct,
        field_types,
        field_defaults,
    }))
}

/// 构造一个 JOIN 数据来源计划：记录 namespace、ON 条件与该表需要解码的字段。
///
/// `needed` 收集的是 row id。每个来源把自己的真实 `field_id` 解码出来，再按
/// occurrence slot 映射成 row id，供合并行与表达式求值使用。
fn join_source_plan(
    table: SqlCatalogBoundTable,
    on: Option<SqlCatalogBoundExpression>,
    kind: crate::sql::SqlBoundJoinKind,
    catalog: &SqlCatalog,
    needed: &[u64],
    field_types: &mut std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    field_defaults: &mut std::collections::BTreeMap<u64, DefaultValue>,
) -> Result<JoinSourcePlan, SqlFrontendError> {
    let catalog_table = find_table(catalog, table.type_id)?;
    let mut needed_field_ids = Vec::new();
    let mut row_ids = Vec::new();
    for field in &catalog_table.fields {
        let row_id = table.row_id(field.field_id);
        field_types.insert(row_id, field.field_type.clone());
        if let Some(value) = field.default.as_deref().and_then(parse_default_value) {
            field_defaults.insert(row_id, value);
        }
        if needed.contains(&row_id) {
            needed_field_ids.push(field.field_id);
            row_ids.push(row_id);
        }
    }
    Ok(JoinSourcePlan { namespace: format!("table:{}", catalog_table.type_id), needed_field_ids, row_ids, on, kind })
}

/// 计划一个没有 FROM 的 SELECT：只允许常量与内建函数，不访问任何 namespace。
fn plan_source_less_select(
    select: SqlCatalogBoundSelect,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<SqlExecutionPlan, SqlFrontendError> {
    if select.predicate.is_some() {
        return Err(planner_error("SQL WHERE requires a FROM source"));
    }
    if select.group_by.is_some() {
        return Err(planner_error("SQL GROUP BY requires a FROM source"));
    }
    if select.having.is_some() {
        return Err(planner_error("SQL HAVING requires a FROM source"));
    }
    let mut projections: Vec<(SqlCatalogBoundExpression, String)> = Vec::with_capacity(select.projections.len());
    for projection in select.projections {
        match projection {
            SqlCatalogBoundProjection::Star(_) => {
                return Err(planner_error("SQL star projection requires a catalog table source"));
            }
            SqlCatalogBoundProjection::Expression { expression, alias } => {
                if expression_has_window(&expression) {
                    return Err(planner_error("SQL window functions require a FROM source"));
                }
                let label = alias.unwrap_or_else(|| scalar_projection_label(&expression));
                projections.push((expression, label));
            }
        }
    }
    if projections.is_empty() {
        return Err(planner_error("SQL SELECT without FROM requires at least one projection"));
    }
    // 无 FROM 的 SELECT 最多产出一行：`LIMIT 0` 或非零 `OFFSET` 都意味着空结果集。
    let suppress_row = limit == Some(0) || offset.is_some_and(|value| value > 0);
    Ok(SqlExecutionPlan::Scalar(ScalarPlan { projections, suppress_row }))
}

/// 计划一个 INSERT。每行 VALUES 要求第一个值是主键 literal。多行 VALUES 会产出
/// `SqlExecutionPlan::MultiInsert`，单行保持 `SqlExecutionPlan::Insert`。
///
/// `INSERT ... SELECT` 先按既有 SELECT 计划执行源查询，再在 planner 层对齐目标列。
pub fn plan_insert(insert: SqlCatalogBoundInsert, catalog: &SqlCatalog, dialect: SqlDialect) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let table = find_table(catalog, insert.table.type_id)?;
    let namespace = format!("table:{}", table.type_id);
    let field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType> =
        table.fields.iter().map(|f| (f.field_id, f.field_type.clone())).collect();

    let SqlCatalogBoundInsert { table: _, fields: target_fields, rows, select, conflict, returning } = insert;
    let conflict = plan_on_conflict(conflict, table, dialect)?;
    let returning = plan_returning(returning, table, dialect)?;
    let unique_constraints = unique_constraints_of(table);
    if let Some(select) = select {
        return plan_insert_select(*select, target_fields, namespace, field_types, unique_constraints, catalog, dialect, conflict, returning);
    }

    let mut plans = Vec::with_capacity(rows.len());
    let pk_field = primary_key_of(table)?;
    let pk_index = target_fields
        .iter()
        .position(|field| field.field_id == pk_field.field_id)
        .ok_or_else(|| planner_error("INSERT must provide the table primary key column"))?;
    for row_values in rows {
        // 行以主键字面量为 KV key。主键列未必是第一个目标列（例如省略
        // `BIGSERIAL` 主键、由 registry 追加在末尾），所以按 field_id 定位，
        // 不能依赖位置。
        let pk_value = row_values.get(pk_index).ok_or_else(|| planner_error("INSERT row has no primary key value"))?;
        let rowid = literal_from_expression(pk_value)?;
        let rowid_bytes = pk_literal_to_rowid(&rowid)?;

        let mut write_fields = Vec::with_capacity(target_fields.len());
        for (field, value) in target_fields.iter().zip(row_values.iter()) {
            let lit = literal_from_expression(value)?;
            let field_type = field_types.get(&field.field_id).cloned().unwrap_or(crate::catalog_bind::SqlFieldType::Unknown);
            check_write_literal(&lit, &field_type, &field.field_name, dialect, field.capacity)?;
            write_fields.push(WriteField { field_id: field.field_id, value: lit, field_type, capacity: field.capacity });
        }
        plans.push(WritePlan {
            namespace: namespace.clone(),
            rowid: rowid_bytes,
            fields: write_fields,
            field_types: field_types.clone(),
            conflict: conflict.clone(),
            returning: returning.clone(),
            unique_constraints: unique_constraints.clone(),
        });
    }
    if plans.len() == 1 { Ok(SqlExecutionPlan::Insert(plans.into_iter().next().unwrap())) } else { Ok(SqlExecutionPlan::MultiInsert(plans)) }
}

/// 计划 `ON CONFLICT`：只有主键这一条唯一约束，目标列必须与之相符。
///
/// MySQL 没有 `ON CONFLICT` 语法，同一语句在该方言下按语法错误拒绝。
fn plan_on_conflict(
    conflict: Option<SqlCatalogBoundConflict>,
    table: &crate::SqlCatalogTable,
    dialect: SqlDialect,
) -> Result<Option<ConflictPolicy>, SqlFrontendError> {
    let Some(conflict) = conflict
    else {
        return Ok(None);
    };
    if dialect != SqlDialect::Postgres {
        return Err(SqlFrontendError::syntax("ON CONFLICT is only supported by the PostgreSQL dialect"));
    }
    // PostgreSQL 只允许 `DO NOTHING` 省略冲突目标，`DO UPDATE` 必须先声明目标。
    if conflict.columns.is_empty() {
        if matches!(conflict.action, SqlCatalogBoundConflictAction::DoUpdate { .. }) {
            return Err(SqlFrontendError::syntax("ON CONFLICT DO UPDATE requires an explicit conflict target"));
        }
        return Ok(Some(ConflictPolicy::DoNothing));
    }
    let pk = primary_key_of(table)?;
    if conflict.columns.as_slice() != [pk.name.as_str()] {
        return Err(SqlFrontendError::unsupported(format!(
            "ON CONFLICT target does not match a unique constraint: {}",
            conflict.columns.join(", ")
        )));
    }
    match conflict.action {
        SqlCatalogBoundConflictAction::DoNothing => Ok(Some(ConflictPolicy::DoNothing)),
        SqlCatalogBoundConflictAction::DoUpdate { assignments, predicate } => {
            let mut planned = Vec::with_capacity(assignments.len());
            for (column, expression) in assignments {
                // 行以主键为 KV key，改写主键等于「删除 + 插入」，网关显式拒绝而不是近似执行。
                if column.field_id == pk.field_id {
                    return Err(SqlFrontendError::unsupported("ON CONFLICT DO UPDATE cannot reassign the table primary key"));
                }
                let field = table.fields.iter().find(|field| field.field_id == column.field_id);
                let field_type = field.map(|field| field.field_type.clone()).unwrap_or(SqlFieldType::Unknown);
                let capacity = field.and_then(|field| field.capacity);
                check_conflict_value(&expression)?;
                // 字面量在计划期就能按列类型判定，引用列的形态要等实际值算出来。
                if let SqlCatalogBoundExpression::Literal(lit) = &expression {
                    check_write_literal(lit, &field_type, &column.field_name, dialect, capacity)?;
                }
                planned.push(ConflictAssignment {
                    field_id: column.field_id,
                    field_name: column.field_name,
                    field_type,
                    capacity,
                    value: expression,
                });
            }
            Ok(Some(ConflictPolicy::DoUpdate { assignments: planned, predicate }))
        }
    }
}

/// Restricts `ON CONFLICT DO UPDATE SET` values to the numeric expression shapes
/// the executor can evaluate against a row pair.
///
/// A literal and a column reference (existing row `table.column` or proposed row
/// `EXCLUDED.column`) evaluate on row values directly, and so does arithmetic
/// composed from them: unary minus and `+`, `-`, `*`, `/` over those operands.
/// The value therefore runs through the ordinary numeric expression path, which
/// keeps each dialect's division semantics, division-by-zero handling and
/// integer-overflow rules. Everything else — aggregates, functions, `CASE`,
/// subqueries, window calls — stays explicitly unsupported instead of being
/// approximated.
fn check_conflict_value(expression: &SqlCatalogBoundExpression) -> Result<(), SqlFrontendError> {
    match expression {
        SqlCatalogBoundExpression::Literal(_) | SqlCatalogBoundExpression::Column(_) => Ok(()),
        SqlCatalogBoundExpression::Unary { operator: SqlBoundUnaryOperator::Minus, expression } => check_conflict_value(expression),
        SqlCatalogBoundExpression::Binary { left, operator, right }
            if matches!(
                operator,
                SqlBoundBinaryOperator::Plus
                    | SqlBoundBinaryOperator::Minus
                    | SqlBoundBinaryOperator::Multiply
                    | SqlBoundBinaryOperator::Divide
            ) =>
        {
            check_conflict_value(left)?;
            check_conflict_value(right)
        }
        _ => Err(SqlFrontendError::unsupported(
            "ON CONFLICT DO UPDATE SET supports a literal, a column reference or `+` / `-` / `*` / `/` over them only",
        )),
    }
}

/// 计划 `INSERT ... RETURNING`：把投影绑成可对最终落库行求值的表达式。
///
/// MySQL 的 `INSERT` 没有 `RETURNING` 子句，同一语句在该方言下按语法错误拒绝。
pub(crate) fn plan_returning(
    returning: Option<Vec<SqlCatalogBoundProjection>>,
    table: &crate::SqlCatalogTable,
    dialect: SqlDialect,
) -> Result<Option<ReturningProjections>, SqlFrontendError> {
    let Some(projections) = returning
    else {
        return Ok(None);
    };
    if dialect != SqlDialect::Postgres {
        return Err(SqlFrontendError::syntax("INSERT ... RETURNING is only supported by the PostgreSQL dialect"));
    }
    let mut planned = Vec::new();
    for projection in projections {
        match projection {
            SqlCatalogBoundProjection::Star(_) => {
                for field in &table.fields {
                    reject_unknown_field(field)?;
                    planned.push((
                        SqlCatalogBoundExpression::Column(SqlCatalogBoundColumn {
                            type_id: table.type_id,
                            field_id: field.field_id,
                            row_id: field.field_id,
                            table_name: table.name.clone(),
                            field_name: field.name.clone(),
                            visible_name: field.name.clone(),
                            capacity: field.capacity,
                        }),
                        field.name.clone(),
                    ));
                }
            }
            SqlCatalogBoundProjection::Expression { expression, alias } => match expression {
                SqlCatalogBoundExpression::Column(column) => {
                    let label = alias.unwrap_or_else(|| column.visible_name.clone());
                    planned.push((SqlCatalogBoundExpression::Column(column), label));
                }
                other => {
                    if expression_has_window(&other) {
                        return Err(planner_error("SQL window functions are not allowed in RETURNING"));
                    }
                    let label = alias.unwrap_or_else(|| expression_default_label(&other));
                    planned.push((other, label));
                }
            },
        }
    }
    Ok(Some(planned))
}

/// 计划 `INSERT ... SELECT`：源查询走既有 SELECT 计划，目标列按源结果形状对齐。
///
/// 列数不符按语句错误拒绝，声明类型不兼容按类型错误拒绝，不做隐式截断。
fn plan_insert_select(
    select: SqlCatalogBoundSelect,
    target_fields: Vec<crate::catalog_bind::SqlCatalogBoundColumn>,
    namespace: String,
    field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    unique_constraints: Vec<UniqueConstraintPlan>,
    catalog: &SqlCatalog,
    dialect: SqlDialect,
    conflict: Option<ConflictPolicy>,
    returning: Option<ReturningProjections>,
) -> Result<SqlExecutionPlan, SqlFrontendError> {
    if target_fields.is_empty() {
        return Err(SqlFrontendError::syntax("INSERT has no target columns"));
    }
    let source = plan_select_for_dialect(select, catalog, dialect)?;
    let source_columns = crate::sql_describe::describe_plan(&source);
    if source_columns.len() != target_fields.len() {
        return Err(SqlFrontendError::syntax(format!(
            "INSERT ... SELECT column count mismatch: target supplies {} columns, source selects {}",
            target_fields.len(),
            source_columns.len()
        )));
    }
    let mut fields = Vec::with_capacity(target_fields.len());
    for (field, source_column) in target_fields.iter().zip(source_columns.iter()) {
        let field_type = field_types.get(&field.field_id).cloned().unwrap_or(crate::catalog_bind::SqlFieldType::Unknown);
        let source_type = source_column.field_type.inner();
        if !write_types_compatible(&field_type, &source_type) {
            return Err(SqlFrontendError::new(
                SqlErrorKind::TypeMismatch,
                format!(
                    "INSERT ... SELECT source column `{}` is {source_type:?} but target column `{}` is {field_type:?}",
                    source_column.name, field.field_name
                ),
            ));
        }
        fields.push(InsertSelectColumn {
            field_id: field.field_id,
            field_name: field.field_name.clone(),
            field_type,
            capacity: field.capacity,
        });
    }
    Ok(SqlExecutionPlan::InsertSelect(InsertSelectPlan {
        namespace,
        fields,
        source: Box::new(source),
        field_types,
        conflict,
        returning,
        unique_constraints,
    }))
}

/// `INSERT ... SELECT` 的源列类型能否写进目标列：同类型，或整数向更宽的数值类型提升。
fn write_types_compatible(target: &crate::catalog_bind::SqlFieldType, source: &crate::catalog_bind::SqlFieldType) -> bool {
    use crate::catalog_bind::SqlFieldType;
    let target = target.inner();
    let source = source.inner();
    target == SqlFieldType::Unknown
        || source == SqlFieldType::Unknown
        || target == source
        || matches!(
            (&target, &source),
            (SqlFieldType::Int64, SqlFieldType::Int32) | (SqlFieldType::Float64, SqlFieldType::Int32 | SqlFieldType::Int64)
        )
}

/// 计划一个 UPDATE。`pk = literal` 走单点路径；其他 WHERE 都退化为范围扫描逐行改写。
pub fn plan_update(update: SqlCatalogBoundUpdate, catalog: &SqlCatalog, dialect: SqlDialect) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let table = find_table(catalog, update.table.type_id)?;
    let namespace = format!("table:{}", table.type_id);
    let pk_field = primary_key_of(table)?;
    let field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType> =
        table.fields.iter().map(|f| (f.field_id, f.field_type.clone())).collect();
    let unique_constraints = unique_constraints_of(table);

    let mut write_fields = Vec::new();
    for (field, value_expr) in &update.assignments {
        let lit = literal_from_expression(value_expr)?;
        let field_type = field_types.get(&field.field_id).cloned().unwrap_or(crate::catalog_bind::SqlFieldType::Unknown);
        check_write_literal(&lit, &field_type, &field.field_name, dialect, field.capacity)?;
        write_fields.push(WriteField { field_id: field.field_id, value: lit, field_type, capacity: field.capacity });
    }

    let Some(predicate) = update.predicate
    else {
        return Ok(SqlExecutionPlan::UpdateScan(UpdateScanPlan {
            namespace,
            rowid_prefix: Vec::new(),
            rowid_after: None,
            rowid_before: None,
            fields: write_fields,
            predicate: None,
            field_types,
            unique_constraints,
        }));
    };

    let (pk_literal, pk_prefix, pk_after, pk_before, remaining) = extract_pk_range(predicate.clone(), pk_field.field_id)?;
    if let Some(rowid_literal) = pk_literal {
        return Ok(SqlExecutionPlan::Update(UpdatePlan {
            namespace,
            rowid: pk_literal_to_rowid(&rowid_literal)?,
            fields: write_fields,
            remaining_predicate: remaining,
            field_types,
            unique_constraints,
        }));
    }
    Ok(SqlExecutionPlan::UpdateScan(UpdateScanPlan {
        namespace,
        rowid_prefix: pk_prefix.unwrap_or_default(),
        rowid_after: pk_after,
        rowid_before: pk_before,
        fields: write_fields,
        predicate: remaining,
        field_types,
        unique_constraints,
    }))
}

/// 计划一个 DELETE。`pk = literal` 走单点路径；其他 WHERE 都退化为范围扫描逐行删除。
pub fn plan_delete(delete: SqlCatalogBoundDelete, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let table = find_table(catalog, delete.table.type_id)?;
    let namespace = format!("table:{}", table.type_id);
    let pk_field = primary_key_of(table)?;
    let field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType> =
        table.fields.iter().map(|f| (f.field_id, f.field_type.clone())).collect();

    let Some(predicate) = delete.predicate
    else {
        return Ok(SqlExecutionPlan::DeleteScan(DeleteScanPlan {
            namespace,
            rowid_prefix: Vec::new(),
            rowid_after: None,
            rowid_before: None,
            predicate: None,
            field_types,
        }));
    };

    let (pk_literal, pk_prefix, pk_after, pk_before, remaining) = extract_pk_range(predicate.clone(), pk_field.field_id)?;
    if let Some(rowid_literal) = pk_literal {
        return Ok(SqlExecutionPlan::Delete(DeletePlan {
            namespace,
            rowid: pk_literal_to_rowid(&rowid_literal)?,
            remaining_predicate: remaining,
            field_types,
        }));
    }
    Ok(SqlExecutionPlan::DeleteScan(DeleteScanPlan {
        namespace,
        rowid_prefix: pk_prefix.unwrap_or_default(),
        rowid_after: pk_after,
        rowid_before: pk_before,
        predicate: remaining,
        field_types,
    }))
}

/// Rejects a write literal that the declared column type cannot represent, so a
/// string never lands in an integer column and reads back as the wrong value.
///
/// Binary capacity is a dialect wire fact, checked via
/// [`SqlDialect::bytes_capacity`]: the MySQL path rejects payloads above the
/// plain `BLOB` limit, while PostgreSQL `BYTEA` carries no dialect-level cap.
pub fn check_write_literal(
    literal: &SqlBoundLiteral,
    field_type: &SqlFieldType,
    column: &str,
    dialect: SqlDialect,
    capacity: Option<u32>,
) -> Result<(), SqlFrontendError> {
    // `bytes` 列单独处理：容量诊断必须指名 octets 与声明上限，不能与普通类型不匹配混在一起。
    if matches!(field_type.inner(), SqlFieldType::Bytes) {
        // A declared capacity wins: it is the column's own contract, while the
        // dialect cap only describes the wire's widest binary type.
        let limit = capacity.or_else(|| dialect.bytes_capacity().and_then(|limit| u32::try_from(limit).ok()));
        return match literal {
            // 文本字面量按 UTF-8 写入，容量按实际 octets 判定。
            SqlBoundLiteral::Null => Ok(()),
            SqlBoundLiteral::String(text) => match limit {
                Some(limit) if text.len() as u64 > limit as u64 => Err(SqlFrontendError::new(
                    SqlErrorKind::ValueTooLong,
                    format!("binary payload of {} octets exceeds the declared capacity of {limit} for column `{column}`", text.len()),
                )),
                Some(_) | None => Ok(()),
            },
            SqlBoundLiteral::Bytes(bytes) => match limit {
                Some(limit) if bytes.len() as u64 > limit as u64 => Err(SqlFrontendError::new(
                    SqlErrorKind::ValueTooLong,
                    format!("binary payload of {} octets exceeds the declared capacity of {limit} for column `{column}`", bytes.len()),
                )),
                Some(_) | None => Ok(()),
            },
            _ => Err(SqlFrontendError::new(
                SqlErrorKind::TypeMismatch,
                format!("SQL value {:?} cannot be written to column `{column}`", literal),
            )),
        };
    }
    let fits = match field_type.inner() {
        SqlFieldType::Unknown => true,
        SqlFieldType::Int64 | SqlFieldType::Int32 => match literal {
            SqlBoundLiteral::Null | SqlBoundLiteral::Integer(_) => true,
            SqlBoundLiteral::String(text) => text.parse::<i64>().is_ok(),
            SqlBoundLiteral::Number(_) | SqlBoundLiteral::Boolean(_) | SqlBoundLiteral::Bytes(_) => false,
        },
        SqlFieldType::Float64 => match literal {
            SqlBoundLiteral::Null | SqlBoundLiteral::Integer(_) | SqlBoundLiteral::Number(_) => true,
            SqlBoundLiteral::String(text) => text.parse::<f64>().is_ok(),
            SqlBoundLiteral::Boolean(_) | SqlBoundLiteral::Bytes(_) => false,
        },
        SqlFieldType::Bool => match literal {
            SqlBoundLiteral::Null | SqlBoundLiteral::Boolean(_) => true,
            SqlBoundLiteral::Integer(value) => *value == 0 || *value == 1,
            SqlBoundLiteral::String(text) => {
                matches!(text.to_ascii_lowercase().as_str(), "true" | "false" | "t" | "f" | "1" | "0")
            }
            SqlBoundLiteral::Number(_) | SqlBoundLiteral::Bytes(_) => false,
        },
        SqlFieldType::String => matches!(literal, SqlBoundLiteral::Null | SqlBoundLiteral::String(_)),
        SqlFieldType::Bytes | SqlFieldType::Optional(_) => unreachable!("handled above"),
    };
    if fits {
        if let (SqlFieldType::String, SqlBoundLiteral::String(text), Some(limit)) = (field_type.inner(), literal, capacity) {
            let chars = text.chars().count() as u64;
            if chars > u64::from(limit) {
                return Err(SqlFrontendError::new(
                    SqlErrorKind::ValueTooLong,
                    format!("string value of {chars} characters exceeds the declared capacity of {limit} for column `{column}`"),
                ));
            }
        }
        Ok(())
    }
    else {
        Err(SqlFrontendError::new(SqlErrorKind::TypeMismatch, format!("SQL value {:?} cannot be written to column `{column}`", literal)))
    }
}

/// 从 bound expression 里提取 literal。目前 INSERT/UPDATE 的值必须是 literal。
fn literal_from_expression(expr: &SqlCatalogBoundExpression) -> Result<SqlBoundLiteral, SqlFrontendError> {
    match expr {
        SqlCatalogBoundExpression::Literal(lit) => Ok(lit.clone()),
        SqlCatalogBoundExpression::Column(col) => Err(planner_error(&format!(
            "SQL gateway write values must be literals; column reference `{}` is not supported in this first step",
            col.field_name
        ))),
        SqlCatalogBoundExpression::Wildcard => Err(planner_error("SQL gateway write values must be literals; `*` is not a value")),
        SqlCatalogBoundExpression::OuterColumn(_) => Err(planner_error("SQL gateway write values must be simple literals")),
        // 二进制写入值只能来自 wire 层渲染的 `CAST('<hex>' AS BYTEA|BLOB)`：
        // SQL 文本字面量无法承载非 UTF-8 octets，因此这是唯一的二进制入参通道。
        SqlCatalogBoundExpression::Cast { expr, target } if crate::sql::is_binary_type_name(target) => match expr.as_ref() {
            SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Bytes(bytes)) => Ok(SqlBoundLiteral::Bytes(bytes.clone())),
            SqlCatalogBoundExpression::Literal(SqlBoundLiteral::String(text)) => {
                crate::sql::decode_binary_literal(text).map(SqlBoundLiteral::Bytes)
            }
            _ => Err(planner_error("binary write values must be a hexadecimal literal")),
        },
        SqlCatalogBoundExpression::Function { .. }
        | SqlCatalogBoundExpression::Binary { .. }
        | SqlCatalogBoundExpression::Unary { .. }
        | SqlCatalogBoundExpression::InSubquery { .. }
        | SqlCatalogBoundExpression::Exists { .. }
        | SqlCatalogBoundExpression::ScalarSubquery { .. }
        | SqlCatalogBoundExpression::InValues { .. }
        | SqlCatalogBoundExpression::Case { .. }
        | SqlCatalogBoundExpression::Cast { .. } => Err(planner_error("SQL gateway write values must be simple literals")),
    }
}

/// Constant column default carried in a plan. `f64` cannot derive `Eq`, so
/// floating-point defaults keep their bit pattern until execution converts them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefaultValue {
    /// SQL `NULL`.
    Null,
    /// Integer literal.
    Integer(i64),
    /// Floating-point literal kept as its bit pattern so the enum can derive `Eq`.
    NumberBits(u64),
    /// String literal, outer quotes already removed.
    String(String),
    /// Boolean literal.
    Boolean(bool),
}

impl DefaultValue {
    /// Materializes the default as an execution value.
    pub fn to_sql_value(&self) -> SqlValue {
        match self {
            DefaultValue::Null => SqlValue::Null,
            DefaultValue::Integer(value) => SqlValue::Integer(*value),
            DefaultValue::NumberBits(bits) => SqlValue::Number(f64::from_bits(*bits)),
            DefaultValue::String(value) => SqlValue::String(value.clone()),
            DefaultValue::Boolean(value) => SqlValue::Boolean(*value),
        }
    }
}

/// 已计划好的 SQL 操作，可以直接翻译为 KeyValueCommand 或 Scan。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlExecutionPlan {
    /// SELECT — 精确主键等值单点读取。
    Read(ReadPlan),
    /// SELECT — namespace 扫描 + 行级过滤。
    Scan(ScanPlan),
    /// SELECT — 多表内连接（嵌套循环合并后再投影 / 排序 / 限流）。
    Join(JoinPlan),
    /// SELECT without FROM — constant and scalar builtin evaluation.
    Scalar(ScalarPlan),
    /// SELECT — set operation combining two complete result plans.
    SetOperation(SetOperationPlan),
    /// INSERT — PutIfAbsent（pk 必须由 VALUES 提供）。
    Insert(WritePlan),
    /// 多行 INSERT — 依次 PutIfAbsent。
    MultiInsert(Vec<WritePlan>),
    /// `INSERT ... SELECT` — 先执行源查询，校验通过后再逐行 PutIfAbsent。
    InsertSelect(InsertSelectPlan),
    /// UPDATE — 先 Get 再 remaining_predicate 再 Put。
    Update(UpdatePlan),
    /// UPDATE — 扫描匹配行后逐行改写。
    UpdateScan(UpdateScanPlan),
    /// DELETE — 先 Get 再 remaining_predicate 再 Delete。
    Delete(DeletePlan),
    /// DELETE — 扫描匹配行后逐行删除。
    DeleteScan(DeleteScanPlan),
}

/// SELECT 精确主键等值执行计划。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPlan {
    /// namespace 标签，形如 `table:{type_id}`。
    pub namespace: String,
    /// 要读取的 row 的主键值 bytes。
    pub rowid: Vec<u8>,
    /// 投影里直接映射到 catalog 字段的那些列：(field_id, display_name)。
    pub projection_fields: Vec<(u64, String)>,
    /// 投影里需要在 row 上做表达式求值的那些：(expression, display_name)。
    pub projection_expressions: Vec<(SqlCatalogBoundExpression, String)>,
    /// 从原 WHERE 里剥掉主键等值后剩下的表达式；`None` 表示没有剩余。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
    /// `LIMIT 0`：结果集必须为空，不允许单点读绕过。
    pub suppress_rows: bool,
    /// field_id → SqlFieldType 映射，execute 层 decode_row 用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    /// row_id → declared default for columns absent from the stored row.
    pub field_defaults: std::collections::BTreeMap<u64, DefaultValue>,
}

/// SELECT 多行扫描执行计划。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPlan {
    /// namespace 标签，形如 `table:{type_id}`。
    pub namespace: String,
    /// rowid 前缀 bytes；空 = 整个 namespace 全扫。
    pub rowid_prefix: Vec<u8>,
    /// exclusive lower cursor —— 跳过所有 `rowid <= after` 的记录。None = 无下界。
    pub rowid_after: Option<Vec<u8>>,
    /// exclusive upper cursor —— 跳过所有 `rowid >= before` 的记录。None = 无上界。
    pub rowid_before: Option<Vec<u8>>,
    /// 下推到 kv-store 的 scan limit（0 = 无上限）。ORDER BY / 聚合 / GROUP BY 存在时保持 0。
    pub limit: usize,
    /// 投影里直接映射到 catalog 字段的那些列：(field_id, display_name)。
    pub projection_fields: Vec<(u64, String)>,
    /// 投影里需要在 row 上做表达式求值的那些：(expression, display_name)。
    pub projection_expressions: Vec<(SqlCatalogBoundExpression, String)>,
    /// 行级过滤表达式；`None` 表示不过滤。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
    /// 可选 GROUP BY 表达式列表；scan 全部 rows 后由 execute 层做 bucket 聚合。
    pub group_by: Option<Vec<SqlCatalogBoundExpression>>,
    /// 可选 HAVING 表达式；GROUP BY 聚合完后应用。
    pub having: Option<SqlCatalogBoundExpression>,
    /// 可选 ORDER BY 字段列表；scan 全部 rows 后由 execute 层内存排序。
    pub order_by: Option<Vec<SqlCatalogBoundOrderByItem>>,
    /// 排序 / 聚合之后才应用的 LIMIT。ORDER BY / 聚合 / GROUP BY 存在时 kv-scan limit=0，
    /// 由本字段承接 SQL 的 LIMIT 值。0 = 无上限。
    pub limit_after_sort: usize,
    /// 排序 / 聚合之后才应用的 OFFSET。默认 0。
    pub offset_after_sort: usize,
    /// 本方言下 `NULL` 是否排在任何非 NULL 值之前（MySQL 是，PostgreSQL 否）。
    pub nulls_first: bool,
    /// True when any projection expression is an aggregate function
    /// (COUNT/SUM/AVG/MAX/MIN). execute_scan will produce aggregated result rows.
    pub has_aggregate: bool,
    /// True when any projection expression is a window function call. execute_scan
    /// evaluates it per partition before applying DISTINCT / ORDER BY / LIMIT.
    pub has_window: bool,
    /// `SELECT DISTINCT`：execute 层在排序/分页之前按投影值去重。
    pub distinct: bool,
    /// `LIMIT 0`：结果集必须为空，包括聚合结果。
    pub suppress_rows: bool,
    /// field_id → SqlFieldType 映射，execute 层 decode_row 用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    /// row_id → declared default for columns absent from the stored row.
    pub field_defaults: std::collections::BTreeMap<u64, DefaultValue>,
}

/// SELECT 多表内连接执行计划（嵌套循环合并）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinPlan {
    /// 基表来源。
    pub base: JoinSourcePlan,
    /// 依次连接的来源，每个带自己的 ON 条件。
    pub joins: Vec<JoinSourcePlan>,
    /// 投影里直接映射到 catalog 字段的那些列：(field_id, display_name)。
    pub projection_fields: Vec<(u64, String)>,
    /// 投影里需要在合并行上做表达式求值的那些：(expression, display_name)。
    pub projection_expressions: Vec<(SqlCatalogBoundExpression, String)>,
    /// 行级过滤表达式；`None` 表示不过滤。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
    /// 可选 ORDER BY 字段列表；合并出全部行后由 execute 层内存排序。
    pub order_by: Option<Vec<SqlCatalogBoundOrderByItem>>,
    /// 排序之后才应用的 LIMIT。0 = 无上限。
    pub limit_after_sort: usize,
    /// 排序之后才应用的 OFFSET。默认 0。
    pub offset_after_sort: usize,
    /// 本方言下 `NULL` 是否排在任何非 NULL 值之前（MySQL 是，PostgreSQL 否）。
    pub nulls_first: bool,
    /// `LIMIT 0`：结果集必须为空。
    pub suppress_rows: bool,
    /// `SELECT DISTINCT`：execute 层在排序/分页之前按投影值去重。
    pub distinct: bool,
    /// field_id → SqlFieldType 映射，execute 层 decode_row / 求值用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    /// row_id → declared default for columns absent from the stored row.
    pub field_defaults: std::collections::BTreeMap<u64, DefaultValue>,
}

/// JOIN 的一个数据来源。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSourcePlan {
    /// namespace 标签，形如 `table:{type_id}`。
    pub namespace: String,
    /// 需要从该表 TLV payload 里解码的字段（其余字段跳过）。
    pub needed_field_ids: Vec<u64>,
    /// 与 [`Self::needed_field_ids`] 平行的 row id，解码后按它写入合并行。
    pub row_ids: Vec<u64>,
    /// 该来源的 ON 条件；基表为 `None`。
    pub on: Option<SqlCatalogBoundExpression>,
    /// 基表为 `Inner`；连接表可为 `Inner` 或 `Left`。
    pub kind: crate::sql::SqlBoundJoinKind,
}

/// SELECT without FROM 执行计划：纯常量与标量内建函数求值，产出单行结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarPlan {
    /// 投影表达式与其结果列名。
    pub projections: Vec<(SqlCatalogBoundExpression, String)>,
    /// `LIMIT 0` 时按 SQL 语义返回空结果集。
    pub suppress_row: bool,
}

/// SELECT 集合运算执行计划：每个操作数已完整计划，执行层按运算符左到右折叠。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetOperationPlan {
    /// 操作数执行计划，按源码从左到右排列。
    pub operands: Vec<SqlExecutionPlan>,
    /// 相邻操作数之间的运算符，长度为 `operands.len() - 1`。
    pub operators: Vec<SqlBoundSetOperator>,
    /// 合并结果上的 `ORDER BY`，已解析为输出列序号。
    pub order_by: Option<Vec<SqlSetOrderByItem>>,
    /// 排序之后应用的 `LIMIT`。0 = 无上限。
    pub limit_after_sort: usize,
    /// 排序之后应用的 `OFFSET`。默认 0。
    pub offset_after_sort: usize,
    /// 本方言下 `NULL` 是否排在任何非 NULL 值之前。
    pub nulls_first: bool,
    /// `LIMIT 0`：结果集必须为空。
    pub suppress_rows: bool,
}

/// 集合运算结果上的一列 `ORDER BY` 项。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlSetOrderByItem {
    /// 结果列序号（从 0 开始）。
    pub column: usize,
    /// 排序方向。
    pub direction: SqlBoundOrderDirection,
    /// 显式 `NULLS FIRST` / `NULLS LAST`，当写入时。
    pub nulls: Option<SqlNullsOrder>,
}

/// INSERT 执行计划。payload 由 execute 层做 TLV 编码。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritePlan {
    /// namespace 标签。
    pub namespace: String,
    /// 要写入的 row 的主键值 bytes。
    pub rowid: Vec<u8>,
    /// INSERT 时提供所有字段值；UPDATE 时提供要 SET 的字段。
    pub fields: Vec<WriteField>,
    /// field_id → SqlFieldType 映射，execute 层 encode/eval 用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    /// 冲突策略，`None` 表示冲突按唯一约束错误上报。
    pub conflict: Option<ConflictPolicy>,
    /// `RETURNING` 投影，`None` 表示语句不返回结果集。
    pub returning: Option<ReturningProjections>,
    /// 该表声明的 `UNIQUE` 约束，执行层在写入前逐条校验。
    pub unique_constraints: Vec<UniqueConstraintPlan>,
}

/// `RETURNING` 投影：对最终落库行求值的表达式 + 展示列名。
pub type ReturningProjections = Vec<(SqlCatalogBoundExpression, String)>;

/// 冲突命中后的处理策略。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// `DO NOTHING` — 跳过冲突行。
    DoNothing,
    /// `DO UPDATE SET ... [WHERE ...]` — 用赋值改写既有行。
    DoUpdate {
        /// 要写入既有行的字段（此时是 `SET` 的结果）。
        assignments: Vec<ConflictAssignment>,
        /// 只在既有行满足该谓词时才改写。
        predicate: Option<SqlCatalogBoundExpression>,
    },
}

/// `ON CONFLICT DO UPDATE` 的一条赋值。
///
/// 右值在既有行与 `EXCLUDED` 行合并后的作用域里求值，因此可以引用两侧的列。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictAssignment {
    /// 目标 field_id。
    pub field_id: u64,
    /// 目标列名，用于类型错误诊断。
    pub field_name: String,
    /// 目标列声明类型，执行层编码校验用。
    pub field_type: crate::catalog_bind::SqlFieldType,
    /// 目标列声明的字符/octet 容量，执行层长度守卫用。
    pub capacity: Option<u32>,
    /// 赋值右值。
    pub value: SqlCatalogBoundExpression,
}

/// 一条 `UNIQUE` 约束的绑定结果。
///
/// `columns` 非空：catalog 与 schema 校验都拒绝空约束。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueConstraintPlan {
    /// 参与列 `(field_id, 列名)`，按约束声明顺序排列。
    pub columns: Vec<(u64, String)>,
}

/// `INSERT ... SELECT` 执行计划：源查询先按读路径执行，再逐行对齐写入。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertSelectPlan {
    /// 目标 namespace 标签。
    pub namespace: String,
    /// 目标列，与源查询输出列按下标一一对应。
    pub fields: Vec<InsertSelectColumn>,
    /// 源查询执行计划。
    pub source: Box<SqlExecutionPlan>,
    /// field_id → SqlFieldType 映射，execute 层编码用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    /// 冲突策略，`None` 表示冲突按唯一约束错误上报。
    pub conflict: Option<ConflictPolicy>,
    /// `RETURNING` 投影，`None` 表示语句不返回结果集。
    pub returning: Option<ReturningProjections>,
    /// 该表声明的 `UNIQUE` 约束，执行层在写入前逐条校验。
    pub unique_constraints: Vec<UniqueConstraintPlan>,
}

/// `INSERT ... SELECT` 的一个目标列绑定。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertSelectColumn {
    /// 目标 field_id。
    pub field_id: u64,
    /// 目标列名，用于类型错误诊断。
    pub field_name: String,
    /// 目标列声明类型。
    pub field_type: crate::catalog_bind::SqlFieldType,
    /// 目标列声明的字符/octet 容量。
    pub capacity: Option<u32>,
}

/// UPDATE 执行计划。payload 由 execute 层做 TLV 编码。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePlan {
    /// namespace 标签。
    pub namespace: String,
    /// 要写入的 row 的主键值 bytes。
    pub rowid: Vec<u8>,
    /// 要 SET 的字段。
    pub fields: Vec<WriteField>,
    /// 从原 WHERE 里剥掉主键等值后剩下的表达式。
    /// execute_update 会在写入前对现有 row 评估此 predicate。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
    /// field_id → SqlFieldType 映射，execute 层 eval predicate 用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    /// 该表声明的 `UNIQUE` 约束，执行层在写入前逐条校验。
    pub unique_constraints: Vec<UniqueConstraintPlan>,
}

/// 一个要写入的字段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteField {
    /// field_id。
    pub field_id: u64,
    /// 要写入的值（必须是 Literal，复杂表达式暂不支持）。
    pub value: SqlBoundLiteral,
    /// 从 catalog 投影的字段类型，execute 层编码校验用。
    pub field_type: crate::catalog_bind::SqlFieldType,
    /// 目标列声明的字符/octet 容量，执行层长度守卫用。
    pub capacity: Option<u32>,
}

/// DELETE 执行计划。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletePlan {
    /// 目标表所属的 namespace。
    pub namespace: String,
    /// 待删除行的 row id。
    pub rowid: Vec<u8>,
    /// 从原 WHERE 里剥掉主键等值后剩下的表达式。
    /// execute_delete 会在删除前对现有 row 评估此 predicate。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
    /// field_id → SqlFieldType 映射，execute 层 eval predicate 用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
}

/// 多行 UPDATE 执行计划：扫描 rowid 区间，逐行评估完整 WHERE 后改写。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateScanPlan {
    /// namespace 标签，形如 `table:{type_id}`。
    pub namespace: String,
    /// rowid 前缀 bytes；空 = 整个 namespace 全扫。
    pub rowid_prefix: Vec<u8>,
    /// exclusive lower cursor。
    pub rowid_after: Option<Vec<u8>>,
    /// exclusive upper cursor。
    pub rowid_before: Option<Vec<u8>>,
    /// 要 SET 的字段。
    pub fields: Vec<WriteField>,
    /// 已去掉可下推主键范围后的完整行级过滤条件；`None` 表示整段区间都匹配。
    pub predicate: Option<SqlCatalogBoundExpression>,
    /// field_id → SqlFieldType 映射，execute 层解码与求值用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    /// 该表声明的 `UNIQUE` 约束，执行层在写入前逐条校验。
    pub unique_constraints: Vec<UniqueConstraintPlan>,
}

/// 多行 DELETE 执行计划：扫描 rowid 区间，逐行评估完整 WHERE 后删除。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteScanPlan {
    /// namespace 标签，形如 `table:{type_id}`。
    pub namespace: String,
    /// rowid 前缀 bytes；空 = 整个 namespace 全扫。
    pub rowid_prefix: Vec<u8>,
    /// exclusive lower cursor。
    pub rowid_after: Option<Vec<u8>>,
    /// exclusive upper cursor。
    pub rowid_before: Option<Vec<u8>>,
    /// 已去掉可下推主键范围后的完整行级过滤条件；`None` 表示整段区间都匹配。
    pub predicate: Option<SqlCatalogBoundExpression>,
    /// field_id → SqlFieldType 映射，execute 层解码与求值用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
}

/// 从 WHERE predicate 里提取所有关于 pk 字段的条件。
///
/// 返回五元组：
/// - `Option<SqlBoundLiteral>` —— pk = literal 等值（读单点）
/// - `Option<Vec<u8>>` —— pk LIKE 'prefix%' 前缀
/// - `Option<Vec<u8>>` —— after exclusive lower cursor（来自 pk > X 或 pk >= X → exclusive X）
/// - `Option<Vec<u8>>` —— before exclusive upper cursor（来自 pk < Y 或 pk <= Y → exclusive Y+ε）
/// - `Option<SqlCatalogBoundExpression>` —— 剩余不能下推的条件
///
/// 冲突规则：两个等值 / 两个不同前缀 / after ≥ before → planner_error。
fn extract_pk_range(
    predicate: SqlCatalogBoundExpression,
    pk_row_id: u64,
) -> Result<(Option<SqlBoundLiteral>, Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>, Option<SqlCatalogBoundExpression>), SqlFrontendError> {
    match predicate {
        // AND 拆分 — 两侧分别提取，再合并。
        SqlCatalogBoundExpression::Binary { left, operator: SqlBoundBinaryOperator::And, right } => {
            let (l_eq, l_pre, l_after, l_before, l_rem) = extract_pk_range(*left, pk_row_id)?;
            let (r_eq, r_pre, r_after, r_before, r_rem) = extract_pk_range(*right, pk_row_id)?;
            let eq = merge_opt(l_eq, r_eq, "conflicting primary-key equality inside AND")?;
            let prefix = merge_opt(l_pre, r_pre, "conflicting primary-key LIKE prefix inside AND")?;
            let after = merge_opt(l_after, r_after, "conflicting primary-key lower bound inside AND")?;
            let before = merge_opt(l_before, r_before, "conflicting primary-key upper bound inside AND")?;
            let remaining = combine_remaining(l_rem, r_rem);
            Ok((eq, prefix, after, before, remaining))
        }

        // 所有其他 Binary 表达式 —— 尝试识别 pk 条件。
        SqlCatalogBoundExpression::Binary { left, operator, right } => {
            match (operator, *left, *right) {
                // pk = literal
                (SqlBoundBinaryOperator::Equal, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                | (SqlBoundBinaryOperator::Equal, SqlCatalogBoundExpression::Literal(lit), SqlCatalogBoundExpression::Column(col))
                    if col.row_id == pk_row_id =>
                {
                    let _rowid = pk_literal_to_rowid(&lit)?;
                    Ok((Some(lit), None, None, None, None))
                }

                // pk > literal → after exclusive
                (SqlBoundBinaryOperator::Greater, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                    if col.row_id == pk_row_id =>
                {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, Some(rowid), None, None))
                }

                // pk >= literal → after exclusive：key > (lit-1) 等价于 key >= lit
                (SqlBoundBinaryOperator::GreaterEqual, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                    if col.row_id == pk_row_id =>
                {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, Some(rowid_decrement(&rowid)), None, None))
                }

                // pk < literal → before exclusive
                (SqlBoundBinaryOperator::Less, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                    if col.row_id == pk_row_id =>
                {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, None, Some(rowid), None))
                }

                // pk <= literal → before exclusive：key < (lit+1) 等价于 key <= lit
                (SqlBoundBinaryOperator::LessEqual, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                    if col.row_id == pk_row_id =>
                {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, None, Some(rowid_increment(&rowid)), None))
                }

                // pk LIKE 'prefix%'
                (SqlBoundBinaryOperator::Like, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                    if col.row_id == pk_row_id =>
                {
                    let prefix = match &lit {
                        SqlBoundLiteral::String(s) if s.ends_with('%') && !s.ends_with("%%") => s[..s.len() - 1].as_bytes().to_vec(),
                        _ => {
                            // LIKE 模式不只是 prefix%，下推成 remaining
                            return Ok((
                                None,
                                None,
                                None,
                                None,
                                Some(SqlCatalogBoundExpression::Binary {
                                    left: Box::new(SqlCatalogBoundExpression::Column(col)),
                                    operator: SqlBoundBinaryOperator::Like,
                                    right: Box::new(SqlCatalogBoundExpression::Literal(lit)),
                                }),
                            ));
                        }
                    };
                    Ok((None, Some(prefix), None, None, None))
                }

                // 其他所有 Binary —— 留在 remaining
                (op, left, right) => Ok((
                    None,
                    None,
                    None,
                    None,
                    Some(SqlCatalogBoundExpression::Binary { left: Box::new(left), operator: op, right: Box::new(right) }),
                )),
            }
        }

        // OR、Between、Unary、Column-alone、Literal-alone 等全部留作 remaining。
        other => Ok((None, None, None, None, Some(other))),
    }
}

fn merge_opt<T>(left: Option<T>, right: Option<T>, conflict_msg: &str) -> Result<Option<T>, SqlFrontendError>
where
    T: PartialEq,
{
    match (left, right) {
        (Some(a), Some(b)) if a == b => Ok(Some(a)),
        (Some(_), Some(_)) => Err(planner_error(conflict_msg)),
        (Some(a), None) => Ok(Some(a)),
        (None, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

fn combine_remaining(left: Option<SqlCatalogBoundExpression>, right: Option<SqlCatalogBoundExpression>) -> Option<SqlCatalogBoundExpression> {
    match (left, right) {
        (None, None) => None,
        (Some(l), None) | (None, Some(l)) => Some(l),
        (Some(l), Some(r)) => {
            Some(SqlCatalogBoundExpression::Binary { left: Box::new(l), operator: SqlBoundBinaryOperator::And, right: Box::new(r) })
        }
    }
}

/// Encodes a primary-key literal into the canonical row id bytes.
pub fn pk_literal_to_rowid(literal: &SqlBoundLiteral) -> Result<Vec<u8>, SqlFrontendError> {
    match literal {
        SqlBoundLiteral::Integer(value) => Ok(value.to_string().into_bytes()),
        SqlBoundLiteral::Number(text) => Ok(text.as_bytes().to_vec()),
        SqlBoundLiteral::String(text) => Ok(text.as_bytes().to_vec()),
        SqlBoundLiteral::Bytes(bytes) => Ok(bytes.clone()),
        SqlBoundLiteral::Boolean(value) => Ok((if *value { "1" } else { "0" }).as_bytes().to_vec()),
        SqlBoundLiteral::Null => Err(SqlFrontendError::new(SqlErrorKind::NotNullViolation, "primary key must not be NULL")),
    }
}

/// rowid + 1 并转为 bytes。用于把 inclusive upper bound 转为 exclusive before。
fn rowid_increment(rowid: &[u8]) -> Vec<u8> {
    let s = String::from_utf8_lossy(rowid);
    match s.parse::<u128>() {
        Ok(n) => (n + 1).to_string().into_bytes(),
        Err(_) => {
            let mut v = rowid.to_vec();
            if let Some(last) = v.last_mut() {
                *last = last.saturating_add(1);
            }
            v
        }
    }
}

/// rowid - 1 并转为 bytes。用于把 inclusive lower bound 转为 exclusive after。
fn rowid_decrement(rowid: &[u8]) -> Vec<u8> {
    let s = String::from_utf8_lossy(rowid);
    match s.parse::<u128>() {
        Ok(0) => rowid.to_vec(),
        Ok(n) => (n - 1).to_string().into_bytes(),
        Err(_) => {
            let mut v = rowid.to_vec();
            if let Some(last) = v.last_mut() {
                *last = last.saturating_sub(1);
            }
            v
        }
    }
}

fn primary_key_of(table: &crate::SqlCatalogTable) -> Result<&crate::SqlCatalogField, SqlFrontendError> {
    // VOS 约定：第一个字段是主键（通常名为 @@id）。
    table.fields.first().ok_or_else(|| planner_error(&format!("table `{}` has no fields", table.name)))
}

/// 绑定表声明的次要 `UNIQUE` 约束。
///
/// catalog 已校验约束只引用本表字段，这里把 field_id 映射成 `(field_id, 列名)`。
fn unique_constraints_of(table: &crate::SqlCatalogTable) -> Vec<UniqueConstraintPlan> {
    table
        .unique_constraints
        .iter()
        .map(|constraint| UniqueConstraintPlan {
            columns: constraint
                .iter()
                .filter_map(|field_id| {
                    table.fields.iter().find(|field| field.field_id == *field_id).map(|field| (*field_id, field.name.clone()))
                })
                .collect(),
        })
        .filter(|constraint| !constraint.columns.is_empty())
        .collect()
}

fn find_table(catalog: &SqlCatalog, type_id: u64) -> Result<&crate::SqlCatalogTable, SqlFrontendError> {
    catalog.tables().iter().find(|t| t.type_id == type_id).ok_or_else(|| planner_error("catalog table is missing"))
}

/// 无 FROM 投影的默认列名：字面量取字面量文本，函数取大写函数名，
/// 其余表达式退回 debug 形状。驱动探测依赖这些名字（`SELECT 1` 的列名是 `1`）。
fn scalar_projection_label(expr: &SqlCatalogBoundExpression) -> String {
    match expr {
        SqlCatalogBoundExpression::Literal(lit) => match lit {
            SqlBoundLiteral::Integer(value) => value.to_string(),
            SqlBoundLiteral::Number(text) | SqlBoundLiteral::String(text) => text.clone(),
            SqlBoundLiteral::Bytes(bytes) => crate::sql::encode_hex(bytes),
            SqlBoundLiteral::Boolean(value) => if *value { "true" } else { "false" }.to_string(),
            SqlBoundLiteral::Null => "NULL".to_string(),
        },
        SqlCatalogBoundExpression::Function { name, .. } => name.to_ascii_uppercase(),
        other => expression_default_label(other),
    }
}

fn expression_default_label(expr: &SqlCatalogBoundExpression) -> String {
    format!("{expr:?}")
}

/// 表达式里是否出现聚合函数（含嵌套在运算里）。窗口调用不算聚合：
/// 它按分区逐行输出，不压缩行数。
fn expression_has_aggregate(expr: &SqlCatalogBoundExpression) -> bool {
    match expr {
        SqlCatalogBoundExpression::Function { name, args, window, .. } => {
            window.is_none() && (SqlAggregateFunction::from_name(name).is_some() || args.iter().any(expression_has_aggregate))
        }
        SqlCatalogBoundExpression::Binary { left, right, .. } => expression_has_aggregate(left) || expression_has_aggregate(right),
        SqlCatalogBoundExpression::Unary { expression, .. } => expression_has_aggregate(expression),
        SqlCatalogBoundExpression::InSubquery { expression, .. } | SqlCatalogBoundExpression::InValues { expression, .. } => {
            expression_has_aggregate(expression)
        }
        SqlCatalogBoundExpression::Case { branches, else_branch } => {
            branches.iter().any(|(w, t)| expression_has_aggregate(w) || expression_has_aggregate(t))
                || else_branch.as_ref().is_some_and(|e| expression_has_aggregate(e))
        }
        SqlCatalogBoundExpression::Cast { expr, .. } => expression_has_aggregate(expr),
        // 子查询只依赖自己的结果，对分组恒定：它内部的聚合不属于外层分组。
        SqlCatalogBoundExpression::Exists { .. } | SqlCatalogBoundExpression::ScalarSubquery { .. } => false,
        SqlCatalogBoundExpression::Column(_) | SqlCatalogBoundExpression::OuterColumn(_) => false,
        SqlCatalogBoundExpression::Literal(_) | SqlCatalogBoundExpression::Wildcard => false,
    }
}

/// 表达式本身是否是一个窗口函数调用（`ROW_NUMBER()` / `SUM(...) OVER (...)`）。
fn is_window_call(expr: &SqlCatalogBoundExpression) -> bool {
    matches!(expr, SqlCatalogBoundExpression::Function { window: Some(_), .. })
}

/// 表达式里是否出现窗口函数调用（含嵌套在运算或实参里的情况）。
fn expression_has_window(expr: &SqlCatalogBoundExpression) -> bool {
    match expr {
        SqlCatalogBoundExpression::Function { args, window, .. } => window.is_some() || args.iter().any(expression_has_window),
        SqlCatalogBoundExpression::Binary { left, right, .. } => expression_has_window(left) || expression_has_window(right),
        SqlCatalogBoundExpression::Unary { expression, .. } => expression_has_window(expression),
        SqlCatalogBoundExpression::InSubquery { expression, .. } | SqlCatalogBoundExpression::InValues { expression, .. } => {
            expression_has_window(expression)
        }
        SqlCatalogBoundExpression::Case { branches, else_branch } => {
            branches.iter().any(|(w, t)| expression_has_window(w) || expression_has_window(t))
                || else_branch.as_ref().is_some_and(|e| expression_has_window(e))
        }
        SqlCatalogBoundExpression::Cast { expr, .. } => expression_has_window(expr),
        SqlCatalogBoundExpression::Column(_)
        | SqlCatalogBoundExpression::OuterColumn(_)
        | SqlCatalogBoundExpression::Literal(_)
        | SqlCatalogBoundExpression::Wildcard
        | SqlCatalogBoundExpression::Exists { .. }
        | SqlCatalogBoundExpression::ScalarSubquery { .. } => false,
    }
}

/// 表达式是否读取直接外层查询的列，即自身或内部子查询是否相关。
///
/// 相关子查询按外层行逐行重算，因此不能在被过滤/投影的行确定之前一次性物化。
/// 执行层用这个判定在「一次物化」与「逐行重算」之间选择。
pub fn expression_reads_outer(expr: &SqlCatalogBoundExpression) -> bool {
    let mut found = false;
    visit_outer_columns(expr, &mut |_| found = true);
    found
}

/// 执行计划是否读取直接外层查询的列（递归下探嵌套子查询计划）。
/// 计划是否读取 OUTER 标记行。
pub fn plan_reads_outer(plan: &SqlExecutionPlan) -> bool {
    let mut found = false;
    visit_plan_outer_columns(plan, &mut |_| found = true);
    found
}

/// 收集表达式（含内部子查询计划）读取的外层行键。
///
/// 外层行按这些 row id 改成整行解码，相关子查询逐行重算时才能真的拿到该行的值，
/// 而不是把未解码的字段当成 SQL NULL。行槽偏移已包含在 row id 里。
pub(crate) fn collect_outer_row_ids(expr: &SqlCatalogBoundExpression, out: &mut Vec<u64>) {
    visit_outer_columns(expr, &mut |column| push_outer_row_id(column, out));
}

/// 收集一个子查询计划读取的外层行键，语义同 [`collect_outer_row_ids`]。
pub(crate) fn collect_plan_outer_row_ids(plan: &SqlExecutionPlan, out: &mut Vec<u64>) {
    visit_plan_outer_columns(plan, &mut |column| push_outer_row_id(column, out));
}

fn push_outer_row_id(column: &SqlCatalogOuterColumn, out: &mut Vec<u64>) {
    if !out.contains(&column.column.row_id) {
        out.push(column.column.row_id);
    }
}

/// 深度优先访问表达式里的每个外层列引用，含嵌套子查询计划里的引用。
fn visit_outer_columns(expr: &SqlCatalogBoundExpression, visit: &mut impl FnMut(&SqlCatalogOuterColumn)) {
    match expr {
        SqlCatalogBoundExpression::OuterColumn(column) => visit(column),
        SqlCatalogBoundExpression::Function { args, window, .. } => {
            for arg in args {
                visit_outer_columns(arg, &mut *visit);
            }
            if let Some(window) = window {
                for element in &window.partition_by {
                    visit_outer_columns(element, &mut *visit);
                }
                for item in &window.order_by {
                    visit_outer_columns(&item.expression, &mut *visit);
                }
            }
        }
        SqlCatalogBoundExpression::Binary { left, right, .. } => {
            visit_outer_columns(left, &mut *visit);
            visit_outer_columns(right, &mut *visit);
        }
        SqlCatalogBoundExpression::Unary { expression, .. }
        | SqlCatalogBoundExpression::InValues { expression, .. }
        | SqlCatalogBoundExpression::Cast { expr: expression, .. } => visit_outer_columns(expression, &mut *visit),
        SqlCatalogBoundExpression::InSubquery { expression, plan, .. } => {
            visit_outer_columns(expression, &mut *visit);
            visit_plan_outer_columns(plan, &mut *visit);
        }
        SqlCatalogBoundExpression::Case { branches, else_branch } => {
            for (when, then) in branches {
                visit_outer_columns(when, &mut *visit);
                visit_outer_columns(then, &mut *visit);
            }
            if let Some(else_branch) = else_branch {
                visit_outer_columns(else_branch, &mut *visit);
            }
        }
        SqlCatalogBoundExpression::Exists { plan, .. } | SqlCatalogBoundExpression::ScalarSubquery { plan } => {
            visit_plan_outer_columns(plan, &mut *visit)
        }
        SqlCatalogBoundExpression::Column(_) | SqlCatalogBoundExpression::Literal(_) | SqlCatalogBoundExpression::Wildcard => {}
    }
}

/// 深度优先访问一个执行计划里的每个外层列引用，含嵌套子查询计划里的引用。
fn visit_plan_outer_columns(plan: &SqlExecutionPlan, visit: &mut impl FnMut(&SqlCatalogOuterColumn)) {
    match plan {
        SqlExecutionPlan::Read(read) => {
            if let Some(predicate) = &read.remaining_predicate {
                visit_outer_columns(predicate, &mut *visit);
            }
            for (expression, _) in &read.projection_expressions {
                visit_outer_columns(expression, &mut *visit);
            }
        }
        SqlExecutionPlan::Scan(scan) => {
            if let Some(predicate) = &scan.remaining_predicate {
                visit_outer_columns(predicate, &mut *visit);
            }
            if let Some(having) = &scan.having {
                visit_outer_columns(having, &mut *visit);
            }
            for expression in scan.group_by.iter().flatten() {
                visit_outer_columns(expression, &mut *visit);
            }
            for item in scan.order_by.iter().flatten() {
                visit_outer_columns(&item.expression, &mut *visit);
            }
            for (expression, _) in &scan.projection_expressions {
                visit_outer_columns(expression, &mut *visit);
            }
        }
        SqlExecutionPlan::Join(join) => {
            for on in join.base.on.iter().chain(join.joins.iter().filter_map(|source| source.on.as_ref())) {
                visit_outer_columns(on, &mut *visit);
            }
            if let Some(predicate) = &join.remaining_predicate {
                visit_outer_columns(predicate, &mut *visit);
            }
            for item in join.order_by.iter().flatten() {
                visit_outer_columns(&item.expression, &mut *visit);
            }
            for (expression, _) in &join.projection_expressions {
                visit_outer_columns(expression, &mut *visit);
            }
        }
        SqlExecutionPlan::Scalar(scalar) => {
            for (expression, _) in &scalar.projections {
                visit_outer_columns(expression, &mut *visit);
            }
        }
        SqlExecutionPlan::SetOperation(set) => {
            for operand in &set.operands {
                visit_plan_outer_columns(operand, &mut *visit);
            }
        }
        SqlExecutionPlan::InsertSelect(insert) => visit_plan_outer_columns(&insert.source, &mut *visit),
        // 写语句计划不接受相关子查询，绑定期已经拒绝，因此没有外层列可读。
        SqlExecutionPlan::Insert(_)
        | SqlExecutionPlan::MultiInsert(_)
        | SqlExecutionPlan::Update(_)
        | SqlExecutionPlan::UpdateScan(_)
        | SqlExecutionPlan::Delete(_)
        | SqlExecutionPlan::DeleteScan(_) => {}
    }
}

/// 拒绝「外层已经无法指向具体行」的位置上的相关子查询。
///
/// 分组或聚合之后，一行输出不再对应一行外层输入；窗口函数同样按分区整体求值。
/// 逐行重算在这些位置没有可替换的外层行，因此在计划期显式拒绝，而不是挑一行近似。
fn validate_correlated_placement(select: &SqlCatalogBoundSelect, has_aggregate: bool, has_window: bool) -> Result<(), SqlFrontendError> {
    let projection_expressions = || {
        select.projections.iter().filter_map(|projection| match projection {
            SqlCatalogBoundProjection::Expression { expression, .. } => Some(expression),
            SqlCatalogBoundProjection::Star(_) => None,
        })
    };
    if has_aggregate || select.group_by.is_some() {
        if projection_expressions().any(expression_reads_outer) {
            return Err(planner_error("SQL correlated subquery cannot be projected once the enclosing rows are grouped"));
        }
        if select.having.as_ref().is_some_and(expression_reads_outer) {
            return Err(planner_error("SQL correlated subquery cannot appear in HAVING"));
        }
        if select.order_by.as_ref().is_some_and(|items| items.iter().any(|item| expression_reads_outer(&item.expression))) {
            return Err(planner_error("SQL correlated subquery cannot appear in the ORDER BY of a grouped query"));
        }
    }
    if has_window && projection_expressions().any(|expression| expression_has_window(expression) && expression_reads_outer(expression)) {
        return Err(planner_error("SQL correlated subquery cannot be evaluated inside a window function"));
    }
    Ok(())
}

/// 窗口函数只允许出现在投影列表里：WHERE / GROUP BY / HAVING / ORDER BY 拿到窗口
/// 调用时显式拒绝，而不是在执行期按错误的行集合求值。
fn validate_window_placement(select: &SqlCatalogBoundSelect) -> Result<(), SqlFrontendError> {
    if select.predicate.as_ref().is_some_and(expression_has_window) {
        return Err(planner_error("SQL window functions are not allowed in WHERE"));
    }
    if select.group_by.as_ref().is_some_and(|items| items.iter().any(expression_has_window)) {
        return Err(planner_error("SQL window functions are not allowed in GROUP BY"));
    }
    if select.having.as_ref().is_some_and(expression_has_window) {
        return Err(planner_error("SQL window functions are not allowed in HAVING"));
    }
    if select.order_by.as_ref().is_some_and(|items| items.iter().any(|item| expression_has_window(&item.expression))) {
        return Err(planner_error("SQL window functions are not allowed in ORDER BY"));
    }
    Ok(())
}

/// 分组查询只允许投影分组列/分组表达式或聚合函数，避免返回任意组内行。
fn validate_grouped_select(select: &SqlCatalogBoundSelect, has_aggregate: bool) -> Result<(), SqlFrontendError> {
    let Some(group_expressions) = select.group_by.as_ref()
    else {
        let Some(having) = select.having.as_ref()
        else {
            return Ok(());
        };
        if !has_aggregate || !grouped_expression_allowed(having, &[], &[]) {
            return Err(planner_error("SQL HAVING without GROUP BY requires aggregate projections and aggregate-only predicates"));
        }
        return Ok(());
    };
    let group_columns: Vec<u64> = group_expressions
        .iter()
        .filter_map(|expression| match expression {
            SqlCatalogBoundExpression::Column(column) => Some(column.row_id),
            _ => None,
        })
        .collect();
    let allowed = |expression: &SqlCatalogBoundExpression| grouped_expression_allowed(expression, group_expressions, &group_columns);
    for projection in &select.projections {
        match projection {
            SqlCatalogBoundProjection::Star(_) => {
                return Err(planner_error("SELECT * cannot be combined with GROUP BY"));
            }
            SqlCatalogBoundProjection::Expression { expression, .. } if !allowed(expression) => {
                let detail = match expression {
                    SqlCatalogBoundExpression::Column(column) => format!("column `{}`", column.field_name),
                    other => format!("expression {other:?}"),
                };
                return Err(grouping_error(&format!("{detail} must appear in the GROUP BY clause or be used in an aggregate function")));
            }
            SqlCatalogBoundProjection::Expression { .. } => {}
        }
    }
    if let Some(having) = select.having.as_ref() {
        if !allowed(having) {
            return Err(planner_error("HAVING must reference GROUP BY columns or aggregate functions"));
        }
    }
    if let Some(items) = select.order_by.as_ref() {
        for item in items {
            if !allowed(&item.expression) {
                return Err(grouping_error("ORDER BY must reference GROUP BY columns or aggregate functions"));
            }
        }
    }
    Ok(())
}

fn grouped_expression_allowed(
    expression: &SqlCatalogBoundExpression,
    group_expressions: &[SqlCatalogBoundExpression],
    group_columns: &[u64],
) -> bool {
    if group_expressions.iter().any(|group| group == expression) {
        return true;
    }
    match expression {
        SqlCatalogBoundExpression::Column(column) => group_columns.contains(&column.row_id),
        // 外层列属于某一行外层输入，分组后不再有确定的取值来源。
        SqlCatalogBoundExpression::OuterColumn(_) => false,
        SqlCatalogBoundExpression::Literal(_) => true,
        SqlCatalogBoundExpression::Wildcard => false,
        SqlCatalogBoundExpression::Function { name, .. } if SqlAggregateFunction::from_name(name).is_some() => true,
        SqlCatalogBoundExpression::Function { name, args, .. } => {
            SqlScalarFunction::from_name(name).is_some()
                && args.iter().all(|arg| grouped_expression_allowed(arg, group_expressions, group_columns))
        }
        SqlCatalogBoundExpression::Unary { expression, .. } => grouped_expression_allowed(expression, group_expressions, group_columns),
        SqlCatalogBoundExpression::Binary { left, right, .. } => {
            grouped_expression_allowed(left, group_expressions, group_columns)
                && grouped_expression_allowed(right, group_expressions, group_columns)
        }
        SqlCatalogBoundExpression::InValues { expression, .. } => grouped_expression_allowed(expression, group_expressions, group_columns),
        SqlCatalogBoundExpression::InSubquery { .. } => false,
        // `EXISTS` 只依赖自己子查询的结果，对分组恒定，等同于常量布尔值。
        SqlCatalogBoundExpression::Exists { .. } => true,
        // 标量子查询同理：不相关子查询对每个分组都求同一个值。
        SqlCatalogBoundExpression::ScalarSubquery { .. } => true,
        SqlCatalogBoundExpression::Case { branches, else_branch } => {
            let branches_ok = branches.iter().all(|(w, t)| {
                grouped_expression_allowed(w, group_expressions, group_columns)
                    && grouped_expression_allowed(t, group_expressions, group_columns)
            });
            let else_ok = else_branch.as_ref().map_or(true, |e| grouped_expression_allowed(e, group_expressions, group_columns));
            branches_ok && else_ok
        }
        SqlCatalogBoundExpression::Cast { expr, .. } => grouped_expression_allowed(expr, group_expressions, group_columns),
    }
}

/// `DISTINCT` 只实现整条 SELECT 的投影去重；与分组/聚合组合的形式显式拒绝。
fn validate_distinct_shape(select: &SqlCatalogBoundSelect) -> Result<(), SqlFrontendError> {
    if !select.distinct {
        return Ok(());
    }
    if select.group_by.is_some() || select.having.is_some() {
        return Err(planner_error("SELECT DISTINCT cannot be combined with GROUP BY or HAVING"));
    }
    if select.projections.iter().any(|projection| match projection {
        SqlCatalogBoundProjection::Expression { expression, .. } => expression_has_aggregate(expression),
        SqlCatalogBoundProjection::Star(_) => false,
    }) {
        return Err(planner_error("SELECT DISTINCT cannot be combined with aggregate projections"));
    }
    Ok(())
}

/// `SELECT DISTINCT` 下 ORDER BY 只能引用投影里出现的表达式（PostgreSQL / MySQL 共同规则）。
fn validate_distinct_order_by(
    order_by: Option<&Vec<SqlCatalogBoundOrderByItem>>,
    projection_fields: &[(u64, String)],
    projection_expressions: &[(SqlCatalogBoundExpression, String)],
) -> Result<(), SqlFrontendError> {
    let Some(items) = order_by
    else {
        return Ok(());
    };
    for item in items {
        let projected = match &item.expression {
            SqlCatalogBoundExpression::Column(column) => projection_fields.iter().any(|(row_id, _)| *row_id == column.row_id),
            other => projection_expressions.iter().any(|(candidate, _)| candidate == other),
        };
        if !projected {
            return Err(planner_error("ORDER BY under SELECT DISTINCT must reference a projected expression"));
        }
    }
    Ok(())
}

fn planner_error(message: &str) -> SqlFrontendError {
    SqlFrontendError::unsupported(message)
}

/// 分组语义错误有独立的 SQLSTATE：PostgreSQL `42803`、MySQL `1055`。
fn grouping_error(message: &str) -> SqlFrontendError {
    SqlFrontendError::new(SqlErrorKind::InvalidGrouping, message)
}

#[cfg(test)]
mod default_value_tests {
    use super::{DefaultValue, parse_default_value};

    #[test]
    fn parses_constant_default_literals() {
        assert_eq!(parse_default_value("42"), Some(DefaultValue::Integer(42)));
        assert_eq!(parse_default_value("'x'"), Some(DefaultValue::String("x".to_owned())));
        assert_eq!(parse_default_value("NULL"), Some(DefaultValue::Null));
        assert_eq!(parse_default_value("TRUE"), Some(DefaultValue::Boolean(true)));
        assert_eq!(parse_default_value("1.5"), Some(DefaultValue::NumberBits(1.5_f64.to_bits())));
        assert_eq!(parse_default_value("CURRENT_TIMESTAMP"), None);
    }
}
