//! 读取、JOIN 与扫描执行路径。

use super::*;

/// Value of a projected column: what the stored row carries, otherwise the
/// column default declared by `ALTER TABLE ... ADD COLUMN ... DEFAULT`.
fn projected_value(
    row: &RowFieldValues,
    field_id: u64,
    field_types: &std::collections::BTreeMap<u64, database_query::catalog_bind::SqlFieldType>,
    field_defaults: &std::collections::BTreeMap<u64, database_query::DefaultValue>,
) -> SqlValue {
    row.value_typed(field_id, field_types)
        .or_else(|| field_defaults.get(&field_id).map(database_query::DefaultValue::to_sql_value))
        .unwrap_or(SqlValue::Null)
}

pub(crate) fn execute_read(
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
        Some(handle) => {
            handle.execute_read_batch(&[command.clone()]).map_err(|e| internal_error(&format!("TransactionHandle.read_batch failed: {e}")))?
        }
        None => {
            executor.execute_read_batch(None, &[command]).map_err(|e| internal_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?
        }
    };
    let result = batch_result.into_iter().next().ok_or_else(|| internal_error("KeyValueExecutor returned empty batch"))?;
    let row_bytes = match result {
        database_query::cluster_kv::KeyValueResult::Get(Some(bytes)) => bytes,
        _ => return Ok(SqlResult { columns: read_column_labels(plan), rows: Vec::new(), affected_rows: 0 }),
    };
    // 只解码本次读取实际需要的字段；值仍由 `value_typed` 按 catalog 类型解释。
    let needed_ids = expression_needed_field_ids(&plan.projection_fields, &plan.projection_expressions, plan.remaining_predicate.as_ref());
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
        row_values.push(projected_value(&row, *field_id, &plan.field_types, &plan.field_defaults));
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
pub(crate) fn execute_join(
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
    // 只引用基表列的 `WHERE` 合取项先在基表行上过滤：连接前的行数降一个量级，
    // 后续嵌套/哈希连接的成本随之下降。这些合取项在合并行上仍会再判一次，
    // 因此这里只是提前剪枝，不改变最终语义。
    if !plan.joins.is_empty() {
        if let Some(predicate) = &plan.remaining_predicate {
            let mut conjuncts = Vec::new();
            split_conjuncts(predicate, &mut conjuncts);
            let base_row_ids: Vec<u64> = plan.base.row_ids.clone();
            let pushable: Vec<&SqlCatalogBoundExpression> = conjuncts
                .iter()
                .filter(|conjunct| {
                    let mut columns = Vec::new();
                    database_query::catalog_bind::collect_column_ids(conjunct, &mut columns);
                    !columns.is_empty() && columns.iter().all(|column| base_row_ids.contains(column))
                })
                .copied()
                .collect();
            if !pushable.is_empty() {
                let mut kept = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut matches = true;
                    for conjunct in &pushable {
                        if !truthy(&eval_row_expression(conjunct, &row, &plan.field_types, dialect, executor, transaction)?) {
                            matches = false;
                            break;
                        }
                    }
                    if matches {
                        kept.push(row);
                    }
                }
                rows = kept;
            }
        }
    }
    for source in &plan.joins {
        let right_rows = scan_join_source(executor, transaction, source)?;
        let keep_left = matches!(source.kind, database_query::sql::SqlBoundJoinKind::Left | database_query::sql::SqlBoundJoinKind::Full);
        let keep_right = matches!(source.kind, database_query::sql::SqlBoundJoinKind::Right | database_query::sql::SqlBoundJoinKind::Full);
        let mut right_matched = vec![false; right_rows.len()];
        let mut merged = Vec::new();
        // `ON <左侧列> = <右侧列>` 是连接里最常见的形态。命中时先按右行建一次
        // 等值索引，左行只探针候选桶，候选再走完整 `ON` 求值，因此多出来的
        // `AND` 分支与 NULL 三值逻辑仍由原判定负责，不会被索引放宽。
        let hash_columns = source.on.as_ref().map(|on| join_hash_columns(on, &source.row_ids, &plan.field_types)).unwrap_or_default();
        let hash_index = (!hash_columns.is_empty()).then(|| build_join_hash_index(&right_rows, &hash_columns, plan));
        for left in &rows {
            let mut matched_any = false;
            match &hash_index {
                Some(index) => {
                    if let Some(key) = join_hash_key(left, &hash_columns, plan) {
                        if let Some(candidates) = index.get(&key) {
                            for candidate in candidates {
                                let mut row = left.clone();
                                row.merge(&right_rows[*candidate]);
                                if let Some(on) = &source.on {
                                    if !truthy(&eval_row_expression(on, &row, &plan.field_types, dialect, executor, transaction)?) {
                                        continue;
                                    }
                                }
                                merged.push(row);
                                matched_any = true;
                                right_matched[*candidate] = true;
                            }
                        }
                    }
                }
                None => {
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
                }
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

    // Aggregates run on the merged join output, so `COUNT(*)` over a join counts
    // joined rows rather than rows of either side.
    if plan.has_aggregate || plan.group_by.is_some() {
        let mut all_rows: Vec<RowFieldValues> = Vec::with_capacity(rows.len());
        for row in &rows {
            if let Some(predicate) = &plan.remaining_predicate {
                if !truthy(&eval_row_expression(predicate, row, &plan.field_types, dialect, executor, transaction)?) {
                    continue;
                }
            }
            all_rows.push(row.clone());
        }
        if let Some(group_exprs) = &plan.group_by {
            // Bucket rows by the GROUP BY key, then project and filter each bucket.
            let mut buckets: std::collections::BTreeMap<String, (Vec<SqlValue>, Vec<RowFieldValues>)> = std::collections::BTreeMap::new();
            for row in &all_rows {
                let key_vals: Vec<SqlValue> =
                    group_exprs.iter().map(|e| eval_expression(e, row, &plan.field_types, dialect)).collect::<Result<Vec<_>, _>>()?;
                let key_str = serialize_group_key(&key_vals);
                buckets.entry(key_str).or_insert_with(|| (key_vals, Vec::new())).1.push(row.clone());
            }
            let mut staged: Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)> = Vec::new();
            for (_key_str, (group_key_vals, bucket_rows)) in &buckets {
                let mut row_values: Vec<SqlValue> = Vec::new();
                for (field_id, _display_name) in &plan.projection_fields {
                    let Some(idx) = group_exprs.iter().position(|ge| matches!(ge, SqlCatalogBoundExpression::Column(col) if col.row_id == *field_id))
                    else {
                        return Err(internal_error("grouped projection references a column outside the GROUP BY key"));
                    };
                    row_values.push(group_key_vals[idx].clone());
                }
                for (expr, _) in &plan.projection_expressions {
                    row_values.push(eval_having(expr, bucket_rows, group_exprs, &plan.field_types, dialect)?);
                }
                if let Some(having) = &plan.having {
                    let keep = eval_having(having, bucket_rows, group_exprs, &plan.field_types, dialect)?;
                    if !truthy(&keep) {
                        continue;
                    }
                }
                let sort_keys: Vec<Option<SqlValue>> = match &plan.order_by {
                    Some(items) => items
                        .iter()
                        .map(|item| Ok(Some(eval_having(&item.expression, bucket_rows, group_exprs, &plan.field_types, dialect)?)))
                        .collect::<Result<Vec<_>, _>>()?,
                    None => Vec::new(),
                };
                staged.push((sort_keys, row_values));
            }
            return Ok(finish_scan_staged(plan, columns, staged));
        }
        // Global aggregate: one result row over every joined row.
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
                .map(|item| Ok(Some(eval_row_expression(&item.expression, row, &plan.field_types, dialect, executor, transaction)?)))
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };
        let mut row_values: Vec<SqlValue> = Vec::new();
        for (field_id, _) in &plan.projection_fields {
            row_values.push(projected_value(row, *field_id, &plan.field_types, &plan.field_defaults));
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
                        database_query::sql::SqlBoundOrderDirection::Asc => ord,
                        database_query::sql::SqlBoundOrderDirection::Desc => ord.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    let offset = plan.offset_after_sort;
    let limit = plan.limit_after_sort;
    let rows: Vec<Vec<SqlValue>> = staged.into_iter().skip(offset).take(if limit == 0 { usize::MAX } else { limit }).map(|(_, r)| r).collect();
    Ok(SqlResult { columns, rows, affected_rows: 0 })
}

/// 把顶层 `AND` 条件拆成合取项；非 `AND` 节点自身算一个合取项。
fn split_conjuncts<'a>(expression: &'a SqlCatalogBoundExpression, out: &mut Vec<&'a SqlCatalogBoundExpression>) {
    match expression {
        SqlCatalogBoundExpression::Binary { left, operator: SqlBoundBinaryOperator::And, right } => {
            split_conjuncts(left, out);
            split_conjuncts(right, out);
        }
        _ => out.push(expression),
    }
}

/// 从 JOIN 的 `ON` 条件里收集可以下推成哈希探针的等值列对。
///
/// 只接受「两侧都是 catalog 列、两侧字段类型完全相同、其中一侧属于本次连接来源」的
/// `=`，并沿 `AND` 递归。类型不同或任一侧是表达式时不参与索引：SQL 的隐式转换会让
/// 等值判断跳出原始字节，这类情况仍走完整的嵌套循环与 `ON` 求值。
fn join_hash_columns(
    on: &database_query::catalog_bind::SqlCatalogBoundExpression,
    right_row_ids: &[u64],
    field_types: &std::collections::BTreeMap<u64, database_query::catalog_bind::SqlFieldType>,
) -> Vec<(u64, u64)> {
    use database_query::catalog_bind::SqlCatalogBoundExpression;
    use database_query::sql::SqlBoundBinaryOperator;
    let mut pairs = Vec::new();
    fn collect(
        expression: &SqlCatalogBoundExpression,
        right_row_ids: &[u64],
        field_types: &std::collections::BTreeMap<u64, database_query::catalog_bind::SqlFieldType>,
        pairs: &mut Vec<(u64, u64)>,
    ) {
        match expression {
            SqlCatalogBoundExpression::Binary { left, operator: SqlBoundBinaryOperator::And, right } => {
                collect(left, right_row_ids, field_types, pairs);
                collect(right, right_row_ids, field_types, pairs);
            }
            SqlCatalogBoundExpression::Binary { left, operator: SqlBoundBinaryOperator::Equal, right } => {
                let (SqlCatalogBoundExpression::Column(left_column), SqlCatalogBoundExpression::Column(right_column)) =
                    (left.as_ref(), right.as_ref())
                else {
                    return;
                };
                let left_is_right = right_row_ids.contains(&left_column.row_id);
                let right_is_right = right_row_ids.contains(&right_column.row_id);
                if left_is_right == right_is_right {
                    return;
                }
                let (left_row_id, right_row_id) =
                    if left_is_right { (right_column.row_id, left_column.row_id) } else { (left_column.row_id, right_column.row_id) };
                let (Some(left_type), Some(right_type)) = (field_types.get(&left_row_id), field_types.get(&right_row_id)) else {
                    return;
                };
                if left_type == right_type {
                    pairs.push((left_row_id, right_row_id));
                }
            }
            _ => {}
        }
    }
    collect(on, right_row_ids, field_types, &mut pairs);
    pairs
}

/// 按右行的等值列建一次哈希桶，桶内保留右行下标以维持与嵌套循环一致的输出顺序。
fn build_join_hash_index(
    right_rows: &[RowFieldValues],
    columns: &[(u64, u64)],
    plan: &JoinPlan,
) -> std::collections::HashMap<String, Vec<usize>> {
    let mut index: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::with_capacity(right_rows.len());
    for (row_index, row) in right_rows.iter().enumerate() {
        if let Some(key) = join_hash_row_key(row, columns.iter().map(|(_, right)| *right), plan) {
            index.entry(key).or_default().push(row_index);
        }
    }
    index
}

/// 左行的探针键；任一列是 SQL `NULL` 时不可能等于任何右行，直接跳过。
fn join_hash_key(row: &RowFieldValues, columns: &[(u64, u64)], plan: &JoinPlan) -> Option<String> {
    join_hash_row_key(row, columns.iter().map(|(left, _)| *left), plan)
}

fn join_hash_row_key(
    row: &RowFieldValues,
    row_ids: impl Iterator<Item = u64>,
    plan: &JoinPlan,
) -> Option<String> {
    let mut values = Vec::new();
    for row_id in row_ids {
        let value = projected_value(row, row_id, &plan.field_types, &plan.field_defaults);
        if matches!(value, SqlValue::Null) {
            return None;
        }
        values.push(value);
    }
    Some(serialize_group_key(&values))
}

/// 整表扫描一个 JOIN 来源，只解码该表参与的字段，并按来源的 row id 存进合并行。
pub(crate) fn scan_join_source(
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
pub(crate) fn scan_rows(
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

pub(crate) fn execute_scan(
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
                    .map(|item| Ok(Some(eval_row_expression(&item.expression, row, &plan.field_types, dialect, executor, transaction)?)))
                    .collect::<Result<Vec<_>, _>>()?,
                None => Vec::new(),
            };
            let mut row_values: Vec<SqlValue> = Vec::new();
            for (field_id, _) in &plan.projection_fields {
                row_values.push(projected_value(row, *field_id, &plan.field_types, &plan.field_defaults));
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
        // `COUNT(*)` 一类不读任何列的聚合不需要解码行；只有带 WHERE 时才必须解码。
        let decode_needed = !needed_ids.is_empty() || plan.remaining_predicate.is_some();
        for (_rowid, value_bytes) in scanned {
            let row = if decode_needed {
                decode_row_fields(&value_bytes, &needed_ids)?
            }
            else {
                RowFieldValues::new()
            };
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
        let mut buckets: std::collections::BTreeMap<String, (Vec<SqlValue>, Vec<RowFieldValues>)> = std::collections::BTreeMap::new();
        for row in &all_rows {
            let key_vals: Vec<SqlValue> =
                group_exprs.iter().map(|e| eval_expression(e, row, &plan.field_types, dialect)).collect::<Result<Vec<_>, _>>()?;
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
                            database_query::sql::SqlBoundOrderDirection::Asc => ord,
                            database_query::sql::SqlBoundOrderDirection::Desc => ord.reverse(),
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
                row_values.push(projected_value(&row, *field_id, &plan.field_types, &plan.field_defaults));
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
                .map(|item| Ok(Some(eval_row_expression(&item.expression, &row, &plan.field_types, dialect, executor, transaction)?)))
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };
        let mut row_values: Vec<SqlValue> = Vec::new();
        for (field_id, _) in &plan.projection_fields {
            row_values.push(projected_value(&row, *field_id, &plan.field_types, &plan.field_defaults));
        }
        for (expr, _) in &plan.projection_expressions {
            row_values.push(eval_row_expression(expr, &row, &plan.field_types, dialect, executor, transaction)?);
        }
        staged.push((sort_keys, row_values));
    }
    Ok(finish_scan_staged(plan, columns, staged))
}

/// Tail shared by scan and join plans after projection values are produced.
pub(crate) trait StagedPlan {
    fn staged_distinct(&self) -> bool;
    fn staged_order_by(&self) -> Option<&Vec<database_query::catalog_bind::SqlCatalogBoundOrderByItem>>;
    fn staged_nulls_first(&self) -> bool;
    fn staged_offset(&self) -> usize;
    fn staged_limit(&self) -> usize;
}

impl StagedPlan for ScanPlan {
    fn staged_distinct(&self) -> bool {
        self.distinct
    }
    fn staged_order_by(&self) -> Option<&Vec<database_query::catalog_bind::SqlCatalogBoundOrderByItem>> {
        self.order_by.as_ref()
    }
    fn staged_nulls_first(&self) -> bool {
        self.nulls_first
    }
    fn staged_offset(&self) -> usize {
        self.offset_after_sort
    }
    fn staged_limit(&self) -> usize {
        self.limit_after_sort
    }
}

impl StagedPlan for JoinPlan {
    fn staged_distinct(&self) -> bool {
        self.distinct
    }
    fn staged_order_by(&self) -> Option<&Vec<database_query::catalog_bind::SqlCatalogBoundOrderByItem>> {
        self.order_by.as_ref()
    }
    fn staged_nulls_first(&self) -> bool {
        self.nulls_first
    }
    fn staged_offset(&self) -> usize {
        self.offset_after_sort
    }
    fn staged_limit(&self) -> usize {
        self.limit_after_sort
    }
}

/// 对已求值的投影行套用 `DISTINCT`、`ORDER BY` 与 `OFFSET` / `LIMIT`。
///
/// 常规扫描路径、连接路径与窗口路径共用这一段，保证 DISTINCT、NULL 排序和分页
/// 语义只实现一次。
pub(crate) fn finish_scan_staged(
    plan: &impl StagedPlan,
    columns: Vec<String>,
    mut staged: Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)>,
) -> SqlResult {
    if plan.staged_distinct() {
        retain_distinct_projected_rows(&mut staged);
    }

    // Sort if ORDER BY present — multi-column, 逐列比较直到分出胜负。
    let nulls_first = plan.staged_nulls_first();
    if let Some(order_items) = plan.staged_order_by() {
        let orderings = order_items
            .iter()
            .map(|item| (item.direction.clone(), order_nulls_first(&item.direction, item.nulls, nulls_first)))
            .collect::<Vec<_>>();
        staged.sort_by(move |a, b| {
            for (idx, (dir, nulls_first)) in orderings.iter().enumerate() {
                let ord = compare_values_with_nulls(a.0[idx].as_ref(), b.0[idx].as_ref(), *nulls_first);
                if ord != std::cmp::Ordering::Equal {
                    return match dir {
                        database_query::sql::SqlBoundOrderDirection::Asc => ord,
                        database_query::sql::SqlBoundOrderDirection::Desc => ord.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    let offset = plan.staged_offset();
    let limit = plan.staged_limit();
    let rows: Vec<Vec<SqlValue>> = staged.into_iter().skip(offset).take(if limit == 0 { usize::MAX } else { limit }).map(|(_, r)| r).collect();

    SqlResult { columns, rows, affected_rows: 0 }
}

/// 执行层内部：Top-N heap 用的可排序行。
/// Ord 语义：`is_asc` → sort_key 越大 Greater；`!is_asc` → sort_key 越小 Greater。
/// BinaryHeap（max-heap）会把 Greater 的堆顶 → ASC heap 顶是最大的（被 pop 掉，保留小的 N 个），
/// DESC heap 顶是最小的（被 pop 掉，保留大的 N 个）。
#[derive(Debug)]
pub(crate) struct SortableRow {
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
        if self.is_asc { natural } else { natural.reverse() }
    }
}

pub(crate) fn read_column_labels(plan: &ReadPlan) -> Vec<String> {
    let mut labels = Vec::new();
    for (_, name) in &plan.projection_fields {
        labels.push(name.clone());
    }
    for (_, name) in &plan.projection_expressions {
        labels.push(name.clone());
    }
    labels
}

pub(crate) fn scan_column_labels(plan: &ScanPlan) -> Vec<String> {
    let mut labels = Vec::new();
    for (_, name) in &plan.projection_fields {
        labels.push(name.clone());
    }
    for (_, name) in &plan.projection_expressions {
        labels.push(name.clone());
    }
    labels
}

pub(crate) fn join_column_labels(plan: &JoinPlan) -> Vec<String> {
    let mut labels = Vec::new();
    for (_, name) in &plan.projection_fields {
        labels.push(name.clone());
    }
    for (_, name) in &plan.projection_expressions {
        labels.push(name.clone());
    }
    labels
}

#[cfg(test)]
mod tests {
    use super::*;
    use database_query::{DefaultValue, RowFieldValues};

    #[test]
    fn added_column_reads_the_declared_default_from_a_short_row() {
        let row = RowFieldValues::new();
        let field_types = std::collections::BTreeMap::new();
        let mut defaults = std::collections::BTreeMap::new();
        defaults.insert(42_u64, DefaultValue::Integer(7));
        assert_eq!(projected_value(&row, 42, &field_types, &defaults), SqlValue::Integer(7));
        assert_eq!(projected_value(&row, 43, &field_types, &defaults), SqlValue::Null);
    }
}
