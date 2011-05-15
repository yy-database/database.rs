//! 写入、冲突策略与约束校验。

use super::*;

pub(crate) fn execute_insert(
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
pub(crate) fn insert_row_if_absent(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &WritePlan,
    do_nothing: bool,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let row = write_plan_to_row(plan);
    if let Some(constraint) = unique_conflict_on_write(executor, transaction, &plan.namespace, &plan.unique_constraints, &row, None)? {
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
pub(crate) fn upsert_row(
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
            if let Some(constraint) = unique_conflict_on_write(executor, transaction, &plan.namespace, &plan.unique_constraints, &row, None)? {
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
        let command = KeyValueCommand { namespace: namespace.clone(), key: plan.rowid.clone(), action: KeyValueAction::Put(encoded) };
        match transaction {
            Some(handle) => handle
                .write_if_unchanged(command, Some(existing_bytes))
                .map_err(|e| write_error(format!("TransactionHandle.upsert failed: {e}"), &e))?,
            None => executor.execute(None, command).map_err(|e| internal_error(&format!("KeyValueExecutor.upsert failed: {e}")))?,
        };
        return returning_result(&plan.returning, &plan.field_types, vec![row], 1, dialect);
    }
    Err(SqlFrontendError::new(SqlErrorKind::Conflict, format!("concurrent write to row in {} prevented the conflict update", plan.namespace)))
}

pub(crate) fn put_if_absent(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    namespace: &str,
    rowid: &[u8],
    bytes: Vec<u8>,
) -> Result<bool, SqlFrontendError> {
    let command =
        KeyValueCommand { namespace: Namespace(namespace.to_string()), key: rowid.to_vec(), action: KeyValueAction::PutIfAbsent(bytes) };
    let result = match transaction {
        Some(handle) => handle.execute(command).map_err(|e| internal_error(&format!("TransactionHandle.insert failed: {e}")))?,
        None => executor.execute(None, command).map_err(|e| internal_error(&format!("KeyValueExecutor.insert failed: {e}")))?,
    };
    match result {
        database_query::cluster_kv::KeyValueResult::PutIfAbsent { revision } => Ok(revision.is_some()),
        other => Err(internal_error(&format!("unexpected storage result for insert: {other:?}"))),
    }
}

pub(crate) fn read_row_bytes(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    namespace: &Namespace,
    rowid: &[u8],
) -> Result<Option<Vec<u8>>, SqlFrontendError> {
    let command = KeyValueCommand { namespace: namespace.clone(), key: rowid.to_vec(), action: KeyValueAction::Get };
    let results = match transaction {
        Some(handle) => {
            handle.execute_read_batch(&[command]).map_err(|e| internal_error(&format!("TransactionHandle.read_batch failed: {e}")))?
        }
        None => {
            executor.execute_read_batch(None, &[command]).map_err(|e| internal_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?
        }
    };
    Ok(match results.into_iter().next() {
        Some(database_query::cluster_kv::KeyValueResult::Get(bytes)) => bytes,
        _ => None,
    })
}

/// 把 `RETURNING` 投影应用到真正落库的行上。没有 `RETURNING` 时写语句不返回结果集。
pub(crate) fn returning_result(
    returning: &Option<ReturningProjections>,
    field_types: &BTreeMap<u64, database_query::catalog_bind::SqlFieldType>,
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

pub(crate) fn returning_column_labels(projections: &ReturningProjections) -> Vec<String> {
    projections.iter().map(|(_, label)| label.clone()).collect()
}

pub(crate) fn execute_multi_insert(
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
pub(crate) struct BatchedRow {
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
pub(crate) fn execute_multi_insert_atomic(
    executor: &dyn KeyValueExecutor,
    plans: &[WritePlan],
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let first = plans.first().expect("atomic multi-insert requires at least one plan");
    let namespace = Namespace(first.namespace.clone());
    let unique_conflict =
        || SqlFrontendError::new(SqlErrorKind::DuplicateKey, format!("duplicate key value violates unique constraint: {}", first.namespace));
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
                    database_query::cluster_kv::KeyValueResult::Get(bytes) => bytes,
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
                        BatchedRow { current: Some(write_plan_to_row(plan)), applied: true, condition: CommitCondition::PutIfAbsent },
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
                            if let Some(updated) = conflict_updated_row(existing_row, plan, assignments, predicate.as_ref(), dialect)? {
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
            Err(database_types::cluster::Error::CasConflict { .. }) if needs_preimage && attempt == 0 => continue,
            Err(database_types::cluster::Error::CasConflict { .. }) if needs_preimage => {
                return Err(SqlFrontendError::new(
                    SqlErrorKind::Conflict,
                    format!("concurrent writes to {} prevented the batch commit", first.namespace),
                ));
            }
            Err(database_types::cluster::Error::CasConflict { .. }) => return Err(unique_conflict()),
            Err(error) => {
                return Err(internal_error(&format!("KeyValueExecutor.commit_batch failed: {error}")));
            }
        }
    }
    Err(SqlFrontendError::new(SqlErrorKind::Conflict, format!("concurrent writes to {} prevented the batch commit", first.namespace)))
}

/// 计算 `ON CONFLICT DO UPDATE` 命中既有行后的最终行值。
///
/// 谓词只对既有行求值，不成立时返回 `None` 表示跳过；赋值在既有行与 `EXCLUDED` 行合并后的
/// 作用域里求值（列带 `ROW_ID_STRIDE` 偏移），再统一应用，等价于 PostgreSQL 的「同时赋值」语义。
/// 单行 `upsert_row` 与批量 `execute_multi_insert_atomic` 共用本函数，冲突语义只有一份。
pub(crate) fn conflict_updated_row(
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
pub(crate) fn execute_insert_select(
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
            fields.push(WriteField { field_id: column.field_id, value, field_type: column.field_type.clone(), capacity: column.capacity });
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
pub(crate) fn ensure_insert_targets_absent(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plans: &[WritePlan],
) -> Result<(), SqlFrontendError> {
    let Some(first) = plans.first()
    else {
        return Ok(());
    };
    let conflict = |plan: &WritePlan| {
        SqlFrontendError::new(SqlErrorKind::DuplicateKey, format!("duplicate key value violates unique constraint: {}", plan.namespace))
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
        Some(handle) => {
            handle.execute_read_batch(&commands).map_err(|e| internal_error(&format!("TransactionHandle.read_batch failed: {e}")))?
        }
        None => {
            executor.execute_read_batch(None, &commands).map_err(|e| internal_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?
        }
    };
    if results.iter().any(|result| matches!(result, database_query::cluster_kv::KeyValueResult::Get(Some(_)))) {
        return Err(conflict(first));
    }
    Ok(())
}

/// 写入前确认候选行之间与目标表内都不存在 `UNIQUE` 冲突。
pub(crate) fn ensure_unique_targets_absent(
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
        let pending_without_self: Vec<(Vec<u8>, RowFieldValues)> = pending.iter().filter(|(rowid, _)| rowid != &plan.rowid).cloned().collect();
        if let Some(constraint) =
            unique_conflict_in_snapshot(&first.unique_constraints, &row, Some(&plan.rowid), &pending_without_self, &stored)?
        {
            return Err(duplicate_key_error(&constraint));
        }
        pending.push((plan.rowid.clone(), row));
    }
    Ok(())
}

pub(crate) fn execute_update(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &UpdatePlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    let get_cmd = KeyValueCommand { namespace: namespace.clone(), key: plan.rowid.clone(), action: KeyValueAction::Get };
    let batch_result = match transaction {
        Some(handle) => {
            handle.execute_read_batch(&[get_cmd.clone()]).map_err(|e| internal_error(&format!("TransactionHandle.read_batch failed: {e}")))?
        }
        None => {
            executor.execute_read_batch(None, &[get_cmd]).map_err(|e| internal_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?
        }
    };
    let existing_bytes = match batch_result.into_iter().next() {
        Some(database_query::cluster_kv::KeyValueResult::Get(Some(b))) => b,
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

pub(crate) fn execute_delete(
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
            Some(database_query::cluster_kv::KeyValueResult::Get(Some(b))) => b,
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
        Some(handle) => {
            handle.write_if_unchanged(command, pre_image).map_err(|e| write_error(format!("TransactionHandle.delete failed: {e}"), &e))?
        }
        // 单行 DELETE 的自动提交同样是一条命令、一个 durable 提交点。
        None => executor.execute(None, command).map_err(|e| internal_error(&format!("KeyValueExecutor.delete failed: {e}")))?,
    };
    let affected = match delete_result {
        database_query::cluster_kv::KeyValueResult::Delete { removed: true } => 1,
        _ => 0,
    };
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: affected })
}

/// 一条语句扫描加谓词求值后命中的一行：最终写入命令与扫描时看到的 pre-image。
///
/// `pre_image` 会作为 `CommitCondition::ValueEquals(Some(pre_image))` 交给提交边界复核，
/// 因此读改写与删除共享同一套 OCC 语义，不随语句动词分叉。
pub(crate) struct StatementChange {
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
pub(crate) fn apply_statement_changes(
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
                .map(|change| BufferedCommit { command: change.command, condition: Some(CommitCondition::ValueEquals(Some(change.pre_image))) })
                .collect();
            executor.commit_batch(None, batch).map_err(|error| match error {
                database_types::cluster::Error::CasConflict { .. } => SqlFrontendError::new(
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
pub(crate) fn execute_update_scan(
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
            if let Some(constraint) = unique_conflict_in_snapshot(&plan.unique_constraints, &row, Some(&rowid), &pending_without_self, &stored)?
            {
                return Err(duplicate_key_error(&constraint));
            }
            pending.push((rowid.clone(), row.clone()));
        }
        changes.push(StatementChange {
            command: KeyValueCommand { namespace: namespace.clone(), key: rowid, action: KeyValueAction::Put(encode_row(&row)) },
            pre_image,
        });
    }
    let affected = apply_statement_changes(executor, transaction, changes, &namespace, "update_scan")?;
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: affected })
}

/// 多行 DELETE：扫描 rowid 区间，逐行判定 WHERE 后删除，返回真实影响行数。
///
/// 与多行 UPDATE 共用同一条语句变更集与提交边界，删除命中行在一次 durable commit 内发布。
pub(crate) fn execute_delete_scan(
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

pub(crate) fn write_plan_to_row(plan: &WritePlan) -> RowFieldValues {
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
pub(crate) fn constraint_tuple(row: &RowFieldValues, columns: &[(u64, String)]) -> Option<Vec<Vec<u8>>> {
    let mut tuple = Vec::with_capacity(columns.len());
    for (field_id, _) in columns {
        tuple.push(row.get(*field_id)?.to_vec());
    }
    Some(tuple)
}

/// 候选行与既有行构成的 `UNIQUE` 冲突描述。
pub(crate) fn unique_conflict_between(
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
pub(crate) fn unique_conflict_in_snapshot(
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
pub(crate) fn unique_conflict_on_write(
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
pub(crate) fn duplicate_key_error(constraint: &str) -> SqlFrontendError {
    SqlFrontendError::new(SqlErrorKind::DuplicateKey, format!("duplicate key value violates unique constraint {constraint}"))
}

pub(crate) fn sql_value_to_bytes(lit: &SqlBoundLiteral) -> Option<Vec<u8>> {
    match lit {
        SqlBoundLiteral::Integer(value) => Some(value.to_string().into_bytes()),
        SqlBoundLiteral::Number(text) => Some(text.as_bytes().to_vec()),
        SqlBoundLiteral::String(text) => Some(text.as_bytes().to_vec()),
        SqlBoundLiteral::Bytes(bytes) => Some(bytes.clone()),
        SqlBoundLiteral::Boolean(v) => Some((if *v { "1" } else { "0" }).as_bytes().to_vec()),
        SqlBoundLiteral::Null => None,
    }
}
