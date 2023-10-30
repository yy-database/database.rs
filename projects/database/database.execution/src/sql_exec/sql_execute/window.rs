//! 窗口函数求值与分组聚合。

use super::*;

/// 求值一个窗口函数投影：按 `PARTITION BY` 分桶，桶内按窗口 `ORDER BY` 排序，
/// 再为每一行算出排名值或聚合窗口值。返回值按输入行下标对齐。
pub(crate) fn eval_window_projection(
    expr: &SqlCatalogBoundExpression,
    rows: &[RowFieldValues],
    field_types: &std::collections::BTreeMap<u64, database_query::catalog_bind::SqlFieldType>,
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
            let value = args.first().ok_or_else(|| internal_error(&format!("window function {name}() is missing its value argument")))?;
            let offset = match args.get(1) {
                Some(SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Integer(offset))) if *offset >= 0 => *offset,
                Some(_) => {
                    return Err(internal_error(&format!("window function {name}() requires the row offset to be an integer literal >= 0")));
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
        let key_values =
            spec.partition_by.iter().map(|partition| eval_expression(partition, row, field_types, dialect)).collect::<Result<Vec<_>, _>>()?;
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
                let descending = matches!(spec.order_by.first().map(|item| &item.direction), Some(SqlBoundOrderDirection::Desc));
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
                    let buckets = ntile_buckets.ok_or_else(|| internal_error("window function NTILE() is missing its bucket count"))?;
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
                    let (frame_start, frame_end) =
                        window_frame_bounds(spec, position, peer_start[position], peer_end[position], ordered_rows.len(), range_axis.as_ref())?;
                    let frame = if frame_start < frame_end { &ordered_rows[frame_start..frame_end] } else { &ordered_rows[0..0] };
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
pub(crate) fn window_frame_bounds(
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
pub(crate) fn range_frame_bounds(
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
                if axis.is_null(position) { if end { peer_end } else { peer_start } } else { axis.offset_bound(position, offset, false, end) }
            }
            SqlBoundWindowFrameBound::Following(offset) => {
                let axis = range_axis.ok_or_else(missing_axis)?;
                if axis.is_null(position) { if end { peer_end } else { peer_start } } else { axis.offset_bound(position, offset, true, end) }
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
pub(crate) enum RangeAxis {
    Integers(Vec<Option<i128>>),
    Numbers(Vec<Option<f64>>),
}

impl RangeAxis {
    /// 从分区排序键构建值轴。带偏移的 `RANGE` 帧按值比较，值距离只有单列
    /// `ORDER BY` 才有唯一依据；`NULL` 键不产生距离，但仍可参与排序，因此保留
    /// `None` 而不是拒绝整个分区。
    fn build(keyed: &[(Vec<Option<SqlValue>>, usize)], descending: bool, order_columns: usize) -> Result<Self, SqlFrontendError> {
        if order_columns != 1 {
            return Err(SqlFrontendError::unsupported("window RANGE frame with an offset requires exactly one ORDER BY column"));
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
        if saw_number { Ok(Self::Numbers(numbers)) } else { Ok(Self::Integers(integers)) }
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
                offset_index(&values[run_start..run_end], run_start, values.len(), target, end, |value, target| value.cmp(&target))
            }
            Self::Numbers(values) => {
                let step = offset as f64;
                let current = values[position].expect("offset bounds start from a non-NULL key");
                let target = if following { current + step } else { current - step };
                offset_index(&values[run_start..run_end], run_start, values.len(), target, end, |value, target| value.total_cmp(&target))
            }
        }
    }
}

/// 在一段升序、无 `NULL` 的排序键里定位偏移目标：起点 bound 取第一个不小于目标的
/// 行，终点 bound 取第一个大于目标的行（开区间上界）。段内没有命中时起终点分别
/// 退到分区两端，让调用方得到空帧。
pub(crate) fn offset_index<T: Copy>(
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
pub(crate) fn frame_bound_position(bound: SqlBoundWindowFrameBound, position: usize) -> i64 {
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
pub(crate) fn peer_keys_equal(left: &[Option<SqlValue>], right: &[Option<SqlValue>]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| compare_values_with_nulls(left.as_ref(), right.as_ref(), false) == std::cmp::Ordering::Equal)
}

/// 对一个聚合函数在所有收集到的行上求值，返回单个 SqlValue。
pub(crate) fn eval_aggregate(
    expr: &SqlCatalogBoundExpression,
    rows: &[RowFieldValues],
    field_types: &std::collections::BTreeMap<u64, database_query::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    let (name, args, distinct) = match expr {
        SqlCatalogBoundExpression::Function { name, args, distinct, .. } => (name.clone(), args.clone(), *distinct),
        _ => return Err(internal_error(&format!("eval_aggregate: non-Function expression encountered: {expr:?}"))),
    };

    let agg = SqlAggregateFunction::from_name(&name).ok_or_else(|| internal_error(&format!("unknown aggregate function: {name}")))?;

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
            if count > 0 { Ok(SqlValue::Number(sum / count as f64)) } else { Ok(SqlValue::Null) }
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
                        let replace = if want_greater { order == std::cmp::Ordering::Greater } else { order == std::cmp::Ordering::Less };
                        if replace { value } else { current }
                    }
                    None => value,
                });
            }
            Ok(best.unwrap_or(SqlValue::Null))
        }
    }
}

/// 把 GROUP BY 的 key values 序列化成稳定字符串，用作 BTreeMap bucket key。
pub(crate) fn serialize_group_key(values: &[SqlValue]) -> String {
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
                buf.push_str(&database_query::sql::encode_hex(bytes));
            }
        }
    }
    buf
}

/// 在 GROUP BY bucket 上下文中求值表达式。
/// 与 `eval_expression` 的区别：Function 会被当作聚合函数对 bucket_rows 求值；
/// Column 从 bucket 首行取（同一 bucket 内 group key 列值相同）。
pub(crate) fn eval_having(
    expr: &SqlCatalogBoundExpression,
    bucket_rows: &[RowFieldValues],
    group_exprs: &[SqlCatalogBoundExpression],
    field_types: &std::collections::BTreeMap<u64, database_query::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    let first = bucket_rows.first();
    Ok(match expr {
        SqlCatalogBoundExpression::Column(col) => first.and_then(|row| row.value_typed(col.row_id, field_types)).unwrap_or(SqlValue::Null),
        SqlCatalogBoundExpression::Literal(lit) => SqlValue::from_literal(lit),
        SqlCatalogBoundExpression::Wildcard => return Err(internal_error("wildcard `*` is only valid as the argument of COUNT")),
        // 分组路径不接受相关子查询，计划期已拒绝，走到这里是内部漏检。
        SqlCatalogBoundExpression::OuterColumn(_) => {
            return Err(internal_error("outer column reference is not allowed in a grouped expression"));
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
        SqlCatalogBoundExpression::InValues { expression, values, negated, sorted_integers } => {
            let target = eval_having(expression, bucket_rows, group_exprs, field_types, dialect)?;
            eval_in_values(target, values, *negated, *sorted_integers, dialect)?
        }
        SqlCatalogBoundExpression::InSubquery { .. } => return Err(internal_error("IN subquery was not materialized before execution")),
        SqlCatalogBoundExpression::Exists { .. } => return Err(internal_error("EXISTS predicate was not materialized before execution")),
        SqlCatalogBoundExpression::ScalarSubquery { .. } => return Err(internal_error("scalar subquery was not evaluated before execution")),
        SqlCatalogBoundExpression::Case { branches, else_branch } => {
            eval_case_having(branches, else_branch, bucket_rows, group_exprs, field_types, dialect)?
        }
        SqlCatalogBoundExpression::Cast { expr, target } => {
            let inner = eval_having(expr, bucket_rows, group_exprs, field_types, dialect)?;
            eval_cast(inner, target, matches!(expr.as_ref(), SqlCatalogBoundExpression::Literal(_)))?
        }
    })
}
