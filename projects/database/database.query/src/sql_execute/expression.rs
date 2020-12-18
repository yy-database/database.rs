//! 表达式求值、CASE/CAST、比较与排序语义。

use super::*;

/// Evaluates a clause expression that has no row context (`LIMIT` / `OFFSET`).
///
/// The planner folds these clauses before execution, and it deliberately uses
/// this same evaluator so the fold cannot disagree with runtime evaluation of
/// the same expression (arithmetic overflow, division semantics, function
/// results). The caller only accepts a non-negative integer scalar.
pub(crate) fn eval_constant(expression: &SqlCatalogBoundExpression, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
    eval_expression(expression, &RowFieldValues::new(), &std::collections::BTreeMap::new(), dialect)
}

/// Evaluates one bound expression against a single row.
pub fn eval_expression(
    expr: &SqlCatalogBoundExpression,
    row: &RowFieldValues,
    field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    Ok(match expr {
        // 缺失字段就是 SQL NULL（写入 NULL 时字段会被省略），比较和投影都按 NULL 处理。
        SqlCatalogBoundExpression::Column(col) => row.value_typed(col.row_id, field_types).unwrap_or(SqlValue::Null),
        SqlCatalogBoundExpression::Literal(lit) => SqlValue::from_literal(lit),
        SqlCatalogBoundExpression::Wildcard => return Err(internal_error("wildcard `*` is only valid as the argument of COUNT")),
        // 外层列应当在逐行求值时先用该行外层值替换掉，走到这里说明替换被漏掉。
        SqlCatalogBoundExpression::OuterColumn(_) => {
            return Err(internal_error("outer column reference was not substituted before evaluation"));
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
            return Err(internal_error("window function was not evaluated by the window pass"));
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
        SqlCatalogBoundExpression::InSubquery { .. } => return Err(internal_error("IN subquery was not materialized before execution")),
        SqlCatalogBoundExpression::Exists { .. } => return Err(internal_error("EXISTS predicate was not materialized before execution")),
        SqlCatalogBoundExpression::ScalarSubquery { .. } => return Err(internal_error("scalar subquery was not evaluated before execution")),
        SqlCatalogBoundExpression::Case { branches, else_branch } => eval_case(branches, else_branch, row, field_types, dialect)?,
        SqlCatalogBoundExpression::Cast { expr, target } => {
            let inner = eval_expression(expr, row, field_types, dialect)?;
            eval_cast(inner, target, matches!(expr.as_ref(), SqlCatalogBoundExpression::Literal(_)))?
        }
    })
}

/// 对 CASE WHEN ... THEN ... [ELSE ...] END 求值。
///
/// 依次检查每个 WHEN 条件：第一个求值为 TRUE 的触发对应 THEN 分支；
/// 条件为 FALSE 或 NULL 都继续下一个分支。全部落空时走 ELSE（若有）
/// 否则返回 NULL。
pub(crate) fn eval_case(
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

/// CAST(expr AS type_name) 求值。支持 INTEGER/INT, REAL/DOUBLE/FLOAT/NUMBER, TEXT/STRING/VARCHAR/CHAR, BLOB/BINARY 互转。
///
/// `from_literal` 区分两种文本→二进制来源：字面量按十六进制解码（wire 层把二进制
/// 入参渲染成 `CAST('<hex>' AS BINARY)`），其余表达式是恒等字节视图
/// （Iris 用 `CAST(f AS BINARY)` 规避 MySQL collation）。
///
/// 类型不支持或无法转换时返回 NULL（与 SQLite/Postgres CAST 行为一致），而不是报错。
pub fn eval_cast(value: SqlValue, target: &str, from_literal: bool) -> Result<SqlValue, SqlFrontendError> {
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
        (SqlValue::Boolean(b), "INTEGER" | "INT" | "INT4" | "BIGINT" | "SMALLINT") => Ok(SqlValue::Integer(if b { 1 } else { 0 })),
        (SqlValue::Boolean(b), "REAL" | "DOUBLE" | "FLOAT" | "NUMBER") => Ok(SqlValue::Number(if b { 1.0 } else { 0.0 })),
        (SqlValue::Boolean(b), "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "CHARACTER") => {
            Ok(SqlValue::String(if b { "TRUE".to_string() } else { "FALSE".to_string() }))
        }
        // MySQL's `CAST(... AS SIGNED|UNSIGNED)` truncates toward zero and reads a
        // numeric prefix from text, returning 0 when no prefix exists.
        (SqlValue::Integer(n), "SIGNED" | "UNSIGNED") => Ok(SqlValue::Integer(n)),
        (SqlValue::Number(n), "SIGNED" | "UNSIGNED") => Ok(SqlValue::Integer(n as i64)),
        (SqlValue::Boolean(b), "SIGNED" | "UNSIGNED") => Ok(SqlValue::Integer(if b { 1 } else { 0 })),
        (SqlValue::String(text), "SIGNED" | "UNSIGNED") => Ok(SqlValue::Integer(mysql_integer_prefix(&text))),
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

/// Reads the leading decimal integer from text the way MySQL does for
/// `CAST(... AS SIGNED)`: optional sign, digits, then stop. No digits yields 0
/// and an out-of-range value saturates instead of wrapping.
fn mysql_integer_prefix(text: &str) -> i64 {
    let trimmed = text.trim_start();
    let bytes = trimmed.as_bytes();
    let mut end = usize::from(bytes.first().is_some_and(|byte| *byte == b'+' || *byte == b'-'));
    let digits_start = end;
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end == digits_start {
        return 0;
    }
    trimmed[..end].parse::<i64>().unwrap_or(if trimmed.starts_with('-') { i64::MIN } else { i64::MAX })
}

/// Evaluates `expr IN (...)` with SQL three-valued logic.
pub fn eval_in_values(target: SqlValue, values: &[SqlBoundLiteral], negated: bool, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
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

/// Applies one unary operator to an already evaluated value.
pub fn eval_unary(op: SqlBoundUnaryOperator, operand: SqlValue) -> Result<SqlValue, SqlFrontendError> {
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

/// Applies one binary operator to two already evaluated values.
pub fn eval_binary(op: SqlBoundBinaryOperator, left: &SqlValue, right: &SqlValue, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
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
pub(crate) fn is_comparison(op: SqlBoundBinaryOperator) -> bool {
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
pub(crate) fn comparison_value(op: SqlBoundBinaryOperator, ordering: std::cmp::Ordering) -> SqlValue {
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
pub(crate) fn compare_integer_float(integer: i64, float: f64) -> Option<std::cmp::Ordering> {
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
pub(crate) fn integer_binary(op: SqlBoundBinaryOperator, a: i64, b: i64, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
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
pub(crate) fn float_binary(op: SqlBoundBinaryOperator, a: f64, b: f64, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
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
pub(crate) fn like_match(haystack: &str, pattern: &str) -> bool {
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

/// SQL value equality used by `SELECT DISTINCT`.
///
/// `NULL` equals `NULL` for de-duplication, integers and floats compare by exact
/// numeric value (never through a lossy `f64` cast for `i64`), and unrelated
/// storage variants are never collapsed into one row.
/// 投影去重与集合运算用的值相等比较。
pub fn values_equal(left: &SqlValue, right: &SqlValue) -> bool {
    use std::cmp::Ordering;
    match (left, right) {
        (SqlValue::Integer(left), SqlValue::Integer(right)) => left == right,
        (SqlValue::Integer(integer), SqlValue::Number(float)) => compare_integer_float(*integer, *float) == Some(Ordering::Equal),
        (SqlValue::Number(float), SqlValue::Integer(integer)) => compare_integer_float(*integer, *float) == Some(Ordering::Equal),
        (SqlValue::Number(left), SqlValue::Number(right)) => left.partial_cmp(right).unwrap_or(Ordering::Equal) == Ordering::Equal,
        (SqlValue::String(left), SqlValue::String(right)) => eq_string(left, right),
        (SqlValue::Boolean(left), SqlValue::Boolean(right)) => left == right,
        (SqlValue::Null, SqlValue::Null) => true,
        _ => false,
    }
}

/// 投影行（按列）相等比较。
pub fn projected_rows_equal(left: &[SqlValue], right: &[SqlValue]) -> bool {
    left.len() == right.len() && left.iter().zip(right.iter()).all(|(left, right)| values_equal(left, right))
}

/// Keeps the first occurrence of each projected row, preserving scan order.
/// 保留每个投影行的首次出现，维持扫描顺序。
pub fn retain_distinct_projected_rows(staged: &mut Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)>) {
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
pub fn compare_values(a: Option<&SqlValue>, b: Option<&SqlValue>) -> std::cmp::Ordering {
    compare_values_with_nulls(a, b, false)
}

/// 解析 ORDER BY 项排序前的 NULL 置前标记：未写 `NULLS FIRST/LAST` 时用方言默认，
/// 显式子句则在 ASC 或 DESC 下分别映射为置前或置后。
/// 解析 ORDER BY 项的 NULL 置前标记。
pub fn order_nulls_first(direction: &SqlBoundOrderDirection, explicit: Option<SqlNullsOrder>, dialect_default: bool) -> bool {
    match explicit {
        None => dialect_default,
        Some(SqlNullsOrder::First) => matches!(direction, SqlBoundOrderDirection::Asc),
        Some(SqlNullsOrder::Last) => matches!(direction, SqlBoundOrderDirection::Desc),
    }
}

/// 比较两个排序键，NULL 位置由方言决定：PostgreSQL 把 NULL 视为最大值，
/// MySQL 把 NULL 视为最小值。
pub fn compare_values_with_nulls(a: Option<&SqlValue>, b: Option<&SqlValue>, nulls_first: bool) -> std::cmp::Ordering {
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
            (SqlValue::Number(n1), SqlValue::Integer(n2)) => compare_integer_float(*n2, *n1).map(Ordering::reverse).unwrap_or(Ordering::Equal),
            (SqlValue::Number(n1), SqlValue::Number(n2)) => n1.partial_cmp(n2).unwrap_or(Ordering::Equal),
            (SqlValue::String(s1), SqlValue::String(s2)) => cmp_string(s1, s2),
            (SqlValue::Boolean(b1), SqlValue::Boolean(b2)) => b1.cmp(b2),
            // 二进制列按原始 octets 的字节序排序，不经过任何文本折叠。
            (SqlValue::Bytes(a), SqlValue::Bytes(b)) => a.cmp(b),
            _ => Ordering::Equal,
        },
    }
}
