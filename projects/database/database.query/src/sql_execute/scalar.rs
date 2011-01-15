//! 标量内建函数与时间/数值格式化。

use super::*;

/// 对一行求值标量内建函数。参数已按从左到右求值完毕。
///
/// `dialect` 参与的函数只有这些真正分叉的语义：`LENGTH` 的字节/字符计数、
/// `CONCAT` 的 NULL 传播、`SUBSTRING` 的越界裁剪与 `TRIM` 的显式字符集。
pub fn eval_scalar_function(function: SqlScalarFunction, args: &[SqlValue], dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
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
        return Err(SqlFrontendError::new(SqlErrorKind::TypeMismatch, format!("{}() is not defined for binary values", function.name())));
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
pub(crate) fn substring_value(text: &str, start: i64, length: Option<i64>, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
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
                return Err(SqlFrontendError::new(SqlErrorKind::InvalidSubstringLength, "negative substring length not allowed"));
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
pub(crate) fn scalar_text(args: &[SqlValue], index: usize) -> Option<String> {
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
pub(crate) fn scalar_integer(args: &[SqlValue], index: usize) -> Option<i64> {
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
pub(crate) fn text_view(args: &[SqlValue], index: usize) -> Result<Option<String>, SqlFrontendError> {
    match args.get(index) {
        None | Some(SqlValue::Null) => Ok(None),
        Some(SqlValue::Bytes(bytes)) => std::str::from_utf8(bytes).map(|text| Some(text.to_string())).map_err(|_| {
            SqlFrontendError::new(SqlErrorKind::TypeMismatch, "binary value is not valid UTF-8 text and cannot serve as text".to_string())
        }),
        _ => Ok(scalar_text(args, index)),
    }
}

/// `INSTR` / `STRPOS` 的 1-based 字符位置；未找到为 0，空 needle 为 1（两方言一致）。
pub(crate) fn character_position(haystack: &str, needle: &str) -> i64 {
    if needle.is_empty() {
        return 1;
    }
    match haystack.find(needle) {
        Some(byte_offset) => i64::try_from(haystack[..byte_offset].chars().count()).map_or(i64::MAX, |chars| chars + 1),
        None => 0,
    }
}

/// `LEFT` / `RIGHT` 按字符计数取一侧片段；非正数计数返回空串，超出长度取整串。
pub(crate) fn edge_characters(text: &str, count: i64, from_end: bool) -> String {
    if count <= 0 {
        return String::new();
    }
    let characters: Vec<char> = text.chars().collect();
    let take = usize::try_from(count).unwrap_or(usize::MAX).min(characters.len());
    if from_end { characters[characters.len() - take..].iter().collect() } else { characters[..take].iter().collect() }
}

/// 会话时间戳 `YYYY-MM-DD HH:MM:SS`，UTC。
pub(crate) fn current_timestamp() -> String {
    let seconds = current_utc_seconds();
    format!("{} {}", date_of(seconds), time_of(seconds))
}

/// 会话日期 `YYYY-MM-DD`，UTC。
pub(crate) fn current_date() -> String {
    date_of(current_utc_seconds())
}

/// 会话时间 `HH:MM:SS`，UTC。
pub(crate) fn current_time() -> String {
    time_of(current_utc_seconds())
}

/// 当前 UTC 秒数；时钟早于 Unix epoch 时按 0 处理。
pub(crate) fn current_utc_seconds() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|elapsed| elapsed.as_secs() as i64).unwrap_or(0)
}

pub(crate) fn date_of(seconds: i64) -> String {
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    format!("{year:04}-{month:02}-{day:02}")
}

pub(crate) fn time_of(seconds: i64) -> String {
    let remainder = seconds.rem_euclid(86_400);
    format!("{:02}:{:02}:{:02}", remainder / 3600, (remainder % 3600) / 60, remainder % 60)
}

/// Gregorian date from days since 1970-01-01 (Howard Hinnant's civil_from_days).
pub(crate) fn civil_from_days(days: i64) -> (i64, u32, u32) {
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
pub fn format_number(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 1e15 { format!("{}", value as i64) } else { format!("{value}") }
}
