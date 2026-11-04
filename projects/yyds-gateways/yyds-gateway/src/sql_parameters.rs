//! 参数占位符替换。
//!
//! Oak 的 SQL AST 没有占位符节点，因此参数必须先按字面量渲染回 SQL 文本，
//! 再交给 `bind_sql_catalog`。替换只识别字符串、标识符、注释之外的位置，
//! 不会改写已有字面量内容。SQL 语义本身仍由 planner 和 executor 决定。

use crate::{
    sql::{SqlErrorKind, SqlFrontendError},
    sql_execute::{format_number, SqlValue},
};

/// 参数占位符风格。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlParameterStyle {
    /// MySQL 风格 `?`，按出现顺序绑定。
    QuestionMark,
    /// PostgreSQL 风格 `$1`，按一基序号绑定。
    DollarNumber,
}

impl SqlParameterStyle {
    fn label(self) -> &'static str {
        match self {
            Self::QuestionMark => "`?`",
            Self::DollarNumber => "`$n`",
        }
    }
}

/// 把参数按风格渲染成 SQL 字面量并替换占位符。
///
/// 替换只发生在代码位置：单引号字符串、双引号标识符、反引号标识符、
/// `--` 行注释和 `/* */` 块注释内的 `?` / `$n` 保持原样。
pub fn substitute_sql_parameters(
    sql: &str,
    parameters: &[SqlValue],
    style: SqlParameterStyle,
) -> Result<String, SqlFrontendError> {
    let (out, referenced) = walk_parameters(sql, style, &mut |index| render_parameter(parameters, index, style))?;
    if referenced < parameters.len() {
        return Err(parameter_error(&format!(
            "{} parameters were supplied but only {referenced} placeholders were referenced",
            style.label()
        )));
    }
    Ok(out)
}

/// Renders every referenced placeholder as the literal `0` and reports how
/// many positions the statement references. Used to derive statement metadata
/// (parameter count, row description) without bound values. A numeric literal
/// keeps `WHERE pk = $1` plannable, unlike `NULL`.
pub fn neutralized_sql_parameters(sql: &str, style: SqlParameterStyle) -> Result<(String, usize), SqlFrontendError> {
    walk_parameters(sql, style, &mut |_| Ok("0".to_string()))
}

/// Renders every referenced placeholder as `NULL` for metadata-only describes
/// whose target column rejects the `0` stand-in, since every column type
/// accepts `NULL`.
pub fn nulled_sql_parameters(sql: &str, style: SqlParameterStyle) -> Result<(String, usize), SqlFrontendError> {
    walk_parameters(sql, style, &mut |_| Ok("NULL".to_string()))
}

fn walk_parameters(
    sql: &str,
    style: SqlParameterStyle,
    render: &mut dyn FnMut(usize) -> Result<String, SqlFrontendError>,
) -> Result<(String, usize), SqlFrontendError> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len() + 16);
    let mut index = 0;
    let mut referenced = 0usize;

    while index < chars.len() {
        let current = chars[index];
        match current {
            '\'' | '"' | '`' => {
                index = copy_quoted(&chars, index, current, &mut out);
            }
            '-' if chars.get(index + 1) == Some(&'-') => {
                while index < chars.len() && chars[index] != '\n' {
                    out.push(chars[index]);
                    index += 1;
                }
            }
            '/' if chars.get(index + 1) == Some(&'*') => {
                out.push('/');
                out.push('*');
                index += 2;
                while index < chars.len() {
                    if chars[index] == '*' && chars.get(index + 1) == Some(&'/') {
                        out.push('*');
                        out.push('/');
                        index += 2;
                        break;
                    }
                    out.push(chars[index]);
                    index += 1;
                }
            }
            '?' if style == SqlParameterStyle::QuestionMark => {
                out.push_str(&render(referenced)?);
                referenced += 1;
                index += 1;
            }
            '$' if style == SqlParameterStyle::DollarNumber && chars.get(index + 1).is_some_and(char::is_ascii_digit) => {
                let start = index + 1;
                let mut end = start;
                while end < chars.len() && chars[end].is_ascii_digit() {
                    end += 1;
                }
                let ordinal = chars[start..end]
                    .iter()
                    .collect::<String>()
                    .parse::<usize>()
                    .map_err(|_| parameter_error("SQL parameter index is not a valid integer"))?;
                if ordinal == 0 {
                    return Err(parameter_error("SQL parameter indexes are one-based"));
                }
                out.push_str(&render(ordinal - 1)?);
                referenced = referenced.max(ordinal);
                index = end;
            }
            _ => {
                out.push(current);
                index += 1;
            }
        }
    }

    Ok((out, referenced))
}

fn copy_quoted(chars: &[char], mut index: usize, quote: char, out: &mut String) -> usize {
    out.push(quote);
    index += 1;
    while index < chars.len() {
        let current = chars[index];
        out.push(current);
        index += 1;
        if current == '\\' && quote == '\'' {
            // MySQL 风格反斜杠转义：吞掉下一个字符，避免把 `\'` 当成字符串结束。
            if index < chars.len() {
                out.push(chars[index]);
                index += 1;
            }
            continue;
        }
        if current == quote {
            if chars.get(index) == Some(&quote) {
                out.push(quote);
                index += 1;
                continue;
            }
            break;
        }
    }
    index
}

fn render_parameter(parameters: &[SqlValue], index: usize, style: SqlParameterStyle) -> Result<String, SqlFrontendError> {
    let Some(value) = parameters.get(index)
    else {
        return Err(parameter_error(&format!("{} placeholder {} has no bound parameter", style.label(), index + 1)));
    };
    Ok(match value {
        SqlValue::Integer(value) => value.to_string(),
        SqlValue::Number(value) if value.is_finite() => format_number(*value),
        SqlValue::Number(_) => return Err(parameter_error("non-finite floating parameters are not supported")),
        SqlValue::Boolean(value) => (if *value { "TRUE" } else { "FALSE" }).to_string(),
        SqlValue::String(value) => {
            let mut literal = String::with_capacity(value.len() + 2);
            literal.push('\'');
            for character in value.chars() {
                if character == '\'' {
                    literal.push('\'');
                }
                literal.push(character);
            }
            literal.push('\'');
            literal
        }
        SqlValue::Bytes(value) => {
            // 原始 octets 无法放进 SQL 文本的字符串字面量，只有十六进制 CAST 能承载它们。
            // PostgreSQL 用 `bytea` 的 `\x` 拼写，MySQL 用裸十六进制 `BLOB` 拼写。
            let hex = crate::sql::encode_hex(value);
            match style {
                SqlParameterStyle::DollarNumber => format!("CAST('\\x{hex}' AS BYTEA)"),
                SqlParameterStyle::QuestionMark => format!("CAST('{hex}' AS BLOB)"),
            }
        }
        SqlValue::Null => "NULL".to_string(),
    })
}

fn parameter_error(message: &str) -> SqlFrontendError {
    SqlFrontendError::new(SqlErrorKind::Parameter, message)
}
