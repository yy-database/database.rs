//! 把一个 SimpleQuery 字符串切分为按顺序执行的语句批次。
//!
//! PostgreSQL 的 `batch_execute` 允许客户端把多条语句用顶层分号拼成一个
//! SimpleQuery。gateway 必须先按真实词法把批次切开，再逐条执行。这里只做
//! 切分与偏移计算，不做语义解析：语句内容仍交给既有 DDL parser、会话命令
//! 识别与 SQL binder。
//!
//! 切分规则按 PostgreSQL 词法：
//!
//! - 单引号字符串中的 `''` 是转义，`;` 不是分隔符；
//! - 双引号标识符中的 `""` 是转义，`;` 不是分隔符；
//! - `$tag$` 美元引用的内容原样跳过，`;` 不是分隔符；
//! - `--` 行注释到行尾，`/* ... */` 块注释可嵌套；
//! - 其余位置的 `;` 结束一条语句。
//!
//! 空语句（只有空白或注释）被丢弃。未闭合的引号或块注释是语法错误，
//! 报错带起始字节偏移。

use crate::sql::{SqlErrorKind, SqlFrontendError};

/// 批次中的一条语句。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBatchStatement {
    /// 该语句第一个非空白字符在整个批次字符串中的字节偏移。
    pub offset: usize,
    /// 去掉首尾空白与结束分号后的语句文本。
    pub text: String,
}

/// 切分一个 SimpleQuery 字符串为按顺序执行的语句。
///
/// 返回空 `Vec` 表示整串只有空白与注释。
pub fn split_sql_batch(sql: &str) -> Result<Vec<SqlBatchStatement>, SqlFrontendError> {
    let bytes = sql.as_bytes();
    let mut statements = Vec::new();
    let mut start: Option<usize> = None;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        match byte {
            b';' => {
                if let Some(statement_start) = start {
                    push_statement(&mut statements, sql, statement_start, index);
                    start = None;
                }
                index += 1;
            }
            b'\'' => {
                start.get_or_insert(index);
                index = skip_single_quoted(sql, index)?;
            }
            b'"' => {
                start.get_or_insert(index);
                index = skip_double_quoted(sql, index)?;
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index = skip_line_comment(bytes, index + 2);
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = skip_block_comment(sql, index)?;
            }
            b'$' => {
                if let Some(tag_end) = dollar_quote_tag_end(bytes, index) {
                    start.get_or_insert(index);
                    index = skip_dollar_quoted(sql, index, tag_end)?;
                }
                else {
                    start.get_or_insert(index);
                    index += 1;
                }
            }
            byte if byte.is_ascii_whitespace() => {
                index += 1;
            }
            _ => {
                start.get_or_insert(index);
                index += 1;
            }
        }
    }
    if let Some(statement_start) = start {
        push_statement(&mut statements, sql, statement_start, bytes.len());
    }
    Ok(statements)
}

fn push_statement(statements: &mut Vec<SqlBatchStatement>, sql: &str, start: usize, end: usize) {
    let text = sql[start..end].trim();
    if text.is_empty() {
        return;
    }
    statements.push(SqlBatchStatement { offset: start, text: text.to_string() });
}

fn skip_single_quoted(sql: &str, start: usize) -> Result<usize, SqlFrontendError> {
    let bytes = sql.as_bytes();
    let mut index = start + 1;
    while index < bytes.len() {
        if bytes[index] == b'\'' {
            if bytes.get(index + 1) == Some(&b'\'') {
                index += 2;
                continue;
            }
            return Ok(index + 1);
        }
        index += 1;
    }
    Err(quote_error("single-quoted string", start))
}

fn skip_double_quoted(sql: &str, start: usize) -> Result<usize, SqlFrontendError> {
    let bytes = sql.as_bytes();
    let mut index = start + 1;
    while index < bytes.len() {
        if bytes[index] == b'"' {
            if bytes.get(index + 1) == Some(&b'"') {
                index += 2;
                continue;
            }
            return Ok(index + 1);
        }
        index += 1;
    }
    Err(quote_error("double-quoted identifier", start))
}

fn skip_line_comment(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() {
        if bytes[index] == b'\n' {
            return index + 1;
        }
        index += 1;
    }
    index
}

fn skip_block_comment(sql: &str, start: usize) -> Result<usize, SqlFrontendError> {
    let bytes = sql.as_bytes();
    let mut depth = 1usize;
    let mut index = start + 2;
    while index < bytes.len() {
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            depth += 1;
            index += 2;
            continue;
        }
        if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
            depth -= 1;
            index += 2;
            if depth == 0 {
                return Ok(index);
            }
            continue;
        }
        index += 1;
    }
    Err(SqlFrontendError::new(SqlErrorKind::Syntax, format!("unterminated block comment starting at byte offset {start}")))
}

/// 若 `start` 处的 `$` 是一个美元引用标记的开头，返回标记结束位置（`$tag$` 之后的字节）。
fn dollar_quote_tag_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut index = start + 1;
    while index < bytes.len() {
        match bytes[index] {
            b'$' => return Some(index + 1),
            byte if byte.is_ascii_alphanumeric() || byte == b'_' => index += 1,
            _ => return None,
        }
    }
    None
}

fn skip_dollar_quoted(sql: &str, start: usize, tag_end: usize) -> Result<usize, SqlFrontendError> {
    let bytes = sql.as_bytes();
    let tag = &sql[start..tag_end];
    let mut index = tag_end;
    while index < bytes.len() {
        if bytes[index] == b'$' && sql[index..].starts_with(tag) {
            return Ok(index + tag.len());
        }
        index += 1;
    }
    Err(quote_error("dollar-quoted string", start))
}

fn quote_error(kind: &str, start: usize) -> SqlFrontendError {
    SqlFrontendError::new(SqlErrorKind::Syntax, format!("unterminated {kind} starting at byte offset {start}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(sql: &str) -> Vec<String> {
        split_sql_batch(sql).unwrap().into_iter().map(|statement| statement.text).collect()
    }

    #[test]
    fn splits_iris_commit_batch() {
        let sql = "CREATE TABLE IF NOT EXISTS \"_iris_commit\" (\"shard\" TEXT PRIMARY KEY NOT NULL, \"value\" TEXT NOT NULL);\n\
                   CREATE TABLE IF NOT EXISTS \"_iris_outbox\" (\n  \"id\" BIGSERIAL PRIMARY KEY,\n  \"shard\" TEXT NOT NULL,\n  \"seq\" BIGINT NOT NULL,\n  \"operation_id\" TEXT NOT NULL,\n  \"table_name\" TEXT NOT NULL,\n  \"entity_id\" TEXT NOT NULL,\n  \"entity_version\" BIGINT NOT NULL,\n  \"effect\" TEXT NOT NULL,\n  UNIQUE (\"operation_id\", \"entity_version\")\n);\n\
                   CREATE INDEX IF NOT EXISTS \"_iris_outbox_seq\" ON \"_iris_outbox\" (\"shard\", \"seq\");";
        let statements = split_sql_batch(sql).unwrap();
        assert_eq!(statements.len(), 3);
        assert!(statements[0].text.starts_with("CREATE TABLE IF NOT EXISTS"));
        assert!(statements[1].text.contains("UNIQUE (\"operation_id\", \"entity_version\")"));
        assert_eq!(
            statements[2].text,
            "CREATE INDEX IF NOT EXISTS \"_iris_outbox_seq\" ON \"_iris_outbox\" (\"shard\", \"seq\")"
        );
        assert_eq!(statements[0].offset, 0);
        assert!(statements[1].offset > statements[0].offset);
    }

    #[test]
    fn keeps_semicolons_inside_quotes_comments_and_dollar_bodies() {
        let sql = "INSERT INTO t VALUES ('a;b', \"c;d\");\n-- ; not a separator\nSELECT $$x;y$$;\nSELECT $tag$z;w$tag$;";
        assert_eq!(
            texts(sql),
            vec![
                "INSERT INTO t VALUES ('a;b', \"c;d\")".to_string(),
                "SELECT $$x;y$$".to_string(),
                "SELECT $tag$z;w$tag$".to_string(),
            ]
        );
    }

    #[test]
    fn handles_doubled_quotes_and_nested_block_comments() {
        let sql = "SELECT 'it''s;fine'; /* outer ; /* inner ; */ still comment */ SELECT 1";
        assert_eq!(texts(sql), vec!["SELECT 'it''s;fine'".to_string(), "SELECT 1".to_string()]);
    }

    #[test]
    fn drops_empty_statements_and_trailing_separator() {
        let sql = " ;  ;; SELECT 1; ; ";
        assert_eq!(texts(sql), vec!["SELECT 1".to_string()]);
    }

    #[test]
    fn reports_unterminated_quote_with_offset() {
        let error = split_sql_batch("SELECT 1; SELECT 'oops").unwrap_err();
        assert_eq!(error.kind, SqlErrorKind::Syntax);
        assert!(error.message.contains("single-quoted string"), "{}", error.message);
        assert!(error.message.contains("offset 17"), "{}", error.message);
    }

    #[test]
    fn reports_unterminated_block_comment_with_offset() {
        let error = split_sql_batch("SELECT 1; /* oops").unwrap_err();
        assert!(error.message.contains("unterminated block comment"), "{}", error.message);
        assert!(error.message.contains("offset 10"), "{}", error.message);
    }
}
