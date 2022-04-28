//! Oak SQL frontend entry point for compatibility gateways.
//!
//! This module only parses the SQL surface. It does not execute SQL, own a
//! query planner, or bypass the YY execution model.

use std::borrow::Cow;

use oak_sql::ast::{BinaryOperator, Expression, Literal, SelectItem, SqlStatement};

/// Structured failure category from the Oak SQL frontend, gateway binder,
/// planner or executor.
///
/// Protocol adapters map categories to wire error codes and SQLSTATE values.
/// They never inspect [`SqlFrontendError::message`] to recover the category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlErrorKind {
    /// Oak rejected the SQL surface.
    Syntax,
    /// The catalog does not publish the referenced table.
    UnknownTable,
    /// The visible scope has no table matching a column qualifier.
    UnknownTableQualifier,
    /// The catalog publishes more than one table with the requested name.
    AmbiguousTable,
    /// No visible column matches the requested name.
    UnknownColumn,
    /// More than one visible column matches an unqualified name.
    AmbiguousColumn,
    /// A write value cannot be represented by the target column type.
    TypeMismatch,
    /// A write value exceeds the target column's declared capacity.
    ValueTooLong,
    /// A write supplies `NULL` for a primary key.
    NotNullViolation,
    /// A write omits a `NOT NULL` column that carries no `DEFAULT`, so there is
    /// no value to store. MySQL reports `1364` / `HY000`; PostgreSQL folds this
    /// into its not-null violation `23502`.
    MissingDefault,
    /// The statement is valid SQL but outside the gateway's supported subset.
    Unsupported,
    /// A function name or arity is not part of the gateway subset.
    UnknownFunction,
    /// A `@@` system variable is not published by the gateway.
    UnknownVariable,
    /// A scalar subquery produced more than one row. PostgreSQL reports
    /// `21000` and MySQL `1242`.
    CardinalityViolation,
    /// A grouped projection references a column outside the `GROUP BY` key.
    /// PostgreSQL reports `42803` and MySQL `1055`.
    InvalidGrouping,
    /// An `IN (SELECT …)` subquery selects more than one column. PostgreSQL
    /// reports `42601` and MySQL `1241`.
    OperandColumns,
    /// Last resort for failures the gateway cannot classify: an inconsistent
    /// catalog publication or a storage/executor failure. MySQL `1105`,
    /// PostgreSQL `XX000`. Every user-visible scenario must be classified
    /// before it reaches this variant.
    Internal,
    /// Parameter substitution or a bind parameter position is invalid.
    Parameter,
    /// A write would create a duplicate key: the primary key or a declared
    /// `UNIQUE` constraint. PostgreSQL maps this to `23505`, MySQL to
    /// `1062` with SQLSTATE `23000`.
    DuplicateKey,
    /// Optimistic concurrency rejected a read-modify-write.
    Conflict,
    /// Division or modulo by zero — PostgreSQL raises an error, MySQL returns NULL.
    DivisionByZero,
    /// `SUBSTRING` was given a negative length. PostgreSQL rejects it with SQLSTATE
    /// `22011`, while MySQL returns an empty string, so this category is PostgreSQL-only.
    InvalidSubstringLength,
    /// Exact integer arithmetic left the `i64` range. Both dialects report SQLSTATE
    /// `22003`, but MySQL names it with its own error number `1690`.
    NumericOverflow,
    /// `CREATE TABLE` targets a table the local schema registry already holds.
    TableAlreadyExists,
    /// `ALTER TABLE ADD COLUMN` targets a column the table already holds.
    ColumnAlreadyExists,
    /// `CREATE INDEX` reuses an index name already registered.
    IndexAlreadyExists,
    /// `DROP INDEX` names an index the local schema registry does not hold.
    UnknownIndex,
}

/// Structured failure from the Oak SQL frontend or gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlFrontendError {
    /// Failure category consumed by protocol adapters.
    pub kind: SqlErrorKind,
    /// Diagnostic text returned by the failing gateway stage.
    pub message: String,
}

impl SqlFrontendError {
    /// Creates a categorized frontend error.
    pub fn new(kind: SqlErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }

    /// Oak rejected the SQL surface.
    pub fn syntax(message: impl Into<String>) -> Self {
        Self::new(SqlErrorKind::Syntax, message)
    }

    /// A literal cannot be represented by the target column type.
    pub fn type_mismatch(message: impl Into<String>) -> Self {
        Self::new(SqlErrorKind::TypeMismatch, message)
    }

    /// The statement is outside the gateway subset.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(SqlErrorKind::Unsupported, message)
    }

    /// A catalog identity or storage failure the gateway cannot classify.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(SqlErrorKind::Internal, message)
    }
}

/// A session statement lowered from Oak's SQL syntax tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlSessionCommand {
    /// `SET NAMES` character-set negotiation.
    SetNames {
        /// Character set requested by the client.
        character_set: String,
        /// Optional collation requested alongside the character set.
        collation: Option<String>,
    },
    /// Transaction boundary used by SQL protocol sessions.
    Transaction(SqlTransactionAction),
    /// `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT`.
    Savepoint(SavepointIntent),
    /// `SHOW [GLOBAL|SESSION] VARIABLES [LIKE 'pattern']` — variable table probe.
    ShowVariables {
        /// Requested scope prefix, absent when the client did not name one.
        scope: Option<SqlVariableScope>,
        /// Optional `LIKE` pattern covering the variable name.
        pattern: Option<String>,
    },
    /// `SHOW <name>` — single-parameter probe used by PostgreSQL clients.
    ShowParameter {
        /// Requested scope prefix, absent when the client did not name one.
        scope: Option<SqlVariableScope>,
        /// Parameter name as written by the client.
        name: String,
    },
    /// `SET [GLOBAL|SESSION|LOCAL] <name> = <value>` — session assignment.
    SetVariable {
        /// Requested scope prefix, absent when the client did not name one.
        scope: Option<SqlVariableScope>,
        /// Variable name as written by the client.
        name: String,
        /// Assigned value, unbound against any catalog table.
        value: SqlBoundExpression,
    },
    /// SQLite `PRAGMA <name> [= <value>]`.
    Pragma {
        /// Pragma name as written by the client.
        name: String,
        /// Optional assigned value.
        value: Option<SqlBoundExpression>,
    },
    /// `RESET <parameter>` / `RESET ALL` — restore GUC defaults.
    ResetVariables {
        /// Parameter to reset, `None` for `RESET ALL`.
        name: Option<String>,
    },
    /// `DISCARD <target>` — release session-scoped caches.
    Discard(DiscardTarget),
}

/// One of the three savepoint verbs the gateway recognises.
///
/// Oak's SQL AST does not project a Savepoint node yet, so the gateway lowers the
/// verbs itself via [`parse_session_command`]'s pre-filter rather than routing them
/// through Oak.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SavepointIntent {
    /// `SAVEPOINT <name>` — create a new named savepoint.
    Create {
        /// Savepoint name.
        name: String,
    },
    /// `ROLLBACK TO SAVEPOINT <name>` — restore buffer state to the named savepoint.
    RollbackTo {
        /// Savepoint name.
        name: String,
    },
    /// `RELEASE SAVEPOINT <name>` — remove the savepoint without rolling back.
    Release {
        /// Savepoint name.
        name: String,
    },
}

/// Target of a PostgreSQL `DISCARD` statement.
///
/// Like [`SavepointIntent`], Oak's SQL AST does not project `DISCARD` yet, so the
/// gateway lowers the verb itself before Oak runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardTarget {
    /// `DISCARD ALL` — full session reset.
    All,
    /// `DISCARD PLANS` — release cached plans.
    Plans,
    /// `DISCARD SEQUENCES` — release cached sequence state.
    Sequences,
    /// `DISCARD TEMPORARY` / `DISCARD TEMP` — drop temporary tables.
    Temporary,
}

/// Scope prefix carried by `SET` and `SHOW` session statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlVariableScope {
    /// `GLOBAL` — server-wide scope.
    Global,
    /// `SESSION` — connection scope.
    Session,
    /// `LOCAL` — dialect-specific local scope.
    Local,
}

/// Transaction operation recognized by Oak and projected for protocol adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlTransactionAction {
    /// Start a transaction.
    Begin,
    /// Commit a transaction.
    Commit,
    /// Roll back a transaction.
    Rollback,
    /// Create a named savepoint.
    Savepoint(String),
    /// Roll back to a named savepoint.
    RollbackToSavepoint(String),
    /// Release a named savepoint.
    ReleaseSavepoint(String),
}

/// Parses one SQL surface statement through the official Oak SQL frontend.
///
/// The Oak parser is always built with [`SqlLanguage::mysql`] because that is the
/// only language configuration that lexes backtick identifiers, which both the
/// normalization step and MySQL clients need. PostgreSQL double-quoted identifiers
/// are therefore normalized into backtick identifiers before parsing.
pub fn parse_sql(source: &str, dialect: SqlDialect) -> Result<oak_sql::ast::SqlRoot, SqlFrontendError> {
    parse_ast(&normalise_sql_source(source, dialect))
}

/// Parses already-normalized SQL text and rejects Oak error statements.
fn parse_ast(source: &str) -> Result<oak_sql::ast::SqlRoot, SqlFrontendError> {
    use oak_core::{Builder, parser::session::ParseSession, source::SourceText};

    let language = oak_sql::SqlLanguage::mysql();
    let builder = oak_sql::SqlBuilder::new(&language);
    let source_text = SourceText::new(source.to_string());
    let mut cache = ParseSession::default();
    let root = builder.build(&source_text, &[], &mut cache).result.map_err(|error| SqlFrontendError::syntax(error.to_string()))?;
    for statement in &root.statements {
        match statement {
            SqlStatement::Error { message, .. } => return Err(SqlFrontendError::syntax(message.to_string())),
            SqlStatement::Unknown { .. } => {}
            _ => {}
        }
    }
    if root.statements.is_empty() {
        return Err(bind_error("Oak produced no SQL statements"));
    }
    Ok(root)
}

/// 按方言把 SQL 文本改写成 Oak 能解析的形状。
///
/// 两处改写都是词法层事实，不是语义近似：
/// - PostgreSQL 的双引号标识符改成反引号（Oak 只在反引号配置下产出标识符节点）。
/// - `LEFT` / `RIGHT` 在 Oak 词表里是 JOIN 关键字，函数调用位置无法解析，改写成引号标识符。
fn normalise_sql_source(source: &str, dialect: SqlDialect) -> Cow<'_, str> {
    let quoted = normalise_quoted_identifiers(source, dialect);
    match normalize_reserved_function_calls(&quoted) {
        // Borrowed result still points at `source` or into `quoted`, and `quoted`
        // owning the text is exactly what the caller needs to receive.
        Cow::Borrowed(_) => quoted,
        Cow::Owned(text) => Cow::Owned(text),
    }
}

/// PostgreSQL 双引号标识符 → 反引号标识符；MySQL 的双引号是字符串字面量，保持原样。
///
/// 只扫描代码位置：单引号字符串、反引号标识符与注释原样保留。
fn normalise_quoted_identifiers(source: &str, dialect: SqlDialect) -> Cow<'_, str> {
    if dialect == SqlDialect::Mysql || !source.contains('"') {
        return Cow::Borrowed(source);
    }
    let bytes = source.as_bytes();
    let mut out = String::with_capacity(source.len());
    let mut index = 0;
    let mut copied = 0;
    let mut changed = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => index = skip_quoted_bytes(bytes, index, b'\''),
            b'`' => index = skip_quoted_bytes(bytes, index, b'`'),
            b'-' if bytes.get(index + 1) == Some(&b'-') => index = skip_line_comment(bytes, index),
            b'/' if bytes.get(index + 1) == Some(&b'*') => index = skip_block_comment(bytes, index),
            b'"' => {
                let start = index;
                let mut content = String::new();
                let mut cursor = index + 1;
                while cursor < bytes.len() {
                    if bytes[cursor] == b'"' {
                        if bytes.get(cursor + 1) == Some(&b'"') {
                            content.push('"');
                            cursor += 2;
                            continue;
                        }
                        cursor += 1;
                        break;
                    }
                    content.push(source[cursor..].chars().next().expect("in-bounds character"));
                    cursor += source[cursor..].chars().next().map_or(1, |ch| ch.len_utf8());
                }
                out.push_str(&source[copied..start]);
                out.push('`');
                out.push_str(&content);
                out.push('`');
                copied = cursor;
                index = cursor;
                changed = true;
            }
            _ => index += 1,
        }
    }
    if !changed {
        return Cow::Borrowed(source);
    }
    out.push_str(&source[copied..]);
    Cow::Owned(out)
}

fn skip_line_comment(bytes: &[u8], start: usize) -> usize {
    let mut index = start + 2;
    while index < bytes.len() && bytes[index] != b'\n' {
        index += 1;
    }
    index
}

fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut index = start + 2;
    while index < bytes.len() {
        if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
            return index + 2;
        }
        index += 1;
    }
    index
}

/// Projects the small set of protocol session commands from Oak's SQL AST.
pub fn parse_session_command(source: &str) -> Result<Option<SqlSessionCommand>, SqlFrontendError> {
    if let Some(intent) = detect_savepoint_statement(source) {
        return Ok(Some(SqlSessionCommand::Savepoint(intent)));
    }
    if let Some(command) = detect_session_reset_statement(source) {
        return Ok(Some(command));
    }
    let normalized = normalize_start_transaction(source).or_else(|| normalize_quoted_set_names(source));
    // 这里只做协议命令投影，从不读取标识符文本；按 PostgreSQL 口径消除双引号，
    // 让 PostgreSQL 客户端带 `"identifiers"` 的语句也能通过可解析性检查。
    let source = normalized.as_deref().unwrap_or(source);
    let root = parse_ast(&normalise_sql_source(source, SqlDialect::Postgres))?;
    if root.statements.len() != 1 {
        return Ok(None);
    }
    Ok(match &root.statements[0] {
        SqlStatement::SetNames(statement) => Some(SqlSessionCommand::SetNames {
            character_set: statement.character_set.to_string(),
            collation: statement.collation.as_deref().map(str::to_string),
        }),
        SqlStatement::Transaction(statement) => Some(SqlSessionCommand::Transaction(match &statement.action {
            oak_sql::ast::TransactionAction::Begin => SqlTransactionAction::Begin,
            oak_sql::ast::TransactionAction::Commit => SqlTransactionAction::Commit,
            oak_sql::ast::TransactionAction::Rollback => SqlTransactionAction::Rollback,
            oak_sql::ast::TransactionAction::Savepoint(name) => SqlTransactionAction::Savepoint(name.to_string()),
            oak_sql::ast::TransactionAction::RollbackToSavepoint(name) => SqlTransactionAction::RollbackToSavepoint(name.to_string()),
            oak_sql::ast::TransactionAction::ReleaseSavepoint(name) => SqlTransactionAction::ReleaseSavepoint(name.to_string()),
        })),
        SqlStatement::SetVariable(statement) => Some(SqlSessionCommand::SetVariable {
            scope: statement.scope.map(project_variable_scope),
            name: statement.name.to_string(),
            value: bind_expression(&statement.value)?,
        }),
        SqlStatement::Show(statement) => {
            let scope = statement.scope.map(project_variable_scope);
            if statement.target.eq_ignore_ascii_case("VARIABLES") {
                Some(SqlSessionCommand::ShowVariables { scope, pattern: statement.like.as_deref().map(str::to_string) })
            }
            else {
                Some(SqlSessionCommand::ShowParameter { scope, name: statement.target.to_string() })
            }
        }
        SqlStatement::Pragma(statement) => Some(SqlSessionCommand::Pragma {
            name: statement.name.to_string(),
            value: statement.value.as_ref().map(bind_expression).transpose()?,
        }),
        _ => None,
    })
}

/// Matches `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, and `RELEASE SAVEPOINT` at the
/// text level so the gateway can honour savepoints before Oak grows a Savepoint
/// AST node. Returns `None` when the source is not a savepoint verb.
pub fn detect_savepoint_statement(source: &str) -> Option<SavepointIntent> {
    let trimmed = source.trim();
    let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    let mut words = trimmed.split_whitespace();
    let first = words.next()?;
    if first.eq_ignore_ascii_case("SAVEPOINT") {
        let name = words.next()?;
        if words.next().is_some() {
            return None;
        }
        return Some(SavepointIntent::Create { name: name.to_string() });
    }
    if first.eq_ignore_ascii_case("RELEASE") {
        let second = words.next()?;
        if !second.eq_ignore_ascii_case("SAVEPOINT") {
            return None;
        }
        let name = words.next()?;
        if words.next().is_some() {
            return None;
        }
        return Some(SavepointIntent::Release { name: name.to_string() });
    }
    if first.eq_ignore_ascii_case("ROLLBACK") {
        let second = words.next()?;
        if !second.eq_ignore_ascii_case("TO") {
            return None;
        }
        let third = words.next()?;
        let name = if third.eq_ignore_ascii_case("SAVEPOINT") { words.next()? } else { third };
        if words.next().is_some() {
            return None;
        }
        return Some(SavepointIntent::RollbackTo { name: name.to_string() });
    }
    None
}

/// Matches PostgreSQL `RESET <parameter>` / `RESET ALL` and
/// `DISCARD ALL|PLANS|SEQUENCES|TEMPORARY|TEMP` at the text level, the same way
/// [`detect_savepoint_statement`] covers the savepoint verbs Oak does not project.
///
/// The match is deliberately lexical and case-insensitive: only a complete
/// statement made of a bare identifier (no quoting, no extra tokens) is claimed.
/// Anything else falls through to Oak, which reports its own syntax error, so a
/// mis-shaped statement can never be silently treated as a session reset.
pub fn detect_session_reset_statement(source: &str) -> Option<SqlSessionCommand> {
    let trimmed = source.trim();
    let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    let mut words = trimmed.split_whitespace();
    let first = words.next()?;
    if first.eq_ignore_ascii_case("RESET") {
        let name = words.next()?;
        if words.next().is_some() || !is_bare_session_identifier(name) {
            return None;
        }
        return Some(if name.eq_ignore_ascii_case("ALL") {
            SqlSessionCommand::ResetVariables { name: None }
        }
        else {
            SqlSessionCommand::ResetVariables { name: Some(name.to_string()) }
        });
    }
    if first.eq_ignore_ascii_case("DISCARD") {
        let target = words.next()?;
        if words.next().is_some() {
            return None;
        }
        let target = if target.eq_ignore_ascii_case("ALL") {
            DiscardTarget::All
        }
        else if target.eq_ignore_ascii_case("PLANS") {
            DiscardTarget::Plans
        }
        else if target.eq_ignore_ascii_case("SEQUENCES") {
            DiscardTarget::Sequences
        }
        else if target.eq_ignore_ascii_case("TEMPORARY") || target.eq_ignore_ascii_case("TEMP") {
            DiscardTarget::Temporary
        }
        else {
            return None;
        };
        return Some(SqlSessionCommand::Discard(target));
    }
    None
}

/// Whether `token` is an unquoted SQL identifier: `$`/`_`/letters first, then
/// `$`/`_`/letters/digits. Quoted or punctuated spellings stay outside the
/// pre-filter and are left to Oak.
fn is_bare_session_identifier(token: &str) -> bool {
    let mut characters = token.chars();
    let Some(first) = characters.next()
    else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_' || first == '$') {
        return false;
    }
    characters.all(|character| character.is_ascii_alphanumeric() || character == '_' || character == '$')
}

/// MySQL and PostgreSQL clients may spell the opening boundary as `START
/// TRANSACTION`; Oak's session grammar only reads `BEGIN`. Rewrite that one
/// statement and leave every other SQL surface untouched.
fn normalize_start_transaction(source: &str) -> Option<String> {
    let trimmed = source.trim();
    let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    let mut words = trimmed.split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("START") {
        return None;
    }
    if !words.next()?.eq_ignore_ascii_case("TRANSACTION") {
        return None;
    }
    if words.next().is_some() {
        return None;
    }
    Some("BEGIN".to_string())
}

/// MySQL clients spell charset negotiation as `SET NAMES 'utf8mb4' COLLATE
/// 'utf8mb4_general_ci'`, while Oak's session grammar only reads bare
/// identifiers. Unquote simple identifier literals for this one statement and
/// leave every other SQL surface untouched.
fn normalize_quoted_set_names(source: &str) -> Option<String> {
    let trimmed = source.trim();
    let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    trimmed.get(..9).filter(|prefix| prefix.eq_ignore_ascii_case("SET NAMES"))?;
    let tokens = trimmed[9..].split_whitespace().collect::<Vec<_>>();
    let mut words = Vec::with_capacity(tokens.len());
    for token in tokens {
        words.push(unquote_simple_identifier(token)?);
    }
    if !(words.len() == 1 || (words.len() == 3 && words[1].eq_ignore_ascii_case("COLLATE"))) {
        return None;
    }
    Some(format!("SET NAMES {}", words.join(" ")))
}

fn unquote_simple_identifier(token: &str) -> Option<&str> {
    let inner = token.strip_prefix('\'').and_then(|rest| rest.strip_suffix('\'')).unwrap_or(token);
    if inner.is_empty() || !inner.chars().all(|character| character.is_ascii_alphanumeric() || character == '_') {
        return None;
    }
    Some(inner)
}

fn project_variable_scope(scope: oak_sql::ast::SessionScope) -> SqlVariableScope {
    match scope {
        oak_sql::ast::SessionScope::Global => SqlVariableScope::Global,
        oak_sql::ast::SessionScope::Session => SqlVariableScope::Session,
        oak_sql::ast::SessionScope::Local => SqlVariableScope::Local,
    }
}

/// Gateway-owned statement after Oak parsing and SQL-surface binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundStatement {
    /// One SQL SELECT request awaiting 集群 catalog binding.
    Select(SqlBoundSelect),
    /// One SQL INSERT request awaiting 集群 catalog binding.
    Insert(SqlBoundInsert),
    /// One SQL UPDATE request awaiting 集群 catalog binding.
    Update(SqlBoundUpdate),
    /// One SQL DELETE request awaiting 集群 catalog binding.
    Delete(SqlBoundDelete),
    /// One SQL set operation awaiting 集群 catalog binding.
    SetOperation(SqlBoundSetOperation),
}

/// MySQL's plain `BLOB` protocol type holds at most 65 535 octets.
///
/// This is a wire fact of the MySQL dialect, not a shared column-type
/// constraint: PostgreSQL `BYTEA` carries no such limit and must not inherit
/// it. Payloads above this size are rejected on the MySQL path instead of
/// being silently widened into `MEDIUMBLOB` / `LONGBLOB`.
pub const MYSQL_BLOB_CAPACITY: usize = 65_535;

/// SQL dialect that owns protocol-visible ordering rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlDialect {
    /// PostgreSQL: `NULL` sorts after every non-NULL value.
    #[default]
    Postgres,
    /// MySQL: `NULL` sorts before every non-NULL value.
    Mysql,
}

impl SqlDialect {
    /// Whether `NULL` values sort before every non-NULL value.
    pub fn nulls_sort_first(self) -> bool {
        matches!(self, Self::Mysql)
    }

    /// Octet capacity this dialect enforces on binary column writes, or `None`
    /// when its wire type carries no dialect-level limit.
    ///
    /// MySQL describes a `bytes` column as a plain `BLOB` capped at
    /// [`MYSQL_BLOB_CAPACITY`]; a longer payload is rejected at plan time.
    /// PostgreSQL `BYTEA` has no such cap, so the shared planner must not
    /// reject anything on its behalf.
    pub fn bytes_capacity(self) -> Option<usize> {
        match self {
            Self::Mysql => Some(MYSQL_BLOB_CAPACITY),
            Self::Postgres => None,
        }
    }
}

/// One non-recursive `WITH` definition retained by the gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundCte {
    /// Name the CTE is referenced by in `FROM` / `JOIN`.
    pub name: String,
    /// Explicit output column names written as `WITH name (a, b) AS (...)`. Empty
    /// means the body's own output names apply.
    pub columns: Vec<String>,
    /// Bound body of the CTE.
    pub query: Box<SqlBoundSelect>,
}

/// SQL SELECT request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundSelect {
    /// Common table expressions declared by this query, in source order.
    pub with: Vec<SqlBoundCte>,
    /// Optional source row as written by the client.
    pub source: Option<SqlBoundSource>,
    /// JOIN clauses in source order.
    pub joins: Vec<SqlBoundJoin>,
    /// Projection expressions in source order.
    pub projections: Vec<SqlBoundProjection>,
    /// Optional SQL WHERE expression.
    pub predicate: Option<SqlBoundExpression>,
    /// Optional SQL GROUP BY expressions.
    pub group_by: Option<Vec<SqlBoundExpression>>,
    /// Optional SQL HAVING expression (applied after GROUP BY aggregation).
    pub having: Option<SqlBoundExpression>,
    /// Optional SQL `LIMIT` count expression, folded to a row count at plan time.
    pub limit: Option<SqlBoundExpression>,
    /// Optional SQL `OFFSET` count expression, folded to a row count at plan time.
    pub offset: Option<SqlBoundExpression>,
    /// Optional SQL ORDER BY clause — supports multi-column ordering.
    pub order_by: Option<Vec<SqlBoundOrderByItem>>,
    /// Whether the projection list carries the `DISTINCT` modifier.
    pub distinct: bool,
}

/// One `FROM` / `JOIN` row source retained by the gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundSource {
    /// A catalog table reference, optionally renamed by an alias.
    Table {
        /// Catalog table name as written by the client.
        name: String,
        /// Optional alias that replaces the table name in the column scope.
        alias: Option<String>,
    },
    /// A derived table `(SELECT ...) alias` — the inner query keeps its own binding.
    Derived {
        /// Bound inner query. Table sources are the only shape the planner accepts.
        query: Box<SqlBoundSelect>,
        /// Alias that names the derived table in the column scope.
        alias: String,
    },
}

/// One JOIN clause retained by the gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundJoin {
    /// Join kind as written by the client.
    pub kind: SqlBoundJoinKind,
    /// Joined row source as written by the client.
    pub source: SqlBoundSource,
    /// Join condition from the `ON` clause.
    pub on: SqlBoundExpression,
}

/// SQL join kind retained by the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBoundJoinKind {
    /// `[INNER] JOIN` — only rows matching on both sides survive.
    Inner,
    /// `LEFT [OUTER] JOIN`.
    Left,
    /// `RIGHT [OUTER] JOIN`.
    Right,
    /// `FULL [OUTER] JOIN`.
    Full,
    /// `CROSS JOIN` — Cartesian product, no `ON` clause.
    Cross,
}

/// One SQL ORDER BY item — expression, direction and explicit NULL placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundOrderByItem {
    /// Sort key expression.
    pub expr: SqlBoundExpression,
    /// Ascending or descending order.
    pub direction: SqlBoundOrderDirection,
    /// Explicit NULL placement, absent when the dialect default applies.
    pub nulls: Option<SqlNullsOrder>,
}

/// Sort direction for ORDER BY.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundOrderDirection {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

/// Explicit `NULLS FIRST` / `NULLS LAST` written in ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlNullsOrder {
    /// NULL sorts before any value.
    First,
    /// NULL sorts after any value.
    Last,
}

/// One `OVER (...)` clause attached to a window function call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundWindowSpec {
    /// `PARTITION BY` expressions, in source order.
    pub partition_by: Vec<SqlBoundExpression>,
    /// Window `ORDER BY` items, in source order.
    pub order_by: Vec<SqlBoundOrderByItem>,
    /// Explicit `ROWS` / `RANGE` frame, when one was written.
    pub frame: Option<SqlBoundWindowFrame>,
}

/// Explicit window frame bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundWindowFrame {
    /// Whether the frame counts physical rows or compares the single `ORDER BY` value.
    pub unit: SqlBoundWindowFrameUnit,
    /// The bound the frame starts at.
    pub start: SqlBoundWindowFrameBound,
    /// The bound the frame ends at. The single-bound spelling is normalized to
    /// `CURRENT ROW` here, so execution always sees both bounds.
    pub end: Option<SqlBoundWindowFrameBound>,
}

/// Frame unit of an explicit window frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBoundWindowFrameUnit {
    /// `ROWS` — physical row positions.
    Rows,
    /// `RANGE` — value distances over the single `ORDER BY` column.
    Range,
}

impl SqlBoundWindowFrame {
    /// Whether the frame carries a numeric offset bound (`n PRECEDING` / `n FOLLOWING`).
    pub fn has_value_offset(&self) -> bool {
        let offset_bound =
            |bound: SqlBoundWindowFrameBound| matches!(bound, SqlBoundWindowFrameBound::Preceding(_) | SqlBoundWindowFrameBound::Following(_));
        offset_bound(self.start) || self.end.is_some_and(offset_bound)
    }
}

/// One bound of an explicit `ROWS` / `RANGE` frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBoundWindowFrameBound {
    /// `UNBOUNDED PRECEDING`.
    UnboundedPreceding,
    /// `n PRECEDING`.
    Preceding(u64),
    /// `CURRENT ROW`.
    CurrentRow,
    /// `n FOLLOWING`.
    Following(u64),
    /// `UNBOUNDED FOLLOWING`.
    UnboundedFollowing,
}

/// SQL INSERT request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundInsert {
    /// Target table name.
    pub table: String,
    /// Optional explicit column list. When absent the catalog will infer order.
    pub columns: Option<Vec<String>>,
    /// Bound values grouped by row, empty unless [`SqlBoundInsertSource::Values`] is used.
    pub rows: Vec<Vec<SqlBoundExpression>>,
    /// Where the inserted rows come from.
    pub source: SqlBoundInsertSource,
    /// `ON CONFLICT` clause, when one was written.
    pub conflict: Option<SqlBoundConflict>,
    /// `RETURNING` projection list, when one was written.
    pub returning: Option<Vec<SqlBoundProjection>>,
}

/// Source of the rows an `INSERT` writes.
///
/// Oak reports three distinct spellings, and the gateway must keep all three
/// apart: `VALUES` supplies its rows through [`SqlBoundInsert::rows`],
/// `DEFAULT VALUES` asks for one row built from column defaults (a shape the
/// gateway does not implement), and `SELECT` carries its own bound query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundInsertSource {
    /// `INSERT ... VALUES (...)`: rows are carried by [`SqlBoundInsert::rows`].
    Values,
    /// `INSERT ... DEFAULT VALUES`: one row of column defaults.
    DefaultValues,
    /// `INSERT ... SELECT`: rows produced by a nested query.
    Select(Box<SqlBoundSelect>),
}

/// `ON CONFLICT` clause retained by the gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundConflict {
    /// Conflict target columns, normalized. Empty when the clause omitted its target.
    pub columns: Vec<String>,
    /// Action taken when the target row already exists.
    pub action: SqlBoundConflictAction,
}

/// Conflict action of an `ON CONFLICT` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundConflictAction {
    /// `DO NOTHING` — skip the conflicting row.
    DoNothing,
    /// `DO UPDATE SET ... [WHERE ...]` — rewrite the conflicting row in place.
    DoUpdate {
        /// Column → expression assignments applied to the existing row.
        assignments: Vec<SqlBoundAssignment>,
        /// Optional predicate restricting which conflicting rows are rewritten.
        predicate: Option<SqlBoundExpression>,
    },
}

/// SQL UPDATE request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundUpdate {
    /// Target table name.
    pub table: String,
    /// Column → expression assignments in source order.
    pub assignments: Vec<SqlBoundAssignment>,
    /// Optional SQL WHERE expression.
    pub predicate: Option<SqlBoundExpression>,
}

/// One SET column = expression binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundAssignment {
    /// Column name as written by the client.
    pub column: String,
    /// Bound value expression.
    pub value: SqlBoundExpression,
}

/// SQL DELETE request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundDelete {
    /// Target table name.
    pub table: String,
    /// Optional SQL WHERE expression.
    pub predicate: Option<SqlBoundExpression>,
}

/// SQL set operator retained by the gateway binder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBoundSetOperator {
    /// `UNION` — concatenation with duplicate elimination.
    Union,
    /// `UNION ALL` — concatenation keeping duplicates.
    UnionAll,
    /// `INTERSECT` — distinct rows present on both sides.
    Intersect,
    /// `INTERSECT ALL` — each row kept with the smaller of its two multiplicities.
    IntersectAll,
    /// `EXCEPT` — distinct left rows absent from the right side.
    Except,
    /// `EXCEPT ALL` — each left row cancelled once per right occurrence, and the
    /// remaining multiplicity emitted.
    ExceptAll,
}

impl SqlBoundSetOperator {
    /// Whether this operator belongs to the tighter `INTERSECT` precedence level.
    ///
    /// SQL gives `INTERSECT` and `INTERSECT ALL` the same precedence, above
    /// `UNION`, `UNION ALL`, `EXCEPT` and `EXCEPT ALL`, so the precedence fold
    /// must group both spellings together.
    fn is_intersect(self) -> bool {
        matches!(self, Self::Intersect | Self::IntersectAll)
    }
}

/// One operand of a SQL set operation.
///
/// Oak publishes a flat operand chain, so the precedence fold may place an
/// `INTERSECT` group here while leaves stay plain `SELECT` bindings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundSetOperand {
    /// One bound `SELECT` operand.
    Select(Box<SqlBoundSelect>),
    /// A nested set-operation group produced by the `INTERSECT` precedence fold.
    SetOperation(Box<SqlBoundSetOperation>),
}

/// SQL set operation request owned by the compatibility gateway.
///
/// `operands` and `operators` describe one precedence level: `operators` holds
/// `operands.len() - 1` entries, each combining its two neighbours
/// left-associatively. The output column names come from the leftmost operand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundSetOperation {
    /// Operands in left-to-right order.
    pub operands: Vec<SqlBoundSetOperand>,
    /// Operators between consecutive operands.
    pub operators: Vec<SqlBoundSetOperator>,
    /// `ORDER BY` applied to the combined result.
    pub order_by: Option<Vec<SqlBoundOrderByItem>>,
    /// Optional `LIMIT` count expression applied to the combined result.
    pub limit: Option<SqlBoundExpression>,
    /// Optional `OFFSET` count expression applied to the combined result.
    pub offset: Option<SqlBoundExpression>,
}

/// One SQL projection expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundProjection {
    /// `*` projection, resolved later against the cluster catalog.
    Star,
    /// Expression projection with an optional client alias.
    Expression {
        /// Bound expression.
        expression: SqlBoundExpression,
        /// Optional SQL alias.
        alias: Option<String>,
    },
}

/// SQL expression retained by the gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundExpression {
    /// Identifier that still needs catalog binding.
    Identifier(String),
    /// SQL literal preserving surface spelling where relevant.
    Literal(SqlBoundLiteral),
    /// A bare `*` argument, as written by `COUNT(*)`.
    Wildcard,
    /// SQL binary operator. Its null semantics remain SQL semantics.
    Binary {
        /// Left operand.
        left: Box<Self>,
        /// Operator.
        operator: SqlBoundBinaryOperator,
        /// Right operand.
        right: Box<Self>,
    },
    /// SQL unary operator.
    Unary {
        /// Operator spelling.
        operator: SqlBoundUnaryOperator,
        /// Operand.
        expression: Box<Self>,
    },
    /// A SQL function call — currently only aggregate functions (COUNT/SUM/AVG/MAX/MIN)
    /// are recognized as first-class. Unknown function names are rejected at bind time.
    Function {
        /// Canonical lower-case function name.
        name: String,
        /// Bound argument expressions.
        args: Vec<SqlBoundExpression>,
        /// Whether the aggregate uses `DISTINCT` quantifier. Only meaningful on COUNT/SUM/AVG.
        distinct: bool,
        /// `OVER (...)` clause when the call is a window function.
        window: Option<SqlBoundWindowSpec>,
    },
    /// `expr IN (<subquery>)` / `expr NOT IN (<subquery>)`.
    InSubquery {
        /// Left-hand side expression.
        expression: Box<Self>,
        /// Bound subquery.
        select: Box<SqlBoundSelect>,
        /// Whether the predicate is spelled `NOT IN`.
        negated: bool,
    },
    /// `(SELECT ...)` used where a single value is expected.
    ScalarSubquery {
        /// Bound subquery whose one output column supplies the value.
        select: Box<SqlBoundSelect>,
    },
    /// `[NOT] EXISTS (<subquery>)`.
    Exists {
        /// Bound subquery whose rows are tested for existence.
        select: Box<SqlBoundSelect>,
        /// Whether the predicate is spelled `NOT EXISTS`.
        negated: bool,
    },
    /// `CASE WHEN cond THEN result ... [ELSE fallback] END`.
    Case {
        /// `(when, then)` branches in source order.
        branches: Vec<(Box<Self>, Box<Self>)>,
        /// Optional ELSE fallback.
        else_branch: Option<Box<Self>>,
    },
    /// `CAST(expr AS type_name)`.
    Cast {
        /// The expression being cast.
        expr: Box<Self>,
        /// The target type spelling as written (e.g. `INTEGER`, `TEXT`, `REAL`, `BLOB`).
        target: String,
    },
}

/// SQL aggregate function identity recognized by the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlAggregateFunction {
    /// `COUNT(...)`.
    COUNT,
    /// `SUM(...)`.
    SUM,
    /// `AVG(...)`.
    AVG,
    /// `MAX(...)`.
    MAX,
    /// `MIN(...)`.
    MIN,
}

impl SqlAggregateFunction {
    /// Resolves an aggregate function name case-insensitively.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "COUNT" => Some(Self::COUNT),
            "SUM" => Some(Self::SUM),
            "AVG" => Some(Self::AVG),
            "MAX" => Some(Self::MAX),
            "MIN" => Some(Self::MIN),
            _ => None,
        }
    }
    /// Canonical SQL spelling of this aggregate function.
    pub fn name(&self) -> &'static str {
        match self {
            Self::COUNT => "COUNT",
            Self::SUM => "SUM",
            Self::AVG => "AVG",
            Self::MAX => "MAX",
            Self::MIN => "MIN",
        }
    }
}

/// SQL window-only function identity recognized by the gateway.
///
/// Ranking functions take no argument; `NTILE` takes one integer bucket count;
/// `LAG` / `LEAD` take a value expression plus an optional integer row offset
/// and default. Every one of them requires an `OVER (...)` clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlWindowFunction {
    /// `ROW_NUMBER()` — 1-based position inside the window ordering.
    RowNumber,
    /// `RANK()` — position of the first peer row, so ties share a rank and gaps remain.
    Rank,
    /// `DENSE_RANK()` — peer group ordinal without gaps.
    DenseRank,
    /// `NTILE(n)` — 1-based ordinal of the evenly sized bucket the row falls into.
    Ntile,
    /// `LAG(expr[, offset[, default]])` — value from `offset` rows before the current one.
    Lag,
    /// `LEAD(expr[, offset[, default]])` — value from `offset` rows after the current one.
    Lead,
}

impl SqlWindowFunction {
    /// Resolves a window function name case-insensitively.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "ROW_NUMBER" => Some(Self::RowNumber),
            "RANK" => Some(Self::Rank),
            "DENSE_RANK" => Some(Self::DenseRank),
            "NTILE" => Some(Self::Ntile),
            "LAG" => Some(Self::Lag),
            "LEAD" => Some(Self::Lead),
            _ => None,
        }
    }
    /// Canonical SQL spelling of this window function.
    pub fn name(&self) -> &'static str {
        match self {
            Self::RowNumber => "ROW_NUMBER",
            Self::Rank => "RANK",
            Self::DenseRank => "DENSE_RANK",
            Self::Ntile => "NTILE",
            Self::Lag => "LAG",
            Self::Lead => "LEAD",
        }
    }
}

/// SQL scalar (non-aggregate) function identity recognized by the gateway.
///
/// These are the functions drivers and ORMs probe on connect. They take no
/// table source and evaluate to a single value per row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlScalarFunction {
    /// `VERSION()` — engine banner.
    Version,
    /// `DATABASE()` — current schema name.
    Database,
    /// `CURRENT_SCHEMA()` / `SCHEMA()` — current schema name.
    CurrentSchema,
    /// `NOW()` / `CURRENT_TIMESTAMP()` / bare `CURRENT_TIMESTAMP` — UTC session
    /// timestamp rendered as TEXT `YYYY-MM-DD HH:MM:SS`.
    Now,
    /// `CURRENT_DATE()` / bare `CURRENT_DATE` — UTC date rendered as TEXT `YYYY-MM-DD`.
    CurrentDate,
    /// `CURRENT_TIME()` / bare `CURRENT_TIME` — UTC time rendered as TEXT `HH:MM:SS`.
    CurrentTime,
    /// `CURDATE()` — MySQL spelling of [`Self::CurrentDate`], rejected by PostgreSQL.
    Curdate,
    /// `CURTIME()` — MySQL spelling of [`Self::CurrentTime`], rejected by PostgreSQL.
    Curtime,
    /// `LOWER(x)` / `LCASE(x)`.
    Lower,
    /// `UPPER(x)` / `UCASE(x)`.
    Upper,
    /// `LENGTH(x)` — PostgreSQL counts characters, MySQL counts UTF-8 bytes.
    Length,
    /// `CHAR_LENGTH(x)` / `CHARACTER_LENGTH(x)` / `LEN(x)` — character count in both dialects.
    CharLength,
    /// `SUBSTRING(x, start[, length])` / `SUBSTR(x, start[, length])` / `MID(x, start[, length])`.
    Substring,
    /// `TRIM(x[, chars])` / `BTRIM(x[, chars])`.
    Trim,
    /// `LTRIM(x[, chars])`.
    Ltrim,
    /// `RTRIM(x[, chars])`.
    Rtrim,
    /// `REPLACE(x, from, to)`.
    Replace,
    /// `CONCAT(a, b, ...)` — MySQL propagates NULL, PostgreSQL skips NULL arguments.
    Concat,
    /// `COALESCE(a, b, ...)` / `IFNULL(a, b)`.
    Coalesce,
    /// `ABS(x)`.
    Abs,
    /// `ROUND(x[, digits])`.
    Round,
    /// `INSTR(haystack, needle)` — MySQL spelling, 1-based character position, `0` when absent.
    Instr,
    /// `STRPOS(haystack, needle)` — PostgreSQL spelling of [`Self::Instr`].
    Strpos,
    /// `LEFT(text, n)` — first `n` characters.
    Left,
    /// `RIGHT(text, n)` — last `n` characters.
    Right,
}

impl SqlScalarFunction {
    /// Resolves a scalar function name case-insensitively, including dialect aliases.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "VERSION" => Some(Self::Version),
            "DATABASE" | "CURRENT_DATABASE" => Some(Self::Database),
            "CURRENT_SCHEMA" | "SCHEMA" => Some(Self::CurrentSchema),
            "NOW" | "CURRENT_TIMESTAMP" => Some(Self::Now),
            "CURRENT_DATE" => Some(Self::CurrentDate),
            "CURRENT_TIME" => Some(Self::CurrentTime),
            "CURDATE" => Some(Self::Curdate),
            "CURTIME" => Some(Self::Curtime),
            "LOWER" | "LCASE" => Some(Self::Lower),
            "UPPER" | "UCASE" => Some(Self::Upper),
            "LENGTH" => Some(Self::Length),
            "CHAR_LENGTH" | "CHARACTER_LENGTH" | "LEN" => Some(Self::CharLength),
            "SUBSTRING" | "SUBSTR" | "MID" => Some(Self::Substring),
            "TRIM" | "BTRIM" => Some(Self::Trim),
            "LTRIM" => Some(Self::Ltrim),
            "RTRIM" => Some(Self::Rtrim),
            "REPLACE" => Some(Self::Replace),
            "CONCAT" => Some(Self::Concat),
            "COALESCE" | "IFNULL" => Some(Self::Coalesce),
            "ABS" => Some(Self::Abs),
            "ROUND" => Some(Self::Round),
            // 文本谓词：两种方言各用各的拼写，跨方言调用在求值期报未知函数。
            "INSTR" => Some(Self::Instr),
            "STRPOS" => Some(Self::Strpos),
            "LEFT" => Some(Self::Left),
            "RIGHT" => Some(Self::Right),
            _ => None,
        }
    }

    /// Niladic spellings the SQL standard writes without parentheses.
    ///
    /// Oak's SQL tree has no node for these keywords, so the catalog binder falls
    /// back to them only after an unqualified identifier fails to resolve to a
    /// column. `NOW` is deliberately absent: both dialects require `NOW()`.
    pub fn from_keyword(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "CURRENT_DATE" => Some(Self::CurrentDate),
            "CURRENT_TIME" => Some(Self::CurrentTime),
            "CURRENT_TIMESTAMP" => Some(Self::Now),
            _ => None,
        }
    }

    /// Canonical spelling used by diagnostics for this function identity.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Version => "VERSION",
            Self::Database => "DATABASE",
            Self::CurrentSchema => "CURRENT_SCHEMA",
            Self::Now => "NOW",
            Self::CurrentDate => "CURRENT_DATE",
            Self::CurrentTime => "CURRENT_TIME",
            Self::Curdate => "CURDATE",
            Self::Curtime => "CURTIME",
            Self::Lower => "LOWER",
            Self::Upper => "UPPER",
            Self::Length => "LENGTH",
            Self::CharLength => "CHAR_LENGTH",
            Self::Substring => "SUBSTRING",
            Self::Trim => "TRIM",
            Self::Ltrim => "LTRIM",
            Self::Rtrim => "RTRIM",
            Self::Replace => "REPLACE",
            Self::Concat => "CONCAT",
            Self::Coalesce => "COALESCE",
            Self::Abs => "ABS",
            Self::Round => "ROUND",
            Self::Instr => "INSTR",
            Self::Strpos => "STRPOS",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
        }
    }

    /// Expected argument count as `(minimum, maximum)`, where `maximum == None`
    /// means variadic.
    pub fn arity(&self) -> (usize, Option<usize>) {
        match self {
            Self::Version
            | Self::Database
            | Self::CurrentSchema
            | Self::Now
            | Self::CurrentDate
            | Self::CurrentTime
            | Self::Curdate
            | Self::Curtime => (0, Some(0)),
            Self::Lower | Self::Upper | Self::Length | Self::CharLength | Self::Abs => (1, Some(1)),
            Self::Round => (1, Some(2)),
            Self::Substring => (2, Some(3)),
            Self::Trim | Self::Ltrim | Self::Rtrim => (1, Some(2)),
            Self::Replace => (3, Some(3)),
            Self::Instr | Self::Strpos | Self::Left | Self::Right => (2, Some(2)),
            Self::Concat | Self::Coalesce => (1, None),
        }
    }
}

/// SQL literal retained by the gateway surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundLiteral {
    /// Integer literal kept exact as `i64`; never round-trips through `f64`.
    Integer(i64),
    /// Non-integer numeric SQL token (decimal point or exponent), kept in source spelling.
    Number(String),
    /// SQL string literal contents.
    String(String),
    /// Raw octets produced by `CAST(<hex> AS BYTEA|BLOB)`. The gateway renders
    /// binary bind parameters as exactly that cast because the SQL text
    /// substitution layer cannot carry non-UTF-8 octets inside a string literal.
    Bytes(Vec<u8>),
    /// SQL boolean literal.
    Boolean(bool),
    /// SQL NULL literal.
    Null,
}

/// SQL binary operator retained without changing SQL three-valued semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBoundBinaryOperator {
    /// `+`.
    Plus,
    /// `-`.
    Minus,
    /// `*`.
    Multiply,
    /// `/`.
    Divide,
    /// `%`.
    Modulo,
    /// `AND`.
    And,
    /// `OR`.
    Or,
    /// `=`.
    Equal,
    /// `<>` or `!=`.
    NotEqual,
    /// `<`.
    Less,
    /// `>`.
    Greater,
    /// `<=`.
    LessEqual,
    /// `>=`.
    GreaterEqual,
    /// `LIKE`.
    Like,
}

/// SQL unary operator retained by the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBoundUnaryOperator {
    /// Unary `+`.
    Plus,
    /// Unary `-`.
    Minus,
    /// `NOT`.
    Not,
    /// `IS NULL` — null-existence test, never compares against a value.
    IsNull,
    /// `IS NOT NULL`.
    IsNotNull,
    /// `IS [NOT] TRUE` / `IS [NOT] FALSE` — three-valued truth test, not a comparison.
    IsTruth {
        /// The boolean value the operand is tested against.
        value: bool,
        /// Whether the predicate is spelled `IS NOT`.
        negated: bool,
    },
}

/// Binds the supported SQL compatibility subset without executing it.
pub fn bind_sql(source: &str, dialect: SqlDialect) -> Result<SqlBoundStatement, SqlFrontendError> {
    let root = parse_sql(source, dialect)?;
    if root.statements.len() != 1 {
        return Err(bind_error("SQL gateway accepts exactly one statement"));
    }
    match &root.statements[0] {
        SqlStatement::Select(select) => Ok(SqlBoundStatement::Select(bind_select_statement(select)?)),
        SqlStatement::Insert(insert) => {
            // `ON CONFLICT` 保留冲突目标与动作；方言与唯一约束校验在 planner 层完成。
            let conflict = match &insert.on_conflict {
                None => None,
                Some(conflict) => Some(SqlBoundConflict {
                    columns: conflict.columns.iter().map(|id| normalize_identifier(id.name.as_ref())).collect(),
                    action: match &conflict.action {
                        oak_sql::ast::OnConflictAction::DoNothing => SqlBoundConflictAction::DoNothing,
                        oak_sql::ast::OnConflictAction::DoUpdate { assignments, selection } => SqlBoundConflictAction::DoUpdate {
                            assignments: assignments
                                .iter()
                                .map(|assignment| {
                                    Ok(SqlBoundAssignment {
                                        column: normalize_identifier(assignment.column.name.as_ref()),
                                        value: bind_expression(&assignment.value)?,
                                    })
                                })
                                .collect::<Result<Vec<_>, SqlFrontendError>>()?,
                            predicate: selection.as_ref().map(bind_expression).transpose()?,
                        },
                    },
                }),
            };
            let returning = match &insert.returning {
                None => None,
                Some(clause) => Some(clause.items.iter().map(bind_projection).collect::<Result<Vec<_>, _>>()?),
            };
            let columns: Option<Vec<String>> = if insert.columns.is_empty() {
                None
            }
            else {
                Some(insert.columns.iter().map(|id| normalize_identifier(id.name.as_ref())).collect())
            };
            let mut rows = Vec::with_capacity(insert.rows.len());
            for row in &insert.rows {
                rows.push(row.iter().map(bind_expression).collect::<Result<Vec<_>, _>>()?);
            }
            // `INSERT ... SELECT` 复用唯一一份 SELECT 绑定，不另造一套值绑定。
            let source = match &insert.source {
                oak_sql::ast::InsertSource::Values => SqlBoundInsertSource::Values,
                oak_sql::ast::InsertSource::DefaultValues => SqlBoundInsertSource::DefaultValues,
                oak_sql::ast::InsertSource::Select(select) => SqlBoundInsertSource::Select(Box::new(bind_select_statement(select)?)),
            };
            // `DEFAULT VALUES` 自身不携带任何值，行宽校验只对真正的 `VALUES` 列表成立。
            if matches!(source, SqlBoundInsertSource::Values) {
                let Some(width) = rows.first().map(Vec::len)
                else {
                    return Err(bind_error("INSERT has no VALUES rows"));
                };
                if rows.iter().any(|row| row.len() != width) {
                    return Err(bind_error("INSERT VALUES rows must all carry the same number of values"));
                }
                if let Some(cols) = &columns {
                    if cols.len() != width {
                        return Err(bind_error(&format!(
                            "INSERT column/value count mismatch: {} columns, {} values per row",
                            cols.len(),
                            width
                        )));
                    }
                }
            }
            Ok(SqlBoundStatement::Insert(SqlBoundInsert {
                table: normalize_identifier(insert.table_name.name.name.as_ref()),
                columns,
                rows,
                source,
                conflict,
                returning,
            }))
        }
        SqlStatement::Update(update) => {
            let assignments = update
                .assignments
                .iter()
                .map(|a| Ok(SqlBoundAssignment { column: normalize_identifier(a.column.name.as_ref()), value: bind_expression(&a.value)? }))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(SqlBoundStatement::Update(SqlBoundUpdate {
                table: normalize_identifier(update.table_name.name.name.as_ref()),
                assignments,
                predicate: update.selection.as_ref().map(bind_expression).transpose()?,
            }))
        }
        SqlStatement::Delete(delete) => Ok(SqlBoundStatement::Delete(SqlBoundDelete {
            table: normalize_identifier(delete.table_name.name.name.as_ref()),
            predicate: delete.selection.as_ref().map(bind_expression).transpose()?,
        })),
        SqlStatement::SetOperation(set) => Ok(SqlBoundStatement::SetOperation(bind_set_operation(set)?)),
        _ => Err(SqlFrontendError::unsupported("SQL gateway binding accepts SELECT/INSERT/UPDATE/DELETE only")),
    }
}

/// Binds the non-recursive `WITH` prefix into CTE definitions.
///
/// Each body goes through the same [`bind_select_statement`] path as every other
/// query, so a CTE can only carry shapes the gateway already binds. Oak rejects
/// `WITH RECURSIVE` while parsing. An explicit column list renames the CTE output;
/// it is carried here and applied to the flattened column scope at catalog bind time.
fn bind_with_clause(clause: Option<&oak_sql::ast::WithClause>) -> Result<Vec<SqlBoundCte>, SqlFrontendError> {
    let Some(clause) = clause
    else {
        return Ok(Vec::new());
    };
    if clause.ctes.is_empty() {
        return Err(bind_error("SQL common table expression list must not be empty"));
    }
    let mut ctes: Vec<SqlBoundCte> = Vec::with_capacity(clause.ctes.len());
    for cte in &clause.ctes {
        let name = normalize_identifier(cte.name.name.as_ref());
        if ctes.iter().any(|existing| existing.name == name) {
            return Err(bind_error(&format!("SQL common table expression `{name}` is defined more than once")));
        }
        let columns: Vec<String> = cte.columns.iter().map(|column| normalize_identifier(column.name.as_ref())).collect();
        if let Some(duplicate) =
            columns.iter().enumerate().find_map(|(index, column)| columns[..index].contains(column).then_some(column.as_str()))
        {
            return Err(bind_error(&format!("SQL common table expression `{name}` lists output column `{duplicate}` more than once")));
        }
        ctes.push(SqlBoundCte { name, columns, query: Box::new(bind_select_statement(&cte.query)?) });
    }
    Ok(ctes)
}

/// Binds one Oak `SELECT` — shared by top-level statements and `IN (SELECT ...)` subqueries.
fn bind_select_statement(select: &oak_sql::ast::SelectStatement) -> Result<SqlBoundSelect, SqlFrontendError> {
    let with = bind_with_clause(select.with.as_ref())?;
    let distinct = match &select.quantifier {
        None | Some(oak_sql::ast::SelectQuantifier::All { .. }) => false,
        Some(oak_sql::ast::SelectQuantifier::Distinct { .. }) => true,
        Some(oak_sql::ast::SelectQuantifier::DistinctOn { .. }) => {
            return Err(SqlFrontendError::unsupported("SELECT DISTINCT ON is outside the gateway subset"));
        }
    };
    let projections = select.items.iter().map(bind_projection).collect::<Result<Vec<_>, _>>()?;
    let (limit, offset) = bind_limit_clause(select.limit.as_ref())?;
    let order_by = bind_order_by_clause(select.order_by.as_ref())?;
    let group_by = match &select.group_by {
        Some(clause) if !clause.items.is_empty() => {
            let mut expressions = Vec::with_capacity(clause.items.len());
            for item in &clause.items {
                expressions.push(match item {
                    oak_sql::ast::GroupByItem::Column(identifier) => {
                        SqlBoundExpression::Identifier(normalize_identifier(identifier.name.as_ref()))
                    }
                    oak_sql::ast::GroupByItem::Expression(expression) => bind_expression(expression)?,
                    oak_sql::ast::GroupByItem::Position { index, .. } => match projections.get(index.saturating_sub(1)) {
                        Some(SqlBoundProjection::Expression { expression, .. }) => expression.clone(),
                        Some(SqlBoundProjection::Star) => {
                            return Err(SqlFrontendError::unsupported("GROUP BY ordinal position cannot reference a star projection"));
                        }
                        None => {
                            return Err(SqlFrontendError::unsupported("GROUP BY ordinal position is outside the projection list"));
                        }
                    },
                    oak_sql::ast::GroupByItem::GroupingSets(_) | oak_sql::ast::GroupByItem::Rollup(_) | oak_sql::ast::GroupByItem::Cube(_) => {
                        return Err(SqlFrontendError::unsupported(
                            "GROUPING SETS, ROLLUP and CUBE are not yet supported by the cluster gateway",
                        ));
                    }
                });
            }
            Some(expressions)
        }
        _ => None,
    };
    let having = select.having.as_ref().map(|h| bind_expression(&h.condition)).transpose()?;
    let joins = select
        .joins
        .iter()
        .map(|join| {
            let kind = match join.join_type {
                oak_sql::ast::JoinType::Inner => SqlBoundJoinKind::Inner,
                oak_sql::ast::JoinType::Left => SqlBoundJoinKind::Left,
                oak_sql::ast::JoinType::Right => SqlBoundJoinKind::Right,
                oak_sql::ast::JoinType::Full => SqlBoundJoinKind::Full,
                oak_sql::ast::JoinType::Cross => SqlBoundJoinKind::Cross,
            };
            let on = if matches!(join.join_type, oak_sql::ast::JoinType::Cross) {
                // CROSS JOIN has no ON clause — use a tautology so downstream
                // binders keep treating it as a predicate. The executor ignores
                // the condition for the Cartesian product.
                SqlBoundExpression::Literal(SqlBoundLiteral::Boolean(true))
            }
            else {
                join.on.as_ref().ok_or_else(|| bind_error("SQL JOIN requires an ON condition")).and_then(bind_expression)?
            };
            Ok(SqlBoundJoin { kind, source: bind_table_source(&join.table)?, on })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SqlBoundSelect {
        with,
        source: select.from.as_ref().map(bind_table_source).transpose()?,
        joins,
        projections,
        predicate: select.expr.as_ref().map(bind_expression).transpose()?,
        group_by,
        having,
        limit,
        offset,
        order_by,
        distinct,
    })
}

/// Binds one Oak set operation. `ORDER BY` stays in surface form because it
/// references the combined output columns rather than catalog columns.
fn bind_set_operation(set: &oak_sql::ast::SetOperationStatement) -> Result<SqlBoundSetOperation, SqlFrontendError> {
    let (limit, offset) = bind_limit_clause(set.limit.as_ref())?;
    let order_by = bind_order_by_clause(set.order_by.as_ref())?;
    let operands = set
        .operands
        .iter()
        .map(|operand| bind_select_statement(operand).map(|select| SqlBoundSetOperand::Select(Box::new(select))))
        .collect::<Result<Vec<_>, _>>()?;
    let operators = set.operators.iter().copied().map(bind_set_operator).collect::<Vec<_>>();
    fold_set_operation(operands, operators, order_by, limit, offset)
}

fn bind_set_operator(operator: oak_sql::ast::SetOperator) -> SqlBoundSetOperator {
    match operator {
        oak_sql::ast::SetOperator::Union => SqlBoundSetOperator::Union,
        oak_sql::ast::SetOperator::UnionAll => SqlBoundSetOperator::UnionAll,
        oak_sql::ast::SetOperator::Intersect => SqlBoundSetOperator::Intersect,
        oak_sql::ast::SetOperator::IntersectAll => SqlBoundSetOperator::IntersectAll,
        oak_sql::ast::SetOperator::Except => SqlBoundSetOperator::Except,
        oak_sql::ast::SetOperator::ExceptAll => SqlBoundSetOperator::ExceptAll,
    }
}

/// Folds Oak's flat set-operation chain into a precedence tree.
///
/// SQL gives `INTERSECT` / `INTERSECT ALL` tighter precedence than `UNION` /
/// `UNION ALL` / `EXCEPT` / `EXCEPT ALL`, and combines operators of equal
/// precedence left-associatively. Consecutive `INTERSECT` operands therefore
/// collapse into their own level first, and the remaining operators chain
/// left-to-right over those levels. Every operator inside a collapsed level is
/// kept as written, so an `INTERSECT ALL` in the group stays `ALL` instead of
/// degrading to the distinct `INTERSECT`. A flat chain of equal precedence
/// produces exactly the same rows as its left-associative binary tree, so this
/// fold needs no binary nesting.
fn fold_set_operation(
    operands: Vec<SqlBoundSetOperand>,
    operators: Vec<SqlBoundSetOperator>,
    order_by: Option<Vec<SqlBoundOrderByItem>>,
    limit: Option<SqlBoundExpression>,
    offset: Option<SqlBoundExpression>,
) -> Result<SqlBoundSetOperation, SqlFrontendError> {
    if operands.len() < 2 || operators.len() + 1 != operands.len() {
        return Err(bind_error("set operation operands and operators do not line up"));
    }
    let mut rest = operands.into_iter();
    let first = rest.next().ok_or_else(|| bind_error("set operation requires at least two operands"))?;
    let mut group = vec![first];
    let mut group_operators: Vec<SqlBoundSetOperator> = Vec::new();
    let mut groups: Vec<SqlBoundSetOperand> = Vec::new();
    let mut join_operators: Vec<SqlBoundSetOperator> = Vec::new();
    for (operator, operand) in operators.into_iter().zip(rest) {
        if operator.is_intersect() {
            group_operators.push(operator);
            group.push(operand);
            continue;
        }
        groups.push(close_intersect_group(std::mem::take(&mut group), std::mem::take(&mut group_operators)));
        join_operators.push(operator);
        group.push(operand);
    }
    groups.push(close_intersect_group(group, group_operators));

    if join_operators.is_empty() {
        // 全部操作数由 `INTERSECT` 连接：折叠出的组本身就是顶层节点。
        let SqlBoundSetOperand::SetOperation(group) = groups.pop().ok_or_else(|| bind_error("set operation requires at least two operands"))?
        else {
            return Err(bind_error("set operation requires at least two operands"));
        };
        let mut node = *group;
        node.order_by = order_by;
        node.limit = limit;
        node.offset = offset;
        return Ok(node);
    }
    Ok(SqlBoundSetOperation { operands: groups, operators: join_operators, order_by, limit, offset })
}

/// One `INTERSECT` precedence level: a single operand stays a leaf, several
/// operands become a nested node carrying the group's written `INTERSECT` /
/// `INTERSECT ALL` operators.
fn close_intersect_group(operands: Vec<SqlBoundSetOperand>, operators: Vec<SqlBoundSetOperator>) -> SqlBoundSetOperand {
    if operands.len() == 1 {
        return operands.into_iter().next().expect("single operand");
    }
    SqlBoundSetOperand::SetOperation(Box::new(SqlBoundSetOperation { operands, operators, order_by: None, limit: None, offset: None }))
}

/// Binds an Oak `LIMIT` clause into gateway expressions.
///
/// Oak publishes `LIMIT` and `OFFSET` as expressions, so the gateway keeps them
/// in the normal expression system and folds them to a row count while planning.
/// Only the planner knows the statement dialect, and folding there keeps one
/// evaluation path instead of a second literal-only parser.
///
/// `LIMIT` is optional in the AST: a standalone `OFFSET n` binds to
/// `(None, Some(n))` and must skip rows without capping the result, while
/// `LIMIT m` alone keeps its row-count meaning. The keyword split is Oak's, so
/// the gateway never re-reads the text to guess which clause was written.
///
/// Anything that would need a row — column references, `*`, subqueries or
/// window functions — is rejected here. Silently dropping the clause is never
/// an option: `OFFSET` changes the result set, so an unsupported shape fails the
/// statement.
fn bind_limit_clause(
    clause: Option<&oak_sql::ast::LimitClause>,
) -> Result<(Option<SqlBoundExpression>, Option<SqlBoundExpression>), SqlFrontendError> {
    let Some(clause) = clause
    else {
        return Ok((None, None));
    };
    let limit = match clause.limit.as_ref() {
        Some(limit) => {
            let bound = bind_expression(limit)?;
            validate_row_count_expression(&bound, "LIMIT")?;
            Some(bound)
        }
        None => None,
    };
    let offset = match clause.offset.as_ref() {
        Some(offset) => {
            let bound = bind_expression(offset)?;
            validate_row_count_expression(&bound, "OFFSET")?;
            Some(bound)
        }
        None => None,
    };
    Ok((limit, offset))
}

/// Rejects `LIMIT` / `OFFSET` shapes that cannot be evaluated without a row.
///
/// The remaining expression still has to fold to a non-negative integer
/// scalar, but that check needs the dialect's arithmetic semantics and so runs
/// in the planner.
fn validate_row_count_expression(expression: &SqlBoundExpression, clause: &str) -> Result<(), SqlFrontendError> {
    match expression {
        SqlBoundExpression::Literal(_) => Ok(()),
        SqlBoundExpression::Identifier(name) => {
            Err(bind_error(&format!("SQL {clause} must be a constant non-negative integer, `{name}` is not constant")))
        }
        SqlBoundExpression::Wildcard => Err(bind_error(&format!("SQL {clause} must be a constant non-negative integer, `*` is not valid"))),
        SqlBoundExpression::Binary { left, right, .. } => {
            validate_row_count_expression(left, clause)?;
            validate_row_count_expression(right, clause)
        }
        SqlBoundExpression::Unary { expression, .. } => validate_row_count_expression(expression, clause),
        SqlBoundExpression::Function { name, args, window, .. } => {
            if window.is_some() {
                return Err(SqlFrontendError::unsupported(format!(
                    "SQL {clause} must be a constant non-negative integer, window function `{name}` is not constant"
                )));
            }
            for arg in args {
                validate_row_count_expression(arg, clause)?;
            }
            Ok(())
        }
        SqlBoundExpression::Case { branches, else_branch } => {
            for (when, then) in branches {
                validate_row_count_expression(when, clause)?;
                validate_row_count_expression(then, clause)?;
            }
            match else_branch {
                Some(else_branch) => validate_row_count_expression(else_branch, clause),
                None => Ok(()),
            }
        }
        SqlBoundExpression::Cast { expr, .. } => validate_row_count_expression(expr, clause),
        SqlBoundExpression::ScalarSubquery { .. } | SqlBoundExpression::InSubquery { .. } | SqlBoundExpression::Exists { .. } => {
            Err(SqlFrontendError::unsupported(format!("SQL {clause} does not accept a subquery")))
        }
    }
}

/// Binds an Oak `ORDER BY` clause into gateway order items.
fn bind_order_by_clause(clause: Option<&oak_sql::ast::OrderByClause>) -> Result<Option<Vec<SqlBoundOrderByItem>>, SqlFrontendError> {
    Ok(match clause {
        Some(clause) if !clause.items.is_empty() => {
            let items = clause
                .items
                .iter()
                .map(|item| {
                    Ok(SqlBoundOrderByItem {
                        expr: bind_expression(&item.expr)?,
                        direction: match item.direction {
                            oak_sql::ast::OrderDirection::Asc => SqlBoundOrderDirection::Asc,
                            oak_sql::ast::OrderDirection::Desc => SqlBoundOrderDirection::Desc,
                        },
                        nulls: item.nulls.map(|nulls| match nulls {
                            oak_sql::ast::NullsOrder::First => SqlNullsOrder::First,
                            oak_sql::ast::NullsOrder::Last => SqlNullsOrder::Last,
                        }),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Some(items)
        }
        _ => None,
    })
}

/// Binds an Oak `OVER (...)` clause into a gateway window spec.
///
/// Oak only parses `PARTITION BY <column>` and `ORDER BY <column>` inside a
/// window, so every partitioning / ordering term is a plain identifier.
fn bind_window_spec(spec: &oak_sql::ast::WindowSpec) -> Result<SqlBoundWindowSpec, SqlFrontendError> {
    let partition_by =
        spec.partition_by.iter().map(|column| SqlBoundExpression::Identifier(normalize_identifier(column.name.as_ref()))).collect();
    let order_by: Vec<SqlBoundOrderByItem> = spec
        .order_by
        .iter()
        .map(|item| SqlBoundOrderByItem {
            expr: SqlBoundExpression::Identifier(normalize_identifier(item.column.name.as_ref())),
            direction: match item.direction {
                oak_sql::ast::OrderDirection::Asc => SqlBoundOrderDirection::Asc,
                oak_sql::ast::OrderDirection::Desc => SqlBoundOrderDirection::Desc,
            },
            nulls: item.nulls.map(|nulls| match nulls {
                oak_sql::ast::NullsOrder::First => SqlNullsOrder::First,
                oak_sql::ast::NullsOrder::Last => SqlNullsOrder::Last,
            }),
        })
        .collect();
    let frame = spec.frame.as_ref().map(bind_window_frame).transpose()?;
    if let Some(frame) = frame.as_ref() {
        if frame.unit == SqlBoundWindowFrameUnit::Range {
            // `RANGE` 比较 `ORDER BY` 的值：数值偏移 bound 的「值距离」只在恰好一列
            // 排序上有唯一依据，零列与多列都显式拒绝，不退化成 `ROWS`。没有偏移的
            // `RANGE` 帧只用到 peer 边界，仍然需要一层排序。
            if frame.has_value_offset() && order_by.len() != 1 {
                return Err(SqlFrontendError::unsupported(
                    "SQL window `RANGE` frame offsets require exactly one ORDER BY column inside the OVER (...) clause",
                ));
            }
            if order_by.is_empty() {
                return Err(SqlFrontendError::unsupported("SQL window `RANGE` frames require an ORDER BY inside the OVER (...) clause"));
            }
        }
    }
    Ok(SqlBoundWindowSpec { partition_by, order_by, frame })
}

/// Binds an explicit window frame. `ROWS` counts physical row positions;
/// `RANGE` compares values on the window `ORDER BY`, so it requires an
/// `ORDER BY` and, for value offsets, exactly one ordering column. `GROUPS`
/// never reaches this point: the Oak AST has no `GROUPS` unit and the parser
/// rejects the spelling before binding.
fn bind_window_frame(frame: &oak_sql::ast::WindowFrame) -> Result<SqlBoundWindowFrame, SqlFrontendError> {
    let unit = match frame.unit {
        oak_sql::ast::WindowFrameUnit::Rows => SqlBoundWindowFrameUnit::Rows,
        oak_sql::ast::WindowFrameUnit::Range => SqlBoundWindowFrameUnit::Range,
    };
    let start = bind_window_frame_bound(frame.start);
    let end = match frame.end {
        Some(end) => Some(bind_window_frame_bound(end)),
        // 单 bound 的 `ROWS <start>` 就是 `BETWEEN <start> AND CURRENT ROW`。起点在
        // 当前行之后时该区间恒为空，标准 SQL 要求显式写出 end。
        None => {
            if matches!(start, SqlBoundWindowFrameBound::Following(_) | SqlBoundWindowFrameBound::UnboundedFollowing) {
                return Err(bind_error("SQL window frame starting at a following row requires `BETWEEN ... AND ...`"));
            }
            Some(SqlBoundWindowFrameBound::CurrentRow)
        }
    };
    Ok(SqlBoundWindowFrame { unit, start, end })
}

fn bind_window_frame_bound(bound: oak_sql::ast::WindowFrameBound) -> SqlBoundWindowFrameBound {
    match bound {
        oak_sql::ast::WindowFrameBound::UnboundedPreceding => SqlBoundWindowFrameBound::UnboundedPreceding,
        oak_sql::ast::WindowFrameBound::Preceding(value) => SqlBoundWindowFrameBound::Preceding(value),
        oak_sql::ast::WindowFrameBound::CurrentRow => SqlBoundWindowFrameBound::CurrentRow,
        oak_sql::ast::WindowFrameBound::Following(value) => SqlBoundWindowFrameBound::Following(value),
        oak_sql::ast::WindowFrameBound::UnboundedFollowing => SqlBoundWindowFrameBound::UnboundedFollowing,
    }
}

/// 提取窗口函数的整数常量参数：必须是整数字面量且不小于 `minimum`。
/// `NTILE` 的分桶数与 `LAG` / `LEAD` 的行偏移在绑定期就固定为常量，执行层只按
/// 同一形状防御性读取，不接受列引用或计算表达式。
fn window_integer_argument(argument: &SqlBoundExpression, display: &str, role: &str, minimum: i64) -> Result<i64, SqlFrontendError> {
    let SqlBoundExpression::Literal(SqlBoundLiteral::Integer(value)) = argument
    else {
        return Err(SqlFrontendError::unsupported(format!(
            "{display}() requires the {role} to be an integer literal, not a computed expression"
        )));
    };
    if *value < minimum {
        return Err(SqlFrontendError::unsupported(format!("{display}() requires the {role} to be >= {minimum}, got {value}")));
    }
    Ok(*value)
}

/// Binds an Oak `FROM` / `JOIN` source into a catalog table reference or a
/// derived table. A derived table is a full nested `SELECT`, so it is bound
/// through the same [`bind_select_statement`] path as every other query.
fn bind_table_source(source: &oak_sql::ast::TableSource) -> Result<SqlBoundSource, SqlFrontendError> {
    match source {
        oak_sql::ast::TableSource::Table(table) => Ok(SqlBoundSource::Table {
            name: normalize_identifier(table.name.name.as_ref()),
            alias: table.alias.as_ref().map(|alias| normalize_identifier(alias.name.as_ref())),
        }),
        oak_sql::ast::TableSource::Derived { query, alias, .. } => {
            let alias = alias.as_ref().ok_or_else(|| bind_error("SQL derived table requires an alias"))?;
            Ok(SqlBoundSource::Derived { query: Box::new(bind_select_statement(query)?), alias: normalize_identifier(alias.name.as_ref()) })
        }
    }
}

fn bind_projection(item: &SelectItem) -> Result<SqlBoundProjection, SqlFrontendError> {
    match item {
        SelectItem::Star { .. } => Ok(SqlBoundProjection::Star),
        SelectItem::Expression { expr, alias, .. } => Ok(SqlBoundProjection::Expression {
            expression: bind_expression(expr)?,
            alias: alias.as_ref().map(|alias| normalize_identifier(alias.name.as_ref())),
        }),
    }
}

fn bind_expression(expression: &Expression) -> Result<SqlBoundExpression, SqlFrontendError> {
    match expression {
        Expression::Identifier(identifier) => Ok(SqlBoundExpression::Identifier(normalize_identifier(identifier.name.as_ref()))),
        Expression::Literal(literal) => Ok(SqlBoundExpression::Literal(bind_literal(literal)?)),
        Expression::Wildcard { .. } => Ok(SqlBoundExpression::Wildcard),
        Expression::Binary { left, op, right, .. } => Ok(SqlBoundExpression::Binary {
            left: Box::new(bind_expression(left)?),
            operator: bind_binary_operator(*op)?,
            right: Box::new(bind_expression(right)?),
        }),
        Expression::Unary { op, expr, .. } => {
            // `-<integer>` 折叠成一个精确的负整数字面量：这样 `-9223372036854775808`
            // 仍落在 `i64` 内，而不是先解析成正数再溢出回浮点。
            if let oak_sql::ast::UnaryOperator::Minus = op {
                if let Expression::Literal(Literal::Number(text, _)) = expr.as_ref() {
                    if let Some(value) = negated_integer_literal(text) {
                        return Ok(SqlBoundExpression::Literal(SqlBoundLiteral::Integer(value)));
                    }
                }
            }
            Ok(SqlBoundExpression::Unary {
                operator: match op {
                    oak_sql::ast::UnaryOperator::Plus => SqlBoundUnaryOperator::Plus,
                    oak_sql::ast::UnaryOperator::Minus => SqlBoundUnaryOperator::Minus,
                    oak_sql::ast::UnaryOperator::Not => SqlBoundUnaryOperator::Not,
                },
                expression: Box::new(bind_expression(expr)?),
            })
        }
        Expression::IsNull { expr, negated, .. } => Ok(SqlBoundExpression::Unary {
            operator: if *negated { SqlBoundUnaryOperator::IsNotNull } else { SqlBoundUnaryOperator::IsNull },
            expression: Box::new(bind_expression(expr)?),
        }),
        Expression::IsTruth { expr, value, negated, .. } => Ok(SqlBoundExpression::Unary {
            operator: SqlBoundUnaryOperator::IsTruth { value: *value, negated: *negated },
            expression: Box::new(bind_expression(expr)?),
        }),
        Expression::Between { expr, low, high, negated, .. } => {
            // 先构造 `expr >= low AND expr <= high`
            let e = bind_expression(expr)?;
            let lo = bind_expression(low)?;
            let hi = bind_expression(high)?;
            let left =
                SqlBoundExpression::Binary { left: Box::new(e.clone()), operator: SqlBoundBinaryOperator::GreaterEqual, right: Box::new(lo) };
            let right = SqlBoundExpression::Binary { left: Box::new(e), operator: SqlBoundBinaryOperator::LessEqual, right: Box::new(hi) };
            let between = SqlBoundExpression::Binary { left: Box::new(left), operator: SqlBoundBinaryOperator::And, right: Box::new(right) };
            if *negated {
                Ok(SqlBoundExpression::Unary { operator: SqlBoundUnaryOperator::Not, expression: Box::new(between) })
            }
            else {
                Ok(between)
            }
        }
        Expression::InList { expr, list, negated, .. } => {
            // Expand IN into `expr = v1 OR expr = v2 OR ... OR expr = vN`
            if list.is_empty() {
                return Err(bind_error("IN list must not be empty"));
            }
            let bound_expr = bind_expression(expr)?;
            let mut or_tree: Option<SqlBoundExpression> = None;
            for item in list {
                let eq = SqlBoundExpression::Binary {
                    left: Box::new(bound_expr.clone()),
                    operator: SqlBoundBinaryOperator::Equal,
                    right: Box::new(bind_expression(item)?),
                };
                or_tree = match or_tree {
                    None => Some(eq),
                    Some(acc) => {
                        Some(SqlBoundExpression::Binary { left: Box::new(acc), operator: SqlBoundBinaryOperator::Or, right: Box::new(eq) })
                    }
                };
            }
            let in_expr = or_tree.unwrap();
            if *negated {
                Ok(SqlBoundExpression::Unary { operator: SqlBoundUnaryOperator::Not, expression: Box::new(in_expr) })
            }
            else {
                Ok(in_expr)
            }
        }
        Expression::FunctionCall { name, args, distinct, window, .. } => {
            // Canonical lower-case name: description inference matches the
            // documented spelling, while execution stays case-insensitive.
            // Diagnostics keep the spelling the query used.
            let display = normalize_identifier(name.name.as_ref());
            let fname = display.to_ascii_lowercase();

            let bound_args = args.iter().map(bind_expression).collect::<Result<Vec<_>, _>>()?;
            let bound_window = window.as_ref().map(bind_window_spec).transpose()?;

            // 窗口函数只存在于窗口上下文，且不接受 DISTINCT；参数形状按函数逐个校验。
            if let Some(function) = SqlWindowFunction::from_name(&fname) {
                let Some(spec) = bound_window.as_ref()
                else {
                    return Err(SqlFrontendError::unsupported(format!("{display}() is a window function and requires an OVER (...) clause")));
                };
                if *distinct {
                    return Err(SqlFrontendError::unsupported(format!("{display}() does not accept the DISTINCT quantifier")));
                }
                if let Some(nested) = bound_args.iter().find_map(find_aggregate_call) {
                    return Err(SqlFrontendError::unsupported(format!(
                        "window function {display}() cannot take the aggregate {nested}() as its argument"
                    )));
                }
                let args = match function {
                    SqlWindowFunction::RowNumber | SqlWindowFunction::Rank | SqlWindowFunction::DenseRank => {
                        if !bound_args.is_empty() {
                            return Err(SqlFrontendError::unsupported(format!("{display}() takes no arguments")));
                        }
                        Vec::new()
                    }
                    SqlWindowFunction::Ntile => {
                        if bound_args.len() != 1 {
                            return Err(SqlFrontendError::unsupported(format!(
                                "{display}() expects exactly 1 argument (bucket count), got {}",
                                bound_args.len()
                            )));
                        }
                        if spec.order_by.is_empty() {
                            return Err(SqlFrontendError::unsupported(format!(
                                "{display}() requires an ORDER BY inside the OVER (...) clause"
                            )));
                        }
                        if spec.frame.is_some() {
                            return Err(SqlFrontendError::unsupported(format!("{display}() does not accept an explicit window frame")));
                        }
                        window_integer_argument(&bound_args[0], &display, "bucket count", 1)?;
                        bound_args
                    }
                    SqlWindowFunction::Lag | SqlWindowFunction::Lead => {
                        if bound_args.is_empty() || bound_args.len() > 3 {
                            return Err(SqlFrontendError::unsupported(format!(
                                "{display}() expects 1 to 3 arguments (value, offset, default), got {}",
                                bound_args.len()
                            )));
                        }
                        if spec.order_by.is_empty() {
                            return Err(SqlFrontendError::unsupported(format!(
                                "{display}() requires an ORDER BY inside the OVER (...) clause"
                            )));
                        }
                        if spec.frame.is_some() {
                            return Err(SqlFrontendError::unsupported(format!("{display}() does not accept an explicit window frame")));
                        }
                        if bound_args.len() >= 2 {
                            window_integer_argument(&bound_args[1], &display, "row offset", 0)?;
                        }
                        bound_args
                    }
                };
                return Ok(SqlBoundExpression::Function { name: fname, args, distinct: false, window: bound_window });
            }

            // 聚合函数与标量内建函数是两类；未知名称在绑定期直接拒绝。
            if SqlAggregateFunction::from_name(&fname).is_some() {
                if bound_args.is_empty() {
                    return Err(SqlFrontendError::unsupported("aggregate functions require an argument"));
                }
                if let Some(nested) = bound_args.iter().find_map(find_aggregate_call) {
                    return Err(SqlFrontendError::unsupported(format!(
                        "aggregate function {display}() cannot take the aggregate {nested}() as its argument"
                    )));
                }
                if *distinct && fname != "count" {
                    return Err(SqlFrontendError::unsupported("only COUNT supports the DISTINCT quantifier"));
                }
                if *distinct && bound_window.is_some() {
                    return Err(SqlFrontendError::unsupported("DISTINCT is not supported in window aggregates"));
                }
                return Ok(SqlBoundExpression::Function { name: fname, args: bound_args, distinct: *distinct, window: bound_window });
            }
            let Some(scalar) = SqlScalarFunction::from_name(&fname)
            else {
                // `OVER (...)` 只挂在已知的窗口/聚合函数上；其余名字既不是标量函数
                // 也没有窗口语义（`FIRST_VALUE` / `NTH_VALUE` / ...）。
                return Err(if bound_window.is_some() {
                    SqlFrontendError::unsupported(format!("SQL window function is not supported by the gateway: {display}"))
                }
                else {
                    SqlFrontendError::new(
                        SqlErrorKind::UnknownFunction,
                        format!(
                            "unknown SQL function: {display} (supported: aggregate COUNT/SUM/AVG/MAX/MIN and scalar VERSION/DATABASE/CURRENT_SCHEMA/NOW/CURRENT_DATE/CURRENT_TIME/CURDATE/CURTIME/LOWER/UPPER/LENGTH/CHAR_LENGTH/SUBSTRING/TRIM/LTRIM/RTRIM/REPLACE/CONCAT/COALESCE/ABS/ROUND/INSTR/STRPOS/LEFT/RIGHT)"
                        ),
                    )
                });
            };
            if bound_window.is_some() {
                return Err(SqlFrontendError::unsupported(format!("SQL scalar function {display}() cannot take an OVER (...) clause")));
            }
            let (minimum, maximum) = scalar.arity();
            let supplied = bound_args.len();
            if supplied < minimum || maximum.is_some_and(|max| supplied > max) {
                return Err(SqlFrontendError::new(
                    SqlErrorKind::UnknownFunction,
                    format!(
                        "{display}() expects {}{} argument(s), got {supplied}",
                        minimum,
                        match maximum {
                            Some(max) if max == minimum => String::new(),
                            Some(max) => format!("..{max}"),
                            None => "+".to_string(),
                        }
                    ),
                ));
            }
            Ok(SqlBoundExpression::Function { name: fname, args: bound_args, distinct: false, window: None })
        }
        Expression::InSubquery { expr, query, negated, .. } => Ok(SqlBoundExpression::InSubquery {
            expression: Box::new(bind_expression(expr)?),
            select: Box::new(bind_select_statement(query)?),
            negated: *negated,
        }),
        Expression::Case { branches, else_branch, .. } => {
            let bound_branches = branches
                .iter()
                .map(|(when, then)| {
                    let w = bind_expression(when)?;
                    let t = bind_expression(then)?;
                    Ok((Box::new(w), Box::new(t)))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let bound_else = match else_branch {
                Some(els) => Some(Box::new(bind_expression(els)?)),
                None => None,
            };
            Ok(SqlBoundExpression::Case { branches: bound_branches, else_branch: bound_else })
        }
        Expression::Cast { expr, target, .. } => {
            Ok(SqlBoundExpression::Cast { expr: Box::new(bind_expression(expr)?), target: target.to_string() })
        }
        // 单独出现的 `(SELECT ...)` 是标量子查询：值语义由 planner 与执行层固定。
        Expression::Subquery { query, .. } => Ok(SqlBoundExpression::ScalarSubquery { select: Box::new(bind_select_statement(query)?) }),
        Expression::Exists { query, negated, .. } => {
            Ok(SqlBoundExpression::Exists { select: Box::new(bind_select_statement(query)?), negated: *negated })
        }
        Expression::Vector { .. } => Err(SqlFrontendError::unsupported("SQL vector literals are outside the gateway binding subset")),
        Expression::Error { message, .. } => Err(bind_error(message)),
    }
}

/// 实参里是否还有聚合调用。SQL 没有第二次分组，聚合套聚合在单行上没有可求值的
/// 语义；空表时执行层还会静默返回 NULL，因此必须在绑定期拒绝而不是留到执行期。
fn find_aggregate_call(expression: &SqlBoundExpression) -> Option<&str> {
    match expression {
        SqlBoundExpression::Function { name, args, .. } => {
            if SqlAggregateFunction::from_name(name).is_some() {
                return Some(name.as_str());
            }
            args.iter().find_map(find_aggregate_call)
        }
        SqlBoundExpression::Binary { left, right, .. } => find_aggregate_call(left).or_else(|| find_aggregate_call(right)),
        SqlBoundExpression::Unary { expression, .. } => find_aggregate_call(expression),
        SqlBoundExpression::Case { branches, else_branch } => branches
            .iter()
            .find_map(|(when, then)| find_aggregate_call(when).or_else(|| find_aggregate_call(then)))
            .or_else(|| else_branch.as_ref().and_then(|expression| find_aggregate_call(expression))),
        SqlBoundExpression::Cast { expr: expression, .. } => find_aggregate_call(expression),
        // 子查询自成一个作用域，它的聚合属于内层 SELECT。
        SqlBoundExpression::Identifier(_)
        | SqlBoundExpression::Literal(_)
        | SqlBoundExpression::Wildcard
        | SqlBoundExpression::InSubquery { .. }
        | SqlBoundExpression::ScalarSubquery { .. }
        | SqlBoundExpression::Exists { .. } => None,
    }
}

/// Binds one Oak literal, keeping integer spellings exact and rejecting numeric
/// literals that no SQL value in this gateway can represent.
fn bind_literal(literal: &Literal) -> Result<SqlBoundLiteral, SqlFrontendError> {
    match literal {
        Literal::Number(text, _) => bind_number_literal(text),
        Literal::String(value, _) => Ok(SqlBoundLiteral::String(value.to_string())),
        Literal::Boolean(value, _) => Ok(SqlBoundLiteral::Boolean(*value)),
        Literal::Null(_) => Ok(SqlBoundLiteral::Null),
    }
}

/// Encodes raw octets as upper-case hexadecimal, the spelling PostgreSQL uses
/// for `bytea` text output and the one MySQL hexadecimal literals accept.
pub fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02X}");
    }
    out
}

/// Whether a `CAST(... AS <target>)` names a binary column type.
///
/// Length modifiers (`BLOB(1024)`) and case do not change the answer. The
/// gateway uses this to decide that a cast carries raw octets instead of text.
pub fn is_binary_type_name(target: &str) -> bool {
    let upper = target.trim().to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or(&upper);
    matches!(base, "BYTEA" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" | "BYTES")
}

/// Rejects the `::<type>` cast operator.
///
/// Oak's SQL surface lexes `::` but does not build a cast node, so the suffix
/// is dropped at bind time. Accepting it would silently change both the
/// projected column name and the declared type, and `'…'::bytea` would be
/// stored as UTF-8 text instead of the raw octets the client meant to send.
/// The gateway refuses every `::` spelling instead of returning wrong metadata
/// or a lossy value. Only code positions are inspected, so `::` inside string
/// literals, quoted identifiers and comments is left alone.
pub fn reject_cast_operator(sql: &str) -> Result<(), SqlFrontendError> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            quote @ (b'\'' | b'"' | b'`') => index = skip_quoted_bytes(bytes, index, quote),
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index < bytes.len() {
                    if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
            }
            b':' if bytes.get(index + 1) == Some(&b':') => {
                let target: String =
                    sql[index + 2..].chars().take_while(|character| character.is_alphanumeric() || *character == '_').collect();
                if is_binary_type_name(&target) {
                    return Err(SqlFrontendError::unsupported(format!(
                        "`::{target}` casts to a binary type are not supported, send the binary wire format or a hexadecimal `CAST(... AS {target})`"
                    )));
                }
                return Err(SqlFrontendError::unsupported(format!(
                    "the `::{target}` cast operator is not supported, write `CAST(<expression> AS {target})`"
                )));
            }
            _ => index += 1,
        }
    }
    Ok(())
}

/// 把 `LEFT(...)` / `RIGHT(...)` 的保留字函数拼写改写成引号标识符。
///
/// Oak 的 SQL 词表把 `LEFT` / `RIGHT` 固定成 JOIN 关键字，函数调用位置根本无法解析。
/// MySQL 与 PostgreSQL 都把这两个词当函数名使用，所以这里只在代码位置、只在这两个词
/// 后面紧跟 `(` 时加一层引号（`` `LEFT` ``），绑定器 `normalize_identifier` 再还原成
/// 同一个函数名。`LEFT JOIN` 等没有 `(` 的用法原样保留，字符串字面量、引号标识符与
/// 注释也一律不动。
/// 仅在代码位重写保留函数拼写，不触碰字符串/注释内容。
pub fn normalize_reserved_function_calls(sql: &str) -> Cow<'_, str> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut index = 0;
    let mut copied = 0;
    let mut changed = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_alphabetic() || byte == b'_' {
            let start = index;
            while index < bytes.len() && (bytes[index].is_ascii_alphanumeric() || matches!(bytes[index], b'_' | b'$')) {
                index += 1;
            }
            let word = &sql[start..index];
            if (word.eq_ignore_ascii_case("LEFT") || word.eq_ignore_ascii_case("RIGHT"))
                && previous_code_byte(bytes, start).is_none_or(|byte| byte != b'.')
                && next_code_byte(bytes, index) == Some(b'(')
            {
                out.push_str(&sql[copied..start]);
                out.push('`');
                out.push_str(word);
                out.push('`');
                copied = index;
                changed = true;
            }
            continue;
        }
        match byte {
            quote @ (b'\'' | b'"' | b'`') => index = skip_quoted_bytes(bytes, index, quote),
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index < bytes.len() {
                    if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
            }
            _ => index += 1,
        }
    }
    if !changed {
        Cow::Borrowed(sql)
    }
    else {
        out.push_str(&sql[copied..]);
        Cow::Owned(out)
    }
}

/// 从 `index` 起跳过空白后的下一个字节（`None` 表示已到结尾）。
fn next_code_byte(bytes: &[u8], mut index: usize) -> Option<u8> {
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    bytes.get(index).copied()
}

/// `start` 之前跳过空白后的最后一个字节（`None` 表示前面没有代码）。
fn previous_code_byte(bytes: &[u8], mut start: usize) -> Option<u8> {
    while start > 0 && bytes[start - 1].is_ascii_whitespace() {
        start -= 1;
    }
    if start == 0 {
        return None;
    }
    bytes.get(start - 1).copied()
}

/// Advances past one quoted run, honouring both doubled quotes and the MySQL
/// backslash escape inside single-quoted strings.
fn skip_quoted_bytes(bytes: &[u8], mut index: usize, quote: u8) -> usize {
    index += 1;
    while index < bytes.len() {
        if bytes[index] == b'\\' && quote == b'\'' {
            index += 2;
            continue;
        }
        if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
                continue;
            }
            return index + 1;
        }
        index += 1;
    }
    index
}

/// Decodes a binary literal spelling into raw octets.
///
/// Both SQL wire dialects spell binary payloads as hexadecimal text: PostgreSQL
/// `bytea` uses the `\x…` form and MySQL uses bare `X'…'` content. The optional
/// `\x`/`0x` prefix is accepted so one helper serves both. An odd number of
/// digits or a non-hexadecimal character is rejected instead of yielding a
/// truncated payload.
pub fn decode_binary_literal(text: &str) -> Result<Vec<u8>, SqlFrontendError> {
    let digits = text
        .strip_prefix("\\x")
        .or_else(|| text.strip_prefix("\\X"))
        .or_else(|| text.strip_prefix("0x"))
        .or_else(|| text.strip_prefix("0X"))
        .unwrap_or(text);
    if digits.len() % 2 != 0 {
        return Err(bind_error("binary literal must contain an even number of hexadecimal digits"));
    }
    let mut out = Vec::with_capacity(digits.len() / 2);
    let mut high: Option<u8> = None;
    for character in digits.chars() {
        let Some(digit) = character.to_digit(16)
        else {
            return Err(bind_error(&format!("binary literal contains a non-hexadecimal character: {character}")));
        };
        match high.take() {
            Some(value) => out.push(value << 4 | digit as u8),
            None => high = Some(digit as u8),
        }
    }
    Ok(out)
}

/// Numeric literal binding: pure integer spellings must fit `i64` exactly,
/// decimal/exponent spellings must be finite `f64`. Out-of-range literals are
/// rejected instead of being silently truncated to a float.
fn bind_number_literal(text: &str) -> Result<SqlBoundLiteral, SqlFrontendError> {
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        return match text.parse::<i64>() {
            Ok(value) => Ok(SqlBoundLiteral::Integer(value)),
            Err(_) => Err(bind_error(&format!("numeric literal out of range: {text}"))),
        };
    }
    match text.parse::<f64>() {
        Ok(value) if value.is_finite() => Ok(SqlBoundLiteral::Number(text.to_string())),
        _ => Err(bind_error(&format!("numeric literal out of range: {text}"))),
    }
}

/// Folds `-<integer literal>` into a negative `i64` when the magnitude fits,
/// including `9223372036854775808` for `i64::MIN`. Non-integer or wider
/// spellings return `None` and keep the unary node.
fn negated_integer_literal(text: &str) -> Option<i64> {
    if text.contains('.') || text.contains('e') || text.contains('E') {
        return None;
    }
    let magnitude = text.parse::<i128>().ok()?;
    i64::try_from(-magnitude).ok()
}

fn bind_binary_operator(operator: BinaryOperator) -> Result<SqlBoundBinaryOperator, SqlFrontendError> {
    Ok(match operator {
        BinaryOperator::Plus => SqlBoundBinaryOperator::Plus,
        BinaryOperator::Minus => SqlBoundBinaryOperator::Minus,
        BinaryOperator::Star => SqlBoundBinaryOperator::Multiply,
        BinaryOperator::Slash => SqlBoundBinaryOperator::Divide,
        BinaryOperator::Percent => SqlBoundBinaryOperator::Modulo,
        BinaryOperator::And => SqlBoundBinaryOperator::And,
        BinaryOperator::Or => SqlBoundBinaryOperator::Or,
        BinaryOperator::Equal => SqlBoundBinaryOperator::Equal,
        BinaryOperator::NotEqual => SqlBoundBinaryOperator::NotEqual,
        BinaryOperator::Less => SqlBoundBinaryOperator::Less,
        BinaryOperator::Greater => SqlBoundBinaryOperator::Greater,
        BinaryOperator::LessEqual => SqlBoundBinaryOperator::LessEqual,
        BinaryOperator::GreaterEqual => SqlBoundBinaryOperator::GreaterEqual,
        BinaryOperator::Like => SqlBoundBinaryOperator::Like,
        // `oak-sql` now keeps `||` in the AST instead of dropping it. The gateway
        // dialects have no portable concat spelling (`||` is logical OR in MySQL,
        // concat in PostgreSQL), so reject it explicitly instead of guessing.
        BinaryOperator::Concat => {
            return Err(SqlFrontendError::unsupported("operator `||` is not supported by the gateway SQL frontend"));
        }
    })
}

fn bind_error(message: &str) -> SqlFrontendError {
    SqlFrontendError::syntax(message)
}

/// 规范化一个标识符：去掉一层引号（`` `x` `` / `"x"` / `[x]`）与前后空白。
///
/// MySQL 客户端的标识符一律用反引号，PostgreSQL 用双引号，Oak 保留引号文本。
/// 网关按去掉引号后的名字解析表、列与函数，因此这里统一剥一层。
/// 去掉标识符外层引号的一层包装。
pub fn normalize_identifier(name: &str) -> String {
    let trimmed = name.trim();
    let unquoted = match trimmed.as_bytes() {
        [first, .., last] if *first == b'`' && *last == b'`' => &trimmed[1..trimmed.len() - 1],
        [first, .., last] if *first == b'"' && *last == b'"' => &trimmed[1..trimmed.len() - 1],
        [first, .., last] if *first == b'[' && *last == b']' => &trimmed[1..trimmed.len() - 1],
        _ => trimmed,
    };
    unquoted.to_string()
}
