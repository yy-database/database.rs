//! Read-only synthesized metadata views.
//!
//! Iris ORM adapters and ordinary drivers introspect `information_schema`,
//! `pg_catalog` and a few session probes (`DATABASE()`, `current_schema()`,
//! `version()`). The registry is the only schema source, so every supported
//! statement is answered from it directly. Anything outside the documented
//! shape is rejected with `Unsupported` instead of approximated.

use oak_sql::ast::{
    BinaryOperator, Expression, JoinType, Literal, OrderDirection, SelectItem, SelectStatement, SqlStatement, TableName, TableSource,
};

use crate::{
    SqlColumn, SqlFieldType, SqlFrontendError, SqlResult, SqlSchemaRegistry, SqlValue,
    sql::{SqlDialect, parse_sql},
    sql_schema::{RegistryState, SchemaField, SchemaTable},
};

/// Default PostgreSQL `server_version` reported by the gateway, also returned by
/// `version()` metadata probes. Hosts may replace it with
/// [`set_pg_server_version`] to present their own identity.
pub const PG_SERVER_VERSION: &str = concat!("database.rs ", env!("CARGO_PKG_VERSION"));

static PG_SERVER_VERSION_OVERRIDE: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Installs the host-supplied `server_version` identity. Later calls are ignored.
pub fn set_pg_server_version(identity: impl Into<String>) {
    let _ = PG_SERVER_VERSION_OVERRIDE.set(identity.into());
}

/// The `server_version` identity the gateway reports.
pub fn pg_server_version() -> &'static str {
    PG_SERVER_VERSION_OVERRIDE.get().map(String::as_str).unwrap_or(PG_SERVER_VERSION)
}

const MYSQL_SHAPES: &str = "accepted MySQL metadata shapes: `SHOW TABLES [LIKE '<pattern>']`, \
`SELECT <columns> FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() [AND TABLE_TYPE = 'BASE TABLE'] \
[ORDER BY TABLE_NAME]`, `SELECT <columns> FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() \
AND TABLE_NAME = <literal> [ORDER BY ORDINAL_POSITION]`, `SELECT DATABASE()`";

const PG_SHAPES: &str = "accepted PostgreSQL metadata shapes: `information_schema.tables`, \
`information_schema.columns` (optionally with the correlated `EXISTS` primary-key expression), \
single-view `information_schema.table_constraints` / `information_schema.key_column_usage`, \
single-view `pg_catalog.pg_class` / `pg_namespace` / `pg_attribute` / `pg_type`, `SELECT current_schema()`, \
`SELECT version()`";

/// One synthesized metadata query.
///
/// The shape is private: callers only get the described result columns and the
/// resolved rows.
pub struct MetaQuery {
    view: View,
    dialect: SqlDialect,
    columns: Vec<MetaColumn>,
    filters: Vec<(ColumnId, FilterValue)>,
    order: Vec<(ColumnId, bool)>,
    limit: Option<usize>,
    offset: Option<usize>,
    like: Option<String>,
    with_pk: bool,
    pk: Option<PkProbe>,
    scalar: Option<ScalarKind>,
    /// `SELECT COUNT(*)` over the metadata view; the result is a single row.
    count: bool,
}

/// Accepted `EXISTS` primary-key probe over `information_schema.columns`.
///
/// Iris correlates `column_name` between `key_column_usage` and the outer row,
/// while other clients correlate `table_schema` / `table_name` on
/// `table_constraints`. Both bounded variants are recorded here so the
/// synthesized `is_pk` column stays faithful to the probe.
struct PkProbe {
    /// `tc.constraint_name = '<literal>'` filter.
    constraint: Option<String>,
    /// `tc.table_schema = '<literal>'` filter inside the subquery.
    schema: Option<String>,
    /// `kcu.column_name = c.column_name`, which makes `is_pk` per column.
    column_correlated: bool,
}

struct MetaColumn {
    id: ColumnId,
    name: String,
    field_type: SqlFieldType,
}

impl MetaQuery {
    /// Result shape, usable by a prepared-statement description.
    pub fn describe(&self) -> Vec<SqlColumn> {
        self.columns
            .iter()
            .map(|column| SqlColumn { name: column.name.clone(), field_type: column.field_type.clone(), field_id: None })
            .collect()
    }

    /// Executes the synthesized query against the registry snapshot.
    pub fn execute(&self, registry: &SqlSchemaRegistry, schema: &str) -> SqlResult {
        if let Some(scalar) = self.scalar {
            let value = match scalar {
                ScalarKind::Database => schema.to_string(),
                ScalarKind::CurrentSchema => "public".to_string(),
                ScalarKind::Version => pg_server_version().to_string(),
            };
            let columns = self.columns.iter().map(|column| self.label(column, schema)).collect();
            return SqlResult { columns, rows: vec![vec![SqlValue::String(value)]], affected_rows: 0 };
        }

        let state = registry.snapshot();
        let mut rows = build_rows(self, &state, schema);
        rows.retain(|row| self.matches(row, schema));
        if self.count {
            let columns = self.columns.iter().map(|column| self.label(column, schema)).collect();
            return SqlResult { columns, rows: vec![vec![SqlValue::Integer(rows.len() as i64)]], affected_rows: 0 };
        }
        if !self.order.is_empty() {
            let nulls_first = self.dialect.nulls_sort_first();
            rows.sort_by(|left, right| {
                for (column, descending) in &self.order {
                    let ordering = compare_values(value_of(left, *column), value_of(right, *column), nulls_first);
                    if ordering != std::cmp::Ordering::Equal {
                        return if *descending { ordering.reverse() } else { ordering };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        if let Some(offset) = self.offset {
            rows.drain(..offset.min(rows.len()));
        }
        if let Some(limit) = self.limit {
            rows.truncate(limit);
        }
        let columns = self.columns.iter().map(|column| self.label(column, schema)).collect();
        let rows = rows.into_iter().map(|row| self.columns.iter().map(|column| value_of(&row, column.id).clone()).collect()).collect();
        SqlResult { columns, rows, affected_rows: 0 }
    }

    fn label(&self, column: &MetaColumn, schema: &str) -> String {
        if column.id == ColumnId::TablesIn {
            return format!("Tables_in_{schema}");
        }
        column.name.clone()
    }

    fn matches(&self, row: &[(ColumnId, SqlValue)], schema: &str) -> bool {
        self.filters.iter().all(|(column, filter)| {
            let actual = value_of(row, *column);
            match filter {
                FilterValue::Value(value) => equal_values(actual, value),
                FilterValue::Pattern(pattern) => match actual {
                    SqlValue::String(text) if self.dialect == SqlDialect::Mysql => {
                        like_matches(&pattern.to_lowercase(), &text.to_lowercase())
                    }
                    SqlValue::String(text) => like_matches(pattern, text),
                    _ => false,
                },
                FilterValue::Database => equal_values(actual, &SqlValue::String(schema.to_string())),
                FilterValue::CurrentSchema => equal_values(actual, &SqlValue::String("public".to_string())),
            }
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ScalarKind {
    Database,
    CurrentSchema,
    Version,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum View {
    ShowTables,
    MysqlTables,
    MysqlColumns,
    PgTables,
    PgColumns,
    PgTableConstraints,
    PgKeyColumnUsage,
    PgClass,
    PgNamespace,
    PgAttribute,
    PgType,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ColumnId {
    TablesIn,
    TableCatalog,
    TableSchema,
    TableName,
    TableType,
    ColumnName,
    OrdinalPosition,
    ColumnDefault,
    IsNullable,
    DataType,
    UdtName,
    CharacterMaxLength,
    ColumnKey,
    NumericPrecision,
    NumericScale,
    ConstraintCatalog,
    ConstraintSchema,
    ConstraintName,
    ConstraintType,
    IsPk,
    Oid,
    RelName,
    RelNamespace,
    RelKind,
    NspName,
    AttRelId,
    AttName,
    AttTypeId,
    AttNum,
    AttNotNull,
    AttTypMod,
    TypeName,
    ScalarValue,
    Count,
}

enum FilterValue {
    Value(SqlValue),
    Pattern(String),
    Database,
    CurrentSchema,
}

struct ViewColumn {
    id: ColumnId,
    mysql: &'static str,
    pg: &'static str,
}

const fn column(id: ColumnId, mysql: &'static str, pg: &'static str) -> ViewColumn {
    ViewColumn { id, mysql, pg }
}

fn view_columns(view: View) -> &'static [ViewColumn] {
    const SHOW: &[ViewColumn] = &[column(ColumnId::TablesIn, "Tables_in_", "")];
    const MYSQL_TABLES: &[ViewColumn] = &[
        column(ColumnId::TableSchema, "TABLE_SCHEMA", ""),
        column(ColumnId::TableName, "TABLE_NAME", ""),
        column(ColumnId::TableType, "TABLE_TYPE", ""),
    ];
    const MYSQL_COLUMNS: &[ViewColumn] = &[
        column(ColumnId::TableSchema, "TABLE_SCHEMA", ""),
        column(ColumnId::TableName, "TABLE_NAME", ""),
        column(ColumnId::ColumnName, "COLUMN_NAME", ""),
        column(ColumnId::OrdinalPosition, "ORDINAL_POSITION", ""),
        column(ColumnId::ColumnDefault, "COLUMN_DEFAULT", ""),
        column(ColumnId::IsNullable, "IS_NULLABLE", ""),
        column(ColumnId::DataType, "DATA_TYPE", ""),
        column(ColumnId::CharacterMaxLength, "CHARACTER_MAXIMUM_LENGTH", ""),
        column(ColumnId::ColumnKey, "COLUMN_KEY", ""),
    ];
    const PG_TABLES: &[ViewColumn] = &[
        column(ColumnId::TableCatalog, "", "table_catalog"),
        column(ColumnId::TableSchema, "", "table_schema"),
        column(ColumnId::TableName, "", "table_name"),
        column(ColumnId::TableType, "", "table_type"),
    ];
    const PG_COLUMNS: &[ViewColumn] = &[
        column(ColumnId::TableCatalog, "", "table_catalog"),
        column(ColumnId::TableSchema, "", "table_schema"),
        column(ColumnId::TableName, "", "table_name"),
        column(ColumnId::ColumnName, "", "column_name"),
        column(ColumnId::OrdinalPosition, "", "ordinal_position"),
        column(ColumnId::ColumnDefault, "", "column_default"),
        column(ColumnId::IsNullable, "", "is_nullable"),
        column(ColumnId::DataType, "", "data_type"),
        column(ColumnId::UdtName, "", "udt_name"),
        column(ColumnId::CharacterMaxLength, "", "character_maximum_length"),
        column(ColumnId::NumericPrecision, "", "numeric_precision"),
        column(ColumnId::NumericScale, "", "numeric_scale"),
    ];
    const PG_CONSTRAINTS: &[ViewColumn] = &[
        column(ColumnId::ConstraintCatalog, "", "constraint_catalog"),
        column(ColumnId::ConstraintSchema, "", "constraint_schema"),
        column(ColumnId::ConstraintName, "", "constraint_name"),
        column(ColumnId::TableCatalog, "", "table_catalog"),
        column(ColumnId::TableSchema, "", "table_schema"),
        column(ColumnId::TableName, "", "table_name"),
        column(ColumnId::ConstraintType, "", "constraint_type"),
    ];
    const PG_KEY_COLUMN_USAGE: &[ViewColumn] = &[
        column(ColumnId::ConstraintCatalog, "", "constraint_catalog"),
        column(ColumnId::ConstraintSchema, "", "constraint_schema"),
        column(ColumnId::ConstraintName, "", "constraint_name"),
        column(ColumnId::TableCatalog, "", "table_catalog"),
        column(ColumnId::TableSchema, "", "table_schema"),
        column(ColumnId::TableName, "", "table_name"),
        column(ColumnId::ColumnName, "", "column_name"),
        column(ColumnId::OrdinalPosition, "", "ordinal_position"),
    ];
    const PG_CLASS: &[ViewColumn] = &[
        column(ColumnId::Oid, "", "oid"),
        column(ColumnId::RelName, "", "relname"),
        column(ColumnId::RelNamespace, "", "relnamespace"),
        column(ColumnId::RelKind, "", "relkind"),
    ];
    const PG_NAMESPACE: &[ViewColumn] = &[column(ColumnId::Oid, "", "oid"), column(ColumnId::NspName, "", "nspname")];
    const PG_ATTRIBUTE: &[ViewColumn] = &[
        column(ColumnId::AttRelId, "", "attrelid"),
        column(ColumnId::AttName, "", "attname"),
        column(ColumnId::AttTypeId, "", "atttypid"),
        column(ColumnId::AttNum, "", "attnum"),
        column(ColumnId::AttNotNull, "", "attnotnull"),
        column(ColumnId::AttTypMod, "", "atttypmod"),
    ];
    const PG_TYPE: &[ViewColumn] = &[column(ColumnId::Oid, "", "oid"), column(ColumnId::TypeName, "", "typname")];
    match view {
        View::ShowTables => SHOW,
        View::MysqlTables => MYSQL_TABLES,
        View::MysqlColumns => MYSQL_COLUMNS,
        View::PgTables => PG_TABLES,
        View::PgColumns => PG_COLUMNS,
        View::PgTableConstraints => PG_CONSTRAINTS,
        View::PgKeyColumnUsage => PG_KEY_COLUMN_USAGE,
        View::PgClass => PG_CLASS,
        View::PgNamespace => PG_NAMESPACE,
        View::PgAttribute => PG_ATTRIBUTE,
        View::PgType => PG_TYPE,
    }
}

fn view_column_name(view: View, dialect: SqlDialect, id: ColumnId) -> Option<&'static str> {
    view_columns(view).iter().find(|column| column.id == id).map(|column| match dialect {
        SqlDialect::Mysql => column.mysql,
        SqlDialect::Postgres => column.pg,
    })
}

fn field_type(id: ColumnId) -> SqlFieldType {
    let base = match id {
        ColumnId::Oid | ColumnId::RelNamespace | ColumnId::AttRelId | ColumnId::AttTypeId => SqlFieldType::Int64,
        ColumnId::OrdinalPosition
        | ColumnId::CharacterMaxLength
        | ColumnId::NumericPrecision
        | ColumnId::NumericScale
        | ColumnId::AttNum
        | ColumnId::AttTypMod => SqlFieldType::Int32,
        ColumnId::IsPk | ColumnId::AttNotNull => SqlFieldType::Bool,
        _ => SqlFieldType::String,
    };
    if matches!(id, ColumnId::ColumnDefault | ColumnId::CharacterMaxLength | ColumnId::NumericPrecision | ColumnId::NumericScale) {
        SqlFieldType::Optional(Box::new(base))
    }
    else {
        base
    }
}

/// Analyzes a statement and returns the metadata query it describes.
///
/// Returns `Ok(None)` for statements that touch no metadata surface; those keep
/// the ordinary catalog/executor path.
pub fn analyze_meta_query(sql: &str, dialect: SqlDialect) -> Result<Option<MetaQuery>, SqlFrontendError> {
    if !mentions_metadata(sql) {
        return Ok(None);
    }
    if let Some(scalar) = scalar_probe(sql, dialect) {
        return Ok(Some(scalar));
    }
    // Oak stops at a dotted table reference, so the qualified metadata names are
    // folded into single identifiers of the same byte length before parsing.
    let normalised = normalize_metadata_tables(sql);
    // `DATABASE` is a lexer keyword in Oak, so it cannot be used as a function
    // name in the AST. Rewriting the token keeps Oak as the only SQL parser.
    let normalized = normalize_database_calls(&normalised);
    let root = match parse_sql(&normalized, dialect) {
        Ok(root) => root,
        Err(error) => return Err(error),
    };
    if root.statements.len() != 1 {
        return Err(unsupported(format!("expected exactly one statement, found {}", root.statements.len())));
    }
    match &root.statements[0] {
        SqlStatement::Show(show) => analyze_show(show, dialect),
        SqlStatement::Select(select) => analyze_select(select, dialect, &normalized),
        other => Err(unsupported(format!("`{}` does not read metadata; {}", statement_kind(other), shapes(dialect)))),
    }
}

fn shapes(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Mysql => MYSQL_SHAPES,
        SqlDialect::Postgres => PG_SHAPES,
    }
}

fn unsupported(detail: String) -> SqlFrontendError {
    SqlFrontendError::unsupported(detail)
}

const METADATA_TABLES: &[(&str, &str)] = &[
    ("information_schema.tables", "information_schema_tables"),
    ("information_schema.columns", "information_schema_columns"),
    ("information_schema.table_constraints", "information_schema_table_constraints"),
    ("information_schema.key_column_usage", "information_schema_key_column_usage"),
    ("pg_catalog.pg_class", "pg_catalog_pg_class"),
    ("pg_catalog.pg_namespace", "pg_catalog_pg_namespace"),
    ("pg_catalog.pg_attribute", "pg_catalog_pg_attribute"),
    ("pg_catalog.pg_type", "pg_catalog_pg_type"),
];

fn normalize_metadata_tables(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    let bytes = sql.as_bytes();
    let mut result = String::with_capacity(sql.len());
    let mut index = 0;
    'scan: while index < lower.len() {
        for (dotted, folded) in METADATA_TABLES {
            if !lower[index..].starts_with(*dotted) {
                continue;
            }
            let end = index + dotted.len();
            let after_dotted = end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || matches!(bytes[end], b'_' | b'.'));
            let before_dotted = index > 0 && (bytes[index - 1].is_ascii_alphanumeric() || matches!(bytes[index - 1], b'_' | b'.'));
            if before_dotted || after_dotted {
                continue;
            }
            result.push_str(folded);
            index = end;
            continue 'scan;
        }
        let character = sql[index..].chars().next().unwrap_or(' ');
        result.push(character);
        index += character.len_utf8();
    }
    result
}

const DATABASE_SENTINEL: &str = "__yyds_metadata_database__";

/// `DATABASE` is a lexer keyword in Oak and a function call on the right of
/// `=` is dropped by the parser, so the call is replaced with a string literal
/// sentinel that [`filter_value`] maps back to the session schema.
fn normalize_database_calls(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    let mut result = String::with_capacity(sql.len());
    let mut index = 0;
    while let Some(found) = lower[index..].find("database") {
        let start = index + found;
        let before_ok = start == 0 || !sql.as_bytes()[start - 1].is_ascii_alphanumeric() && sql.as_bytes()[start - 1] != b'_';
        result.push_str(&sql[index..start]);
        if !before_ok {
            result.push_str(&sql[start..start + "database".len()]);
            index = start + "database".len();
            continue;
        }
        let mut cursor = start + "database".len();
        while cursor < sql.len() && sql.as_bytes()[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if !sql[cursor..].starts_with('(') {
            result.push_str(&sql[start..cursor]);
            index = cursor;
            continue;
        }
        cursor += 1;
        while cursor < sql.len() && sql.as_bytes()[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if !sql[cursor..].starts_with(')') {
            result.push_str(&sql[start..cursor]);
            index = cursor;
            continue;
        }
        result.push_str(&format!("'{DATABASE_SENTINEL}'"));
        index = cursor + 1;
    }
    result.push_str(&sql[index..]);
    result
}

fn mentions_metadata(sql: &str) -> bool {
    let words = sql.split_whitespace().collect::<Vec<_>>().join(" ").to_ascii_lowercase();
    words.contains("information_schema")
        || words.contains("pg_catalog")
        || words.contains("show tables")
        || words.contains("show databases")
        || words.contains("database(")
        || words.contains("current_database(")
        || words.contains("current_schema(")
        || words.contains("version(")
}

fn scalar_probe(sql: &str, dialect: SqlDialect) -> Option<MetaQuery> {
    let text = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
    let (kind, label) = match text.as_str() {
        "select database()" => (ScalarKind::Database, "DATABASE()".to_string()),
        "select current_schema()" if dialect == SqlDialect::Postgres => (ScalarKind::CurrentSchema, "current_schema()".to_string()),
        "select schema()" if dialect == SqlDialect::Postgres => (ScalarKind::CurrentSchema, "schema()".to_string()),
        "select version()" | "select pg_catalog.version()" => (ScalarKind::Version, "version()".to_string()),
        _ => return None,
    };
    Some(scalar_query(dialect, kind, label))
}

fn scalar_query(dialect: SqlDialect, kind: ScalarKind, label: String) -> MetaQuery {
    MetaQuery {
        view: View::ShowTables,
        dialect,
        columns: vec![MetaColumn { id: ColumnId::ScalarValue, name: label, field_type: SqlFieldType::String }],
        filters: Vec::new(),
        order: Vec::new(),
        limit: None,
        offset: None,
        like: None,
        with_pk: false,
        pk: None,
        scalar: Some(kind),
        count: false,
    }
}

fn statement_kind(statement: &SqlStatement) -> &'static str {
    match statement {
        SqlStatement::Select(_) | SqlStatement::SetOperation(_) => "SELECT",
        SqlStatement::Insert(_) => "INSERT",
        SqlStatement::Update(_) => "UPDATE",
        SqlStatement::Delete(_) => "DELETE",
        SqlStatement::Create(_) => "CREATE",
        SqlStatement::Drop(_) => "DROP",
        SqlStatement::Alter(_) => "ALTER",
        _ => "statement",
    }
}

fn analyze_show(show: &oak_sql::ast::ShowStatement, dialect: SqlDialect) -> Result<Option<MetaQuery>, SqlFrontendError> {
    let target = show.target.to_ascii_uppercase();
    match target.as_str() {
        "TABLES" if dialect == SqlDialect::Mysql => Ok(Some(MetaQuery {
            view: View::ShowTables,
            dialect,
            columns: vec![MetaColumn {
                id: ColumnId::TablesIn,
                name: view_column_name(View::ShowTables, dialect, ColumnId::TablesIn).unwrap_or("Tables_in_").to_string(),
                field_type: SqlFieldType::String,
            }],
            filters: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: None,
            like: show.like.as_ref().map(|pattern| pattern.to_string()),
            with_pk: false,
            pk: None,
            scalar: None,
            count: false,
        })),
        "TABLES" => {
            Err(unsupported(format!("`SHOW TABLES` is MySQL-only and the PostgreSQL gateway has no such statement; {}", shapes(dialect))))
        }
        other => Err(unsupported(format!("`SHOW {other}` is not a supported metadata view; {}", shapes(dialect)))),
    }
}

fn analyze_select(select: &SelectStatement, dialect: SqlDialect, sql: &str) -> Result<Option<MetaQuery>, SqlFrontendError> {
    if select.with.is_some() {
        return Err(unsupported(format!("`WITH` around a metadata query is not supported; {}", shapes(dialect))));
    }
    if select.quantifier.is_some() {
        return Err(unsupported(format!("`SELECT DISTINCT`/`ALL` is not supported for metadata; {}", shapes(dialect))));
    }
    if select.group_by.is_some() || select.having.is_some() {
        return Err(unsupported(format!("`GROUP BY` / `HAVING` over metadata is not supported; {}", shapes(dialect))));
    }
    if !select.joins.is_empty() {
        return Err(unsupported(format!("`JOIN` outside the accepted primary-key `EXISTS` shape is not supported; {}", shapes(dialect))));
    }
    let (limit, offset) = parse_limit(select, dialect)?;
    let Some(table) = from_table(select, sql)?
    else {
        return analyze_scalar_select(select, dialect);
    };
    let view = match table.view {
        Some(view) if dialect == SqlDialect::Mysql => match view {
            View::PgTables => View::MysqlTables,
            View::PgColumns => View::MysqlColumns,
            _ => {
                return Err(unsupported(format!("`{}` is not a MySQL metadata view; {}", display_metadata_name(&table.name), shapes(dialect))));
            }
        },
        Some(view) if dialect == SqlDialect::Postgres && matches!(view, View::MysqlTables | View::MysqlColumns) => {
            return Err(unsupported(format!(
                "`{}` is not a PostgreSQL metadata view; {}",
                display_metadata_name(&table.name),
                shapes(dialect)
            )));
        }
        Some(view) => view,
        None => {
            return Err(unsupported(format!("`{}` is not a supported metadata view; {}", display_metadata_name(&table.name), shapes(dialect))));
        }
    };

    let (projection_ids, count) = project_items(select, view, table.alias.as_deref(), dialect)?;
    let filters = parse_filters(select, view, table.alias.as_deref(), dialect)?;
    let order = parse_order(select, view, table.alias.as_deref(), dialect)?;
    let with_pk = projection_ids.iter().any(|(id, _)| *id == ColumnId::IsPk);
    if with_pk && view != View::PgColumns {
        return Err(unsupported(format!("primary-key `EXISTS` is only supported on `information_schema.columns`; {}", shapes(dialect))));
    }
    let pk = if with_pk { Some(validate_pk_exists(select, table.alias.as_deref(), dialect, sql)?) } else { None };
    let columns = if count {
        let name = projection_ids.first().map(|(_, name)| name.clone()).unwrap_or_else(|| "count".to_string());
        vec![MetaColumn { id: ColumnId::Count, name, field_type: SqlFieldType::Int64 }]
    } else {
        projection_ids.into_iter().map(|(id, name)| MetaColumn { id, name, field_type: field_type(id) }).collect()
    };
    Ok(Some(MetaQuery { view, dialect, columns, filters, order, limit, offset, like: None, with_pk, pk, scalar: None, count }))
}

fn analyze_scalar_select(select: &SelectStatement, dialect: SqlDialect) -> Result<Option<MetaQuery>, SqlFrontendError> {
    if select.items.len() != 1 {
        return Err(unsupported(format!("scalar metadata probes accept exactly one projection; {}", shapes(dialect))));
    }
    let SelectItem::Expression { expr, alias, .. } = &select.items[0]
    else {
        return Err(unsupported(format!("`SELECT *` is not a supported metadata view; {}", shapes(dialect))));
    };
    let Expression::FunctionCall { name, args, distinct, window, .. } = expr
    else {
        return Err(unsupported(format!(
            "scalar metadata probes accept only `DATABASE()`, `current_schema()`, `version()`; {}",
            shapes(dialect)
        )));
    };
    if !args.is_empty() || *distinct || window.is_some() {
        return Err(unsupported(format!("metadata probe `{}()` takes no arguments; {}", name.name, shapes(dialect))));
    }
    let function = name.name.rsplit('.').next().unwrap_or(name.name.as_ref()).to_ascii_lowercase();
    let kind = match function.as_str() {
        "database" | "current_database" if dialect == SqlDialect::Mysql => ScalarKind::Database,
        "current_schema" | "schema" if dialect == SqlDialect::Postgres => ScalarKind::CurrentSchema,
        "version" => ScalarKind::Version,
        other => return Err(unsupported(format!("function `{other}()` is not a metadata probe; {}", shapes(dialect)))),
    };
    let label = alias.as_ref().map(|alias| alias.name.to_string()).unwrap_or_else(|| format!("{}()", name.name));
    Ok(Some(scalar_query(dialect, kind, label)))
}

struct SourceTable {
    name: String,
    view: Option<View>,
    alias: Option<String>,
}

fn from_table(select: &SelectStatement, sql: &str) -> Result<Option<SourceTable>, SqlFrontendError> {
    let Some(from) = &select.from
    else {
        return Ok(None);
    };
    let TableSource::Table(table) = from
    else {
        return Err(unsupported("derived tables are not metadata views".to_string()));
    };
    Ok(Some(source_table(table, sql)))
}

fn source_table(table: &TableName, sql: &str) -> SourceTable {
    // Oak keeps only the first segment of a dotted table reference in `name`,
    // so the qualified path and the alias are recovered from the source text
    // at the table span.
    let (name, alias) = table_reference(sql, table.span.start);
    let view = view_of(&name);
    SourceTable { name, view, alias }
}

fn table_reference(sql: &str, start: usize) -> (String, Option<String>) {
    let bytes = sql.as_bytes();
    let mut end = start;
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || matches!(bytes[end], b'_' | b'.' | b'$')) {
        end += 1;
    }
    let path = sql[start..end].to_string();
    let rest = sql[end..].trim_start();
    let word = rest.split(|character: char| character.is_whitespace() || matches!(character, ',' | ';' | ')' | '(')).next().unwrap_or("");
    let lower = word.to_ascii_lowercase();
    if word.is_empty() || is_clause_keyword(&lower) {
        return (path, None);
    }
    if lower == "as" {
        let after_as = rest[word.len()..].trim_start();
        let alias =
            after_as.split(|character: char| character.is_whitespace() || matches!(character, ',' | ';' | ')' | '(')).next().unwrap_or("");
        return (path, if alias.is_empty() { None } else { Some(alias.to_string()) });
    }
    (path, Some(word.to_string()))
}

fn is_clause_keyword(word: &str) -> bool {
    matches!(
        word,
        "where"
            | "join"
            | "inner"
            | "left"
            | "right"
            | "outer"
            | "full"
            | "cross"
            | "natural"
            | "on"
            | "using"
            | "group"
            | "order"
            | "limit"
            | "having"
            | "union"
            | "offset"
            | "select"
            | "from"
    )
}

fn view_of(name: &str) -> Option<View> {
    let folded = name.to_ascii_lowercase();
    let (schema, object) = if let Some((schema, object)) = folded.split_once('.') {
        (schema.to_string(), object.to_string())
    }
    else if let Some(object) = folded.strip_prefix("information_schema_") {
        ("information_schema".to_string(), object.to_string())
    }
    else if let Some(object) = folded.strip_prefix("pg_catalog_") {
        ("pg_catalog".to_string(), object.to_string())
    }
    else {
        return None;
    };
    match (schema.as_str(), object.as_str()) {
        ("information_schema", "tables") => Some(View::PgTables),
        ("information_schema", "columns") => Some(View::PgColumns),
        ("information_schema", "table_constraints") => Some(View::PgTableConstraints),
        ("information_schema", "key_column_usage") => Some(View::PgKeyColumnUsage),
        ("pg_catalog", "pg_class") => Some(View::PgClass),
        ("pg_catalog", "pg_namespace") => Some(View::PgNamespace),
        ("pg_catalog", "pg_attribute") => Some(View::PgAttribute),
        ("pg_catalog", "pg_type") => Some(View::PgType),
        _ => None,
    }
}

fn display_metadata_name(name: &str) -> String {
    for (dotted, folded) in METADATA_TABLES {
        if name.eq_ignore_ascii_case(folded) {
            return dotted.to_string();
        }
    }
    name.to_string()
}

fn project_items(
    select: &SelectStatement,
    view: View,
    alias: Option<&str>,
    dialect: SqlDialect,
) -> Result<(Vec<(ColumnId, String)>, bool), SqlFrontendError> {
    let mut columns = Vec::with_capacity(select.items.len());
    let mut count = false;
    for item in &select.items {
        let SelectItem::Expression { expr, alias: item_alias, .. } = item
        else {
            return Err(unsupported(format!("`SELECT *` over metadata is not supported; {}", shapes(dialect))));
        };
        match expr {
            Expression::Identifier(identifier) => {
                let (id, written) = resolve_identifier(&identifier.name, view, alias, dialect)?;
                let label = item_alias.as_ref().map(|alias| alias.name.to_string()).unwrap_or_else(|| label_of(&written, dialect));
                columns.push((id, label));
            }
            Expression::Exists { negated, .. } if view == View::PgColumns => {
                if *negated {
                    return Err(unsupported(format!("`NOT EXISTS` is not the accepted primary-key probe; {}", shapes(dialect))));
                }
                let label = item_alias.as_ref().map(|alias| alias.name.to_string()).unwrap_or_else(|| "is_pk".to_string());
                columns.push((ColumnId::IsPk, label));
            }
            Expression::FunctionCall { name, args, distinct, window, .. }
                if !*distinct
                    && window.is_none()
                    && args.len() == 1
                    && matches!(args[0], Expression::Wildcard { .. })
                    && name.name.rsplit('.').next().unwrap_or(name.name.as_ref()).eq_ignore_ascii_case("count") =>
            {
                if select.items.len() > 1 || count {
                    return Err(unsupported(format!("`COUNT(*)` must be the only metadata projection; {}", shapes(dialect))));
                }
                count = true;
                let label = item_alias.as_ref().map(|alias| alias.name.to_string()).unwrap_or_else(|| "COUNT(*)".to_string());
                columns.push((ColumnId::Count, label));
            }
            _ => return Err(unsupported(format!("`{}` is not a supported metadata projection; {}", expression_text(expr), shapes(dialect)))),
        }
    }
    if columns.is_empty() {
        return Err(unsupported(format!("metadata projection must not be empty; {}", shapes(dialect))));
    }
    Ok((columns, count))
}

fn resolve_identifier(name: &str, view: View, alias: Option<&str>, dialect: SqlDialect) -> Result<(ColumnId, String), SqlFrontendError> {
    let parts = name.split('.').collect::<Vec<_>>();
    let written = match parts.as_slice() {
        [single] => *single,
        [qualifier, column] => {
            let qualifier = qualifier.to_ascii_lowercase();
            if alias.is_some_and(|alias| alias.eq_ignore_ascii_case(&qualifier)) {
                *column
            }
            else {
                return Err(unsupported(format!(
                    "column qualifier `{qualifier}` does not match table alias `{}`; {}",
                    alias.unwrap_or(""),
                    shapes(dialect)
                )));
            }
        }
        _ => {
            return Err(unsupported(format!("nested column qualifier in `{name}` is not supported; {}", shapes(dialect))));
        }
    };
    let id = view_columns(view)
        .iter()
        .find(|column| match dialect {
            SqlDialect::Mysql => column.mysql.eq_ignore_ascii_case(written),
            SqlDialect::Postgres => column.pg.eq_ignore_ascii_case(written),
        })
        .map(|column| column.id)
        .ok_or_else(|| unsupported(format!("column `{written}` is not a supported metadata column; {}", shapes(dialect))))?;
    Ok((id, written.to_string()))
}

fn label_of(written: &str, dialect: SqlDialect) -> String {
    match dialect {
        SqlDialect::Mysql => written.to_string(),
        SqlDialect::Postgres => written.to_ascii_lowercase(),
    }
}

fn expression_text(expr: &Expression) -> String {
    match expr {
        Expression::Identifier(identifier) => identifier.name.to_string(),
        Expression::FunctionCall { name, .. } => format!("{}()", name.name),
        Expression::Literal(Literal::Number(value, _)) => value.to_string(),
        Expression::Literal(Literal::String(value, _)) => format!("'{value}'"),
        Expression::Literal(Literal::Boolean(value, _)) => value.to_string(),
        Expression::Literal(Literal::Null(_)) => "NULL".to_string(),
        Expression::Wildcard { .. } => "*".to_string(),
        Expression::Exists { .. } => "EXISTS(...)".to_string(),
        _ => "expression".to_string(),
    }
}

fn parse_filters(
    select: &SelectStatement,
    view: View,
    alias: Option<&str>,
    dialect: SqlDialect,
) -> Result<Vec<(ColumnId, FilterValue)>, SqlFrontendError> {
    let mut filters = Vec::new();
    if let Some(expr) = &select.expr {
        for conjunct in conjuncts(expr) {
            let Expression::Binary { left, op, right, .. } = conjunct
            else {
                return Err(unsupported(format!("metadata filters accept only `=` or `LIKE` comparisons joined with `AND`; {}", shapes(dialect))));
            };
            let like = match op {
                BinaryOperator::Equal => false,
                BinaryOperator::Like => true,
                _ => {
                    return Err(unsupported(format!(
                        "metadata filters accept only `=` or `LIKE`, found `{}`; {}",
                        expression_text(conjunct),
                        shapes(dialect)
                    )));
                }
            };
            let Expression::Identifier(identifier) = left.as_ref()
            else {
                return Err(unsupported(format!("metadata filter left side must be a column; {}", shapes(dialect))));
            };
            let (id, _) = resolve_identifier(&identifier.name, view, alias, dialect)?;
            let value = if like {
                match right.as_ref() {
                    Expression::Literal(Literal::String(value, _)) => FilterValue::Pattern(value.to_string()),
                    _ => {
                        return Err(unsupported(format!(
                            "`LIKE` accepts only a string literal pattern; {}",
                            shapes(dialect)
                        )));
                    }
                }
            }
            else {
                filter_value(right, dialect)?
            };
            filters.push((id, value));
        }
    }
    Ok(filters)
}

fn conjuncts<'a>(expr: &'a Expression) -> Vec<&'a Expression> {
    match expr {
        Expression::Binary { left, op: BinaryOperator::And, right, .. } => {
            let mut items = conjuncts(left);
            items.extend(conjuncts(right));
            items
        }
        other => vec![other],
    }
}

fn filter_value(expr: &Expression, dialect: SqlDialect) -> Result<FilterValue, SqlFrontendError> {
    match expr {
        Expression::Literal(Literal::String(value, _)) if value.as_ref() == DATABASE_SENTINEL => Ok(FilterValue::Database),
        Expression::Literal(literal) => Ok(FilterValue::Value(literal_value(literal))),
        Expression::FunctionCall { name, args, .. } if args.is_empty() => {
            match name.name.rsplit('.').next().unwrap_or(name.name.as_ref()).to_ascii_lowercase().as_str() {
                "current_schema" | "schema" if dialect == SqlDialect::Postgres => Ok(FilterValue::CurrentSchema),
                _ => Err(unsupported(format!("filter function `{}()` is not supported; {}", name.name, shapes(dialect)))),
            }
        }
        _ => Err(unsupported(format!("metadata filter values accept only literals or `DATABASE()`; {}", shapes(dialect)))),
    }
}

fn literal_value(literal: &Literal) -> SqlValue {
    match literal {
        Literal::Number(value, _) => match value.parse::<i64>() {
            Ok(integer) => SqlValue::Integer(integer),
            Err(_) => SqlValue::Number(value.parse().unwrap_or(0.0)),
        },
        Literal::String(value, _) => SqlValue::String(value.to_string()),
        Literal::Boolean(value, _) => SqlValue::Boolean(*value),
        Literal::Null(_) => SqlValue::Null,
    }
}

fn parse_order(
    select: &SelectStatement,
    view: View,
    alias: Option<&str>,
    dialect: SqlDialect,
) -> Result<Vec<(ColumnId, bool)>, SqlFrontendError> {
    let Some(order) = &select.order_by
    else {
        return Ok(Vec::new());
    };
    let mut items = Vec::with_capacity(order.items.len());
    for item in &order.items {
        if item.nulls.is_some() {
            return Err(unsupported(format!("`NULLS FIRST`/`NULLS LAST` is not supported; {}", shapes(dialect))));
        }
        let Expression::Identifier(identifier) = &item.expr
        else {
            return Err(unsupported(format!("`ORDER BY` accepts only metadata columns; {}", shapes(dialect))));
        };
        let (id, _) = resolve_identifier(&identifier.name, view, alias, dialect)?;
        items.push((id, item.direction == OrderDirection::Desc));
    }
    Ok(items)
}

/// Reads `LIMIT` / `OFFSET` for a metadata view.
///
/// Paging is applied after filtering and ordering, so both clauses are kept as
/// plain row counts. PostgreSQL also accepts `OFFSET` without a `LIMIT`, which
/// a row slice handles just as well as the common pair.
fn parse_limit(select: &SelectStatement, dialect: SqlDialect) -> Result<(Option<usize>, Option<usize>), SqlFrontendError> {
    let Some(limit) = &select.limit
    else {
        return Ok((None, None));
    };
    let rows = match &limit.limit {
        Some(Expression::Literal(Literal::Number(text, _))) => {
            Some(text.parse::<usize>().map_err(|_| unsupported(format!("`LIMIT {text}` is not a non-negative integer; {}", shapes(dialect))))?)
        }
        Some(_) => return Err(unsupported(format!("`LIMIT` accepts only a non-negative integer; {}", shapes(dialect)))),
        None => None,
    };
    let offset = match &limit.offset {
        Some(Expression::Literal(Literal::Number(text, _))) => Some(
            text.parse::<usize>().map_err(|_| unsupported(format!("`OFFSET {text}` is not a non-negative integer; {}", shapes(dialect))))?,
        ),
        Some(_) => return Err(unsupported(format!("`OFFSET` accepts only a non-negative integer; {}", shapes(dialect)))),
        None => None,
    };
    Ok((rows, offset))
}

/// Accepted shapes:
/// - `tc.table_schema = 'public'`, `tc.constraint_type = 'PRIMARY KEY'`, `tc.table_name = c.table_name`, and `kcu.column_name = c.column_name` (Iris verbatim form).
/// - `tc.constraint_name = kcu.constraint_name`, `tc.table_schema = kcu.table_schema`, `tc.constraint_type = 'PRIMARY KEY'`, and `table_schema` / `table_name` correlated with the outer row.
fn validate_pk_exists(outer: &SelectStatement, outer_alias: Option<&str>, dialect: SqlDialect, sql: &str) -> Result<PkProbe, SqlFrontendError> {
    let Some(query) = outer.items.iter().find_map(|item| match item {
        SelectItem::Expression { expr: Expression::Exists { query, negated, .. }, .. } if !*negated => Some(query.as_ref()),
        _ => None,
    })
    else {
        let negated =
            outer.items.iter().any(|item| matches!(item, SelectItem::Expression { expr: Expression::Exists { negated: true, .. }, .. }));
        return Err(if negated {
            unsupported(format!("`NOT EXISTS` is not the accepted primary-key probe; {}", shapes(dialect)))
        }
        else {
            unsupported(format!("primary-key probe must be `EXISTS (...)`; {}", shapes(dialect)))
        });
    };
    let Some(TableSource::Table(constraints)) = &query.from
    else {
        return Err(unsupported(format!("primary-key probe must read `information_schema.table_constraints`; {}", shapes(dialect))));
    };
    let (constraints_path, constraints_alias) = table_reference(sql, constraints.span.start);
    if view_of(&constraints_path) != Some(View::PgTableConstraints) {
        return Err(unsupported(format!("primary-key probe must read `information_schema.table_constraints`; {}", shapes(dialect))));
    }
    let Some(tc) = constraints_alias.map(|alias| alias.to_ascii_lowercase())
    else {
        return Err(unsupported(format!("primary-key probe requires a `table_constraints` alias; {}", shapes(dialect))));
    };
    if query.joins.len() != 1 {
        return Err(unsupported(format!(
            "primary-key probe must join `information_schema.key_column_usage` exactly once; {}",
            shapes(dialect)
        )));
    }
    let join = &query.joins[0];
    if join.join_type != JoinType::Inner {
        return Err(unsupported(format!("primary-key probe accepts only `JOIN`; {}", shapes(dialect))));
    }
    let TableSource::Table(usage) = &join.table
    else {
        return Err(unsupported(format!("primary-key probe must join `information_schema.key_column_usage`; {}", shapes(dialect))));
    };
    let (usage_path, usage_alias) = table_reference(sql, usage.span.start);
    if view_of(&usage_path) != Some(View::PgKeyColumnUsage) {
        return Err(unsupported(format!("primary-key probe must join `information_schema.key_column_usage`; {}", shapes(dialect))));
    }
    let Some(kcu) = usage_alias.map(|alias| alias.to_ascii_lowercase())
    else {
        return Err(unsupported(format!("primary-key probe requires a `key_column_usage` alias; {}", shapes(dialect))));
    };
    let selects_one = query.items.len() == 1
        && match &query.items[0] {
            SelectItem::Star { .. } => true,
            SelectItem::Expression { expr: Expression::Literal(Literal::Number(value, _)), .. } => value.as_ref() == "1",
            _ => false,
        };
    if !selects_one {
        return Err(unsupported(format!("primary-key `EXISTS` must select `1`; {}", shapes(dialect))));
    }

    let mut inner_correlations = Vec::new();
    let mut outer_correlations = Vec::new();
    let mut saw_primary_type = false;
    let mut probe = PkProbe { constraint: None, schema: None, column_correlated: false };
    let conditions = join.on.iter().chain(query.expr.iter()).flat_map(conjuncts).collect::<Vec<_>>();
    for condition in conditions {
        let Expression::Binary { left, op: BinaryOperator::Equal, right, .. } = condition
        else {
            return Err(unsupported(format!("primary-key `EXISTS` accepts only ANDed equality conditions; {}", shapes(dialect))));
        };
        match (identifier_parts(left), identifier_parts(right)) {
            (Some((left_alias, left_column)), Some((right_alias, right_column))) => {
                match (side(&left_alias, &tc, &kcu, outer_alias), side(&right_alias, &tc, &kcu, outer_alias)) {
                    (Some(InnerSide::Tc), Some(InnerSide::Kcu)) | (Some(InnerSide::Kcu), Some(InnerSide::Tc)) => {
                        if left_column.eq_ignore_ascii_case(&right_column) {
                            inner_correlations.push(left_column.to_ascii_lowercase());
                        }
                        else {
                            return Err(unsupported(format!("primary-key `EXISTS` joins different columns; {}", shapes(dialect))));
                        }
                    }
                    (Some(InnerSide::Outer), Some(InnerSide::Tc)) | (Some(InnerSide::Tc), Some(InnerSide::Outer)) => {
                        if !left_column.eq_ignore_ascii_case(&right_column)
                            || !matches!(left_column.to_ascii_lowercase().as_str(), "table_schema" | "table_name")
                        {
                            return Err(unsupported(format!(
                                "primary-key `EXISTS` must correlate `table_schema` / `table_name`; {}",
                                shapes(dialect)
                            )));
                        }
                        outer_correlations.push(left_column.to_ascii_lowercase());
                    }
                    (Some(InnerSide::Outer), Some(InnerSide::Kcu)) | (Some(InnerSide::Kcu), Some(InnerSide::Outer)) => {
                        // Iris 逐字形态用 `kcu.column_name = c.column_name`，`is_pk` 必须逐列判定。
                        if !left_column.eq_ignore_ascii_case(&right_column) || !left_column.eq_ignore_ascii_case("column_name") {
                            return Err(unsupported(format!(
                                "primary-key `EXISTS` must correlate `column_name` with `key_column_usage`; {}",
                                shapes(dialect)
                            )));
                        }
                        probe.column_correlated = true;
                    }
                    _ => return Err(unsupported(format!("primary-key `EXISTS` condition is outside the accepted shape; {}", shapes(dialect)))),
                }
            }
            (Some((alias, column)), None)
                if side(&alias, &tc, &kcu, outer_alias) == Some(InnerSide::Tc) && column.eq_ignore_ascii_case("constraint_type") =>
            {
                match right.as_ref() {
                    Expression::Literal(Literal::String(value, _)) if value.eq_ignore_ascii_case("PRIMARY KEY") => {
                        saw_primary_type = true;
                    }
                    _ => {
                        return Err(unsupported(format!(
                            "primary-key `EXISTS` must compare `constraint_type` with `'PRIMARY KEY'`; {}",
                            shapes(dialect)
                        )));
                    }
                }
            }
            (Some((alias, column)), None)
                if side(&alias, &tc, &kcu, outer_alias) == Some(InnerSide::Tc) && column.eq_ignore_ascii_case("constraint_name") =>
            {
                match right.as_ref() {
                    Expression::Literal(Literal::String(value, _)) => probe.constraint = Some(value.to_string()),
                    _ => {
                        return Err(unsupported(format!(
                            "primary-key `EXISTS` constraint-name filter must be a string literal; {}",
                            shapes(dialect)
                        )));
                    }
                }
            }
            (Some((alias, column)), None)
                if side(&alias, &tc, &kcu, outer_alias) == Some(InnerSide::Tc) && column.eq_ignore_ascii_case("table_schema") =>
            {
                match right.as_ref() {
                    Expression::Literal(Literal::String(value, _)) => probe.schema = Some(value.to_string()),
                    _ => return Err(unsupported(format!("primary-key `EXISTS` schema filter must be a string literal; {}", shapes(dialect)))),
                }
            }
            _ => return Err(unsupported(format!("primary-key `EXISTS` condition is outside the accepted shape; {}", shapes(dialect)))),
        }
    }
    if !saw_primary_type {
        return Err(unsupported(format!("primary-key `EXISTS` must filter `constraint_type = 'PRIMARY KEY'`; {}", shapes(dialect))));
    }
    for required in ["constraint_name", "table_schema"] {
        if !inner_correlations.iter().any(|column| column == required) {
            return Err(unsupported(format!(
                "primary-key `EXISTS` must compare `{required}` between table_constraints and key_column_usage; {}",
                shapes(dialect)
            )));
        }
    }
    if !outer_correlations.iter().any(|column| column == "table_name") {
        return Err(unsupported(format!("primary-key `EXISTS` must correlate `table_name` with the outer row; {}", shapes(dialect))));
    }
    // 没有外层 `table_schema` 关联时，schema 必须由子查询里的字面量钉住，
    // 否则 `is_pk` 会跨 schema 误报。
    if !outer_correlations.iter().any(|column| column == "table_schema") && probe.schema.is_none() {
        return Err(unsupported(format!("primary-key `EXISTS` must pin the schema by correlation or a literal filter; {}", shapes(dialect))));
    }
    Ok(probe)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InnerSide {
    Tc,
    Kcu,
    Outer,
}

fn side(alias: &str, tc: &str, kcu: &str, outer: Option<&str>) -> Option<InnerSide> {
    if alias.eq_ignore_ascii_case(tc) {
        Some(InnerSide::Tc)
    }
    else if alias.eq_ignore_ascii_case(kcu) {
        Some(InnerSide::Kcu)
    }
    else if outer.is_some_and(|outer| alias.eq_ignore_ascii_case(outer)) {
        Some(InnerSide::Outer)
    }
    else {
        None
    }
}

fn identifier_parts(expr: &Expression) -> Option<(String, String)> {
    let Expression::Identifier(identifier) = expr
    else {
        return None;
    };
    let mut parts = identifier.name.split('.');
    let qualifier = parts.next()?;
    let column = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    Some((qualifier.to_string(), column.to_string()))
}

fn build_rows(query: &MetaQuery, state: &RegistryState, schema: &str) -> Vec<Vec<(ColumnId, SqlValue)>> {
    let mut rows = Vec::new();
    match query.view {
        View::ShowTables => {
            for table in &state.tables {
                if query.like.as_ref().is_some_and(|pattern| !like_matches(pattern, &table.name)) {
                    continue;
                }
                rows.push(vec![(ColumnId::TablesIn, SqlValue::String(table.name.clone()))]);
            }
        }
        View::MysqlTables => {
            for table in &state.tables {
                rows.push(vec![
                    (ColumnId::TableSchema, SqlValue::String(schema.to_string())),
                    (ColumnId::TableName, SqlValue::String(table.name.clone())),
                    (ColumnId::TableType, SqlValue::String("BASE TABLE".to_string())),
                ]);
            }
        }
        View::MysqlColumns => {
            for table in &state.tables {
                for (index, field) in table.fields.iter().enumerate() {
                    rows.push(vec![
                        (ColumnId::TableSchema, SqlValue::String(schema.to_string())),
                        (ColumnId::TableName, SqlValue::String(table.name.clone())),
                        (ColumnId::ColumnName, SqlValue::String(field.name.clone())),
                        (ColumnId::OrdinalPosition, SqlValue::Integer(index as i64 + 1)),
                        (ColumnId::ColumnDefault, optional_string(field.default.as_ref())),
                        (ColumnId::IsNullable, SqlValue::String(yes_no(field.field_type.is_nullable()))),
                        (ColumnId::DataType, SqlValue::String(mysql_data_type(&field.sql_type).to_string())),
                        (ColumnId::CharacterMaxLength, optional_integer(field.capacity)),
                        (ColumnId::ColumnKey, SqlValue::String(column_key(table, &field.name))),
                    ]);
                }
            }
        }
        View::PgTables => {
            for table in &state.tables {
                rows.push(vec![
                    (ColumnId::TableCatalog, SqlValue::String("yyds".to_string())),
                    (ColumnId::TableSchema, SqlValue::String(schema.to_string())),
                    (ColumnId::TableName, SqlValue::String(table.name.clone())),
                    (ColumnId::TableType, SqlValue::String("BASE TABLE".to_string())),
                ]);
            }
        }
        View::PgColumns => {
            for table in &state.tables {
                let schema_matches =
                    query.pk.as_ref().and_then(|probe| probe.schema.as_ref()).map_or(true, |name| name.eq_ignore_ascii_case(schema));
                let constraint_matches = query
                    .pk
                    .as_ref()
                    .and_then(|probe| probe.constraint.as_ref())
                    .map_or(true, |name| name.eq_ignore_ascii_case(&format!("{}_pkey", table.name.to_ascii_lowercase())));
                for (index, field) in table.fields.iter().enumerate() {
                    let (data_type, udt_name) = postgres_type(&field.sql_type);
                    let mut row = vec![
                        (ColumnId::TableCatalog, SqlValue::String("yyds".to_string())),
                        (ColumnId::TableSchema, SqlValue::String(schema.to_string())),
                        (ColumnId::TableName, SqlValue::String(table.name.clone())),
                        (ColumnId::ColumnName, SqlValue::String(field.name.clone())),
                        (ColumnId::OrdinalPosition, SqlValue::Integer(index as i64 + 1)),
                        (ColumnId::ColumnDefault, optional_string(field.default.as_ref())),
                        (ColumnId::IsNullable, SqlValue::String(yes_no(field.field_type.is_nullable()))),
                        (ColumnId::DataType, SqlValue::String(data_type.to_string())),
                        (ColumnId::UdtName, SqlValue::String(udt_name.to_string())),
                        (ColumnId::CharacterMaxLength, optional_integer(field.capacity)),
                        (ColumnId::NumericPrecision, SqlValue::Null),
                        (ColumnId::NumericScale, SqlValue::Null),
                    ];
                    if query.with_pk {
                        // `kcu.column_name = c.column_name` 让 `is_pk` 逐列判定。
                        let column_matches = query.pk.as_ref().map_or(true, |probe| {
                            !probe.column_correlated || table.primary_key.iter().any(|column| column.eq_ignore_ascii_case(&field.name))
                        });
                        let is_pk = !table.primary_key.is_empty() && schema_matches && constraint_matches && column_matches;
                        row.push((ColumnId::IsPk, SqlValue::Boolean(is_pk)));
                    }
                    rows.push(row);
                }
            }
        }
        View::PgTableConstraints => {
            for table in &state.tables {
                if !table.primary_key.is_empty() {
                    rows.push(constraint_row(table, schema, &format!("{}_pkey", table.name.to_ascii_lowercase()), "PRIMARY KEY"));
                }
                for constraint in &table.unique_constraints {
                    rows.push(constraint_row(table, schema, &unique_name(table, constraint), "UNIQUE"));
                }
            }
        }
        View::PgKeyColumnUsage => {
            for table in &state.tables {
                if !table.primary_key.is_empty() {
                    push_key_columns(&mut rows, table, schema, &format!("{}_pkey", table.name.to_ascii_lowercase()), &table.primary_key);
                }
                for constraint in &table.unique_constraints {
                    push_key_columns(&mut rows, table, schema, &unique_name(table, constraint), constraint);
                }
            }
        }
        View::PgClass => {
            for table in &state.tables {
                rows.push(vec![
                    (ColumnId::Oid, SqlValue::Integer(table.type_id as i64)),
                    (ColumnId::RelName, SqlValue::String(table.name.clone())),
                    (ColumnId::RelNamespace, SqlValue::Integer(1)),
                    (ColumnId::RelKind, SqlValue::String("r".to_string())),
                ]);
            }
        }
        View::PgNamespace => {
            rows.push(vec![(ColumnId::Oid, SqlValue::Integer(1)), (ColumnId::NspName, SqlValue::String("public".to_string()))]);
        }
        View::PgAttribute => {
            for table in &state.tables {
                for (index, field) in table.fields.iter().enumerate() {
                    let (_, udt_name) = postgres_type(&field.sql_type);
                    rows.push(vec![
                        (ColumnId::AttRelId, SqlValue::Integer(table.type_id as i64)),
                        (ColumnId::AttName, SqlValue::String(field.name.clone())),
                        (ColumnId::AttTypeId, SqlValue::Integer(i64::from(type_oid(udt_name)))),
                        (ColumnId::AttNum, SqlValue::Integer(index as i64 + 1)),
                        (ColumnId::AttNotNull, SqlValue::Boolean(!field.field_type.is_nullable())),
                        (ColumnId::AttTypMod, SqlValue::Integer(att_typmod(field))),
                    ]);
                }
            }
        }
        View::PgType => {
            let mut oids =
                state.tables.iter().flat_map(|table| table.fields.iter()).map(|field| postgres_type(&field.sql_type).1).collect::<Vec<_>>();
            oids.sort_by_key(|udt| type_oid(udt));
            oids.dedup_by_key(|udt| type_oid(udt));
            for udt in oids {
                rows.push(vec![
                    (ColumnId::Oid, SqlValue::Integer(i64::from(type_oid(udt)))),
                    (ColumnId::TypeName, SqlValue::String(udt.to_string())),
                ]);
            }
        }
    }
    rows
}

fn constraint_row(table: &SchemaTable, schema: &str, name: &str, constraint_type: &str) -> Vec<(ColumnId, SqlValue)> {
    vec![
        (ColumnId::ConstraintCatalog, SqlValue::String("yyds".to_string())),
        (ColumnId::ConstraintSchema, SqlValue::String(schema.to_string())),
        (ColumnId::ConstraintName, SqlValue::String(name.to_string())),
        (ColumnId::TableCatalog, SqlValue::String("yyds".to_string())),
        (ColumnId::TableSchema, SqlValue::String(schema.to_string())),
        (ColumnId::TableName, SqlValue::String(table.name.clone())),
        (ColumnId::ConstraintType, SqlValue::String(constraint_type.to_string())),
    ]
}

fn push_key_columns(rows: &mut Vec<Vec<(ColumnId, SqlValue)>>, table: &SchemaTable, schema: &str, name: &str, columns: &[String]) {
    for (index, column) in columns.iter().enumerate() {
        rows.push(vec![
            (ColumnId::ConstraintCatalog, SqlValue::String("yyds".to_string())),
            (ColumnId::ConstraintSchema, SqlValue::String(schema.to_string())),
            (ColumnId::ConstraintName, SqlValue::String(name.to_string())),
            (ColumnId::TableCatalog, SqlValue::String("yyds".to_string())),
            (ColumnId::TableSchema, SqlValue::String(schema.to_string())),
            (ColumnId::TableName, SqlValue::String(table.name.clone())),
            (ColumnId::ColumnName, SqlValue::String(column.clone())),
            (ColumnId::OrdinalPosition, SqlValue::Integer(index as i64 + 1)),
        ]);
    }
}

fn unique_name(table: &SchemaTable, columns: &[String]) -> String {
    let joined = columns.iter().map(|column| column.to_ascii_lowercase()).collect::<Vec<_>>().join("_");
    format!("{}_{}_key", table.name.to_ascii_lowercase(), joined)
}

fn column_key(table: &SchemaTable, column: &str) -> String {
    if table.primary_key.iter().any(|key| key == column) { "PRI".to_string() } else { String::new() }
}

fn yes_no(value: bool) -> String {
    if value { "YES".to_string() } else { "NO".to_string() }
}

fn optional_string(value: Option<&String>) -> SqlValue {
    value.map_or(SqlValue::Null, |value| SqlValue::String(value.clone()))
}

fn optional_integer(value: Option<u32>) -> SqlValue {
    value.map_or(SqlValue::Null, |value| SqlValue::Integer(i64::from(value)))
}

fn att_typmod(field: &SchemaField) -> i64 {
    let (_, udt_name) = postgres_type(&field.sql_type);
    match (udt_name, field.capacity) {
        ("varchar" | "bpchar", Some(capacity)) => i64::from(capacity) + 4,
        _ => -1,
    }
}

fn value_of(row: &[(ColumnId, SqlValue)], id: ColumnId) -> &SqlValue {
    row.iter().find(|(column, _)| *column == id).map(|(_, value)| value).unwrap_or(&SqlValue::Null)
}

fn equal_values(left: &SqlValue, right: &SqlValue) -> bool {
    match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => false,
        (SqlValue::String(left), SqlValue::String(right)) => left == right,
        (SqlValue::Integer(left), SqlValue::Integer(right)) => left == right,
        (SqlValue::Boolean(left), SqlValue::Boolean(right)) => left == right,
        (SqlValue::String(left), SqlValue::Integer(right)) => left == &right.to_string(),
        (SqlValue::Integer(left), SqlValue::String(right)) => &left.to_string() == right,
        _ => false,
    }
}

fn compare_values(left: &SqlValue, right: &SqlValue, nulls_first: bool) -> std::cmp::Ordering {
    match (left, right) {
        (SqlValue::Null, SqlValue::Null) => std::cmp::Ordering::Equal,
        (SqlValue::Null, _) if nulls_first => std::cmp::Ordering::Less,
        (SqlValue::Null, _) => std::cmp::Ordering::Greater,
        (_, SqlValue::Null) if nulls_first => std::cmp::Ordering::Greater,
        (_, SqlValue::Null) => std::cmp::Ordering::Less,
        (SqlValue::String(left), SqlValue::String(right)) => left.cmp(right),
        (SqlValue::Integer(left), SqlValue::Integer(right)) => left.cmp(right),
        (SqlValue::Boolean(left), SqlValue::Boolean(right)) => left.cmp(right),
        _ => std::cmp::Ordering::Equal,
    }
}

fn like_matches(pattern: &str, text: &str) -> bool {
    fn walk(pattern: &[char], text: &[char]) -> bool {
        match pattern.first() {
            None => text.is_empty(),
            Some('\\') => match pattern.get(1) {
                Some(next) => text.first().is_some_and(|candidate| candidate == next) && walk(&pattern[2..], &text[1..]),
                None => text.first().is_some_and(|candidate| candidate == &'\\') && walk(&pattern[1..], &text[1..]),
            },
            Some('%') => walk(&pattern[1..], text) || (!text.is_empty() && walk(pattern, &text[1..])),
            Some('_') => !text.is_empty() && walk(&pattern[1..], &text[1..]),
            Some(next) => text.first().is_some_and(|candidate| candidate == next) && walk(&pattern[1..], &text[1..]),
        }
    }
    walk(&pattern.chars().collect::<Vec<_>>(), &text.chars().collect::<Vec<_>>())
}

/// MySQL `DATA_TYPE` for a stored SQL type text.
pub fn mysql_data_type(sql_type: &str) -> &'static str {
    let (base, parameters) = split_sql_type(sql_type);
    match base {
        "TINYINT" if parameters == Some("1") => "tinyint",
        "BOOL" | "BOOLEAN" | "TINYINT" => "tinyint",
        "SMALLINT" | "INT2" => "smallint",
        "MEDIUMINT" => "mediumint",
        "INT" | "INTEGER" | "INT4" | "SERIAL" => "int",
        "BIGINT" | "INT8" | "BIGSERIAL" => "bigint",
        "FLOAT" | "FLOAT4" => "float",
        "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => "double",
        "VARCHAR" | "CHARACTER VARYING" => "varchar",
        "CHAR" | "CHARACTER" => "char",
        "TEXT" => "text",
        "TINYTEXT" => "tinytext",
        "MEDIUMTEXT" => "mediumtext",
        "LONGTEXT" => "longtext",
        "DATE" => "date",
        "TIME" => "time",
        "DATETIME" => "datetime",
        "TIMESTAMP" | "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMP WITHOUT TIME ZONE" => "timestamp",
        "UUID" => "char",
        "BINARY" => "binary",
        "VARBINARY" => "varbinary",
        "BLOB" => "blob",
        "TINYBLOB" => "tinyblob",
        "MEDIUMBLOB" => "mediumblob",
        "LONGBLOB" => "longblob",
        "BYTEA" => "binary",
        _ => "text",
    }
}

/// PostgreSQL `(data_type, udt_name)` for a stored SQL type text.
pub fn postgres_type(sql_type: &str) -> (&'static str, &'static str) {
    let (base, parameters) = split_sql_type(sql_type);
    match base {
        "TINYINT" if parameters == Some("1") => ("boolean", "bool"),
        "BOOL" | "BOOLEAN" => ("boolean", "bool"),
        "TINYINT" | "SMALLINT" | "INT2" => ("smallint", "int2"),
        "MEDIUMINT" | "INT" | "INTEGER" | "INT4" | "SERIAL" => ("integer", "int4"),
        "BIGINT" | "INT8" | "BIGSERIAL" => ("bigint", "int8"),
        "FLOAT" | "FLOAT4" | "REAL" => ("real", "float4"),
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => ("double precision", "float8"),
        "VARCHAR" | "CHARACTER VARYING" => ("character varying", "varchar"),
        "CHAR" | "CHARACTER" => ("character", "bpchar"),
        "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => ("text", "text"),
        "DATE" => ("date", "date"),
        "TIME" => ("time without time zone", "time"),
        "DATETIME" | "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => ("timestamp without time zone", "timestamp"),
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => ("timestamp with time zone", "timestamptz"),
        "UUID" => ("uuid", "uuid"),
        "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BYTEA" => ("bytea", "bytea"),
        _ => ("text", "text"),
    }
}

fn type_oid(udt_name: &str) -> u32 {
    match udt_name {
        "bool" => 16,
        "bytea" => 17,
        "int8" => 20,
        "int2" => 21,
        "int4" => 23,
        "text" => 25,
        "float4" => 700,
        "float8" => 701,
        "bpchar" => 1042,
        "varchar" => 1043,
        "date" => 1082,
        "time" => 1083,
        "timestamp" => 1114,
        "timestamptz" => 1184,
        "uuid" => 2950,
        _ => 25,
    }
}

fn split_sql_type(sql_type: &str) -> (&str, Option<&str>) {
    let Some(open) = sql_type.find('(')
    else {
        return (sql_type, None);
    };
    let close = sql_type.rfind(')').unwrap_or(sql_type.len());
    let parameters = if open + 1 < close { Some(&sql_type[open + 1..close]) } else { None };
    (sql_type[..open].trim_end(), parameters)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn analyzed(sql: &str, dialect: SqlDialect) -> MetaQuery {
        match analyze_meta_query(sql, dialect) {
            Ok(Some(query)) => query,
            Ok(None) => panic!("expected a metadata query: {sql}"),
            Err(error) => panic!("expected the shape to be supported: {error:?}"),
        }
    }

    #[test]
    fn count_star_metadata_query_returns_one_row() {
        let registry = SqlSchemaRegistry::new_in_memory();
        let cases = [
            ("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()", SqlDialect::Mysql),
            ("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'", SqlDialect::Postgres),
        ];
        for (sql, dialect) in cases {
            let result = analyzed(sql, dialect).execute(&registry, "app");
            assert_eq!(result.columns.len(), 1);
            assert_eq!(result.columns, vec!["COUNT(*)".to_string()]);
            assert_eq!(result.rows, vec![vec![SqlValue::Integer(0)]]);
        }
    }

    #[test]
    fn like_order_and_paging_are_supported() {
        let registry = SqlSchemaRegistry::new_in_memory();
        let cases = [
            (
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 'user%' ORDER BY TABLE_NAME LIMIT 10 OFFSET 2",
                SqlDialect::Mysql,
            ),
            (
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'user%' ORDER BY table_name LIMIT 10 OFFSET 2",
                SqlDialect::Postgres,
            ),
        ];
        for (sql, dialect) in cases {
            let result = analyzed(sql, dialect).execute(&registry, "app");
            assert_eq!(result.columns.len(), 1);
            assert!(result.rows.is_empty());
        }
    }

    #[test]
    fn unsafe_metadata_shapes_stay_unsupported() {
        let cases = [
            ("SELECT COUNT(*) + 1 FROM information_schema.TABLES", SqlDialect::Mysql),
            ("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_NAME LIKE TABLE_SCHEMA", SqlDialect::Postgres),
        ];
        for (sql, dialect) in cases {
            let error = match analyze_meta_query(sql, dialect) {
                Err(error) => error,
                Ok(_) => panic!("expected the shape to be rejected: {sql}"),
            };
            assert_eq!(error.kind, crate::sql::SqlErrorKind::Unsupported);
        }
    }
}
