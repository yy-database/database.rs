use database_types::{Error, Result};

use database_formats::sqlite::{RecordLimits, RecordValue, TextEncoding, WriteValue, decode_record, encode_record, validate_database};

use crate::sqlite::table::{TableLimits, TableRow, read_index_table, read_table};

/// Reads a secondary index b-tree in key order.
///
/// Each payload holds the indexed columns followed by the rowid column.
/// The walk rejects out-of-order keys across page boundaries.
pub fn read_named_index(bytes: &[u8], name: &str, limits: TableLimits) -> Result<Vec<Vec<u8>>> {
    validate_database(bytes)?;
    let objects = read_schema(bytes, limits)?;
    let object = objects
        .iter()
        .find(|object| object.kind == SchemaObjectKind::Index && object.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::Corrupt("sqlite named index not found"))?;
    let root = object.root_page.filter(|root| *root != 0 && *root != 1).ok_or(Error::Unsupported("sqlite virtual index access"))?;
    read_index_table(bytes, root, limits)
}

/// Object types stored in the SQLite schema table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaObjectKind {
    /// Ordinary, WITHOUT ROWID, or virtual table.
    Table,
    /// Explicit or automatically created index.
    Index,
    /// Stored view definition, not evaluated by this reader.
    View,
    /// Stored trigger definition, not evaluated by this reader.
    Trigger,
}

/// One typed schema record with SQL preserved as opaque source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaObject {
    /// The object type reported by SQLite.
    pub kind: SchemaObjectKind,
    /// Stored object name, without identifier parsing or normalization.
    pub name: String,
    /// Associated table or view name.
    pub table_name: String,
    /// Stored root page, preserving NULL and zero for non-b-tree objects.
    pub root_page: Option<u32>,
    /// Opaque CREATE source, or NULL for an automatic index.
    pub sql: Option<String>,
}

/// Reads the five schema columns without parsing CREATE statements.
/// Empty uninitialized databases have no schema objects.
pub fn read_schema(bytes: &[u8], limits: TableLimits) -> Result<Vec<SchemaObject>> {
    Ok(read_schema_with_rowids(bytes, limits)?.into_iter().map(|(_, object)| object).collect())
}

/// Reads the schema objects together with the rowid of each `sqlite_master`
/// record, so a caller can reproduce `SELECT rowid FROM sqlite_master`.
pub fn read_schema_with_rowids(bytes: &[u8], limits: TableLimits) -> Result<Vec<(i64, SchemaObject)>> {
    validate_database(bytes)?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let rows = read_table(bytes, 1, limits)?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let encoding = match u32::from_be_bytes(bytes[56..60].try_into().unwrap()) {
        1 => TextEncoding::Utf8,
        2 => TextEncoding::Utf16Le,
        3 => TextEncoding::Utf16Be,
        _ => return Err(Error::Corrupt("sqlite nonempty schema has no text encoding")),
    };
    rows.into_iter().map(|row| Ok((row.rowid, decode_schema(&row.payload, encoding, limits.max_payload_bytes)?))).collect()
}

/// Reads a table or a simple view by stored name, using ASCII-insensitive matching.
/// Schema aliases refer to the main-file schema, not a connection's TEMP schema.
/// Virtual tables, index b-trees and WITHOUT ROWID tables are unsupported here;
/// views are resolved when their definition is a plain column projection onto
/// a single rowid table, and rejected with a distinguishable error otherwise.
pub fn read_named_table(bytes: &[u8], name: &str, limits: TableLimits) -> Result<Vec<TableRow>> {
    read_named_table_at_depth(bytes, name, limits, 0)
}

const MAX_VIEW_DEPTH: usize = 8;

fn read_named_table_at_depth(bytes: &[u8], name: &str, limits: TableLimits, depth: usize) -> Result<Vec<TableRow>> {
    if depth > MAX_VIEW_DEPTH {
        return Err(Error::Unsupported("sqlite view nesting depth exceeded"));
    }
    if name.eq_ignore_ascii_case("sqlite_schema") || name.eq_ignore_ascii_case("sqlite_master") {
        validate_database(bytes)?;
        return if bytes.is_empty() { Ok(Vec::new()) } else { read_table(bytes, 1, limits) };
    }
    let objects = read_schema(bytes, limits)?;
    let mut matching = objects
        .iter()
        .filter(|object| matches!(object.kind, SchemaObjectKind::Table | SchemaObjectKind::View) && object.name.eq_ignore_ascii_case(name));
    let object = matching.next().ok_or_else(|| Error::Corrupt("sqlite named table not found"))?;
    if matching.next().is_some() {
        return Err(Error::Corrupt("sqlite ambiguous table name"));
    }
    match object.kind {
        SchemaObjectKind::Table => read_table_object(bytes, object, limits),
        SchemaObjectKind::View => read_view_rows(bytes, object, limits, depth),
        _ => Err(Error::Corrupt("sqlite named object is not a table or view")),
    }
}

fn read_table_object(bytes: &[u8], object: &SchemaObject, limits: TableLimits) -> Result<Vec<TableRow>> {
    let root = object.root_page.filter(|root| *root != 0).ok_or(Error::Unsupported("sqlite virtual table access"))?;
    if root == 1 {
        return Err(Error::Corrupt("sqlite user table aliases schema root"));
    }
    let rows = read_table(bytes, root, limits)?;
    let Some(sql) = object.sql.as_deref()
    else {
        return Err(Error::Unsupported("sqlite table has no CREATE source"));
    };
    if is_strict(sql) {
        validate_strict_rows(&rows, sql, database_encoding(bytes)?, limits)?;
    }
    Ok(rows)
}

/// Resolves `CREATE VIEW <name> AS SELECT <columns> FROM <table>` onto the base
/// table. Expressions, joins, filters, aliases, subqueries and view-on-view
/// chains are rejected instead of being half evaluated.
fn read_view_rows(bytes: &[u8], object: &SchemaObject, limits: TableLimits, depth: usize) -> Result<Vec<TableRow>> {
    let sql = object.sql.as_deref().ok_or(Error::Unsupported("sqlite view has no CREATE source"))?;
    let projection = parse_view(sql)?;
    let base = read_named_table_at_depth(bytes, &projection.base, limits, depth + 1)?;
    let encoding = database_encoding(bytes)?;
    let definitions = base_table_columns(bytes, &projection.base, limits)?;
    let alias_index = definitions.iter().position(|column| column.rowid_alias);
    let positions = match &projection.columns {
        Some(columns) => {
            let mut positions = Vec::with_capacity(columns.len());
            for column in columns {
                let position = definitions
                    .iter()
                    .position(|definition| definition.name.eq_ignore_ascii_case(column))
                    .ok_or(Error::Corrupt("sqlite view selects an unknown column"))?;
                positions.push(position);
            }
            positions
        }
        None => (0..definitions.len()).collect(),
    };
    let mut rows = Vec::with_capacity(base.len());
    for row in base {
        let values = decode_record(
            &row.payload,
            encoding,
            RecordLimits { max_columns: definitions.len().max(1), max_payload_bytes: limits.max_payload_bytes },
        )
        .map_err(|_| Error::Corrupt("sqlite view base record decode failed"))?;
        // An INTEGER PRIMARY KEY column is stored as NULL in the base record and
        // only takes its value from the rowid, so the projection must substitute
        // it instead of handing the viewer a null.
        let projected: Vec<WriteValue> = positions
            .iter()
            .map(|position| {
                if Some(*position) == alias_index {
                    WriteValue::Integer(row.rowid)
                }
                else {
                    values.get(*position).map(write_value_from_record).unwrap_or(WriteValue::Null)
                }
            })
            .collect();
        rows.push(TableRow { rowid: row.rowid, payload: encode_record(&projected, encoding) });
    }
    Ok(rows)
}

fn write_value_from_record(value: &RecordValue<'_>) -> WriteValue {
    match value {
        RecordValue::Null => WriteValue::Null,
        RecordValue::Integer(value) => WriteValue::Integer(*value),
        RecordValue::Real(value) => WriteValue::Real(*value),
        RecordValue::Text(value) => WriteValue::Text(value.clone()),
        RecordValue::Blob(value) => WriteValue::Blob(value.to_vec()),
    }
}

/// 只读视图投影：基表名与可选列重排。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewProjection {
    /// 视图所引用的单表名。
    pub base: String,
    /// `None` 保持基表列序（`SELECT *`）；`Some` 为显式列名列表。
    pub columns: Option<Vec<String>>,
}

/// 解析仅支持「单表、纯标识符列清单」的 `CREATE VIEW`。
pub fn parse_view(sql: &str) -> Result<ViewProjection> {
    let source = sql.trim().trim_end_matches(';').trim();
    let upper = source.to_ascii_uppercase();
    if !upper.starts_with("CREATE VIEW") {
        return Err(Error::Corrupt("sqlite view source does not start with CREATE VIEW"));
    }
    let as_position = upper.find(" AS ").ok_or(Error::Unsupported("sqlite view definition has no AS clause"))?;
    let rest = source[as_position + 4..].trim();
    let rest_upper = rest.to_ascii_uppercase();
    if !rest_upper.starts_with("SELECT") {
        return Err(Error::Unsupported("sqlite view definition is not a SELECT"));
    }
    let body = &rest["SELECT".len()..];
    let from_position = body.to_ascii_uppercase().find(" FROM ").ok_or(Error::Unsupported("sqlite view definition has no FROM clause"))?;
    let column_source = body[..from_position].trim();
    let table_source = body[from_position + 6..].trim();
    if column_source.is_empty() {
        return Err(Error::Corrupt("sqlite view selects no columns"));
    }
    let base = parse_view_base(table_source)?;
    if column_source.contains('(') || column_source.contains(')') {
        return Err(Error::Unsupported("sqlite view column expressions are not supported"));
    }
    let mut columns = Vec::new();
    for item in column_source.split(',') {
        let item = item.trim();
        if item == "*" {
            if column_source.split(',').count() > 1 {
                return Err(Error::Unsupported("sqlite view mixes SELECT * with explicit columns"));
            }
            return Ok(ViewProjection { base, columns: None });
        }
        let (name, remainder) = parse_identifier(item).ok_or(Error::Unsupported("sqlite view columns must be plain identifiers"))?;
        if !remainder.trim().is_empty() {
            return Err(Error::Unsupported("sqlite view column aliases or expressions are not supported"));
        }
        columns.push(name);
    }
    if columns.is_empty() {
        return Err(Error::Corrupt("sqlite view selects no columns"));
    }
    Ok(ViewProjection { base, columns: Some(columns) })
}

fn parse_view_base(source: &str) -> Result<String> {
    let upper = source.to_ascii_uppercase();
    for keyword in [" WHERE ", " ORDER ", " GROUP ", " LIMIT ", " JOIN ", " UNION ", " HAVING "] {
        if upper.contains(keyword) {
            return Err(Error::Unsupported("sqlite view filters or joins are not supported"));
        }
    }
    let (base, remainder) = parse_identifier(source).ok_or(Error::Unsupported("sqlite view base must be a single table"))?;
    if !remainder.trim().is_empty() {
        return Err(Error::Unsupported("sqlite view base must be a single table"));
    }
    Ok(base)
}

/// Accepts a bare, double-quoted, bracketed or backticked identifier and
/// returns it together with the unconsumed remainder.
fn parse_identifier(source: &str) -> Option<(String, &str)> {
    let source = source.trim_start();
    let (raw_name, rest) = if let Some(rest) = source.strip_prefix('"') {
        let end = rest.find('"')?;
        (&rest[..end], &rest[end + 1..])
    }
    else if let Some(rest) = source.strip_prefix('[') {
        let end = rest.find(']')?;
        (&rest[..end], &rest[end + 1..])
    }
    else if let Some(rest) = source.strip_prefix('`') {
        let end = rest.find('`')?;
        (&rest[..end], &rest[end + 1..])
    }
    else {
        let end =
            source.find(|character: char| character.is_whitespace() || matches!(character, '(' | ')' | ',' | '.')).unwrap_or(source.len());
        let name = &source[..end];
        if !name.chars().all(|character| character.is_alphanumeric() || matches!(character, '_' | '$')) {
            return None;
        }
        (name, &source[end..])
    };
    if raw_name.is_empty() || raw_name.chars().any(char::is_control) {
        return None;
    }
    Some((raw_name.to_string(), rest))
}

fn database_encoding(bytes: &[u8]) -> Result<TextEncoding> {
    validate_database(bytes)?;
    match u32::from_be_bytes(bytes[56..60].try_into().map_err(|_| Error::Corrupt("sqlite header too short"))?) {
        1 => Ok(TextEncoding::Utf8),
        2 => Ok(TextEncoding::Utf16Le),
        3 => Ok(TextEncoding::Utf16Be),
        _ => Err(Error::Corrupt("sqlite nonempty schema has no text encoding")),
    }
}

fn base_table_columns(bytes: &[u8], name: &str, limits: TableLimits) -> Result<Vec<ColumnDefinition>> {
    let objects = read_schema(bytes, limits)?;
    let object = objects
        .iter()
        .find(|object| object.kind == SchemaObjectKind::Table && object.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::Unsupported("sqlite view base is not a rowid table"))?;
    let sql = object.sql.as_deref().ok_or(Error::Unsupported("sqlite view base has no CREATE source"))?;
    if is_without_rowid(sql) {
        return Err(Error::Unsupported("sqlite view over a WITHOUT ROWID table"));
    }
    column_definitions(sql)
}

struct ColumnDefinition {
    name: String,
    declared_type: String,
    rowid_alias: bool,
}

fn column_definitions(sql: &str) -> Result<Vec<ColumnDefinition>> {
    let open = sql.find('(').ok_or(Error::Corrupt("sqlite CREATE TABLE has no column list"))?;
    let close = sql.rfind(')').ok_or(Error::Corrupt("sqlite CREATE TABLE has no column list"))?;
    if close <= open {
        return Err(Error::Corrupt("sqlite CREATE TABLE column list is malformed"));
    }
    let mut columns = Vec::new();
    for fragment in split_top_level(&sql[open + 1..close]) {
        let fragment = fragment.trim();
        if fragment.is_empty() {
            continue;
        }
        let upper = fragment.to_ascii_uppercase();
        if ["CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"].iter().any(|keyword| upper.starts_with(keyword)) {
            continue;
        }
        let (name, remainder) = parse_identifier(fragment).ok_or(Error::Corrupt("sqlite CREATE TABLE column name invalid"))?;
        let rest = remainder.trim();
        let declared_type = rest.split_whitespace().next().unwrap_or("").to_ascii_uppercase();
        let rowid_alias = declared_type == "INTEGER" && rest.to_ascii_uppercase().contains("PRIMARY KEY");
        columns.push(ColumnDefinition { name, declared_type, rowid_alias });
    }
    if columns.is_empty() {
        return Err(Error::Corrupt("sqlite CREATE TABLE declares no columns"));
    }
    Ok(columns)
}

fn split_top_level(source: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, character) in source.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&source[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&source[start..]);
    parts
}

fn is_strict(sql: &str) -> bool {
    sql.trim().trim_end_matches(';').trim().to_ascii_uppercase().ends_with("STRICT")
}

/// SQLite enforces STRICT storage classes at write time. Reading such a table
/// as an ordinary rowid table would silently accept values the external engine
/// would have rejected, so a violation is reported as corruption instead.
/// 校验 STRICT 表行载荷是否落在声明类型内。
pub fn validate_strict_rows(rows: &[TableRow], sql: &str, encoding: TextEncoding, limits: TableLimits) -> Result<()> {
    let columns = column_definitions(sql)?;
    for column in &columns {
        match column.declared_type.as_str() {
            "INT" | "INTEGER" | "REAL" | "TEXT" | "BLOB" | "ANY" => {}
            _ => return Err(Error::Unsupported("sqlite STRICT column declares a type outside STRICT rules")),
        }
    }
    for row in rows {
        let values =
            decode_record(&row.payload, encoding, RecordLimits { max_columns: columns.len(), max_payload_bytes: limits.max_payload_bytes })
                .map_err(|_| Error::Corrupt("sqlite STRICT record decode failed"))?;
        for (value, column) in values.iter().zip(columns.iter()) {
            if matches!(value, RecordValue::Null) {
                continue;
            }
            let conforms = match column.declared_type.as_str() {
                "INT" | "INTEGER" => matches!(value, RecordValue::Integer(_)) || column.rowid_alias,
                "REAL" => matches!(value, RecordValue::Real(_)),
                "TEXT" => matches!(value, RecordValue::Text(_)),
                "BLOB" => matches!(value, RecordValue::Blob(_)),
                _ => true,
            };
            if !conforms {
                return Err(Error::Corrupt("sqlite STRICT column stores a value outside its declared type"));
            }
        }
    }
    Ok(())
}

/// Reads a WITHOUT ROWID table's records in primary-key order.
///
/// SQLite stores these rows in the primary-key index b-tree, so the schema
/// `rootpage` is walked as an index b-tree rather than a rowid table.
pub fn read_named_without_rowid(bytes: &[u8], name: &str, limits: TableLimits) -> Result<Vec<Vec<u8>>> {
    validate_database(bytes)?;
    let objects = read_schema(bytes, limits)?;
    let object = objects
        .iter()
        .find(|object| object.kind == SchemaObjectKind::Table && object.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::Corrupt("sqlite named table not found"))?;
    let sql = object.sql.as_deref().ok_or(Error::Unsupported("sqlite table has no CREATE source"))?;
    if !is_without_rowid(sql) {
        return Err(Error::Unsupported("sqlite table is not WITHOUT ROWID"));
    }
    let root = object.root_page.filter(|root| *root != 0 && *root != 1).ok_or(Error::Unsupported("sqlite virtual table access"))?;
    read_index_table(bytes, root, limits)
}

fn is_without_rowid(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains("without rowid")
}

fn decode_schema(payload: &[u8], encoding: TextEncoding, max_payload_bytes: usize) -> Result<SchemaObject> {
    let values = decode_record(payload, encoding, RecordLimits { max_columns: 5, max_payload_bytes })?;
    let [kind, name, table_name, root_page, sql]: [RecordValue<'_>; 5] =
        values.try_into().map_err(|_| Error::Corrupt("sqlite schema column count invalid"))?;
    let kind = match text(kind)?.as_str() {
        "table" => SchemaObjectKind::Table,
        "index" => SchemaObjectKind::Index,
        "view" => SchemaObjectKind::View,
        "trigger" => SchemaObjectKind::Trigger,
        _ => return Err(Error::Corrupt("sqlite schema object kind invalid")),
    };
    let name = text(name)?;
    let table_name = text(table_name)?;
    if name.is_empty() || table_name.is_empty() {
        return Err(Error::Corrupt("sqlite schema object name empty"));
    }
    let root_page = match root_page {
        RecordValue::Null => None,
        RecordValue::Integer(value) => Some(u32::try_from(value).map_err(|_| Error::Corrupt("sqlite schema root page invalid"))?),
        _ => return Err(Error::Corrupt("sqlite schema root page is not integer or NULL")),
    };
    if matches!(kind, SchemaObjectKind::View | SchemaObjectKind::Trigger) && root_page.is_some_and(|root| root != 0) {
        return Err(Error::Corrupt("sqlite schema non-b-tree object has root page"));
    }
    let sql = match sql {
        RecordValue::Null => None,
        RecordValue::Text(value) => Some(value),
        _ => return Err(Error::Corrupt("sqlite schema SQL is not text or NULL")),
    };
    Ok(SchemaObject { kind, name, table_name, root_page, sql })
}

fn text(value: RecordValue<'_>) -> Result<String> {
    match value {
        RecordValue::Text(value) => Ok(value),
        _ => Err(Error::Corrupt("sqlite schema field is not text")),
    }
}
