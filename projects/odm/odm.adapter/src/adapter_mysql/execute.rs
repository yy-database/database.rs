//! Physical-plan execution over the MySQL wire client.
//!
//! SQL stays private: plans are lowered to parameterised prepared statements
//! (`?` placeholders) and binary result cells are decoded back through the
//! column definitions the server returned.
//!
//! MySQL has no `RETURNING`. `INSERT` is followed by a keyed re-query and
//! `UPDATE` reads its target rows before writing, then returns them with the
//! patched literals applied. Neither path pretends the server echoed rows.

use std::collections::HashSet;

use database_client::mysql::{Cell, ColumnInfo, Param, QueryResult, mysql_type};
use database_query::ir::{CmpOp, LiteralKind, PhysicalOp, PhysicalPlan, Pred, ProjectField, StringMatchOp, WriteField};
use odm_types::{Row, RowWrite, Value, hex_to_bytes, is_date, is_decimal, is_time};

use crate::{Error, Result};

use super::{Session, try_parse_uuid_bytes, uuid_bytes_to_str};

pub(crate) fn execute_plan(session: &mut Session, plan: &PhysicalPlan, uuid_fields: &HashSet<(String, String)>) -> Result<Vec<Row>> {
    if plan.nodes.iter().any(|node| matches!(node.op, PhysicalOp::Insert { .. } | PhysicalOp::Patch { .. } | PhysicalOp::Delete { .. })) {
        execute_write_plan(session, plan, uuid_fields)
    }
    else {
        execute_read_plan(session, plan, uuid_fields)
    }
}

fn execute_read_plan(session: &mut Session, plan: &PhysicalPlan, uuid_fields: &HashSet<(String, String)>) -> Result<Vec<Row>> {
    let table = plan
        .nodes
        .iter()
        .find_map(|node| match &node.op {
            PhysicalOp::Scan { table } => Some(table.clone()),
            _ => None,
        })
        .ok_or_else(|| Error::Policy("plan missing Scan".into()))?;

    let mut where_sql: Option<String> = None;
    let mut bindings = Bindings::default();
    let mut order: Option<String> = None;
    let mut limit: Option<u64> = None;
    let mut offset: Option<u64> = None;
    let mut projection: Option<Vec<ProjectField>> = None;

    for node in &plan.nodes {
        match &node.op {
            PhysicalOp::Scan { .. } => {}
            PhysicalOp::Filter { predicate } => where_sql = Some(pred_to_sql(predicate, &table, uuid_fields, &mut bindings)?),
            PhysicalOp::Project { fields } => projection = Some(fields.clone()),
            PhysicalOp::Sort { keys } => {
                let parts: Vec<String> =
                    keys.iter().map(|key| format!("`{}` {}", key.field, if key.ascending { "ASC" } else { "DESC" })).collect();
                order = Some(parts.join(", "));
            }
            PhysicalOp::Skip { count } => offset = Some(*count),
            PhysicalOp::Take { count } => limit = Some(*count),
            PhysicalOp::Collect => {}
            PhysicalOp::Insert { .. } | PhysicalOp::Patch { .. } | PhysicalOp::Delete { .. } => {
                return Err(Error::Policy("write op in read plan".into()));
            }
        }
    }

    let select = projection.as_deref().map(project_fields_sql).map(|fields| fields.join(", ")).unwrap_or_else(|| "*".into());
    let mut sql = format!("SELECT {select} FROM `{table}`");
    if let Some(where_sql) = &where_sql {
        sql.push_str(" WHERE ");
        sql.push_str(where_sql);
    }
    if let Some(order) = &order {
        sql.push_str(" ORDER BY ");
        sql.push_str(order);
    }
    match (limit, offset) {
        (Some(limit), Some(offset)) => sql.push_str(&format!(" LIMIT {limit} OFFSET {offset}")),
        (Some(limit), None) => sql.push_str(&format!(" LIMIT {limit}")),
        (None, Some(offset)) => sql.push_str(&format!(" LIMIT 18446744073709551615 OFFSET {offset}")),
        (None, None) => {}
    }

    let params = bindings.as_params();
    let result = session.query_with_params(&sql, &params).map_err(Error::Wire)?;
    result_to_rows(result, &table, uuid_fields, projection.as_deref())
}

fn execute_write_plan(session: &mut Session, plan: &PhysicalPlan, uuid_fields: &HashSet<(String, String)>) -> Result<Vec<Row>> {
    let mut insert: Option<(String, Vec<WriteField>)> = None;
    let mut patch: Option<(String, Option<Pred>, Vec<WriteField>)> = None;
    let mut delete: Option<(String, Option<Pred>)> = None;
    let mut projection: Option<Vec<ProjectField>> = None;

    for node in &plan.nodes {
        match &node.op {
            PhysicalOp::Insert { table, fields } => insert = Some((table.clone(), fields.clone())),
            PhysicalOp::Patch { table, filter, fields } => patch = Some((table.clone(), filter.clone(), fields.clone())),
            PhysicalOp::Delete { table, filter } => delete = Some((table.clone(), filter.clone())),
            PhysicalOp::Project { fields } => projection = Some(fields.clone()),
            PhysicalOp::Collect => {}
            other => return Err(Error::Policy(format!("unexpected op in write plan: {other:?}"))),
        }
    }

    if let Some((table, filter)) = delete {
        let mut bindings = Bindings::default();
        let where_sql = filter_to_sql(filter.as_ref(), &table, uuid_fields, &mut bindings)?;
        let sql = format!("DELETE FROM `{table}` WHERE {where_sql}");
        session.execute_with_params(&sql, &bindings.as_params()).map_err(Error::Wire)?;
        return Ok(Vec::new());
    }

    if let Some((table, fields)) = insert {
        let mut bindings = Bindings::default();
        let cols: Vec<String> = fields.iter().map(|field| format!("`{}`", field.name)).collect();
        let placeholders: Vec<String> =
            fields.iter().map(|field| write_field_binding(&table, field, uuid_fields, &mut bindings)).collect::<Result<Vec<_>>>()?;
        let sql = format!("INSERT INTO `{table}` ({}) VALUES ({})", cols.join(", "), placeholders.join(", "));
        session.query_with_params(&sql, &bindings.as_params()).map_err(Error::Wire)?;
        let readback = read_inserted_row(session, &table, &fields, uuid_fields)?;
        let rows = result_to_rows(readback, &table, uuid_fields, None)?;
        return Ok(apply_projection(rows, projection.as_deref()));
    }

    if let Some((table, filter, fields)) = patch {
        let targets = select_patch_targets(session, &table, filter.as_ref(), uuid_fields)?;

        let mut bindings = Bindings::default();
        let sets: Vec<String> = fields
            .iter()
            .map(|field| Ok(format!("`{}` = {}", field.name, write_field_binding(&table, field, uuid_fields, &mut bindings)?)))
            .collect::<Result<Vec<_>>>()?;
        let where_sql = filter_to_sql(filter.as_ref(), &table, uuid_fields, &mut bindings)?;
        let sql = format!("UPDATE `{table}` SET {} WHERE {where_sql}", sets.join(", "));
        session.execute_with_params(&sql, &bindings.as_params()).map_err(Error::Wire)?;

        let mut rows = targets;
        for row in &mut rows {
            for field in &fields {
                row.insert(field.name.clone(), literal_to_value(field)?);
            }
        }
        return Ok(apply_projection(rows, projection.as_deref()));
    }

    Ok(Vec::new())
}

/// Re-query the row just inserted.
///
/// When the primary key columns are known and all of them were supplied, the
/// read-back is keyed on them; otherwise every inserted assignment is used
/// (`IS NULL` for null literals) and the result is capped at one row.
fn read_inserted_row(
    session: &mut Session,
    table: &str,
    fields: &[WriteField],
    uuid_fields: &HashSet<(String, String)>,
) -> Result<QueryResult> {
    let primary = primary_key_columns(session, table)?;
    let supplied: Vec<&WriteField> = primary.iter().filter_map(|name| fields.iter().find(|field| &field.name == name)).collect();
    let selected: Vec<&WriteField> = if !primary.is_empty() && supplied.len() == primary.len() { supplied } else { fields.iter().collect() };

    let mut bindings = Bindings::default();
    let mut clauses = Vec::new();
    for field in selected {
        if matches!(field.kind, LiteralKind::Null) {
            clauses.push(format!("`{}` IS NULL", field.name));
            continue;
        }
        let placeholder = write_field_binding(table, field, uuid_fields, &mut bindings)?;
        clauses.push(format!("`{}` = {placeholder}", field.name));
    }
    let sql = if clauses.is_empty() {
        format!("SELECT * FROM `{table}` LIMIT 1")
    }
    else {
        format!("SELECT * FROM `{table}` WHERE {} LIMIT 1", clauses.join(" AND "))
    };
    session.query_with_params(&sql, &bindings.as_params()).map_err(Error::Wire)
}

/// Read the rows a patch is about to touch, before the update runs.
///
/// MySQL cannot echo updated rows, so the target set is read first and the
/// patched literals are applied in memory afterwards.
fn select_patch_targets(
    session: &mut Session,
    table: &str,
    filter: Option<&Pred>,
    uuid_fields: &HashSet<(String, String)>,
) -> Result<Vec<Row>> {
    let mut bindings = Bindings::default();
    let sql = match filter {
        Some(filter) => format!("SELECT * FROM `{table}` WHERE {}", pred_to_sql(filter, table, uuid_fields, &mut bindings)?),
        None => format!("SELECT * FROM `{table}`"),
    };
    let result = session.query_with_params(&sql, &bindings.as_params()).map_err(Error::Wire)?;
    result_to_rows(result, table, uuid_fields, None)
}

fn primary_key_columns(session: &mut Session, table: &str) -> Result<Vec<String>> {
    let result = session
        .query_with_params(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI' \
             ORDER BY ORDINAL_POSITION",
            &[Param::text(Some(table))],
        )
        .map_err(Error::Wire)?;
    Ok(result
        .rows
        .into_iter()
        .filter_map(|row| match row.into_iter().next() {
            Some(Cell::Bytes(bytes)) => Some(String::from_utf8_lossy(&bytes).into_owned()),
            _ => None,
        })
        .collect())
}

pub(crate) fn insert_row(session: &mut Session, write: &RowWrite, uuid_fields: &HashSet<(String, String)>) -> Result<()> {
    let mut bindings = Bindings::default();
    let cols: Vec<String> = write.fields.keys().map(|name| format!("`{name}`")).collect();
    let placeholders: Vec<String> = write
        .fields
        .iter()
        .map(|(name, value)| value_field_binding(&write.table, name, value, uuid_fields, &mut bindings))
        .collect::<Result<Vec<_>>>()?;
    let sql = format!("INSERT INTO `{}` ({}) VALUES ({})", write.table, cols.join(", "), placeholders.join(", "));
    session.execute_with_params(&sql, &bindings.as_params()).map_err(Error::Wire)?;
    Ok(())
}

pub(crate) fn update_row(session: &mut Session, write: &RowWrite, uuid_fields: &HashSet<(String, String)>) -> Result<u64> {
    let key = write.fields.get(&write.primary_key).ok_or_else(|| Error::Policy("update missing primary key value".into()))?;
    let mut bindings = Bindings::default();
    let sets: Vec<String> = write
        .fields
        .iter()
        .filter(|(name, _)| *name != &write.primary_key)
        .map(|(name, value)| Ok(format!("`{name}` = {}", value_field_binding(&write.table, name, value, uuid_fields, &mut bindings)?)))
        .collect::<Result<Vec<_>>>()?;
    if sets.is_empty() {
        return Ok(0);
    }
    let key_placeholder = value_field_binding(&write.table, &write.primary_key, key, uuid_fields, &mut bindings)?;
    let sql = format!("UPDATE `{}` SET {} WHERE `{}` = {key_placeholder}", write.table, sets.join(", "), write.primary_key);
    session.execute_with_params(&sql, &bindings.as_params()).map_err(Error::Wire)
}

pub(crate) fn delete_row(
    session: &mut Session,
    table: &str,
    primary_key: &str,
    key: &Value,
    uuid_fields: &HashSet<(String, String)>,
) -> Result<u64> {
    let mut bindings = Bindings::default();
    let placeholder = value_field_binding(table, primary_key, key, uuid_fields, &mut bindings)?;
    let sql = format!("DELETE FROM `{table}` WHERE `{primary_key}` = {placeholder}");
    session.execute_with_params(&sql, &bindings.as_params()).map_err(Error::Wire)
}

fn result_to_rows(
    result: QueryResult,
    table: &str,
    uuid_fields: &HashSet<(String, String)>,
    projection: Option<&[ProjectField]>,
) -> Result<Vec<Row>> {
    if result.column_info.len() != result.columns.len() {
        return Err(Error::Policy("MySQL result is missing column metadata".into()));
    }
    let mut rows = Vec::with_capacity(result.rows.len());
    for values in result.rows {
        let mut out = Row::new();
        for (index, column) in result.column_info.iter().enumerate() {
            let cell = values.get(index).cloned().unwrap_or(Cell::Null);
            let uuid = is_uuid_column(table, &column.name, projection, uuid_fields);
            out.insert(column.name.clone(), decode_cell(cell, column, uuid)?);
        }
        rows.push(out);
    }
    Ok(rows)
}

fn decode_cell(cell: Cell, column: &ColumnInfo, uuid: bool) -> Result<Value> {
    let Cell::Bytes(bytes) = cell
    else {
        return Ok(Value::Null);
    };
    if uuid {
        if let Some(text) = uuid_bytes_to_str(&bytes) {
            return Ok(Value::Str(text));
        }
        let text = String::from_utf8_lossy(&bytes).into_owned();
        if text.chars().count() == 36 {
            return Ok(Value::Str(text));
        }
        return Ok(Value::Bytes(bytes));
    }
    let text = String::from_utf8_lossy(&bytes).into_owned();
    match column.type_code {
        mysql_type::TINY if column.column_length == 1 => match text.as_str() {
            "0" => Ok(Value::Bool(false)),
            "1" => Ok(Value::Bool(true)),
            other => Err(Error::Policy(format!("MySQL returned `{other}` for a boolean column"))),
        },
        mysql_type::TINY | mysql_type::SHORT | mysql_type::LONG | mysql_type::INT24 | mysql_type::LONGLONG | mysql_type::YEAR => {
            if let Ok(value) = text.parse::<i64>() {
                Ok(Value::Int(value))
            }
            else if let Ok(value) = text.parse::<u64>() {
                Ok(Value::UInt(value))
            }
            else {
                Err(Error::Policy(format!("MySQL returned `{text}` for an integer column")))
            }
        }
        mysql_type::FLOAT | mysql_type::DOUBLE => {
            let value = text.parse::<f64>().map_err(|_| Error::Policy(format!("MySQL returned `{text}` for a float column")))?;
            if value.is_finite() { Ok(Value::Float(value)) } else { Err(Error::Policy("MySQL returned a non-finite float".into())) }
        }
        mysql_type::DECIMAL | mysql_type::NEWDECIMAL => {
            if is_decimal(&text) {
                Ok(Value::Decimal(text))
            }
            else {
                Err(Error::Policy(format!("MySQL returned `{text}` for a decimal column")))
            }
        }
        mysql_type::DATE | mysql_type::NEWDATE => {
            if is_date(&text) {
                Ok(Value::Date(text))
            }
            else {
                Ok(Value::Str(text))
            }
        }
        mysql_type::TIME | mysql_type::TIME2 => {
            if is_time(&text) {
                Ok(Value::Time(text))
            }
            else {
                Ok(Value::Str(text))
            }
        }
        mysql_type::JSON => Ok(Value::Str(text)),
        _ if column.binary() => Ok(Value::Bytes(bytes)),
        _ => Ok(Value::Str(text)),
    }
}

fn is_uuid_column(table: &str, name: &str, projection: Option<&[ProjectField]>, uuid_fields: &HashSet<(String, String)>) -> bool {
    let source =
        projection.and_then(|fields| fields.iter().find(|field| field.name == name)).and_then(|field| field.from.as_deref()).unwrap_or(name);
    uuid_fields.contains(&(table.to_owned(), source.to_owned()))
}

fn pred_to_sql(predicate: &Pred, table: &str, uuid_fields: &HashSet<(String, String)>, bindings: &mut Bindings) -> Result<String> {
    match predicate {
        Pred::FieldBool { field, value } => Ok(format!("`{field}` = {}", bindings.text(Some(if *value { "1" } else { "0" })))),
        Pred::FieldCmp { field, op, literal, kind } => {
            let op = match op {
                CmpOp::Eq => "=",
                CmpOp::Ne => "<>",
                CmpOp::Lt => "<",
                CmpOp::Le => "<=",
                CmpOp::Gt => ">",
                CmpOp::Ge => ">=",
            };
            let placeholder = if uuid_fields.contains(&(table.to_owned(), field.clone())) {
                uuid_literal_binding(literal, *kind, bindings)?
            }
            else {
                literal_to_binding(literal, *kind, bindings)?
            };
            Ok(format!("`{field}` {op} {placeholder}"))
        }
        Pred::StringMatch { field, op, needle } => {
            let placeholder = bindings.text(Some(needle));
            let sql = match op {
                StringMatchOp::Contains => format!("LOCATE(BINARY {placeholder}, BINARY `{field}`) > 0"),
                StringMatchOp::StartsWith => format!("LEFT(BINARY `{field}`, LENGTH({placeholder})) = BINARY {placeholder}"),
                StringMatchOp::EndsWith => format!("RIGHT(BINARY `{field}`, LENGTH({placeholder})) = BINARY {placeholder}"),
            };
            Ok(sql)
        }
        Pred::And(left, right) => {
            Ok(format!("({} AND {})", pred_to_sql(left, table, uuid_fields, bindings)?, pred_to_sql(right, table, uuid_fields, bindings)?))
        }
        Pred::Or(left, right) => {
            Ok(format!("({} OR {})", pred_to_sql(left, table, uuid_fields, bindings)?, pred_to_sql(right, table, uuid_fields, bindings)?))
        }
    }
}

fn filter_to_sql(filter: Option<&Pred>, table: &str, uuid_fields: &HashSet<(String, String)>, bindings: &mut Bindings) -> Result<String> {
    match filter {
        Some(filter) => pred_to_sql(filter, table, uuid_fields, bindings),
        None => Ok("1 = 1".into()),
    }
}

fn project_fields_sql(fields: &[ProjectField]) -> Vec<String> {
    fields
        .iter()
        .map(|field| match &field.from {
            Some(source) if source != &field.name => format!("`{source}` AS `{}`", field.name),
            _ => format!("`{}`", field.name),
        })
        .collect()
}

fn apply_projection(rows: Vec<Row>, projection: Option<&[ProjectField]>) -> Vec<Row> {
    let Some(fields) = projection
    else {
        return rows;
    };
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut mapped = Row::new();
        for field in fields {
            let source = field.from.as_deref().unwrap_or(&field.name);
            if let Some(value) = row.get(source) {
                mapped.insert(field.name.clone(), value.clone());
            }
        }
        out.push(mapped);
    }
    out
}

fn literal_to_value(field: &WriteField) -> Result<Value> {
    let text = &field.literal;
    match field.kind {
        LiteralKind::Null => Ok(Value::Null),
        LiteralKind::Bool => Ok(Value::Bool(is_true(text))),
        LiteralKind::Int => Ok(Value::Int(text.parse::<i64>().map_err(|_| Error::Policy(format!("invalid integer literal `{text}`")))?)),
        LiteralKind::UInt => Ok(Value::UInt(text.parse::<u64>().map_err(|_| Error::Policy(format!("invalid unsigned literal `{text}`")))?)),
        LiteralKind::Float => {
            let value = text.parse::<f64>().map_err(|_| Error::Policy(format!("invalid float literal `{text}`")))?;
            if value.is_finite() { Ok(Value::Float(value)) } else { Err(Error::Policy(format!("float literal `{text}` is not finite"))) }
        }
        LiteralKind::Decimal => Ok(Value::Decimal(text.clone())),
        LiteralKind::Date => Ok(Value::Date(text.clone())),
        LiteralKind::Time => Ok(Value::Time(text.clone())),
        LiteralKind::DateTime => Ok(Value::DateTime(text.clone())),
        LiteralKind::Str => Ok(Value::Str(text.clone())),
        LiteralKind::Bytes => hex_to_bytes(text).map(Value::Bytes).ok_or_else(|| Error::Policy(format!("invalid byte literal `{text}`"))),
        LiteralKind::Vector | LiteralKind::File => Ok(Value::Null),
    }
}

fn literal_to_binding(literal: &str, kind: LiteralKind, bindings: &mut Bindings) -> Result<String> {
    let placeholder = match kind {
        LiteralKind::Null => bindings.text(None),
        LiteralKind::Bool => bindings.text(Some(if is_true(literal) { "1" } else { "0" })),
        LiteralKind::Int
        | LiteralKind::UInt
        | LiteralKind::Float
        | LiteralKind::Decimal
        | LiteralKind::Date
        | LiteralKind::Time
        | LiteralKind::DateTime
        | LiteralKind::Str => bindings.text(Some(literal)),
        LiteralKind::Bytes => {
            bindings.binary(&hex_to_bytes(literal).ok_or_else(|| Error::Policy(format!("invalid byte literal `{literal}`")))?)
        }
        LiteralKind::Vector | LiteralKind::File => bindings.text(None),
    };
    Ok(placeholder)
}

/// Bind a literal against a `BINARY(16)` uuid column.
fn uuid_literal_binding(literal: &str, kind: LiteralKind, bindings: &mut Bindings) -> Result<String> {
    if matches!(kind, LiteralKind::Str) {
        if let Some(bytes) = try_parse_uuid_bytes(literal) {
            return Ok(bindings.binary(&bytes));
        }
    }
    literal_to_binding(literal, kind, bindings)
}

fn write_field_binding(table: &str, field: &WriteField, uuid_fields: &HashSet<(String, String)>, bindings: &mut Bindings) -> Result<String> {
    if uuid_fields.contains(&(table.to_owned(), field.name.clone())) {
        return uuid_literal_binding(&field.literal, field.kind, bindings);
    }
    literal_to_binding(&field.literal, field.kind, bindings)
}

fn value_field_binding(
    table: &str,
    name: &str,
    value: &Value,
    uuid_fields: &HashSet<(String, String)>,
    bindings: &mut Bindings,
) -> Result<String> {
    if uuid_fields.contains(&(table.to_owned(), name.to_owned())) {
        if let Value::Str(text) = value {
            if let Some(bytes) = try_parse_uuid_bytes(text) {
                return Ok(bindings.binary(&bytes));
            }
        }
    }
    value_to_binding(value, bindings)
}

fn value_to_binding(value: &Value, bindings: &mut Bindings) -> Result<String> {
    let placeholder = match value {
        Value::Null => bindings.text(None),
        Value::Bool(value) => bindings.text(Some(if *value { "1" } else { "0" })),
        Value::Int(value) => bindings.text(Some(&value.to_string())),
        Value::UInt(value) => bindings.text(Some(&value.to_string())),
        Value::Float(value) => {
            if !value.is_finite() {
                return Err(Error::Policy("non-finite float cannot be bound to MySQL".into()));
            }
            bindings.text(Some(&value.to_string()))
        }
        Value::Str(value) | Value::Decimal(value) | Value::Date(value) | Value::Time(value) | Value::DateTime(value) => {
            bindings.text(Some(value))
        }
        Value::Bytes(value) => bindings.binary(value),
        Value::Vector(_) | Value::File(_) => bindings.text(None),
        Value::Object(_) => bindings.text(None),
    };
    Ok(placeholder)
}

fn is_true(text: &str) -> bool {
    matches!(text, "true" | "TRUE" | "1")
}

/// Accumulates parameter values and hands out `?` placeholders.
#[derive(Default)]
struct Bindings {
    params: Vec<OwnedParam>,
}

enum OwnedParam {
    Text(Option<String>),
    Binary(Vec<u8>),
}

impl Bindings {
    fn text(&mut self, value: Option<&str>) -> String {
        self.params.push(OwnedParam::Text(value.map(str::to_owned)));
        "?".into()
    }

    fn binary(&mut self, value: &[u8]) -> String {
        self.params.push(OwnedParam::Binary(value.to_vec()));
        "?".into()
    }

    fn as_params(&self) -> Vec<Param<'_>> {
        self.params
            .iter()
            .map(|param| match param {
                OwnedParam::Text(value) => Param::Text(value.as_deref()),
                OwnedParam::Binary(value) => Param::Binary(value),
            })
            .collect()
    }
}
