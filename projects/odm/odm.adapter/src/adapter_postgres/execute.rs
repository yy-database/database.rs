//! Physical-plan execution over the PostgreSQL wire client.
//!
//! SQL stays private: plans are lowered to parameterised statements and text
//! result cells are decoded back through the type OIDs the server returned.

use database_client::pgsql::{Cell, Param, TypedQueryResult};
use database_query::ir::{CmpOp, LiteralKind, PhysicalOp, PhysicalPlan, Pred, ProjectField, StringMatchOp, WriteField};
use odm_types::{is_date, is_decimal, is_time, is_utc_datetime, hex_to_bytes, Row, RowWrite, Value};

use crate::{Error, Result};

use super::Session;

// PostgreSQL type OIDs used by the decoder.
const OID_BOOL: u32 = 16;
const OID_BYTEA: u32 = 17;
const OID_INT8: u32 = 20;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_OID: u32 = 26;
const OID_FLOAT4: u32 = 700;
const OID_FLOAT8: u32 = 701;
const OID_NUMERIC: u32 = 1700;
const OID_DATE: u32 = 1082;
const OID_TIME: u32 = 1083;
const OID_TIMESTAMP: u32 = 1114;
const OID_TIMESTAMPTZ: u32 = 1184;

pub(crate) fn execute_plan(session: &mut Session, plan: &PhysicalPlan) -> Result<Vec<Row>> {
    if plan.nodes.iter().any(|node| matches!(node.op, PhysicalOp::Insert { .. } | PhysicalOp::Patch { .. } | PhysicalOp::Delete { .. })) {
        execute_write_plan(session, plan)
    }
    else {
        execute_read_plan(session, plan)
    }
}

fn execute_read_plan(session: &mut Session, plan: &PhysicalPlan) -> Result<Vec<Row>> {
    let mut table: Option<String> = None;
    let mut where_sql: Option<String> = None;
    let mut bindings = Bindings::default();
    let mut order: Option<String> = None;
    let mut limit: Option<u64> = None;
    let mut offset: Option<u64> = None;
    let mut projection: Option<Vec<String>> = None;

    for node in &plan.nodes {
        match &node.op {
            PhysicalOp::Scan { table: name } => table = Some(name.clone()),
            PhysicalOp::Filter { predicate } => where_sql = Some(pred_to_sql(predicate, &mut bindings)?),
            PhysicalOp::Project { fields } => projection = Some(project_fields_sql(fields)),
            PhysicalOp::Sort { keys } => {
                let parts: Vec<String> =
                    keys.iter().map(|key| format!("\"{}\" {}", key.field, if key.ascending { "ASC" } else { "DESC" })).collect();
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

    let table = table.ok_or_else(|| Error::Policy("plan missing Scan".into()))?;
    let select = projection.map(|fields| fields.join(", ")).unwrap_or_else(|| "*".into());
    let mut sql = format!("SELECT {select} FROM \"{table}\"");
    if let Some(where_sql) = &where_sql {
        sql.push_str(" WHERE ");
        sql.push_str(where_sql);
    }
    if let Some(order) = &order {
        sql.push_str(" ORDER BY ");
        sql.push_str(order);
    }
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }

    let params = bindings.as_params();
    let result = session.query_with_params(&sql, &params).map_err(Error::Wire)?;
    result_to_rows(result)
}

fn execute_write_plan(session: &mut Session, plan: &PhysicalPlan) -> Result<Vec<Row>> {
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
        let where_sql = filter_to_sql(filter.as_ref(), &mut bindings)?;
        let sql = format!("DELETE FROM \"{table}\" WHERE {where_sql}");
        let params = bindings.as_params();
        session.execute_with_params(&sql, &params).map_err(Error::Wire)?;
        return Ok(Vec::new());
    }

    if let Some((table, fields)) = insert {
        let mut bindings = Bindings::default();
        let cols: Vec<String> = fields.iter().map(|field| format!("\"{}\"", field.name)).collect();
        let placeholders: Vec<String> =
            fields.iter().map(|field| literal_to_binding(&field.literal, field.kind, &mut bindings)).collect::<Result<Vec<_>>>()?;
        let sql = format!("INSERT INTO \"{table}\" ({}) VALUES ({}) RETURNING *", cols.join(", "), placeholders.join(", "));
        let params = bindings.as_params();
        let result = session.query_with_params(&sql, &params).map_err(Error::Wire)?;
        let rows = result_to_rows(result)?;
        return Ok(apply_projection(rows, projection.as_deref()));
    }

    if let Some((table, filter, fields)) = patch {
        let mut bindings = Bindings::default();
        let sets: Vec<String> = fields
            .iter()
            .map(|field| Ok(format!("\"{}\" = {}", field.name, literal_to_binding(&field.literal, field.kind, &mut bindings)?)))
            .collect::<Result<Vec<_>>>()?;
        let where_sql = filter_to_sql(filter.as_ref(), &mut bindings)?;
        let sql = format!("UPDATE \"{table}\" SET {} WHERE {where_sql} RETURNING *", sets.join(", "));
        let params = bindings.as_params();
        let result = session.query_with_params(&sql, &params).map_err(Error::Wire)?;
        let rows = result_to_rows(result)?;
        return Ok(apply_projection(rows, projection.as_deref()));
    }

    Ok(Vec::new())
}

pub(crate) fn insert_row(session: &mut Session, write: &RowWrite) -> Result<()> {
    let mut bindings = Bindings::default();
    let cols: Vec<String> = write.fields.keys().map(|name| format!("\"{name}\"")).collect();
    let placeholders: Vec<String> =
        write.fields.values().map(|value| value_to_binding(value, &mut bindings)).collect::<Result<Vec<_>>>()?;
    let sql = format!("INSERT INTO \"{}\" ({}) VALUES ({})", write.table, cols.join(", "), placeholders.join(", "));
    let params = bindings.as_params();
    session.execute_with_params(&sql, &params).map_err(Error::Wire)?;
    Ok(())
}

pub(crate) fn update_row(session: &mut Session, write: &RowWrite) -> Result<u64> {
    let key = write.fields.get(&write.primary_key).ok_or_else(|| Error::Policy("update missing primary key value".into()))?;
    let mut bindings = Bindings::default();
    let sets: Vec<String> = write
        .fields
        .iter()
        .filter(|(name, _)| *name != &write.primary_key)
        .map(|(name, value)| Ok(format!("\"{name}\" = {}", value_to_binding(value, &mut bindings)?)))
        .collect::<Result<Vec<_>>>()?;
    if sets.is_empty() {
        return Ok(0);
    }
    let key_placeholder = value_to_binding(key, &mut bindings)?;
    let sql = format!("UPDATE \"{}\" SET {} WHERE \"{}\" = {key_placeholder}", write.table, sets.join(", "), write.primary_key);
    let params = bindings.as_params();
    session.execute_with_params(&sql, &params).map_err(Error::Wire)
}

pub(crate) fn delete_row(session: &mut Session, table: &str, primary_key: &str, key: &Value) -> Result<u64> {
    let mut bindings = Bindings::default();
    let placeholder = value_to_binding(key, &mut bindings)?;
    let sql = format!("DELETE FROM \"{table}\" WHERE \"{primary_key}\" = {placeholder}");
    let params = bindings.as_params();
    session.execute_with_params(&sql, &params).map_err(Error::Wire)
}

fn result_to_rows(result: TypedQueryResult) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for values in result.rows {
        let mut out = Row::new();
        for (index, column) in result.columns.iter().enumerate() {
            let cell = values.get(index).cloned().unwrap_or(Cell::Null);
            out.insert(column.name.clone(), decode_cell(cell, column.type_oid)?);
        }
        rows.push(out);
    }
    Ok(rows)
}

fn decode_cell(cell: Cell, type_oid: u32) -> Result<Value> {
    let Cell::Bytes(bytes) = cell
    else {
        return Ok(Value::Null);
    };
    let text = String::from_utf8_lossy(&bytes);
    match type_oid {
        OID_BOOL => match text.as_ref() {
            "t" | "true" | "1" => Ok(Value::Bool(true)),
            "f" | "false" | "0" => Ok(Value::Bool(false)),
            other => Err(Error::Policy(format!("PostgreSQL returned `{other}` for a boolean column"))),
        },
        OID_INT2 | OID_INT4 | OID_INT8 | OID_OID => {
            let value = text.parse::<i64>().map_err(|_| Error::Policy(format!("PostgreSQL returned `{text}` for an integer column")))?;
            Ok(Value::Int(value))
        }
        OID_FLOAT4 | OID_FLOAT8 => {
            let value = text.parse::<f64>().map_err(|_| Error::Policy(format!("PostgreSQL returned `{text}` for a float column")))?;
            if value.is_finite() { Ok(Value::Float(value)) } else { Err(Error::Policy("PostgreSQL returned a non-finite float".into())) }
        }
        OID_NUMERIC => {
            if is_decimal(&text) { Ok(Value::Decimal(text.into_owned())) } else { Err(Error::Policy(format!("PostgreSQL returned `{text}` for a numeric column"))) }
        }
        OID_BYTEA => match text.strip_prefix("\\x").and_then(hex_to_bytes) {
            Some(octets) => Ok(Value::Bytes(octets)),
            None => Err(Error::Policy("PostgreSQL returned a bytea cell outside the `\\x` hex form".into())),
        },
        OID_DATE => Ok(if is_date(&text) { Value::Date(text.into_owned()) } else { Value::Str(text.into_owned()) }),
        OID_TIME => Ok(if is_time(&text) { Value::Time(text.into_owned()) } else { Value::Str(text.into_owned()) }),
        OID_TIMESTAMP | OID_TIMESTAMPTZ => {
            Ok(if is_utc_datetime(&text) { Value::DateTime(text.into_owned()) } else { Value::Str(text.into_owned()) })
        }
        _ => Ok(Value::Str(text.into_owned())),
    }
}

fn project_fields_sql(fields: &[ProjectField]) -> Vec<String> {
    fields
        .iter()
        .map(|field| {
            let source = field.from.as_deref().unwrap_or(field.name.as_str());
            if source == field.name { format!("\"{source}\"") } else { format!("\"{source}\" AS \"{}\"", field.name) }
        })
        .collect()
}

fn apply_projection(rows: Vec<Row>, fields: Option<&[ProjectField]>) -> Vec<Row> {
    let Some(fields) = fields
    else {
        return rows;
    };
    rows.into_iter()
        .map(|row| {
            let mut out = Row::new();
            for field in fields {
                let source = field.from.as_deref().unwrap_or(field.name.as_str());
                if let Some(value) = row.get(source) {
                    out.insert(field.name.clone(), value.clone());
                }
            }
            out
        })
        .collect()
}

fn filter_to_sql(filter: Option<&Pred>, bindings: &mut Bindings) -> Result<String> {
    match filter {
        Some(predicate) => pred_to_sql(predicate, bindings),
        None => Ok("TRUE".into()),
    }
}

fn pred_to_sql(predicate: &Pred, bindings: &mut Bindings) -> Result<String> {
    match predicate {
        Pred::FieldBool { field, value } => Ok(format!("\"{field}\" = {}", bindings.text(Some(if *value { "true" } else { "false" })))),
        Pred::FieldCmp { field, op, literal, kind } => {
            let op_sql = match op {
                CmpOp::Eq => "=",
                CmpOp::Ne => "!=",
                CmpOp::Lt => "<",
                CmpOp::Le => "<=",
                CmpOp::Gt => ">",
                CmpOp::Ge => ">=",
            };
            let placeholder = literal_to_binding(literal, *kind, bindings)?;
            Ok(format!("\"{field}\" {op_sql} {placeholder}"))
        }
        Pred::StringMatch { field, op, needle } => {
            let placeholder = bindings.text(Some(needle));
            Ok(match op {
                StringMatchOp::Contains => format!("strpos(\"{field}\"::text, {placeholder}::text) > 0"),
                StringMatchOp::StartsWith => format!("left(\"{field}\"::text, length({placeholder}::text)) = {placeholder}::text"),
                StringMatchOp::EndsWith => format!("right(\"{field}\"::text, length({placeholder}::text)) = {placeholder}::text"),
            })
        }
        Pred::And(left, right) => {
            let left = pred_to_sql(left, bindings)?;
            let right = pred_to_sql(right, bindings)?;
            Ok(format!("({left}) AND ({right})"))
        }
        Pred::Or(left, right) => {
            let left = pred_to_sql(left, bindings)?;
            let right = pred_to_sql(right, bindings)?;
            Ok(format!("({left}) OR ({right})"))
        }
    }
}

fn literal_to_binding(text: &str, kind: LiteralKind, bindings: &mut Bindings) -> Result<String> {
    match kind {
        LiteralKind::Null => Ok(bindings.text(None)),
        LiteralKind::Bool => match text {
            "true" => Ok(bindings.text(Some("true"))),
            "false" => Ok(bindings.text(Some("false"))),
            other => Err(Error::Policy(format!("`{other}` is not a boolean literal"))),
        },
        LiteralKind::Int | LiteralKind::UInt | LiteralKind::Decimal | LiteralKind::Date | LiteralKind::Time | LiteralKind::DateTime
        | LiteralKind::Str => Ok(bindings.text(Some(text))),
        LiteralKind::Float => match text.parse::<f64>() {
            Ok(value) if value.is_finite() => Ok(bindings.text(Some(text))),
            _ => Err(Error::Policy(format!("`{text}` is not a finite float literal"))),
        },
        LiteralKind::Bytes => match hex_to_bytes(text) {
            Some(octets) => Ok(bindings.binary(&octets)),
            None => Err(Error::Policy(format!("`{text}` is not canonical hex bytes"))),
        },
        // Payload literals never reach a relational predicate (the typed plan
        // rejects them at plan time); they bind as SQL NULL.
        LiteralKind::Vector | LiteralKind::File => Ok(bindings.text(None)),
    }
}

fn value_to_binding(value: &Value, bindings: &mut Bindings) -> Result<String> {
    match value {
        Value::Null => Ok(bindings.text(None)),
        Value::Bool(value) => Ok(bindings.text(Some(if *value { "true" } else { "false" }))),
        Value::Int(value) => Ok(bindings.text(Some(&value.to_string()))),
        Value::UInt(value) => Ok(bindings.text(Some(&value.to_string()))),
        Value::Float(value) => {
            if !value.is_finite() {
                return Err(Error::Policy("VOS `f64` values must be finite".into()));
            }
            Ok(bindings.text(Some(&value.to_string())))
        }
        Value::Str(text) | Value::Decimal(text) | Value::Date(text) | Value::Time(text) | Value::DateTime(text) => {
            Ok(bindings.text(Some(text)))
        }
        Value::Bytes(octets) => Ok(bindings.binary(octets)),
        Value::Vector(_) | Value::File(_) => {
            Err(Error::Policy("VOS `vector`/`file` payloads need a datasource that binds typed values (yydb)".into()))
        }
        Value::Object(_) => Ok(bindings.text(None)),
    }
}

/// One owned parameter plus its wire format (`0` text, `1` binary).
enum OwnedParam {
    Text(Option<String>),
    Binary(Vec<u8>),
}

/// Parameter list built before the statement is sent, because [`Param`]
/// borrows its payload and the placeholders are numbered in insertion order.
#[derive(Default)]
struct Bindings {
    values: Vec<OwnedParam>,
}

impl Bindings {
    fn text(&mut self, value: Option<&str>) -> String {
        self.values.push(OwnedParam::Text(value.map(str::to_owned)));
        self.placeholder()
    }

    fn binary(&mut self, octets: &[u8]) -> String {
        self.values.push(OwnedParam::Binary(octets.to_vec()));
        self.placeholder()
    }

    fn placeholder(&self) -> String {
        format!("${}", self.values.len())
    }

    fn as_params(&self) -> Vec<Param<'_>> {
        self.values
            .iter()
            .map(|value| match value {
                OwnedParam::Text(text) => Param::text(text.as_deref()),
                OwnedParam::Binary(octets) => Param::binary(octets),
            })
            .collect()
    }
}
