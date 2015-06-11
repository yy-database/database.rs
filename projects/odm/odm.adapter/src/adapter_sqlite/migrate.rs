//! Managed Push: logical plan + private apply.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use odm_types::{LogicalChange, LogicalMigrationPlan, ObservedCatalog};
use vos::{CanonicalType, FieldContract, SchemaProjection, TypeContract, TypeContractKind};

use crate::{Error, Result};

use super::provider::RusqliteProvider;

/// Report after applying a managed push.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushReport {
    /// Plan id applied.
    pub plan_id: String,
    /// Tables created.
    pub created_tables: Vec<String>,
}

/// Build a non-destructive plan: create missing tables + add missing fields.
pub fn plan_push(document: &SchemaProjection, observed: &ObservedCatalog) -> Result<LogicalMigrationPlan> {
    let target = fingerprint_document(document);
    let mut changes = Vec::new();
    for table in document.types.iter().filter(|item| item.kind == TypeContractKind::Table) {
        let name = table_name(table);
        match observed.table(name) {
            None => {
                changes.push(LogicalChange::CreateTable { vos_table: name.to_owned() });
            }
            Some(obs) => {
                for field in &table.fields {
                    if !obs.columns.iter().any(|column| column.name == field.canonical_name) {
                        changes.push(LogicalChange::AddField { vos_table: name.to_owned(), vos_field: field.canonical_name.clone() });
                    }
                }
            }
        }
    }
    Ok(LogicalMigrationPlan {
        id: format!("push-{target}"),
        parent_fingerprint: fingerprint_catalog(observed),
        target_fingerprint: target,
        changes,
        destructive: false,
    })
}

/// Apply a reviewed logical plan by emitting private SQLite DDL.
pub fn apply_push(provider: &RusqliteProvider, plan: &LogicalMigrationPlan, document: &SchemaProjection) -> Result<PushReport> {
    if plan.destructive {
        return Err(Error::Policy("refusing to apply destructive plan without explicit policy".into()));
    }
    let mut created = Vec::new();
    provider.begin_immediate()?;
    let applied = (|| {
        for change in &plan.changes {
            match change {
                LogicalChange::CreateTable { vos_table } => {
                    let table = find_table(document, vos_table)?;
                    let ddl = create_table_sql(table)?;
                    provider.execute_batch(&ddl)?;
                    created.push(vos_table.clone());
                }
                LogicalChange::AddField { vos_table, vos_field } => {
                    let table = find_table(document, vos_table)?;
                    let field = table
                        .fields
                        .iter()
                        .find(|field| field.canonical_name == *vos_field)
                        .ok_or_else(|| Error::Policy(format!("plan references unknown VOS field `{vos_table}.{vos_field}`")))?;
                    if is_primary(field) {
                        return Err(Error::Policy(format!("refusing AddField for primary key `{vos_table}.{vos_field}` — recreate table")));
                    }
                    let ddl = add_field_sql(vos_table, field)?;
                    provider.execute_batch(&ddl)?;
                }
            }
        }
        provider.commit()?;
        Ok(PushReport { plan_id: plan.id.clone(), created_tables: created })
    })();
    if applied.is_err() {
        let _ = provider.rollback();
    }
    applied
}

fn find_table<'a>(document: &'a SchemaProjection, name: &str) -> Result<&'a TypeContract> {
    document
        .types
        .iter()
        .find(|item| item.kind == TypeContractKind::Table && table_name(item) == name)
        .ok_or_else(|| Error::Policy(format!("plan references unknown VOS table `{name}`")))
}

fn create_table_sql(table: &TypeContract) -> Result<String> {
    let name = table_name(table);
    let mut cols = Vec::new();
    let mut pks = Vec::new();
    for field in &table.fields {
        let (sql_ty, not_null) = map_field_type(field)?;
        let mut piece = format!("\"{}\" {sql_ty}", field.canonical_name);
        if not_null {
            piece.push_str(" NOT NULL");
        }
        if is_primary(field) {
            pks.push(format!("\"{}\"", field.canonical_name));
        }
        cols.push(piece);
    }
    if pks.is_empty() {
        return Err(Error::Policy(format!("table `{name}` has no primary key --?cannot push")));
    }
    cols.push(format!("PRIMARY KEY ({})", pks.join(", ")));
    Ok(format!("CREATE TABLE IF NOT EXISTS \"{name}\" ({});", cols.join(", ")))
}

fn add_field_sql(table: &str, field: &FieldContract) -> Result<String> {
    let (sql_ty, not_null) = map_field_type(field)?;
    let mut piece = format!("\"{}\" {sql_ty}", field.canonical_name);
    if not_null {
        piece.push_str(" NOT NULL");
        piece.push_str(&format!(" DEFAULT {}", default_literal(field)?));
    }
    else {
        piece.push_str(" NULL");
    }
    Ok(format!("ALTER TABLE \"{table}\" ADD COLUMN {piece};"))
}

fn default_literal(field: &FieldContract) -> Result<String> {
    let (inner, _) = strip_optional(&field.canonical_type);
    match inner {
        CanonicalType::Named(path) => match path.last().map(String::as_str).unwrap_or_default() {
            "bool" | "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" | "isize" | "usize" | "f32" | "f64" => Ok("0".into()),
            "utf8" | "utf16" | "uuid" | "decimal" | "d128" | "date" | "time" | "datetime" | "datetimeUtc" => Ok("''".into()),
            "bytes" => Ok("x''".into()),
            other => Err(Error::Policy(format!("no AddField default for VOS type {other}"))),
        },
        other => Err(Error::Policy(format!("no AddField default for VOS type {other:?}"))),
    }
}

fn map_field_type(field: &FieldContract) -> Result<(String, bool)> {
    let (inner, optional) = strip_optional(&field.canonical_type);
    let sql = match inner {
        CanonicalType::Named(path) => match path.last().map(String::as_str).unwrap_or_default() {
            "bool" => "INTEGER",
            "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" | "isize" | "usize" => "INTEGER",
            "f32" | "f64" => "REAL",
            "utf8" | "utf16" | "uuid" | "decimal" | "d128" | "date" | "time" | "datetime" | "datetimeUtc" => "TEXT",
            "bytes" => "BLOB",
            other => {
                return Err(Error::Policy(format!("unsupported VOS type for SQLite push: {other}")));
            }
        },
        other => {
            return Err(Error::Policy(format!("unsupported VOS type for SQLite push: {other:?}")));
        }
    };
    Ok((sql.into(), !optional))
}

fn strip_optional(ty: &CanonicalType) -> (&CanonicalType, bool) {
    match ty {
        CanonicalType::Optional(inner) => (inner.as_ref(), true),
        other => (other, false),
    }
}

fn is_primary(field: &FieldContract) -> bool {
    field.attributes.iter().any(|attribute| attribute.name == "primary")
}

fn table_name(table: &TypeContract) -> &str {
    table.canonical_path.last().map(String::as_str).unwrap_or_default()
}

fn fingerprint_document(document: &SchemaProjection) -> String {
    let mut h = DefaultHasher::new();
    format!("{document:?}").hash(&mut h);
    format!("{:x}", h.finish())
}

fn fingerprint_catalog(catalog: &ObservedCatalog) -> String {
    let mut h = DefaultHasher::new();
    format!("{catalog:?}").hash(&mut h);
    format!("{:x}", h.finish())
}
