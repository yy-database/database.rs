//! PostgreSQL catalog inspect, adopt, and drift over the gateway wire client.

use database_client::pgsql::{Cell, Session};
use odm_types::{DriftReport, FieldMapping, MappingManifest, MappingQuality, ObservedCatalog, ObservedColumn, ObservedTable, TableMapping};
use vos::{CanonicalType, CanonicalTypeArgument, SchemaProjection, TypeContract, TypeContractKind};

use crate::{Error, Result};

use super::{ADAPTER_VERSION, BACKEND_ID};

pub(crate) fn inspect_catalog(session: &mut Session) -> Result<ObservedCatalog> {
    let result = session
        .query(
            "SELECT table_name
             FROM information_schema.tables
             WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
             ORDER BY table_name",
        )
        .map_err(Error::Wire)?;
    let mut tables = Vec::new();
    for row in result.rows {
        let name = cell_text(row.first()).unwrap_or_default();
        if name.is_empty() || super::outbox::is_meta_table(&name) {
            continue;
        }
        let columns = inspect_table(session, &name)?;
        tables.push(ObservedTable { name, columns });
    }
    Ok(ObservedCatalog { backend_id: BACKEND_ID.into(), tables })
}

fn inspect_table(session: &mut Session, table: &str) -> Result<Vec<ObservedColumn>> {
    let sql = format!(
        "SELECT column_name, data_type, is_nullable, CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.key_column_usage k
            JOIN information_schema.table_constraints t
              ON k.constraint_name = t.constraint_name
             AND k.table_schema = t.table_schema
           WHERE t.constraint_type = 'PRIMARY KEY'
             AND k.table_schema = current_schema()
             AND k.table_name = '{table}'
             AND k.column_name = c.column_name
         ) THEN 'PRI' ELSE '' END AS column_key
         FROM information_schema.columns c
         WHERE table_schema = current_schema() AND table_name = '{table}'
         ORDER BY ordinal_position"
    );
    let result = session.query(&sql).map_err(Error::Wire)?;
    Ok(result
        .rows
        .into_iter()
        .map(|row| ObservedColumn {
            name: cell_text(row.first()).unwrap_or_default(),
            type_name: cell_text(row.get(1)).unwrap_or_default(),
            nullable: cell_text(row.get(2)).unwrap_or_default().eq_ignore_ascii_case("YES"),
            primary_key: cell_text(row.get(3)).unwrap_or_default().eq_ignore_ascii_case("PRI"),
        })
        .collect())
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    match cell? {
        Cell::Null => None,
        Cell::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
    }
}

/// Map an observed catalog onto a VOS projection.
pub fn adopt_plan(document: &SchemaProjection, catalog: &ObservedCatalog) -> MappingManifest {
    let mut tables = Vec::new();
    for table in document.types.iter().filter(|item| item.kind == TypeContractKind::Table) {
        let name = table_name(table);
        let Some(observed) = catalog.table(name)
        else {
            tables.push(TableMapping {
                vos_table: name.to_owned(),
                physical_table: name.to_owned(),
                fields: Vec::new(),
                blockers: vec![format!("physical table `{name}` not found in observed catalog")],
            });
            continue;
        };
        let mut fields = Vec::new();
        let mut blockers = Vec::new();
        for field in &table.fields {
            let vos_type = type_label(&field.canonical_type);
            if let Some(column) = observed.columns.iter().find(|column| column.name == field.canonical_name) {
                let quality = match classify_type(&column.type_name) {
                    Some(_) => MappingQuality::Exact,
                    None => MappingQuality::LossyBlocked,
                };
                if quality == MappingQuality::LossyBlocked {
                    blockers.push(format!(
                        "field `{}`.`{}`: cannot map VOS `{vos_type}` to physical `{}` without waiver",
                        name, field.canonical_name, column.type_name
                    ));
                }
                fields.push(FieldMapping {
                    vos_field: field.canonical_name.clone(),
                    physical_column: column.name.clone(),
                    vos_type,
                    physical_type: column.type_name.clone(),
                    quality,
                    note: None,
                });
            }
            else {
                blockers.push(format!("VOS field `{}`.`{}` has no physical column", name, field.canonical_name));
            }
        }
        tables.push(TableMapping { vos_table: name.to_owned(), physical_table: observed.name.clone(), fields, blockers });
    }
    MappingManifest { adapter_id: BACKEND_ID.into(), adapter_version: ADAPTER_VERSION.into(), tables }
}

/// Drift between a VOS projection and the observed catalog.
pub fn drift_report(document: &SchemaProjection, catalog: &ObservedCatalog) -> DriftReport {
    let mut missing_physical_tables = Vec::new();
    let mut field_mismatches = Vec::new();
    for table in document.types.iter().filter(|item| item.kind == TypeContractKind::Table) {
        let name = table_name(table);
        match catalog.table(name) {
            None => missing_physical_tables.push(name.to_owned()),
            Some(observed) => {
                for field in &table.fields {
                    if !observed.columns.iter().any(|column| column.name == field.canonical_name) {
                        field_mismatches.push(format!("{name}.{} missing physically", field.canonical_name));
                    }
                }
            }
        }
    }
    DriftReport { missing_physical_tables, extra_physical_tables: Vec::new(), field_mismatches }
}

/// Classify a PostgreSQL type name into a coarse VOS canonical type label.
pub fn classify_type(type_name: &str) -> Option<&'static str> {
    match type_name.to_ascii_lowercase().as_str() {
        "integer" | "int4" | "bigint" | "int8" | "smallint" | "int2" => Some("integer"),
        "real" | "float4" | "double precision" | "float8" | "numeric" | "decimal" => Some("number"),
        "boolean" | "bool" => Some("boolean"),
        "text" | "varchar" | "character varying" | "char" | "character" | "uuid" | "json" | "jsonb" => Some("string"),
        "bytea" => Some("bytes"),
        "date" | "time" | "timestamp" | "timestamptz" | "timestamp with time zone" | "timestamp without time zone" => Some("temporal"),
        _ => None,
    }
}

fn table_name(table: &TypeContract) -> &str {
    table.canonical_path.last().map(String::as_str).unwrap_or_default()
}

#[allow(dead_code)]
fn type_label(ty: &CanonicalType) -> String {
    match ty {
        CanonicalType::Named(path) => path.last().cloned().unwrap_or_default(),
        CanonicalType::Optional(inner) => format!("{}?", type_label(inner)),
        CanonicalType::List(inner) => format!("[{}]", type_label(inner)),
        CanonicalType::Reference(inner) => format!("&{}", type_label(inner)),
        CanonicalType::Generic { path, arguments } => {
            let arguments = arguments
                .iter()
                .map(|argument| match argument {
                    CanonicalTypeArgument::Type(value) => type_label(value),
                    CanonicalTypeArgument::Literal(value) => value.clone(),
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}<{arguments}>", path.last().cloned().unwrap_or_default())
        }
    }
}
