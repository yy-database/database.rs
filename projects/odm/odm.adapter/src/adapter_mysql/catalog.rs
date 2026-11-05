//! MySQL catalog inspect, adopt, and drift over the gateway wire client.

use database_client::mysql::{Cell, Session};
use odm_types::{DriftReport, FieldMapping, MappingManifest, MappingQuality, ObservedCatalog, ObservedColumn, ObservedTable, TableMapping};
use vos::{CanonicalType, CanonicalTypeArgument, SchemaProjection, TypeContract, TypeContractKind};

use crate::{Error, Result};

use super::{ADAPTER_VERSION, BACKEND_ID};

pub(crate) fn inspect_catalog(session: &mut Session) -> Result<ObservedCatalog> {
    let result = session
        .query(
            "SELECT TABLE_NAME
             FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
             ORDER BY TABLE_NAME",
        )
        .map_err(Error::Wire)?;
    let mut tables = Vec::new();
    for row in result.rows {
        let name = cell_text(row.first()).unwrap_or_default();
        if name.is_empty() {
            continue;
        }
        let columns = inspect_table(session, &name)?;
        tables.push(ObservedTable { name, columns });
    }
    Ok(ObservedCatalog { backend_id: BACKEND_ID.into(), tables })
}

fn inspect_table(session: &mut Session, table: &str) -> Result<Vec<ObservedColumn>> {
    let sql = format!(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
         FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}'
         ORDER BY ORDINAL_POSITION"
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

/// Build a reviewable adopt mapping; never invents VOS business names from SQL.
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

        let mut blockers = Vec::new();
        let pk_count = observed.columns.iter().filter(|c| c.primary_key).count();
        if pk_count == 0 {
            blockers.push(format!("table `{}` has no primary key --?adopt blocked", observed.name));
        }

        let mut fields = Vec::new();
        for field in &table.fields {
            let vos_type = type_label(&field.canonical_type);
            match observed.columns.iter().find(|c| c.name == field.canonical_name) {
                Some(col) => {
                    let (quality, note) = classify_type(&vos_type, &col.type_name);
                    if quality == MappingQuality::LossyBlocked {
                        blockers.push(format!(
                            "field `{}`.`{}`: cannot map VOS `{vos_type}` to physical `{}` without waiver",
                            name, field.canonical_name, col.type_name
                        ));
                    }
                    fields.push(FieldMapping {
                        vos_field: field.canonical_name.clone(),
                        physical_column: col.name.clone(),
                        vos_type,
                        physical_type: col.type_name.clone(),
                        quality,
                        note,
                    });
                }
                None => {
                    blockers.push(format!("VOS field `{}`.`{}` has no physical column --?will not auto-invent", name, field.canonical_name));
                }
            }
        }

        tables.push(TableMapping { vos_table: name.to_owned(), physical_table: observed.name.clone(), fields, blockers });
    }

    MappingManifest { adapter_id: BACKEND_ID.into(), adapter_version: ADAPTER_VERSION.into(), tables }
}

pub(crate) fn drift_report(document: &SchemaProjection, catalog: &ObservedCatalog) -> DriftReport {
    let vos_tables: Vec<String> =
        document.types.iter().filter(|item| item.kind == TypeContractKind::Table).map(|item| table_name(item).to_owned()).collect();
    let physical: Vec<String> = catalog.tables.iter().map(|t| t.name.clone()).collect();

    let missing_physical_tables = vos_tables.iter().filter(|t| !physical.iter().any(|p| p == *t)).cloned().collect();
    let extra_physical_tables = physical.iter().filter(|p| !vos_tables.iter().any(|t| t == *p)).cloned().collect();

    let mut field_mismatches = Vec::new();
    for table in document.types.iter().filter(|item| item.kind == TypeContractKind::Table) {
        let name = table_name(table);
        let Some(obs) = catalog.table(name)
        else {
            continue;
        };
        for field in &table.fields {
            match obs.columns.iter().find(|c| c.name == field.canonical_name) {
                None => field_mismatches.push(format!("{name}.{} missing physically", field.canonical_name)),
                Some(col) => {
                    let vos_type = type_label(&field.canonical_type);
                    if classify_type(&vos_type, &col.type_name).0 == MappingQuality::LossyBlocked {
                        field_mismatches.push(format!("{name}.{} type drift VOS `{vos_type}` vs `{}`", field.canonical_name, col.type_name));
                    }
                }
            }
        }
    }

    DriftReport { missing_physical_tables, extra_physical_tables, field_mismatches }
}

fn table_name(table: &TypeContract) -> &str {
    table.canonical_path.last().map(String::as_str).unwrap_or_default()
}

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

/// MySQL-specific type classification (`information_schema.DATA_TYPE`).
pub fn classify_type(vos_type: &str, physical: &str) -> (MappingQuality, Option<String>) {
    let p = physical.to_ascii_lowercase();
    let v = vos_type.to_ascii_lowercase();
    if (v.contains("utf8") || v.contains("string")) && (p.contains("char") || p.contains("text")) {
        return (MappingQuality::Exact, None);
    }
    if v.contains("bool") && (p == "tinyint" || p == "bool" || p == "boolean") {
        return (MappingQuality::Compatible, Some("bool stored as TINYINT(1)".into()));
    }
    if (v.contains("i64") || v.contains("u64") || v.contains("i32") || v.contains("u32")) && (p.contains("int")) {
        return (MappingQuality::Exact, None);
    }
    if v.contains("uuid") && p == "binary" {
        return (MappingQuality::Exact, Some("uuid stored as BINARY(16)".into()));
    }
    if v.contains("uuid") && p.contains("char") {
        return (MappingQuality::Compatible, Some("uuid stored as CHAR(36)".into()));
    }
    if (v.contains("f64") || v.contains("f32")) && (p == "double" || p == "float" || p == "real" || p == "decimal") {
        return (MappingQuality::Exact, None);
    }
    if v.contains("bytes") && (p.contains("blob") || p == "binary" || p == "varbinary") {
        return (MappingQuality::Exact, None);
    }
    if v.contains("vector") {
        return (MappingQuality::LossyBlocked, Some("complex payload types need an explicit waiver".into()));
    }
    (MappingQuality::LossyBlocked, Some(format!("no safe default map for `{vos_type}` ???`{physical}`")))
}
