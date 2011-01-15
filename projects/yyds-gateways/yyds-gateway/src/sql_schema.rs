//! Local SQL schema registry persisted for the gateway DDL subset.
//!
//! The gateways start from an empty schema unless the host points them at a
//! catalog. Clients then describe tables with the DDL subset in [`crate::sql_ddl`]
//! and the registry keeps the resulting identities in memory, optionally saving
//! them as a strict `yyds-sql-schema/v0` document so a restart sees the same
//! schema. Dropping a table only removes its metadata: rows already written to
//! the key/value store are retained on purpose.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use serde::{Deserialize, Serialize};

use crate::{
    catalog_bind::{SqlCatalog, SqlCatalogField, SqlCatalogTable, SqlFieldType},
    sql::{SqlErrorKind, SqlFrontendError},
    sql_ddl::SqlDdlStatement,
};

/// Contract tag written into every persisted schema document.
const CONTRACT_TAG: &str = "yyds-sql-schema/v0";

/// Declared octet capacity MySQL uses for an unqualified `BLOB`.
const MYSQL_BLOB_CAPACITY: u32 = 65_535;

/// Result of one applied DDL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlDdlOutcome {
    /// `CREATE TABLE`, with `created = false` when `IF NOT EXISTS` matched.
    CreateTable {
        /// Whether the table was actually created.
        created: bool,
    },
    /// `ALTER TABLE ADD COLUMN`, with `added = false` when `IF NOT EXISTS` matched.
    AlterTableAddColumn {
        /// Whether the column was actually added.
        added: bool,
    },
    /// `DROP TABLE`, with the number of tables that existed and were removed.
    DropTable {
        /// Number of tables removed.
        dropped: usize,
    },
    /// `CREATE INDEX`, with `created = false` when `IF NOT EXISTS` matched.
    CreateIndex {
        /// Whether the index was actually created.
        created: bool,
    },
    /// `DROP INDEX`, with `dropped = false` when `IF EXISTS` matched nothing.
    DropIndex {
        /// Whether an index was actually removed.
        dropped: bool,
    },
}

impl SqlDdlOutcome {
    /// Command tag reported by a PostgreSQL `CommandComplete`.
    pub fn command_tag(&self) -> &'static str {
        match self {
            Self::CreateTable { .. } => "CREATE TABLE",
            Self::AlterTableAddColumn { .. } => "ALTER TABLE",
            Self::DropTable { .. } => "DROP TABLE",
            Self::CreateIndex { .. } => "CREATE INDEX",
            Self::DropIndex { .. } => "DROP INDEX",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaField {
    pub(crate) name: String,
    pub(crate) field_id: u64,
    pub(crate) field_type: SqlFieldType,
    pub(crate) capacity: Option<u32>,
    pub(crate) sql_type: String,
    pub(crate) default: Option<String>,
    /// Whether writes that omit this column allocate a value from an owned
    /// sequence: PostgreSQL `SERIAL` / `BIGSERIAL` or MySQL `AUTO_INCREMENT`.
    pub(crate) auto_increment: bool,
    /// Next value the owned sequence hands out. Persisted with the schema so a
    /// restart never repeats a value the client already observed.
    pub(crate) next_value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaTable {
    pub(crate) name: String,
    pub(crate) type_id: u64,
    pub(crate) primary_key: Vec<String>,
    pub(crate) unique_constraints: Vec<Vec<String>>,
    pub(crate) fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaIndex {
    pub(crate) name: String,
    pub(crate) table: String,
    pub(crate) unique: bool,
    pub(crate) columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegistryState {
    revision: u32,
    next_type_id: u64,
    next_field_id: u64,
    pub(crate) tables: Vec<SchemaTable>,
    indexes: Vec<SchemaIndex>,
}

impl RegistryState {
    fn empty() -> Self {
        Self { revision: 1, next_type_id: 1, next_field_id: 1, tables: Vec::new(), indexes: Vec::new() }
    }

    fn table(&self, name: &str) -> Option<&SchemaTable> {
        self.tables.iter().find(|table| table.name == name)
    }

    fn table_index(&self, name: &str) -> Option<usize> {
        self.tables.iter().position(|table| table.name == name)
    }

    /// Next sequence value of one field, when it owns a sequence.
    fn next_value(&self, type_id: u64, field_id: u64) -> Option<u64> {
        self.tables
            .iter()
            .find(|table| table.type_id == type_id)
            .and_then(|table| table.fields.iter().find(|field| field.field_id == field_id))
            .filter(|field| field.auto_increment)
            .map(|field| field.next_value)
    }
}

/// Local schema registry shared by a gateway runtime.
#[derive(Debug)]
pub struct SqlSchemaRegistry {
    state: Mutex<RegistryState>,
    path: Option<PathBuf>,
}

/// Opaque registry snapshot used to discard DDL from a failed batch or a
/// rolled-back transaction.
#[derive(Debug, Clone)]
pub struct SqlSchemaCheckpoint {
    state: RegistryState,
}

/// One registered column that owns a value sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlAutoIncrementField {
    /// Stable YYDS field identity.
    pub field_id: u64,
    /// Catalog field name.
    pub name: String,
    /// Declared character/octet capacity of the column.
    pub capacity: Option<u32>,
}

impl SqlSchemaRegistry {
    /// Creates an empty in-memory registry at revision `1` without persistence.
    pub fn new_in_memory() -> Self {
        Self { state: Mutex::new(RegistryState::empty()), path: None }
    }

    /// Seeds an in-memory registry from a published or locally built catalog.
    ///
    /// DDL remains allowed, but nothing is written to disk.
    pub fn from_catalog(catalog: &SqlCatalog) -> Result<Self, SqlFrontendError> {
        let tables = catalog
            .tables()
            .iter()
            .map(|table| SchemaTable {
                name: table.name.clone(),
                type_id: table.type_id,
                primary_key: Vec::new(),
                unique_constraints: Vec::new(),
                fields: table
                    .fields
                    .iter()
                    .map(|field| SchemaField {
                        name: field.name.clone(),
                        field_id: field.field_id,
                        field_type: field.field_type.clone(),
                        capacity: field.capacity,
                        sql_type: canonical_sql_type(&field.field_type).to_string(),
                        default: None,
                        auto_increment: false,
                        next_value: 1,
                    })
                    .collect(),
            })
            .collect();
        Ok(Self { state: Mutex::new(RegistryState::empty().seeded(tables)), path: None })
    }

    /// Opens a persisted registry, creating an empty state when the file is absent.
    ///
    /// A malformed file or an unknown contract tag is an error — the registry
    /// never resets schema state silently.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, SqlFrontendError> {
        let path = path.into();
        let state = if path.exists() {
            let source = fs::read_to_string(&path)
                .map_err(|error| SqlFrontendError::internal(format!("cannot read SQL schema `{}`: {error}", path.display())))?;
            let document: SchemaDocument = serde_json::from_str(&source)
                .map_err(|error| SqlFrontendError::internal(format!("cannot parse SQL schema `{}`: {error}", path.display())))?;
            document.into_state()?
        }
        else {
            RegistryState::empty()
        };
        Ok(Self { state: Mutex::new(state), path: Some(path) })
    }

    /// Returns the current schema revision.
    pub fn revision(&self) -> u32 {
        self.lock().revision
    }

    /// Returns a consistent snapshot of the registered schema.
    pub fn catalog(&self) -> SqlCatalog {
        catalog_from_state(&self.lock())
    }

    /// Cloned registry state for read-only metadata synthesis.
    pub(crate) fn snapshot(&self) -> RegistryState {
        self.lock().clone()
    }

    /// Captures the whole registry state so a failed batch or aborted
    /// transaction can discard every DDL change made since the capture.
    ///
    /// The checkpoint is opaque: it is a full state copy, not a delta, and it
    /// is never merged with a newer state.
    pub fn checkpoint(&self) -> SqlSchemaCheckpoint {
        SqlSchemaCheckpoint { state: self.snapshot() }
    }

    /// Restores a previously captured checkpoint and persists the restored
    /// state when the registry is file-backed.
    ///
    /// Callers restore only on an explicit rollback path. Committing a
    /// transaction never needs to restore a checkpoint.
    ///
    /// Sequence counters are the one exception to a full restore: PostgreSQL
    /// `nextval` and MySQL `AUTO_INCREMENT` allocations survive a rollback, so
    /// the restored fields keep the higher counter the rollback already
    /// consumed instead of handing the same value out twice.
    pub fn restore(&self, checkpoint: SqlSchemaCheckpoint) -> Result<(), SqlFrontendError> {
        let mut guard = self.lock();
        let current = guard.clone();
        let mut restored = checkpoint.state;
        for table in &mut restored.tables {
            for field in &mut table.fields {
                if let Some(consumed) = current.next_value(table.type_id, field.field_id) {
                    field.next_value = field.next_value.max(consumed);
                }
            }
        }
        *guard = restored;
        if let Some(path) = &self.path {
            save(path, &guard)?;
        }
        Ok(())
    }

    /// Returns every column of `type_id` that owns a value sequence.
    ///
    /// The gateway calls this after binding an INSERT: a column that the client
    /// omitted is filled from the registry, exactly as PostgreSQL defaults a
    /// serial column and MySQL defaults an `AUTO_INCREMENT` column.
    pub fn auto_increment_fields(&self, type_id: u64) -> Vec<SqlAutoIncrementField> {
        let guard = self.lock();
        guard
            .tables
            .iter()
            .find(|table| table.type_id == type_id)
            .map(|table| {
                table
                    .fields
                    .iter()
                    .filter(|field| field.auto_increment)
                    .map(|field| SqlAutoIncrementField {
                        field_id: field.field_id,
                        name: field.name.clone(),
                        capacity: field.capacity,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Allocates the next value from the sequence owned by one column.
    ///
    /// The counter advances before the caller executes the write and the new
    /// value is persisted immediately, so neither a failed statement nor a
    /// rolled-back transaction reuses it. That is PostgreSQL's `nextval`
    /// contract and MySQL's `AUTO_INCREMENT` contract for a committed-value
    /// sequence.
    pub fn allocate_auto_increment(&self, type_id: u64, field_id: u64) -> Result<i64, SqlFrontendError> {
        let mut guard = self.lock();
        let mut next = guard.clone();
        let field = next
            .tables
            .iter_mut()
            .find(|table| table.type_id == type_id)
            .and_then(|table| table.fields.iter_mut().find(|field| field.field_id == field_id))
            .ok_or_else(|| SqlFrontendError::internal(format!("auto increment field {field_id} is not registered")))?;
        if !field.auto_increment {
            return Err(SqlFrontendError::internal(format!("field `{}` does not own a value sequence", field.name)));
        }
        let value = i64::try_from(field.next_value).map_err(|_| {
            SqlFrontendError::new(SqlErrorKind::NumericOverflow, "auto increment sequence exhausted the 64-bit signed range")
        })?;
        field.next_value += 1;
        if let Some(path) = &self.path {
            save(path, &next)?;
        }
        *guard = next;
        Ok(value)
    }

    /// Applies one DDL statement and persists the result when the state changed.
    pub fn apply_ddl(&self, statement: &SqlDdlStatement) -> Result<SqlDdlOutcome, SqlFrontendError> {
        let mut guard = self.lock();
        let mut next = guard.clone();
        let (outcome, changed) = apply_ddl(&mut next, statement)?;
        if changed {
            next.revision = next.revision.saturating_add(1);
            if let Some(path) = &self.path {
                save(path, &next)?;
            }
            *guard = next;
        }
        Ok(outcome)
    }

    fn lock(&self) -> MutexGuard<'_, RegistryState> {
        self.state.lock().unwrap_or_else(|poison| poison.into_inner())
    }
}

impl RegistryState {
    /// Fills identity counters above the seeded tables.
    fn seeded(mut self, tables: Vec<SchemaTable>) -> Self {
        self.next_type_id = tables.iter().map(|table| table.type_id).max().unwrap_or(0) + 1;
        self.next_field_id =
            tables.iter().flat_map(|table| table.fields.iter()).map(|field| field.field_id).max().unwrap_or(0) + 1;
        self.tables = tables;
        self
    }
}

fn catalog_from_state(state: &RegistryState) -> SqlCatalog {
    let tables = state
        .tables
        .iter()
        .map(|table| SqlCatalogTable {
            type_id: table.type_id,
            name: table.name.clone(),
            fields: table
                .fields
                .iter()
                .map(|field| SqlCatalogField {
                    field_id: field.field_id,
                    name: field.name.clone(),
                    field_type: field.field_type.clone(),
                    vos_type_name: None,
                    capacity: field.capacity,
                })
                .collect(),
            unique_constraints: table
                .unique_constraints
                .iter()
                .map(|constraint| {
                    constraint
                        .iter()
                        .filter_map(|name| table.fields.iter().find(|field| &field.name == name).map(|field| field.field_id))
                        .collect()
                })
                .collect(),
        })
        .collect();
    SqlCatalog::assemble(state.revision, tables)
}

fn apply_ddl(state: &mut RegistryState, statement: &SqlDdlStatement) -> Result<(SqlDdlOutcome, bool), SqlFrontendError> {
    match statement {
        SqlDdlStatement::CreateTable { name, if_not_exists, columns, primary_key, unique_constraints, .. } => {
            if state.table(name).is_some() {
                if *if_not_exists {
                    return Ok((SqlDdlOutcome::CreateTable { created: false }, false));
                }
                return Err(SqlFrontendError::new(SqlErrorKind::TableAlreadyExists, format!("table `{name}` already exists")));
            }
            if columns.is_empty() {
                return Err(SqlFrontendError::syntax(format!("table `{name}` must declare at least one column")));
            }
            let mut seen = BTreeSet::new();
            for column in columns {
                if !seen.insert(column.name.clone()) {
                    return Err(SqlFrontendError::syntax(format!("table `{name}` declares column `{}` twice", column.name)));
                }
            }
            let inline: Vec<String> =
                columns.iter().filter(|column| column.inline_primary_key).map(|column| column.name.clone()).collect();
            if inline.len() > 1 {
                return Err(SqlFrontendError::syntax(format!(
                    "table `{name}` declares PRIMARY KEY inline on more than one column"
                )));
            }
            let primary = if primary_key.is_empty() {
                inline
            }
            else if inline.is_empty() {
                primary_key.clone()
            }
            else if *primary_key == inline {
                primary_key.clone()
            }
            else {
                return Err(SqlFrontendError::syntax(format!(
                    "table `{name}` declares inline and table-level PRIMARY KEY columns that disagree"
                )));
            };
            let mut distinct = BTreeSet::new();
            for column in &primary {
                if !distinct.insert(column.clone()) {
                    return Err(SqlFrontendError::syntax(format!("table `{name}` lists primary key column `{column}` twice")));
                }
                if !columns.iter().any(|candidate| &candidate.name == column) {
                    return Err(SqlFrontendError::new(
                        SqlErrorKind::UnknownColumn,
                        format!("primary key column `{column}` does not exist in table `{name}`"),
                    ));
                }
            }
            for constraint in unique_constraints {
                for column in constraint {
                    if !columns.iter().any(|candidate| &candidate.name == column) {
                        return Err(SqlFrontendError::new(
                            SqlErrorKind::UnknownColumn,
                            format!("unique constraint column `{column}` does not exist in table `{name}`"),
                        ));
                    }
                }
            }
            let mut fields = Vec::with_capacity(columns.len());
            for column in columns {
                let (base, capacity) = column_type(column, name)?;
                let not_null = column.not_null || primary.iter().any(|candidate| candidate == &column.name);
                let field_type = if not_null { base } else { SqlFieldType::Optional(Box::new(base)) };
                let field_id = state.next_field_id;
                state.next_field_id += 1;
                fields.push(SchemaField {
                    name: column.name.clone(),
                    field_id,
                    field_type,
                    capacity,
                    sql_type: column.sql_type.clone(),
                    default: column.default.clone(),
                    auto_increment: column.auto_increment,
                    next_value: 1,
                });
            }
            let type_id = state.next_type_id;
            state.next_type_id += 1;
            state.tables.push(SchemaTable {
                name: name.clone(),
                type_id,
                primary_key: primary,
                unique_constraints: unique_constraints.clone(),
                fields,
            });
            Ok((SqlDdlOutcome::CreateTable { created: true }, true))
        }
        SqlDdlStatement::AlterTableAddColumn { table, column, if_not_exists } => {
            let index = state
                .table_index(table)
                .ok_or_else(|| SqlFrontendError::new(SqlErrorKind::UnknownTable, format!("table `{table}` does not exist")))?;
            if state.tables[index].fields.iter().any(|field| field.name == column.name) {
                if *if_not_exists {
                    return Ok((SqlDdlOutcome::AlterTableAddColumn { added: false }, false));
                }
                return Err(SqlFrontendError::new(
                    SqlErrorKind::ColumnAlreadyExists,
                    format!("column `{}` already exists in table `{table}`", column.name),
                ));
            }
            if column.inline_primary_key && !state.tables[index].primary_key.is_empty() {
                return Err(SqlFrontendError::syntax(format!(
                    "table `{table}` already has a PRIMARY KEY, cannot add another inline PRIMARY KEY column"
                )));
            }
            let (base, capacity) = column_type(column, table)?;
            let not_null = column.not_null || column.inline_primary_key;
            let field_type = if not_null { base } else { SqlFieldType::Optional(Box::new(base)) };
            let field_id = state.next_field_id;
            state.next_field_id += 1;
            state.tables[index].fields.push(SchemaField {
                name: column.name.clone(),
                field_id,
                field_type,
                capacity,
                sql_type: column.sql_type.clone(),
                default: column.default.clone(),
                auto_increment: column.auto_increment,
                next_value: 1,
            });
            if column.inline_primary_key {
                state.tables[index].primary_key.push(column.name.clone());
            }
            Ok((SqlDdlOutcome::AlterTableAddColumn { added: true }, true))
        }
        SqlDdlStatement::DropTable { names, if_exists } => {
            let mut dropped = 0;
            for name in names {
                let index = match state.table_index(name) {
                    Some(index) => index,
                    None if *if_exists => continue,
                    None => {
                        return Err(SqlFrontendError::new(
                            SqlErrorKind::UnknownTable,
                            format!("table `{name}` does not exist"),
                        ));
                    }
                };
                state.tables.remove(index);
                state.indexes.retain(|index| &index.table != name);
                dropped += 1;
            }
            Ok((SqlDdlOutcome::DropTable { dropped }, dropped > 0))
        }
        SqlDdlStatement::CreateIndex { name, table, if_not_exists, unique, columns } => {
            let target = state
                .table(table)
                .ok_or_else(|| SqlFrontendError::new(SqlErrorKind::UnknownTable, format!("table `{table}` does not exist")))?;
            if state.indexes.iter().any(|index| index.name == *name) {
                if *if_not_exists {
                    return Ok((SqlDdlOutcome::CreateIndex { created: false }, false));
                }
                return Err(SqlFrontendError::new(SqlErrorKind::IndexAlreadyExists, format!("index `{name}` already exists")));
            }
            for column in columns {
                if !target.fields.iter().any(|field| &field.name == column) {
                    return Err(SqlFrontendError::new(
                        SqlErrorKind::UnknownColumn,
                        format!("index `{name}` references column `{column}` that does not exist in table `{table}`"),
                    ));
                }
            }
            state.indexes.push(SchemaIndex {
                name: name.clone(),
                table: table.clone(),
                unique: *unique,
                columns: columns.clone(),
            });
            Ok((SqlDdlOutcome::CreateIndex { created: true }, true))
        }
        SqlDdlStatement::DropIndex { name, table, if_exists } => {
            if let Some(table) = table {
                if state.table(table).is_none() {
                    return Err(SqlFrontendError::new(SqlErrorKind::UnknownTable, format!("table `{table}` does not exist")));
                }
            }
            let position = state
                .indexes
                .iter()
                .position(|index| index.name == *name && table.as_ref().is_none_or(|table| index.table == *table));
            match position {
                Some(position) => {
                    state.indexes.remove(position);
                    Ok((SqlDdlOutcome::DropIndex { dropped: true }, true))
                }
                None if *if_exists => Ok((SqlDdlOutcome::DropIndex { dropped: false }, false)),
                None => Err(SqlFrontendError::new(SqlErrorKind::UnknownIndex, format!("index `{name}` does not exist"))),
            }
        }
    }
}

/// SQL type text reported for a catalog-seeded canonical field type.
fn canonical_sql_type(field_type: &SqlFieldType) -> &'static str {
    match field_type.inner() {
        SqlFieldType::Int32 => "INTEGER",
        SqlFieldType::Int64 => "BIGINT",
        SqlFieldType::Float64 => "DOUBLE PRECISION",
        SqlFieldType::String => "TEXT",
        SqlFieldType::Bool => "BOOLEAN",
        SqlFieldType::Bytes => "BYTEA",
        SqlFieldType::Unknown | SqlFieldType::Optional(_) => "TEXT",
    }
}

/// Maps one DDL column type onto a gateway field type and declared capacity.
fn column_type(column: &crate::sql_ddl::DdlColumn, table: &str) -> Result<(SqlFieldType, Option<u32>), SqlFrontendError> {
    map_sql_type(&column.sql_type).map_err(|error| {
        SqlFrontendError::new(error.kind, format!("column `{}` of table `{table}`: {}", column.name, error.message))
    })
}

fn map_sql_type(sql_type: &str) -> Result<(SqlFieldType, Option<u32>), SqlFrontendError> {
    let (base, parameters) = split_type(sql_type);
    let declared =
        parameters.and_then(|parameters| parameters.split(',').next()).and_then(|first| first.trim().parse::<u32>().ok());
    let field_type = match base {
        "TINYINT" if parameters == Some("1") => SqlFieldType::Bool,
        "BOOL" | "BOOLEAN" => SqlFieldType::Bool,
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "INT2" | "INT4" | "SMALLSERIAL" | "SERIAL" => {
            SqlFieldType::Int32
        }
        "BIGINT" | "INT8" | "BIGSERIAL" => SqlFieldType::Int64,
        "FLOAT" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT4" | "FLOAT8" => SqlFieldType::Float64,
        "VARCHAR" | "CHAR" | "CHARACTER" | "CHARACTER VARYING" => SqlFieldType::String,
        "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => SqlFieldType::String,
        "DATE"
        | "TIME"
        | "DATETIME"
        | "TIMESTAMP"
        | "TIMESTAMPTZ"
        | "TIMESTAMP WITH TIME ZONE"
        | "TIMESTAMP WITHOUT TIME ZONE"
        | "UUID" => SqlFieldType::String,
        "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BYTEA" => SqlFieldType::Bytes,
        _ => {
            return Err(SqlFrontendError::unsupported(format!("column type `{sql_type}` has no YYDS SQL gateway equivalent")));
        }
    };
    let capacity = match base {
        "BLOB" => Some(MYSQL_BLOB_CAPACITY),
        "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BYTEA" => None,
        "BINARY" | "VARBINARY" | "VARCHAR" | "CHAR" | "CHARACTER" | "CHARACTER VARYING" => declared,
        _ => None,
    };
    Ok((field_type, capacity))
}

/// Splits `TINYINT(1)` / `DOUBLE PRECISION` into a base word and parameter text.
fn split_type(sql_type: &str) -> (&str, Option<&str>) {
    let Some(open) = sql_type.find('(')
    else {
        return (sql_type, None);
    };
    let close = sql_type.rfind(')').unwrap_or(sql_type.len());
    let parameters = if open + 1 < close { Some(&sql_type[open + 1..close]) } else { None };
    (sql_type[..open].trim_end(), parameters)
}

fn validate_state(state: &RegistryState) -> Result<(), SqlFrontendError> {
    if state.revision == 0 {
        return Err(SqlFrontendError::internal("SQL schema revision must be nonzero"));
    }
    let mut type_ids = BTreeSet::new();
    let mut field_ids = BTreeSet::new();
    for table in &state.tables {
        if table.name.is_empty() {
            return Err(SqlFrontendError::internal("SQL schema table has no name"));
        }
        if table.type_id == 0 || !type_ids.insert(table.type_id) {
            return Err(SqlFrontendError::internal("invalid or duplicate SQL schema type identity"));
        }
        for field in &table.fields {
            if field.name.is_empty() {
                return Err(SqlFrontendError::internal("SQL schema field has no name"));
            }
            if field.field_id == 0 || !field_ids.insert(field.field_id) {
                return Err(SqlFrontendError::internal("invalid or duplicate SQL schema field identity"));
            }
            if field.sql_type.trim().is_empty() {
                return Err(SqlFrontendError::internal(format!(
                    "SQL schema field `{}` of table `{}` has no sql_type",
                    field.name, table.name
                )));
            }
            if field.auto_increment {
                if !matches!(field.field_type.inner(), SqlFieldType::Int32 | SqlFieldType::Int64) {
                    return Err(SqlFrontendError::internal(format!(
                        "SQL schema field `{}` of table `{}` owns a sequence but is not an integer column",
                        field.name, table.name
                    )));
                }
                if field.next_value == 0 {
                    return Err(SqlFrontendError::internal(format!(
                        "SQL schema field `{}` of table `{}` has a zero sequence counter",
                        field.name, table.name
                    )));
                }
            }
        }
        for constraint in &table.unique_constraints {
            if constraint.is_empty() {
                return Err(SqlFrontendError::internal(format!(
                    "SQL schema table `{}` has an empty unique constraint",
                    table.name
                )));
            }
            for column in constraint {
                if !table.fields.iter().any(|field| &field.name == column) {
                    return Err(SqlFrontendError::internal(format!(
                        "unique constraint column `{column}` does not exist in table `{}`",
                        table.name
                    )));
                }
            }
        }
    }
    let next_type_id = type_ids.iter().max().copied().unwrap_or(0) + 1;
    if state.next_type_id < next_type_id {
        return Err(SqlFrontendError::internal("SQL schema type identity counter would reuse a registered identity"));
    }
    let next_field_id = field_ids.iter().max().copied().unwrap_or(0) + 1;
    if state.next_field_id < next_field_id {
        return Err(SqlFrontendError::internal("SQL schema field identity counter would reuse a registered identity"));
    }
    Ok(())
}

fn save(path: &Path, state: &RegistryState) -> Result<(), SqlFrontendError> {
    let document = SchemaDocument::from_state(state);
    let json = serde_json::to_string_pretty(&document)
        .map_err(|error| SqlFrontendError::internal(format!("cannot encode SQL schema: {error}")))?;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|error| {
                SqlFrontendError::internal(format!("cannot create SQL schema directory `{}`: {error}", parent.display()))
            })?;
        }
    }
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    fs::write(&temporary, json)
        .map_err(|error| SqlFrontendError::internal(format!("cannot write SQL schema `{}`: {error}", temporary.display())))?;
    if fs::rename(&temporary, path).is_err() {
        // Windows keeps the destination busy when another handle is open, so
        // remove it first and retry the replacement.
        if path.exists() {
            fs::remove_file(path).map_err(|error| {
                SqlFrontendError::internal(format!("cannot replace SQL schema `{}`: {error}", path.display()))
            })?;
        }
        fs::rename(&temporary, path).map_err(|error| {
            let _ = fs::remove_file(&temporary);
            SqlFrontendError::internal(format!("cannot replace SQL schema `{}`: {error}", path.display()))
        })?;
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaDocument {
    contract: String,
    revision: u32,
    next_type_id: u64,
    next_field_id: u64,
    tables: Vec<TableDocument>,
    indexes: Vec<IndexDocument>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableDocument {
    name: String,
    type_id: u64,
    primary_key: Vec<String>,
    unique_constraints: Vec<Vec<String>>,
    fields: Vec<FieldDocument>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldDocument {
    name: String,
    field_id: u64,
    #[serde(rename = "type")]
    field_type: FieldTypeDocument,
    nullable: bool,
    capacity: Option<u32>,
    #[serde(rename = "sql_type")]
    sql_type: String,
    #[serde(default)]
    default: Option<String>,
    #[serde(default)]
    auto_increment: bool,
    #[serde(default = "initial_next_value")]
    next_value: u64,
}

/// First value handed out by a freshly created owned sequence.
fn initial_next_value() -> u64 {
    1
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexDocument {
    name: String,
    table: String,
    unique: bool,
    columns: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum FieldTypeDocument {
    Int32,
    Int64,
    Float64,
    String,
    Bool,
    Bytes,
    Unknown,
}

impl FieldTypeDocument {
    fn base(self) -> SqlFieldType {
        match self {
            Self::Int32 => SqlFieldType::Int32,
            Self::Int64 => SqlFieldType::Int64,
            Self::Float64 => SqlFieldType::Float64,
            Self::String => SqlFieldType::String,
            Self::Bool => SqlFieldType::Bool,
            Self::Bytes => SqlFieldType::Bytes,
            Self::Unknown => SqlFieldType::Unknown,
        }
    }

    fn of(field_type: &SqlFieldType) -> Self {
        match field_type.inner() {
            SqlFieldType::Int32 => Self::Int32,
            SqlFieldType::Int64 => Self::Int64,
            SqlFieldType::Float64 => Self::Float64,
            SqlFieldType::String => Self::String,
            SqlFieldType::Bool => Self::Bool,
            SqlFieldType::Bytes => Self::Bytes,
            SqlFieldType::Optional(_) | SqlFieldType::Unknown => Self::Unknown,
        }
    }
}

impl SchemaDocument {
    fn from_state(state: &RegistryState) -> Self {
        Self {
            contract: CONTRACT_TAG.to_string(),
            revision: state.revision,
            next_type_id: state.next_type_id,
            next_field_id: state.next_field_id,
            tables: state
                .tables
                .iter()
                .map(|table| TableDocument {
                    name: table.name.clone(),
                    type_id: table.type_id,
                    primary_key: table.primary_key.clone(),
                    unique_constraints: table.unique_constraints.clone(),
                    fields: table
                        .fields
                        .iter()
                        .map(|field| FieldDocument {
                            name: field.name.clone(),
                            field_id: field.field_id,
                            field_type: FieldTypeDocument::of(&field.field_type),
                            nullable: field.field_type.is_nullable(),
                            capacity: field.capacity,
                            sql_type: field.sql_type.clone(),
                            default: field.default.clone(),
                            auto_increment: field.auto_increment,
                            next_value: field.next_value,
                        })
                        .collect(),
                })
                .collect(),
            indexes: state
                .indexes
                .iter()
                .map(|index| IndexDocument {
                    name: index.name.clone(),
                    table: index.table.clone(),
                    unique: index.unique,
                    columns: index.columns.clone(),
                })
                .collect(),
        }
    }

    fn into_state(self) -> Result<RegistryState, SqlFrontendError> {
        if self.contract != CONTRACT_TAG {
            return Err(SqlFrontendError::internal(format!(
                "unsupported SQL schema contract `{}`, expected `{CONTRACT_TAG}`",
                self.contract
            )));
        }
        let state = RegistryState {
            revision: self.revision,
            next_type_id: self.next_type_id,
            next_field_id: self.next_field_id,
            tables: self
                .tables
                .into_iter()
                .map(|table| SchemaTable {
                    name: table.name,
                    type_id: table.type_id,
                    primary_key: table.primary_key,
                    unique_constraints: table.unique_constraints,
                    fields: table
                        .fields
                        .into_iter()
                        .map(|field| {
                            let base = field.field_type.base();
                            let field_type = if field.nullable { SqlFieldType::Optional(Box::new(base)) } else { base };
                            SchemaField {
                                name: field.name,
                                field_id: field.field_id,
                                field_type,
                                capacity: field.capacity,
                                sql_type: field.sql_type,
                                default: field.default,
                                auto_increment: field.auto_increment,
                                next_value: field.next_value,
                            }
                        })
                        .collect(),
                })
                .collect(),
            indexes: self
                .indexes
                .into_iter()
                .map(|index| SchemaIndex { name: index.name, table: index.table, unique: index.unique, columns: index.columns })
                .collect(),
        };
        validate_state(&state)?;
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::sql_ddl::DdlColumn;

    fn column(name: &str, sql_type: &str, not_null: bool) -> DdlColumn {
        DdlColumn {
            name: name.to_string(),
            sql_type: sql_type.to_string(),
            not_null,
            inline_primary_key: false,
            auto_increment: false,
            default: None,
        }
    }

    fn create_users() -> SqlDdlStatement {
        SqlDdlStatement::CreateTable {
            name: "users".to_string(),
            if_not_exists: false,
            columns: vec![column("id", "BIGINT", false), column("email", "VARCHAR(191)", false), column("bio", "TEXT", false)],
            primary_key: vec!["id".to_string()],
            unique_constraints: vec![vec!["email".to_string()]],
            charset: Some("utf8mb4".to_string()),
            collation: None,
        }
    }

    fn unique_path() -> PathBuf {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|elapsed| elapsed.as_nanos()).unwrap_or(0);
        std::env::temp_dir().join(format!("yyds-sql-schema-{}-{nanos}.json", std::process::id()))
    }

    #[test]
    fn create_table_exposes_catalog_fields_and_primary_key() {
        let registry = SqlSchemaRegistry::new_in_memory();
        let outcome = registry.apply_ddl(&create_users()).expect("create table applies");
        assert_eq!(outcome, SqlDdlOutcome::CreateTable { created: true });
        assert_eq!(registry.revision(), 2);

        let catalog = registry.catalog();
        assert_eq!(catalog.schema_version(), 2);
        assert_eq!(catalog.tables().len(), 1);
        let table = &catalog.tables()[0];
        assert_eq!(table.name, "users");
        assert_eq!(table.fields.len(), 3);
        assert_eq!(table.fields[0].field_type, SqlFieldType::Int64);
        assert_eq!(table.fields[1].field_type, SqlFieldType::Optional(Box::new(SqlFieldType::String)));
        assert_eq!(table.fields[1].capacity, Some(191));
        assert_eq!(table.fields[2].field_type, SqlFieldType::Optional(Box::new(SqlFieldType::String)));
        assert_eq!(table.fields[2].capacity, None);
    }

    #[test]
    fn create_table_if_not_exists_does_not_bump_revision() {
        let registry = SqlSchemaRegistry::new_in_memory();
        registry.apply_ddl(&create_users()).expect("first create applies");
        let mut statement = create_users();
        if let SqlDdlStatement::CreateTable { if_not_exists, .. } = &mut statement {
            *if_not_exists = true;
        }
        let outcome = registry.apply_ddl(&statement).expect("duplicate create is a no-op");
        assert_eq!(outcome, SqlDdlOutcome::CreateTable { created: false });
        assert_eq!(registry.revision(), 2);
        assert_eq!(registry.catalog().tables()[0].fields.len(), 3);
    }

    #[test]
    fn duplicate_table_without_flag_is_rejected() {
        let registry = SqlSchemaRegistry::new_in_memory();
        registry.apply_ddl(&create_users()).expect("first create applies");
        let error = registry.apply_ddl(&create_users()).expect_err("duplicate table is rejected");
        assert_eq!(error.kind, SqlErrorKind::TableAlreadyExists);
        assert!(error.message.contains("users"), "{}", error.message);
        assert_eq!(registry.revision(), 2);
    }

    #[test]
    fn alter_table_adds_nullable_column_and_rejects_duplicates() {
        let registry = SqlSchemaRegistry::new_in_memory();
        registry.apply_ddl(&create_users()).expect("create applies");
        let add = SqlDdlStatement::AlterTableAddColumn {
            table: "users".to_string(),
            column: column("age", "INT", false),
            if_not_exists: false,
        };
        assert_eq!(registry.apply_ddl(&add).expect("add column applies"), SqlDdlOutcome::AlterTableAddColumn { added: true });
        assert_eq!(registry.revision(), 3);
        let catalog = registry.catalog();
        let table = &catalog.tables()[0];
        assert_eq!(table.fields.len(), 4);
        assert_eq!(table.fields[3].name, "age");
        assert_eq!(table.fields[3].field_type, SqlFieldType::Optional(Box::new(SqlFieldType::Int32)));

        let mut duplicate = add.clone();
        if let SqlDdlStatement::AlterTableAddColumn { if_not_exists, .. } = &mut duplicate {
            *if_not_exists = true;
        }
        assert_eq!(
            registry.apply_ddl(&duplicate).expect("duplicate add is a no-op"),
            SqlDdlOutcome::AlterTableAddColumn { added: false }
        );
        let error = registry.apply_ddl(&add).expect_err("duplicate column is rejected");
        assert_eq!(error.kind, SqlErrorKind::ColumnAlreadyExists);
        assert!(error.message.contains("age") && error.message.contains("users"), "{}", error.message);
    }

    #[test]
    fn drop_table_removes_metadata_and_unknown_tables_are_rejected() {
        let registry = SqlSchemaRegistry::new_in_memory();
        registry.apply_ddl(&create_users()).expect("create applies");
        let drop = SqlDdlStatement::DropTable { names: vec!["users".to_string()], if_exists: false };
        assert_eq!(registry.apply_ddl(&drop).expect("drop applies"), SqlDdlOutcome::DropTable { dropped: 1 });
        assert_eq!(registry.revision(), 3);
        assert!(registry.catalog().tables().is_empty());

        let error = registry.apply_ddl(&drop).expect_err("absent table is rejected");
        assert_eq!(error.kind, SqlErrorKind::UnknownTable);
        let mut forgiving = drop.clone();
        if let SqlDdlStatement::DropTable { if_exists, .. } = &mut forgiving {
            *if_exists = true;
        }
        assert_eq!(registry.apply_ddl(&forgiving).expect("if exists is a no-op"), SqlDdlOutcome::DropTable { dropped: 0 });
    }

    #[test]
    fn create_and_drop_index_validate_columns_and_names() {
        let registry = SqlSchemaRegistry::new_in_memory();
        registry.apply_ddl(&create_users()).expect("create applies");
        let create = SqlDdlStatement::CreateIndex {
            name: "idx_users_email".to_string(),
            table: "users".to_string(),
            if_not_exists: false,
            unique: false,
            columns: vec!["email".to_string()],
        };
        assert_eq!(registry.apply_ddl(&create).expect("create index applies"), SqlDdlOutcome::CreateIndex { created: true });
        assert_eq!(registry.revision(), 3);

        let mut duplicate = create.clone();
        if let SqlDdlStatement::CreateIndex { if_not_exists, .. } = &mut duplicate {
            *if_not_exists = true;
        }
        assert_eq!(
            registry.apply_ddl(&duplicate).expect("duplicate index is a no-op"),
            SqlDdlOutcome::CreateIndex { created: false }
        );
        let error = registry.apply_ddl(&create).expect_err("duplicate index is rejected");
        assert_eq!(error.kind, SqlErrorKind::IndexAlreadyExists);
        assert!(error.message.contains("idx_users_email"), "{}", error.message);

        let missing_column = SqlDdlStatement::CreateIndex {
            name: "idx_users_missing".to_string(),
            table: "users".to_string(),
            if_not_exists: false,
            unique: false,
            columns: vec!["nickname".to_string()],
        };
        let error = registry.apply_ddl(&missing_column).expect_err("unknown index column is rejected");
        assert_eq!(error.kind, SqlErrorKind::UnknownColumn);
        assert!(error.message.contains("nickname"), "{}", error.message);

        let drop = SqlDdlStatement::DropIndex {
            name: "idx_users_email".to_string(),
            table: Some("users".to_string()),
            if_exists: false,
        };
        assert_eq!(registry.apply_ddl(&drop).expect("drop index applies"), SqlDdlOutcome::DropIndex { dropped: true });
        let error = registry.apply_ddl(&drop).expect_err("absent index is rejected");
        assert_eq!(error.kind, SqlErrorKind::UnknownIndex);
        assert!(error.message.contains("idx_users_email"), "{}", error.message);
        let mut forgiving = drop.clone();
        if let SqlDdlStatement::DropIndex { if_exists, .. } = &mut forgiving {
            *if_exists = true;
        }
        assert_eq!(registry.apply_ddl(&forgiving).expect("if exists is a no-op"), SqlDdlOutcome::DropIndex { dropped: false });
    }

    #[test]
    fn persistence_round_trips_the_v0_document() {
        let path = unique_path();
        let registry = SqlSchemaRegistry::open(&path).expect("open empty registry");
        registry.apply_ddl(&create_users()).expect("create applies");
        registry
            .apply_ddl(&SqlDdlStatement::AlterTableAddColumn {
                table: "users".to_string(),
                column: column("age", "INT", false),
                if_not_exists: false,
            })
            .expect("add column applies");
        assert_eq!(registry.revision(), 3);

        let source = fs::read_to_string(&path).expect("schema document exists");
        let document: serde_json::Value = serde_json::from_str(&source).expect("document is JSON");
        assert_eq!(document["contract"], CONTRACT_TAG);
        assert_eq!(document["revision"], 3);
        assert_eq!(document["next_type_id"], 2);
        assert_eq!(document["next_field_id"], 5);
        assert_eq!(document["tables"][0]["name"], "users");
        assert_eq!(document["tables"][0]["primary_key"][0], "id");
        assert_eq!(document["tables"][0]["unique_constraints"][0][0], "email");
        assert_eq!(document["tables"][0]["fields"][1]["type"], "string");
        assert_eq!(document["tables"][0]["fields"][1]["capacity"], 191);
        assert_eq!(document["tables"][0]["fields"][1]["sql_type"], "VARCHAR(191)");
        assert_eq!(document["tables"][0]["fields"][2]["nullable"], true);
        assert_eq!(document["tables"][0]["fields"][3]["sql_type"], "INT");

        let reopened = SqlSchemaRegistry::open(&path).expect("reopen registry");
        assert_eq!(reopened.revision(), 3);
        let catalog = reopened.catalog();
        assert_eq!(catalog.schema_version(), 3);
        assert_eq!(catalog.tables()[0].fields.len(), 4);
        assert_eq!(catalog.tables()[0].fields[1].capacity, Some(191));
        assert_eq!(catalog.tables()[0].fields[3].field_type, SqlFieldType::Optional(Box::new(SqlFieldType::Int32)));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn corrupt_schema_documents_are_rejected() {
        let path = unique_path();
        fs::write(&path, "not a schema document").expect("write corrupt schema");
        let error = SqlSchemaRegistry::open(&path).expect_err("corrupt schema is rejected");
        assert_eq!(error.kind, SqlErrorKind::Internal);
        let _ = fs::remove_file(&path);

        let path = unique_path();
        fs::write(
            &path,
            r#"{"contract":"yyds-sql-schema/v1","revision":1,"next_type_id":1,"next_field_id":1,"tables":[],"indexes":[]}"#,
        )
        .expect("write unknown contract");
        let error = SqlSchemaRegistry::open(&path).expect_err("unknown contract is rejected");
        assert_eq!(error.kind, SqlErrorKind::Internal);
        assert!(error.message.contains("yyds-sql-schema/v1"), "{}", error.message);
        let _ = fs::remove_file(&path);

        let path = unique_path();
        fs::write(
            &path,
            r#"{"contract":"yyds-sql-schema/v0","revision":1,"next_type_id":1,"next_field_id":1,"tables":[{"name":"users","type_id":1,"primary_key":[],"unique_constraints":[],"fields":[{"name":"id","field_id":1,"type":"int64","nullable":false,"capacity":null}]}],"indexes":[]}"#,
        )
        .expect("write document without sql_type");
        let error = SqlSchemaRegistry::open(&path).expect_err("missing sql_type is rejected");
        assert_eq!(error.kind, SqlErrorKind::Internal);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn from_catalog_seeds_identities_without_persistence() {
        let catalog = catalog_from_state(&RegistryState {
            revision: 3,
            next_type_id: 2,
            next_field_id: 2,
            tables: vec![SchemaTable {
                name: "users".to_string(),
                type_id: 1,
                primary_key: vec!["id".to_string()],
                unique_constraints: Vec::new(),
                fields: vec![SchemaField {
                    name: "id".to_string(),
                    field_id: 1,
                    field_type: SqlFieldType::Int64,
                    capacity: None,
                    sql_type: "BIGINT".to_string(),
                    default: None,
                    auto_increment: false,
                    next_value: 1,
                }],
            }],
            indexes: Vec::new(),
        });
        let registry = SqlSchemaRegistry::from_catalog(&catalog).expect("seed registry");
        assert_eq!(registry.revision(), 1);
        let reopened = registry.catalog();
        assert_eq!(reopened.tables()[0].name, "users");
        assert_eq!(reopened.tables()[0].fields[0].field_type, SqlFieldType::Int64);
    }

    #[test]
    fn unsupported_column_types_are_rejected_with_the_type_text() {
        let registry = SqlSchemaRegistry::new_in_memory();
        let error = registry
            .apply_ddl(&SqlDdlStatement::CreateTable {
                name: "numbers".to_string(),
                if_not_exists: false,
                columns: vec![column("amount", "DECIMAL(10,2)", true)],
                primary_key: Vec::new(),
                unique_constraints: Vec::new(),
                charset: None,
                collation: None,
            })
            .expect_err("decimal has no gateway equivalent");
        assert_eq!(error.kind, SqlErrorKind::Unsupported);
        assert!(error.message.contains("DECIMAL(10,2)"), "{}", error.message);
        assert!(error.message.contains("amount"), "{}", error.message);
    }
}
