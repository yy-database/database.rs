//! Shared disguise gateway core for YYDS.
//!
//! Protocol adapters (`yyds-gateway-redis`, `yyds-gateway-mysql`, `yyds-gateway-pgsql`)
//! share configuration, banners, and legacy-surface guards from this crate.

use std::path::PathBuf;

use yyds_types::version;

mod catalog_bind;
mod config;
mod kind;
mod sql;
mod sql_batch;
mod sql_ddl;
mod sql_describe;
mod sql_execute;
mod sql_meta;
mod sql_parameters;
mod sql_planner;
mod sql_schema;
mod sql_serial;

pub use crate::{
    catalog_bind::{
        bind_sql_catalog, bind_sql_catalog_with_variables, SqlCatalog, SqlCatalogBoundColumn, SqlCatalogBoundExpression,
        SqlCatalogBoundJoin, SqlCatalogBoundProjection, SqlCatalogBoundSelect, SqlCatalogBoundSetOperand,
        SqlCatalogBoundSetOperation, SqlCatalogBoundStatement, SqlCatalogBoundTable, SqlCatalogField, SqlCatalogOuterColumn,
        SqlCatalogTable, SqlFieldType,
    },
    sql_batch::{split_sql_batch, SqlBatchStatement},
    sql_ddl::{is_accepted_collation, is_ddl_statement, parse_ddl, DdlColumn, SqlDdlStatement, ACCEPTED_COLLATIONS},
    sql_describe::{describe_bound_insert, describe_plan, SqlColumn},
    sql_execute::{decode_row, encode_row, execute_plan, RowFieldValues, SqlResult, SqlValue},
    sql_meta::{analyze_meta_query, mysql_data_type, postgres_type, MetaQuery, PG_SERVER_VERSION},
    sql_parameters::{neutralized_sql_parameters, nulled_sql_parameters, substitute_sql_parameters, SqlParameterStyle},
    sql_planner::{
        plan_select, plan_select_for_dialect, plan_statement, plan_statement_for_dialect, DeletePlan, InsertSelectColumn,
        InsertSelectPlan, JoinPlan, JoinSourcePlan, ReadPlan, ScanPlan, SetOperationPlan, SqlExecutionPlan, SqlSetOrderByItem,
        UniqueConstraintPlan, UpdatePlan, WritePlan,
    },
    sql_schema::{SqlAutoIncrementField, SqlDdlOutcome, SqlSchemaCheckpoint, SqlSchemaRegistry},
    sql_serial::apply_auto_increment,
};

pub use crate::{
    config::GatewayConfig,
    kind::{GatewayKind, ALL_GATEWAY_KINDS},
    sql::{
        bind_sql, detect_savepoint_statement, detect_session_reset_statement, encode_hex, parse_session_command, parse_sql,
        DiscardTarget, SavepointIntent, SqlBoundBinaryOperator, SqlBoundConflict, SqlBoundConflictAction, SqlBoundCte,
        SqlBoundExpression, SqlBoundJoin, SqlBoundJoinKind, SqlBoundLiteral, SqlBoundProjection, SqlBoundSelect,
        SqlBoundSetOperand, SqlBoundSetOperation, SqlBoundSetOperator, SqlBoundSource, SqlBoundStatement,
        SqlBoundUnaryOperator, SqlDialect, SqlErrorKind, SqlFrontendError, SqlSessionCommand, SqlTransactionAction,
        SqlVariableScope, MYSQL_BLOB_CAPACITY,
    },
};

/// Legacy surfaces that must never be revived as YYDS product paths.
pub const FORBIDDEN_LEGACY_SURFACES: &[&str] = &[
    "yyds-gateway/src/sql",
    "yyds-odbc",
    "we-trust-sqlite",
    "we-trust-mysql",
    "we-trust-postgres",
    "we-trust-sqlserver",
    "query_with_sql",
    "SqlQuery",
];

/// Returns the YYDS version label used in disguise responses (`yyds-0.1.0`).
pub fn version_label() -> String {
    format!("yyds-{}", version())
}

/// Startup banner for a configured disguise gateway listener.
pub fn startup_banner(config: &GatewayConfig) -> String {
    format!("{} listening on {} (YYDS {version})", config.kind.crate_name(), config.port, version = version())
}

/// Returns true when `surface` is a forbidden legacy gateway path.
pub fn is_forbidden_legacy_surface(surface: &str) -> bool {
    FORBIDDEN_LEGACY_SURFACES.iter().any(|entry| entry.eq_ignore_ascii_case(surface))
}

/// Optional catalog path attached to a gateway process.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GatewayCatalog {
    path: Option<PathBuf>,
}

impl GatewayCatalog {
    /// Creates an empty catalog binding.
    pub fn new() -> Self {
        Self::default()
    }

    /// Binds the gateway to a `.yyds` catalog file path.
    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self { path: Some(path.into()) }
    }

    /// Returns the configured catalog path, if any.
    pub fn path(&self) -> Option<&PathBuf> {
        self.path.as_ref()
    }
}
