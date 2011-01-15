//! `database-query` owns binding, parameters, predicates, projections, plan validation and result description.
#![deny(missing_docs)]

pub mod catalog_bind;
pub mod cluster_kv;
pub mod ir;
pub mod local;
pub mod sql;
pub mod sql_batch;
pub mod sql_ddl;
pub mod sql_describe;
pub mod sql_execute;
pub mod sql_meta;
pub mod sql_parameters;
pub mod sql_planner;
pub mod sql_schema;

pub use catalog_bind::{
    SqlCatalog, SqlCatalogBoundColumn, SqlCatalogBoundExpression, SqlCatalogBoundJoin, SqlCatalogBoundProjection, SqlCatalogBoundSelect,
    SqlCatalogBoundSetOperand, SqlCatalogBoundSetOperation, SqlCatalogBoundStatement, SqlCatalogBoundTable, SqlCatalogField,
    SqlCatalogOuterColumn, SqlCatalogTable, SqlFieldType, bind_sql_catalog, bind_sql_catalog_with_variables, project_resolved_type,
};
pub use sql::{
    DiscardTarget, MYSQL_BLOB_CAPACITY, SavepointIntent, SqlBoundBinaryOperator, SqlBoundConflict, SqlBoundConflictAction, SqlBoundCte,
    SqlBoundExpression, SqlBoundJoin, SqlBoundJoinKind, SqlBoundLiteral, SqlBoundProjection, SqlBoundSelect, SqlBoundSetOperand,
    SqlBoundSetOperation, SqlBoundSetOperator, SqlBoundSource, SqlBoundStatement, SqlBoundUnaryOperator, SqlDialect, SqlErrorKind,
    SqlFrontendError, SqlSessionCommand, SqlTransactionAction, SqlVariableScope, bind_sql, detect_savepoint_statement,
    detect_session_reset_statement, encode_hex, normalize_identifier, normalize_reserved_function_calls, parse_session_command, parse_sql,
};
pub use sql_batch::{SqlBatchStatement, split_sql_batch};
pub use sql_ddl::{ACCEPTED_COLLATIONS, DdlColumn, SqlDdlStatement, is_accepted_collation, is_ddl_statement, parse_ddl};
pub use sql_describe::{SqlColumn, describe_bound_insert, describe_plan};
pub use sql_execute::{RowFieldValues, SqlResult, SqlValue, decode_row, encode_row};
pub use sql_meta::{MetaQuery, PG_SERVER_VERSION, analyze_meta_query, mysql_data_type, postgres_type};
pub use sql_parameters::{SqlParameterStyle, neutralized_sql_parameters, nulled_sql_parameters, substitute_sql_parameters};
pub use sql_planner::{
    DeletePlan, InsertSelectColumn, InsertSelectPlan, JoinPlan, JoinSourcePlan, ReadPlan, ScanPlan, SetOperationPlan, SqlExecutionPlan,
    SqlSetOrderByItem, UniqueConstraintPlan, UpdatePlan, WritePlan, plan_select, plan_select_for_dialect, plan_statement,
    plan_statement_for_dialect,
};
pub use sql_schema::{SqlAutoIncrementField, SqlDdlOutcome, SqlSchemaCheckpoint, SqlSchemaRegistry, apply_auto_increment};

/// 方言伪装响应使用的版本标签（`database-0.1.0`）。
pub fn version_label() -> String {
    format!("database-{}", database_types::cluster::version())
}
