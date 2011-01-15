//! `database-input`：VOS / SQL / 命令输入语义与降级。
//!
//! 本包是输入面门面，实现落在 [`database_query`]；按能力再导出，不按产品品牌分叉。

#![deny(missing_docs)]

/// SQL 输入面（解析 / 绑定 / 会话命令）。
pub mod sql {
    pub use database_query::{
        DiscardTarget, MYSQL_BLOB_CAPACITY, SavepointIntent, SqlBoundBinaryOperator, SqlBoundConflict, SqlBoundConflictAction, SqlBoundCte,
        SqlBoundExpression, SqlBoundJoin, SqlBoundJoinKind, SqlBoundLiteral, SqlBoundProjection, SqlBoundSelect, SqlBoundSetOperand,
        SqlBoundSetOperation, SqlBoundSetOperator, SqlBoundSource, SqlBoundStatement, SqlBoundUnaryOperator, SqlDialect, SqlErrorKind,
        SqlFrontendError, SqlSessionCommand, SqlTransactionAction, SqlVariableScope, bind_sql, detect_savepoint_statement,
        detect_session_reset_statement, encode_hex, normalize_identifier, normalize_reserved_function_calls, parse_session_command, parse_sql,
    };
}

/// 嵌入式本地查询输入 / 降级。
pub mod local {
    pub use database_query::local::*;
}

/// 分布式输入合同（集群 KV 命令面）。
pub mod cluster {
    pub use database_query::cluster_kv::*;
}
