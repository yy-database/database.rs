//! Bounded loopback MySQL protocol-v10 startup and SQL execution.

use std::{
    borrow::Cow,
    collections::HashMap,
    io::{self, Read, Write},
    net::TcpStream,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use database_execution::{
    cluster::{KeyValueExecutor, TransactionHandle},
    sql_exec::execute_plan,
};
use database_protocol_mysql::PROTOCOL_VERSION;
use database_query::{
    MYSQL_BLOB_CAPACITY, SqlBoundExpression, SqlBoundLiteral, SqlCatalogBoundExpression, SqlCatalogBoundStatement, SqlColumn, SqlDialect,
    SqlErrorKind, SqlExecutionPlan, SqlFieldType, SqlFrontendError, SqlParameterStyle, SqlResult, SqlSchemaCheckpoint, SqlSchemaRegistry,
    SqlSessionCommand, SqlTransactionAction, SqlValue, analyze_meta_query, apply_auto_increment, bind_sql_catalog_with_variables,
    describe_plan_for_dialect, is_accepted_collation, neutralized_sql_parameters, parse_ddl, parse_session_command,
    plan_statement_for_dialect,
    substitute_sql_parameters,
};

use super::wire::{PacketError, encode_packet};

const SERVER_CAPABILITIES: u32 = 0x0000_0001 | 0x0000_0004 | 0x0000_0008 | 0x0000_0200 | 0x0000_2000 | 0x0000_8000 | 0x0002_0000 | 0x0008_0000;
const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_SSL: u32 = 0x0000_0800;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
const CLIENT_PLUGIN_AUTH_LENENC: u32 = 0x0020_0000;
const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
/// Largest request frame accepted, and the value advertised as
/// `@@max_allowed_packet`. Keeping the two equal means a client never sends
/// a frame the server will refuse.
const MAX_PACKET: usize = 1024 * 1024;

const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_PING: u8 = 0x0E;
const COM_STMT_PREPARE: u8 = 0x16;
const COM_STMT_EXECUTE: u8 = 0x17;
const COM_STMT_CLOSE: u8 = 0x19;
const COM_STMT_RESET: u8 = 0x1A;
const COM_RESET_CONNECTION: u8 = 0x1F;

/// `ER_UNKNOWN_COM_ERROR` — returned for every command byte the gateway does not
/// implement so the client sees a real MySQL error instead of a desynchronised
/// stream.
const ER_UNKNOWN_COM_ERROR: u16 = 1047;

/// `MYSQL_TYPE_*` codes carried by parameter and column definitions.
const MYSQL_TYPE_DECIMAL: u8 = 0x00;
const MYSQL_TYPE_TINY: u8 = 0x01;
const MYSQL_TYPE_SHORT: u8 = 0x02;
const MYSQL_TYPE_LONG: u8 = 0x03;
const MYSQL_TYPE_FLOAT: u8 = 0x04;
const MYSQL_TYPE_DOUBLE: u8 = 0x05;
const MYSQL_TYPE_NULL: u8 = 0x06;
const MYSQL_TYPE_LONGLONG: u8 = 0x08;
const MYSQL_TYPE_INT24: u8 = 0x09;
const MYSQL_TYPE_DATE: u8 = 0x0A;
const MYSQL_TYPE_DATETIME: u8 = 0x0C;
const MYSQL_TYPE_YEAR: u8 = 0x0D;
const MYSQL_TYPE_NEWDECIMAL: u8 = 0xF6;
const MYSQL_TYPE_TINY_BLOB: u8 = 0xF9;
const MYSQL_TYPE_MEDIUM_BLOB: u8 = 0xFA;
const MYSQL_TYPE_LONG_BLOB: u8 = 0xFB;
const MYSQL_TYPE_BLOB: u8 = 0xFC;
const MYSQL_TYPE_VAR_STRING: u8 = 0xFD;
const MYSQL_TYPE_STRING: u8 = 0xFE;

/// Emulated `SHOW VARIABLES` table. Values describe what this gateway actually
/// implements: no strict mode, a single-node engine, and utf8mb4 text.
const MYSQL_VARIABLES: &[(&str, &str)] = &[
    ("autocommit", "ON"),
    ("character_set_client", "utf8mb4"),
    ("character_set_connection", "utf8mb4"),
    ("character_set_results", "utf8mb4"),
    ("character_set_server", "utf8mb4"),
    ("collation_connection", "utf8mb4_bin"),
    ("collation_server", "utf8mb4_bin"),
    ("hostname", "localhost"),
    ("init_connect", ""),
    ("lower_case_table_names", "0"),
    ("max_allowed_packet", "1048576"),
    ("net_write_timeout", "60"),
    ("socket", ""),
    ("sql_mode", ""),
    ("time_zone", "SYSTEM"),
    ("transaction_isolation", "READ-COMMITTED"),
    ("tx_isolation", "READ-COMMITTED"),
    ("version", "8.0.36-yyds"),
    ("version_comment", "YYDS MySQL gateway"),
    ("wait_timeout", "28800"),
];

/// Runtime dependencies injected into a MySQL connection so SQL SELECT and DDL can execute.
#[derive(Debug)]
pub struct MysqlSqlRuntime {
    /// Local schema registry holding catalog snapshots and applying DDL.
    pub schema: Arc<SqlSchemaRegistry>,
    /// KV execution plane.
    pub executor: Arc<dyn KeyValueExecutor>,
}

/// Resolves a `SELECT @@version` style system-variable probe against [`MYSQL_VARIABLES`].
///
/// The scope prefix (`@@global.` / `@@session.` / `@@local.`) is accepted and
/// ignored: the gateway keeps one immutable variable table for every scope.
/// Numeric-looking values are returned as numbers so drivers reading
/// `@@max_allowed_packet` or `@@lower_case_table_names` get integer semantics.
///
/// `autocommit` is the one variable the session owns. Drivers read it back to
/// decide whether they still need to disable it, so answering with the static
/// table would hide a session that already switched transactions off.
fn mysql_system_variable(name: &str, autocommit: bool) -> Result<SqlBoundLiteral, SqlFrontendError> {
    let bare = name.strip_prefix("@@").unwrap_or(name);
    let variable = bare.rsplit('.').next().unwrap_or(bare).to_ascii_lowercase();
    let value = MYSQL_VARIABLES
        .iter()
        .find(|(candidate, _)| *candidate == variable)
        .map(|(_, value)| *value)
        .ok_or_else(|| SqlFrontendError::new(SqlErrorKind::UnknownVariable, format!("Unknown system variable '{variable}'")))?;
    let value = if variable == "autocommit" { if autocommit { "ON" } else { "OFF" } } else { value };
    if !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit()) {
        if let Ok(number) = value.parse::<i64>() {
            return Ok(SqlBoundLiteral::Integer(number));
        }
    }
    Ok(SqlBoundLiteral::String(value.to_string()))
}

/// MySQL drivers spell boolean session values as `ON`/`OFF`, which Oak reads as
/// bare identifiers rather than literals. Rewrite only that trailing literal as
/// `1`/`0` — the numeric form binds to an integer the session layer can read —
/// and leave the rest of the statement to Oak.
fn normalize_session_boolean(sql: &str) -> Cow<'_, str> {
    let trimmed = sql.trim_end();
    let (statement, terminator) = match trimmed.strip_suffix(';') {
        Some(statement) => (statement.trim_end(), ";"),
        None => (trimmed, ""),
    };
    if !statement.get(..4).is_some_and(|prefix| prefix.eq_ignore_ascii_case("SET ")) {
        return Cow::Borrowed(sql);
    }
    let Some(index) = statement.rfind('=')
    else {
        return Cow::Borrowed(sql);
    };
    let value = statement[index + 1..].trim();
    let replacement = if value.eq_ignore_ascii_case("ON") {
        "1"
    }
    else if value.eq_ignore_ascii_case("OFF") {
        "0"
    }
    else {
        return Cow::Borrowed(sql);
    };
    Cow::Owned(format!("{} {replacement}{terminator}", &statement[..=index]))
}

/// Whether a `SET` target names a known MySQL session variable the gateway
/// emulates via `MYSQL_VARIABLES`. Rejects unknown names with `1193` at the
/// protocol layer instead of silently accepting them.
fn is_known_mysql_variable(name: &str) -> bool {
    let bare = name.strip_prefix("@@").unwrap_or(name);
    let short = bare.rsplit('.').next().unwrap_or(bare);
    MYSQL_VARIABLES.iter().any(|(key, _)| key.eq_ignore_ascii_case(short))
}

/// Performs a loopback-only unauthenticated startup and executes SQL against the provided runtime.
pub fn serve_connection(stream: TcpStream, runtime: Option<Arc<MysqlSqlRuntime>>) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "MySQL requires loopback until authentication and TLS are implemented"));
    }
    // 结果集由多个小包依次写出，关闭 Nagle 让每包立即发出。留着 Nagle 时
    // Linux 会与对端的延迟 ACK 叠加出约 40ms 的往返停顿。
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    serve_session(stream, runtime)
}

/// Serves a loopback session that can be interrupted by the owning node.
///
/// 套接字保持阻塞模式：数据到达由内核即时唤醒线程，不再有 sleep 轮询地板。
/// 读超时只是停机标志的检查节拍（取消延迟上界 = `CANCELLABLE_READ_TIMEOUT`），
/// 写超时设长，避免慢客户端下半包被超时打断丢数据。
pub fn serve_cancellable(stream: TcpStream, stopping: Arc<AtomicBool>, runtime: Option<Arc<MysqlSqlRuntime>>) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "MySQL requires loopback until authentication and TLS are implemented"));
    }
    // 同 `serve_connection`：结果集的小包必须立即发出，否则延迟 ACK 会带来
    // 约 40ms 的往返地板，把取消检查节拍之外的延迟也一并抬高。
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(CANCELLABLE_READ_TIMEOUT))?;
    stream.set_write_timeout(Some(CANCELLABLE_WRITE_TIMEOUT))?;
    serve_session(CancellableSocket { stream, stopping }, runtime)
}

/// MySQL session-scoped transaction state. Unlike PostgreSQL there is no wire-protocol
/// ReadyForQuery byte for transaction mode, so we only track the storage handle and failure.
struct MysqlTransactionSession {
    executor: Option<Arc<dyn KeyValueExecutor>>,
    handle: Option<Box<dyn TransactionHandle + 'static>>,
    failed: bool,
    /// Whether `autocommit` session variable is ON (default). `SET autocommit = OFF`
    /// clears the `SERVER_STATUS_AUTOCOMMIT` flag on subsequent OK / EOF responses.
    autocommit: bool,
    /// Schema selected by the handshake database field or `COM_INIT_DB`, used
    /// verbatim by `DATABASE()` and the synthesized `TABLE_SCHEMA` columns.
    schema: String,
    /// Registry snapshot captured at `BEGIN`. The catalog rides the same
    /// transaction as rows: `COMMIT` keeps it, every rollback path restores it.
    schema_checkpoint: Option<SqlSchemaCheckpoint>,
}

impl MysqlTransactionSession {
    fn new() -> Self {
        Self { executor: None, handle: None, failed: false, autocommit: true, schema: String::new(), schema_checkpoint: None }
    }

    /// Captures the registry state when a transaction block opens.
    fn capture_schema(&mut self, runtime: Option<&Arc<MysqlSqlRuntime>>) {
        self.schema_checkpoint = runtime.map(|rt| rt.schema.checkpoint());
    }

    /// Restores the snapshot captured at `BEGIN`. Only rollback paths call
    /// this — a successful `COMMIT` drops the checkpoint instead.
    fn restore_schema(&mut self, runtime: Option<&Arc<MysqlSqlRuntime>>) -> io::Result<()> {
        if let (Some(checkpoint), Some(rt)) = (self.schema_checkpoint.take(), runtime) {
            rt.schema.restore(checkpoint).map_err(|error| io::Error::other(error.message))?;
        }
        Ok(())
    }

    fn fail(&mut self) {
        if self.handle.is_some() {
            self.failed = true;
        }
    }

    /// Opens the transaction that `SET autocommit = 0` implies.
    ///
    /// MySQL clients switch transactions on with a session variable instead of
    /// sending `BEGIN`, so the first write of such a connection has to open
    /// the handle lazily. Without it every statement would commit on its own
    /// and the client's `ROLLBACK` would have nothing to undo.
    fn begin_implicit_transaction(&mut self, runtime: &Arc<MysqlSqlRuntime>) -> Result<(), database_types::cluster::Error> {
        if self.handle.is_some() {
            return Ok(());
        }
        let handle = Arc::clone(&runtime.executor).begin_transaction(None)?;
        self.executor = Some(runtime.executor.clone());
        self.handle = Some(handle);
        self.failed = false;
        self.capture_schema(Some(runtime));
        Ok(())
    }

    /// Drops the session without publishing anything: an open transaction is
    /// rolled back and the catalog checkpoint restored.
    fn abandon(&mut self, runtime: Option<&Arc<MysqlSqlRuntime>>) {
        if let Some(handle) = self.handle.take() {
            handle.rollback();
        }
        self.executor = None;
        self.failed = false;
        let _ = self.restore_schema(runtime);
    }

    /// Parse a session-variable value (`ON` / `OFF` / `1` / `0` / `TRUE` / `FALSE`)
    /// into a boolean. Treats anything not explicitly falsy as true (MySQL default).
    fn parse_bool_value(s: &str) -> bool {
        matches!(s.to_ascii_lowercase().as_str(), "on" | "1" | "true" | "yes")
    }

    /// MySQL server-status flags for the current OK / EOF response.
    /// `SERVER_STATUS_IN_TRANS` when an explicit transaction is open;
    /// `SERVER_STATUS_AUTOCOMMIT` when autocommit is on and no explicit handle;
    /// 0 when autocommit is off and no explicit handle.
    fn status_flags(&self) -> u16 {
        if self.handle.is_some() {
            SERVER_STATUS_IN_TRANS
        }
        else if self.autocommit {
            SERVER_STATUS_AUTOCOMMIT
        }
        else {
            0
        }
    }
}

impl Default for MysqlTransactionSession {
    fn default() -> Self {
        Self::new()
    }
}

/// `COM_RESET_CONNECTION` (0x1F): the session must look like a brand-new
/// connection without re-authenticating or rebuilding the socket. Any open
/// explicit transaction is rolled back — never committed — because the gateway
/// keeps one commit point per command, autocommit returns to its default ON,
/// the aborted flag clears and every prepared statement handle is dropped
/// (real MySQL also invalidates them on reset).
fn reset_connection_session(
    session: &mut MysqlTransactionSession,
    statements: &mut PreparedStatements,
    runtime: Option<&Arc<MysqlSqlRuntime>>,
) -> io::Result<()> {
    if let Some(handle) = session.handle.take() {
        handle.rollback();
    }
    session.executor = None;
    session.failed = false;
    session.autocommit = true;
    session.schema.clear();
    statements.entries.clear();
    // Resetting a connection discards an open transaction, catalog changes included.
    session.restore_schema(runtime)
}

/// A prepared statement as advertised by `COM_STMT_PREPARE`: the SQL text, the
/// catalog-derived result shape and the parameter types the client last bound.
struct PreparedStatement {
    sql: String,
    /// Number of `?` placeholders the statement references.
    parameter_count: usize,
    /// Result columns derived from the catalog. Writes describe to no columns.
    description: Vec<SqlColumn>,
    /// `(type code, unsigned flag)` per parameter, remembered so an execute
    /// that reuses the previous binding can decode values.
    parameter_types: Vec<(u8, bool)>,
}

/// Per-connection prepared statement registry. Statement ids are never reused
/// and the whole table is dropped when the session ends.
struct PreparedStatements {
    entries: HashMap<u32, PreparedStatement>,
    next_id: u32,
}

impl PreparedStatements {
    fn new() -> Self {
        Self { entries: HashMap::new(), next_id: 1 }
    }

    fn allocate(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        id
    }
}

fn serve_session(mut stream: impl Read + Write, runtime: Option<Arc<MysqlSqlRuntime>>) -> io::Result<()> {
    let mut session = MysqlTransactionSession::new();
    let mut statements = PreparedStatements::new();
    write_packet(&mut stream, 0, &handshake())?;
    let response = match read_packet(&mut stream)? {
        ReadPacket::Packet(1, response) => response,
        ReadPacket::Packet(_, _) => return Err(invalid_data("invalid MySQL handshake sequence")),
        ReadPacket::TooLarge(sequence) => {
            write_packet_too_large(&mut stream, sequence)?;
            return Ok(());
        }
        ReadPacket::Closed => {
            let _ = session.restore_schema(runtime.as_ref());
            return Ok(());
        }
    };
    let (username, empty_password, database) = match parse_handshake_response(&response) {
        Ok(parsed) => parsed,
        Err(error) => {
            write_error(&mut stream, 2, 1043, "08S01", error)?;
            return Ok(());
        }
    };
    if username.is_empty() || !empty_password {
        write_error(&mut stream, 2, 1045, "28000", "access denied: loopback accepts only an empty password")?;
        return Ok(());
    }
    session.schema = database.unwrap_or_default();
    write_ok(&mut stream, 2, session.status_flags())?;

    loop {
        let (sequence, payload) = match read_packet(&mut stream)? {
            ReadPacket::Packet(sequence, payload) => (sequence, payload),
            ReadPacket::TooLarge(sequence) => {
                write_packet_too_large(&mut stream, sequence)?;
                session.abandon(runtime.as_ref());
                return Ok(());
            }
            ReadPacket::Closed => {
                // A connection that goes away mid-transaction is an implicit rollback.
                session.abandon(runtime.as_ref());
                return Ok(());
            }
        };
        if sequence != 0 {
            write_error(&mut stream, sequence.wrapping_add(1), 1047, "08S01", "invalid command sequence")?;
            continue;
        }
        if payload.is_empty() {
            write_error(&mut stream, sequence.wrapping_add(1), 1047, "08S01", "empty command")?;
            continue;
        }
        match payload[0] {
            COM_QUIT => {
                session.abandon(runtime.as_ref());
                return Ok(());
            }
            // Drivers issue COM_PING to prove liveness before creating a cursor
            // and COM_INIT_DB when switching schema. The gateway keeps a single
            // catalog, so `COM_INIT_DB` only records the new default schema and
            // must not reset any session state, while `COM_PING` is a no-op.
            // Both are answered with the protocol OK packet.
            COM_PING => write_ok(&mut stream, sequence.wrapping_add(1), session.status_flags())?,
            COM_INIT_DB => match std::str::from_utf8(&payload[1..]) {
                Ok(schema) => {
                    session.schema = schema.to_string();
                    write_ok(&mut stream, sequence.wrapping_add(1), session.status_flags())?;
                }
                Err(_) => write_error(&mut stream, sequence.wrapping_add(1), 1049, "42000", "database name is not valid UTF-8")?,
            },
            // Connection pools (mysql-connector, SQLAlchemy `reset`) use this to
            // return an idle connection to its initial state. The socket and the
            // authentication stay untouched; only session state is reset.
            COM_RESET_CONNECTION => {
                reset_connection_session(&mut session, &mut statements, runtime.as_ref())?;
                write_ok(&mut stream, sequence.wrapping_add(1), session.status_flags())?;
            }
            COM_STMT_PREPARE => {
                handle_prepare(&mut stream, sequence.wrapping_add(1), &payload, runtime.as_ref(), &mut statements, &mut session)?
            }
            COM_STMT_EXECUTE => {
                handle_execute(&mut stream, sequence.wrapping_add(1), &payload, runtime.as_ref(), &mut statements, &mut session)?
            }
            COM_STMT_CLOSE => {
                if let Some(id) = take_u32(&payload[1..]) {
                    statements.entries.remove(&id);
                }
            }
            COM_STMT_RESET => handle_reset(&mut stream, sequence.wrapping_add(1), &payload, &statements, session.status_flags())?,
            COM_QUERY => {
                let sql = match std::str::from_utf8(&payload[1..]) {
                    Ok(sql) => sql,
                    Err(_) => {
                        write_error(&mut stream, sequence.wrapping_add(1), 1300, "HY000", "query is not valid UTF-8")?;
                        continue;
                    }
                };
                // DDL resolves against the local schema registry instead of the
                // session-command parser, so a `CREATE` / `ALTER` / `DROP` that the
                // registry cannot apply never falls through to the Oak binder.
                match parse_ddl(sql, SqlDialect::Mysql) {
                    Err(error) => {
                        write_statement_error(&mut stream, sequence.wrapping_add(1), "ddl", error)?;
                        continue;
                    }
                    Ok(Some(statement)) => {
                        let Some(rt) = runtime.as_ref()
                        else {
                            write_error(&mut stream, sequence.wrapping_add(1), 1235, "42000", "no SQL runtime")?;
                            continue;
                        };
                        if session.failed {
                            write_error(&mut stream, sequence.wrapping_add(1), 40001, "HY000", "current transaction is aborted")?;
                            continue;
                        }
                        match rt.schema.apply_ddl(&statement) {
                            Ok(_) => write_ok(&mut stream, sequence.wrapping_add(1), session.status_flags())?,
                            Err(error) => write_statement_error(&mut stream, sequence.wrapping_add(1), "ddl", error)?,
                        }
                        continue;
                    }
                    Ok(None) => {}
                }
                // Metadata views never touch the executor or the session
                // transaction: they are synthesized from the local schema registry.
                match analyze_meta_query(sql, SqlDialect::Mysql) {
                    Err(error) => {
                        session.fail();
                        write_statement_error(&mut stream, sequence.wrapping_add(1), "metadata", error)?;
                        continue;
                    }
                    Ok(Some(meta)) => {
                        let Some(rt) = runtime.as_ref()
                        else {
                            write_error(&mut stream, sequence.wrapping_add(1), 1235, "42000", "no SQL runtime")?;
                            continue;
                        };
                        let described = meta.describe();
                        let result = meta.execute(&rt.schema, &session.schema);
                        let column_types: Vec<u8> = described.iter().map(|column| mysql_field_type(&column.field_type)).collect();
                        let column_nullables: Vec<bool> = described.iter().map(|column| column.field_type.is_nullable()).collect();
                        write_result_set(
                            &mut stream,
                            sequence.wrapping_add(1),
                            &result,
                            &column_types,
                            &column_nullables,
                            false,
                            session.status_flags(),
                        )?;
                        continue;
                    }
                    Ok(None) => {}
                }
                match parse_session_command(&normalize_session_boolean(sql)) {
                    Ok(Some(SqlSessionCommand::SetNames { character_set, .. })) if !character_set.eq_ignore_ascii_case("utf8mb4") => {
                        write_error(&mut stream, sequence.wrapping_add(1), 1115, "42000", "unknown character set")?;
                    }
                    // 文本按 code point 精确比较，collation 只是会话元数据；接受与 DDL
                    // 同一份白名单，驱动默认的 `utf8mb4_0900_ai_ci` 也能建立连接。
                    Ok(Some(SqlSessionCommand::SetNames { collation, .. })) if collation.as_deref().is_none_or(is_accepted_collation) => {
                        write_ok(&mut stream, sequence.wrapping_add(1), session.status_flags())?;
                    }
                    Ok(Some(SqlSessionCommand::SetNames { collation, .. })) => {
                        // Engine text for `ER_UNKNOWN_COLLATION`.
                        let name = collation.as_deref().unwrap_or("utf8mb4");
                        write_error(&mut stream, sequence.wrapping_add(1), 1273, "HY000", &format!("Unknown collation: '{name}'"))?;
                    }
                    Ok(Some(SqlSessionCommand::Transaction(action))) => match action {
                        SqlTransactionAction::Begin if session.failed => {
                            write_error(&mut stream, sequence.wrapping_add(1), 40001, "HY000", "current transaction is aborted")?;
                        }
                        SqlTransactionAction::Begin => {
                            let Some(rt) = runtime.as_ref()
                            else {
                                write_error(&mut stream, sequence.wrapping_add(1), 1235, "42000", "no SQL runtime")?;
                                continue;
                            };
                            // The executor owns the transaction capability contract: BEGIN goes
                            // through `begin_transaction` so an executor without a durable atomic
                            // commit path rejects the verb at its boundary. The single-node
                            // gateway passes `None` explicitly — there is no coordinator fence yet.
                            match Arc::clone(&rt.executor).begin_transaction(None) {
                                Ok(handle) => {
                                    session.executor = Some(rt.executor.clone());
                                    session.handle = Some(handle);
                                    session.failed = false;
                                    session.capture_schema(runtime.as_ref());
                                    write_ok(&mut stream, sequence.wrapping_add(1), SERVER_STATUS_IN_TRANS)?;
                                }
                                Err(err) => {
                                    write_error(&mut stream, sequence.wrapping_add(1), 1235, "42000", &format!("BEGIN failed: {err}"))?;
                                }
                            }
                        }
                        SqlTransactionAction::Commit if session.failed => {
                            if let Some(handle) = session.handle.take() {
                                handle.rollback();
                            }
                            session.executor = None;
                            session.failed = false;
                            // `COMMIT` of an aborted block is a rollback: catalog changes go too.
                            session.restore_schema(runtime.as_ref())?;
                            write_ok(&mut stream, sequence.wrapping_add(1), session.status_flags())?;
                        }
                        SqlTransactionAction::Commit => {
                            let mut commit_error = None;
                            if let Some(handle) = session.handle.take() {
                                if let Err(err) = handle.commit() {
                                    commit_error = Some(err);
                                }
                            }
                            session.executor = None;
                            session.failed = false;
                            match commit_error {
                                Some(err) => {
                                    // A failed commit did not publish the transaction, so
                                    // the DDL it collected is discarded too.
                                    session.restore_schema(runtime.as_ref())?;
                                    match err {
                                        // OCC 拒绝必须回报给客户端，否则并发写会被静默丢弃。
                                        err @ database_types::cluster::Error::CasConflict { .. } => write_error(
                                            &mut stream,
                                            sequence.wrapping_add(1),
                                            1213,
                                            "40001",
                                            &format!("COMMIT failed: serialization conflict: {err}"),
                                        )?,
                                        err => {
                                            write_error(&mut stream, sequence.wrapping_add(1), 1105, "HY000", &format!("COMMIT failed: {err}"))?
                                        }
                                    }
                                }
                                None => {
                                    // The commit point publishes the schema revision as well.
                                    session.schema_checkpoint = None;
                                    write_ok(&mut stream, sequence.wrapping_add(1), session.status_flags())?;
                                }
                            }
                        }
                        SqlTransactionAction::Rollback => {
                            if let Some(handle) = session.handle.take() {
                                handle.rollback();
                            }
                            session.executor = None;
                            session.failed = false;
                            session.restore_schema(runtime.as_ref())?;
                            write_ok(&mut stream, sequence.wrapping_add(1), session.status_flags())?;
                        }
                        SqlTransactionAction::Savepoint(_)
                        | SqlTransactionAction::RollbackToSavepoint(_)
                        | SqlTransactionAction::ReleaseSavepoint(_) => {
                            write_error(
                                &mut stream,
                                sequence.wrapping_add(1),
                                1235,
                                "42000",
                                "SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT are not yet supported by the gateway",
                            )?;
                        }
                    },
                    Ok(Some(SqlSessionCommand::Savepoint(intent))) => {
                        use database_query::SavepointIntent;
                        let handle = match session.handle.as_ref() {
                            Some(handle) => handle.as_ref(),
                            None => {
                                session.fail();
                                write_error(
                                    &mut stream,
                                    sequence.wrapping_add(1),
                                    1305,
                                    "42000",
                                    "SAVEPOINT is only valid inside a transaction",
                                )?;
                                continue;
                            }
                        };
                        let result = match intent {
                            SavepointIntent::Create { name } => handle.savepoint_create(&name),
                            SavepointIntent::RollbackTo { name } => handle.savepoint_rollback_to(&name),
                            SavepointIntent::Release { name } => handle.savepoint_release(&name),
                        };
                        match result {
                            Ok(()) => write_ok(&mut stream, sequence.wrapping_add(1), session.status_flags())?,
                            Err(database_types::cluster::Error::Unsupported(msg))
                                if msg.contains("does not exist") || msg.contains("already exists") =>
                            {
                                session.fail();
                                write_error(&mut stream, sequence.wrapping_add(1), 1305, "42000", msg)?;
                            }
                            Err(err) => {
                                session.fail();
                                write_error(&mut stream, sequence.wrapping_add(1), 1105, "HY000", &format!("savepoint failed: {err}"))?;
                            }
                        }
                    }
                    Ok(Some(SqlSessionCommand::ShowVariables { pattern, .. })) => {
                        let mut rows = Vec::new();
                        for (name, value) in MYSQL_VARIABLES {
                            if pattern.as_deref().is_none_or(|pattern| like_matches(pattern, name)) {
                                let effective_value = if *name == "autocommit" { if session.autocommit { "ON" } else { "OFF" } } else { value };
                                rows.push(vec![SqlValue::String((*name).to_string()), SqlValue::String(effective_value.to_string())]);
                            }
                        }
                        let result = SqlResult { columns: vec!["Variable_name".into(), "Value".into()], rows, affected_rows: 0 };
                        write_result_set(&mut stream, sequence.wrapping_add(1), &result, &[], &[false, false], false, session.status_flags())?;
                    }
                    Ok(Some(SqlSessionCommand::SetVariable { name, value, .. })) => {
                        // MySQL 只接受已知 session 变量；未知变量应返回 1193。
                        if !is_known_mysql_variable(&name) {
                            session.fail();
                            write_error(&mut stream, sequence.wrapping_add(1), 1193, "HY000", &format!("Unknown system variable '{name}'"))?;
                            continue;
                        }
                        // Values arrive as `@@session.autocommit` from drivers that
                        // qualify the scope, so strip the same prefix the variable
                        // resolver strips before matching the session flag.
                        let bare = name.strip_prefix("@@").unwrap_or(&name);
                        let needle = bare.rsplit('.').next().unwrap_or(bare).trim().to_ascii_lowercase();
                        if needle == "autocommit" {
                            let autocommit = match value {
                                SqlBoundExpression::Literal(SqlBoundLiteral::String(s)) => MysqlTransactionSession::parse_bool_value(&s),
                                SqlBoundExpression::Literal(SqlBoundLiteral::Integer(n)) => n != 0,
                                _ => true, // 默认 ON
                            };
                            // MySQL commits whatever transaction is open when autocommit
                            // is switched back on, including the handle this session
                            // opened implicitly.
                            if autocommit && !session.autocommit {
                                if let Some(handle) = session.handle.take() {
                                    let outcome = handle.commit();
                                    session.executor = None;
                                    match outcome {
                                        Ok(()) => session.schema_checkpoint = None,
                                        Err(err) => {
                                            session.restore_schema(runtime.as_ref())?;
                                            session.autocommit = true;
                                            write_error(
                                                &mut stream,
                                                sequence.wrapping_add(1),
                                                1105,
                                                "HY000",
                                                &format!("COMMIT failed: {err}"),
                                            )?;
                                            continue;
                                        }
                                    }
                                }
                            }
                            session.autocommit = autocommit;
                        }
                        write_ok(&mut stream, sequence.wrapping_add(1), session.status_flags())?;
                    }
                    Ok(Some(SqlSessionCommand::ShowParameter { name, .. })) => {
                        session.fail();
                        write_error(
                            &mut stream,
                            sequence.wrapping_add(1),
                            1064,
                            "42000",
                            &format!("MySQL requires SHOW VARIABLES instead of SHOW {name}"),
                        )?;
                    }
                    Ok(Some(SqlSessionCommand::Pragma { .. })) => {
                        session.fail();
                        write_error(&mut stream, sequence.wrapping_add(1), 1064, "42000", "PRAGMA is not MySQL syntax")?;
                    }
                    Ok(Some(SqlSessionCommand::ResetVariables { .. })) | Ok(Some(SqlSessionCommand::Discard(_))) => {
                        // `RESET ...` / `DISCARD ...` are PostgreSQL session verbs. MySQL has no
                        // equivalent statement in the gateway subset, so answer with its normal
                        // syntax error instead of pretending to reset state.
                        write_error(
                            &mut stream,
                            sequence.wrapping_add(1),
                            1064,
                            "42000",
                            "RESET / DISCARD session statements are PostgreSQL syntax",
                        )?;
                    }
                    Err(_) => {
                        // 语法错误与“未接入执行器”必须是不同错误，否则没有 runtime 时
                        // 客户端会把语法错误误判为不支持的能力。
                        session.fail();
                        write_error(&mut stream, sequence.wrapping_add(1), 1064, "42000", "Oak could not parse the SQL statement")?;
                    }
                    Ok(None) => {
                        if session.failed {
                            write_error(&mut stream, sequence.wrapping_add(1), 40001, "HY000", "current transaction is aborted")?;
                            continue;
                        }
                        match runtime.as_ref() {
                            Some(rt) => execute_sql(&mut stream, sequence.wrapping_add(1), sql, rt, &mut session, &[], false, &[])?,
                            None => write_error(
                                &mut stream,
                                sequence.wrapping_add(1),
                                1235,
                                "42000",
                                "YYDS MySQL SQL execution is not implemented (no SQL runtime)",
                            )?,
                        }
                    }
                }
            }
            // Every other command byte is either a real MySQL command this
            // gateway does not implement (`COM_CHANGE_USER`, `COM_STMT_FETCH`,
            // `COM_BINLOG_DUMP`, ...) or not a command at all. Answering with
            // `ER_UNKNOWN_COM_ERROR` keeps one request in, one response out: the
            // next packet still starts a new command, so the stream never shifts.
            command => {
                write_error(&mut stream, sequence.wrapping_add(1), ER_UNKNOWN_COM_ERROR, "HY000", &format!("Unknown command {command:#04X}"))?
            }
        }
    }
}

fn handshake() -> Vec<u8> {
    let mut payload = Vec::with_capacity(80);
    payload.push(PROTOCOL_VERSION);
    payload.extend_from_slice(b"8.0.36-yyds\0");
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.extend_from_slice(b"yydsseed");
    payload.push(0);
    payload.extend_from_slice(&(SERVER_CAPABILITIES as u16).to_le_bytes());
    payload.push(45);
    payload.extend_from_slice(&2_u16.to_le_bytes());
    payload.extend_from_slice(&((SERVER_CAPABILITIES >> 16) as u16).to_le_bytes());
    payload.push(21);
    payload.extend_from_slice(&[0; 10]);
    payload.extend_from_slice(b"yydsseed1234");
    payload.push(0);
    payload.extend_from_slice(b"mysql_native_password\0");
    payload
}

fn parse_handshake_response(payload: &[u8]) -> Result<(String, bool, Option<String>), &'static str> {
    if payload.len() < 32 {
        return Err("truncated handshake response");
    }
    let capabilities = u32::from_le_bytes(payload[..4].try_into().map_err(|_| "invalid capabilities")?);
    if capabilities & CLIENT_PROTOCOL_41 == 0 || capabilities & CLIENT_SSL != 0 {
        return Err("protocol 4.1 is required and TLS is unavailable");
    }
    let mut cursor = 32;
    let username = take_nul_bytes(payload, &mut cursor)?;
    let auth_response = if capabilities & CLIENT_PLUGIN_AUTH_LENENC != 0 {
        let auth_length = take_lenenc(payload, &mut cursor)?;
        let start = cursor;
        advance(payload, &mut cursor, auth_length)?;
        &payload[start..cursor]
    }
    else if capabilities & CLIENT_SECURE_CONNECTION != 0 {
        let auth_length = usize::from(*payload.get(cursor).ok_or("missing auth response length")?);
        cursor += 1;
        let start = cursor;
        advance(payload, &mut cursor, auth_length)?;
        &payload[start..cursor]
    }
    else {
        take_nul_bytes(payload, &mut cursor)?
    };
    let mut database = None;
    if capabilities & CLIENT_CONNECT_WITH_DB != 0 {
        let value = take_nul_bytes(payload, &mut cursor)?;
        let value = String::from_utf8(value.to_vec()).map_err(|_| "database is not UTF-8")?;
        if !value.is_empty() {
            database = Some(value);
        }
    }
    if capabilities & CLIENT_PLUGIN_AUTH != 0 && cursor < payload.len() {
        let _ = take_nul_bytes(payload, &mut cursor)?;
    }
    let username = String::from_utf8(username.to_vec()).map_err(|_| "username is not UTF-8")?;
    Ok((username, auth_response.is_empty(), database))
}

fn take_nul_bytes<'a>(payload: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], &'static str> {
    let rest = payload.get(*cursor..).ok_or("truncated handshake response")?;
    let length = rest.iter().position(|byte| *byte == 0).ok_or("unterminated handshake field")?;
    let value = &rest[..length];
    *cursor += length + 1;
    Ok(value)
}

fn take_lenenc(payload: &[u8], cursor: &mut usize) -> Result<usize, &'static str> {
    let first = *payload.get(*cursor).ok_or("missing auth response length")?;
    *cursor += 1;
    match first {
        0..=250 => Ok(usize::from(first)),
        251 => Err("NULL auth response length is invalid"),
        252 => take_integer(payload, cursor, 2),
        253 => take_integer(payload, cursor, 3),
        254 => take_integer(payload, cursor, 8),
        _ => Err("invalid auth response length"),
    }
}

fn take_integer(payload: &[u8], cursor: &mut usize, length: usize) -> Result<usize, &'static str> {
    let bytes = payload.get(*cursor..*cursor + length).ok_or("truncated length-encoded integer")?;
    *cursor += length;
    let mut value = 0_usize;
    for (shift, byte) in bytes.iter().enumerate() {
        if shift * 8 >= usize::BITS as usize && *byte != 0 {
            return Err("length-encoded integer overflow");
        }
        if shift * 8 < usize::BITS as usize {
            value |= usize::from(*byte) << (shift * 8);
        }
    }
    Ok(value)
}

fn advance(payload: &[u8], cursor: &mut usize, length: usize) -> Result<(), &'static str> {
    if length > MAX_PACKET || payload.get(*cursor..*cursor + length).is_none() {
        return Err("truncated or oversized auth response");
    }
    *cursor += length;
    Ok(())
}

fn read_packet(stream: &mut impl Read) -> io::Result<ReadPacket> {
    let mut header = [0; 4];
    let mut read = 0;
    while read < header.len() {
        match stream.read(&mut header[read..]) {
            Ok(0) if read == 0 => return Ok(ReadPacket::Closed),
            Ok(0) => return Err(invalid_data("truncated MySQL packet header")),
            Ok(count) => read += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    let length = usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    if length > MAX_PACKET {
        return Ok(ReadPacket::TooLarge(header[3]));
    }
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload)?;
    Ok(ReadPacket::Packet(header[3], payload))
}

/// Result of one wire frame read.
///
/// `TooLarge` is separated from I/O errors so the session can answer with
/// MySQL `1153` before closing instead of dropping the connection silently.
enum ReadPacket {
    Packet(u8, Vec<u8>),
    TooLarge(u8),
    Closed,
}

fn write_packet_too_large(stream: &mut impl Write, sequence: u8) -> io::Result<()> {
    write_error(stream, sequence.wrapping_add(1), 1153, "08S01", "Got a packet bigger than 'max_allowed_packet' bytes (1048576)")
}

fn write_packet(stream: &mut impl Write, sequence: u8, payload: &[u8]) -> io::Result<()> {
    let packet = encode_packet(sequence, payload).map_err(packet_error)?;
    stream.write_all(&packet)
}

fn write_ok(stream: &mut impl Write, sequence: u8, status_flags: u16) -> io::Result<()> {
    // OK identifier + affected_rows (length-encoded 0) + last_insert_id (length-encoded 0) + status_flags + warnings
    let mut payload = Vec::with_capacity(9);
    payload.push(0x00);
    payload.push(0x00); // affected_rows = 0
    payload.push(0x00); // last_insert_id = 0
    payload.extend_from_slice(&status_flags.to_le_bytes());
    payload.extend_from_slice(&0_u16.to_le_bytes());
    write_packet(stream, sequence, &payload)
}

fn write_error(stream: &mut impl Write, sequence: u8, code: u16, state: &str, message: &str) -> io::Result<()> {
    let mut payload = Vec::with_capacity(message.len() + 9);
    payload.push(0xff);
    payload.extend_from_slice(&code.to_le_bytes());
    payload.push(b'#');
    payload.extend_from_slice(state.as_bytes());
    payload.extend_from_slice(message.as_bytes());
    write_packet(stream, sequence, &payload)
}

/// 把一条 SQL 走完整的 bind → plan → execute → wire 路径。
/// 根据 plan variant 分发：SELECT → ResultSet；INSERT/UPDATE/DELETE → OK，
/// `affected_rows` 直接取执行器返回的真实行数（多行 INSERT 计 N、UPDATE/DELETE 计命中行数），
/// 不再回固定值。
/// `column_types` 为空时按行值推断列类型，否则使用调用方已经声明的类型。
/// `binary_rows` 为真时按 MySQL binary protocol 编码结果行（prepared statement 路径）。
fn execute_sql(
    stream: &mut impl Write,
    sequence: u8,
    sql: &str,
    runtime: &Arc<MysqlSqlRuntime>,
    session: &mut MysqlTransactionSession,
    column_types: &[u8],
    binary_rows: bool,
    column_nullables: &[bool],
) -> io::Result<()> {
    match analyze_meta_query(sql, SqlDialect::Mysql) {
        Err(error) => {
            session.fail();
            write_statement_error(stream, sequence, "metadata", error)?;
            return Ok(());
        }
        Ok(Some(meta)) => {
            let described = meta.describe();
            let result = meta.execute(&runtime.schema, &session.schema);
            let declared_types: Vec<u8> = described.iter().map(|column| mysql_field_type(&column.field_type)).collect();
            let declared_nullables: Vec<bool> = described.iter().map(|column| column.field_type.is_nullable()).collect();
            let column_types = if column_types.is_empty() { &declared_types } else { column_types };
            let column_nullables = if column_nullables.is_empty() { &declared_nullables } else { column_nullables };
            return write_result_set(stream, sequence, &result, column_types, column_nullables, binary_rows, session.status_flags());
        }
        Ok(None) => {}
    }
    let catalog = runtime.schema.catalog();
    let bound = match bind_sql_catalog_with_variables(
        sql,
        &catalog,
        catalog.schema_version(),
        &|name: &str| mysql_system_variable(name, session.autocommit),
        SqlDialect::Mysql,
    ) {
        Ok(bound) => bound,
        Err(error) => {
            session.fail();
            write_statement_error(stream, sequence, "bind", error)?;
            return Ok(());
        }
    };
    // `AUTO_INCREMENT` / serial columns default at write time, after binding and
    // before planning. The counter is consumed before execution and survives a
    // rollback, matching both MySQL and PostgreSQL sequence semantics.
    let columns_before = bound_insert_column_count(&bound);
    let bound = match apply_auto_increment(&runtime.schema, bound) {
        Ok(bound) => bound,
        Err(error) => {
            session.fail();
            write_statement_error(stream, sequence, "bind", error)?;
            return Ok(());
        }
    };
    // The engine reports the generated value in the OK packet; drivers surface
    // it as `insertId` / `lastrowid`.
    let last_insert_id = if bound_insert_column_count(&bound) > columns_before { allocated_last_insert_id(&bound) } else { None };
    let plan = match plan_statement_for_dialect(bound, &catalog, SqlDialect::Mysql) {
        Ok(plan) => plan,
        Err(error) => {
            session.fail();
            write_statement_error(stream, sequence, "plan", error)?;
            return Ok(());
        }
    };

    // DML 写操作 —— 先 execute 再回 OK packet
    let is_write = !matches!(
        plan,
        SqlExecutionPlan::Read(_)
            | SqlExecutionPlan::Scan(_)
            | SqlExecutionPlan::Join(_)
            | SqlExecutionPlan::Scalar(_)
            | SqlExecutionPlan::SetOperation(_)
    );
    // MySQL drivers enable transactions with `SET autocommit = 0` rather than
    // sending `BEGIN`, so the first write has to open the handle lazily before
    // it can join the client's transaction block.
    if is_write && session.handle.is_none() && !session.autocommit {
        if let Err(err) = session.begin_implicit_transaction(runtime) {
            session.fail();
            write_error(stream, sequence.wrapping_add(1), 1235, "42000", &format!("BEGIN failed: {err}"))?;
            return Ok(());
        }
    }
    let handle_ref: Option<&dyn TransactionHandle> = session.handle.as_deref();
    let result = match execute_plan(runtime.executor.as_ref(), handle_ref, &plan, SqlDialect::Mysql) {
        Ok(result) => result,
        Err(error) => {
            session.fail();
            write_statement_error(stream, sequence, "execution", error)?;
            return Ok(());
        }
    };
    if is_write {
        write_ok_with_affected(stream, sequence, result.affected_rows, last_insert_id, session.status_flags())?;
        return Ok(());
    }
    // Text protocol (`COM_QUERY`) has no prepare-time description, so derive the
    // declared column types from the plan instead of guessing from row values.
    let declared_columns = describe_plan_for_dialect(&plan, SqlDialect::Mysql);
    let declared_types: Vec<u8> = declared_columns.iter().map(|column| mysql_field_type(&column.field_type)).collect();
    let declared_nullables: Vec<bool> = declared_columns.iter().map(|column| column.field_type.is_nullable()).collect();
    let column_types = if column_types.is_empty() { &declared_types } else { column_types };
    let column_nullables = if column_nullables.is_empty() { &declared_nullables } else { column_nullables };
    write_result_set(stream, sequence, &result, column_types, column_nullables, binary_rows, session.status_flags())
}

/// Maps a structured gateway failure to its MySQL error number and SQLSTATE.
fn mysql_error_code(kind: SqlErrorKind) -> (u16, &'static str) {
    use SqlErrorKind::*;
    match kind {
        Syntax => (1064, "42000"),
        UnknownTable | UnknownTableQualifier => (1146, "42S02"),
        AmbiguousTable | AmbiguousColumn => (1052, "23000"),
        UnknownColumn => (1054, "42S22"),
        TypeMismatch => (1366, "HY000"),
        ValueTooLong => (1406, "22001"),
        NotNullViolation => (1048, "23000"),
        // 真库 8.4 对「INSERT 缺 NOT NULL 且无 DEFAULT 的列」报 `ER_NO_DEFAULT_FOR_FIELD`。
        MissingDefault => (1364, "HY000"),
        Unsupported => (1235, "42000"),
        UnknownFunction => (1305, "42000"),
        UnknownVariable => (1193, "HY000"),
        // 只有无法分类的 catalog/storage 失败才落到通用码。
        Internal => (1105, "HY000"),
        CardinalityViolation => (1242, "21000"),
        InvalidGrouping => (1055, "42000"),
        OperandColumns => (1241, "21000"),
        Parameter => (1210, "HY000"),
        DuplicateKey => (1062, "23000"),
        Conflict => (1213, "40001"),
        DivisionByZero => (1365, "22012"),
        // MySQL 用 `1690` 报 BIGINT 越界，SQLSTATE 与 PostgreSQL 一致是 `22003`。
        NumericOverflow => (1690, "22003"),
        // PostgreSQL-only 分类：MySQL 对负长度返回空串，因此不会走到这里。
        InvalidSubstringLength => (1105, "HY000"),
        TableAlreadyExists => (1050, "42S01"),
        ColumnAlreadyExists => (1060, "42S21"),
        IndexAlreadyExists => (1061, "42000"),
        UnknownIndex => (1091, "42000"),
    }
}

fn write_statement_error(stream: &mut impl Write, sequence: u8, stage: &str, error: SqlFrontendError) -> io::Result<()> {
    let (code, state) = mysql_error_code(error.kind);
    write_error(stream, sequence, code, state, &format!("SQL {stage} error: {}", error.message))
}

/// `COM_STMT_PREPARE`: validates names against the catalog, derives the result
/// shape and returns the statement id with parameter and column definitions.
fn handle_prepare(
    stream: &mut impl Write,
    sequence: u8,
    payload: &[u8],
    runtime: Option<&Arc<MysqlSqlRuntime>>,
    statements: &mut PreparedStatements,
    session: &mut MysqlTransactionSession,
) -> io::Result<()> {
    let sql = match std::str::from_utf8(&payload[1..]) {
        Ok(sql) => sql,
        Err(_) => {
            write_error(stream, sequence, 1300, "HY000", "query is not valid UTF-8")?;
            return Ok(());
        }
    };
    let Some(runtime) = runtime
    else {
        session.fail();
        write_error(stream, sequence, 1235, "42000", "YYDS MySQL SQL execution is not implemented (no SQL runtime)")?;
        return Ok(());
    };
    let (neutral, parameter_count) = match neutralized_sql_parameters(sql, SqlParameterStyle::QuestionMark) {
        Ok(result) => result,
        Err(error) => {
            session.fail();
            write_statement_error(stream, sequence, "parameter", error)?;
            return Ok(());
        }
    };
    let description = match analyze_meta_query(&neutral, SqlDialect::Mysql) {
        Err(error) => {
            session.fail();
            write_statement_error(stream, sequence, "metadata", error)?;
            return Ok(());
        }
        Ok(Some(meta)) => meta.describe(),
        Ok(None) => {
            let catalog = runtime.schema.catalog();
            let bound = match bind_sql_catalog_with_variables(
                &neutral,
                &catalog,
                catalog.schema_version(),
                &|name: &str| mysql_system_variable(name, session.autocommit),
                SqlDialect::Mysql,
            ) {
                Ok(bound) => bound,
                Err(error) => {
                    session.fail();
                    write_statement_error(stream, sequence, "bind", error)?;
                    return Ok(());
                }
            };
            // 写入语句的值类型检查推迟到 execute，因为只有执行时才知道参数的真实类型，
            // 因此这里只为 SELECT 推导结果列。
            match bound {
                SqlCatalogBoundStatement::Select(_) | SqlCatalogBoundStatement::SetOperation(_) => {
                    match plan_statement_for_dialect(bound, &catalog, SqlDialect::Mysql) {
                        Ok(plan) => describe_plan_for_dialect(&plan, SqlDialect::Mysql),
                        Err(error) => {
                            session.fail();
                            write_statement_error(stream, sequence, "plan", error)?;
                            return Ok(());
                        }
                    }
                }
                _ => Vec::new(),
            }
        }
    };

    let id = statements.allocate();
    write_prepare_ok(stream, sequence, id, parameter_count as u16, description.len() as u16)?;
    let mut next = sequence.wrapping_add(1);
    if parameter_count > 0 {
        for _ in 0..parameter_count {
            write_packet(stream, next, &encode_column_definition("?", MYSQL_TYPE_VAR_STRING, false))?;
            next = next.wrapping_add(1);
        }
        write_eof(stream, next, session.status_flags())?;
        next = next.wrapping_add(1);
    }
    if !description.is_empty() {
        for column in &description {
            write_packet(
                stream,
                next,
                &encode_column_definition(&column.name, mysql_field_type(&column.field_type), column.field_type.is_nullable()),
            )?;
            next = next.wrapping_add(1);
        }
        write_eof(stream, next, session.status_flags())?;
    }
    statements.entries.insert(id, PreparedStatement { sql: sql.to_string(), description, parameter_types: Vec::new(), parameter_count });
    Ok(())
}

fn write_prepare_ok(stream: &mut impl Write, sequence: u8, statement_id: u32, parameter_count: u16, column_count: u16) -> io::Result<()> {
    let mut payload = Vec::with_capacity(12);
    payload.push(0x00);
    payload.extend_from_slice(&statement_id.to_le_bytes());
    payload.extend_from_slice(&column_count.to_le_bytes());
    payload.extend_from_slice(&parameter_count.to_le_bytes());
    payload.push(0x00); // reserved
    payload.extend_from_slice(&0_u16.to_le_bytes()); // warning count
    write_packet(stream, sequence, &payload)
}

/// `COM_STMT_EXECUTE`: decodes the typed parameter block, renders the statement
/// with real values and runs it through the shared SQL pipeline.
fn handle_execute(
    stream: &mut impl Write,
    sequence: u8,
    payload: &[u8],
    runtime: Option<&Arc<MysqlSqlRuntime>>,
    statements: &mut PreparedStatements,
    session: &mut MysqlTransactionSession,
) -> io::Result<()> {
    let Some(statement_id) = take_u32(&payload[1..])
    else {
        write_error(stream, sequence, 1210, "HY000", "malformed COM_STMT_EXECUTE command")?;
        return Ok(());
    };
    // statement id (4) + flags (1) + iteration count (4)
    let mut cursor = 5;
    let Some(_flags) = payload.get(cursor)
    else {
        write_error(stream, sequence, 1210, "HY000", "malformed COM_STMT_EXECUTE command")?;
        return Ok(());
    };
    cursor += 1;
    let Some(iteration) = take_u32(&payload[cursor..])
    else {
        write_error(stream, sequence, 1210, "HY000", "malformed COM_STMT_EXECUTE iteration count")?;
        return Ok(());
    };
    cursor += 4;
    if iteration != 1 {
        write_error(stream, sequence, 1210, "HY000", "COM_STMT_EXECUTE iteration count must be 1")?;
        return Ok(());
    }
    let Some(statement) = statements.entries.get_mut(&statement_id)
    else {
        write_error(stream, sequence, 1243, "HY000", &format!("Unknown prepared statement handler ({statement_id})"))?;
        return Ok(());
    };

    let mut values = Vec::with_capacity(statement.parameter_count);
    if statement.parameter_count > 0 {
        let bitmap_length = statement.parameter_count.div_ceil(8);
        let Some(bitmap) = payload.get(cursor..cursor + bitmap_length)
        else {
            write_error(stream, sequence, 1210, "HY000", "truncated COM_STMT_EXECUTE null bitmap")?;
            return Ok(());
        };
        cursor += bitmap_length;
        let Some(&bound_flag) = payload.get(cursor)
        else {
            write_error(stream, sequence, 1210, "HY000", "missing COM_STMT_EXECUTE parameter types")?;
            return Ok(());
        };
        cursor += 1;
        if bound_flag == 1 {
            let mut types = Vec::with_capacity(statement.parameter_count);
            for _ in 0..statement.parameter_count {
                let Some(&code) = payload.get(cursor)
                else {
                    write_error(stream, sequence, 1210, "HY000", "truncated COM_STMT_EXECUTE parameter types")?;
                    return Ok(());
                };
                let Some(&flags) = payload.get(cursor + 1)
                else {
                    write_error(stream, sequence, 1210, "HY000", "truncated COM_STMT_EXECUTE parameter types")?;
                    return Ok(());
                };
                types.push((code, flags & 0x80 != 0));
                cursor += 2;
            }
            statement.parameter_types = types;
        }
        else if statement.parameter_types.len() != statement.parameter_count {
            write_error(stream, sequence, 1210, "HY000", "COM_STMT_EXECUTE reused parameter types before they were bound")?;
            return Ok(());
        }
        for index in 0..statement.parameter_count {
            if bitmap[index / 8] & (1 << (index % 8)) != 0 {
                values.push(SqlValue::Null);
                continue;
            }
            let (code, unsigned) = statement.parameter_types[index];
            match decode_parameter(payload, &mut cursor, code, unsigned) {
                Ok(value) => values.push(value),
                Err(message) => {
                    write_error(stream, sequence, 1235, "42000", &message)?;
                    return Ok(());
                }
            }
        }
    }

    let sql = statement.sql.clone();
    let description = statement.description.clone();
    if session.failed {
        write_error(stream, sequence, 40001, "HY000", "current transaction is aborted")?;
        return Ok(());
    }
    let Some(runtime) = runtime
    else {
        session.fail();
        write_error(stream, sequence, 1235, "42000", "YYDS MySQL SQL execution is not implemented (no SQL runtime)")?;
        return Ok(());
    };
    let rendered = match substitute_sql_parameters(&sql, &values, SqlParameterStyle::QuestionMark) {
        Ok(rendered) => rendered,
        Err(error) => {
            session.fail();
            write_statement_error(stream, sequence, "parameter", error)?;
            return Ok(());
        }
    };
    let column_types: Vec<u8> = description.iter().map(|column| mysql_field_type(&column.field_type)).collect();
    let column_nullables: Vec<bool> = description.iter().map(|column| column.field_type.is_nullable()).collect();
    execute_sql(stream, sequence, &rendered, runtime, session, &column_types, true, &column_nullables)
}

/// `COM_STMT_RESET` clears the cached parameter binding and returns OK.
fn handle_reset(stream: &mut impl Write, sequence: u8, payload: &[u8], statements: &PreparedStatements, status_flags: u16) -> io::Result<()> {
    let Some(statement_id) = take_u32(&payload[1..])
    else {
        return write_error(stream, sequence, 1210, "HY000", "malformed COM_STMT_RESET command");
    };
    if statements.entries.contains_key(&statement_id) {
        write_ok(stream, sequence, status_flags)
    }
    else {
        write_error(stream, sequence, 1243, "HY000", &format!("Unknown prepared statement handler ({statement_id})"))
    }
}

/// Decodes one `COM_STMT_EXECUTE` parameter value by its `MYSQL_TYPE_*` code.
fn decode_parameter(payload: &[u8], cursor: &mut usize, code: u8, unsigned: bool) -> Result<SqlValue, String> {
    match code {
        MYSQL_TYPE_TINY => {
            let bytes = take_slice(payload, cursor, 1)?;
            Ok(SqlValue::Integer(if unsigned { i64::from(bytes[0]) } else { i64::from(bytes[0] as i8) }))
        }
        MYSQL_TYPE_SHORT | MYSQL_TYPE_YEAR => {
            let bytes = take_slice(payload, cursor, 2)?;
            let raw = u16::from_le_bytes([bytes[0], bytes[1]]);
            Ok(SqlValue::Integer(if unsigned { i64::from(raw) } else { i64::from(raw as i16) }))
        }
        MYSQL_TYPE_INT24 => {
            let bytes = take_slice(payload, cursor, 3)?;
            let raw = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]);
            Ok(SqlValue::Integer(if unsigned { i64::from(raw) } else { i64::from(((raw << 8) as i32) >> 8) }))
        }
        MYSQL_TYPE_LONG => {
            let bytes = take_slice(payload, cursor, 4)?;
            let raw = u32::from_le_bytes(bytes.try_into().expect("checked length"));
            Ok(SqlValue::Integer(if unsigned { i64::from(raw) } else { i64::from(raw as i32) }))
        }
        MYSQL_TYPE_LONGLONG => {
            let bytes = take_slice(payload, cursor, 8)?;
            let raw = u64::from_le_bytes(bytes.try_into().expect("checked length"));
            Ok(SqlValue::Integer(raw as i64))
        }
        MYSQL_TYPE_FLOAT => {
            let bytes = take_slice(payload, cursor, 4)?;
            Ok(SqlValue::Number(f64::from(f32::from_le_bytes(bytes.try_into().expect("checked length")))))
        }
        MYSQL_TYPE_DOUBLE => {
            let bytes = take_slice(payload, cursor, 8)?;
            Ok(SqlValue::Number(f64::from_le_bytes(bytes.try_into().expect("checked length"))))
        }
        MYSQL_TYPE_NULL => Ok(SqlValue::Null),
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING => {
            let length = take_length_encoded(payload, cursor)?;
            let bytes = take_slice(payload, cursor, length)?;
            let text = std::str::from_utf8(bytes).map_err(|_| format!("COM_STMT_EXECUTE parameter type {code:#04X} is not valid UTF-8"))?;
            Ok(SqlValue::String(text.to_string()))
        }
        // BLOB 家族在二进制协议里就是原始 octets，原样收下，不做文本解码。
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB => {
            let length = take_length_encoded(payload, cursor)?;
            Ok(SqlValue::Bytes(take_slice(payload, cursor, length)?.to_vec()))
        }
        _ => Err(format!("COM_STMT_EXECUTE parameter type {code:#04X} is not supported")),
    }
}

fn take_slice<'a>(payload: &'a [u8], cursor: &mut usize, length: usize) -> Result<&'a [u8], String> {
    let end = cursor.checked_add(length).ok_or_else(|| "truncated COM_STMT_EXECUTE parameter".to_string())?;
    let bytes = payload.get(*cursor..end).ok_or_else(|| "truncated COM_STMT_EXECUTE parameter".to_string())?;
    *cursor = end;
    Ok(bytes)
}

fn take_length_encoded(payload: &[u8], cursor: &mut usize) -> Result<usize, String> {
    let first = *payload.get(*cursor).ok_or_else(|| "truncated length-encoded parameter".to_string())?;
    *cursor += 1;
    match first {
        0..=250 => Ok(usize::from(first)),
        251 => Err("NULL length-encoded parameter must be flagged in the null bitmap".to_string()),
        252 => {
            let bytes = take_slice(payload, cursor, 2)?;
            Ok(usize::from(u16::from_le_bytes([bytes[0], bytes[1]])))
        }
        253 => {
            let bytes = take_slice(payload, cursor, 3)?;
            Ok(usize::try_from(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0])).expect("u24 fits usize"))
        }
        254 => {
            let bytes = take_slice(payload, cursor, 8)?;
            usize::try_from(u64::from_le_bytes(bytes.try_into().expect("checked length")))
                .map_err(|_| "length-encoded parameter length exceeds the address space".to_string())
        }
        _ => Err("invalid length-encoded parameter length".to_string()),
    }
}

fn take_u32(bytes: &[u8]) -> Option<u32> {
    bytes.get(..4).map(|slice| u32::from_le_bytes(slice.try_into().expect("checked length")))
}

/// MySQL column type for a catalog-declared gateway field type.
fn mysql_field_type(field_type: &SqlFieldType) -> u8 {
    match field_type.inner() {
        SqlFieldType::Int32 => MYSQL_TYPE_LONG,
        SqlFieldType::Int64 => MYSQL_TYPE_LONGLONG,
        SqlFieldType::Float64 => MYSQL_TYPE_DOUBLE,
        SqlFieldType::Bool => MYSQL_TYPE_TINY,
        SqlFieldType::Bytes => MYSQL_TYPE_BLOB,
        // Exact aggregate results (`SUM`/`AVG` over integer operands) are reported
        // as `DECIMAL` in MySQL, which drivers read back as strings.
        SqlFieldType::Decimal => MYSQL_TYPE_NEWDECIMAL,
        // 列类型上报为 DATE/DATETIME 后，mysql2 会按类型把文本值转成 JS Date，
        // 与真库 8.4 的 `types` 场景一致。
        SqlFieldType::Date => MYSQL_TYPE_DATE,
        SqlFieldType::Datetime => MYSQL_TYPE_DATETIME,
        SqlFieldType::String | SqlFieldType::Unknown | SqlFieldType::Optional(_) => MYSQL_TYPE_VAR_STRING,
    }
}

/// 带 affected_rows 的 MySQL OK packet（INSERT/UPDATE/DELETE）。
fn write_ok_with_affected(
    stream: &mut impl Write,
    sequence: u8,
    affected: u64,
    last_insert_id: Option<u64>,
    status_flags: u16,
) -> io::Result<()> {
    let mut payload = Vec::with_capacity(13);
    payload.push(0x00); // OK identifier
    encode_length_encoded(&mut payload, affected);
    encode_length_encoded(&mut payload, last_insert_id.unwrap_or(0));
    payload.extend_from_slice(&status_flags.to_le_bytes());
    payload.extend_from_slice(&0_u16.to_le_bytes()); // warnings = 0
    write_packet(stream, sequence, &payload)
}

/// The value `apply_auto_increment` allocated for the last row of an INSERT.
///
/// Only called when a sequence-owning column was actually appended; an INSERT
/// that supplies every column itself reports `0`, like the engine.
fn allocated_last_insert_id(statement: &SqlCatalogBoundStatement) -> Option<u64> {
    let SqlCatalogBoundStatement::Insert(insert) = statement
    else {
        return None;
    };
    match insert.rows.last()?.last()? {
        SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Integer(value)) if *value >= 0 => Some(*value as u64),
        _ => None,
    }
}

/// Number of bound columns an INSERT supplies. `apply_auto_increment` appends
/// one column per sequence-owning field that the statement omitted.
fn bound_insert_column_count(statement: &SqlCatalogBoundStatement) -> usize {
    match statement {
        SqlCatalogBoundStatement::Insert(insert) => insert.fields.len(),
        _ => 0,
    }
}

/// 把 SqlResult 编码成 MySQL ResultSet 协议包。
fn write_result_set(
    stream: &mut impl Write,
    mut sequence: u8,
    result: &SqlResult,
    column_types: &[u8],
    column_nullables: &[bool],
    binary_rows: bool,
    status_flags: u16,
) -> io::Result<()> {
    // 包 1: column count
    let count = result.columns.len();
    let mut col_count = Vec::new();
    encode_length_encoded(&mut col_count, count as u64);
    write_packet(stream, sequence, &col_count)?;
    sequence = sequence.wrapping_add(1);

    // 包 2..N: column definitions
    for (index, col) in result.columns.iter().enumerate() {
        let declared = column_types.get(index).copied();
        let nullable = column_nullables.get(index).copied().unwrap_or(true);
        let def = encode_column_definition(col, declared.unwrap_or_else(|| column_type(&result.rows, index)), nullable);
        write_packet(stream, sequence, &def)?;
        sequence = sequence.wrapping_add(1);
    }

    // EOF after columns
    write_eof(stream, sequence, status_flags)?;
    sequence = sequence.wrapping_add(1);

    // Row packets
    for row in &result.rows {
        let payload = if binary_rows {
            encode_binary_row(row, column_types)
        }
        else {
            let mut payload = Vec::new();
            for value in row {
                encode_sql_value(&mut payload, value);
            }
            payload
        };
        write_packet(stream, sequence, &payload)?;
        sequence = sequence.wrapping_add(1);
    }

    // EOF after rows
    write_eof(stream, sequence, status_flags)
}

/// 按列声明类型把一行编码成 MySQL binary protocol row（prepared statement 结果）。
fn encode_binary_row(row: &[SqlValue], column_types: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0x00); // binary row header
    let mut bitmap = vec![0_u8; (row.len() + 2).div_ceil(8)];
    for (index, value) in row.iter().enumerate() {
        if matches!(value, SqlValue::Null) {
            let bit = index + 2;
            bitmap[bit / 8] |= 1 << (bit % 8);
        }
    }
    out.extend_from_slice(&bitmap);
    for (index, value) in row.iter().enumerate() {
        if matches!(value, SqlValue::Null) {
            continue;
        }
        let column_type = column_types.get(index).copied().unwrap_or(MYSQL_TYPE_VAR_STRING);
        match (column_type, value) {
            (MYSQL_TYPE_LONGLONG, SqlValue::Integer(number)) => out.extend_from_slice(&number.to_le_bytes()),
            (MYSQL_TYPE_LONG, SqlValue::Integer(number)) => out.extend_from_slice(&(*number as i32).to_le_bytes()),
            (MYSQL_TYPE_INT24, SqlValue::Integer(number)) => {
                out.extend_from_slice(&(*number as i32).to_le_bytes()[..3]);
            }
            (MYSQL_TYPE_SHORT, SqlValue::Integer(number)) => out.extend_from_slice(&(*number as i16).to_le_bytes()),
            (MYSQL_TYPE_TINY, SqlValue::Integer(number)) => out.push(*number as u8),
            (MYSQL_TYPE_TINY, SqlValue::Boolean(truth)) => out.push(u8::from(*truth)),
            (MYSQL_TYPE_DOUBLE, SqlValue::Number(number)) => out.extend_from_slice(&number.to_le_bytes()),
            (MYSQL_TYPE_FLOAT, SqlValue::Number(number)) => out.extend_from_slice(&(*number as f32).to_le_bytes()),
            (MYSQL_TYPE_DATE, SqlValue::String(text)) => encode_binary_date(&mut out, text, false),
            (MYSQL_TYPE_DATETIME, SqlValue::String(text)) => encode_binary_date(&mut out, text, true),
            _ => encode_sql_value(&mut out, value),
        }
    }
    out
}

/// `DATE`/`DATETIME` 的 binary protocol 编码：长度字节 + 年（u16 LE）+ 月 + 日，
/// `DATETIME` 再追加时/分/秒（缺省补 0，长度恒为 7），与真库报文布局一致，
/// mysql2 据此还原 JS Date。无法按日期文本解析的值退回文本编码，不伪造字段。
fn encode_binary_date(out: &mut Vec<u8>, text: &str, with_time: bool) {
    let parsed: Option<Vec<u8>> = (|| {
        let (date, time) = text.split_once(' ').unwrap_or((text, ""));
        let mut parts = date.split('-');
        let year = parts.next()?.parse::<u16>().ok()?;
        let month = parts.next()?.parse::<u8>().ok()?;
        let day = parts.next()?.parse::<u8>().ok()?;
        let mut fields = Vec::with_capacity(7);
        fields.extend_from_slice(&year.to_le_bytes());
        fields.push(month);
        fields.push(day);
        if with_time {
            let mut parts = time.split(':');
            fields.push(parts.next().and_then(|value| value.parse::<u8>().ok()).unwrap_or(0));
            fields.push(parts.next().and_then(|value| value.parse::<u8>().ok()).unwrap_or(0));
            fields.push(parts.next().and_then(|value| value.parse::<u8>().ok()).unwrap_or(0));
        }
        Some(fields)
    })();
    match parsed {
        Some(fields) => {
            out.push(fields.len() as u8);
            out.extend_from_slice(&fields);
        }
        None => encode_sql_value(out, &SqlValue::String(text.to_string())),
    }
}

/// 按首批非空值推断 MySQL 列类型：`LONGLONG`、`DOUBLE`、`TINY`、`VAR_STRING`。
fn column_type(rows: &[Vec<SqlValue>], index: usize) -> u8 {
    rows.iter()
        .find_map(|row| match row.get(index) {
            Some(SqlValue::Integer(_)) => Some(MYSQL_TYPE_LONGLONG),
            Some(SqlValue::Number(_)) => Some(MYSQL_TYPE_DOUBLE),
            Some(SqlValue::Boolean(_)) => Some(MYSQL_TYPE_TINY),
            Some(SqlValue::String(_)) => Some(MYSQL_TYPE_VAR_STRING),
            Some(SqlValue::Bytes(_)) => Some(MYSQL_TYPE_BLOB),
            Some(SqlValue::Null) | None => None,
        })
        .unwrap_or(MYSQL_TYPE_VAR_STRING)
}

/// MySQL wire protocol ColumnDefinition 编码。`NOT_NULL = 0x0002` 在列不可空时设置。
/// TINY 列的 column_length 设置为 1，等价于 TINYINT(1)，驱动程序据此把 BOOLEAN 当成 0/1。
/// BLOB 列声明 binary 字符集（63），驱动据此把载荷当作原始 octets 而不是文本。
fn encode_column_definition(name: &str, column_type: u8, nullable: bool) -> Vec<u8> {
    let mut out = Vec::new();
    encode_length_encoded(&mut out, b"def".len() as u64);
    out.extend_from_slice(b"def");
    encode_length_encoded(&mut out, 0); // schema
    encode_length_encoded(&mut out, 0); // table
    encode_length_encoded(&mut out, 0); // org_table
    encode_length_encoded(&mut out, name.len() as u64);
    out.extend_from_slice(name.as_bytes());
    encode_length_encoded(&mut out, 0); // org_name
    out.push(0x0C); // fixed length
    let charset: u16 = if column_type == MYSQL_TYPE_BLOB { 63 } else { 0x000C };
    out.extend_from_slice(&charset.to_le_bytes()); // charset
    let column_length: u32 = match column_type {
        MYSQL_TYPE_TINY => 1,
        MYSQL_TYPE_BLOB => MYSQL_BLOB_CAPACITY as u32,
        _ => 255,
    };
    out.extend_from_slice(&column_length.to_le_bytes());
    out.push(column_type);
    let flags: u16 = if !nullable { 0x0002 } else { 0 };
    out.extend_from_slice(&flags.to_le_bytes());
    out.push(if column_type == MYSQL_TYPE_DOUBLE { 31 } else { 0 }); // decimals
    out.extend_from_slice(&[0u8; 2]); // filler
    out
}

fn encode_sql_value(out: &mut Vec<u8>, value: &SqlValue) {
    match value {
        SqlValue::Null => out.push(0xFB),
        SqlValue::Integer(n) => {
            let text = n.to_string();
            encode_length_encoded(out, text.len() as u64);
            out.extend_from_slice(text.as_bytes());
        }
        SqlValue::Number(n) => {
            let text = format!("{n}");
            encode_length_encoded(out, text.len() as u64);
            out.extend_from_slice(text.as_bytes());
        }
        SqlValue::String(s) => {
            encode_length_encoded(out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
        // 原始 octets 按 length-encoded string 原样发出，文本与二进制行协议一致。
        SqlValue::Bytes(bytes) => {
            encode_length_encoded(out, bytes.len() as u64);
            out.extend_from_slice(bytes);
        }
        SqlValue::Boolean(b) => {
            let text = if *b { "1" } else { "0" };
            encode_length_encoded(out, text.len() as u64);
            out.extend_from_slice(text.as_bytes());
        }
    }
}

/// 把整数写成 MySQL length-encoded integer。
///
/// `251` 不能用单字节 `0xFB`（保留表示 `NULL`），必须走 `0xFC` 双字节形式。
pub fn encode_length_encoded(out: &mut Vec<u8>, value: u64) {
    match value {
        0..=250 => out.push(value as u8),
        251 => out.extend_from_slice(&[252, 251, 0]),
        252..=0xFFFF => {
            out.push(252);
            out.extend_from_slice(&(value as u16).to_le_bytes());
        }
        0x10000..=0xFFFF_FFFF => {
            out.push(253);
            out.extend_from_slice(&(value as u32).to_le_bytes());
        }
        _ => {
            out.push(254);
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
}

fn write_eof(stream: &mut impl Write, sequence: u8, status_flags: u16) -> io::Result<()> {
    // 简化 EOF packet: 0xFE + 0x0000 (warnings) + status_flags
    let mut payload = [0xFE, 0x00, 0x00, 0x00, 0x00];
    payload[3..].copy_from_slice(&status_flags.to_le_bytes());
    write_packet(stream, sequence, &payload)
}

/// MySQL `LIKE` matching for `SHOW VARIABLES LIKE 'pattern'` — `%` matches any
/// run of bytes and `_` matches one byte, case-insensitively.
fn like_matches(pattern: &str, value: &str) -> bool {
    fn walk(pattern: &[u8], value: &[u8]) -> bool {
        match pattern.first() {
            None => value.is_empty(),
            Some(b'%') => walk(&pattern[1..], value) || (!value.is_empty() && walk(pattern, &value[1..])),
            Some(b'_') => !value.is_empty() && walk(&pattern[1..], &value[1..]),
            Some(byte) => value.first().is_some_and(|candidate| candidate.eq_ignore_ascii_case(byte)) && walk(&pattern[1..], &value[1..]),
        }
    }
    walk(pattern.to_ascii_lowercase().as_bytes(), value.to_ascii_lowercase().as_bytes())
}

fn packet_error(error: PacketError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("invalid MySQL packet: {error:?}"))
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// 可取消会话的读超时节拍：阻塞读在数据到达时被内核即时唤醒；超时到期只
/// 意味着“这段时间没有数据”，回到循环顶检查停机标志后继续等。取消延迟上界即此值。
const CANCELLABLE_READ_TIMEOUT: Duration = Duration::from_millis(50);

/// 可取消会话的写超时：设长，避免慢客户端下发送半包时被超时打断丢数据。
const CANCELLABLE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// 单次 I/O 操作的总时限，与 `serve_connection` 的 30 秒空闲超时口径一致。
const CANCELLABLE_IO_DEADLINE: Duration = Duration::from_secs(30);

struct CancellableSocket {
    stream: TcpStream,
    stopping: Arc<AtomicBool>,
}

impl CancellableSocket {
    fn attempt<T>(&mut self, mut operation: impl FnMut(&mut TcpStream) -> io::Result<T>) -> io::Result<T> {
        let deadline = Instant::now() + CANCELLABLE_IO_DEADLINE;
        loop {
            if self.stopping.load(Ordering::Acquire) {
                return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "MySQL service stopping"));
            }
            match operation(&mut self.stream) {
                // 阻塞套接字 + 短超时：超时到期仅代表“这段时间没有数据到达 / 缓冲未腾空”，
                // 检查停机标志后继续等。Linux 超时回 WouldBlock，Windows 回 TimedOut，两者都按可重试处理。
                Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "MySQL socket I/O timeout"));
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => return result,
            }
        }
    }
}

impl Read for CancellableSocket {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.attempt(|stream| stream.read(bytes))
    }
}

impl Write for CancellableSocket {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.attempt(|stream| stream.write(bytes))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.attempt(Write::flush)
    }
}
