//! PostgreSQL protocol-v3 connection establishment and explicit query rejection.

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::TcpStream,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use super::wire::{FrameError, StartupParameterError, StartupParameters, decode_message, decode_startup, encode_message};
use database_execution::{
    cluster::{KeyValueExecutor, TransactionHandle},
    sql_exec::execute_plan,
};
use database_query::{
    DiscardTarget, SqlBoundExpression, SqlBoundLiteral, SqlCatalog, SqlCatalogBoundStatement, SqlColumn, SqlDialect,
    SqlErrorKind, SqlExecutionPlan, SqlFieldType, SqlFrontendError, SqlParameterStyle, SqlSchemaRegistry, SqlSessionCommand,
    SqlTransactionAction, SqlValue, analyze_meta_query, apply_auto_increment, bind_sql_catalog, describe_bound_insert, describe_plan,
    detect_session_reset_statement, encode_hex, neutralized_sql_parameters, parse_ddl, parse_session_command, pg_server_version,
    plan_statement_for_dialect, split_sql_batch, substitute_sql_parameters,
};

/// PostgreSQL type OIDs supported by the gateway.
const OID_BOOL: u32 = 16;
const OID_BYTEA: u32 = 17;
const OID_NAME: u32 = 19;
const OID_INT8: u32 = 20;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_TEXT: u32 = 25;
const OID_FLOAT4: u32 = 700;
const OID_FLOAT8: u32 = 701;
const OID_VARCHAR: u32 = 1043;
const OID_NUMERIC: u32 = 1700;

const PROTOCOL_V3: u32 = 196_608;
const SSL_REQUEST: u32 = 80_877_103;
const GSSENC_REQUEST: u32 = 80_877_104;
const CANCEL_REQUEST: u32 = 80_877_102;
const MAX_STARTUP_FRAME: usize = 64 * 1024;
const MAX_MESSAGE_FRAME: usize = 16 * 1024 * 1024;
static NEXT_SECRET: AtomicU32 = AtomicU32::new(1);

/// PostgreSQL's default `search_path`, also the value `RESET search_path` and
/// `DISCARD ALL` restore. `SHOW search_path` prints it exactly as the engine
/// does, quotes included, because `$user` must be quoted to stay an identifier.
const DEFAULT_SEARCH_PATH: &str = "\"$user\", public";

/// Injects YYDS SQL execution into PostgreSQL SimpleQuery.
#[derive(Debug)]
pub struct PgsqlSqlRuntime {
    /// Local schema registry holding catalog snapshots and applying DDL.
    pub schema: Arc<SqlSchemaRegistry>,
    /// KV execution plane.
    pub executor: Arc<dyn KeyValueExecutor>,
}

/// Establishes an unauthenticated protocol-v3 session, then rejects unsupported SQL explicitly.
///
/// The caller must restrict the peer to loopback until authentication and TLS are implemented.
pub fn serve_connection(stream: TcpStream, runtime: Option<Arc<PgsqlSqlRuntime>>) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "PostgreSQL requires loopback until authentication and TLS are implemented",
        ));
    }
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    serve_session(stream, runtime)
}

/// Serves a loopback session with cooperative service-stop cancellation.
pub fn serve_cancellable(stream: TcpStream, stopping: Arc<AtomicBool>, runtime: Option<Arc<PgsqlSqlRuntime>>) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "PostgreSQL requires loopback until authentication and TLS are implemented",
        ));
    }
    stream.set_nonblocking(true)?;
    serve_session(CancellableSocket { stream, stopping }, runtime)
}

fn serve_session(mut stream: impl Read + Write, runtime: Option<Arc<PgsqlSqlRuntime>>) -> io::Result<()> {
    let mut pending = Vec::new();
    let mut session = TransactionSession::new();
    let parameters = loop {
        let packet = read_startup(&mut stream, &mut pending)?;
        match packet.code {
            SSL_REQUEST | GSSENC_REQUEST => stream.write_all(b"N")?,
            CANCEL_REQUEST => return Ok(()),
            PROTOCOL_V3 => match StartupParameters::parse(&packet.payload) {
                Ok(parameters) => break parameters,
                Err(error) => {
                    write_fatal(&mut stream, "08P01", &format_startup_error(error))?;
                    return Ok(());
                }
            },
            _ => {
                write_fatal(&mut stream, "0A000", "unsupported PostgreSQL protocol version")?;
                return Ok(());
            }
        }
    };

    session.database = parameters.database().to_string();

    send_authentication_ok(&mut stream)?;
    send_parameter(&mut stream, "server_version", pg_server_version())?;
    send_parameter(&mut stream, "server_encoding", "UTF8")?;
    send_parameter(&mut stream, "client_encoding", "UTF8")?;
    send_parameter(&mut stream, "DateStyle", "ISO, MDY")?;
    send_parameter(&mut stream, "integer_datetimes", "on")?;
    send_parameter(&mut stream, "standard_conforming_strings", "on")?;
    send_parameter(&mut stream, "TimeZone", "UTC")?;
    if let Some(application_name) = parameters.get("application_name") {
        send_parameter(&mut stream, "application_name", application_name)?;
    }
    send_backend_key(&mut stream)?;
    send_ready(&mut stream, &session)?;

    let mut extended = ExtendedSession::default();
    let mut extended_failed = false;

    loop {
        let message = match read_message(&mut stream, &mut pending) {
            Ok(Some(message)) => message,
            Ok(None) => {
                // A connection that goes away mid-transaction is an implicit
                // rollback, catalog changes included.
                let _ = session.restore_schema(runtime.as_ref());
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                let _ = session.restore_schema(runtime.as_ref());
                return Ok(());
            }
            Err(error) => {
                let _ = session.restore_schema(runtime.as_ref());
                return Err(error);
            }
        };
        match message.tag {
            b'X' if message.payload.is_empty() => {
                let _ = session.restore_schema(runtime.as_ref());
                return Ok(());
            }
            b'Q' => {
                let Some(sql_bytes) = message.payload.strip_suffix(&[0])
                else {
                    write_error_response(&mut stream, "08P01", "malformed PostgreSQL simple-query message")?;
                    session.fail();
                    send_ready(&mut stream, &session)?;
                    continue;
                };
                if sql_bytes.contains(&0) {
                    write_error_response(&mut stream, "08P01", "embedded NUL in PostgreSQL query")?;
                    session.fail();
                }
                else if let Ok(sql) = std::str::from_utf8(sql_bytes) {
                    handle_simple_query(&mut stream, sql, &mut session, &mut extended, runtime.as_ref())?;
                }
                else {
                    write_error_response(&mut stream, "22021", "query is not valid UTF-8")?;
                    session.fail();
                }
                send_ready(&mut stream, &session)?;
            }
            tag @ (b'P' | b'B' | b'D' | b'E' | b'C' | b'H' | b'S') => {
                if extended_failed && !matches!(tag, b'S' | b'H') {
                    // 扩展查询出错后，PostgreSQL 会丢弃同一批次的后续消息直到 Sync。
                    continue;
                }
                let step = match tag {
                    b'P' => handle_parse(&mut stream, &message.payload, &mut extended),
                    b'B' => handle_bind(&mut stream, &message.payload, &mut extended, runtime.as_ref()),
                    b'D' => handle_describe(&mut stream, &message.payload, &mut extended, &mut session, runtime.as_ref()),
                    b'E' => handle_execute(&mut stream, &message.payload, &mut extended, &mut session, runtime.as_ref()),
                    b'C' => handle_close(&mut stream, &message.payload, &mut extended),
                    b'H' => stream.flush().map(|_| ExtendedStep::Continue),
                    b'S' => {
                        extended_failed = false;
                        send_ready(&mut stream, &session).map(|_| ExtendedStep::Continue)
                    }
                    _ => Ok(ExtendedStep::Continue),
                }?;
                match step {
                    ExtendedStep::Continue => {}
                    ExtendedStep::Failed => {
                        extended_failed = true;
                        session.fail();
                    }
                }
            }
            _ => {
                write_error_response(&mut stream, "08P01", "unsupported PostgreSQL frontend message")?;
                send_ready(&mut stream, &session)?;
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TransactionStatus {
    Idle,
    InTransaction,
    Failed,
}

/// Session-scoped transaction state: the PostgreSQL wire protocol ReadyForQuery byte
/// plus a real storage transaction handle that writes and row reads are bound to.
/// Also holds session-scoped GUC overrides that `SET` mutates and `SHOW` queries.
struct TransactionSession {
    /// The executor Arc stays alive for as long as a transaction is open so the handle's
    /// internal references remain valid.
    executor: Option<Arc<dyn KeyValueExecutor>>,
    handle: Option<Box<dyn TransactionHandle + 'static>>,
    /// Tracks whether we are inside a transaction block even without a runtime handle
    /// (e.g. the PostgreSQL protocol test).
    protocol_only_transaction: bool,
    failed: bool,
    /// `search_path` as last set via `SET search_path = ...`. Defaults to the
    /// PostgreSQL convention `"$user, public"`.
    search_path: String,
    /// Startup `database` parameter, reported by synthesized `current_database()`.
    database: String,
    /// Registry snapshot captured at `BEGIN`. The catalog rides the same
    /// transaction as rows: `COMMIT` keeps it, every rollback path restores it.
    schema_checkpoint: Option<database_query::SqlSchemaCheckpoint>,
}

impl TransactionSession {
    fn new() -> Self {
        Self {
            executor: None,
            handle: None,
            protocol_only_transaction: false,
            failed: false,
            search_path: DEFAULT_SEARCH_PATH.to_string(),
            database: "postgres".to_string(),
            schema_checkpoint: None,
        }
    }

    /// Captures the registry state when a transaction block opens.
    fn capture_schema(&mut self, runtime: Option<&Arc<PgsqlSqlRuntime>>) {
        self.schema_checkpoint = runtime.map(|rt| rt.schema.checkpoint());
    }

    /// Restores the snapshot captured at `BEGIN`. Only rollback paths call
    /// this — a successful `COMMIT` drops the checkpoint instead.
    fn restore_schema(&mut self, runtime: Option<&Arc<PgsqlSqlRuntime>>) -> io::Result<()> {
        if let (Some(checkpoint), Some(rt)) = (self.schema_checkpoint.take(), runtime) {
            rt.schema.restore(checkpoint).map_err(|error| io::Error::other(error.message))?;
        }
        Ok(())
    }

    /// `RESET` / `RESET ALL`: every emulated GUC returns to its default.
    ///
    /// The gateway keeps no override for GUCs other than `search_path` (their
    /// `SET` is accepted as a no-op and `SHOW` answers the default table), so
    /// restoring them is already satisfied by the static default table.
    fn reset_parameters(&mut self) {
        self.search_path = DEFAULT_SEARCH_PATH.to_string();
    }

    /// `DISCARD ALL`: full session cleanup. PostgreSQL only lets the statement run
    /// outside a transaction block; the caller rejects the in-block case with
    /// `25001` before this runs, so any transaction state here is a leftover. It
    /// is rolled back — never committed — emulated GUCs return to their defaults
    /// and the aborted flag clears. Prepared statements and portals live in
    /// [`ExtendedSession`] and are dropped by the caller.
    fn discard_all(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.rollback();
        }
        self.executor = None;
        self.protocol_only_transaction = false;
        self.failed = false;
        // `DISCARD ALL` is rejected inside a transaction block, so there is no
        // pending checkpoint here; dropping the slot keeps the invariant that
        // only an open block owns schema rollback.
        self.schema_checkpoint = None;
        self.reset_parameters();
    }

    fn as_status(&self) -> TransactionStatus {
        if self.failed {
            TransactionStatus::Failed
        }
        else if self.handle.is_some() || self.protocol_only_transaction {
            TransactionStatus::InTransaction
        }
        else {
            TransactionStatus::Idle
        }
    }

    fn fail(&mut self) {
        if self.handle.is_some() || self.protocol_only_transaction {
            self.failed = true;
        }
    }

    fn in_transaction(&self) -> bool {
        self.handle.is_some() || self.protocol_only_transaction
    }

    fn ready_byte(&self) -> u8 {
        self.as_status().ready_byte()
    }
}

impl Default for TransactionSession {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionStatus {
    fn ready_byte(self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction => b'T',
            Self::Failed => b'E',
        }
    }
}

/// How a rejected `RESET` / `DISCARD` session command affects the session. Both
/// wire paths (SimpleQuery and the extended protocol) report the same SQLSTATE
/// and abort an open transaction block the same way, so the decision lives here
/// and cannot diverge.
enum SessionCommandError {
    /// The command was refused before it performed any cleanup (no GUC reset,
    /// no prepared-statement or portal drop, no transaction handle change), but
    /// PostgreSQL raises the guard as a plain error, so an open transaction
    /// block aborts exactly like it would for any other statement failure.
    /// This is `DISCARD ALL` inside a transaction block (`25001`).
    Rejected { code: &'static str, message: &'static str },
    /// The command failed against the session; the caller must abort any open
    /// transaction (`25P02` from an aborted block, `42704` for an unknown GUC,
    /// `0A000` for an unsupported `DISCARD` target).
    Failed { code: String, message: String },
}

/// Executes a `RESET <parameter>` / `RESET ALL` / `DISCARD <target>` session
/// command against the session. Shared by SimpleQuery and the extended protocol
/// so both paths run the same side effects ([`TransactionSession::reset_parameters`]
/// and [`TransactionSession::discard_all`]) with the same error mapping.
///
/// `Ok` carries the CommandComplete tag PostgreSQL reports for the statement.
fn run_session_command(
    command: &SqlSessionCommand,
    session: &mut TransactionSession,
    extended: &mut ExtendedSession,
) -> Result<&'static str, SessionCommandError> {
    // `RESET` and `DISCARD` are ordinary utility statements, so an aborted
    // transaction rejects them exactly like any other command (`25P02`).
    if session.failed {
        return Err(SessionCommandError::Failed {
            code: "25P02".to_string(),
            message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
        });
    }
    match command {
        SqlSessionCommand::ResetVariables { name } => {
            match name.as_deref() {
                // `RESET ALL` restores every emulated GUC to its default value.
                None => session.reset_parameters(),
                Some(name) => {
                    if pg_parameter(name).is_none() {
                        return Err(SessionCommandError::Failed {
                            code: "42704".to_string(),
                            message: format!("unrecognized configuration parameter \"{name}\""),
                        });
                    }
                    // The default table is the RESET value, and `search_path` is the
                    // only parameter the session actually overrides.
                    if name.trim().eq_ignore_ascii_case("search_path") {
                        session.reset_parameters();
                    }
                }
            }
            Ok(if name.is_none() { "RESET ALL" } else { "RESET" })
        }
        SqlSessionCommand::Discard(target) => match target {
            // PostgreSQL refuses `DISCARD ALL` inside a transaction block with
            // `25001` and performs no cleanup at all; the raised error still
            // aborts the block like any other statement failure.
            // Outside a block it applies the full session reset connection pools
            // expect: emulated GUCs return to their defaults and the
            // prepared-statement/portal caches are dropped. Temporary tables and
            // advisory locks are outside the gateway subset, so nothing else is
            // released here.
            DiscardTarget::All => {
                if session.in_transaction() {
                    return Err(SessionCommandError::Rejected { code: "25001", message: "DISCARD ALL cannot run inside a transaction block" });
                }
                session.discard_all();
                extended.statements.clear();
                extended.portals.clear();
                Ok("DISCARD ALL")
            }
            // The remaining targets only release caches this gateway does not keep
            // (`PLANS`, `SEQUENCES`) or objects it does not create (`TEMPORARY`).
            // They stay explicitly unsupported instead of silently pretending to
            // perform a cleanup.
            other => {
                let label = match other {
                    DiscardTarget::Plans => "DISCARD PLANS",
                    DiscardTarget::Sequences => "DISCARD SEQUENCES",
                    DiscardTarget::Temporary => "DISCARD TEMPORARY",
                    DiscardTarget::All => unreachable!("`DISCARD ALL` is handled above"),
                };
                Err(SessionCommandError::Failed { code: "0A000".to_string(), message: format!("{label} is not supported by the gateway") })
            }
        },
        _ => unreachable!("only `RESET` and `DISCARD` reach the session command executor"),
    }
}

fn handle_simple_query(
    stream: &mut impl Write,
    sql: &str,
    session: &mut TransactionSession,
    extended: &mut ExtendedSession,
    runtime: Option<&Arc<PgsqlSqlRuntime>>,
) -> io::Result<()> {
    // PostgreSQL answers an empty query string with EmptyQueryResponse rather
    // than a syntax error. Connection pools (`r2d2` and others) probe a
    // checked-out connection exactly this way, so an error here rejects every
    // checkout even though the connection itself is healthy.
    if sql.trim().is_empty() {
        return write_message(stream, b'I', &[]);
    }
    let statements = match split_sql_batch(sql) {
        Ok(statements) => statements,
        Err(error) => {
            session.fail();
            return write_error_response(stream, pg_sqlstate(error.kind), &format!("SQL batch error: {}", error.message));
        }
    };
    if statements.len() <= 1 {
        let single = statements.first().map(|statement| statement.text.as_str()).unwrap_or(sql);
        return handle_single_statement(stream, single, session, extended, runtime);
    }
    // PostgreSQL parses the whole string and stops at the first failing
    // statement. Outside a transaction block each statement is its own implicit
    // transaction, so earlier statements stay committed. Inside a block the
    // checkpoint captured at `BEGIN` owns the schema rollback, so a failing
    // statement only aborts the block — `ROLLBACK`, `DISCARD ALL` or a
    // disconnect restores the catalog.
    for (index, statement) in statements.iter().enumerate() {
        let mut buffer = Vec::new();
        handle_single_statement(&mut buffer, &statement.text, session, extended, runtime)?;
        match annotate_batch_error(&buffer, index + 1, statements.len(), statement.offset) {
            Some(annotated) => {
                stream.write_all(&annotated)?;
                return Ok(());
            }
            None => stream.write_all(&buffer)?,
        }
    }
    Ok(())
}

/// Executes one already-split statement. This is the former single-statement
/// body: DDL is resolved before the session parser, then session commands and
/// finally SQL execution.
fn handle_single_statement(
    stream: &mut impl Write,
    sql: &str,
    session: &mut TransactionSession,
    extended: &mut ExtendedSession,
    runtime: Option<&Arc<PgsqlSqlRuntime>>,
) -> io::Result<()> {
    // DDL resolves against the local schema registry before the Oak session
    // parser, so a `CREATE` / `ALTER` / `DROP` outside the supported subset is
    // reported as such instead of being misclassified as an unknown table.
    match parse_ddl(sql, SqlDialect::Postgres) {
        Err(error) => {
            session.fail();
            return write_error_response(stream, pg_sqlstate(error.kind), &format!("SQL ddl error: {}", error.message));
        }
        Ok(Some(statement)) => {
            if session.failed {
                return write_error_response(
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                );
            }
            let Some(runtime) = runtime
            else {
                return write_error_response(stream, "0A000", "YYDS PostgreSQL SQL execution is not implemented");
            };
            return match runtime.schema.apply_ddl(&statement) {
                Ok(outcome) => write_command_complete(stream, outcome.command_tag()),
                Err(error) => {
                    session.fail();
                    write_error_response(stream, pg_sqlstate(error.kind), &format!("SQL ddl error: {}", error.message))
                }
            };
        }
        Ok(None) => {}
    }
    match parse_session_command(sql) {
        Err(_) => {
            session.fail();
            write_error_response(stream, "42601", "Oak could not parse the SQL statement")
        }
        Ok(Some(SqlSessionCommand::Transaction(action))) => match action {
            SqlTransactionAction::Begin if session.failed => {
                write_error_response(stream, "25P02", "current transaction is aborted, commands ignored until end of transaction block")
            }
            SqlTransactionAction::Begin => {
                match runtime {
                    Some(rt) => {
                        // The executor owns the transaction contract: `begin_transaction` captures
                        // the ownership context (single-node gateways pass `None` explicitly) and
                        // the returned handle replays it at the commit boundary.
                        match Arc::clone(&rt.executor).begin_transaction(None) {
                            Ok(handle) => {
                                session.executor = Some(rt.executor.clone());
                                session.handle = Some(handle);
                                session.protocol_only_transaction = false;
                                session.failed = false;
                                session.capture_schema(runtime);
                                write_command_complete(stream, "BEGIN")
                            }
                            Err(err) => write_error_response(stream, "0A000", &format!("BEGIN failed: {err}")),
                        }
                    }
                    None => {
                        // No runtime — stay in protocol-only mode. execute_plan will receive
                        // `None` for the handle and go through the executor directly.
                        session.executor = None;
                        session.handle = None;
                        session.protocol_only_transaction = true;
                        session.failed = false;
                        session.schema_checkpoint = None;
                        write_command_complete(stream, "BEGIN")
                    }
                }
            }
            SqlTransactionAction::Commit if session.failed => {
                if let Some(handle) = session.handle.take() {
                    handle.rollback();
                }
                session.executor = None;
                session.handle = None;
                session.protocol_only_transaction = false;
                session.failed = false;
                // `COMMIT` of an aborted block is a rollback: the catalog goes with the rows.
                session.restore_schema(runtime)?;
                write_command_complete(stream, "ROLLBACK")
            }
            SqlTransactionAction::Commit => {
                if let Some(handle) = session.handle.take() {
                    if let Err(err) = handle.commit() {
                        let state = match &err {
                            database_types::cluster::Error::CasConflict { .. } => "40001",
                            _ => "58P01",
                        };
                        // A failed commit did not publish the transaction, so the
                        // DDL it collected is discarded too.
                        session.restore_schema(runtime)?;
                        write_error_response(stream, state, &format!("COMMIT failed: {err}"))?;
                        return Ok(());
                    }
                }
                session.executor = None;
                session.handle = None;
                session.protocol_only_transaction = false;
                session.failed = false;
                // The commit point publishes the schema revision as well.
                session.schema_checkpoint = None;
                write_command_complete(stream, "COMMIT")
            }
            SqlTransactionAction::Rollback => {
                if let Some(handle) = session.handle.take() {
                    handle.rollback();
                }
                session.executor = None;
                session.handle = None;
                session.protocol_only_transaction = false;
                session.failed = false;
                session.restore_schema(runtime)?;
                write_command_complete(stream, "ROLLBACK")
            }
            SqlTransactionAction::Savepoint(_) | SqlTransactionAction::RollbackToSavepoint(_) | SqlTransactionAction::ReleaseSavepoint(_) => {
                write_error_response(
                    stream,
                    "0A000",
                    "SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT are not yet supported by the gateway",
                )
            }
        },
        Ok(Some(SqlSessionCommand::Savepoint(intent))) => {
            use database_query::SavepointIntent;
            if session.failed {
                return write_error_response(
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                );
            }
            let handle = match session.handle.as_ref() {
                Some(handle) => handle.as_ref(),
                None => {
                    session.fail();
                    return write_error_response(stream, "25P01", "SAVEPOINT can only be used in transaction blocks");
                }
            };
            let result = match &intent {
                SavepointIntent::Create { name } => handle.savepoint_create(name),
                SavepointIntent::RollbackTo { name } => handle.savepoint_rollback_to(name),
                SavepointIntent::Release { name } => handle.savepoint_release(name),
            };
            match result {
                Ok(()) => {
                    let tag = match intent {
                        SavepointIntent::Create { .. } => "SAVEPOINT",
                        SavepointIntent::RollbackTo { .. } => "ROLLBACK",
                        SavepointIntent::Release { .. } => "RELEASE",
                    };
                    write_command_complete(stream, tag)
                }
                Err(database_types::cluster::Error::Unsupported(msg)) if msg.contains("does not exist") || msg.contains("already exists") => {
                    session.fail();
                    write_error_response(stream, "3B000", msg)
                }
                Err(err) => {
                    session.fail();
                    write_error_response(stream, "58P01", &format!("savepoint failed: {err}"))
                }
            }
        }
        Ok(Some(SqlSessionCommand::SetVariable { name, value, .. })) => {
            if session.failed {
                return write_error_response(
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                );
            }
            let needle = name.trim().to_ascii_lowercase();
            match needle.as_str() {
                "search_path" => match value {
                    SqlBoundExpression::Literal(SqlBoundLiteral::String(path)) => {
                        session.search_path = path;
                    }
                    other => {
                        session.fail();
                        // The engine rejects any non-string value with
                        // `22023 invalid_parameter_value` and echoes the value
                        // it saw, so the error codes and text stay comparable.
                        let rendered = match &other {
                            SqlBoundExpression::Literal(SqlBoundLiteral::Integer(number)) => number.to_string(),
                            SqlBoundExpression::Literal(SqlBoundLiteral::Number(number)) => number.clone(),
                            SqlBoundExpression::Literal(SqlBoundLiteral::Boolean(flag)) => flag.to_string(),
                            SqlBoundExpression::Identifier(name) => name.clone(),
                            SqlBoundExpression::Literal(SqlBoundLiteral::Null) => "NULL".to_string(),
                            SqlBoundExpression::Literal(SqlBoundLiteral::Bytes(_)) => "\\x".to_string(),
                            _ => String::new(),
                        };
                        return write_error_response(stream, "22023", &format!("invalid value for parameter \"search_path\": \"{rendered}\""));
                    }
                },
                _ => {
                    if pg_parameter(&name).is_none() {
                        session.fail();
                        return write_error_response(stream, "42704", &format!("unrecognized configuration parameter \"{name}\""));
                    }
                    // 其他 PG_PARAMETERS 已知参数接受为 no-op（gateway 不模拟写入）
                }
            }
            write_command_complete(stream, "SET")
        }
        Ok(Some(SqlSessionCommand::ShowParameter { name, .. })) => {
            if session.failed {
                return write_error_response(
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                );
            }
            let needle = name.trim().to_ascii_lowercase();
            let value: Option<String> =
                if needle == "search_path" { Some(session.search_path.clone()) } else { pg_parameter(&name).map(|s| s.to_string()) };
            match value {
                Some(value) => {
                    write_row_description(
                        stream,
                        &[SqlColumn { name: name.clone(), field_type: SqlFieldType::String, field_id: None }],
                        &[0],
                    )?;
                    write_data_row(
                        stream,
                        &[SqlValue::String(value)],
                        &[SqlColumn { name: name.clone(), field_type: SqlFieldType::String, field_id: None }],
                        &[0],
                    )?;
                    write_command_complete(stream, "SHOW")
                }
                None => {
                    session.fail();
                    write_error_response(stream, "42704", &format!("unrecognized configuration parameter \"{name}\""))
                }
            }
        }
        Ok(Some(SqlSessionCommand::ShowVariables { .. })) => {
            session.fail();
            write_error_response(stream, "42601", "SHOW VARIABLES is MySQL syntax, PostgreSQL uses SHOW <parameter>")
        }
        Ok(Some(SqlSessionCommand::Pragma { .. })) => {
            session.fail();
            write_error_response(stream, "42601", "PRAGMA is not PostgreSQL syntax")
        }
        Ok(Some(command @ (SqlSessionCommand::ResetVariables { .. } | SqlSessionCommand::Discard(_)))) => {
            match run_session_command(&command, session, extended) {
                Ok(tag) => write_command_complete(stream, tag),
                Err(SessionCommandError::Rejected { code, message }) => {
                    session.fail();
                    write_error_response(stream, code, message)
                }
                Err(SessionCommandError::Failed { code, message }) => {
                    session.fail();
                    write_error_response(stream, &code, &message)
                }
            }
        }
        Ok(Some(SqlSessionCommand::SetNames { .. })) | Ok(None) => {
            if session.failed {
                return write_error_response(
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                );
            }
            match runtime {
                Some(rt) => execute_simple_query(stream, sql, rt, session),
                None => {
                    session.fail();
                    write_error_response(stream, "0A000", "PostgreSQL query execution is not implemented (no SQL runtime)")
                }
            }
        }
    }
}

/// SimpleQuery 的 SQL bind → plan → execute 路径。
fn execute_simple_query(
    stream: &mut impl Write,
    sql: &str,
    runtime: &Arc<PgsqlSqlRuntime>,
    session: &mut TransactionSession,
) -> io::Result<()> {
    match run_statement(sql, runtime, session) {
        Ok(outcome) => write_execution_outcome(stream, &outcome),
        Err((code, message)) => write_error_response(stream, &code, &message),
    }
}

/// 执行结果的统一形状，供 SimpleQuery 与扩展查询共用。
struct ExecutionOutcome {
    is_read: bool,
    description: Vec<SqlColumn>,
    rows: Vec<Vec<SqlValue>>,
    command_tag: String,
}

impl ExecutionOutcome {
    /// Whether the statement reports a result set. Reads always do, and writes do
    /// only when the statement carried a `RETURNING` projection.
    fn returns_rows(&self) -> bool {
        self.is_read || !self.description.is_empty()
    }
}

/// bind → plan → execute，错误已映射为 SQLSTATE，并已标记事务失败。
fn run_statement(sql: &str, runtime: &Arc<PgsqlSqlRuntime>, session: &mut TransactionSession) -> Result<ExecutionOutcome, (String, String)> {
    // Metadata views are synthesized from the local schema registry. They never
    // bind into the catalog or touch the executor, so they branch off before the
    // regular session-command/bind path.
    let meta = match analyze_meta_query(sql, SqlDialect::Postgres) {
        Ok(meta) => meta,
        Err(error) => {
            session.fail();
            return Err(statement_error("metadata", error));
        }
    };
    if let Some(meta) = meta {
        let description = meta.describe();
        let result = meta.execute(&runtime.schema, "public");
        let command_tag = format!("SELECT {}", result.rows.len());
        return Ok(ExecutionOutcome { is_read: true, description, rows: result.rows, command_tag });
    }
    // 扩展查询不经过 `handle_simple_query`，语法错误必须在这里映射为 `42601`
    // 而不是被后面的 catalog 绑定错误统一归为 `42P01`。
    if parse_session_command(sql).is_err() {
        session.fail();
        return Err(("42601".to_string(), "Oak could not parse the SQL statement".to_string()));
    }
    let catalog = runtime.schema.catalog();
    let bound = bind_sql_catalog(sql, &catalog, catalog.schema_version(), SqlDialect::Postgres).map_err(|error| {
        session.fail();
        statement_error("bind", error)
    })?;
    // Sequence-owning columns default at write time. Allocation advances and
    // persists the counter before execution, so a failed or rolled-back
    // statement never reuses the value, matching PostgreSQL `nextval`.
    let bound = apply_auto_increment(&runtime.schema, bound).map_err(|error| {
        session.fail();
        statement_error("bind", error)
    })?;
    let plan = plan_statement_for_dialect(bound, &catalog, SqlDialect::Postgres).map_err(|error| {
        session.fail();
        statement_error("plan", error)
    })?;
    let is_read = matches!(
        &plan,
        SqlExecutionPlan::Read(_)
            | SqlExecutionPlan::Scan(_)
            | SqlExecutionPlan::Join(_)
            | SqlExecutionPlan::Scalar(_)
            | SqlExecutionPlan::SetOperation(_)
    );
    // Derive the result shape from the catalog plan so an empty result set
    // reports the declared type instead of falling back to `text`.
    let description = describe_plan(&plan);
    let handle_ref: Option<&dyn TransactionHandle> = session.handle.as_deref();
    let result = execute_plan(runtime.executor.as_ref(), handle_ref, &plan, SqlDialect::Postgres).map_err(|error| {
        session.fail();
        statement_error("execution", error)
    })?;
    let command_tag = if is_read {
        format!("SELECT {}", result.rows.len())
    }
    else {
        let affected = result.affected_rows;
        match &plan {
            SqlExecutionPlan::Insert(_) | SqlExecutionPlan::MultiInsert(_) | SqlExecutionPlan::InsertSelect(_) => {
                format!("INSERT 0 {affected}")
            }
            SqlExecutionPlan::Update(_) | SqlExecutionPlan::UpdateScan(_) => format!("UPDATE {affected}"),
            SqlExecutionPlan::Delete(_) | SqlExecutionPlan::DeleteScan(_) => format!("DELETE {affected}"),
            SqlExecutionPlan::Read(_)
            | SqlExecutionPlan::Scan(_)
            | SqlExecutionPlan::Join(_)
            | SqlExecutionPlan::Scalar(_)
            | SqlExecutionPlan::SetOperation(_) => {
                format!("SELECT {}", result.rows.len())
            }
        }
    };
    debug_assert_eq!(description.len(), result.columns.len(), "described columns must match executed columns");
    Ok(ExecutionOutcome { is_read, description, rows: result.rows, command_tag })
}

/// Maps a structured gateway failure to its PostgreSQL SQLSTATE. Protocol code
/// never inspects diagnostic text to recover the category.
fn pg_sqlstate(kind: SqlErrorKind) -> &'static str {
    use SqlErrorKind::*;
    match kind {
        Syntax => "42601",
        UnknownTable | UnknownTableQualifier => "42P01",
        AmbiguousTable | AmbiguousColumn => "42702",
        UnknownColumn => "42703",
        TypeMismatch => "42804",
        ValueTooLong => "22001",
        NotNullViolation => "23502",
        Unsupported => "0A000",
        UnknownFunction => "42883",
        UnknownVariable => "42704",
        // Only failures the gateway cannot classify reach the generic SQLSTATE.
        Internal => "XX000",
        CardinalityViolation => "21000",
        InvalidGrouping => "42803",
        OperandColumns => "42601",
        Parameter => "08P01",
        DuplicateKey => "23505",
        Conflict => "40001",
        DivisionByZero => "22012",
        NumericOverflow => "22003",
        InvalidSubstringLength => "22011",
        TableAlreadyExists => "42P07",
        ColumnAlreadyExists => "42701",
        IndexAlreadyExists => "42P07",
        UnknownIndex => "42704",
    }
}

fn statement_error(stage: &str, error: SqlFrontendError) -> (String, String) {
    (pg_sqlstate(error.kind).to_string(), format!("SQL {stage} error: {}", error.message))
}

fn write_execution_outcome(stream: &mut impl Write, outcome: &ExecutionOutcome) -> io::Result<()> {
    if outcome.returns_rows() {
        let formats = vec![0_i16; outcome.description.len()];
        write_row_description(stream, &outcome.description, &formats)?;
        for row in &outcome.rows {
            write_data_row(stream, row, &outcome.description, &formats)?;
        }
    }
    write_command_complete(stream, &outcome.command_tag)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExtendedStep {
    Continue,
    /// The error suspends the message queue until Sync and aborts the session
    /// transaction, matching PostgreSQL for every extended-protocol failure,
    /// including the `DISCARD ALL` transaction-block guard (`25001`).
    Failed,
}

/// 扩展查询协议的会话状态：命名/匿名 statement 与 portal。
#[derive(Default)]
struct ExtendedSession {
    statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
}

struct PreparedStatement {
    sql: String,
    parameter_oids: Vec<u32>,
    /// Catalog-derived result shape, computed once on the first `Describe(S)`
    /// or `Bind`. Writes describe to an empty shape.
    description: Option<Vec<SqlColumn>>,
    /// Session utility command (`RESET` / `DISCARD`) recognised by the shared
    /// lexical pre-filter at Parse time. Utility statements have no catalog
    /// shape: they describe as `NoData` and execute through
    /// [`run_session_command`] instead of the bind → plan → execute path.
    session_command: Option<SqlSessionCommand>,
}

struct Portal {
    /// Statement name this portal was bound from, used to cascade-close portals
    /// when the parent statement is removed (PostgreSQL protocol contract).
    statement_name: String,
    sql: String,
    parameters: Vec<SqlValue>,
    description: Vec<SqlColumn>,
    /// Requested result formats: empty means all text, one entry applies to all
    /// columns, otherwise one entry per described column.
    result_formats: Vec<i16>,
    /// Copied from the bound statement: utility statements execute as session
    /// side effects with no rows.
    session_command: Option<SqlSessionCommand>,
    outcome: Option<Result<ExecutionOutcome, (String, String)>>,
    sent_rows: usize,
}

fn handle_parse(stream: &mut impl Write, payload: &[u8], extended: &mut ExtendedSession) -> io::Result<ExtendedStep> {
    let mut cursor = 0;
    let (Some(name), Some(sql)) = (take_cstring(payload, &mut cursor), take_cstring(payload, &mut cursor))
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Parse message");
    };
    let Some(parameter_count) = take_i16(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Parse message");
    };
    if parameter_count < 0 {
        return extended_protocol_error(stream, "08P01", "negative PostgreSQL parameter count");
    }
    let mut parameter_oids = Vec::with_capacity(parameter_count as usize);
    for _ in 0..parameter_count {
        let Some(oid) = take_u32(payload, &mut cursor)
        else {
            return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Parse parameter types");
        };
        parameter_oids.push(oid);
    }
    // The extended protocol recognises the same session utility statements as
    // SimpleQuery through the one shared lexical pre-filter, so a `RESET` /
    // `DISCARD` Parse is stored as a session command instead of being deferred
    // to the catalog binder, which would reject it as a syntax error.
    let session_command = detect_session_reset_statement(sql);
    extended
        .statements
        .insert(name.to_string(), PreparedStatement { sql: sql.to_string(), parameter_oids, description: None, session_command });
    write_message(stream, b'1', &[])?;
    Ok(ExtendedStep::Continue)
}

fn handle_bind(
    stream: &mut impl Write,
    payload: &[u8],
    extended: &mut ExtendedSession,
    runtime: Option<&Arc<PgsqlSqlRuntime>>,
) -> io::Result<ExtendedStep> {
    let mut cursor = 0;
    let (Some(portal_name), Some(statement_name)) = (take_cstring(payload, &mut cursor), take_cstring(payload, &mut cursor))
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind message");
    };
    let Some(statement) = extended.statements.get_mut(statement_name)
    else {
        return extended_protocol_error(stream, "26000", &format!("prepared statement \"{statement_name}\" does not exist"));
    };
    let sql = statement.sql.clone();
    // Bind decodes binary parameter bytes by OID, so it must use the same
    // catalog-inferred types Describe reported rather than only what Parse
    // declared (which is usually empty).
    let parameter_oids = statement_parameter_description(&sql, &statement.parameter_oids, runtime);

    let Some(format_count) = take_i16(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind message");
    };
    let mut parameter_formats = Vec::with_capacity(format_count.max(0) as usize);
    for _ in 0..format_count.max(0) {
        let Some(format) = take_i16(payload, &mut cursor)
        else {
            return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind format codes");
        };
        if format != 0 && format != 1 {
            return extended_protocol_error(stream, "08P01", &format!("unsupported PostgreSQL format code {format}"));
        }
        parameter_formats.push(format);
    }

    let Some(parameter_count) = take_i16(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind message");
    };
    if parameter_formats.len() > 1 && parameter_formats.len() != parameter_count.max(0) as usize {
        return extended_protocol_error(stream, "08P01", "PostgreSQL parameter format count does not match parameters");
    }
    let mut parameters = Vec::with_capacity(parameter_count.max(0) as usize);
    for index in 0..parameter_count.max(0) {
        let Some(length) = take_i32(payload, &mut cursor)
        else {
            return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind parameter length");
        };
        if length < 0 {
            parameters.push(SqlValue::Null);
            continue;
        }
        let Some(bytes) = take_bytes(payload, &mut cursor, length as usize)
        else {
            return extended_protocol_error(stream, "08P01", "truncated PostgreSQL Bind parameter");
        };
        let oid = parameter_oids.get(index as usize).copied().unwrap_or(0);
        let format = match parameter_formats.len() {
            0 => 0,
            1 => parameter_formats[0],
            _ => parameter_formats[index as usize],
        };
        let decoded = if format == 1 { binary_parameter_value(oid, bytes) } else { parameter_value(oid, bytes) };
        match decoded {
            Ok(value) => parameters.push(value),
            Err((code, message)) => return extended_protocol_error(stream, &code, &message),
        }
    }

    let Some(result_format_count) = take_i16(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind result formats");
    };
    let mut result_formats = Vec::with_capacity(result_format_count.max(0) as usize);
    for _ in 0..result_format_count.max(0) {
        let Some(format) = take_i16(payload, &mut cursor)
        else {
            return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind result formats");
        };
        if format != 0 && format != 1 {
            return extended_protocol_error(stream, "08P01", &format!("unsupported PostgreSQL format code {format}"));
        }
        result_formats.push(format);
    }

    let session_command = statement.session_command.clone();
    if statement.description.is_none() {
        if session_command.is_some() {
            // Utility statements have no catalog result shape; they describe as
            // `NoData` exactly like PostgreSQL utility statements do.
            statement.description = Some(Vec::new());
        }
        else {
            match statement_description(&sql, runtime, Some(&parameters)) {
                Ok(description) => statement.description = Some(description),
                Err((code, message)) => return extended_protocol_error(stream, &code, &message),
            }
        }
    }
    let description = statement.description.clone().unwrap_or_default();
    if result_formats.len() > 1 && result_formats.len() != description.len() {
        return extended_protocol_error(stream, "08P01", "PostgreSQL result format count does not match columns");
    }

    extended.portals.insert(
        portal_name.to_string(),
        Portal {
            statement_name: statement_name.to_string(),
            sql,
            parameters,
            description,
            result_formats,
            session_command,
            outcome: None,
            sent_rows: 0,
        },
    );
    write_message(stream, b'2', &[])?;
    Ok(ExtendedStep::Continue)
}

fn handle_describe(
    stream: &mut impl Write,
    payload: &[u8],
    extended: &mut ExtendedSession,
    session: &mut TransactionSession,
    runtime: Option<&Arc<PgsqlSqlRuntime>>,
) -> io::Result<ExtendedStep> {
    let Some((&kind, rest)) = payload.split_first()
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Describe message");
    };
    let mut cursor = 0;
    let Some(name) = take_cstring(rest, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Describe target");
    };
    match kind {
        b'S' => {
            let Some(statement) = extended.statements.get_mut(name)
            else {
                return extended_protocol_error(stream, "26000", &format!("prepared statement \"{name}\" does not exist"));
            };
            let parameters = statement_parameter_description(&statement.sql, &statement.parameter_oids, runtime);
            // Session utility statements have no catalog shape; the empty
            // description below answers them with `NoData`, like PostgreSQL.
            if statement.description.is_none() && statement.session_command.is_none() {
                match statement_description(&statement.sql, runtime, None) {
                    Ok(description) => statement.description = Some(description),
                    Err((code, message)) => {
                        write_error_response(stream, &code, &message)?;
                        return Ok(ExtendedStep::Failed);
                    }
                }
            }
            write_parameter_description(stream, &parameters)?;
            match statement.description.as_deref() {
                Some([]) | None => write_message(stream, b'n', &[])?,
                Some(description) => {
                    let formats = vec![0_i16; description.len()];
                    write_row_description(stream, description, &formats)?;
                }
            }
            Ok(ExtendedStep::Continue)
        }
        b'P' => {
            // Describe(portal) reports shape only. Execution belongs to Execute.
            let Some(portal) = extended.portals.get(name)
            else {
                return extended_protocol_error(stream, "34000", &format!("portal \"{name}\" does not exist"));
            };
            if session.failed {
                write_error_response(stream, "25P02", "current transaction is aborted, commands ignored until end of transaction block")?;
                return Ok(ExtendedStep::Failed);
            }
            if portal.description.is_empty() {
                write_message(stream, b'n', &[])?;
            }
            else {
                let formats = resolve_formats(&portal.result_formats, portal.description.len());
                write_row_description(stream, &portal.description, &formats)?;
            }
            Ok(ExtendedStep::Continue)
        }
        _ => extended_protocol_error(stream, "08P01", "unsupported PostgreSQL Describe target"),
    }
}

/// Number of placeholders the SQL surface references, padding declared
/// parameter OIDs with zero (`unspecified`) when the client sent fewer.
///
/// An `unspecified` OID forces strict drivers (`tokio-postgres` and the sync
/// `postgres` crate) to resolve the type through `pg_catalog`, which this
/// gateway answers with metadata-only shapes. A client that parsed with no
/// OIDs therefore needs the catalog-derived OID here, otherwise every
/// parameterized statement fails before it executes.
fn statement_parameter_description(sql: &str, declared: &[u32], runtime: Option<&Arc<PgsqlSqlRuntime>>) -> Vec<u32> {
    let referenced = neutralized_sql_parameters(sql, SqlParameterStyle::DollarNumber).map(|(_, count)| count).unwrap_or(0);
    let mut parameters = declared.to_vec();
    parameters.resize(parameters.len().max(referenced), 0);
    if let Some(runtime) = runtime {
        let catalog = runtime.schema.catalog();
        let inferred = infer_parameter_oids(sql, &catalog, referenced);
        for (position, oid) in parameters.iter_mut().enumerate() {
            if *oid == 0 {
                // Placeholders are 1-based (`$1` is the first parameter), so the
                // parameter at `position` reads inference slot `position + 1`.
                if let Some(inferred) = inferred.get(position + 1).copied().filter(|oid| *oid != 0) {
                    *oid = inferred;
                }
            }
        }
    }
    for oid in parameters.iter_mut() {
        // PostgreSQL always resolves a parameter to a concrete type. A shape the
        // text scan cannot attribute — `strpos("body", $1)`, for example — falls
        // back to `text`, which is how the gateway's own literal binder reads an
        // untyped value. Returning `unspecified` would make strict drivers query
        // `pg_catalog` for the type.
        if *oid == 0 {
            *oid = OID_TEXT;
        }
    }
    parameters
}

/// Catalog-derived parameter OIDs for a statement that declares none.
///
/// The text scan covers the shapes a client can bind against a catalog:
/// `INSERT ... (columns) VALUES (placeholders)` maps placeholders positionally
/// and `column = placeholder` maps a placeholder to the column on either side.
/// Placeholders outside those shapes stay `unspecified`.
fn infer_parameter_oids(sql: &str, catalog: &SqlCatalog, count: usize) -> Vec<u32> {
    let tokens = tokenize_sql_context(sql);
    // Slot 0 is unused: `$1` writes to index 1 so placeholder numbers index the
    // vector directly.
    let mut oids = vec![0u32; count + 1];

    if let Some((table, columns, values_at)) = insert_shape(&tokens) {
        let mut value_index = 0usize;
        // `values_at` points past the tuple's opening parenthesis.
        let mut depth = 1usize;
        for token in &tokens[values_at..] {
            match token {
                SqlToken::Open => depth += 1,
                SqlToken::Close => {
                    if depth <= 1 {
                        break;
                    }
                    depth -= 1;
                }
                SqlToken::Comma if depth == 1 => value_index += 1,
                SqlToken::Placeholder(index) if depth == 1 => {
                    if let Some(column) = columns.get(value_index) {
                        set_inferred_oid(&mut oids, *index, catalog_column_oid(catalog, Some(&table), column));
                    }
                }
                _ => {}
            }
        }
    }

    for (position, token) in tokens.iter().enumerate() {
        let Some(column) = identifier(token)
        else {
            continue;
        };
        let pair = match tokens.get(position + 1) {
            Some(SqlToken::Equals) => tokens.get(position + 2).and_then(|next| match next {
                SqlToken::Placeholder(index) => Some(*index),
                _ => None,
            }),
            Some(SqlToken::Placeholder(index)) => Some(*index),
            _ => None,
        };
        if let Some(index) = pair {
            set_inferred_oid(&mut oids, index, catalog_column_oid(catalog, None, column));
        }
    }

    for (position, token) in tokens.iter().enumerate() {
        let SqlToken::Placeholder(index) = token
        else {
            continue;
        };
        if !matches!(tokens.get(position + 1), Some(SqlToken::Equals)) {
            continue;
        }
        let Some(column) = tokens.get(position + 2).and_then(identifier)
        else {
            continue;
        };
        set_inferred_oid(&mut oids, *index, catalog_column_oid(catalog, None, column));
    }

    oids
}

/// A column or table name, quoted or bare. Bare identifiers reach the tokenizer
/// as [`SqlToken::Word`] because the same token class carries SQL keywords.
fn identifier(token: &SqlToken) -> Option<&str> {
    match token {
        SqlToken::Ident(name) | SqlToken::Word(name) => Some(name),
        _ => None,
    }
}

fn set_inferred_oid(oids: &mut [u32], index: usize, oid: u32) {
    if let Some(slot) = oids.get_mut(index) {
        if *slot == 0 {
            *slot = oid;
        }
    }
}

/// Column type OID for a named catalog column. `table` restricts the lookup to
/// one catalog table when the statement already named it.
fn catalog_column_oid(catalog: &SqlCatalog, table: Option<&str>, column: &str) -> u32 {
    for candidate in catalog.tables() {
        if table.is_some_and(|name| name != candidate.name) {
            continue;
        }
        if let Some(field) = candidate.fields.iter().find(|field| field.name == column) {
            return pg_oid(&field.field_type.inner());
        }
    }
    0
}

/// Token context for parameter type inference: quoted identifiers,
/// placeholders, and the punctuation that separates them.
#[derive(Debug, PartialEq, Eq)]
enum SqlToken {
    Ident(String),
    Word(String),
    Placeholder(usize),
    Equals,
    Comma,
    Open,
    Close,
    Other,
}

fn tokenize_sql_context(sql: &str) -> Vec<SqlToken> {
    let chars: Vec<char> = sql.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0usize;
    while index < chars.len() {
        let current = chars[index];
        if current == '"' {
            let mut name = String::new();
            index += 1;
            while index < chars.len() {
                if chars[index] == '"' {
                    if chars.get(index + 1) == Some(&'"') {
                        name.push('"');
                        index += 2;
                        continue;
                    }
                    index += 1;
                    break;
                }
                name.push(chars[index]);
                index += 1;
            }
            tokens.push(SqlToken::Ident(name));
            continue;
        }
        if current == '\'' {
            index += 1;
            while index < chars.len() {
                if chars[index] == '\'' {
                    index += 1;
                    break;
                }
                index += 1;
            }
            tokens.push(SqlToken::Other);
            continue;
        }
        if current == '$' {
            let start = index + 1;
            let mut end = start;
            while end < chars.len() && chars[end].is_ascii_digit() {
                end += 1;
            }
            if let Ok(value) = chars[start..end].iter().collect::<String>().parse::<usize>() {
                tokens.push(SqlToken::Placeholder(value));
                index = end;
                continue;
            }
        }
        if current.is_alphanumeric() || current == '_' {
            let mut word = String::new();
            while index < chars.len() && (chars[index].is_alphanumeric() || chars[index] == '_') {
                word.push(chars[index]);
                index += 1;
            }
            tokens.push(SqlToken::Word(word));
            continue;
        }
        if current.is_whitespace() {
            // Whitespace carries no shape; skipping it keeps keyword and
            // identifier adjacency meaningful for the position checks below.
            index += 1;
            continue;
        }
        match current {
            '=' => tokens.push(SqlToken::Equals),
            ',' => tokens.push(SqlToken::Comma),
            '(' => tokens.push(SqlToken::Open),
            ')' => tokens.push(SqlToken::Close),
            _ => tokens.push(SqlToken::Other),
        }
        index += 1;
    }
    tokens
}

/// `INSERT INTO "table" ("c1", "c2", ...) VALUES (...)` positions, returning
/// the target table, its column names in VALUES order, and the token index of
/// the first VALUES tuple's contents.
fn insert_shape(tokens: &[SqlToken]) -> Option<(String, Vec<String>, usize)> {
    let insert_at = tokens.iter().position(|token| matches!(token, SqlToken::Word(word) if word.eq_ignore_ascii_case("INSERT")))?;
    if !matches!(tokens.get(insert_at + 1), Some(SqlToken::Word(word)) if word.eq_ignore_ascii_case("INTO")) {
        return None;
    }
    let Some(table) = tokens.get(insert_at + 2).and_then(identifier)
    else {
        return None;
    };
    let mut columns = Vec::new();
    if matches!(tokens.get(insert_at + 3), Some(SqlToken::Open)) {
        for token in &tokens[insert_at + 4..] {
            match token {
                SqlToken::Close => break,
                other => {
                    if let Some(name) = identifier(other) {
                        columns.push(name.to_string());
                    }
                }
            }
        }
    }
    let values_at = tokens.iter().enumerate().skip(insert_at).find_map(|(position, token)| match token {
        SqlToken::Word(word) if word.eq_ignore_ascii_case("VALUES") => Some(position),
        _ => None,
    })?;
    if !matches!(tokens.get(values_at + 1), Some(SqlToken::Open)) {
        return None;
    }
    Some((table.to_string(), columns, values_at + 2))
}

/// Resolves Bind result formats into one entry per described column.
fn resolve_formats(formats: &[i16], columns: usize) -> Vec<i16> {
    match formats.len() {
        0 => vec![0; columns],
        1 => vec![formats[0]; columns],
        _ => formats.to_vec(),
    }
}

/// Derives the statement result shape from the catalog without executing.
///
/// Bound parameters are rendered exactly when they are available, so a
/// describe after Bind cannot disagree with the execution. Metadata-only
/// describes neutralize each placeholder to `0` because a numeric literal
/// keeps pk lookups plannable, and retry with `NULL` when `0` is not writable
/// for the target column.
fn statement_description(
    sql: &str,
    runtime: Option<&Arc<PgsqlSqlRuntime>>,
    parameters: Option<&[SqlValue]>,
) -> Result<Vec<SqlColumn>, (String, String)> {
    let Some(runtime) = runtime
    else {
        return Err(("0A000".to_string(), "PostgreSQL query execution is not implemented (no SQL runtime)".to_string()));
    };
    if let Some(parameters) = parameters {
        let rendered =
            substitute_sql_parameters(sql, parameters, SqlParameterStyle::DollarNumber).map_err(|error| statement_error("parameter", error))?;
        if let Some(meta) = analyze_meta_query(&rendered, SqlDialect::Postgres).map_err(|error| statement_error("metadata", error))? {
            return Ok(meta.describe());
        }
        return plan_description(&rendered, runtime);
    }
    let (neutral_sql, _) =
        neutralized_sql_parameters(sql, SqlParameterStyle::DollarNumber).map_err(|error| statement_error("parameter", error))?;
    if let Some(meta) = analyze_meta_query(&neutral_sql, SqlDialect::Postgres).map_err(|error| statement_error("metadata", error))? {
        return Ok(meta.describe());
    }
    describe_without_values(&neutral_sql, runtime)
}

/// Describes a statement that still carries placeholder stand-ins.
///
/// Write values are validated only when the statement executes, and a
/// metadata describe has no real parameters: a neutral `0` cannot be written
/// to text/uuid/bool columns and a neutral `NULL` cannot be a primary key.
/// Writes therefore describe from their bound shape instead of being planned.
fn describe_without_values(sql: &str, runtime: &Arc<PgsqlSqlRuntime>) -> Result<Vec<SqlColumn>, (String, String)> {
    plan_description(sql, runtime)
}

/// Binds a statement only to read its result shape.
///
/// An `INSERT` never plans here. Sequence-owning columns are filled at execute
/// time, so planning the describe would either fail on the missing primary key
/// or, if the registry allocation were reused, consume `nextval` values the
/// client never observes. `describe_bound_insert` derives the `RETURNING`
/// shape from the bound columns instead.
fn plan_description(sql: &str, runtime: &Arc<PgsqlSqlRuntime>) -> Result<Vec<SqlColumn>, (String, String)> {
    let catalog = runtime.schema.catalog();
    let bound =
        bind_sql_catalog(sql, &catalog, catalog.schema_version(), SqlDialect::Postgres).map_err(|error| statement_error("bind", error))?;
    match bound {
        SqlCatalogBoundStatement::Insert(insert) => Ok(describe_bound_insert(&insert, &catalog, SqlDialect::Postgres)),
        // `UPDATE` / `DELETE` in this gateway carry no `RETURNING` shape.
        SqlCatalogBoundStatement::Update(_) | SqlCatalogBoundStatement::Delete(_) => Ok(Vec::new()),
        bound => {
            let plan = plan_statement_for_dialect(bound, &catalog, SqlDialect::Postgres).map_err(|error| statement_error("plan", error))?;
            Ok(describe_plan(&plan))
        }
    }
}

fn handle_execute(
    stream: &mut impl Write,
    payload: &[u8],
    extended: &mut ExtendedSession,
    session: &mut TransactionSession,
    runtime: Option<&Arc<PgsqlSqlRuntime>>,
) -> io::Result<ExtendedStep> {
    let mut cursor = 0;
    let Some(name) = take_cstring(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Execute message");
    };
    let Some(max_rows) = take_i32(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Execute row limit");
    };
    // Session utility statements run the same shared side effects as SimpleQuery
    // and answer with a lone CommandComplete: no DataRow, no RowDescription.
    let session_command = extended.portals.get(name).and_then(|portal| portal.session_command.clone());
    if let Some(command) = session_command {
        return match run_session_command(&command, session, extended) {
            Ok(tag) => {
                write_command_complete(stream, tag)?;
                Ok(ExtendedStep::Continue)
            }
            Err(SessionCommandError::Rejected { code, message }) => {
                write_error_response(stream, code, message)?;
                Ok(ExtendedStep::Failed)
            }
            Err(SessionCommandError::Failed { code, message }) => {
                write_error_response(stream, &code, &message)?;
                Ok(ExtendedStep::Failed)
            }
        };
    }
    let Some(portal) = extended.portals.get_mut(name)
    else {
        return extended_protocol_error(stream, "34000", &format!("portal \"{name}\" does not exist"));
    };
    // Same contract as SimpleQuery: an empty statement executes to an
    // EmptyQueryResponse with no command tag.
    if portal.sql.trim().is_empty() {
        write_message(stream, b'I', &[])?;
        return Ok(ExtendedStep::Continue);
    }
    prepare_portal(portal, session, runtime);
    let Some(outcome) = portal.outcome.as_ref()
    else {
        unreachable!("prepare_portal always stores an outcome");
    };
    match outcome {
        Ok(outcome) => {
            let command_tag = outcome.command_tag.clone();
            if outcome.returns_rows() {
                let start = portal.sent_rows.min(outcome.rows.len());
                let end = if max_rows > 0 { (start + max_rows as usize).min(outcome.rows.len()) } else { outcome.rows.len() };
                let formats = resolve_formats(&portal.result_formats, outcome.description.len());
                let description = outcome.description.clone();
                let chunk: Vec<Vec<SqlValue>> = outcome.rows[start..end].to_vec();
                portal.sent_rows = end;
                for row in &chunk {
                    write_data_row(stream, row, &description, &formats)?;
                }
                if end < outcome.rows.len() {
                    write_message(stream, b's', &[])?;
                    return Ok(ExtendedStep::Continue);
                }
            }
            write_command_complete(stream, &command_tag)?;
            Ok(ExtendedStep::Continue)
        }
        Err((code, message)) => {
            write_error_response(stream, code, message)?;
            Ok(ExtendedStep::Failed)
        }
    }
}

fn handle_close(stream: &mut impl Write, payload: &[u8], extended: &mut ExtendedSession) -> io::Result<ExtendedStep> {
    let Some((&kind, rest)) = payload.split_first()
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Close message");
    };
    let mut cursor = 0;
    let Some(name) = take_cstring(rest, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Close target");
    };
    match kind {
        b'S' => {
            extended.statements.remove(name);
            // PostgreSQL invalidates any portal bound to the closed statement.
            extended.portals.retain(|_, portal| portal.statement_name != name);
        }
        b'P' => {
            extended.portals.remove(name);
        }
        _ => return extended_protocol_error(stream, "08P01", "unsupported PostgreSQL Close target"),
    }
    write_message(stream, b'3', &[])?;
    Ok(ExtendedStep::Continue)
}

/// Bind 只登记参数，真正执行推迟到 Describe / Execute，结果缓存避免重复写入。
fn prepare_portal(portal: &mut Portal, session: &mut TransactionSession, runtime: Option<&Arc<PgsqlSqlRuntime>>) {
    if portal.outcome.is_some() {
        return;
    }
    if session.failed {
        portal.outcome =
            Some(Err(("25P02".to_string(), "current transaction is aborted, commands ignored until end of transaction block".to_string())));
        return;
    }
    let Some(runtime) = runtime
    else {
        session.fail();
        portal.outcome = Some(Err(("0A000".to_string(), "PostgreSQL query execution is not implemented (no SQL runtime)".to_string())));
        return;
    };
    let sql = match substitute_sql_parameters(&portal.sql, &portal.parameters, SqlParameterStyle::DollarNumber) {
        Ok(sql) => sql,
        Err(error) => {
            session.fail();
            portal.outcome = Some(Err(statement_error("parameter", error)));
            return;
        }
    };
    portal.outcome = Some(run_statement(&sql, runtime, session));
}

fn parameter_value(oid: u32, bytes: &[u8]) -> Result<SqlValue, (String, String)> {
    // `bytea` 的文本形态是转义或十六进制拼写，不是原始 octets。网关不把这段
    // 拼写当成文本收下，否则转义字符本身会被当作载荷写进列里。二进制格式或
    // `CAST('<hex>' AS BYTEA)` 才是无损通道。
    if oid == OID_BYTEA {
        return Err((
            "0A000".to_string(),
            "text-format bytea parameters are not supported, send the binary format or a hexadecimal CAST".to_string(),
        ));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| ("22021".to_string(), "parameter is not valid UTF-8".to_string()))?;
    match oid {
        0 => Ok(infer_parameter_value(text)),
        16 => match text.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" | "on" | "yes" | "y" => Ok(SqlValue::Boolean(true)),
            "f" | "false" | "0" | "off" | "no" | "n" => Ok(SqlValue::Boolean(false)),
            _ => Err(("22P02".to_string(), format!("invalid input syntax for type boolean: \"{text}\""))),
        },
        20 | 21 | 23 | 26 => text
            .parse::<i64>()
            .map(SqlValue::Integer)
            .map_err(|_| ("22P02".to_string(), format!("invalid input syntax for integer: \"{text}\""))),
        700 | 701 => text
            .parse::<f64>()
            .map(SqlValue::Number)
            .map_err(|_| ("22P02".to_string(), format!("invalid input syntax for double precision: \"{text}\""))),
        _ => Ok(SqlValue::String(text.to_string())),
    }
}

/// Decodes a binary-format Bind parameter by its declared OID. An unspecified
/// OID cannot be decoded without text syntax and is rejected explicitly.
fn binary_parameter_value(oid: u32, bytes: &[u8]) -> Result<SqlValue, (String, String)> {
    let width_error =
        |expected: usize| ("08P01".to_string(), format!("binary parameter for type OID {oid} expects {expected} bytes, got {}", bytes.len()));
    match oid {
        0 => Err(("42P18".to_string(), "cannot determine data type of a binary parameter".to_string())),
        OID_BOOL => match bytes {
            [0] => Ok(SqlValue::Boolean(false)),
            [1] => Ok(SqlValue::Boolean(true)),
            _ => Err(width_error(1)),
        },
        OID_INT2 => match bytes {
            [a, b] => Ok(SqlValue::Integer(i16::from_be_bytes([*a, *b]).into())),
            _ => Err(width_error(2)),
        },
        OID_INT4 => match bytes {
            [a, b, c, d] => Ok(SqlValue::Integer(i32::from_be_bytes([*a, *b, *c, *d]).into())),
            _ => Err(width_error(4)),
        },
        OID_INT8 => match bytes {
            [a, b, c, d, e, f, g, h] => Ok(SqlValue::Integer(i64::from_be_bytes([*a, *b, *c, *d, *e, *f, *g, *h]))),
            _ => Err(width_error(8)),
        },
        OID_FLOAT4 => match bytes {
            [a, b, c, d] => Ok(SqlValue::Number(f32::from_be_bytes([*a, *b, *c, *d]).into())),
            _ => Err(width_error(4)),
        },
        OID_FLOAT8 => match bytes {
            [a, b, c, d, e, f, g, h] => Ok(SqlValue::Number(f64::from_be_bytes([*a, *b, *c, *d, *e, *f, *g, *h]))),
            _ => Err(width_error(8)),
        },
        // `bytea` 的二进制格式就是原始 octets，原样收下，不做任何文本解码。
        OID_BYTEA => Ok(SqlValue::Bytes(bytes.to_vec())),
        OID_TEXT | OID_VARCHAR | OID_NAME => std::str::from_utf8(bytes)
            .map(|text| SqlValue::String(text.to_string()))
            .map_err(|_| ("22021".to_string(), "binary parameter is not valid UTF-8".to_string())),
        _ => Err(("0A000".to_string(), format!("binary input for parameter type OID {oid} is not supported"))),
    }
}

/// Parse 未提供 OID 时的宽松推断，保持 `"007"` 这类文本不被数字转义。
fn infer_parameter_value(text: &str) -> SqlValue {
    if text.parse::<i64>().map(|value| value.to_string() == text).unwrap_or(false) {
        return SqlValue::Integer(text.parse().expect("checked integer"));
    }
    if (text.contains('.') || text.contains('e') || text.contains('E')) && text.parse::<f64>().is_ok() {
        return SqlValue::Number(text.parse().expect("checked float"));
    }
    SqlValue::String(text.to_string())
}

fn write_parameter_description(stream: &mut impl Write, oids: &[u32]) -> io::Result<()> {
    let mut payload = Vec::with_capacity(2 + oids.len() * 4);
    payload.extend_from_slice(&(oids.len() as i16).to_be_bytes());
    for oid in oids {
        payload.extend_from_slice(&oid.to_be_bytes());
    }
    write_message(stream, b't', &payload)
}

fn extended_protocol_error(stream: &mut impl Write, code: &str, message: &str) -> io::Result<ExtendedStep> {
    write_error_response(stream, code, message)?;
    Ok(ExtendedStep::Failed)
}

fn take_bytes<'a>(payload: &'a [u8], cursor: &mut usize, length: usize) -> Option<&'a [u8]> {
    let end = cursor.checked_add(length)?;
    let bytes = payload.get(*cursor..end)?;
    *cursor = end;
    Some(bytes)
}

fn take_cstring<'a>(payload: &'a [u8], cursor: &mut usize) -> Option<&'a str> {
    let rest = payload.get(*cursor..)?;
    let position = rest.iter().position(|byte| *byte == 0)?;
    let bytes = &rest[..position];
    *cursor += position + 1;
    std::str::from_utf8(bytes).ok()
}

fn take_i16(payload: &[u8], cursor: &mut usize) -> Option<i16> {
    let bytes = take_bytes(payload, cursor, 2)?;
    Some(i16::from_be_bytes([bytes[0], bytes[1]]))
}

fn take_i32(payload: &[u8], cursor: &mut usize) -> Option<i32> {
    let bytes = take_bytes(payload, cursor, 4)?;
    Some(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn take_u32(payload: &[u8], cursor: &mut usize) -> Option<u32> {
    let bytes = take_bytes(payload, cursor, 4)?;
    Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// Emulated PostgreSQL configuration parameters exposed through `SHOW <name>`.
const PG_PARAMETERS: &[(&str, &str)] = &[
    ("application_name", ""),
    ("client_encoding", "UTF8"),
    ("client_min_messages", "notice"),
    ("datestyle", "ISO, MDY"),
    ("default_transaction_isolation", "read committed"),
    ("extra_float_digits", "3"),
    ("integer_datetimes", "on"),
    ("intervalstyle", "postgres"),
    ("server_encoding", "UTF8"),
    ("search_path", DEFAULT_SEARCH_PATH),
    ("standard_conforming_strings", "on"),
    ("timezone", "UTC"),
    ("transaction_isolation", "read committed"),
    ("transaction_read_only", "off"),
    ("default_transaction_read_only", "off"),
];

/// Case-insensitive lookup in [`PG_PARAMETERS`].
fn pg_parameter(name: &str) -> Option<&'static str> {
    let needle = name.trim().to_ascii_lowercase();
    if needle == "server_version" {
        return Some(pg_server_version());
    }
    PG_PARAMETERS.iter().find(|(key, _)| *key == needle).map(|(_, value)| *value)
}

/// PostgreSQL type OID for a described gateway field type.
fn pg_oid(field_type: &SqlFieldType) -> u32 {
    match field_type.inner() {
        SqlFieldType::Int32 => OID_INT4,
        SqlFieldType::Int64 => OID_INT8,
        SqlFieldType::Float64 => OID_FLOAT8,
        SqlFieldType::Bool => OID_BOOL,
        SqlFieldType::Bytes => OID_BYTEA,
        SqlFieldType::Decimal => OID_NUMERIC,
        SqlFieldType::String | SqlFieldType::Unknown | SqlFieldType::Optional(_) => OID_TEXT,
    }
}

/// PostgreSQL RowDescription message ('T') — catalog-derived OIDs and the
/// format codes requested at Bind.
fn write_row_description(stream: &mut impl Write, columns: &[SqlColumn], formats: &[i16]) -> io::Result<()> {
    let mut payload = Vec::with_capacity(64);
    payload.push(b'T');
    let mut body = Vec::with_capacity(48);
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for (index, column) in columns.iter().enumerate() {
        let oid = pg_oid(&column.field_type);
        body.extend_from_slice(column.name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i32.to_be_bytes()); // table_oid = 0
        body.extend_from_slice(&0_i16.to_be_bytes()); // column_attr = 0
        body.extend_from_slice(&oid.to_be_bytes());
        body.extend_from_slice(&type_size(oid).to_be_bytes());
        body.extend_from_slice(&(-1_i32).to_be_bytes()); // type_mod = -1
        body.extend_from_slice(&formats.get(index).copied().unwrap_or(0).to_be_bytes());
    }
    payload.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    payload.extend_from_slice(&body);
    stream.write_all(&payload)
}

fn type_size(oid: u32) -> i16 {
    match oid {
        OID_BOOL => 1,
        OID_INT2 => 2,
        OID_INT4 | OID_FLOAT4 => 4,
        OID_INT8 | OID_FLOAT8 => 8,
        _ => -1,
    }
}

/// PostgreSQL DataRow message ('D') — text or binary encoding per column.
fn write_data_row(stream: &mut impl Write, row: &[SqlValue], columns: &[SqlColumn], formats: &[i16]) -> io::Result<()> {
    let mut payload = Vec::with_capacity(64);
    payload.push(b'D');
    let mut body = Vec::with_capacity(48);
    body.extend_from_slice(&(row.len() as i16).to_be_bytes());
    for (index, value) in row.iter().enumerate() {
        if matches!(value, SqlValue::Null) {
            body.extend_from_slice(&(-1_i32).to_be_bytes());
            continue;
        }
        let oid = columns.get(index).map(|column| pg_oid(&column.field_type)).unwrap_or(OID_TEXT);
        let bytes = if formats.get(index).copied().unwrap_or(0) == 1 {
            match pg_binary_value(oid, value) {
                Ok(bytes) => bytes,
                Err((code, message)) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, format!("{code}: {message}")));
                }
            }
        }
        else {
            sql_value_to_pg_text(value).into_bytes()
        };
        body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        body.extend_from_slice(&bytes);
    }
    payload.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    payload.extend_from_slice(&body);
    stream.write_all(&payload)
}

/// Encodes a value in the big-endian binary representation of its column OID.
fn pg_binary_value(oid: u32, value: &SqlValue) -> Result<Vec<u8>, (String, String)> {
    match oid {
        OID_BOOL => Ok(vec![u8::from(truthy_value(value)?)]),
        OID_INT2 => {
            let value = integer_value(value)?;
            let narrow = i16::try_from(value).map_err(|_| ("22003".to_string(), format!("smallint out of range: {value}")))?;
            Ok(narrow.to_be_bytes().to_vec())
        }
        OID_INT4 => {
            let value = integer_value(value)?;
            let narrow = i32::try_from(value).map_err(|_| ("22003".to_string(), format!("integer out of range: {value}")))?;
            Ok(narrow.to_be_bytes().to_vec())
        }
        OID_INT8 => Ok(integer_value(value)?.to_be_bytes().to_vec()),
        OID_FLOAT4 => Ok((float_value(value)? as f32).to_be_bytes().to_vec()),
        OID_FLOAT8 => Ok(float_value(value)?.to_be_bytes().to_vec()),
        OID_TEXT | OID_VARCHAR | OID_NAME => Ok(sql_value_to_pg_text(value).into_bytes()),
        OID_BYTEA => match value {
            SqlValue::Bytes(bytes) => Ok(bytes.clone()),
            other => Err(("22P02".to_string(), format!("value {other:?} is not a bytea payload"))),
        },
        _ => Err(("0A000".to_string(), format!("binary output for type OID {oid} is not supported"))),
    }
}

fn integer_value(value: &SqlValue) -> Result<i64, (String, String)> {
    match value {
        SqlValue::Integer(value) => Ok(*value),
        SqlValue::Boolean(value) => Ok(i64::from(*value)),
        SqlValue::Number(value) if value.fract() == 0.0 => Ok(*value as i64),
        SqlValue::String(text) => {
            text.parse::<i64>().map_err(|_| ("22P02".to_string(), format!("invalid input syntax for integer: \"{text}\"")))
        }
        other => Err(("22P02".to_string(), format!("value {other:?} is not a binary integer"))),
    }
}

fn float_value(value: &SqlValue) -> Result<f64, (String, String)> {
    match value {
        SqlValue::Integer(value) => Ok(*value as f64),
        SqlValue::Number(value) => Ok(*value),
        SqlValue::Boolean(value) => Ok(if *value { 1.0 } else { 0.0 }),
        SqlValue::String(text) => {
            text.parse::<f64>().map_err(|_| ("22P02".to_string(), format!("invalid input syntax for double precision: \"{text}\"")))
        }
        other => Err(("22P02".to_string(), format!("value {other:?} is not a binary float"))),
    }
}

fn truthy_value(value: &SqlValue) -> Result<bool, (String, String)> {
    match value {
        SqlValue::Boolean(value) => Ok(*value),
        SqlValue::Integer(value) => Ok(*value != 0),
        SqlValue::String(text) => match text.as_str() {
            "t" | "true" | "1" => Ok(true),
            "f" | "false" | "0" => Ok(false),
            _ => Err(("22P02".to_string(), format!("invalid input syntax for type boolean: \"{text}\""))),
        },
        other => Err(("22P02".to_string(), format!("value {other:?} is not a binary boolean"))),
    }
}

fn sql_value_to_pg_text(value: &SqlValue) -> String {
    match value {
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Number(n) => format!("{n}"),
        SqlValue::String(s) => s.clone(),
        // PostgreSQL 的 `bytea` 文本格式就是十六进制拼写，可以无损表达任意 octets。
        SqlValue::Bytes(bytes) => format!("\\x{}", encode_hex(bytes)),
        SqlValue::Boolean(b) => {
            if *b {
                "t".into()
            }
            else {
                "f".into()
            }
        }
        SqlValue::Null => unreachable!(),
    }
}

struct OwnedStartup {
    code: u32,
    payload: Vec<u8>,
}

struct OwnedMessage {
    tag: u8,
    payload: Vec<u8>,
}

fn read_startup(stream: &mut impl Read, pending: &mut Vec<u8>) -> io::Result<OwnedStartup> {
    loop {
        match decode_startup(pending, MAX_STARTUP_FRAME) {
            Ok(Some(packet)) => {
                let result = OwnedStartup { code: packet.code, payload: packet.payload.to_vec() };
                pending.drain(..packet.consumed);
                return Ok(result);
            }
            Ok(None) => read_more(stream, pending)?,
            Err(error) => return Err(frame_error(error)),
        }
    }
}

fn read_message(stream: &mut impl Read, pending: &mut Vec<u8>) -> io::Result<Option<OwnedMessage>> {
    loop {
        match decode_message(pending, MAX_MESSAGE_FRAME) {
            Ok(Some(message)) => {
                let result = OwnedMessage { tag: message.tag, payload: message.payload.to_vec() };
                pending.drain(..message.consumed);
                return Ok(Some(result));
            }
            Ok(None) => {
                if let Err(error) = read_more(stream, pending) {
                    if error.kind() == io::ErrorKind::UnexpectedEof {
                        return if pending.is_empty() {
                            Ok(None)
                        }
                        else {
                            Err(io::Error::new(io::ErrorKind::InvalidData, "truncated PostgreSQL message"))
                        };
                    }
                    return Err(error);
                }
            }
            Err(error) => return Err(frame_error(error)),
        }
    }
}

fn read_more(stream: &mut impl Read, pending: &mut Vec<u8>) -> io::Result<()> {
    let mut chunk = [0; 8192];
    let read = stream.read(&mut chunk)?;
    if read == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "PostgreSQL peer closed"));
    }
    pending.extend_from_slice(&chunk[..read]);
    Ok(())
}

fn send_authentication_ok(stream: &mut impl Write) -> io::Result<()> {
    write_message(stream, b'R', &0_u32.to_be_bytes())
}

fn send_parameter(stream: &mut impl Write, key: &str, value: &str) -> io::Result<()> {
    let mut payload = Vec::with_capacity(key.len() + value.len() + 2);
    payload.extend_from_slice(key.as_bytes());
    payload.push(0);
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
    write_message(stream, b'S', &payload)
}

fn send_backend_key(stream: &mut impl Write) -> io::Result<()> {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&std::process::id().to_be_bytes());
    payload.extend_from_slice(&NEXT_SECRET.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    write_message(stream, b'K', &payload)
}

fn send_ready(stream: &mut impl Write, session: &TransactionSession) -> io::Result<()> {
    let ready = session.ready_byte();
    write_message(stream, b'Z', &[ready])
}

fn write_command_complete(stream: &mut impl Write, tag: &str) -> io::Result<()> {
    let mut payload = Vec::with_capacity(tag.len() + 1);
    payload.extend_from_slice(tag.as_bytes());
    payload.push(0);
    write_message(stream, b'C', &payload)
}

fn write_error_response(stream: &mut impl Write, code: &str, message: &str) -> io::Result<()> {
    let mut payload = Vec::new();
    payload.push(b'S');
    payload.extend_from_slice(b"ERROR\0C");
    payload.extend_from_slice(code.as_bytes());
    payload.extend_from_slice(b"\0M");
    payload.extend_from_slice(message.as_bytes());
    payload.extend_from_slice(b"\0\0");
    write_message(stream, b'E', &payload)
}

/// Annotates the first ErrorResponse in a buffered statement response with its
/// batch position and string offset.
///
/// Returns `None` when the statement produced no ErrorResponse, in which case
/// the buffer can be forwarded unchanged.
fn annotate_batch_error(buffer: &[u8], index: usize, total: usize, offset: usize) -> Option<Vec<u8>> {
    let mut cursor = 0usize;
    while cursor + 5 <= buffer.len() {
        let length = u32::from_be_bytes([buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4]]) as usize;
        if length < 4 || cursor + 1 + length > buffer.len() {
            return None;
        }
        let payload_start = cursor + 5;
        let payload_end = cursor + 1 + length;
        if buffer[cursor] == b'E' {
            let prefix = format!("statement {index} of {total} failed at batch offset {offset}: ");
            let payload = annotate_error_payload(&buffer[payload_start..payload_end], &prefix);
            let mut output = Vec::with_capacity(buffer.len() + prefix.len());
            output.extend_from_slice(&buffer[..cursor]);
            output.push(b'E');
            output.extend_from_slice(&((payload.len() as u32) + 4).to_be_bytes());
            output.extend_from_slice(&payload);
            output.extend_from_slice(&buffer[payload_end..]);
            return Some(output);
        }
        cursor = payload_end;
    }
    None
}

/// Prepends `prefix` to the `M` (message) field of an ErrorResponse payload.
fn annotate_error_payload(payload: &[u8], prefix: &str) -> Vec<u8> {
    let mut fields: Vec<(u8, Vec<u8>)> = Vec::new();
    let mut cursor = 0usize;
    while cursor < payload.len() && payload[cursor] != 0 {
        let code = payload[cursor];
        let start = cursor + 1;
        let mut end = start;
        while end < payload.len() && payload[end] != 0 {
            end += 1;
        }
        if end >= payload.len() {
            break;
        }
        fields.push((code, payload[start..end].to_vec()));
        cursor = end + 1;
    }
    let mut output = Vec::with_capacity(payload.len() + prefix.len());
    for (code, value) in fields {
        output.push(code);
        if code == b'M' {
            output.extend_from_slice(prefix.as_bytes());
        }
        output.extend_from_slice(&value);
        output.push(0);
    }
    output.push(0);
    output
}

fn write_fatal(stream: &mut impl Write, code: &str, message: &str) -> io::Result<()> {
    let mut payload = Vec::new();
    payload.push(b'S');
    payload.extend_from_slice(b"FATAL\0C");
    payload.extend_from_slice(code.as_bytes());
    payload.extend_from_slice(b"\0M");
    payload.extend_from_slice(message.as_bytes());
    payload.extend_from_slice(b"\0\0");
    write_message(stream, b'E', &payload)
}

fn write_message(stream: &mut impl Write, tag: u8, payload: &[u8]) -> io::Result<()> {
    let encoded = encode_message(tag, payload, MAX_MESSAGE_FRAME).map_err(frame_error)?;
    stream.write_all(&encoded)
}

struct CancellableSocket {
    stream: TcpStream,
    stopping: Arc<AtomicBool>,
}

impl CancellableSocket {
    fn attempt<T>(&mut self, mut operation: impl FnMut(&mut TcpStream) -> io::Result<T>) -> io::Result<T> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if self.stopping.load(Ordering::Acquire) {
                return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "PostgreSQL service stopping"));
            }
            match operation(&mut self.stream) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "PostgreSQL socket I/O timeout"));
                    }
                    std::thread::sleep(Duration::from_millis(5));
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

fn frame_error(error: FrameError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("invalid PostgreSQL frame: {error:?}"))
}

fn format_startup_error(error: StartupParameterError) -> String {
    format!("invalid PostgreSQL startup parameters: {error:?}")
}
