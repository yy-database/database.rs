//! Redis disguise gateway for YYDS.
//!
//! Accepts a tiny Redis-style command surface (`PING`, `INFO`) and answers with
//! YYDS-backed health metadata. This is not a Redis server implementation.
#![warn(missing_docs)]

use std::io;

use database_execution::cluster::KeyValueExecutor;
use database_protocol::{GatewayConfig, GatewayKind, startup_banner as protocol_startup_banner};
use database_query::version_label;
use database_types::{Error, Result};

pub mod blocking;
pub mod connection;
mod collections;
pub mod pubsub;
pub mod server_face;
pub mod service;

/// 命令绑定（来自 `database-protocol-redis`）。
pub use database_protocol_redis::bind;
/// RESP 编解码（来自 `database-protocol-redis`）。
pub use database_protocol_redis::resp;

/// Gateway kind implemented by this crate.
pub const GATEWAY_KIND: GatewayKind = GatewayKind::Redis;

/// Gateway identifier used in logs and diagnostics.
pub const GATEWAY_ID: &str = GATEWAY_KIND.id();

/// Default Redis disguise listen port.
pub const DEFAULT_PORT: u16 = GATEWAY_KIND.default_port();

/// Parses one Redis-style command token sequence into a normalized command name.
pub fn parse_command(input: &str) -> &str {
    input.split_whitespace().next().unwrap_or("").trim()
}

/// Handles a Redis-style command and returns the disguise response body.
pub fn handle_command(input: &str) -> Result<String> {
    match parse_command(input).to_ascii_uppercase().as_str() {
        "PING" => Ok("PONG".to_string()),
        "INFO" => Ok(format!("# Server\r\nredis_version:{}\r\nredis_mode:disguise\r\nrole:master\r\n", version_label())),
        other if other.is_empty() => Err(Error::Unsupported("empty redis command")),
        _other => Err(Error::Unsupported("unknown redis command")),
    }
}

/// Executes one hash-family request against an external executor and writes the RESP reply.
///
/// Command semantics, reply framing and type checks stay in the redis collection
/// handler. The caller only supplies the logical database namespace, the connection
/// protocol version and the key-value backend, so an embedder such as a language
/// binding can reuse this family without reimplementing it.
pub fn execute_hash_command(
    stream: &mut dyn io::Write,
    arguments: &[&[u8]],
    executor: &dyn KeyValueExecutor,
    namespace: &database_types::cluster::Namespace,
    protocol: u8,
) -> io::Result<()> {
    let request = resp::Request {
        arguments: arguments.to_vec(),
        consumed: 0,
    };
    collections::handle_hash(stream, &request, Some(executor), namespace, protocol)
}

/// Executes one set-family request against an external executor and writes the RESP reply.
///
/// Command semantics, reply framing and type checks stay in the redis collection
/// handler. The caller only supplies the logical database namespace, the connection
/// protocol version and the key-value backend, so an embedder such as a language
/// binding can reuse this family without reimplementing it.
pub fn execute_set_command(
    stream: &mut dyn io::Write,
    arguments: &[&[u8]],
    executor: &dyn KeyValueExecutor,
    namespace: &database_types::cluster::Namespace,
    protocol: u8,
) -> io::Result<()> {
    let request = resp::Request {
        arguments: arguments.to_vec(),
        consumed: 0,
    };
    collections::handle_set(stream, &request, Some(executor), namespace, protocol)
}

/// Executes one list-family request against an external executor and writes the RESP reply.
///
/// Command semantics, reply framing and type checks stay in the redis collection
/// handler. Blocking commands run in their non-blocking form: an empty key replies
/// at once and the caller owns the wait loop.
pub fn execute_list_command(
    stream: &mut dyn io::Write,
    arguments: &[&[u8]],
    executor: &dyn KeyValueExecutor,
    namespace: &database_types::cluster::Namespace,
    protocol: u8,
) -> io::Result<()> {
    let request = resp::Request {
        arguments: arguments.to_vec(),
        consumed: 0,
    };
    let registry = blocking::BlockingRegistry::new();
    collections::handle_list(stream, &request, Some(executor), namespace, protocol, &registry, false)
}

/// Executes one zset-family request against an external executor and writes the RESP reply.
///
/// Command semantics, reply framing and type checks stay in the redis collection
/// handler. Blocking commands run in their non-blocking form: an empty key replies
/// at once and the caller owns the wait loop.
pub fn execute_zset_command(
    stream: &mut dyn io::Write,
    arguments: &[&[u8]],
    executor: &dyn KeyValueExecutor,
    namespace: &database_types::cluster::Namespace,
    protocol: u8,
) -> io::Result<()> {
    let request = resp::Request {
        arguments: arguments.to_vec(),
        consumed: 0,
    };
    collections::handle_zset(stream, &request, Some(executor), namespace, protocol)
}

/// Executes one HyperLogLog family request and writes the RESP reply.
///
/// Command semantics, reply framing and type checks stay in the redis collection
/// handler. The caller only supplies the logical database namespace and the
/// key-value backend, so an embedder such as a language binding can reuse this
/// family without reimplementing the hash, register and estimation logic.
pub fn execute_hyperloglog_command(
    stream: &mut dyn io::Write,
    arguments: &[&[u8]],
    executor: &dyn KeyValueExecutor,
    namespace: &database_types::cluster::Namespace,
) -> io::Result<()> {
    let request = resp::Request {
        arguments: arguments.to_vec(),
        consumed: 0,
    };
    collections::handle_hyperloglog(stream, &request, Some(executor), namespace)
}

/// Executes one string or key family request and writes the RESP reply.
///
/// Command semantics, reply framing and type checks stay in the redis connection
/// handler. The caller only supplies the logical database namespace, the connection
/// protocol version and the key-value backend, so an embedder such as a language
/// binding can reuse this family without reimplementing it.
pub fn execute_string_command(
    stream: &mut dyn io::Write,
    arguments: &[&[u8]],
    executor: &dyn KeyValueExecutor,
    namespace: &database_types::cluster::Namespace,
    protocol: u8,
) -> io::Result<()> {
    let request = resp::Request {
        arguments: arguments.to_vec(),
        consumed: 0,
    };
    if connection::handle_string(stream, &request, Some(executor), Some(executor), None, namespace, protocol)? {
        return Ok(());
    }
    connection::write_unknown_command(stream, arguments)
}

/// Banner shown when a disguise server starts.
pub fn startup_banner(port: u16) -> String {
    protocol_startup_banner(&GatewayConfig::new(GATEWAY_KIND).with_port(port))
}

/// 服务器面会话：`CLIENT` / `COMMAND` / `INFO` / `CONFIG` / `ACL`。
///
/// [`ClientRegistry`] 是监听器级状态，嵌入式宿主可让多条 [`EmbeddedServerFace`]
/// 共享同一张登记表，使 `CLIENT LIST` 与 `CLIENT KILL` 看到宿主侧的其它连接。
pub use server_face::{ClientRegistry, EmbeddedServerFace};

/// Executes one server-face request against an external executor and writes the RESP reply.
///
/// `CLIENT` / `COMMAND` / `INFO` / `CONFIG` / `ACL` semantics stay in the upstream
/// server face. The session carries the connection-scoped fields (client name, database,
/// protocol and config overrides), so an embedder such as a language binding keeps the
/// same observable state across calls without reimplementing any command.
pub fn execute_server_command(
    session: &mut EmbeddedServerFace,
    stream: &mut dyn io::Write,
    arguments: &[&[u8]],
    executor: &dyn KeyValueExecutor,
    database: u8,
    protocol: u8,
) -> io::Result<bool> {
    session.set_session(database, protocol);
    session.execute(stream, arguments, executor)
}
