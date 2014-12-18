//! Redis disguise gateway for YYDS.
//!
//! Accepts a tiny Redis-style command surface (`PING`, `INFO`) and answers with
//! YYDS-backed health metadata. This is not a Redis server implementation.
#![warn(missing_docs)]

use database_protocol::{GatewayConfig, GatewayKind, startup_banner as protocol_startup_banner};
use database_query::version_label;
use database_types::{Error, Result};

pub mod blocking;
pub mod connection;
mod collections;
pub mod pubsub;
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

/// Banner shown when a disguise server starts.
pub fn startup_banner(port: u16) -> String {
    protocol_startup_banner(&GatewayConfig::new(GATEWAY_KIND).with_port(port))
}
