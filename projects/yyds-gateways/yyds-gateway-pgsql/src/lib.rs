//! PostgreSQL disguise gateway for YYDS.
//!
//! Accepts a tiny `psql` surface (`SELECT 1`, `SELECT version()`, `\conninfo`) and
//! answers with YYDS-backed metadata. This is not a PostgreSQL server implementation.

use yyds_gateway::{version_label, GatewayConfig, GatewayKind};
use yyds_types::{version, Error, Result};

pub mod connection;
pub mod service;
pub mod wire;

/// Gateway kind implemented by this crate.
pub const GATEWAY_KIND: GatewayKind = GatewayKind::Pgsql;

/// Gateway identifier used in logs and diagnostics.
pub const GATEWAY_ID: &str = GATEWAY_KIND.id();

/// Default PostgreSQL disguise listen port.
pub const DEFAULT_PORT: u16 = GATEWAY_KIND.default_port();

fn matches_select_one(sql: &str) -> bool {
    let normalized = sql.trim().trim_end_matches(';').trim();
    normalized.eq_ignore_ascii_case("select 1")
}

fn matches_select_version(sql: &str) -> bool {
    let normalized = sql.trim().trim_end_matches(';').trim();
    normalized.eq_ignore_ascii_case("select version()")
}

/// Handles one psql command or SQL statement.
pub fn handle_input(input: &str) -> Result<Vec<String>> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(vec![format!("psql (YYDS {version})", version = version()), "Type \"help\" for help.".into()]);
    }

    if trimmed == "\\conninfo" {
        return Ok(vec![format!(
            "You are connected to database \"yyds\" as user \"yyds\" via YYDS {version}.",
            version = version()
        )]);
    }

    if matches_select_one(trimmed) {
        return Ok(vec![" ?column? ".into(), "----------".into(), "        1".into(), "(1 row)".into()]);
    }

    if matches_select_version(trimmed) {
        return Ok(vec![version_label()]);
    }

    Err(Error::Unsupported("unsupported postgresql input"))
}

/// Banner shown when a disguise server starts.
pub fn startup_banner(port: u16) -> String {
    yyds_gateway::startup_banner(&GatewayConfig::new(GATEWAY_KIND).with_port(port))
}
