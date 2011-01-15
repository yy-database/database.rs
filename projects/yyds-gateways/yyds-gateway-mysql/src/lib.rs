//! MySQL disguise gateway for YYDS.
//!
//! Provides a bounded protocol-v10 handshake. SQL parsing and execution are
//! not yet connected.

use yyds_gateway::{GatewayConfig, GatewayKind};

pub mod connection;
pub mod service;
pub mod wire;

/// Gateway kind implemented by this crate.
pub const GATEWAY_KIND: GatewayKind = GatewayKind::Mysql;

/// Gateway identifier used in logs and diagnostics.
pub const GATEWAY_ID: &str = GATEWAY_KIND.id();

/// Default MySQL disguise listen port.
pub const DEFAULT_PORT: u16 = GATEWAY_KIND.default_port();

/// Banner shown when a disguise server starts.
pub fn startup_banner(port: u16) -> String {
    yyds_gateway::startup_banner(&GatewayConfig::new(GATEWAY_KIND).with_port(port))
}
