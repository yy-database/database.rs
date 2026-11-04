use std::path::PathBuf;

use crate::{GatewayCatalog, GatewayKind};

/// Runtime configuration for one disguise gateway listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayConfig {
    /// Disguise protocol kind.
    pub kind: GatewayKind,
    /// TCP listen port.
    pub port: u16,
    /// Optional `.yyds` catalog binding for the gateway process.
    pub catalog: GatewayCatalog,
}

impl GatewayConfig {
    /// Creates a config using the kind's default port.
    pub fn new(kind: GatewayKind) -> Self {
        Self { kind, port: kind.default_port(), catalog: GatewayCatalog::new() }
    }

    /// Overrides the listen port.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Attaches a catalog path to the gateway process.
    pub fn with_catalog_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.catalog = GatewayCatalog::with_path(path);
        self
    }
}
