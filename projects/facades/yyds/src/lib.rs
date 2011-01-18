//! YYDS public facade.
//!
//! Distribution shells (`yyds-napi`, `yyds-pyo3`, …) depend on **this crate only**.
//! Internal crates stay workspace-private and are re-exported here as modules.

#![deny(missing_docs)]

/// Shared identity, errors, KV commands, and routing types.
pub mod types {
    pub use yyds_types::*;
}

/// `.yykv` shard storage.
pub mod kv {
    pub use yyds_kv::*;
}

/// `.yyds` catalog lifecycle.
pub mod catalog {
    pub use yyds_catalog::*;
}

/// Directory ownership and node identity.
pub mod control {
    pub use yyds_control::*;
}

/// Shard / coordinator execution boundary.
pub mod execution {
    pub use yyds_execution::*;
}

/// Protocol disguise gateways (Redis / MySQL / PostgreSQL / Mongo).
pub mod gateway {
    pub use yyds_gateway::*;

    /// Redis RESP disguise.
    pub mod redis {
        pub use yyds_gateway_redis::*;
    }

    /// MySQL classic-protocol disguise.
    pub mod mysql {
        pub use yyds_gateway_mysql::*;
    }

    /// PostgreSQL protocol-v3 disguise.
    pub mod pgsql {
        pub use yyds_gateway_pgsql::*;
    }

    /// MongoDB OP_MSG disguise.
    pub mod mongo {
        pub use yyds_gateway_mongo::*;
    }
}

pub use types::version;

/// Library version string (Cargo package version).
pub fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
