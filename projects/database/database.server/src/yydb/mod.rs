//! YYDB wire server — loopback-first TCP/WebSocket serve for out-of-process clients.
//!
//! Depends on `database.execution` for execution and `database.protocol.yy` for framing.
//! Embedded Rust hosts use `database.execution connection` directly. Remote hosts connect
//! through `database.client::yy` / `@yydb/database.client::yy`.
//! Language bindings (`yydb-napi`, `yydb-pyo3`) start this loop for `serve` /
//! `Database.open()`-style products.

#![deny(missing_docs)]

pub mod auth;
pub mod dispatch;
pub mod host_wire;

mod serve;

pub use auth::{AuthBootstrap, Permission, Principal, PrincipalKind, Role, Session, SessionRegistry};
pub use dispatch::{PreparedPlan, SessionState, dispatch, is_loopback_host};
pub use serve::{assert_bind_allowed, run_serve};
