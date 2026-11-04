//! YYDS SQL catalog bind and execution planning.
//!
//! Intended home for the bind/plan pipeline currently living inside
//! `yyds-gateway` (`catalog_bind`, `sql_planner`). Dialect gateways must only
//! own framing and wire presentation; they must not grow a second planner.
//!
//! Status: scaffold — API not published yet.
